use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use openleadr_client::{Client, ClientCredentials, Filter, VirtualEndNode};
use openleadr_wire::{
    event::{EventRequest, EventType},
    program::ProgramId,
    values_map::Value,
};
use tracing::{info, warn};
use url::Url;

use crate::dispatch::grpc_client::DispatchType;
use crate::dispatch::DispatchAdapter;

/// VEN-side OpenADR 3 listener backed by OpenLEADR.
///
/// The inverse of [`OpenLeadrAdapter`](super::openleadr::OpenLeadrAdapter):
/// instead of *creating* demand-response events on a VTN, this polls a
/// (typically utility-operated) VTN as a Virtual End Node and translates
/// incoming `DISPATCH_SETPOINT` events into downstream dispatch through the
/// injected [`DispatchAdapter`] (e.g. gRPC to edge controllers). Positive
/// setpoints map to FLEX_UP, negative to FLEX_DOWN.
///
/// Configure with:
/// - `OPENLEADR_VEN_VTN_URL` (enables the listener)
/// - optional `OPENLEADR_VEN_CLIENT_ID` + `OPENLEADR_VEN_CLIENT_SECRET`
/// - optional `OPENLEADR_VEN_PROGRAM_ID` (filter events to one program)
/// - optional `OPENLEADR_VEN_POLL_SECS` (default 30)
pub struct OpenLeadrVenListener {
    client: Client<VirtualEndNode>,
    program_id: Option<ProgramId>,
    poll_interval: StdDuration,
    adapter: Arc<dyn DispatchAdapter>,
    // event id -> last modification time we acted on; re-dispatch on update
    seen: HashMap<String, DateTime<Utc>>,
}

impl OpenLeadrVenListener {
    pub fn from_env(adapter: Arc<dyn DispatchAdapter>) -> Result<Option<Self>> {
        let Ok(vtn_url) = std::env::var("OPENLEADR_VEN_VTN_URL") else {
            return Ok(None);
        };

        let credentials = match (
            std::env::var("OPENLEADR_VEN_CLIENT_ID"),
            std::env::var("OPENLEADR_VEN_CLIENT_SECRET"),
        ) {
            (Ok(id), Ok(secret)) => Some((id, secret)),
            _ => None,
        };
        let poll_secs = std::env::var("OPENLEADR_VEN_POLL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);

        Ok(Some(Self::new(
            &vtn_url,
            credentials,
            std::env::var("OPENLEADR_VEN_PROGRAM_ID").ok(),
            poll_secs,
            adapter,
        )?))
    }

    pub fn new(
        vtn_url: &str,
        credentials: Option<(String, String)>,
        program_id: Option<String>,
        poll_secs: u64,
        adapter: Arc<dyn DispatchAdapter>,
    ) -> Result<Self> {
        let vtn_url = vtn_url
            .parse::<Url>()
            .with_context(|| format!("invalid OPENLEADR_VEN_VTN_URL: {vtn_url}"))?;
        let credentials = credentials.map(|(id, secret)| ClientCredentials::new(id, secret));
        let program_id = program_id
            .map(|id| {
                ProgramId::new(&id)
                    .ok_or_else(|| anyhow!("invalid OPENLEADR_VEN_PROGRAM_ID: {id}"))
            })
            .transpose()?;

        Ok(Self {
            client: Client::<VirtualEndNode>::with_url(vtn_url, credentials),
            program_id,
            poll_interval: StdDuration::from_secs(poll_secs.max(1)),
            adapter,
            seen: HashMap::new(),
        })
    }

    pub fn poll_interval(&self) -> StdDuration {
        self.poll_interval
    }

    /// One poll cycle: fetch events, dispatch every new/updated DISPATCH_SETPOINT
    /// event through the adapter. Returns the number of dispatches executed.
    pub async fn poll_once(&mut self) -> Result<usize> {
        let events = self
            .client
            .get_event_list(self.program_id.as_ref(), Filter::<&str>::None)
            .await
            .map_err(|e| anyhow!("OpenADR VEN event poll failed: {e}"))?;

        let mut dispatched = 0;
        for event in events {
            let id = event.id().as_str().to_string();
            let modified = event.modification_date_time();
            if self.seen.get(&id).is_some_and(|prev| *prev >= modified) {
                continue;
            }

            let Some(setpoint_kw) = extract_dispatch_setpoint(event.content()) else {
                // Not a dispatch event (or no numeric payload) — remember it so
                // we don't re-inspect every cycle.
                self.seen.insert(id, modified);
                continue;
            };

            let (action, capacity_kw) = setpoint_to_dispatch(setpoint_kw);
            match self.adapter.execute_dispatch(action, capacity_kw).await {
                Ok(()) => {
                    info!(
                        "OpenADR VEN event executed: event_id={} setpoint_kw={} action={:?} capacity_kw={}",
                        id, setpoint_kw, action, capacity_kw
                    );
                    self.seen.insert(id, modified);
                    dispatched += 1;
                }
                Err(e) => {
                    // Leave it out of `seen` so the next cycle retries.
                    warn!("OpenADR VEN dispatch failed for event {}: {}", id, e);
                }
            }
        }
        Ok(dispatched)
    }

    /// Poll forever at `poll_interval` until `shutdown` resolves.
    pub async fn run(mut self, shutdown: impl std::future::Future<Output = ()>) {
        info!(
            "OpenADR VEN listener started (poll every {:?})",
            self.poll_interval
        );
        tokio::pin!(shutdown);
        let mut ticker = tokio::time::interval(self.poll_interval);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("OpenADR VEN listener shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.poll_once().await {
                        warn!("OpenADR VEN poll error: {}", e);
                    }
                }
            }
        }
    }
}

/// Extract the first numeric DISPATCH_SETPOINT payload value (kW) from an event.
fn extract_dispatch_setpoint(event: &EventRequest) -> Option<f64> {
    event.intervals.as_ref()?.iter().find_map(|interval| {
        interval.payloads.iter().find_map(|payload| {
            if payload.value_type != EventType::DispatchSetpoint {
                return None;
            }
            payload.values.iter().find_map(|v| match v {
                Value::Number(n) => Some(*n),
                _ => None,
            })
        })
    })
}

/// Signed setpoint (kW) → dispatch action + magnitude. Positive = FLEX_UP.
fn setpoint_to_dispatch(setpoint_kw: f64) -> (DispatchType, f64) {
    if setpoint_kw >= 0.0 {
        (DispatchType::FLEX_UP, setpoint_kw)
    } else {
        (DispatchType::FLEX_DOWN, -setpoint_kw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use openleadr_wire::event::{EventInterval, EventValuesMap};

    struct NoopAdapter;

    #[async_trait]
    impl DispatchAdapter for NoopAdapter {
        async fn execute_dispatch(&self, _action: DispatchType, _capacity_kw: f64) -> Result<()> {
            Ok(())
        }
    }

    fn event_with_payload(value_type: EventType, values: Vec<Value>) -> EventRequest {
        let mut event = EventRequest::new(ProgramId::new("test-program").unwrap());
        event.intervals = Some(vec![EventInterval {
            id: 0,
            interval_period: None,
            payloads: vec![EventValuesMap { value_type, values }],
        }]);
        event
    }

    #[test]
    fn setpoint_sign_maps_to_action() {
        assert_eq!(
            setpoint_to_dispatch(75.0),
            (DispatchType::FLEX_UP, 75.0)
        );
        assert_eq!(
            setpoint_to_dispatch(-30.5),
            (DispatchType::FLEX_DOWN, 30.5)
        );
        assert_eq!(setpoint_to_dispatch(0.0), (DispatchType::FLEX_UP, 0.0));
    }

    #[test]
    fn extracts_numeric_dispatch_setpoint() {
        let event = event_with_payload(
            EventType::DispatchSetpoint,
            vec![Value::Number(42.0)],
        );
        assert_eq!(extract_dispatch_setpoint(&event), Some(42.0));
    }

    #[test]
    fn ignores_non_setpoint_payloads() {
        let event = event_with_payload(EventType::Price, vec![Value::Number(1.0)]);
        assert_eq!(extract_dispatch_setpoint(&event), None);
    }

    #[test]
    fn ignores_non_numeric_setpoint_values() {
        let event = event_with_payload(
            EventType::DispatchSetpoint,
            vec![Value::String("oops".to_string())],
        );
        assert_eq!(extract_dispatch_setpoint(&event), None);
    }

    #[test]
    fn invalid_ven_vtn_url_rejected() {
        let err = OpenLeadrVenListener::new(
            "not a url",
            None,
            None,
            30,
            Arc::new(NoopAdapter),
        );
        assert!(err.is_err());
    }

    #[test]
    fn invalid_ven_program_id_rejected() {
        let err = OpenLeadrVenListener::new(
            "http://localhost:4030",
            None,
            Some("has spaces".to_string()),
            30,
            Arc::new(NoopAdapter),
        );
        assert!(err.is_err());
    }
}
