use crate::dispatch::grpc_client::DispatchType;
use crate::dispatch::DispatchAdapter;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use openleadr_client::{BusinessLogic, Client, ClientCredentials};
use openleadr_wire::{
    event::{EventInterval, EventPayloadDescriptor, EventType, EventValuesMap, Priority},
    interval::IntervalPeriod,
    program::{ProgramId, ProgramRequest},
    target::Target,
    values_map::Value,
    Duration,
};
use tokio::sync::Mutex;
use tracing::info;
use url::Url;

/// OpenADR 3.1 adapter backed by OpenLEADR.
///
/// The bridge acts as business logic against a VTN. Configure it with:
/// - `OPENLEADR_VTN_URL`
/// - optional `OPENLEADR_CLIENT_ID` + `OPENLEADR_CLIENT_SECRET`
/// - optional `OPENLEADR_PROGRAM_ID`, `OPENLEADR_PROGRAM_NAME`, `OPENLEADR_TARGET`
pub struct OpenLeadrAdapter {
    client: Client<BusinessLogic>,
    /// Program id from config (`OPENLEADR_PROGRAM_ID`), if any.
    configured_program_id: Option<ProgramId>,
    /// Resolved program handle, cached so each dispatch does not re-fetch the
    /// program from the VTN. Invalidated when event creation fails (e.g. the
    /// program was deleted on the VTN) so the next dispatch re-resolves.
    program: Mutex<Option<openleadr_client::ProgramClient<BusinessLogic>>>,
    program_name: String,
    target: Option<Target>,
    event_duration_hours: f32,
}

impl OpenLeadrAdapter {
    pub fn from_env() -> Result<Option<Self>> {
        let Ok(vtn_url) = std::env::var("OPENLEADR_VTN_URL") else {
            return Ok(None);
        };

        Ok(Some(Self::new(
            &vtn_url,
            env_pair("OPENLEADR_CLIENT_ID", "OPENLEADR_CLIENT_SECRET"),
            std::env::var("OPENLEADR_PROGRAM_ID").ok(),
            std::env::var("OPENLEADR_PROGRAM_NAME")
                .unwrap_or_else(|_| "gridtokenx-flex-dispatch".to_string()),
            std::env::var("OPENLEADR_TARGET").ok(),
            std::env::var("OPENLEADR_EVENT_DURATION_HOURS")
                .ok()
                .and_then(|v| v.parse::<f32>().ok())
                .unwrap_or(1.0),
        )?))
    }

    pub fn new(
        vtn_url: &str,
        credentials: Option<(String, String)>,
        program_id: Option<String>,
        program_name: String,
        target: Option<String>,
        event_duration_hours: f32,
    ) -> Result<Self> {
        let vtn_url = vtn_url
            .parse::<Url>()
            .with_context(|| format!("invalid OPENLEADR_VTN_URL: {vtn_url}"))?;
        let credentials = credentials.map(|(id, secret)| ClientCredentials::new(id, secret));
        let program_id = program_id
            .map(|id| {
                ProgramId::new(&id).ok_or_else(|| anyhow!("invalid OPENLEADR_PROGRAM_ID: {id}"))
            })
            .transpose()?;
        let target = target
            .map(|target| {
                target
                    .parse::<Target>()
                    .with_context(|| format!("invalid OPENLEADR_TARGET: {target}"))
            })
            .transpose()?;

        Ok(Self {
            client: Client::<BusinessLogic>::with_url(vtn_url, credentials),
            configured_program_id: program_id,
            program: Mutex::new(None),
            program_name,
            target,
            event_duration_hours,
        })
    }

    async fn dispatch_event(&self, action: DispatchType, capacity_kw: f64) -> Result<()> {
        // Resolve-once: the lock is held across the dispatch, serializing
        // concurrent dispatches — fine, they are rare and the VTN call
        // dominates anyway.
        let mut cache = self.program.lock().await;
        if cache.is_none() {
            *cache = Some(self.resolve_program().await?);
        }
        let program = cache.as_ref().expect("program cache populated above");

        let setpoint_kw = match action {
            DispatchType::FLEX_UP => capacity_kw,
            DispatchType::FLEX_DOWN => -capacity_kw,
        };

        let mut interval_period = IntervalPeriod::new(gridtokenx_telemetry::time::now());
        interval_period.duration = Some(Duration::hours(self.event_duration_hours));

        let interval = EventInterval {
            id: 0,
            interval_period: Some(interval_period),
            payloads: vec![EventValuesMap {
                value_type: EventType::DispatchSetpoint,
                values: vec![Value::Number(setpoint_kw)],
            }],
        };

        let mut event = program.new_event(vec![interval]);
        event.event_name = Some(format!(
            "gridtokenx-{}-{:.3}kw",
            dispatch_name(action),
            capacity_kw
        ));
        event.priority = Priority::new(10);
        event.duration = Some(Duration::hours(self.event_duration_hours));
        event.payload_descriptors = Some(vec![EventPayloadDescriptor::new(
            EventType::DispatchSetpoint,
        )]);
        if let Some(target) = &self.target {
            event.targets = vec![target.clone()];
        }

        let event = match program.create_event(event).await {
            Ok(event) => event,
            Err(e) => {
                // The program may have been deleted/recreated on the VTN:
                // drop the cache so the next dispatch re-resolves instead of
                // failing forever against a stale handle.
                *cache = None;
                return Err(anyhow!("OpenADR event creation failed: {e}"));
            }
        };

        info!(
            "OpenADR dispatch event created: event_id={} action={} capacity_kw={}",
            event.id(),
            dispatch_name(action),
            capacity_kw
        );
        Ok(())
    }

    async fn resolve_program(&self) -> Result<openleadr_client::ProgramClient<BusinessLogic>> {
        if let Some(program_id) = &self.configured_program_id {
            return self
                .client
                .get_program_by_id(program_id)
                .await
                .map_err(|e| anyhow!("OpenADR program lookup failed: {e}"));
        }

        // Look the program up by name before creating it: program_name is
        // unique on the VTN, so after a process restart (cached id lost) a
        // blind create would 409 forever.
        let existing = self
            .client
            .get_program_list(openleadr_client::Filter::<&str>::None)
            .await
            .map_err(|e| anyhow!("OpenADR program list failed: {e}"))?
            .into_iter()
            .find(|p| p.content().program_name == self.program_name);

        match existing {
            Some(program) => Ok(program),
            None => self
                .client
                .create_program(ProgramRequest::new(self.program_name.clone()))
                .await
                .map_err(|e| anyhow!("OpenADR program creation failed: {e}")),
        }
    }
}

#[async_trait]
impl DispatchAdapter for OpenLeadrAdapter {
    async fn execute_dispatch(&self, action: DispatchType, capacity_kw: f64) -> Result<()> {
        self.dispatch_event(action, capacity_kw).await
    }
}

fn env_pair(left: &str, right: &str) -> Option<(String, String)> {
    match (std::env::var(left), std::env::var(right)) {
        (Ok(left), Ok(right)) => Some((left, right)),
        _ => None,
    }
}

fn dispatch_name(action: DispatchType) -> &'static str {
    match action {
        DispatchType::FLEX_UP => "flex-up",
        DispatchType::FLEX_DOWN => "flex-down",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(
        vtn_url: &str,
        program_id: Option<&str>,
        target: Option<&str>,
    ) -> Result<OpenLeadrAdapter> {
        OpenLeadrAdapter::new(
            vtn_url,
            Some(("client".to_string(), "secret".to_string())),
            program_id.map(str::to_string),
            "test-program".to_string(),
            target.map(str::to_string),
            1.0,
        )
    }

    #[test]
    fn valid_config_constructs() {
        assert!(build("http://localhost:4030", Some("program-1"), Some("GROUP-1")).is_ok());
    }

    #[test]
    fn invalid_vtn_url_rejected() {
        let Err(err) = build("not a url", None, None) else {
            panic!("expected invalid URL to be rejected");
        };
        assert!(err.to_string().contains("OPENLEADR_VTN_URL"), "{err}");
    }

    #[test]
    fn invalid_program_id_rejected() {
        let Err(err) = build("http://localhost:4030", Some("has spaces"), None) else {
            panic!("expected invalid program id to be rejected");
        };
        assert!(err.to_string().contains("OPENLEADR_PROGRAM_ID"), "{err}");
    }

    #[test]
    fn invalid_target_rejected() {
        let Err(err) = build("http://localhost:4030", None, Some("bad target!")) else {
            panic!("expected invalid target to be rejected");
        };
        assert!(err.to_string().contains("OPENLEADR_TARGET"), "{err}");
    }

    #[test]
    fn dispatch_names() {
        assert_eq!(dispatch_name(DispatchType::FLEX_UP), "flex-up");
        assert_eq!(dispatch_name(DispatchType::FLEX_DOWN), "flex-down");
    }
}
