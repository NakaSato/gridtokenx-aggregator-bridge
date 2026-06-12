use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use openleadr_client::{Client, ClientCredentials, EventClient, Filter, VirtualEndNode};
use openleadr_wire::{
    event::{EventRequest, EventType},
    interval::{Interval, IntervalPeriod},
    program::ProgramId,
    report::{ReportResource, ResourceName},
    target::Target,
    values_map::{Value, ValueType, ValuesMap},
};
use redis::aio::ConnectionManager;
use tracing::{debug, info, warn};
use url::Url;

use crate::dispatch::grpc_client::DispatchType;
use crate::dispatch::DispatchAdapter;

/// Redis hash persisting which event versions were already executed, so a
/// process restart does not re-execute still-listed events.
const SEEN_KEY: &str = "gridtokenx:openleadr:ven:executed";

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
/// - optional `OPENLEADR_VEN_TARGET` (only events carrying this target)
/// - optional `OPENLEADR_VEN_POLL_SECS` (default 30)
/// - optional `OPENLEADR_VEN_REPORTS=false` (disable execution reports)
/// - optional `OPENLEADR_VEN_CLIENT_NAME` (report clientName, default
///   `gridtokenx-aggregator-bridge`)
/// - `REDIS_URL` (persists executed-event dedup across restarts; RAM-only if absent)
pub struct OpenLeadrVenListener {
    client: Client<VirtualEndNode>,
    program_id: Option<ProgramId>,
    target: Option<Target>,
    poll_interval: StdDuration,
    adapter: Arc<dyn DispatchAdapter>,
    // event id -> last modification time we acted on; re-dispatch on update
    seen: HashMap<String, DateTime<Utc>>,
    // executed events that are still inside their active window; used to spot
    // VTN-side cancellation (event vanished while active)
    executed_active: HashMap<String, DateTime<Utc>>,
    redis: Option<RedisSeenStore>,
    seen_loaded: bool,
    /// Post an execution report back to the VTN after each dispatch.
    reports_enabled: bool,
    /// `clientName` stamped on execution reports.
    client_name: String,
}

/// What to do with one polled event, given "now".
#[derive(Debug, PartialEq)]
enum EventDecision {
    /// Active dispatch event — execute this setpoint.
    Execute { setpoint_kw: f64 },
    /// Dispatch event whose window hasn't started — re-check next poll.
    NotYetActive,
    /// Dispatch event whose window already ended — never execute.
    Expired,
    /// Not a numeric DISPATCH_SETPOINT event.
    NotDispatch,
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

        let mut listener = Self::new(
            &vtn_url,
            credentials,
            std::env::var("OPENLEADR_VEN_PROGRAM_ID").ok(),
            std::env::var("OPENLEADR_VEN_TARGET").ok(),
            poll_secs,
            adapter,
            std::env::var("REDIS_URL").ok(),
        )?;
        listener.reports_enabled = std::env::var("OPENLEADR_VEN_REPORTS")
            .map(|v| v.to_lowercase() != "false")
            .unwrap_or(true);
        if let Ok(name) = std::env::var("OPENLEADR_VEN_CLIENT_NAME") {
            listener.client_name = name;
        }
        Ok(Some(listener))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vtn_url: &str,
        credentials: Option<(String, String)>,
        program_id: Option<String>,
        target: Option<String>,
        poll_secs: u64,
        adapter: Arc<dyn DispatchAdapter>,
        redis_url: Option<String>,
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
        let target = target
            .map(|t| {
                t.parse::<Target>()
                    .with_context(|| format!("invalid OPENLEADR_VEN_TARGET: {t}"))
            })
            .transpose()?;

        Ok(Self {
            client: Client::<VirtualEndNode>::with_url(vtn_url, credentials),
            program_id,
            target,
            poll_interval: StdDuration::from_secs(poll_secs.max(1)),
            adapter,
            seen: HashMap::new(),
            executed_active: HashMap::new(),
            redis: redis_url.map(RedisSeenStore::new),
            seen_loaded: false,
            reports_enabled: true,
            client_name: "gridtokenx-aggregator-bridge".to_string(),
        })
    }

    pub fn poll_interval(&self) -> StdDuration {
        self.poll_interval
    }

    /// One poll cycle: fetch events, dispatch every new/updated DISPATCH_SETPOINT
    /// event that is inside its active window. Returns dispatches executed.
    pub async fn poll_once(&mut self) -> Result<usize> {
        if !self.seen_loaded {
            if let Some(store) = &mut self.redis {
                match store.load().await {
                    Ok(persisted) => {
                        debug!(
                            "OpenADR VEN dedup: loaded {} executed events from Redis",
                            persisted.len()
                        );
                        self.seen.extend(persisted);
                    }
                    Err(e) => warn!("OpenADR VEN dedup load failed (RAM-only): {}", e),
                }
            }
            self.seen_loaded = true;
        }

        let events = match &self.target {
            Some(target) => {
                let targets = [target.as_str()];
                self.client
                    .get_event_list(self.program_id.as_ref(), Filter::By(&targets))
                    .await
            }
            None => {
                self.client
                    .get_event_list(self.program_id.as_ref(), Filter::<&str>::None)
                    .await
            }
        }
        .map_err(|e| anyhow!("OpenADR VEN event poll failed: {e}"))?;

        let now = Utc::now();
        let mut present: HashMap<String, ()> = HashMap::new();
        let mut dispatched = 0;

        for event in events {
            let id = event.id().as_str().to_string();
            present.insert(id.clone(), ());
            let modified = event.modification_date_time();
            if self.seen.get(&id).is_some_and(|prev| *prev >= modified) {
                continue;
            }

            match decide(event.content(), now) {
                EventDecision::NotDispatch => {
                    self.mark_seen(id, modified).await;
                }
                EventDecision::Expired => {
                    debug!("OpenADR VEN event {} expired before execution", id);
                    self.mark_seen(id, modified).await;
                }
                EventDecision::NotYetActive => {
                    // Deliberately NOT marked seen: re-evaluated every poll
                    // until its window opens.
                    debug!("OpenADR VEN event {} not yet active", id);
                }
                EventDecision::Execute { setpoint_kw } => {
                    let (action, capacity_kw) = setpoint_to_dispatch(setpoint_kw);
                    match self.adapter.execute_dispatch(action, capacity_kw).await {
                        Ok(()) => {
                            info!(
                                "OpenADR VEN event executed: event_id={} setpoint_kw={} action={:?} capacity_kw={}",
                                id, setpoint_kw, action, capacity_kw
                            );
                            self.executed_active
                                .insert(id.clone(), active_window_end(event.content(), now));
                            self.mark_seen(id, modified).await;
                            dispatched += 1;
                            // Best-effort: the dispatch already happened, so a
                            // report failure must not fail (or retry) it.
                            if self.reports_enabled {
                                if let Err(e) =
                                    self.post_execution_report(&event, setpoint_kw, now).await
                                {
                                    warn!(
                                        "OpenADR VEN execution report failed for event {}: {}",
                                        event.id().as_str(),
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            // Leave it out of `seen` so the next cycle retries.
                            warn!("OpenADR VEN dispatch failed for event {}: {}", id, e);
                        }
                    }
                }
            }
        }

        // Cancellation visibility: an executed event that vanished from the VTN
        // while still inside its window was cancelled upstream. No automatic
        // revert — the right counter-action is operator/market specific — but
        // it must not pass silently.
        self.executed_active.retain(|id, end| {
            if present.contains_key(id) {
                return *end > now;
            }
            if *end > now {
                warn!(
                    "OpenADR VEN event {} cancelled on the VTN while active (executed earlier; manual revert may be required)",
                    id
                );
            }
            false
        });

        Ok(dispatched)
    }

    /// Confirm an executed dispatch back to the VTN as an OpenADR report: one
    /// AGGREGATED_REPORT resource with a single SETPOINT interval carrying the
    /// setpoint we acted on, stamped with the execution time.
    async fn post_execution_report(
        &self,
        event: &EventClient<VirtualEndNode>,
        setpoint_kw: f64,
        executed_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut report = event.new_report(self.client_name.clone());
        report.report_name = Some(format!("gridtokenx-execution-{}", event.id().as_str()));
        report.resources = vec![ReportResource {
            resource_name: ResourceName::AggregatedReport,
            interval_period: Some(IntervalPeriod::new(executed_at)),
            intervals: vec![Interval::new(
                0,
                vec![ValuesMap {
                    value_type: ValueType("SETPOINT".to_string()),
                    values: vec![Value::Number(setpoint_kw)],
                }],
            )],
        }];

        let report = event
            .create_report(report)
            .await
            .map_err(|e| anyhow!("report creation failed: {e}"))?;
        info!(
            "OpenADR VEN execution report posted: report_id={} event_id={} setpoint_kw={}",
            report.id().as_str(),
            event.id().as_str(),
            setpoint_kw
        );
        Ok(())
    }

    async fn mark_seen(&mut self, id: String, modified: DateTime<Utc>) {
        if let Some(store) = &mut self.redis {
            if let Err(e) = store.persist(&id, modified).await {
                warn!("OpenADR VEN dedup persist failed for {}: {}", id, e);
            }
        }
        self.seen.insert(id, modified);
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

/// Self-healing Redis store for the executed-event dedup map.
struct RedisSeenStore {
    url: String,
    conn: Option<ConnectionManager>,
}

impl RedisSeenStore {
    fn new(url: String) -> Self {
        Self { url, conn: None }
    }

    async fn conn(&mut self) -> Result<ConnectionManager> {
        if let Some(conn) = &self.conn {
            return Ok(conn.clone());
        }
        let client = redis::Client::open(self.url.as_str())?;
        let conn = ConnectionManager::new(client).await?;
        self.conn = Some(conn.clone());
        Ok(conn)
    }

    async fn load(&mut self) -> Result<HashMap<String, DateTime<Utc>>> {
        let mut conn = self.conn().await?;
        let raw: HashMap<String, String> = redis::AsyncCommands::hgetall(&mut conn, SEEN_KEY).await?;
        Ok(raw
            .into_iter()
            .filter_map(|(id, ts)| {
                DateTime::parse_from_rfc3339(&ts)
                    .ok()
                    .map(|t| (id, t.with_timezone(&Utc)))
            })
            .collect())
    }

    async fn persist(&mut self, id: &str, modified: DateTime<Utc>) -> Result<()> {
        let mut conn = self.conn().await?;
        let res: redis::RedisResult<()> =
            redis::AsyncCommands::hset(&mut conn, SEEN_KEY, id, modified.to_rfc3339()).await;
        if res.is_err() {
            // One rebuild + retry, mirroring the verifier/router pattern.
            self.conn = None;
            let mut conn = self.conn().await?;
            let _: () =
                redis::AsyncCommands::hset(&mut conn, SEEN_KEY, id, modified.to_rfc3339()).await?;
        }
        Ok(())
    }
}

/// First numeric DISPATCH_SETPOINT payload + the interval period governing it.
fn find_setpoint(event: &EventRequest) -> Option<(f64, Option<&IntervalPeriod>)> {
    event.intervals.as_ref()?.iter().find_map(|interval| {
        interval.payloads.iter().find_map(|payload| {
            if payload.value_type != EventType::DispatchSetpoint {
                return None;
            }
            payload.values.iter().find_map(|v| match v {
                Value::Number(n) => Some((*n, interval.interval_period.as_ref())),
                _ => None,
            })
        })
    })
}

/// Decide what to do with an event at time `now`, honoring its schedule.
/// The interval-level period wins over the event-level default; an event with
/// no period at all executes immediately (an "as soon as possible" dispatch).
fn decide(event: &EventRequest, now: DateTime<Utc>) -> EventDecision {
    let Some((setpoint_kw, interval_period)) = find_setpoint(event) else {
        return EventDecision::NotDispatch;
    };

    let Some(period) = interval_period.or(event.interval_period.as_ref()) else {
        return EventDecision::Execute { setpoint_kw };
    };

    if now < period.start {
        return EventDecision::NotYetActive;
    }
    let duration = period.duration.as_ref().or(event.duration.as_ref());
    if let Some(d) = duration {
        let end = period.start + d.to_chrono_at_datetime(period.start);
        if now >= end {
            return EventDecision::Expired;
        }
    }
    EventDecision::Execute { setpoint_kw }
}

/// End of the event's active window (for cancellation tracking). Unbounded
/// events get "now + 24h" — enough to notice a near-term cancellation without
/// tracking them forever.
fn active_window_end(event: &EventRequest, now: DateTime<Utc>) -> DateTime<Utc> {
    let period = find_setpoint(event)
        .and_then(|(_, p)| p)
        .or(event.interval_period.as_ref());
    match period {
        Some(p) => match p.duration.as_ref().or(event.duration.as_ref()) {
            Some(d) => p.start + d.to_chrono_at_datetime(p.start),
            None => now + chrono::Duration::hours(24),
        },
        None => now + chrono::Duration::hours(24),
    }
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
    use openleadr_wire::Duration as WireDuration;

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

    fn setpoint_event(start_offset_mins: i64, duration_hours: f32) -> EventRequest {
        let mut event =
            event_with_payload(EventType::DispatchSetpoint, vec![Value::Number(42.0)]);
        let mut period =
            IntervalPeriod::new(Utc::now() + chrono::Duration::minutes(start_offset_mins));
        period.duration = Some(WireDuration::hours(duration_hours));
        event.interval_period = Some(period);
        event
    }

    #[test]
    fn setpoint_sign_maps_to_action() {
        assert_eq!(setpoint_to_dispatch(75.0), (DispatchType::FLEX_UP, 75.0));
        assert_eq!(
            setpoint_to_dispatch(-30.5),
            (DispatchType::FLEX_DOWN, 30.5)
        );
        assert_eq!(setpoint_to_dispatch(0.0), (DispatchType::FLEX_UP, 0.0));
    }

    #[test]
    fn unscheduled_event_executes_immediately() {
        let event =
            event_with_payload(EventType::DispatchSetpoint, vec![Value::Number(42.0)]);
        assert_eq!(
            decide(&event, Utc::now()),
            EventDecision::Execute { setpoint_kw: 42.0 }
        );
    }

    #[test]
    fn active_window_executes() {
        // Started 10 min ago, lasts 1h.
        let event = setpoint_event(-10, 1.0);
        assert_eq!(
            decide(&event, Utc::now()),
            EventDecision::Execute { setpoint_kw: 42.0 }
        );
    }

    #[test]
    fn future_event_waits() {
        let event = setpoint_event(30, 1.0);
        assert_eq!(decide(&event, Utc::now()), EventDecision::NotYetActive);
    }

    #[test]
    fn expired_event_skipped() {
        // Started 3h ago, lasted 1h.
        let event = setpoint_event(-180, 1.0);
        assert_eq!(decide(&event, Utc::now()), EventDecision::Expired);
    }

    #[test]
    fn interval_period_wins_over_event_period() {
        // Event-level period says active; interval-level says future.
        let mut event = setpoint_event(-10, 1.0);
        let future = IntervalPeriod::new(Utc::now() + chrono::Duration::minutes(30));
        event.intervals.as_mut().unwrap()[0].interval_period = Some(future);
        assert_eq!(decide(&event, Utc::now()), EventDecision::NotYetActive);
    }

    #[test]
    fn non_setpoint_event_is_not_dispatch() {
        let event = event_with_payload(EventType::Price, vec![Value::Number(1.0)]);
        assert_eq!(decide(&event, Utc::now()), EventDecision::NotDispatch);
        let text = event_with_payload(
            EventType::DispatchSetpoint,
            vec![Value::String("oops".to_string())],
        );
        assert_eq!(decide(&text, Utc::now()), EventDecision::NotDispatch);
    }

    #[test]
    fn invalid_config_rejected() {
        assert!(OpenLeadrVenListener::new(
            "not a url",
            None,
            None,
            None,
            30,
            Arc::new(NoopAdapter),
            None,
        )
        .is_err());
        assert!(OpenLeadrVenListener::new(
            "http://localhost:4031",
            None,
            Some("has spaces".to_string()),
            None,
            30,
            Arc::new(NoopAdapter),
            None,
        )
        .is_err());
        assert!(OpenLeadrVenListener::new(
            "http://localhost:4031",
            None,
            None,
            Some("bad target!".to_string()),
            30,
            Arc::new(NoopAdapter),
            None,
        )
        .is_err());
    }
}
