use std::collections::{HashMap, HashSet};
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

/// What to do with one polled event, given "now" and the set of interval ids
/// already executed for the current event version.
#[derive(Debug, PartialEq)]
enum EventDecision {
    /// An active, not-yet-executed dispatch interval — execute this setpoint.
    /// `more_pending` means other intervals (future, or also active) remain,
    /// so the event must stay live for the next poll instead of being marked
    /// fully seen.
    Execute {
        setpoint_kw: f64,
        interval_id: i32,
        more_pending: bool,
    },
    /// Dispatch event with no active interval yet — re-check next poll.
    NotYetActive,
    /// No pending interval remains (all expired or already executed).
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
        let mut present: HashSet<String> = HashSet::new();
        let mut dispatched = 0;

        for event in events {
            let id = event.id().as_str().to_string();
            present.insert(id.clone());
            let modified = event.modification_date_time();
            if self.seen.get(&id).is_some_and(|prev| *prev >= modified) {
                continue;
            }

            // Interval ids already executed for THIS version of the event
            // (per-interval dedup keys: "{event_id}#{interval_id}").
            let interval_prefix = format!("{id}#");
            let executed: HashSet<i32> = self
                .seen
                .iter()
                .filter_map(|(key, ts)| {
                    let suffix = key.strip_prefix(&interval_prefix)?;
                    (*ts >= modified)
                        .then(|| suffix.parse::<i32>().ok())
                        .flatten()
                })
                .collect();

            match decide(event.content(), now, &executed) {
                EventDecision::NotDispatch => {
                    self.mark_seen(id, modified).await;
                }
                EventDecision::Expired => {
                    debug!("OpenADR VEN event {} has no pending interval", id);
                    self.mark_seen(id, modified).await;
                }
                EventDecision::NotYetActive => {
                    // Deliberately NOT marked seen: re-evaluated every poll
                    // until its window opens.
                    debug!("OpenADR VEN event {} not yet active", id);
                }
                EventDecision::Execute {
                    setpoint_kw,
                    interval_id,
                    more_pending,
                } => {
                    let (action, capacity_kw) = setpoint_to_dispatch(setpoint_kw);
                    match self.adapter.execute_dispatch(action, capacity_kw).await {
                        Ok(()) => {
                            info!(
                                "OpenADR VEN event executed: event_id={} interval_id={} setpoint_kw={} action={:?} capacity_kw={}",
                                id, interval_id, setpoint_kw, action, capacity_kw
                            );
                            self.executed_active
                                .insert(id.clone(), active_window_end(event.content(), now));
                            if more_pending {
                                // Other intervals still pending: dedup only
                                // this interval, keep the event live.
                                self.mark_seen(format!("{id}#{interval_id}"), modified)
                                    .await;
                            } else {
                                self.mark_seen(id.clone(), modified).await;
                            }
                            dispatched += 1;
                            crate::metrics::record_ven_event("executed");
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
                                    crate::metrics::record_ven_event("report_failed");
                                }
                            }
                        }
                        Err(e) => {
                            // Leave it out of `seen` so the next cycle retries.
                            warn!("OpenADR VEN dispatch failed for event {}: {}", id, e);
                            crate::metrics::record_ven_event("dispatch_failed");
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
            if present.contains(id) {
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

        // Bound the dedup map: drop entries for events the VTN no longer lists
        // AND whose recorded modification time is old. The age guard protects
        // against a paginated/filtered listing that hides an event the VTN
        // still has — pruning it while fresh could re-execute it.
        let prune_cutoff = now - chrono::Duration::days(7);
        let stale: Vec<String> = self
            .seen
            .iter()
            .filter(|(key, modified)| {
                let base = key.split('#').next().unwrap_or(key);
                !present.contains(base) && **modified < prune_cutoff
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in stale {
            self.seen.remove(&key);
            if let Some(store) = &mut self.redis {
                if let Err(e) = store.remove(&key).await {
                    warn!("OpenADR VEN dedup prune failed for {}: {}", key, e);
                }
            }
        }

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

    async fn remove(&mut self, id: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let res: redis::RedisResult<()> = redis::AsyncCommands::hdel(&mut conn, SEEN_KEY, id).await;
        if res.is_err() {
            // One rebuild + retry, mirroring the verifier/router pattern.
            self.conn = None;
            let mut conn = self.conn().await?;
            let _: () = redis::AsyncCommands::hdel(&mut conn, SEEN_KEY, id).await?;
        }
        Ok(())
    }
}

/// Every numeric DISPATCH_SETPOINT payload in the event: (interval id,
/// setpoint, interval-level period). One entry per interval (first numeric
/// setpoint payload of each).
fn find_setpoints(event: &EventRequest) -> Vec<(i32, f64, Option<&IntervalPeriod>)> {
    event
        .intervals
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|interval| {
            interval.payloads.iter().find_map(|payload| {
                if payload.value_type != EventType::DispatchSetpoint {
                    return None;
                }
                payload.values.iter().find_map(|v| match v {
                    Value::Number(n) => {
                        Some((interval.id, *n, interval.interval_period.as_ref()))
                    }
                    _ => None,
                })
            })
        })
        .collect()
}

/// Decide what to do with an event at time `now`, honoring its schedule
/// across ALL setpoint intervals (not only the first — a multi-interval
/// schedule executes each interval as its window opens). `executed` holds the
/// interval ids already dispatched for this event version.
/// Per interval: the interval-level period wins over the event-level default;
/// an interval with no period at all executes immediately ("as soon as
/// possible") unless already executed.
fn decide(event: &EventRequest, now: DateTime<Utc>, executed: &HashSet<i32>) -> EventDecision {
    let setpoints = find_setpoints(event);
    if setpoints.is_empty() {
        return EventDecision::NotDispatch;
    }

    let mut any_future = false;
    let mut active_unexecuted: Vec<(i32, f64)> = Vec::new();
    for (interval_id, setpoint_kw, interval_period) in setpoints {
        let active = match interval_period.or(event.interval_period.as_ref()) {
            // No schedule: an "as soon as possible" dispatch, always active.
            None => true,
            Some(period) if now < period.start => {
                any_future = true;
                false
            }
            Some(period) => {
                let duration = period.duration.as_ref().or(event.duration.as_ref());
                match duration {
                    Some(d) => now < period.start + d.to_chrono_at_datetime(period.start),
                    None => true, // open-ended interval
                }
            }
        };
        if active && !executed.contains(&interval_id) {
            active_unexecuted.push((interval_id, setpoint_kw));
        }
    }

    if let Some(&(interval_id, setpoint_kw)) = active_unexecuted.first() {
        return EventDecision::Execute {
            setpoint_kw,
            interval_id,
            more_pending: any_future || active_unexecuted.len() > 1,
        };
    }
    if any_future {
        EventDecision::NotYetActive
    } else {
        EventDecision::Expired
    }
}

/// End of the event's active window (for cancellation tracking): the latest
/// end across all setpoint intervals. Unbounded/period-less intervals count
/// as "now + 24h" — enough to notice a near-term cancellation without
/// tracking them forever.
fn active_window_end(event: &EventRequest, now: DateTime<Utc>) -> DateTime<Utc> {
    let fallback = now + chrono::Duration::hours(24);
    find_setpoints(event)
        .iter()
        .map(|(_, _, interval_period)| {
            match interval_period.or(event.interval_period.as_ref()) {
                Some(p) => match p.duration.as_ref().or(event.duration.as_ref()) {
                    Some(d) => p.start + d.to_chrono_at_datetime(p.start),
                    None => fallback,
                },
                None => fallback,
            }
        })
        .max()
        .unwrap_or(fallback)
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

    fn no_executed() -> HashSet<i32> {
        HashSet::new()
    }

    #[test]
    fn unscheduled_event_executes_immediately() {
        let event =
            event_with_payload(EventType::DispatchSetpoint, vec![Value::Number(42.0)]);
        assert_eq!(
            decide(&event, Utc::now(), &no_executed()),
            EventDecision::Execute {
                setpoint_kw: 42.0,
                interval_id: 0,
                more_pending: false
            }
        );
    }

    #[test]
    fn active_window_executes() {
        // Started 10 min ago, lasts 1h.
        let event = setpoint_event(-10, 1.0);
        assert_eq!(
            decide(&event, Utc::now(), &no_executed()),
            EventDecision::Execute {
                setpoint_kw: 42.0,
                interval_id: 0,
                more_pending: false
            }
        );
    }

    #[test]
    fn future_event_waits() {
        let event = setpoint_event(30, 1.0);
        assert_eq!(
            decide(&event, Utc::now(), &no_executed()),
            EventDecision::NotYetActive
        );
    }

    #[test]
    fn expired_event_skipped() {
        // Started 3h ago, lasted 1h.
        let event = setpoint_event(-180, 1.0);
        assert_eq!(
            decide(&event, Utc::now(), &no_executed()),
            EventDecision::Expired
        );
    }

    #[test]
    fn interval_period_wins_over_event_period() {
        // Event-level period says active; interval-level says future.
        let mut event = setpoint_event(-10, 1.0);
        let future = IntervalPeriod::new(Utc::now() + chrono::Duration::minutes(30));
        event.intervals.as_mut().unwrap()[0].interval_period = Some(future);
        assert_eq!(
            decide(&event, Utc::now(), &no_executed()),
            EventDecision::NotYetActive
        );
    }

    #[test]
    fn non_setpoint_event_is_not_dispatch() {
        let event = event_with_payload(EventType::Price, vec![Value::Number(1.0)]);
        assert_eq!(
            decide(&event, Utc::now(), &no_executed()),
            EventDecision::NotDispatch
        );
        let text = event_with_payload(
            EventType::DispatchSetpoint,
            vec![Value::String("oops".to_string())],
        );
        assert_eq!(
            decide(&text, Utc::now(), &no_executed()),
            EventDecision::NotDispatch
        );
    }

    /// Two intervals: one active now, one starting later. The active one
    /// executes with more_pending=true; once executed, the event waits for
    /// the future interval instead of being marked expired; when that opens
    /// it executes as the last pending interval (more_pending=false).
    #[test]
    fn multi_interval_schedule_executes_each_window() {
        fn interval(id: i32, setpoint: f64, start_offset_mins: i64) -> EventInterval {
            let mut period =
                IntervalPeriod::new(Utc::now() + chrono::Duration::minutes(start_offset_mins));
            period.duration = Some(WireDuration::hours(1.0));
            EventInterval {
                id,
                interval_period: Some(period),
                payloads: vec![EventValuesMap {
                    value_type: EventType::DispatchSetpoint,
                    values: vec![Value::Number(setpoint)],
                }],
            }
        }
        let mut event = EventRequest::new(ProgramId::new("test-program").unwrap());
        event.intervals = Some(vec![interval(0, 10.0, -10), interval(1, -20.0, 120)]);

        let now = Utc::now();
        // First poll: interval 0 active, interval 1 future.
        assert_eq!(
            decide(&event, now, &no_executed()),
            EventDecision::Execute {
                setpoint_kw: 10.0,
                interval_id: 0,
                more_pending: true
            }
        );
        // Interval 0 executed: hold for interval 1, do NOT expire the event.
        let executed: HashSet<i32> = [0].into();
        assert_eq!(decide(&event, now, &executed), EventDecision::NotYetActive);
        // Interval 1's window opens (starts +120min, lasts 1h — jump to +150min).
        let later = now + chrono::Duration::minutes(150);
        assert_eq!(
            decide(&event, later, &executed),
            EventDecision::Execute {
                setpoint_kw: -20.0,
                interval_id: 1,
                more_pending: false
            }
        );
        // Both executed, nothing future: done.
        let executed: HashSet<i32> = [0, 1].into();
        assert_eq!(decide(&event, later, &executed), EventDecision::Expired);
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
