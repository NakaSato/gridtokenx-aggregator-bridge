use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::aggregator::Aggregator;
use crate::dispatch::grpc_client::{DispatchClient, DispatchType};
use crate::dispatch::DispatchAdapter;
use crate::standards::ieee2030_5::Ieee2030_5Adapter;
use crate::standards::openleadr::OpenLeadrAdapter;

pub struct DispatchEngine {
    aggregator: Arc<Mutex<Aggregator>>,
    adapters: std::collections::HashMap<String, Arc<dyn DispatchAdapter>>,
    // Fan-out targets. One freq excursion dispatches to every adapter in this
    // list (e.g. a downstream in-mesh VTN AND a utility VTN at once). A single
    // configured adapter is just a one-element list — the common case.
    active_adapters: Vec<String>,
    freq_low_hz: f64,
    freq_high_hz: f64,
    capacity_kw: f64,
    cooldown: std::time::Duration,
    // Last successful dispatch per (action, adapter). Keyed on the adapter too,
    // not just the action: with fan-out, one adapter firing FLEX_UP must NOT
    // suppress another adapter's FLEX_UP. Per-action-only would let one target's
    // dispatch sit out every other target's cooldown. Oscillation protection is
    // unchanged — each (action, adapter) pair still holds its own timer.
    last_dispatch: Vec<(DispatchType, String, std::time::Instant)>,
}

fn env_f64(var: &str, default: f64) -> f64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

/// Repeat-suppression policy: a dispatch goes through when this action has
/// never fired, or when its own cooldown has elapsed. A flipped action still
/// reacts immediately (its timer is independent), but — unlike tracking only
/// the most recent action — an oscillating frequency cannot reset the timer
/// by alternating FLEX_UP/FLEX_DOWN: each direction holds its own cooldown.
/// Without this, a sustained or oscillating excursion re-fires on every
/// grid-status message and floods the adapter (e.g. one OpenADR event per
/// publish interval).
fn cooldown_allows(
    last_same_action: Option<std::time::Instant>,
    cooldown: std::time::Duration,
) -> bool {
    match last_same_action {
        Some(at) => at.elapsed() >= cooldown,
        None => true,
    }
}

impl DispatchEngine {
    pub fn new(aggregator: Arc<Mutex<Aggregator>>, grpc_client: DispatchClient) -> Self {
        let mut adapters = std::collections::HashMap::new();
        adapters.insert(
            "grpc".to_string(),
            Arc::new(grpc_client) as Arc<dyn DispatchAdapter>,
        );
        adapters.insert(
            "ieee".to_string(),
            Arc::new(Ieee2030_5Adapter::new()) as Arc<dyn DispatchAdapter>,
        );
        match OpenLeadrAdapter::from_env() {
            Ok(Some(adapter)) => {
                adapters.insert(
                    "openleadr".to_string(),
                    Arc::new(adapter) as Arc<dyn DispatchAdapter>,
                );
            }
            Ok(None) => {}
            Err(e) => {
                warn!("OpenADR adapter disabled: {}", e);
            }
        }

        // Adapter selection precedence:
        //   1. DISPATCH_ADAPTERS (csv) — fan-out to several targets at once.
        //   2. DISPATCH_ADAPTER (single) — back-compat for existing deploys.
        //   3. default — openleadr when configured, else ieee.
        // Unknown names are dropped loud; if nothing valid remains we fall back
        // to the default (always present: `ieee`/`grpc` are unconditionally
        // registered above), so `active_adapters` is never empty and `new`
        // stays infallible — no main.rs signature change.
        let active_adapters = Self::select_adapters(&adapters);
        info!("Dispatch adapters: {:?}", active_adapters);

        Self {
            aggregator,
            adapters,
            active_adapters,
            freq_low_hz: env_f64("DISPATCH_FREQ_LOW_HZ", 49.8),
            freq_high_hz: env_f64("DISPATCH_FREQ_HIGH_HZ", 50.2),
            capacity_kw: env_f64("DISPATCH_CAPACITY_KW", 100.0),
            // Default one 15-minute window: a sustained excursion produces one
            // dispatch per 15 minutes, not one per grid-status message.
            cooldown: std::time::Duration::from_secs(
                std::env::var("DISPATCH_COOLDOWN_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(900),
            ),
            last_dispatch: Vec::new(),
        }
    }

    /// Resolve the configured fan-out adapter list from the environment.
    /// Pure over `adapters` + env so it is unit-testable. Never returns empty:
    /// an all-invalid or absent config falls back to the single default.
    fn select_adapters(
        adapters: &std::collections::HashMap<String, Arc<dyn DispatchAdapter>>,
    ) -> Vec<String> {
        let raw = match std::env::var("DISPATCH_ADAPTERS") {
            Ok(csv) => csv,
            // Fall back to the legacy single-adapter var.
            Err(_) => std::env::var("DISPATCH_ADAPTER").unwrap_or_default(),
        };
        Self::select_adapters_from(&raw, adapters)
    }

    /// Pure core of [`select_adapters`] — takes the raw config string so it is
    /// unit-testable without mutating process-global env vars.
    fn select_adapters_from(
        raw: &str,
        adapters: &std::collections::HashMap<String, Arc<dyn DispatchAdapter>>,
    ) -> Vec<String> {
        let mut selected: Vec<String> = Vec::new();
        for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if !adapters.contains_key(name) {
                warn!(
                    "dispatch adapter {} unknown (available: {:?}); skipping",
                    name,
                    adapters.keys().collect::<Vec<_>>()
                );
                continue;
            }
            if !selected.iter().any(|s| s == name) {
                selected.push(name.to_string());
            }
        }

        if selected.is_empty() {
            // No valid config — use the default. Warn only if the user actually
            // set something that resolved to nothing (a bare default is normal).
            if !raw.trim().is_empty() {
                warn!(
                    "no valid dispatch adapter in {:?}; using default",
                    raw
                );
            }
            selected.push(Self::default_adapter_name(adapters));
        }
        selected
    }

    fn last_dispatch_of(
        &self,
        action: DispatchType,
        adapter: &str,
    ) -> Option<std::time::Instant> {
        self.last_dispatch
            .iter()
            .find(|(a, name, _)| *a == action && name == adapter)
            .map(|(_, _, at)| *at)
    }

    fn record_dispatch(&mut self, action: DispatchType, adapter: &str) {
        self.last_dispatch
            .retain(|(a, name, _)| !(*a == action && name == adapter));
        self.last_dispatch
            .push((action, adapter.to_string(), std::time::Instant::now()));
    }

    fn default_adapter_name(
        adapters: &std::collections::HashMap<String, Arc<dyn DispatchAdapter>>,
    ) -> String {
        if adapters.contains_key("openleadr") {
            "openleadr".to_string()
        } else {
            "ieee".to_string()
        }
    }

    /// Evaluates grid status and decides whether to dispatch flex commands.
    /// Fans the decided action out to every configured adapter. Partial-failure
    /// policy: one adapter erroring (e.g. a downstream VTN outage) must NOT stop
    /// the others — we return `Ok` if at least one adapter fired, and `Err` only
    /// when every active adapter failed.
    pub async fn evaluate_and_dispatch(&mut self, frequency: f64) -> Result<()> {
        info!("Evaluating grid frequency: {} Hz", frequency);

        let action = if frequency < self.freq_low_hz {
            Some(DispatchType::FLEX_UP)
        } else if frequency > self.freq_high_hz {
            Some(DispatchType::FLEX_DOWN)
        } else {
            info!("Frequency stable.");
            None
        };

        let Some(action) = action else {
            return Ok(());
        };
        let action_label = format!("{:?}", action);

        match action {
            DispatchType::FLEX_UP => warn!("Frequency low! Dispatching FLEX_UP command."),
            DispatchType::FLEX_DOWN => info!("Frequency high! Dispatching FLEX_DOWN command."),
        }

        // Capacity is a property of the fleet, not the adapter — check it once
        // for the whole fan-out. Zero capacity skips ALL adapters (no target
        // can be served), not one.
        if !self.has_dispatch_capacity().await {
            return Err(anyhow!("No available capacity for dispatch"));
        }

        // Snapshot the target list so the dispatch loop can borrow `&mut self`
        // (record_dispatch) without aliasing `self.active_adapters`.
        let targets = self.active_adapters.clone();
        let mut fired = 0usize;
        let mut errors: Vec<String> = Vec::new();

        for name in &targets {
            let Some(adapter) = self.adapters.get(name).cloned() else {
                // Shouldn't happen — names are validated at construction.
                warn!("dispatch adapter {} vanished from registry; skipping", name);
                continue;
            };

            if !cooldown_allows(self.last_dispatch_of(action, name), self.cooldown) {
                debug!(
                    "Dispatch of {:?} to {} suppressed (cooldown {:?} active)",
                    action, name, self.cooldown
                );
                crate::metrics::record_dispatch_outcome(&action_label, name, "suppressed");
                continue;
            }

            match adapter.execute_dispatch(action, self.capacity_kw).await {
                Ok(()) => {
                    crate::metrics::record_dispatch_outcome(&action_label, name, "fired");
                    // Record only on success: a failed dispatch must retry on
                    // the next grid-status message, not sit out the cooldown.
                    self.record_dispatch(action, name);
                    fired += 1;
                }
                Err(e) => {
                    crate::metrics::record_dispatch_outcome(&action_label, name, "failed");
                    warn!("Dispatch of {:?} to {} failed: {}", action, name, e);
                    errors.push(format!("{name}: {e}"));
                }
            }
        }

        // Ok if anything fired (or everything was cooldown-suppressed — not an
        // error). Err only when every attempted adapter errored.
        if !errors.is_empty() && fired == 0 {
            return Err(anyhow!(
                "all dispatch adapters failed: {}",
                errors.join("; ")
            ));
        }
        Ok(())
    }

    /// True when at least one completed aggregation window carries generation
    /// capacity. Read-only; zero grace so every window whose end_time has passed
    /// counts without waiting for late readings.
    async fn has_dispatch_capacity(&self) -> bool {
        let aggregator = self.aggregator.lock().await;
        let bins = aggregator.peek_completed_bins(chrono::Duration::zero());
        let total: rust_decimal::Decimal = bins.iter().map(|b| b.energy_generated).sum();
        total > rust_decimal::Decimal::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct NoopAdapter;

    #[async_trait]
    impl DispatchAdapter for NoopAdapter {
        async fn execute_dispatch(&self, _action: DispatchType, _capacity_kw: f64) -> Result<()> {
            Ok(())
        }
    }

    fn adapters(names: &[&str]) -> std::collections::HashMap<String, Arc<dyn DispatchAdapter>> {
        names
            .iter()
            .map(|n| {
                (
                    n.to_string(),
                    Arc::new(NoopAdapter) as Arc<dyn DispatchAdapter>,
                )
            })
            .collect()
    }

    #[test]
    fn default_adapter_prefers_openleadr() {
        assert_eq!(
            DispatchEngine::default_adapter_name(&adapters(&["grpc", "ieee", "openleadr"])),
            "openleadr"
        );
    }

    #[test]
    fn default_adapter_falls_back_to_ieee() {
        assert_eq!(
            DispatchEngine::default_adapter_name(&adapters(&["grpc", "ieee"])),
            "ieee"
        );
    }

    #[test]
    fn env_f64_falls_back_on_missing_or_garbage() {
        assert_eq!(env_f64("DISPATCH_TEST_UNSET_VAR", 49.8), 49.8);
    }

    #[test]
    fn cooldown_allows_first_dispatch() {
        assert!(cooldown_allows(None, std::time::Duration::from_secs(900)));
    }

    #[test]
    fn cooldown_suppresses_repeat_action() {
        assert!(!cooldown_allows(
            Some(std::time::Instant::now()),
            std::time::Duration::from_secs(900)
        ));
    }

    #[test]
    fn cooldown_expires() {
        assert!(cooldown_allows(
            Some(std::time::Instant::now()),
            std::time::Duration::from_secs(0)
        ));
    }

    // Per-action tracking: a flipped action fires immediately (independent
    // timer), but oscillation cannot reset a direction's own cooldown.
    #[test]
    fn per_action_timers_survive_oscillation() {
        let mut last: Vec<(DispatchType, std::time::Instant)> = Vec::new();
        let lookup = |last: &Vec<(DispatchType, std::time::Instant)>, action: DispatchType| {
            last.iter().find(|(a, _)| *a == action).map(|(_, at)| *at)
        };
        let cooldown = std::time::Duration::from_secs(900);

        // FLEX_UP fires, then frequency flips: FLEX_DOWN still allowed.
        last.push((DispatchType::FLEX_UP, std::time::Instant::now()));
        assert!(cooldown_allows(
            lookup(&last, DispatchType::FLEX_DOWN),
            cooldown
        ));

        // FLEX_DOWN fires too; now flipping BACK to FLEX_UP is suppressed —
        // its own timer is still hot.
        last.push((DispatchType::FLEX_DOWN, std::time::Instant::now()));
        assert!(!cooldown_allows(
            lookup(&last, DispatchType::FLEX_UP),
            cooldown
        ));
        assert!(!cooldown_allows(
            lookup(&last, DispatchType::FLEX_DOWN),
            cooldown
        ));
    }

    // ---- fan-out (DISPATCH_ADAPTERS) -------------------------------------

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts calls; optionally fails to test partial-failure isolation.
    struct CountingAdapter {
        hits: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl DispatchAdapter for CountingAdapter {
        async fn execute_dispatch(&self, _action: DispatchType, _capacity_kw: f64) -> Result<()> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(anyhow!("simulated adapter failure"))
            } else {
                Ok(())
            }
        }
    }

    /// Build an engine directly (bypassing DispatchClient) with the given
    /// adapters + active list. `seeded` controls whether the aggregator has a
    /// completed bin with capacity.
    fn engine(
        adapters: Vec<(&str, Arc<dyn DispatchAdapter>)>,
        active: &[&str],
        seeded: bool,
    ) -> DispatchEngine {
        let mut agg = Aggregator::new();
        if seeded {
            let ts = gridtokenx_telemetry::time::now() - chrono::Duration::minutes(20);
            agg.handle_reading(
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4(),
                "test-meter".to_string(),
                rust_decimal::Decimal::from(10),
                rust_decimal::Decimal::ZERO,
                ts,
                None,
                None,
            );
        }
        DispatchEngine {
            aggregator: Arc::new(Mutex::new(agg)),
            adapters: adapters.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            active_adapters: active.iter().map(|s| s.to_string()).collect(),
            freq_low_hz: 49.8,
            freq_high_hz: 50.2,
            capacity_kw: 100.0,
            cooldown: std::time::Duration::from_secs(900),
            last_dispatch: Vec::new(),
        }
    }

    #[test]
    fn select_parses_csv_dedupes_and_drops_unknown() {
        let avail = adapters(&["grpc", "ieee", "openleadr"]);
        assert_eq!(
            DispatchEngine::select_adapters_from("openleadr, grpc", &avail),
            vec!["openleadr".to_string(), "grpc".to_string()]
        );
        // dedup + whitespace + unknown skipped
        assert_eq!(
            DispatchEngine::select_adapters_from("grpc, grpc , nope", &avail),
            vec!["grpc".to_string()]
        );
    }

    #[test]
    fn select_single_value_back_compat() {
        let avail = adapters(&["grpc", "ieee"]);
        assert_eq!(
            DispatchEngine::select_adapters_from("ieee", &avail),
            vec!["ieee".to_string()]
        );
    }

    #[test]
    fn select_empty_or_all_invalid_falls_back_to_default() {
        let avail = adapters(&["grpc", "ieee", "openleadr"]);
        assert_eq!(
            DispatchEngine::select_adapters_from("", &avail),
            vec!["openleadr".to_string()]
        );
        assert_eq!(
            DispatchEngine::select_adapters_from("nope, alsobad", &avail),
            vec!["openleadr".to_string()]
        );
    }

    #[tokio::test]
    async fn fan_out_fires_all_active_adapters() {
        let a = Arc::new(AtomicUsize::new(0));
        let b = Arc::new(AtomicUsize::new(0));
        let mut eng = engine(
            vec![
                ("a", Arc::new(CountingAdapter { hits: a.clone(), fail: false })),
                ("b", Arc::new(CountingAdapter { hits: b.clone(), fail: false })),
            ],
            &["a", "b"],
            true,
        );
        // 49.0 < freq_low → FLEX_UP
        eng.evaluate_and_dispatch(49.0).await.unwrap();
        assert_eq!(a.load(Ordering::SeqCst), 1, "adapter a fired");
        assert_eq!(b.load(Ordering::SeqCst), 1, "adapter b fired");
    }

    #[tokio::test]
    async fn partial_failure_does_not_stop_other_adapters() {
        let ok = Arc::new(AtomicUsize::new(0));
        let bad = Arc::new(AtomicUsize::new(0));
        let mut eng = engine(
            vec![
                ("bad", Arc::new(CountingAdapter { hits: bad.clone(), fail: true })),
                ("ok", Arc::new(CountingAdapter { hits: ok.clone(), fail: false })),
            ],
            &["bad", "ok"],
            true,
        );
        // One adapter errored, one fired → overall Ok.
        eng.evaluate_and_dispatch(49.0).await.unwrap();
        assert_eq!(bad.load(Ordering::SeqCst), 1, "bad adapter attempted");
        assert_eq!(ok.load(Ordering::SeqCst), 1, "ok adapter still fired");
    }

    #[tokio::test]
    async fn all_adapters_failing_returns_err() {
        let bad = Arc::new(AtomicUsize::new(0));
        let mut eng = engine(
            vec![("bad", Arc::new(CountingAdapter { hits: bad.clone(), fail: true }))],
            &["bad"],
            true,
        );
        assert!(eng.evaluate_and_dispatch(49.0).await.is_err());
    }

    #[tokio::test]
    async fn zero_capacity_skips_all_adapters() {
        let a = Arc::new(AtomicUsize::new(0));
        let mut eng = engine(
            vec![("a", Arc::new(CountingAdapter { hits: a.clone(), fail: false }))],
            &["a"],
            false, // no completed bin → no capacity
        );
        assert!(eng.evaluate_and_dispatch(49.0).await.is_err());
        assert_eq!(a.load(Ordering::SeqCst), 0, "no adapter called without capacity");
    }

    #[tokio::test]
    async fn stable_frequency_dispatches_nothing() {
        let a = Arc::new(AtomicUsize::new(0));
        let mut eng = engine(
            vec![("a", Arc::new(CountingAdapter { hits: a.clone(), fail: false }))],
            &["a"],
            true,
        );
        eng.evaluate_and_dispatch(50.0).await.unwrap(); // within band
        assert_eq!(a.load(Ordering::SeqCst), 0);
    }

    // Per-(action, adapter) cooldown: adapter "a" firing FLEX_UP must NOT
    // suppress adapter "b" FLEX_UP — each target holds its own timer.
    #[test]
    fn cooldown_is_keyed_per_action_and_adapter() {
        let mut eng = engine(vec![], &[], false);
        eng.record_dispatch(DispatchType::FLEX_UP, "a");
        // Same action, different adapter: still allowed.
        assert!(cooldown_allows(
            eng.last_dispatch_of(DispatchType::FLEX_UP, "b"),
            eng.cooldown
        ));
        // Same action AND adapter: suppressed.
        assert!(!cooldown_allows(
            eng.last_dispatch_of(DispatchType::FLEX_UP, "a"),
            eng.cooldown
        ));
        // Flipped action on the same adapter: independent timer, allowed.
        assert!(cooldown_allows(
            eng.last_dispatch_of(DispatchType::FLEX_DOWN, "a"),
            eng.cooldown
        ));
    }
}
