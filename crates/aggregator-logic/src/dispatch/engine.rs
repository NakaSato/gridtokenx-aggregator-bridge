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
    adapter_name: String,
    freq_low_hz: f64,
    freq_high_hz: f64,
    capacity_kw: f64,
    cooldown: std::time::Duration,
    // Last successful dispatch per action. Per-action (not just "the last
    // action") so an oscillating frequency cannot defeat the cooldown by
    // flip-flopping between FLEX_UP and FLEX_DOWN.
    last_dispatch: Vec<(DispatchType, std::time::Instant)>,
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

        // DISPATCH_ADAPTER picks the adapter explicitly; default prefers
        // openleadr when configured, falling back to ieee.
        let adapter_name = match std::env::var("DISPATCH_ADAPTER") {
            Ok(name) if adapters.contains_key(&name) => name,
            Ok(name) => {
                warn!(
                    "DISPATCH_ADAPTER={} unknown (available: {:?}); using default",
                    name,
                    adapters.keys().collect::<Vec<_>>()
                );
                Self::default_adapter_name(&adapters)
            }
            Err(_) => Self::default_adapter_name(&adapters),
        };
        info!("Dispatch adapter: {}", adapter_name);

        Self {
            aggregator,
            adapters,
            adapter_name,
            freq_low_hz: env_f64("DISPATCH_FREQ_LOW_HZ", 49.8),
            freq_high_hz: env_f64("DISPATCH_FREQ_HIGH_HZ", 50.2),
            capacity_kw: env_f64("DISPATCH_CAPACITY_KW", 100.0),
            // Default one settlement window: a sustained excursion produces one
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

    fn last_dispatch_of(&self, action: DispatchType) -> Option<std::time::Instant> {
        self.last_dispatch
            .iter()
            .find(|(a, _)| *a == action)
            .map(|(_, at)| *at)
    }

    fn record_dispatch(&mut self, action: DispatchType) {
        self.last_dispatch.retain(|(a, _)| *a != action);
        self.last_dispatch.push((action, std::time::Instant::now()));
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

    /// Evaluates grid status and decides whether to dispatch flex commands
    pub async fn evaluate_and_dispatch(&mut self, frequency: f64) -> Result<()> {
        info!("Evaluating grid frequency: {} Hz", frequency);

        let adapter = self
            .adapters
            .get(&self.adapter_name)
            .ok_or_else(|| anyhow!("dispatch adapter {} not registered", self.adapter_name))?
            .clone();

        let action = if frequency < self.freq_low_hz {
            Some(DispatchType::FLEX_UP)
        } else if frequency > self.freq_high_hz {
            Some(DispatchType::FLEX_DOWN)
        } else {
            info!("Frequency stable.");
            None
        };

        if let Some(action) = action {
            if !cooldown_allows(self.last_dispatch_of(action), self.cooldown) {
                debug!(
                    "Dispatch of {:?} suppressed (cooldown {:?} active)",
                    action, self.cooldown
                );
                return Ok(());
            }
            match action {
                DispatchType::FLEX_UP => warn!("Frequency low! Dispatching FLEX_UP command."),
                DispatchType::FLEX_DOWN => info!("Frequency high! Dispatching FLEX_DOWN command."),
            }
            self.dispatch_action(adapter, action, self.capacity_kw)
                .await?;
            // Record only on success: a failed dispatch must retry on the next
            // grid-status message, not silently sit out the cooldown.
            self.record_dispatch(action);
        }

        Ok(())
    }

    async fn dispatch_action(
        &mut self,
        adapter: Arc<dyn DispatchAdapter>,
        action: DispatchType,
        capacity_kw: f64,
    ) -> Result<()> {
        // Query aggregator state (read-only: dispatch must NOT drain settlement's bins).
        let aggregator = self.aggregator.lock().await;
        let bins = aggregator.peek_completed_bins();

        // Calculate total capacity
        let total_capacity: rust_decimal::Decimal = bins.iter().map(|b| b.energy_generated).sum();

        if total_capacity <= rust_decimal::Decimal::ZERO {
            return Err(anyhow!("No available capacity for dispatch"));
        }

        // Execute dispatch via trait
        adapter.execute_dispatch(action, capacity_kw).await
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
            .map(|n| (n.to_string(), Arc::new(NoopAdapter) as Arc<dyn DispatchAdapter>))
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
        assert!(cooldown_allows(lookup(&last, DispatchType::FLEX_DOWN), cooldown));

        // FLEX_DOWN fires too; now flipping BACK to FLEX_UP is suppressed —
        // its own timer is still hot.
        last.push((DispatchType::FLEX_DOWN, std::time::Instant::now()));
        assert!(!cooldown_allows(lookup(&last, DispatchType::FLEX_UP), cooldown));
        assert!(!cooldown_allows(lookup(&last, DispatchType::FLEX_DOWN), cooldown));
    }
}
