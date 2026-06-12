//! Grid-frequency aggregation from ingested telemetry.
//!
//! Meter readings carry an instantaneous grid frequency; this module folds
//! those samples into a rolling-window mean that a periodic publisher task
//! turns into `GridStatusEvent`s on Kafka — the dispatch engine's trigger.
//! This closes the loop without an external SCADA feed: the fleet itself is
//! the frequency sensor.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Plausible grid frequency band (Hz). Samples outside are sensor garbage
/// (e.g. a meter reporting 0 during boot) and must not drag the mean.
const MIN_PLAUSIBLE_HZ: f64 = 40.0;
const MAX_PLAUSIBLE_HZ: f64 = 70.0;

/// Thread-safe rolling window of grid-frequency samples.
pub struct FrequencyMonitor {
    samples: Mutex<VecDeque<(Instant, f64)>>,
    window: Duration,
}

impl FrequencyMonitor {
    pub fn new(window: Duration) -> Self {
        Self {
            samples: Mutex::new(VecDeque::new()),
            window,
        }
    }

    /// Record one frequency sample (Hz). Implausible values are dropped.
    pub fn record(&self, hz: f64) {
        if !hz.is_finite() || !(MIN_PLAUSIBLE_HZ..=MAX_PLAUSIBLE_HZ).contains(&hz) {
            return;
        }
        let mut samples = self.samples.lock().expect("frequency monitor poisoned");
        samples.push_back((Instant::now(), hz));
        // Opportunistic eviction keeps the deque bounded even without reads.
        Self::evict(&mut samples, self.window);
    }

    /// Evict samples older than the window. `checked_sub`: early in process
    /// life `Instant::now() - window` underflows on some platforms — a panic
    /// here would poison the mutex and kill frequency tracking permanently.
    fn evict(samples: &mut VecDeque<(Instant, f64)>, window: Duration) {
        let Some(cutoff) = Instant::now().checked_sub(window) else {
            return;
        };
        while samples.front().is_some_and(|(t, _)| *t < cutoff) {
            samples.pop_front();
        }
    }

    /// Mean frequency over the window, or None when no fresh samples exist.
    pub fn mean(&self) -> Option<f64> {
        let mut samples = self.samples.lock().expect("frequency monitor poisoned");
        Self::evict(&mut samples, self.window);
        if samples.is_empty() {
            return None;
        }
        Some(samples.iter().map(|(_, hz)| hz).sum::<f64>() / samples.len() as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_monitor_has_no_mean() {
        let m = FrequencyMonitor::new(Duration::from_secs(60));
        assert_eq!(m.mean(), None);
    }

    #[test]
    fn mean_of_recorded_samples() {
        let m = FrequencyMonitor::new(Duration::from_secs(60));
        m.record(49.8);
        m.record(50.2);
        assert_eq!(m.mean(), Some(50.0));
    }

    #[test]
    fn implausible_samples_dropped() {
        let m = FrequencyMonitor::new(Duration::from_secs(60));
        m.record(0.0);
        m.record(f64::NAN);
        m.record(440.0);
        assert_eq!(m.mean(), None);
        m.record(50.0);
        assert_eq!(m.mean(), Some(50.0));
    }

    #[test]
    fn stale_samples_evicted() {
        let m = FrequencyMonitor::new(Duration::from_millis(0));
        m.record(50.0);
        assert_eq!(m.mean(), None);
    }
}
