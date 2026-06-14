use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;
use uuid::Uuid;

/// Duration of a billing window in minutes
const WINDOW_MINUTES: u32 = 15;

/// Stable identity of a billing bin: (meter, window start). Used as the
/// in-memory map key and as the durable-store field key.
pub type BinKey = (Uuid, DateTime<Utc>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingBin {
    pub meter_id: Uuid,
    pub user_id: Uuid,
    pub meter_serial: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub energy_generated: Decimal,
    pub energy_consumed: Decimal,
    pub reading_count: u64,
}

impl BillingBin {
    /// Stable key for this bin: (meter_id, window_start).
    pub fn key(&self) -> BinKey {
        (self.meter_id, self.start_time)
    }
}

pub struct Aggregator {
    /// active_bins: (meter_id, window_start_time) -> BillingBin
    active_bins: HashMap<(Uuid, DateTime<Utc>), BillingBin>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            active_bins: HashMap::new(),
        }
    }

    /// Handles a new meter reading and updates or creates the corresponding billing
    /// bin. Returns a snapshot (clone) of the updated bin so the async edge can
    /// write it through to the durable store (crash-recovery of accumulated energy).
    pub fn handle_reading(
        &mut self,
        meter_id: Uuid,
        user_id: Uuid,
        meter_serial: String,
        generated: Decimal,
        consumed: Decimal,
        timestamp: DateTime<Utc>,
    ) -> BillingBin {
        let start_time = self.get_window_start(timestamp);
        let end_time = start_time + chrono::Duration::minutes(WINDOW_MINUTES as i64);

        let bin = self
            .active_bins
            .entry((meter_id, start_time))
            .or_insert_with(|| {
                debug!(
                    "🆕 Creating new billing bin for {} starting at {}",
                    meter_serial, start_time
                );
                BillingBin {
                    meter_id,
                    user_id,
                    meter_serial,
                    start_time,
                    end_time,
                    energy_generated: Decimal::ZERO,
                    energy_consumed: Decimal::ZERO,
                    reading_count: 0,
                }
            });

        bin.energy_generated += generated;
        bin.energy_consumed += consumed;
        bin.reading_count += 1;
        bin.clone()
    }

    /// Returns clones of all billing bins whose window closed at least `grace`
    /// ago, WITHOUT removing them. The `grace` delay lets late / briefly-buffered
    /// readings for a just-closed window still land in it before it is read.
    /// Pass `Duration::zero()` for the strict `end_time <= now` semantics used by
    /// the dispatch engine's completed-window capacity query.
    ///
    /// Non-destructive on purpose: the dispatch engine only reads completed-window
    /// capacity and never drains bins.
    pub fn peek_completed_bins(&self, grace: chrono::Duration) -> Vec<BillingBin> {
        let cutoff = gridtokenx_telemetry::time::now() - grace;
        self.active_bins
            .values()
            .filter(|bin| bin.end_time <= cutoff)
            .cloned()
            .collect()
    }

    /// Removes the given bins (settled & evicted from the durable store by the caller).
    pub fn remove_bins(&mut self, keys: &[BinKey]) {
        for key in keys {
            self.active_bins.remove(key);
        }
    }

    /// Helper to calculate the start of the 15-minute window for a given timestamp
    fn get_window_start(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let minute = (time.minute() / WINDOW_MINUTES) * WINDOW_MINUTES;
        time.with_minute(minute)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
    }
}

impl Default for Aggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, h, m, s).unwrap()
    }

    fn reading(agg: &mut Aggregator, gen: i64, con: i64, ts: DateTime<Utc>) -> BillingBin {
        agg.handle_reading(
            Uuid::nil(),
            Uuid::nil(),
            "M1".to_string(),
            Decimal::from(gen),
            Decimal::from(con),
            ts,
        )
    }

    // --- window floor: timestamp snaps down to the 15-min window start ---

    #[test]
    fn window_floors_to_quarter_hour() {
        let mut agg = Aggregator::new();
        // 10:07:42 → window [10:00, 10:15)
        let bin = reading(&mut agg, 1, 0, at(10, 7, 42));
        assert_eq!(bin.start_time, at(10, 0, 0), "07 min floors to :00");
        assert_eq!(bin.end_time, at(10, 15, 0), "end is start + 15 min");
    }

    #[test]
    fn window_floor_covers_all_four_quarters() {
        let mut agg = Aggregator::new();
        for (min, want) in [(0, 0), (14, 0), (15, 15), (29, 15), (30, 30), (44, 30), (45, 45), (59, 45)] {
            let bin = reading(&mut agg, 1, 0, at(10, min, 30));
            assert_eq!(bin.start_time, at(10, want, 0), "minute {min} floors to :{want:02}");
        }
    }

    #[test]
    fn window_start_strips_sub_minute() {
        let mut agg = Aggregator::new();
        // :14:59.999 must still floor to :00 (boundary just below the next window).
        let bin = reading(&mut agg, 1, 0, at(10, 14, 59));
        assert_eq!(bin.start_time, at(10, 0, 0));
        assert_eq!(bin.start_time.second(), 0, "seconds zeroed");
        assert_eq!(bin.start_time.nanosecond(), 0, "nanos zeroed");
    }

    // --- bin accumulate: readings in the same window+meter fold together ---

    #[test]
    fn readings_in_same_window_accumulate() {
        let mut agg = Aggregator::new();
        reading(&mut agg, 10, 2, at(10, 1, 0));
        let bin = reading(&mut agg, 5, 3, at(10, 13, 0)); // same [10:00,10:15) window
        assert_eq!(bin.energy_generated, Decimal::from(15), "generation sums");
        assert_eq!(bin.energy_consumed, Decimal::from(5), "consumption sums");
        assert_eq!(bin.reading_count, 2, "both readings counted in one bin");
    }

    #[test]
    fn readings_in_different_windows_are_separate_bins() {
        let mut agg = Aggregator::new();
        reading(&mut agg, 10, 0, at(10, 1, 0)); // [10:00,10:15)
        let later = reading(&mut agg, 7, 0, at(10, 20, 0)); // [10:15,10:30)
        // The second window's bin holds only its own reading — no carry-over.
        assert_eq!(later.energy_generated, Decimal::from(7));
        assert_eq!(later.reading_count, 1);
    }

    // --- peek_completed_bins: returns only bins past end_time, non-destructively ---

    #[test]
    fn peek_returns_only_closed_windows() {
        let mut agg = Aggregator::new();
        let past = gridtokenx_telemetry::time::now() - chrono::Duration::minutes(25); // window closed ~10 min ago
        reading(&mut agg, 30, 0, past);
        reading(&mut agg, 9, 0, gridtokenx_telemetry::time::now()); // current window end is in the future

        let done = agg.peek_completed_bins(chrono::Duration::zero());
        assert_eq!(done.len(), 1, "only the closed window is completed");
        assert_eq!(done[0].energy_generated, Decimal::from(30));
    }

    #[test]
    fn peek_is_non_destructive() {
        let mut agg = Aggregator::new();
        let past = gridtokenx_telemetry::time::now() - chrono::Duration::minutes(25);
        reading(&mut agg, 30, 0, past);

        assert_eq!(agg.peek_completed_bins(chrono::Duration::zero()).len(), 1);
        // A second peek still sees it — eviction only happens via remove_bins,
        // so a consumer that fails mid-read retries next tick instead of losing energy.
        let again = agg.peek_completed_bins(chrono::Duration::zero());
        assert_eq!(again.len(), 1, "peek must not evict");

        agg.remove_bins(&[again[0].key()]);
        assert!(agg.peek_completed_bins(chrono::Duration::zero()).is_empty(), "remove_bins evicts");
    }

    #[test]
    fn peek_grace_delays_recently_closed_window() {
        // Grace guard: a window that closed only moments ago is held back so a
        // late/buffered reading can still land in it before a consumer reads the bin.
        let mut agg = Aggregator::new();
        let now = gridtokenx_telemetry::time::now();
        let bin = BillingBin {
            meter_id: Uuid::nil(),
            user_id: Uuid::nil(),
            meter_serial: "M".to_string(),
            start_time: now - chrono::Duration::minutes(15) - chrono::Duration::seconds(30),
            end_time: now - chrono::Duration::seconds(30), // closed 30s ago
            energy_generated: Decimal::from(10),
            energy_consumed: Decimal::ZERO,
            reading_count: 1,
        };
        agg.active_bins.insert(bin.key(), bin);

        // Strict (zero grace): the just-closed window is eligible.
        assert_eq!(agg.peek_completed_bins(chrono::Duration::zero()).len(), 1);
        // 120s grace: closed only 30s ago → NOT yet eligible (hold for stragglers).
        assert!(agg.peek_completed_bins(chrono::Duration::seconds(120)).is_empty());
        // 20s grace: closed 30s ago > grace → now eligible.
        assert_eq!(agg.peek_completed_bins(chrono::Duration::seconds(20)).len(), 1);
    }
}
