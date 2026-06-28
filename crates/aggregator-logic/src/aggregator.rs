use chrono::{DateTime, Timelike, Utc};
use rust_decimal::prelude::ToPrimitive;
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
    // Time-of-use split of the window energy (peak = tariff rate 1, off-peak =
    // rate 2), so settlement can price each tariff window. `serde(default)` keeps
    // bins persisted before these fields existed deserializable (crash recovery).
    #[serde(default)]
    pub energy_generated_peak: Decimal,
    #[serde(default)]
    pub energy_generated_offpeak: Decimal,
    #[serde(default)]
    pub energy_consumed_peak: Decimal,
    #[serde(default)]
    pub energy_consumed_offpeak: Decimal,
    /// Peak net import demand seen in the window (kW), for demand-charge billing.
    #[serde(default)]
    pub max_demand_kw: Decimal,
}

impl BillingBin {
    /// Stable key for this bin: (meter_id, window_start).
    pub fn key(&self) -> BinKey {
        (self.meter_id, self.start_time)
    }

    /// Window start as Unix epoch milliseconds. Used as the mint idempotency
    /// component (`mint:{serial}:{window_start_ms}`) and the on-chain
    /// `(meter_id, window_start_ms)` settlement PDA seed.
    pub fn window_start_ms(&self) -> i64 {
        self.start_time.timestamp_millis()
    }

    /// Net surplus generation for the window, in kWh — `Some(kwh)` only when the
    /// bin generated more than it consumed (the mintable amount). Returns `None`
    /// for net-zero or net-import windows (nothing to tokenize).
    pub fn net_surplus_kwh(&self) -> Option<f64> {
        let net = self.energy_generated - self.energy_consumed;
        if net > Decimal::ZERO {
            net.to_f64()
        } else {
            None
        }
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
    #[allow(clippy::too_many_arguments)]
    pub fn handle_reading(
        &mut self,
        meter_id: Uuid,
        user_id: Uuid,
        meter_serial: String,
        generated: Decimal,
        consumed: Decimal,
        timestamp: DateTime<Utc>,
        // Active tariff (1 = peak, 2 = off-peak) and net import demand (kW) for
        // this reading, decoded from the DLMS payload. `None` leaves the TOU
        // split / demand untouched (e.g. non-DLMS or pre-TOU sources).
        tariff_period: Option<u8>,
        demand_kw: Option<Decimal>,
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
                    energy_generated_peak: Decimal::ZERO,
                    energy_generated_offpeak: Decimal::ZERO,
                    energy_consumed_peak: Decimal::ZERO,
                    energy_consumed_offpeak: Decimal::ZERO,
                    max_demand_kw: Decimal::ZERO,
                }
            });

        bin.energy_generated += generated;
        bin.energy_consumed += consumed;
        bin.reading_count += 1;

        // TOU split: route this reading's energy into the active tariff's bucket.
        // Rate 1 = peak, rate 2 = off-peak; any other/missing value leaves the
        // split untouched (totals above stay authoritative regardless).
        match tariff_period {
            Some(1) => {
                bin.energy_generated_peak += generated;
                bin.energy_consumed_peak += consumed;
            }
            Some(2) => {
                bin.energy_generated_offpeak += generated;
                bin.energy_consumed_offpeak += consumed;
            }
            _ => {}
        }

        // Track the window's peak net import demand.
        if let Some(d) = demand_kw {
            if d > bin.max_demand_kw {
                bin.max_demand_kw = d;
            }
        }

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
            None,
            None,
        )
    }

    fn reading_tou(
        agg: &mut Aggregator,
        gen: i64,
        con: i64,
        ts: DateTime<Utc>,
        tariff: u8,
        demand_kw: i64,
    ) -> BillingBin {
        agg.handle_reading(
            Uuid::nil(),
            Uuid::nil(),
            "M1".to_string(),
            Decimal::from(gen),
            Decimal::from(con),
            ts,
            Some(tariff),
            Some(Decimal::from(demand_kw)),
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
        for (min, want) in [
            (0, 0),
            (14, 0),
            (15, 15),
            (29, 15),
            (30, 30),
            (44, 30),
            (45, 45),
            (59, 45),
        ] {
            let bin = reading(&mut agg, 1, 0, at(10, min, 30));
            assert_eq!(
                bin.start_time,
                at(10, want, 0),
                "minute {min} floors to :{want:02}"
            );
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
    fn tou_split_and_max_demand_accumulate() {
        let mut agg = Aggregator::new();
        // peak reading then off-peak reading in the same window.
        reading_tou(&mut agg, 1, 10, at(10, 1, 0), 1, 8); // peak, 8 kW demand
        let bin = reading_tou(&mut agg, 2, 4, at(10, 5, 0), 2, 5); // off-peak, 5 kW
                                                                   // Totals fold across both.
        assert_eq!(bin.energy_consumed, Decimal::from(14));
        assert_eq!(bin.energy_generated, Decimal::from(3));
        // TOU buckets split by tariff.
        assert_eq!(bin.energy_consumed_peak, Decimal::from(10));
        assert_eq!(bin.energy_consumed_offpeak, Decimal::from(4));
        assert_eq!(bin.energy_generated_peak, Decimal::from(1));
        assert_eq!(bin.energy_generated_offpeak, Decimal::from(2));
        // Max demand is the running peak (8 > 5).
        assert_eq!(bin.max_demand_kw, Decimal::from(8));
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
        assert!(
            agg.peek_completed_bins(chrono::Duration::zero()).is_empty(),
            "remove_bins evicts"
        );
    }

    #[test]
    fn flush_drain_evicts_completed_bins_but_keeps_open_ones() {
        // Mirrors the settlement flush loop: peek completed bins, then
        // remove_bins(their keys). The invariant that bounds the otherwise-
        // unbounded active_bins map is that EVERY completed bin is evicted while
        // a still-open window survives for more readings.
        let mut agg = Aggregator::new();
        // Two distinct closed windows (different meters → different bins).
        reading(&mut agg, 30, 0, gridtokenx_telemetry::time::now() - chrono::Duration::minutes(25));
        reading(&mut agg, 12, 0, gridtokenx_telemetry::time::now() - chrono::Duration::minutes(40));
        // One open (current-window) bin that must NOT be evicted.
        reading(&mut agg, 5, 0, gridtokenx_telemetry::time::now());

        let completed = agg.peek_completed_bins(chrono::Duration::zero());
        assert_eq!(completed.len(), 2, "both closed windows are completed");

        let keys: Vec<_> = completed.iter().map(|b| b.key()).collect();
        agg.remove_bins(&keys);

        // All completed bins gone (map bounded); the open window remains.
        assert!(
            agg.peek_completed_bins(chrono::Duration::zero()).is_empty(),
            "every completed bin must be evicted by the drain"
        );
        // The open bin still accumulates — a later reading folds into it, proving
        // it survived eviction rather than being dropped.
        let still_open = reading(&mut agg, 3, 0, gridtokenx_telemetry::time::now());
        assert_eq!(
            still_open.energy_generated,
            Decimal::from(8),
            "open window survived the drain and kept accumulating (5 + 3)"
        );
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
            energy_generated_peak: Decimal::ZERO,
            energy_generated_offpeak: Decimal::ZERO,
            energy_consumed_peak: Decimal::ZERO,
            energy_consumed_offpeak: Decimal::ZERO,
            max_demand_kw: Decimal::ZERO,
        };
        agg.active_bins.insert(bin.key(), bin);

        // Strict (zero grace): the just-closed window is eligible.
        assert_eq!(agg.peek_completed_bins(chrono::Duration::zero()).len(), 1);
        // 120s grace: closed only 30s ago → NOT yet eligible (hold for stragglers).
        assert!(agg
            .peek_completed_bins(chrono::Duration::seconds(120))
            .is_empty());
        // 20s grace: closed 30s ago > grace → now eligible.
        assert_eq!(
            agg.peek_completed_bins(chrono::Duration::seconds(20)).len(),
            1
        );
    }
}
