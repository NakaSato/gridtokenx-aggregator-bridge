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
    /// The meter's **real** electrical zone, as reported on the reading
    /// (`DeviceReading.zone_code` — derived by the simulator from the GLM
    /// transformer topology, i.e. an actual feeder).
    ///
    /// Deliberately NOT the ingester's `get_zone_index` partition. That partition
    /// exists to spread load over a bounded set of Redis streams, so it hashes the
    /// zone modulo `IOT_NUM_ZONES` and therefore COLLIDES distinct feeders: on the
    /// 80-meter fixture, real zones 1 (29 meters) and 2 (31) both land in bucket 5
    /// while zone 3 (20) lands in bucket 9 — so a "per-zone" balance computed from
    /// the partition silently sums two unrelated feeders and cannot tell you which
    /// one is importing. Energy conservation is a property of a physical zone, so
    /// the balance has to key on the physical zone.
    ///
    /// `None` = the reading carried no zone (unzoned/grid-edge meters, code 0 in
    /// the simulator), or a bin restored from the durable store before this field
    /// existed.
    #[serde(default, alias = "zone_code")]
    pub zone_code: Option<u16>,
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

    /// Net import (deficit) for the window, in kWh — the exact mirror of
    /// [`Self::net_surplus_kwh`]: `Some(kwh)` only when the bin consumed more than
    /// it generated, `None` for net-zero or net-export windows.
    ///
    /// Returned POSITIVE (magnitude of the deficit), so callers do not have to
    /// reason about sign. Exactly one of `net_surplus_kwh` / `net_deficit_kwh` is
    /// `Some` for a given bin, and both are `None` at exact net zero.
    pub fn net_deficit_kwh(&self) -> Option<f64> {
        let net = self.energy_consumed - self.energy_generated;
        if net > Decimal::ZERO {
            net.to_f64()
        } else {
            None
        }
    }

    /// Signed net energy for the window in kWh: positive = export (surplus),
    /// negative = import (deficit). Used for zone balance, where the two directions
    /// must sum rather than be treated as separate cases.
    pub fn net_energy_kwh(&self) -> f64 {
        (self.energy_generated - self.energy_consumed)
            .to_f64()
            .unwrap_or(0.0)
    }
}

/// Per-meter ingest progress, used for **event-time** window completion.
///
/// Completion used to compare a bin's `end_time` (event time, derived from
/// reading timestamps) against the wall clock — so a meter whose timestamps run
/// behind the wall clock (simulator with a past sim-clock, backfill, deep
/// buffering) had every bin judged "complete" the moment it formed. The bin
/// settled and minted mid-window, the next reading re-created it, the re-settle
/// hit Chain Bridge's mint dedup (`mint:{serial}:{window}`) which replays the
/// prior signature WITHOUT minting — and the write-back then stamped the new
/// rows minted/confirmed with that replayed signature. Observed 2026-08-03:
/// DB-claimed minted energy far exceeding the on-chain balance.
struct MeterProgress {
    /// Highest reading timestamp seen from this meter — the event-time
    /// watermark. A window is only over once the meter's own data moves past it.
    watermark: DateTime<Utc>,
    /// Wall-clock time the last reading arrived. Idle fallback: a meter that
    /// stops sending would otherwise strand its final window forever.
    last_arrival: DateTime<Utc>,
}

/// How long a settled window's key is remembered (relative to the meter's
/// watermark) so a straggler cannot re-create — and re-settle — its bin.
const SETTLED_RETENTION_HOURS: i64 = 24;

pub struct Aggregator {
    /// active_bins: (meter_id, window_start_time) -> BillingBin
    active_bins: HashMap<(Uuid, DateTime<Utc>), BillingBin>,
    /// Event-time progress per meter (see [`MeterProgress`]).
    progress: HashMap<Uuid, MeterProgress>,
    /// Window starts already settled per meter. A reading for one of these is a
    /// LATE ARRIVAL: its window minted already and the on-chain
    /// `(meter, window)` PDA permits no top-up, so re-binning it can only
    /// produce a dedup-replayed "mint" that never happened. Process-local: a
    /// straggler landing after a restart still re-forms its bin, where the
    /// bridge dedup remains the (silent) backstop.
    settled: HashMap<Uuid, std::collections::BTreeSet<DateTime<Utc>>>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            active_bins: HashMap::new(),
            progress: HashMap::new(),
            settled: HashMap::new(),
        }
    }

    /// Handles a new meter reading and updates or creates the corresponding billing
    /// bin. Returns a snapshot (clone) of the updated bin so the async edge can
    /// write it through to the durable store (crash-recovery of accumulated energy).
    ///
    /// Returns `None` for a **late arrival** — a reading whose window this
    /// process already settled. Its window minted already and the on-chain
    /// `(meter, window)` PDA permits no second mint, so binning it again could
    /// only re-settle into a dedup replay that stamps unminted energy as minted.
    /// The reading still reaches every non-billing sink (zone streams, InfluxDB,
    /// Kafka, the Postgres history row) — only the billing bin drops it. The
    /// caller should count the drop (it is the residual, visible form of energy
    /// that arrived too late to tokenize).
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
        // Zone partition the ingester routed this reading to. Recorded on the bin
        // for per-zone energy balance; see `BillingBin::zone_code`.
        zone_code: Option<u16>,
    ) -> Option<BillingBin> {
        let start_time = self.get_window_start(timestamp);
        let end_time = start_time + chrono::Duration::minutes(WINDOW_MINUTES as i64);

        // Progress first — even a late reading is evidence the meter is alive
        // and of how far its event clock has advanced.
        let now = gridtokenx_telemetry::time::now();
        let p = self.progress.entry(meter_id).or_insert(MeterProgress {
            watermark: timestamp,
            last_arrival: now,
        });
        if timestamp > p.watermark {
            p.watermark = timestamp;
        }
        p.last_arrival = now;

        if self
            .settled
            .get(&meter_id)
            .is_some_and(|w| w.contains(&start_time))
        {
            debug!(
                "late reading for {} window {} — window already settled, excluded from billing",
                meter_serial, start_time
            );
            return None;
        }

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
                    zone_code,
                }
            });

        bin.energy_generated += generated;
        bin.energy_consumed += consumed;
        bin.reading_count += 1;
        // Backfill for a bin restored from the durable store before this field
        // existed, or created by a path that had no zone at the time.
        if bin.zone_code.is_none() {
            bin.zone_code = zone_code;
        }

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

        Some(bin.clone())
    }

    /// Returns clones of all billing bins whose window is complete, WITHOUT
    /// removing them.
    ///
    /// Completion is judged in **event time**, per meter: a window is over when
    /// the meter's own watermark (highest reading timestamp seen) has moved past
    /// `end_time + grace`. Judging by wall clock — the old rule — settled a
    /// window the instant it formed whenever the meter's timestamps ran behind
    /// the wall clock (sim clock, backfill), minting mid-window and turning
    /// every subsequent reading into a dedup-replay that falsified the
    /// write-back. Two fallbacks keep event time from stranding bins:
    ///
    /// - **Idle**: a meter quiet for `grace` of wall time is done sending; its
    ///   bins complete once their `end_time` is also past (skew guard). Only
    ///   when `grace > 0` — the dispatch engine's zero-grace capacity peek must
    ///   not see every bin as complete.
    /// - **No progress entry** (bin restored from the durable store, meter
    ///   silent since restart): wall-clock completion, the pre-watermark rule.
    ///
    /// Non-destructive on purpose: the dispatch engine only reads completed-window
    /// capacity and never drains bins.
    pub fn peek_completed_bins(&self, grace: chrono::Duration) -> Vec<BillingBin> {
        self.peek_completed_bins_at(gridtokenx_telemetry::time::now(), grace)
    }

    /// [`Self::peek_completed_bins`] against an explicit `now` (testability —
    /// the idle and restored-bin arms are wall-clock-dependent).
    pub fn peek_completed_bins_at(
        &self,
        now: DateTime<Utc>,
        grace: chrono::Duration,
    ) -> Vec<BillingBin> {
        self.active_bins
            .values()
            .filter(|bin| match self.progress.get(&bin.meter_id) {
                Some(p) => {
                    p.watermark >= bin.end_time + grace
                        || (grace > chrono::Duration::zero()
                            && now - p.last_arrival >= grace
                            && bin.end_time <= now)
                }
                None => bin.end_time + grace <= now,
            })
            .cloned()
            .collect()
    }

    /// Removes the given bins (settled & evicted from the durable store by the
    /// caller), and remembers each settled window so a straggler reading cannot
    /// re-create — and re-settle — a window that already minted.
    pub fn remove_bins(&mut self, keys: &[BinKey]) {
        for (meter_id, window_start) in keys {
            self.active_bins.remove(&(*meter_id, *window_start));
            let windows = self.settled.entry(*meter_id).or_default();
            windows.insert(*window_start);
            // Bounded memory: no straggler arrives a day of event time late —
            // and if one somehow does, the bridge's mint dedup is the backstop.
            if let Some(p) = self.progress.get(meter_id) {
                let horizon = p.watermark - chrono::Duration::hours(SETTLED_RETENTION_HOURS);
                windows.retain(|w| *w >= horizon);
            }
        }
    }

    /// Bulk-loads bins recovered from the durable store at startup (crash
    /// recovery). Each bin is keyed by its `(meter_id, window_start)` so it
    /// resumes accumulating from its persisted totals; an already-present key is
    /// overwritten by the restored bin. Pure (no I/O) — the caller fetches the
    /// bins from the async durable store and hands them in.
    pub fn restore_bins(&mut self, bins: Vec<BillingBin>) {
        for bin in bins {
            self.active_bins.insert(bin.key(), bin);
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
            None,
        )
        .expect("window not settled — reading must bin")
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
            Some(7),
        )
        .expect("window not settled — reading must bin")
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
        // Advance the meter's watermark past the first window's end so it
        // completes in event time (the newer reading's own window stays open).
        reading(&mut agg, 1, 0, past + chrono::Duration::minutes(20));

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
        reading(
            &mut agg,
            30,
            0,
            gridtokenx_telemetry::time::now() - chrono::Duration::minutes(25),
        );
        reading(
            &mut agg,
            12,
            0,
            gridtokenx_telemetry::time::now() - chrono::Duration::minutes(40),
        );
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
    fn restore_bins_reloads_windows_that_keep_accumulating() {
        // Crash-recovery path: a bin persisted by the durable store is restored
        // into a fresh aggregator, then a later reading in the same window folds
        // into the restored totals (proves it re-keyed correctly).
        let mut agg = Aggregator::new();
        let restored = BillingBin {
            meter_id: Uuid::nil(),
            user_id: Uuid::nil(),
            meter_serial: "M1".to_string(),
            start_time: at(10, 0, 0),
            end_time: at(10, 15, 0),
            energy_generated: Decimal::from(10),
            energy_consumed: Decimal::from(2),
            reading_count: 3,
            energy_generated_peak: Decimal::ZERO,
            energy_generated_offpeak: Decimal::ZERO,
            energy_consumed_peak: Decimal::ZERO,
            energy_consumed_offpeak: Decimal::ZERO,
            max_demand_kw: Decimal::ZERO,
            zone_code: None,
        };
        agg.restore_bins(vec![restored]);

        // A new reading in the SAME [10:00,10:15) window must fold into the
        // restored bin, not start a fresh one.
        let bin = reading(&mut agg, 5, 1, at(10, 7, 0));
        assert_eq!(
            bin.energy_generated,
            Decimal::from(15),
            "restored 10 + new 5"
        );
        assert_eq!(bin.energy_consumed, Decimal::from(3), "restored 2 + new 1");
        assert_eq!(
            bin.reading_count, 4,
            "restored count carried forward (3 + 1)"
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
            zone_code: None,
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

    // --- event-time completion: the mint write-back divergence regression ---

    #[test]
    fn backdated_window_completes_by_watermark_not_wall_clock() {
        // The 2026-08-03 incident: a simulator with a days-behind sim clock had
        // every bin judged complete the moment it formed (end_time <= wall now),
        // so windows settled and minted MID-window, re-formed on the next
        // reading, and re-settled into Chain Bridge dedup replays that stamped
        // unminted energy as minted. A still-filling backdated window must NOT
        // be complete; it completes only when the meter's own data moves past it.
        let mut agg = Aggregator::new();
        let sim = at(10, 1, 0); // days behind wall clock in the incident; any past instant works
        reading(&mut agg, 5, 0, sim);
        reading(&mut agg, 5, 0, at(10, 8, 0));

        let grace = chrono::Duration::seconds(120);
        assert!(
            agg.peek_completed_bins(grace).is_empty(),
            "window still filling in event time — settling it here is the divergence bug"
        );

        // The meter's clock passes end + grace → now (and only now) it settles.
        reading(&mut agg, 1, 0, at(10, 17, 1));
        let done = agg.peek_completed_bins(grace);
        assert_eq!(
            done.len(),
            1,
            "watermark past end+grace completes the window"
        );
        assert_eq!(
            done[0].energy_generated,
            Decimal::from(10),
            "the FULL window energy settles in one mint — no mid-window partial"
        );
    }

    #[test]
    fn late_reading_for_settled_window_is_excluded_from_billing() {
        // Once a window settles (mints), the on-chain (meter, window) PDA
        // permits no top-up: a straggler must not re-create the bin, or the
        // re-settle dedup-replays the old signature over unminted energy.
        let mut agg = Aggregator::new();
        reading(&mut agg, 10, 0, at(10, 1, 0));
        reading(&mut agg, 1, 0, at(10, 20, 0)); // watermark past window end
        let done = agg.peek_completed_bins(chrono::Duration::zero());
        assert_eq!(done.len(), 1);
        agg.remove_bins(&[done[0].key()]);

        let late = agg.handle_reading(
            Uuid::nil(),
            Uuid::nil(),
            "M1".to_string(),
            Decimal::from(7),
            Decimal::ZERO,
            at(10, 9, 0), // inside the already-settled [10:00,10:15) window
            None,
            None,
            None,
        );
        assert!(late.is_none(), "late reading must be excluded from billing");
        assert!(
            agg.peek_completed_bins(chrono::Duration::zero())
                .iter()
                .all(|b| b.start_time != at(10, 0, 0)),
            "the settled window must not re-form"
        );
    }

    #[test]
    fn idle_meter_settles_by_wall_clock_fallback() {
        // A meter that stops sending would strand its final window forever under
        // pure event-time completion — after `grace` of wall-clock silence its
        // bins settle anyway.
        let mut agg = Aggregator::new();
        reading(&mut agg, 10, 0, at(10, 1, 0)); // watermark stuck inside the window
        let grace = chrono::Duration::seconds(120);

        assert!(
            agg.peek_completed_bins(grace).is_empty(),
            "meter just sent — not idle yet"
        );
        let later = gridtokenx_telemetry::time::now() + chrono::Duration::seconds(121);
        assert_eq!(
            agg.peek_completed_bins_at(later, grace).len(),
            1,
            "after grace of silence the stranded window settles"
        );
        assert!(
            agg.peek_completed_bins_at(later, chrono::Duration::zero())
                .is_empty(),
            "zero-grace (dispatch capacity) peek must never use the idle arm"
        );
    }
}
