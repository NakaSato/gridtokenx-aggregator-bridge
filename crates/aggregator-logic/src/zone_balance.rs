//! Per-zone energy balance over completed billing bins — measurement only.
//!
//! # Why this exists
//!
//! The token model mints on surplus and (once the burn path exists) retires on
//! consumption. The proposed invariant is that within a zone those two net out, so
//! a zone's minted and burned energy match and the zone carries no phantom energy.
//!
//! A zone can only net to zero if it is self-sufficient, which real zones are not —
//! they import and export across their boundary. So the honest form of the invariant
//! is not "zone == 0" but:
//!
//! ```text
//!   surplus(z) - deficit(z) = net_flow(z)      // per zone, its import/export
//!   Σ_z net_flow(z) = 0                        // the grid conserves overall
//! ```
//!
//! This module computes the left-hand side from the bins the aggregator already
//! produces. It deliberately does NOT act on the result: nothing here mints, burns,
//! dispatches, or blocks settlement. It exists so the imbalance can be observed for
//! real windows before any control or token behaviour is built on top of it — if the
//! zones turn out to be wildly unbalanced, that is a measurement problem to solve
//! before it becomes a settlement problem.
//!
//! Pure and sync (Sync Core, Async Edges): every function takes bins and returns
//! numbers, with no I/O.

use crate::aggregator::BillingBin;
use std::collections::BTreeMap;

/// Energy balance for one zone over a set of bins, all in kWh.
#[derive(Debug, Clone, PartialEq)]
pub struct ZoneBalance {
    /// `None` for bins with no recorded zone (see `BillingBin::zone_code`).
    pub zone_code: Option<u16>,
    /// Total generation across the zone's bins.
    pub generated_kwh: f64,
    /// Total consumption across the zone's bins.
    pub consumed_kwh: f64,
    /// Signed net: positive = the zone exported, negative = it imported.
    /// This is the quantity that must be zero for a self-sufficient zone, and
    /// equals the zone's cross-boundary flow otherwise.
    pub net_kwh: f64,
    /// How many meter-windows contributed.
    pub bin_count: usize,
}

impl ZoneBalance {
    /// Magnitude of the imbalance, direction discarded — the number to threshold
    /// on when deciding whether a zone needs demand-response attention.
    pub fn imbalance_kwh(&self) -> f64 {
        self.net_kwh.abs()
    }

    /// Imbalance as a fraction of the zone's throughput (generation + consumption).
    /// Scale-free, so a small zone and a large one can be compared on one threshold.
    /// `None` when the zone moved no energy at all (0/0 is not a ratio).
    pub fn imbalance_ratio(&self) -> Option<f64> {
        let throughput = self.generated_kwh + self.consumed_kwh;
        if throughput > 0.0 {
            Some(self.net_kwh.abs() / throughput)
        } else {
            None
        }
    }
}

/// Group bins by their recorded zone and total each zone's energy.
///
/// Bins with `zone_code == None` are kept as their own group rather than dropped
/// or folded into zone 0 — silently attributing unzoned energy to a real zone would
/// corrupt exactly the number this module exists to measure.
///
/// Ordered by zone so log output is stable between windows.
pub fn zone_balances(bins: &[BillingBin]) -> Vec<ZoneBalance> {
    let mut by_zone: BTreeMap<Option<u16>, ZoneBalance> = BTreeMap::new();

    for bin in bins {
        let entry = by_zone.entry(bin.zone_code).or_insert(ZoneBalance {
            zone_code: bin.zone_code,
            generated_kwh: 0.0,
            consumed_kwh: 0.0,
            net_kwh: 0.0,
            bin_count: 0,
        });
        entry.generated_kwh += to_f64(bin.energy_generated);
        entry.consumed_kwh += to_f64(bin.energy_consumed);
        entry.net_kwh += bin.net_energy_kwh();
        entry.bin_count += 1;
    }

    by_zone.into_values().collect()
}

/// System-wide net across every zone. Should trend to zero on a conserving grid;
/// a persistent non-zero total means generation and consumption measurement do not
/// agree system-wide, which is a metering problem, not a zone problem.
pub fn system_net_kwh(balances: &[ZoneBalance]) -> f64 {
    balances.iter().map(|b| b.net_kwh).sum()
}

/// Emit one sweep's zone balances. Lives here rather than inline in the binary,
/// which is a wiring-only entrypoint by contract (see CLAUDE.md "Runtime shape").
///
/// The only side effect is logging — deliberately, so the caller cannot mistake
/// this for something that mints, burns, dispatches, or gates settlement.
pub fn log_zone_balances(bins: &[BillingBin]) {
    let balances = zone_balances(bins);
    for zb in &balances {
        let zone = zb
            .zone_code
            .map(|z| z.to_string())
            .unwrap_or_else(|| "none".to_string());
        tracing::info!(
            zone = %zone,
            bins = zb.bin_count,
            generated_kwh = zb.generated_kwh,
            consumed_kwh = zb.consumed_kwh,
            net_kwh = zb.net_kwh,
            imbalance_ratio = zb.imbalance_ratio().unwrap_or(0.0),
            "⚖️  zone energy balance (observation only)"
        );
    }
    tracing::info!(
        zones = balances.len(),
        system_net_kwh = system_net_kwh(&balances),
        "⚖️  system energy balance across zones"
    );
}

fn to_f64(d: rust_decimal::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn bin(zone: Option<u16>, gen: i64, con: i64) -> BillingBin {
        let start = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        BillingBin {
            meter_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            meter_serial: "M-TEST".to_string(),
            start_time: start,
            end_time: start + chrono::Duration::minutes(15),
            energy_generated: Decimal::from(gen),
            energy_consumed: Decimal::from(con),
            reading_count: 1,
            energy_generated_peak: Decimal::ZERO,
            energy_generated_offpeak: Decimal::ZERO,
            energy_consumed_peak: Decimal::ZERO,
            energy_consumed_offpeak: Decimal::ZERO,
            max_demand_kw: Decimal::ZERO,
            zone_code: zone,
        }
    }

    #[test]
    fn surplus_and_deficit_are_exact_mirrors() {
        let exporter = bin(Some(1), 10, 4);
        assert_eq!(exporter.net_surplus_kwh(), Some(6.0));
        assert_eq!(exporter.net_deficit_kwh(), None);
        assert_eq!(exporter.net_energy_kwh(), 6.0);

        let importer = bin(Some(1), 4, 10);
        assert_eq!(importer.net_surplus_kwh(), None);
        // Deficit is reported POSITIVE.
        assert_eq!(importer.net_deficit_kwh(), Some(6.0));
        assert_eq!(importer.net_energy_kwh(), -6.0);
    }

    #[test]
    fn exact_net_zero_is_neither_surplus_nor_deficit() {
        let balanced = bin(Some(2), 7, 7);
        assert_eq!(balanced.net_surplus_kwh(), None);
        assert_eq!(balanced.net_deficit_kwh(), None);
        assert_eq!(balanced.net_energy_kwh(), 0.0);
    }

    #[test]
    fn a_self_sufficient_zone_nets_to_zero() {
        // One meter exports 6, another imports 6 — the zone is balanced even though
        // neither meter is.
        let balances = zone_balances(&[bin(Some(3), 10, 4), bin(Some(3), 4, 10)]);
        assert_eq!(balances.len(), 1);
        assert_eq!(balances[0].net_kwh, 0.0);
        assert_eq!(balances[0].generated_kwh, 14.0);
        assert_eq!(balances[0].consumed_kwh, 14.0);
        assert_eq!(balances[0].imbalance_kwh(), 0.0);
        assert_eq!(balances[0].imbalance_ratio(), Some(0.0));
    }

    #[test]
    fn zones_are_totalled_separately_and_the_system_conserves() {
        // Zone 1 exports 5, zone 2 imports 5 → each zone non-zero, system zero.
        let balances = zone_balances(&[bin(Some(1), 10, 5), bin(Some(2), 5, 10)]);
        assert_eq!(balances.len(), 2);
        assert_eq!(balances[0].net_kwh, 5.0);
        assert_eq!(balances[1].net_kwh, -5.0);
        assert_eq!(system_net_kwh(&balances), 0.0);
    }

    #[test]
    fn unzoned_bins_stay_separate_and_never_pollute_a_real_zone() {
        let balances = zone_balances(&[bin(None, 8, 0), bin(Some(0), 0, 8)]);
        assert_eq!(balances.len(), 2);
        // BTreeMap orders None before Some(0).
        assert_eq!(balances[0].zone_code, None);
        assert_eq!(balances[0].net_kwh, 8.0);
        assert_eq!(balances[1].zone_code, Some(0));
        assert_eq!(balances[1].net_kwh, -8.0);
    }

    #[test]
    fn distinct_feeders_must_not_be_summed_into_one_balance() {
        // Regression guard for the bug this keying fixes. The bin's zone used to be
        // the ingester's hash partition (zone hashed % IOT_NUM_ZONES), which on the
        // 80-meter fixture put real zones 1 and 2 in the SAME bucket. Two feeders,
        // one exporting and one importing by the same amount, then netted to zero
        // and reported a perfectly balanced "zone" while both were badly unbalanced.
        //
        // Keyed on the real zone_code the two stay separate and each imbalance is
        // visible; the system total is zero either way, which is exactly why the
        // system total alone cannot detect this.
        let balances = zone_balances(&[bin(Some(1), 30, 0), bin(Some(2), 0, 30)]);
        assert_eq!(balances.len(), 2, "distinct feeders must not be merged");
        assert_eq!(balances[0].net_kwh, 30.0, "zone 1 exports");
        assert_eq!(balances[1].net_kwh, -30.0, "zone 2 imports");
        assert_eq!(balances[0].imbalance_ratio(), Some(1.0));
        assert_eq!(balances[1].imbalance_ratio(), Some(1.0));
        // The system nets to zero — the very reading that hid the per-feeder problem.
        assert_eq!(system_net_kwh(&balances), 0.0);
    }

    #[test]
    fn imbalance_ratio_is_scale_free_and_safe_on_an_idle_zone() {
        // Same 50% imbalance at two very different scales.
        let small = zone_balances(&[bin(Some(1), 3, 1)]);
        let large = zone_balances(&[bin(Some(1), 300, 100)]);
        assert_eq!(small[0].imbalance_ratio(), large[0].imbalance_ratio());

        // A zone that moved nothing has no ratio rather than a NaN.
        let idle = zone_balances(&[bin(Some(9), 0, 0)]);
        assert_eq!(idle[0].imbalance_ratio(), None);
    }
}
