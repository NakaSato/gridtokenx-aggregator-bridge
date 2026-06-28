//! Maps completed 15-minute billing bins to InfluxDB points (the `billing`
//! measurement).
//!
//! With the on-chain settlement path removed (see `bin/.../main.rs`), completed
//! billing bins — TOU peak/off-peak energy split plus the window's max net-import
//! demand — had no durable consumer: they only fed the dispatch engine's
//! capacity query (which reads `energy_generated` alone) and were never evicted.
//!
//! This module gives those aggregated, tariff-split values a home: the binary's
//! flush loop peeks completed bins, converts each here, records it to the
//! independent InfluxDB sink, then evicts it — which also bounds the previously
//! unbounded `active_bins` map.

use aggregator_persistence::infra::influxdb::TelemetryPoint;
use rust_decimal::prelude::ToPrimitive;

use crate::aggregator::BillingBin;

/// Convert a completed billing bin into a `billing`-measurement InfluxDB point,
/// timestamped at the window's `end_time`.
///
/// Returns `None` only when `end_time` is not representable as nanoseconds since
/// the epoch (mirrors the energy path in `router::reading_to_point`). `Decimal`
/// fields degrade to `0.0` if not finite-convertible — bins hold small kWh/kW
/// magnitudes, so this is defensive, not lossy in practice.
pub fn bin_to_billing_point(bin: &BillingBin) -> Option<TelemetryPoint> {
    let timestamp_ns = bin.end_time.timestamp_nanos_opt()?;

    let f = |d: rust_decimal::Decimal| d.to_f64().unwrap_or(0.0);

    Some(TelemetryPoint {
        measurement: "billing",
        device_id: bin.meter_id.to_string(),
        device_type: "smart_meter".to_string(),
        serial_number: bin.meter_serial.clone(),
        // Bins do not carry the originating zone; billing rolls up per meter.
        zone_code: None,
        extra_tags: vec![("user_id", bin.user_id.to_string())],
        fields: vec![
            ("energy_generated", f(bin.energy_generated)),
            ("energy_consumed", f(bin.energy_consumed)),
            ("energy_generated_peak", f(bin.energy_generated_peak)),
            ("energy_generated_offpeak", f(bin.energy_generated_offpeak)),
            ("energy_consumed_peak", f(bin.energy_consumed_peak)),
            ("energy_consumed_offpeak", f(bin.energy_consumed_offpeak)),
            ("max_demand_kw", f(bin.max_demand_kw)),
            ("reading_count", bin.reading_count as f64),
        ],
        timestamp_ns,
    })
}

/// What the settlement flush loop should do with a completed bin's surplus,
/// given whether minting is enabled. The flush loop (`bin/.../main.rs`) calls
/// this so the mint-vs-skip policy is a pure, unit-testable decision rather than
/// branching buried in a `tokio::spawn`. **Eviction is independent of this** —
/// the loop evicts every completed bin regardless of the decision (bounding the
/// `active_bins` map); this only governs the mint side effect.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum MintDecision {
    /// Net generation > consumption — mint this many kWh to the meter owner.
    Surplus(f64),
    /// Net consumption / net-zero with minting enabled — nothing to mint, but
    /// the loop still records a `no_surplus` outcome (the metric denominator).
    NoSurplus,
    /// Minting disabled (`MINT_VIA_CHAIN_BRIDGE` off / NATS down) — skip the
    /// mint path entirely, emit no mint metric.
    Disabled,
}

/// Decide what to mint for a completed bin. Pure: mirrors the flush-loop branch
/// exactly so the loop can be a thin caller.
pub fn plan_mint(bin: &BillingBin, mint_enabled: bool) -> MintDecision {
    if !mint_enabled {
        return MintDecision::Disabled;
    }
    match bin.net_surplus_kwh() {
        Some(kwh) => MintDecision::Surplus(kwh),
        None => MintDecision::NoSurplus,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_bin() -> BillingBin {
        BillingBin {
            meter_id: Uuid::nil(),
            user_id: Uuid::nil(),
            meter_serial: "SN-1".to_string(),
            start_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            end_time: Utc.timestamp_opt(1_700_000_900, 0).unwrap(),
            energy_generated: Decimal::new(15, 1), // 1.5
            energy_consumed: Decimal::new(40, 1),  // 4.0
            reading_count: 3,
            energy_generated_peak: Decimal::new(10, 1), // 1.0
            energy_generated_offpeak: Decimal::new(5, 1), // 0.5
            energy_consumed_peak: Decimal::new(25, 1),  // 2.5
            energy_consumed_offpeak: Decimal::new(15, 1), // 1.5
            max_demand_kw: Decimal::new(24, 1),         // 2.4
        }
    }

    #[test]
    fn maps_bin_fields_and_tou_split() {
        let point = bin_to_billing_point(&sample_bin()).expect("end_time representable");

        assert_eq!(point.measurement, "billing");
        assert_eq!(point.device_type, "smart_meter");
        assert_eq!(point.serial_number, "SN-1");
        assert!(point.zone_code.is_none());
        // user_id surfaced as a tag for per-user billing queries.
        assert!(point.extra_tags.iter().any(|(k, _)| *k == "user_id"));

        let field = |name: &str| {
            point
                .fields
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| *v)
        };
        assert_eq!(field("energy_generated"), Some(1.5));
        assert_eq!(field("energy_consumed"), Some(4.0));
        assert_eq!(field("energy_generated_peak"), Some(1.0));
        assert_eq!(field("energy_generated_offpeak"), Some(0.5));
        assert_eq!(field("energy_consumed_peak"), Some(2.5));
        assert_eq!(field("energy_consumed_offpeak"), Some(1.5));
        assert_eq!(field("max_demand_kw"), Some(2.4));
        assert_eq!(field("reading_count"), Some(3.0));

        // Timestamp is the window close (end_time), not start.
        assert_eq!(point.timestamp_ns, 1_700_000_900 * 1_000_000_000);
    }

    // --- plan_mint: the flush-loop mint-vs-skip policy ---

    #[test]
    fn plan_mint_disabled_skips_regardless_of_surplus() {
        // sample_bin is net-import, but even a surplus bin must yield Disabled
        // when minting is off — no metric, no mint.
        let mut bin = sample_bin();
        bin.energy_generated = Decimal::new(100, 1); // 10.0 > consumed 4.0 (surplus)
        assert_eq!(plan_mint(&bin, false), MintDecision::Disabled);
    }

    #[test]
    fn plan_mint_net_import_yields_no_surplus() {
        // sample_bin: generated 1.5 < consumed 4.0 → nothing to mint.
        assert_eq!(plan_mint(&sample_bin(), true), MintDecision::NoSurplus);
    }

    #[test]
    fn plan_mint_surplus_yields_net_kwh() {
        let mut bin = sample_bin();
        bin.energy_generated = Decimal::new(100, 1); // 10.0
        bin.energy_consumed = Decimal::new(40, 1); // 4.0  → net surplus 6.0
        assert_eq!(plan_mint(&bin, true), MintDecision::Surplus(6.0));
    }

    #[test]
    fn plan_mint_net_zero_is_no_surplus_not_a_zero_mint() {
        let mut bin = sample_bin();
        bin.energy_generated = Decimal::new(40, 1);
        bin.energy_consumed = Decimal::new(40, 1); // exactly net-zero
        assert_eq!(
            plan_mint(&bin, true),
            MintDecision::NoSurplus,
            "net-zero must not mint a 0 kWh surplus"
        );
    }
}
