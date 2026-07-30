//! Durable billing-bin store (Redis) — crash-recovery for in-flight 15-minute
//! windows.
//!
//! The [`Aggregator`](crate::aggregator::Aggregator) accumulates each open
//! window in an in-memory map; a process restart would otherwise lose every
//! partially-filled bin (the raw readings survive on the zone Redis Streams, but
//! the rolled-up gen/cons totals do not). This store **write-through**s each
//! updated bin to a single Redis hash so a restart can [`load_all`] them back
//! into the aggregator before the settlement loop starts. The settlement loop
//! [`remove`]s a bin's durable entry once it has been settled + evicted.
//!
//! Async edge by design (sync-core / async-edges): the pure `Aggregator` never
//! touches Redis — the ingest edge and the settlement loop drive this store.
//! Degrade-safe: every method returns `Err` on a Redis fault so callers can
//! `warn!` and continue in memory-only mode (the underlying
//! [`ConnectionManager`] reconnects on its own). Never make a fault fatal.

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::HashMap;

use crate::aggregator::{BillingBin, BinKey};
use crate::redis_hash::parse_redis_hash;

/// Redis hash holding every in-flight bin: field = [`bin_field`], value = JSON.
const BINS_KEY: &str = "gridtokenx:billing:bins";

/// Durable field key for a bin: `"{meter_id}:{window_start_ms}"`. Stable for a
/// `(meter, window)` so a write-through overwrites the same field as the bin
/// accumulates, and `remove` deletes exactly what was settled.
fn bin_field(key: &BinKey) -> String {
    format!("{}:{}", key.0, key.1.timestamp_millis())
}

/// Decodes an `HGETALL` map into bins, skipping (with a `warn!`) any value that
/// fails to deserialize rather than aborting the whole restore. `BillingBin`'s
/// `#[serde(default)]` fields keep bins written by an older binary loadable here.
fn parse_bins(map: HashMap<String, String>) -> Vec<BillingBin> {
    parse_redis_hash(map, "durable bin store")
}

/// Redis-backed durable store for billing bins. Cheap to clone (the
/// `ConnectionManager` is a multiplexed, self-reconnecting handle).
#[derive(Clone)]
pub struct BinStore {
    conn: ConnectionManager,
}

impl BinStore {
    /// Wraps an already-connected Redis connection manager.
    #[must_use]
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// Write-through a bin's current accumulated state (overwrites its field).
    ///
    /// # Errors
    /// Returns an error if the Redis `HSET` fails (caller should `warn!` and
    /// continue in memory-only mode).
    pub async fn write(&self, bin: &BillingBin) -> Result<()> {
        let json = serde_json::to_string(bin).context("serialize billing bin")?;
        let mut conn = self.conn.clone();
        let _: () = conn
            .hset(BINS_KEY, bin_field(&bin.key()), json)
            .await
            .context("HSET durable billing bin")?;
        Ok(())
    }

    /// Delete the durable entries for the given (settled + evicted) bins.
    /// No-op for an empty slice.
    ///
    /// # Errors
    /// Returns an error if the Redis `HDEL` fails.
    pub async fn remove(&self, keys: &[BinKey]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let fields: Vec<String> = keys.iter().map(bin_field).collect();
        let mut conn = self.conn.clone();
        let _: () = conn
            .hdel(BINS_KEY, fields)
            .await
            .context("HDEL durable billing bins")?;
        Ok(())
    }

    /// Load every persisted bin (crash recovery). Unparsable entries are skipped
    /// with a `warn!` rather than failing the whole restore.
    ///
    /// # Errors
    /// Returns an error if the Redis `HGETALL` fails.
    pub async fn load_all(&self) -> Result<Vec<BillingBin>> {
        let mut conn = self.conn.clone();
        let map: HashMap<String, String> = conn
            .hgetall(BINS_KEY)
            .await
            .context("HGETALL durable billing bins")?;
        Ok(parse_bins(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::BillingBin;
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_bin() -> BillingBin {
        BillingBin {
            meter_id: Uuid::from_u128(0x1234_5678),
            user_id: Uuid::from_u128(0x9abc),
            meter_serial: "MTR-001".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 1, 1, 10, 15, 0).unwrap(),
            energy_generated: Decimal::new(125, 1), // 12.5
            energy_consumed: Decimal::new(40, 1),   // 4.0
            reading_count: 7,
            energy_generated_peak: Decimal::new(80, 1),
            energy_generated_offpeak: Decimal::new(45, 1),
            energy_consumed_peak: Decimal::new(30, 1),
            energy_consumed_offpeak: Decimal::new(10, 1),
            max_demand_kw: Decimal::new(33, 1),
            zone_index: Some(5),
        }
    }

    #[test]
    fn field_is_meter_id_and_window_ms() {
        let bin = sample_bin();
        let want = format!("{}:{}", bin.meter_id, bin.start_time.timestamp_millis());
        assert_eq!(bin_field(&bin.key()), want);
        // Window component is epoch-millis (matches the mint idempotency window).
        assert!(bin_field(&bin.key()).ends_with(":1767261600000"));
    }

    #[test]
    fn serialize_restore_roundtrip_preserves_all_fields() {
        // Mirrors write()->load_all(): JSON encode, then parse_bins decodes the
        // HGETALL map. All TOU/demand fields must survive the roundtrip.
        let bin = sample_bin();
        let json = serde_json::to_string(&bin).unwrap();
        let map = HashMap::from([(bin_field(&bin.key()), json)]);

        let got = parse_bins(map);
        assert_eq!(got.len(), 1);
        let r = &got[0];
        assert_eq!(r.meter_id, bin.meter_id);
        assert_eq!(r.user_id, bin.user_id);
        assert_eq!(r.meter_serial, bin.meter_serial);
        assert_eq!(r.start_time, bin.start_time);
        assert_eq!(r.end_time, bin.end_time);
        assert_eq!(r.energy_generated, bin.energy_generated);
        assert_eq!(r.energy_consumed, bin.energy_consumed);
        assert_eq!(r.reading_count, bin.reading_count);
        assert_eq!(r.energy_generated_peak, bin.energy_generated_peak);
        assert_eq!(r.energy_generated_offpeak, bin.energy_generated_offpeak);
        assert_eq!(r.energy_consumed_peak, bin.energy_consumed_peak);
        assert_eq!(r.energy_consumed_offpeak, bin.energy_consumed_offpeak);
        assert_eq!(r.max_demand_kw, bin.max_demand_kw);
        // The mintable surplus is preserved (12.5 - 4.0 = 8.5).
        assert_eq!(r.net_surplus_kwh(), Some(8.5));
    }

    #[test]
    fn load_skips_unparsable_entries_but_keeps_good_ones() {
        let good = sample_bin();
        let map = HashMap::from([
            (bin_field(&good.key()), serde_json::to_string(&good).unwrap()),
            ("0:0".to_string(), "{not valid json".to_string()),
            ("1:1".to_string(), "{}".to_string()), // valid json, wrong shape
        ]);
        let got = parse_bins(map);
        assert_eq!(got.len(), 1, "only the well-formed bin is restored");
        assert_eq!(got[0].meter_serial, "MTR-001");
    }

    #[test]
    fn bins_written_without_tou_fields_still_load() {
        // A bin persisted by an older binary (no TOU/demand fields) must still
        // deserialize via serde(default) — crash recovery across an upgrade.
        let json = r#"{
            "meter_id":"00000000-0000-0000-0000-000000001234",
            "user_id":"00000000-0000-0000-0000-000000009abc",
            "meter_serial":"MTR-OLD",
            "start_time":"2026-01-01T10:00:00Z",
            "end_time":"2026-01-01T10:15:00Z",
            "energy_generated":"5.0",
            "energy_consumed":"2.0",
            "reading_count":3
        }"#;
        let map = HashMap::from([("00000000-0000-0000-0000-000000001234:1767261600000".to_string(), json.to_string())]);
        let got = parse_bins(map);
        assert_eq!(got.len(), 1, "legacy bin loads via serde(default)");
        assert_eq!(got[0].meter_serial, "MTR-OLD");
        assert_eq!(got[0].max_demand_kw, Decimal::ZERO, "missing field defaults");
        assert_eq!(got[0].net_surplus_kwh(), Some(3.0));
    }
}
