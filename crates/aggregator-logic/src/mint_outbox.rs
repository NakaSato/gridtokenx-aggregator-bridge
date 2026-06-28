//! Durable mint outbox (Redis) — guarantees a settled surplus is **never lost**
//! when it can't be minted on-chain yet (Chain Bridge down, validator down, a
//! sim rejection like `Custom(6000)`, a dropped reply).
//!
//! The settlement loop, instead of fire-and-forget minting and evicting the bin,
//! **enqueues** a [`PendingMint`] here (durable) and only removes it once the
//! mint is *confirmed* on-chain. A drain loop retries every sweep; a restart
//! reloads the outbox from Redis. Retries are safe because the mint is
//! idempotent: the bridge dedups on the key `mint:{serial}:{window_start_ms}`
//! and the on-chain `(meter_id, window_start_ms)` PDA is the ultimate backstop —
//! so a retry of a mint that actually *did* land (but whose reply was lost) does
//! not double-mint.
//!
//! Async edge by design (sync-core / async-edges): pure billing logic never
//! touches Redis. Degrade-safe: every method returns `Err` on a Redis fault so
//! the caller can fall back to a best-effort immediate mint and `warn!` rather
//! than treating the fault as fatal. The [`ConnectionManager`] self-reconnects.

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;
use uuid::Uuid;

/// Redis hash holding every unsettled mint: field = [`PendingMint::field`],
/// value = JSON. Distinct from the billing-bin hash — a bin is evicted once
/// settled, but its mint lives here until confirmed on-chain.
const OUTBOX_KEY: &str = "gridtokenx:billing:mint_outbox";

/// A surplus mint that has been settled out of a billing bin but not yet
/// confirmed on-chain. Carries everything needed to (re)attempt the mint; the
/// recipient wallet is intentionally **not** stored — it is resolved fresh at
/// each attempt, so a meter that registers its wallet *after* the window still
/// mints on a later retry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PendingMint {
    pub meter_serial: String,
    pub meter_id: Uuid,
    pub window_start_ms: i64,
    pub energy_kwh: f64,
    /// When this entry was first enqueued (epoch ms), for age/observability.
    pub enqueued_at_ms: i64,
}

impl PendingMint {
    /// Builds a pending mint, stamping `enqueued_at_ms` with the service clock
    /// (NTP-backed `gridtokenx_telemetry::time::now`, the project's wall-clock).
    #[must_use]
    pub fn new(meter_serial: String, meter_id: Uuid, window_start_ms: i64, energy_kwh: f64) -> Self {
        Self {
            meter_serial,
            meter_id,
            window_start_ms,
            energy_kwh,
            enqueued_at_ms: gridtokenx_telemetry::time::now().timestamp_millis(),
        }
    }

    /// Durable field key: `"{meter_serial}:{window_start_ms}"` — the same
    /// `(meter, window)` identity the bridge dedups on, so re-enqueuing the same
    /// window overwrites rather than duplicates.
    pub fn field(&self) -> String {
        format!("{}:{}", self.meter_serial, self.window_start_ms)
    }
}

/// Decodes an `HGETALL` map into pending mints, skipping (with a `warn!`) any
/// unparsable value. Pure — unit-testable without a live Redis.
fn parse_pending(map: HashMap<String, String>) -> Vec<PendingMint> {
    map.into_iter()
        .filter_map(|(field, json)| match serde_json::from_str::<PendingMint>(&json) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!("mint outbox: dropping unparsable entry {field}: {e}");
                None
            }
        })
        .collect()
}

/// Redis-backed durable outbox of unsettled surplus mints.
#[derive(Clone)]
pub struct MintOutbox {
    conn: ConnectionManager,
}

impl MintOutbox {
    /// Wraps an already-connected Redis connection manager.
    #[must_use]
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// Persist a pending mint (overwrites the same `(meter, window)` field).
    ///
    /// # Errors
    /// Returns an error if the Redis `HSET` fails (caller should fall back to a
    /// best-effort immediate mint and `warn!`).
    pub async fn enqueue(&self, pending: &PendingMint) -> Result<()> {
        let json = serde_json::to_string(pending).context("serialize pending mint")?;
        let mut conn = self.conn.clone();
        let _: () = conn
            .hset(OUTBOX_KEY, pending.field(), json)
            .await
            .context("HSET pending mint")?;
        Ok(())
    }

    /// Remove a pending mint once its on-chain mint is confirmed.
    ///
    /// # Errors
    /// Returns an error if the Redis `HDEL` fails (a stale entry only causes one
    /// idempotent retry, which the bridge dedups — never a double-mint).
    pub async fn remove(&self, field: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: () = conn
            .hdel(OUTBOX_KEY, field)
            .await
            .context("HDEL pending mint")?;
        Ok(())
    }

    /// Load every unsettled mint (drain loop + crash recovery). Unparsable
    /// entries are skipped with a `warn!`.
    ///
    /// # Errors
    /// Returns an error if the Redis `HGETALL` fails.
    pub async fn load_all(&self) -> Result<Vec<PendingMint>> {
        let mut conn = self.conn.clone();
        let map: HashMap<String, String> = conn
            .hgetall(OUTBOX_KEY)
            .await
            .context("HGETALL mint outbox")?;
        Ok(parse_pending(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending() -> PendingMint {
        PendingMint {
            meter_serial: "MTR-001".to_string(),
            meter_id: Uuid::from_u128(0x1234_5678),
            window_start_ms: 1_767_261_600_000,
            energy_kwh: 8.5,
            enqueued_at_ms: 1_767_261_600_123,
        }
    }

    #[test]
    fn field_is_serial_and_window() {
        let p = pending();
        assert_eq!(p.field(), "MTR-001:1767261600000");
    }

    #[test]
    fn enqueue_value_roundtrips_via_load() {
        // Mirrors enqueue()->load_all(): JSON encode, then parse_pending decodes.
        let p = pending();
        let map = HashMap::from([(p.field(), serde_json::to_string(&p).unwrap())]);
        let got = parse_pending(map);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], p, "all fields survive the roundtrip incl. f64 kwh");
    }

    #[test]
    fn ugly_surplus_float_survives_roundtrip() {
        // Real billing deltas are ugly floats (0.06069600000000001). The retry
        // path must preserve the exact value so the on-chain amount matches.
        let mut p = pending();
        p.energy_kwh = 0.060_696_000_000_000_01;
        let map = HashMap::from([(p.field(), serde_json::to_string(&p).unwrap())]);
        let got = parse_pending(map);
        assert_eq!(got[0].energy_kwh, p.energy_kwh);
    }

    #[test]
    fn load_skips_unparsable_but_keeps_good() {
        let good = pending();
        let map = HashMap::from([
            (good.field(), serde_json::to_string(&good).unwrap()),
            ("bad:1".to_string(), "{not json".to_string()),
        ]);
        let got = parse_pending(map);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].meter_serial, "MTR-001");
    }

    #[test]
    fn same_window_reenqueue_uses_same_field() {
        // Re-enqueuing the same (meter, window) must target the same field so the
        // outbox holds at most one entry per window (no duplicate mint intents).
        let p1 = pending();
        let mut p2 = pending();
        p2.energy_kwh = 9.9; // a corrected amount, same window
        assert_eq!(p1.field(), p2.field());
    }
}
