//! Durable billing-bin store (crash recovery for in-flight energy).
//!
//! The `Aggregator` accumulates energy into per-(meter, 15-min-window) bins in
//! RAM only. A crash/restart between bin creation and settlement would lose every
//! unsettled kWh — and therefore the GRID those kWh should mint. This store
//! mirrors each bin into a Redis hash so the aggregator can `rehydrate` on boot.
//!
//! Lives in the api crate (the async edge) on purpose: `aggregator-logic` is
//! sync-core and must not own a Redis handle. Write-through happens after the
//! aggregator lock is released (no Redis I/O under the mutex).

use crate::aggregator::{BillingBin, BinKey};
use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tracing::{debug, warn};

/// Redis hash holding all unsettled billing bins. Field = `{meter_id}:{start_ms}`.
const BINS_HASH: &str = "gridtokenx:settlement:bins";

/// Cloneable handle to the durable bin store. `ConnectionManager` auto-reconnects
/// internally, so a Redis blip degrades to a warn rather than a freeze.
#[derive(Clone)]
pub struct BinStore {
    conn: ConnectionManager,
}

impl BinStore {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// Stable Redis field for a bin key: `{meter_id}:{window_start_ms}`.
    fn field(key: &BinKey) -> String {
        format!("{}:{}", key.0, key.1.timestamp_millis())
    }

    /// Write-through a single updated bin. Best-effort: a failure means this bin is
    /// in-memory only and at-risk on restart — caller logs loud, never fatal.
    pub async fn persist(&self, bin: &BillingBin) -> Result<()> {
        let field = Self::field(&bin.key());
        let value = serde_json::to_string(bin).context("serialize BillingBin")?;
        let mut conn = self.conn.clone();
        conn.hset::<_, _, _, ()>(BINS_HASH, &field, value)
            .await
            .context("HSET billing bin")?;
        debug!("💾 Persisted billing bin {}", field);
        Ok(())
    }

    /// Evict settled bins from the durable store (called only after the mint is
    /// confirmed submitted, so we never drop unsettled energy).
    pub async fn remove(&self, keys: &[BinKey]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let fields: Vec<String> = keys.iter().map(Self::field).collect();
        let mut conn = self.conn.clone();
        conn.hdel::<_, _, ()>(BINS_HASH, fields)
            .await
            .context("HDEL settled bins")?;
        Ok(())
    }

    /// Load all persisted bins on boot. Corrupt entries are skipped (logged), never
    /// fatal — a partial restore beats refusing to start.
    pub async fn load_all(&self) -> Result<Vec<BillingBin>> {
        let mut conn = self.conn.clone();
        let raw: std::collections::HashMap<String, String> = conn
            .hgetall(BINS_HASH)
            .await
            .context("HGETALL billing bins")?;

        let mut bins = Vec::with_capacity(raw.len());
        for (field, value) in raw {
            match serde_json::from_str::<BillingBin>(&value) {
                Ok(bin) => bins.push(bin),
                Err(e) => warn!("⚠️ Skipping corrupt persisted bin {}: {}", field, e),
            }
        }
        Ok(bins)
    }
}
