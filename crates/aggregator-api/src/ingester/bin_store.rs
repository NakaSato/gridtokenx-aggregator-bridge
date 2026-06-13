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

/// Redis set of bin fields whose mint was already submitted to Chain Bridge but not
/// yet evicted. It guards the crash window between a successful submit and eviction:
/// on restart, a bin present here is treated as already minted and is NOT rehydrated,
/// so its mint is never replayed. Members use the same `{meter_id}:{start_ms}` field.
const MINTED_SET: &str = "gridtokenx:settlement:minted";

/// Dead-letter hash for bins that failed to deserialize on rehydrate. A corrupt entry
/// is unsettled energy we cannot parse, so we never silently drop it — we copy the raw
/// value here (for manual recovery) and remove it from [`BINS_HASH`] so it stops being
/// re-read and re-logged on every boot. Field = the original `{meter_id}:{start_ms}`.
const CORRUPT_HASH: &str = "gridtokenx:settlement:bins:corrupt";

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

    /// Mark bins as minted-but-not-yet-evicted. Written after a successful Chain Bridge
    /// submit and before eviction so a crash in that window does not replay the mint on
    /// rehydrate. Best-effort at the call site: a failed mark only shrinks the guard.
    pub async fn mark_minted(&self, keys: &[BinKey]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let fields: Vec<String> = keys.iter().map(Self::field).collect();
        let mut conn = self.conn.clone();
        conn.sadd::<_, _, ()>(MINTED_SET, fields)
            .await
            .context("SADD minted markers")?;
        Ok(())
    }

    /// Evict settled bins from the durable store (called only after the mint is
    /// confirmed submitted, so we never drop unsettled energy). Also clears the replay
    /// marker for the same bins; a leftover marker is harmless (`load_all` prunes
    /// orphans) but dropping it here keeps the set bounded.
    pub async fn remove(&self, keys: &[BinKey]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let fields: Vec<String> = keys.iter().map(Self::field).collect();
        let mut conn = self.conn.clone();
        conn.hdel::<_, _, ()>(BINS_HASH, &fields)
            .await
            .context("HDEL settled bins")?;
        // Best-effort: a stale marker is reconciled on the next boot, so never fail
        // eviction on it.
        let _: std::result::Result<(), _> = conn.srem::<_, _, ()>(MINTED_SET, &fields).await;
        Ok(())
    }

    /// Load persisted bins on boot, skipping any already minted (crash-window replay
    /// guard) and corrupt entries (logged, never fatal — partial restore beats refusing
    /// to start). A bin present in [`MINTED_SET`] was submitted to Chain Bridge before
    /// the crash; rehydrating it would replay the mint, so it is dropped and its hash +
    /// marker entries are reconciled away. Orphan markers (bin already evicted, `SREM`
    /// lost to a crash) are pruned in the same pass to keep the set bounded.
    pub async fn load_all(&self) -> Result<Vec<BillingBin>> {
        let mut conn = self.conn.clone();
        let raw: std::collections::HashMap<String, String> = conn
            .hgetall(BINS_HASH)
            .await
            .context("HGETALL billing bins")?;
        // Fail closed: if the marker set is unreadable we must NOT rehydrate, because
        // rehydrating an already-minted bin replays an irreversible on-chain mint.
        // Erroring here just skips this boot's restore (main.rs logs and starts empty);
        // the bins stay in the hash and are restored on the next boot once Redis is
        // healthy — delayed retry beats double-mint.
        let minted: std::collections::HashSet<String> = conn
            .smembers(MINTED_SET)
            .await
            .context("SMEMBERS minted markers")?;

        let mut bins = Vec::with_capacity(raw.len());
        let mut to_clean: Vec<String> = Vec::new();
        // Corrupt bins to dead-letter: (field, raw value) preserved before deletion.
        let mut to_quarantine: Vec<(String, String)> = Vec::new();
        for (field, value) in &raw {
            if minted.contains(field) {
                // Minted before the crash, never evicted — do not replay.
                to_clean.push(field.clone());
                continue;
            }
            match serde_json::from_str::<BillingBin>(value) {
                Ok(bin) => bins.push(bin),
                Err(e) => {
                    warn!(
                        "⚠️ Quarantining corrupt persisted bin {} (unsettled energy preserved \
                         in {}): {}",
                        field, CORRUPT_HASH, e
                    );
                    to_quarantine.push((field.clone(), value.clone()));
                }
            }
        }
        // Move corrupt bins to the dead-letter hash, then drop them from the live hash so
        // they stop being re-read every boot. Copy-before-delete: if the copy fails we
        // leave them in the live hash for the next boot rather than lose the raw value.
        if !to_quarantine.is_empty() {
            match conn
                .hset_multiple::<_, _, _, ()>(CORRUPT_HASH, &to_quarantine)
                .await
            {
                Ok(()) => {
                    let fields: Vec<&String> = to_quarantine.iter().map(|(f, _)| f).collect();
                    let _: std::result::Result<(), _> =
                        conn.hdel::<_, _, ()>(BINS_HASH, &fields).await;
                }
                Err(e) => warn!(
                    "⚠️ Could not dead-letter {} corrupt bin(s); leaving in live hash for \
                     next boot: {}",
                    to_quarantine.len(),
                    e
                ),
            }
        }
        // Orphan markers: a marker whose bin is already gone from the hash.
        for m in &minted {
            if !raw.contains_key(m) {
                to_clean.push(m.clone());
            }
        }
        if !to_clean.is_empty() {
            warn!(
                "♻️ Reconciling {} already-minted/orphan bin marker(s) on rehydrate",
                to_clean.len()
            );
            let _: std::result::Result<(), _> =
                conn.hdel::<_, _, ()>(BINS_HASH, &to_clean).await;
            let _: std::result::Result<(), _> =
                conn.srem::<_, _, ()>(MINTED_SET, &to_clean).await;
        }
        Ok(bins)
    }
}
