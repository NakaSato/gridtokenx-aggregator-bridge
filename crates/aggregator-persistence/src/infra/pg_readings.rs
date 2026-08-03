//! Optional Postgres `meter_readings` sink for the shared gridtokenx schema.
//!
//! The Aggregator Bridge is the ingest authority: it verifies signed telemetry
//! and already resolves each meter's owner (`meters` JOIN `users`) via the
//! [`MeterRegistry`](super::meter_registry). This sink mirrors a disseminated
//! `DeviceReading` into the IAM-owned partitioned `meter_readings` table so the
//! trading UI / meter-service (read-only) can list a meter's Recent Readings —
//! which otherwise stays empty because no other service writes that table.
//!
//! Write path is **async fire-and-forget** (same shape as [`InfluxWriter`]):
//! [`PgReadingsWriter::record`] enqueues a [`ReadingRow`] onto a bounded channel
//! and returns immediately; a background task batches rows and inserts each
//! batch in one round-trip (same size/interval batching as `InfluxWriter`). A
//! slow/down Postgres therefore never blocks the realtime dissemination path —
//! rows are dropped (logged) when the queue is full. Owner + wallet are
//! resolved *inside* the INSERT (`JOIN meters/users ... WHERE wallet IS NOT
//! NULL`), so an unattributed meter simply inserts no row (no error, no
//! orphan) and `wallet_address` (NOT NULL) is always satisfied.
//!
//! Opt-in: enabled only when `AGGREGATOR_PG_READINGS=true` AND a Postgres pool is
//! available; unset ⇒ disabled (returns `None`), exactly like the InfluxDB sink.

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Bounded queue capacity. Once full, new rows are dropped (and logged) rather
/// than applying backpressure to the realtime ingest path.
const CHANNEL_CAPACITY: usize = 10_000;
/// Max rows buffered before a flush is forced. Mirrors `InfluxWriter`.
const BATCH_SIZE: usize = 500;
/// Max time a partial batch waits before being flushed. Mirrors `InfluxWriter`.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Billing/mint aggregation window length in ms — matches the aggregator's
/// 15-minute bin (`aggregator.rs` `WINDOW_MINUTES = 15`). A confirmed surplus
/// mint marks every reading row whose `reading_timestamp` falls in
/// `[window_start_ms, window_start_ms + WINDOW_MS)`.
const WINDOW_MS: i64 = 15 * 60 * 1000;

/// One meter reading destined for the shared `meter_readings` table.
///
/// Defined here (not in `aggregator-core`) so this persistence edge stays free of
/// domain deps; the router maps a `DeviceReading` into this shape. `timestamp_ms`
/// is Unix epoch milliseconds so no `sqlx` `chrono` feature is needed — the SQL
/// converts it with `to_timestamp()`.
#[derive(Debug, Clone)]
pub struct ReadingRow {
    pub serial_number: String,
    pub timestamp_ms: i64,
    pub generated_kwh: f64,
    pub consumed_kwh: f64,
    pub surplus_kwh: f64,
    pub deficit_kwh: f64,
    pub voltage: Option<f64>,
    pub power_factor: Option<f64>,
    pub frequency: Option<f64>,
}

/// Handle to the background Postgres readings-writer task.
#[derive(Clone)]
pub struct PgReadingsWriter {
    tx: mpsc::Sender<ReadingRow>,
    /// Direct handle for the low-volume mint write-back (`mark_minted`), which
    /// UPDATEs confirmed rows synchronously rather than through the insert queue.
    pool: PgPool,
}

impl PgReadingsWriter {
    /// Start the sink when `AGGREGATOR_PG_READINGS=true` and a pool is present.
    ///
    /// Returns `None` when the flag is unset/false or no pool is available —
    /// disabled-by-default, matching the InfluxDB sink. The pool is cloned from
    /// the one the `MeterRegistry` already uses (`PgPool` is an `Arc` internally).
    /// `use_read_model` (the aggregator's `own_meter_db` gate) selects the owner
    /// resolution the batch INSERT uses: the local `meter_owner_read_model` on the
    /// dedicated metering DB, or the legacy `meters ⋈ users` JOIN on the shared DB.
    pub fn start(pool: Option<PgPool>, use_read_model: bool) -> Option<Self> {
        let enabled = std::env::var("AGGREGATOR_PG_READINGS")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        if !enabled {
            info!("ℹ️ Postgres meter_readings sink disabled (AGGREGATOR_PG_READINGS != true)");
            return None;
        }
        let Some(pool) = pool else {
            warn!("⚠️ AGGREGATOR_PG_READINGS=true but no Postgres pool — meter_readings sink disabled");
            return None;
        };

        let (tx, rx) = mpsc::channel::<ReadingRow>(CHANNEL_CAPACITY);
        tokio::spawn(run_writer(pool.clone(), rx, use_read_model));
        info!("🗃️ Postgres meter_readings sink enabled (dashboard Recent Readings)");
        Some(Self { tx, pool })
    }

    /// Write-back a confirmed surplus mint onto the reading rows of its 15-minute
    /// billing window. Flips those rows `pending → minted` for the meter-service's
    /// `MINT_STATUS_CASE` (which derives the status from `minted`/`on_chain_confirmed`)
    /// and surfaces the tx signature/slot that the trading UI's Reading History
    /// renders — without this, a settled on-chain mint never touches `meter_readings`
    /// and every row stays `pending` forever.
    ///
    /// Idempotent: `AND NOT minted` skips rows a prior confirmation already marked,
    /// so a mint retry that re-confirms the same window is a no-op. Awaited (not
    /// queued through the insert channel): confirmed mints are low-volume (one per
    /// meter per 15-minute window). Best-effort at the call site — the caller logs a
    /// DB error and never fails the mint, since the on-chain token has already moved.
    /// Returns the number of reading rows marked.
    pub async fn mark_minted(
        &self,
        meter_serial: &str,
        window_start_ms: i64,
        tx_signature: &str,
        slot: u64,
    ) -> Result<u64, sqlx::Error> {
        // Canonicalize the same way the insert path does, so the UPDATE's
        // `meter_serial =` predicate matches the rows this window wrote.
        let serial = canonicalize_serial(meter_serial);
        let window_end_ms = window_start_ms + WINDOW_MS;
        let res = sqlx::query(
            "UPDATE meter_readings
                SET minted = true,
                    on_chain_confirmed = true,
                    on_chain_confirmed_at = now(),
                    on_chain_slot = $1,
                    mint_tx_signature = $2,
                    blockchain_status = 'confirmed'
              WHERE meter_serial = $3
                AND reading_timestamp >= to_timestamp($4::double precision / 1000.0)
                AND reading_timestamp <  to_timestamp($5::double precision / 1000.0)
                AND NOT COALESCE(minted, false)",
        )
        .bind(slot as i64) // on_chain_slot is bigint; Solana slots fit in i64
        .bind(tx_signature)
        .bind(&serial)
        .bind(window_start_ms)
        .bind(window_end_ms)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Write-back a completed bin that closed with **no surplus** (net
    /// consumption/zero) — the sibling of [`Self::mark_minted`] for a
    /// `MintDecision::NoSurplus` bin. Without this, a reading whose billing
    /// window never generates a surplus has no code path that ever leaves
    /// `blockchain_status = 'pending'`, so `MINT_STATUS_CASE` shows it as
    /// "Pending" in the trading UI forever even though nothing was ever going
    /// to mint for it.
    ///
    /// Only touches rows still at the default `'pending'` status, so it never
    /// clobbers a `'confirmed'` (minted) or `'failed'` (denied) row — same
    /// idempotency shape as `mark_minted`'s `AND NOT minted`. Best-effort: the
    /// caller logs a DB error and moves on, since the bin is evicted either way.
    pub async fn mark_no_surplus(
        &self,
        meter_serial: &str,
        window_start_ms: i64,
    ) -> Result<u64, sqlx::Error> {
        let serial = canonicalize_serial(meter_serial);
        let window_end_ms = window_start_ms + WINDOW_MS;
        let res = sqlx::query(
            "UPDATE meter_readings
                SET blockchain_status = 'no_surplus'
              WHERE meter_serial = $1
                AND reading_timestamp >= to_timestamp($2::double precision / 1000.0)
                AND reading_timestamp <  to_timestamp($3::double precision / 1000.0)
                AND NOT COALESCE(minted, false)
                AND blockchain_status = 'pending'",
        )
        .bind(&serial)
        .bind(window_start_ms)
        .bind(window_end_ms)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Enqueue a reading for insertion. Fire-and-forget: never blocks, drops
    /// (with a `warn!`) when the queue is full so ingest is never back-pressured.
    pub fn record(&self, row: ReadingRow) {
        if let Err(e) = self.tx.try_send(row) {
            warn!("⚠️ meter_readings sink queue full/closed; dropping reading ({e})");
        }
    }
}

/// Background batcher: drains the queue, flushing on size or interval — same
/// shape as `InfluxWriter::run_writer`. A batch is always cleared after an
/// attempt (success or failure) so one bad batch can't wedge the writer.
async fn run_writer(pool: PgPool, mut rx: mpsc::Receiver<ReadingRow>, use_read_model: bool) {
    let mut batch: Vec<ReadingRow> = Vec::with_capacity(BATCH_SIZE);
    let mut ticker = interval(FLUSH_INTERVAL);

    loop {
        tokio::select! {
            maybe_row = rx.recv() => {
                match maybe_row {
                    Some(row) => {
                        batch.push(row);
                        if batch.len() >= BATCH_SIZE {
                            flush(&pool, &mut batch, use_read_model).await;
                        }
                    }
                    None => {
                        flush(&pool, &mut batch, use_read_model).await;
                        info!("meter_readings sink channel closed; writer task exiting");
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&pool, &mut batch, use_read_model).await;
                }
            }
        }
    }
}

/// Insert the batch in one round-trip and always drain it, logging (not
/// propagating) a failure so a transient DB fault never kills the sink.
async fn flush(pool: &PgPool, batch: &mut Vec<ReadingRow>, use_read_model: bool) {
    if let Err(e) = insert_batch(pool, batch, use_read_model).await {
        error!(
            "meter_readings batch insert failed for {} row(s) ({e})",
            batch.len()
        );
    }
    batch.clear();
}

/// Canonicalize a telemetry serial to the form `meters.serial_number` stores.
///
/// meter-service persists UUID serials in the canonical hyphenated-lowercase
/// form (its own `canonicalize_serial`, commit `bcf8cf9`). A device that emits
/// the same UUID in the 32-hex undashed or upper-case form would then miss the
/// exact-match `JOIN meters ON serial_number = t.serial_number` below, and its
/// reading would be silently dropped (unattributed) or split onto a stale
/// duplicate meter row. Canonicalizing the JOIN key the same way makes any UUID
/// dash/case variant resolve to the single canonical meter row. Non-UUID serials
/// pass through trimmed, unchanged.
fn canonicalize_serial(raw: &str) -> String {
    let trimmed = raw.trim();
    Uuid::parse_str(trimmed).map_or_else(|_| trimmed.to_string(), |u| u.to_string())
}

/// Insert a batch of readings, resolving `user_id`/`wallet_address` per row
/// from the shared `meters` ⋈ `users` tables via `UNNEST` — one wire
/// round-trip for the whole batch. A row is skipped (no insert, no error)
/// when its serial is unknown or its owner has no wallet (keeps the NOT NULL
/// `wallet_address` satisfied and never orphans an unattributed reading).
/// `meter_id` is left NULL: its FK points at the dormant `meter_registry`
/// table (not the meter-service `meters` table), and the dashboard
/// identifies readings by `meter_serial`/`user_id`.
async fn insert_batch(
    pool: &PgPool,
    rows: &[ReadingRow],
    use_read_model: bool,
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let serial_numbers: Vec<String> = rows
        .iter()
        .map(|r| canonicalize_serial(&r.serial_number))
        .collect();
    let timestamps_ms: Vec<i64> = rows.iter().map(|r| r.timestamp_ms).collect();
    let generated_kwh: Vec<f64> = rows.iter().map(|r| r.generated_kwh).collect();
    let consumed_kwh: Vec<f64> = rows.iter().map(|r| r.consumed_kwh).collect();
    let surplus_kwh: Vec<f64> = rows.iter().map(|r| r.surplus_kwh).collect();
    let deficit_kwh: Vec<f64> = rows.iter().map(|r| r.deficit_kwh).collect();
    let voltage: Vec<Option<f64>> = rows.iter().map(|r| r.voltage).collect();
    let power_factor: Vec<Option<f64>> = rows.iter().map(|r| r.power_factor).collect();
    let frequency: Vec<Option<f64>> = rows.iter().map(|r| r.frequency).collect();

    // DB-per-service Phase 2 (docs/db-split-phase2.md §3): on the dedicated metering
    // DB resolve `(user_id, wallet_address)` from the local `meter_owner_read_model`
    // (no cross-domain read, recommendation B — drops BOTH the `meters` and `users`
    // JOINs); on the legacy shared DB keep the `meters ⋈ users` JOIN until the
    // read-model is populated + verified (plan forbids deleting the source read).
    // The SELECT column list + UNNEST binds are identical; only the owner JOIN differs.
    let owner_join = if use_read_model {
        "SELECT gen_random_uuid(), t.serial_number, NULL::uuid, rm.user_id, rm.wallet_address,
               to_timestamp(t.timestamp_ms::double precision / 1000.0),
               t.generated_kwh, t.consumed_kwh, t.surplus_kwh, t.deficit_kwh, t.generated_kwh,
               t.voltage, t.power_factor, t.frequency, 'verified',
               to_timestamp(t.timestamp_ms::double precision / 1000.0)
        FROM UNNEST($1::text[], $2::bigint[], $3::float8[], $4::float8[], $5::float8[],
                     $6::float8[], $7::float8[], $8::float8[], $9::float8[])
             AS t(serial_number, timestamp_ms, generated_kwh, consumed_kwh, surplus_kwh,
                  deficit_kwh, voltage, power_factor, frequency)
        JOIN meter_owner_read_model rm ON rm.serial_number = t.serial_number
        WHERE rm.wallet_address IS NOT NULL"
    } else {
        "SELECT gen_random_uuid(), m.serial_number, NULL::uuid, m.user_id, u.wallet_address,
               to_timestamp(t.timestamp_ms::double precision / 1000.0),
               t.generated_kwh, t.consumed_kwh, t.surplus_kwh, t.deficit_kwh, t.generated_kwh,
               t.voltage, t.power_factor, t.frequency, 'verified',
               to_timestamp(t.timestamp_ms::double precision / 1000.0)
        FROM UNNEST($1::text[], $2::bigint[], $3::float8[], $4::float8[], $5::float8[],
                     $6::float8[], $7::float8[], $8::float8[], $9::float8[])
             AS t(serial_number, timestamp_ms, generated_kwh, consumed_kwh, surplus_kwh,
                  deficit_kwh, voltage, power_factor, frequency)
        JOIN meters m ON m.serial_number = t.serial_number
        JOIN users u ON u.id = m.user_id
        WHERE u.wallet_address IS NOT NULL"
    };
    let sql = format!(
        "INSERT INTO meter_readings
            (id, meter_serial, meter_id, user_id, wallet_address, timestamp,
             energy_generated, energy_consumed, surplus_energy, deficit_energy, kwh_amount,
             voltage, power_factor, frequency, verification_status, reading_timestamp)
        {owner_join}"
    );
    let submitted = serial_numbers.len();
    let res = sqlx::query(&sql)
        .bind(&serial_numbers)
        .bind(&timestamps_ms)
        .bind(&generated_kwh)
        .bind(&consumed_kwh)
        .bind(&surplus_kwh)
        .bind(&deficit_kwh)
        .bind(&voltage)
        .bind(&power_factor)
        .bind(&frequency)
        .execute(pool)
        .await?;

    // The owner join keeps only rows whose meter resolves to a wallet, so anything
    // missing from `rows_affected` was DROPPED — not stored, not counted, invisible.
    // Surface it as a counter rather than a log: the condition lasts until someone
    // registers the meter, so logging it per batch is exactly the noise the mint
    // drain had to be fixed for. `debug!` keeps a breadcrumb for a live session.
    let dropped = unattributed_count(submitted, res.rows_affected());
    if dropped > 0 {
        crate::metrics::record_unattributed_readings(dropped);
        tracing::debug!(
            "meter_readings: {dropped} of {submitted} readings had no owner row and were not persisted"
        );
    }
    Ok(())
}

/// Readings submitted minus readings actually written. Saturating: a driver that
/// ever reported more affected rows than submitted must read as "none dropped"
/// rather than underflow into a huge bogus count.
fn unattributed_count(submitted: usize, inserted: u64) -> u64 {
    (submitted as u64).saturating_sub(inserted)
}

#[cfg(test)]
mod tests {
    use super::{canonicalize_serial, unattributed_count};

    /// Every submitted reading that the owner join did not keep was dropped.
    #[test]
    fn unattributed_count_is_submitted_minus_inserted() {
        assert_eq!(
            unattributed_count(8, 8),
            0,
            "all attributed ⇒ nothing dropped"
        );
        assert_eq!(unattributed_count(8, 3), 5);
        assert_eq!(
            unattributed_count(8, 0),
            8,
            "no owner rows at all ⇒ every reading dropped"
        );
        assert_eq!(unattributed_count(0, 0), 0, "empty batch");
    }

    /// Saturating on purpose: an impossible driver answer must read as "none
    /// dropped", never underflow into a huge bogus counter increment.
    #[test]
    fn unattributed_count_saturates_instead_of_underflowing() {
        assert_eq!(unattributed_count(3, 9), 0);
    }

    #[test]
    fn canonicalizes_undashed_uppercase_uuid_to_meters_form() {
        // Device emits 32-hex undashed; meter row stores hyphenated-lowercase.
        // Both must produce the same JOIN key or the reading is dropped.
        assert_eq!(
            canonicalize_serial("  3EB13B9046684257BDD640FB06671AD1  "),
            "3eb13b90-4668-4257-bdd6-40fb06671ad1"
        );
    }

    #[test]
    fn leaves_canonical_uuid_untouched() {
        assert_eq!(
            canonicalize_serial("3eb13b90-4668-4257-bdd6-40fb06671ad1"),
            "3eb13b90-4668-4257-bdd6-40fb06671ad1"
        );
    }

    #[test]
    fn non_uuid_serial_passes_through_trimmed() {
        assert_eq!(canonicalize_serial("  METER-XYZ-1  "), "METER-XYZ-1");
    }
}
