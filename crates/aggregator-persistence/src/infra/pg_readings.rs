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
}

impl PgReadingsWriter {
    /// Start the sink when `AGGREGATOR_PG_READINGS=true` and a pool is present.
    ///
    /// Returns `None` when the flag is unset/false or no pool is available —
    /// disabled-by-default, matching the InfluxDB sink. The pool is cloned from
    /// the one the `MeterRegistry` already uses (`PgPool` is an `Arc` internally).
    pub fn start(pool: Option<PgPool>) -> Option<Self> {
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
        tokio::spawn(run_writer(pool, rx));
        info!("🗃️ Postgres meter_readings sink enabled (dashboard Recent Readings)");
        Some(Self { tx })
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
async fn run_writer(pool: PgPool, mut rx: mpsc::Receiver<ReadingRow>) {
    let mut batch: Vec<ReadingRow> = Vec::with_capacity(BATCH_SIZE);
    let mut ticker = interval(FLUSH_INTERVAL);

    loop {
        tokio::select! {
            maybe_row = rx.recv() => {
                match maybe_row {
                    Some(row) => {
                        batch.push(row);
                        if batch.len() >= BATCH_SIZE {
                            flush(&pool, &mut batch).await;
                        }
                    }
                    None => {
                        flush(&pool, &mut batch).await;
                        info!("meter_readings sink channel closed; writer task exiting");
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&pool, &mut batch).await;
                }
            }
        }
    }
}

/// Insert the batch in one round-trip and always drain it, logging (not
/// propagating) a failure so a transient DB fault never kills the sink.
async fn flush(pool: &PgPool, batch: &mut Vec<ReadingRow>) {
    if let Err(e) = insert_batch(pool, batch).await {
        error!("meter_readings batch insert failed for {} row(s) ({e})", batch.len());
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
async fn insert_batch(pool: &PgPool, rows: &[ReadingRow]) -> Result<(), sqlx::Error> {
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

    // TODO(db-split): this INSERT…SELECT JOINs IAM-owned `users` (u.wallet_address)
    // to fill wallet_address — a cross-domain read that DB-per-service forbids
    // (Phase 2, docs/db-split-phase2.md). Once cutover to gridtokenx_meter lands,
    // resolve wallet from the local `meter_owner_read_model` table (or pass the
    // already-resolved wallet down from MeterRegistry) and drop the `JOIN users u`.
    // Do NOT remove the JOIN until the read-model is populated + verified.
    sqlx::query(
        r#"
        INSERT INTO meter_readings
            (id, meter_serial, meter_id, user_id, wallet_address, timestamp,
             energy_generated, energy_consumed, surplus_energy, deficit_energy, kwh_amount,
             voltage, power_factor, frequency, verification_status, reading_timestamp)
        SELECT gen_random_uuid(), m.serial_number, NULL::uuid, m.user_id, u.wallet_address,
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
        WHERE u.wallet_address IS NOT NULL
        "#,
    )
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
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::canonicalize_serial;

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
