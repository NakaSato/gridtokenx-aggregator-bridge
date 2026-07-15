//! Aggregator's own metering DB — pool + embedded migration runner.
//!
//! Part of DB-per-service Phase 2 (`docs/db-split-phase2.md` §5 step 2). The
//! metering domain owns the dedicated `gridtokenx_meter` database; this module
//! connects it and applies the `migrations/` set at boot, so the aggregator no
//! longer depends on migrations being run out-of-band.
//!
//! **Only ever point this at the dedicated metering DB** (via `METER_DATABASE_URL`),
//! never the shared platform `DATABASE_URL`: these migrations `CREATE` tables that
//! IAM already owns on the shared DB, and running them there would collide with
//! IAM's own `_sqlx_migrations` ledger (the image/DB-drift crash-loop). The
//! dedicated DB carries its own private ledger, decoupled from every other service.

use std::time::Duration;

use sqlx::migrate::MigrateError;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Migrations embedded at compile time from the service-root `migrations/` dir
/// (relative to this crate's manifest). Same set the doc enumerates: metering
/// functions, meter_registry, partitioned meter_readings, oracle_submissions,
/// grid_status_history, meter_owner_read_model.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Connect the metering pool with the same degrade-safe budget the aggregator
/// uses elsewhere (small pool, short acquire timeout).
pub async fn connect_pool(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(3))
        .connect(url)
        .await
}

/// Apply pending migrations to the dedicated metering DB. Idempotent — applied
/// versions are skipped via the private `_sqlx_migrations` table; sqlx takes an
/// advisory lock during migrate, so concurrent replicas are safe.
pub async fn run_migrations(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}
