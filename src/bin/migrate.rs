//! Dedicated one-shot migrate job for the shared `gridtokenx_meter` database.
//!
//! `gridtokenx_meter` is shared by meter-service + aggregator (one metering
//! bounded context, one DB — see `docs/db-split-phase2.md` §5 and the parent
//! `docs/design-docs/db-per-service-migration.md` §3.5). A single `_sqlx_migrations`
//! ledger means it must have a SINGLE migration authority: if both services ran
//! `sqlx::migrate!` at boot they would each see the other's migrations as
//! "applied but missing locally" and crash-loop. So NEITHER service migrates at
//! boot — this standalone binary owns applying the full metering schema, run as an
//! init container / CI step / the `_migrate` role, then exits.
//!
//! ```bash
//! METER_DATABASE_URL=postgres://…/gridtokenx_meter cargo run --bin migrate
//! # falls back to DATABASE_URL when METER_DATABASE_URL is unset
//! ```
//!
//! Applies `aggregator_persistence::infra::db::MIGRATOR` (the full `migrations/`
//! set). Idempotent — already-applied versions are skipped. Exit 0 on success,
//! non-zero (with the error) on failure, so a job runner can gate on it.

use aggregator_api::infra::db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("METER_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| {
            anyhow::anyhow!(
                "set METER_DATABASE_URL (or DATABASE_URL) to the gridtokenx_meter database"
            )
        })?;

    eprintln!("→ applying gridtokenx_meter migrations…");
    let pool = db::connect_pool(&url).await?;
    db::run_migrations(&pool).await?;
    eprintln!("✅ gridtokenx_meter migrations applied");
    Ok(())
}
