//! Live-Postgres check that the hand-extracted metering migrations apply cleanly
//! to a fresh `gridtokenx_meter` (DB-per-service Phase 2). The `migrations/` set
//! was lifted verbatim from a shared-DB `pg_dump` (partitioned `meter_readings`,
//! functions, triggers) — this proves they actually build the schema standalone,
//! and that `run_migrations` is idempotent.
//!
//! Gated on `METER_TEST_DATABASE_URL` — unset ⇒ skip (pass), so the DB-free unit
//! suite stays green. Run against a throwaway PG:
//!
//! ```bash
//! docker run -d --rm --name agg-test-pg -e POSTGRES_PASSWORD=test \
//!   -e POSTGRES_DB=gridtokenx_meter -p 55433:5432 postgres:16-alpine
//! METER_TEST_DATABASE_URL=postgres://postgres:test@localhost:55433/gridtokenx_meter \
//!   cargo test -p aggregator-persistence --test meter_db_migrations -- --nocapture
//! ```

use aggregator_persistence::infra::db;

fn test_url() -> Option<String> {
    std::env::var("METER_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
}

async fn reg_exists(pool: &sqlx::PgPool, qualified: &str) -> bool {
    let oid: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(qualified)
        .fetch_one(pool)
        .await
        .expect("to_regclass query");
    oid.is_some()
}

#[tokio::test]
async fn metering_migrations_apply_and_schema_is_present() {
    let Some(url) = test_url() else {
        eprintln!("SKIP metering_migrations_apply_and_schema_is_present: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = db::connect_pool(&url).await.expect("connect metering DB");

    // Apply — and prove idempotency by running twice (second run skips applied).
    db::run_migrations(&pool).await.expect("migrations apply");
    db::run_migrations(&pool).await.expect("second run is a no-op");

    // Domain tables (0002/0004/0005/0006 + partitioned parent from 0003).
    for t in [
        "meters",
        "meter_registry",
        "meter_verification_attempts",
        "oracle_submissions",
        "grid_status_history",
        "meter_owner_read_model",
        "meter_readings",
        "meter_readings_archive",
    ] {
        assert!(
            reg_exists(&pool, &format!("public.{t}")).await,
            "table {t} must exist after migrate"
        );
    }

    // A monthly range partition + the DEFAULT catch-all partition (0003).
    for p in ["meter_readings_2026_07", "meter_readings_default"] {
        assert!(
            reg_exists(&pool, &format!("public.{p}")).await,
            "partition {p} must exist"
        );
    }

    // The current ⋃ archive view (0003).
    assert!(
        reg_exists(&pool, "public.meter_readings_all").await,
        "meter_readings_all view must exist"
    );

    // Metering functions (0001).
    for f in [
        "canonicalize_meter_serial",
        "create_meter_readings_partition",
        "archive_old_meter_readings",
    ] {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_proc WHERE proname = $1")
            .bind(f)
            .fetch_one(&pool)
            .await
            .expect("pg_proc query");
        assert!(n >= 1, "function {f} must exist");
    }

    // The Phase-2 owner/wallet read-model shape (0006) — the table that removes
    // the last cross-domain read.
    let cols: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'meter_owner_read_model'",
    )
    .fetch_all(&pool)
    .await
    .expect("columns query");
    for c in ["serial_number", "user_id", "wallet_address", "updated_at"] {
        assert!(
            cols.iter().any(|x| x == c),
            "meter_owner_read_model must have column {c}"
        );
    }

    // canonicalize_meter_serial is used as a write-time normalizer — exercise it.
    let canon: String =
        sqlx::query_scalar("SELECT canonicalize_meter_serial($1)")
            .bind("  Abc-123 ")
            .fetch_one(&pool)
            .await
            .expect("call canonicalize_meter_serial");
    assert!(!canon.is_empty(), "canonicalize_meter_serial returns a value");
}
