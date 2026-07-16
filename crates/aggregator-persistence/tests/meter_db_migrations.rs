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
use aggregator_persistence::infra::meter_registry::MeterRegistry;

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

/// Phase-2 step 4: with the read-model flag ON, `MeterRegistry` resolves owner +
/// wallet purely from the local `meter_owner_read_model` — no `meters ⋈ users`
/// cross-domain read. Proven by the fact that the legacy JOIN path *cannot* run
/// on this metering DB (it has no IAM `users` table), so a resolve there errors:
/// only the read-model path can succeed.
#[tokio::test]
async fn owner_resolves_from_read_model_without_cross_domain_join() {
    let Some(url) = test_url() else {
        eprintln!("SKIP owner_resolves_from_read_model_without_cross_domain_join: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = db::connect_pool(&url).await.expect("connect");
    db::run_migrations(&pool).await.expect("migrate"); // idempotent

    const SERIAL: &str = "RM-TEST-0001";
    const WALLET: &str = "So1anaWa11etAddr1111111111111111111111111111";
    sqlx::query(
        "INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
         VALUES ($1, '11111111-1111-1111-1111-111111111111'::uuid, $2, now())
         ON CONFLICT (serial_number)
         DO UPDATE SET wallet_address = EXCLUDED.wallet_address, updated_at = now()",
    )
    .bind(SERIAL)
    .bind(WALLET)
    .execute(&pool)
    .await
    .expect("seed read-model");

    // read-model ON: redis None + empty caches ⇒ the Postgres tier reads the
    // local read-model. Resolves owner + wallet with zero cross-domain access.
    let reg = MeterRegistry::new(None, Some(pool.clone())).with_read_model(true);
    assert!(
        reg.resolve_user_id(SERIAL).await.expect("resolve_user_id ok").is_some(),
        "read-model must resolve the owner"
    );
    assert_eq!(
        reg.resolve_wallet(SERIAL).await.expect("resolve_wallet ok").as_deref(),
        Some(WALLET),
        "read-model must resolve the wallet"
    );

    // read-model OFF: the legacy `meters ⋈ users` JOIN references the IAM `users`
    // table, which does not exist on the metering DB → the query errors. This is
    // the proof that the ON case above went through the read-model, not the JOIN.
    let legacy = MeterRegistry::new(None, Some(pool)).with_read_model(false);
    assert!(
        legacy.resolve_user_id(SERIAL).await.is_err(),
        "legacy JOIN path must error on the metering DB (no users table)"
    );
}

/// A UUID serial emitted in a dash/case VARIANT must resolve to the canonical
/// read-model row — the owner lookup matches on `canonicalize_meter_serial($1)`,
/// so a variant can't miss its owner (which would land readings but skip the mint).
#[tokio::test]
async fn read_model_resolves_a_noncanonical_uuid_serial() {
    let Some(url) = test_url() else {
        eprintln!("SKIP read_model_resolves_a_noncanonical_uuid_serial: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = db::connect_pool(&url).await.expect("connect");
    db::run_migrations(&pool).await.expect("migrate");

    const CANON: &str = "22222222-2222-2222-2222-222222222222";
    const UNDASHED: &str = "22222222222222222222222222222222"; // same UUID, no dashes
    const WALLET: &str = "WcanonWallet";
    sqlx::query(
        "INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
         VALUES (canonicalize_meter_serial($1), '33333333-3333-3333-3333-333333333333'::uuid, $2, now())
         ON CONFLICT (serial_number) DO UPDATE SET wallet_address = EXCLUDED.wallet_address, updated_at = now()",
    )
    .bind(CANON)
    .bind(WALLET)
    .execute(&pool)
    .await
    .expect("seed canonical uuid serial");

    // Resolve with the UNDASHED variant (redis None + empty caches ⇒ Postgres tier).
    let reg = MeterRegistry::new(None, Some(pool)).with_read_model(true);
    assert!(
        reg.resolve_user_id(UNDASHED).await.expect("resolve").is_some(),
        "a dash/case-variant UUID serial must resolve to the canonical read-model row"
    );
    assert_eq!(
        reg.resolve_wallet(UNDASHED).await.expect("wallet").as_deref(),
        Some(WALLET)
    );
}
