//! Live-Postgres tests for the owner/wallet read-model WRITE path (Phase 2 step 3
//! feed repo). The `OwnerReadModel` SQL was previously unverified against a real
//! DB ("need live infra"), yet its semantics — last-writer-wins, wallet
//! COALESCE-preserve on meter events, authoritative clear on user events — are
//! exactly what the step-4 read path depends on. These lock that contract.
//!
//! Gated on `METER_TEST_DATABASE_URL` (unset ⇒ skip/pass). Run against a throwaway
//! `gridtokenx_meter` (see `meter_db_migrations.rs` header for the docker one-liner).

use aggregator_persistence::infra::db;
use aggregator_persistence::infra::owner_read_model::OwnerReadModel;
use uuid::Uuid;

fn test_url() -> Option<String> {
    std::env::var("METER_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
}

/// Read back a read-model row by (already-canonical) serial. The test serials are
/// non-UUID with no whitespace, so `canonicalize_meter_serial` is the identity —
/// a plain equality read matches what `upsert_by_serial` canonicalizes on write.
async fn row(pool: &sqlx::PgPool, serial: &str) -> Option<(Uuid, Option<String>)> {
    sqlx::query_as("SELECT user_id, wallet_address FROM meter_owner_read_model WHERE serial_number = $1")
        .bind(serial)
        .fetch_optional(pool)
        .await
        .expect("read row")
}

async fn setup(url: &str, serials: &[&str]) -> sqlx::PgPool {
    let pool = db::connect_pool(url).await.expect("connect");
    db::run_migrations(&pool).await.expect("migrate"); // idempotent
    // Clean this test's serials so re-runs against a reused DB are deterministic.
    for s in serials {
        sqlx::query("DELETE FROM meter_owner_read_model WHERE serial_number = $1")
            .bind(s)
            .execute(&pool)
            .await
            .expect("cleanup");
    }
    pool
}

#[tokio::test]
async fn upsert_by_serial_is_last_writer_wins_and_preserves_wallet() {
    let Some(url) = test_url() else {
        eprintln!("SKIP upsert_by_serial_is_last_writer_wins_and_preserves_wallet: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-1"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let (u1, u2) = (Uuid::new_v4(), Uuid::new_v4());

    // First meter event with a wallet.
    repo.upsert_by_serial("OWNREPO-1", u1, Some("W1")).await.unwrap();
    assert_eq!(row(&pool, "OWNREPO-1").await, Some((u1, Some("W1".to_string()))));

    // A later meter event with NO wallet must NOT blank the existing wallet
    // (COALESCE-preserve): meters often re-register without a wallet.
    repo.upsert_by_serial("OWNREPO-1", u1, None).await.unwrap();
    assert_eq!(
        row(&pool, "OWNREPO-1").await,
        Some((u1, Some("W1".to_string()))),
        "meter event without wallet must preserve the existing wallet"
    );

    // Owner CHANGE (re-registration to a different user): take the event's fresh
    // snapshot — the previous owner's wallet is wrong for the new owner.
    repo.upsert_by_serial("OWNREPO-1", u2, Some("W2")).await.unwrap();
    assert_eq!(row(&pool, "OWNREPO-1").await, Some((u2, Some("W2".to_string()))));
}

#[tokio::test]
async fn stale_meter_redelivery_does_not_regress_iam_wallet() {
    // Fund-misdirection regression: a meter event carries a STALE registration-time
    // wallet snapshot (never re-emitted). A Kafka redelivery of the old
    // MeterRegistered after IAM moved the primary must NOT revert the read-model
    // wallet — else the surplus mint credits the wallet the user moved away from.
    let Some(url) = test_url() else {
        eprintln!("SKIP stale_meter_redelivery_does_not_regress_iam_wallet: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-REG"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let owner = Uuid::new_v4();

    // Meter registered with the owner's then-current wallet W1.
    repo.upsert_by_serial("OWNREPO-REG", owner, Some("W1")).await.unwrap();
    // IAM moves the primary to W2 (the authoritative user-event path).
    assert_eq!(repo.update_wallet_by_user(owner, Some("W2")).await.unwrap(), 1);
    assert_eq!(row(&pool, "OWNREPO-REG").await.unwrap().1.as_deref(), Some("W2"));

    // Kafka redelivers the ORIGINAL MeterRegistered(W1) for the SAME owner.
    repo.upsert_by_serial("OWNREPO-REG", owner, Some("W1")).await.unwrap();
    assert_eq!(
        row(&pool, "OWNREPO-REG").await.unwrap().1.as_deref(),
        Some("W2"),
        "stale meter redelivery must not regress the IAM-set primary wallet"
    );

    // But a first-touch fill (no wallet yet) still works: fresh serial, no IAM event.
    repo.upsert_by_serial("OWNREPO-REG", owner, None).await.unwrap(); // same owner, no wallet ⇒ keep W2
    assert_eq!(row(&pool, "OWNREPO-REG").await.unwrap().1.as_deref(), Some("W2"));
}

#[tokio::test]
async fn update_wallet_by_user_touches_all_owned_and_can_clear() {
    let Some(url) = test_url() else {
        eprintln!("SKIP update_wallet_by_user_touches_all_owned_and_can_clear: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-A", "OWNREPO-B", "OWNREPO-C"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let (owner, other) = (Uuid::new_v4(), Uuid::new_v4());

    // Two serials for `owner`, one for a different user (must stay untouched).
    repo.upsert_by_serial("OWNREPO-A", owner, None).await.unwrap();
    repo.upsert_by_serial("OWNREPO-B", owner, None).await.unwrap();
    repo.upsert_by_serial("OWNREPO-C", other, Some("OTHER")).await.unwrap();

    // An IAM wallet event (keyed by user) fills every serial that user owns.
    let n = repo.update_wallet_by_user(owner, Some("WU")).await.unwrap();
    assert_eq!(n, 2, "wallet event must touch both of the owner's meters");
    assert_eq!(row(&pool, "OWNREPO-A").await.unwrap().1.as_deref(), Some("WU"));
    assert_eq!(row(&pool, "OWNREPO-B").await.unwrap().1.as_deref(), Some("WU"));
    assert_eq!(
        row(&pool, "OWNREPO-C").await.unwrap().1.as_deref(),
        Some("OTHER"),
        "another user's row must be untouched"
    );

    // Authoritative clear: unlinking the wallet sets NULL directly (unlike the
    // serial upsert, which only fills an absent wallet).
    let n2 = repo.update_wallet_by_user(owner, None).await.unwrap();
    assert_eq!(n2, 2);
    assert!(row(&pool, "OWNREPO-A").await.unwrap().1.is_none(), "wallet cleared to NULL");
    assert!(row(&pool, "OWNREPO-B").await.unwrap().1.is_none());
}

#[tokio::test]
async fn clear_wallet_if_matches_only_clears_the_matching_wallet() {
    let Some(url) = test_url() else {
        eprintln!("SKIP clear_wallet_if_matches_only_clears_the_matching_wallet: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-CLR1", "OWNREPO-CLR2"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let owner = Uuid::new_v4();

    repo.upsert_by_serial("OWNREPO-CLR1", owner, Some("WALLET-A")).await.unwrap();
    repo.upsert_by_serial("OWNREPO-CLR2", owner, Some("WALLET-A")).await.unwrap();

    // Unlinking a DIFFERENT wallet than the stored one is a no-op (a non-primary
    // wallet the read-model never held).
    assert_eq!(repo.clear_wallet_if_matches(owner, "WALLET-OTHER").await.unwrap(), 0);
    assert_eq!(row(&pool, "OWNREPO-CLR1").await.unwrap().1.as_deref(), Some("WALLET-A"));

    // Unlinking the STORED (mint-recipient) wallet clears it on every row the user owns.
    assert_eq!(repo.clear_wallet_if_matches(owner, "WALLET-A").await.unwrap(), 2);
    assert!(row(&pool, "OWNREPO-CLR1").await.unwrap().1.is_none(), "stored wallet cleared on unlink");
    assert!(row(&pool, "OWNREPO-CLR2").await.unwrap().1.is_none());
}

#[tokio::test]
async fn wallet_event_before_first_meter_event_is_not_lost() {
    // The two feed streams (IAM user events / meter-service meter events) have no
    // cross-topic ordering. A user's wallet event consumed BEFORE their first
    // MeterRegistered row used to match 0 rows and vanish — the read-model row
    // then stayed wallet-NULL forever and surplus mints deferred with
    // "no wallet registered" (observed live 2026-07-18). The durable
    // user_wallet_read_model edge makes the merge order-independent.
    let Some(url) = test_url() else {
        eprintln!("SKIP wallet_event_before_first_meter_event_is_not_lost: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-RACE1", "OWNREPO-RACE2"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let owner = Uuid::new_v4();

    // 1. Wallet event first — user owns no meters yet: 0 meter rows touched,
    //    but the user → wallet edge must be recorded durably.
    let n = repo.update_wallet_by_user(owner, Some("WALLET-EARLY")).await.unwrap();
    assert_eq!(n, 0, "no meter rows yet");

    // 2. Meter event arrives later WITHOUT a wallet snapshot (registration-time
    //    wallet was not provisioned yet) — the upsert must fill from the edge.
    repo.upsert_by_serial("OWNREPO-RACE1", owner, None).await.unwrap();
    assert_eq!(
        row(&pool, "OWNREPO-RACE1").await.unwrap().1.as_deref(),
        Some("WALLET-EARLY"),
        "serial upsert back-fills the wallet from user_wallet_read_model"
    );

    // 3. A meter event that DOES carry a snapshot still wins over the edge for
    //    a fresh row (COALESCE order: event snapshot first).
    repo.upsert_by_serial("OWNREPO-RACE2", owner, Some("WALLET-SNAP")).await.unwrap();
    assert_eq!(row(&pool, "OWNREPO-RACE2").await.unwrap().1.as_deref(), Some("WALLET-SNAP"));

    // 4. Unlink clears BOTH the meter rows and the durable edge — a later meter
    //    event must not resurrect the unlinked wallet.
    repo.update_wallet_by_user(owner, Some("WALLET-EARLY")).await.unwrap(); // realign both rows
    assert_eq!(repo.clear_wallet_if_matches(owner, "WALLET-EARLY").await.unwrap(), 2);
    repo.upsert_by_serial("OWNREPO-RACE1", owner, None).await.unwrap();
    assert!(
        row(&pool, "OWNREPO-RACE1").await.unwrap().1.is_none(),
        "unlinked wallet must not come back from the edge table"
    );
}

#[tokio::test]
async fn backfill_seeds_from_local_meters_when_users_table_absent() {
    // Post-split regression (observed live 2026-07-18): the dedicated metering DB
    // has no IAM `users` table, so the legacy `meters ⋈ users` seed 42P01'd on
    // every boot and pre-cutover meters never entered the read-model. The fallback
    // must seed the serial → user edge from the local `meters` table, taking any
    // wallet the user_wallet_read_model edge already knows.
    let Some(url) = test_url() else {
        eprintln!("SKIP backfill_seeds_from_local_meters_when_users_table_absent: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-BF1", "OWNREPO-BF2"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let (u_walleted, u_bare) = (Uuid::new_v4(), Uuid::new_v4());

    // Precondition: this throwaway DB must be the post-split shape (no `users`),
    // else the legacy join succeeds and the fallback under test never runs.
    let users_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.users') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("regclass probe");
    assert!(
        !users_exists,
        "test DB has a users table — fallback path not exercised; point METER_TEST_DATABASE_URL at a bridge-migrations-only DB"
    );

    for s in ["OWNREPO-BF1", "OWNREPO-BF2"] {
        sqlx::query("DELETE FROM meters WHERE serial_number = $1")
            .bind(s)
            .execute(&pool)
            .await
            .expect("meters cleanup");
    }
    sqlx::query("INSERT INTO meters (user_id, serial_number) VALUES ($1, $2)")
        .bind(u_walleted)
        .bind("OWNREPO-BF1")
        .execute(&pool)
        .await
        .expect("insert meter 1");
    sqlx::query("INSERT INTO meters (user_id, serial_number) VALUES ($1, $2)")
        .bind(u_bare)
        .bind("OWNREPO-BF2")
        .execute(&pool)
        .await
        .expect("insert meter 2");
    // u_walleted already has a durable wallet edge (0 meter rows in the read-model
    // yet, so this writes only user_wallet_read_model); u_bare has none.
    repo.update_wallet_by_user(u_walleted, Some("W-EDGE")).await.unwrap();

    let n = repo.backfill().await.expect("backfill must not error on a users-less DB");
    assert!(n >= 2, "both meters rows must seed (got {n})");
    assert_eq!(
        row(&pool, "OWNREPO-BF1").await,
        Some((u_walleted, Some("W-EDGE".to_string()))),
        "seed must pick up the known wallet edge"
    );
    assert_eq!(
        row(&pool, "OWNREPO-BF2").await,
        Some((u_bare, None)),
        "no edge ⇒ seeded with NULL wallet (repair pass fills it later)"
    );
}

#[tokio::test]
async fn backfill_seeds_the_reverse_user_wallet_edge() {
    // TD-004: meter-service resolves owner wallets through `user_wallet_read_model`
    // ALONE (keyed on its own meters.user_id) — it no longer reads this service's
    // serial-keyed projection. Both backfills only ever flowed serial-ward, so an
    // owner whose wallet arrived via a backfill (not a live IAM event) had a wallet
    // in meter_owner_read_model and NO row in user_wallet_read_model. That owner is
    // invisible to repair_missing_wallets (it only targets NULL wallets), so the
    // hole was permanent and blanked the wallet on meter-service's list/map.
    let Some(url) = test_url() else {
        eprintln!("SKIP backfill_seeds_the_reverse_user_wallet_edge: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-REV1", "OWNREPO-REV2"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let (u_seeded, u_live) = (Uuid::new_v4(), Uuid::new_v4());
    for u in [u_seeded, u_live] {
        sqlx::query("DELETE FROM user_wallet_read_model WHERE user_id = $1")
            .bind(u)
            .execute(&pool)
            .await
            .expect("edge cleanup");
    }

    // u_seeded: the pre-split shape — a wallet on the serial row, nothing on the
    // user edge. Written directly, as the legacy `meters ⋈ users` backfill did.
    sqlx::query(
        "INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
         VALUES ($1, $2, $3, now())",
    )
    .bind("OWNREPO-REV1")
    .bind(u_seeded)
    .bind("W-FROM-BACKFILL")
    .execute(&pool)
    .await
    .expect("seed serial row");

    // u_live: already has an authoritative edge row from a live IAM event, and a
    // STALER wallet on its serial row. The seed must not regress it.
    repo.update_wallet_by_user(u_live, Some("W-AUTHORITATIVE"))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
         VALUES ($1, $2, $3, now())",
    )
    .bind("OWNREPO-REV2")
    .bind(u_live)
    .bind("W-STALE")
    .execute(&pool)
    .await
    .expect("seed stale serial row");

    repo.backfill().await.expect("backfill");

    let edge = |u: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT wallet_address FROM user_wallet_read_model WHERE user_id = $1",
            )
            .bind(u)
            .fetch_optional(&pool)
            .await
            .expect("read edge")
        }
    };
    assert_eq!(
        edge(u_seeded).await,
        Some(Some("W-FROM-BACKFILL".to_string())),
        "a backfill-sourced wallet must also land on the user edge, or meter-service blanks it"
    );
    assert_eq!(
        edge(u_live).await,
        Some(Some("W-AUTHORITATIVE".to_string())),
        "an existing IAM-written edge row must NOT be regressed by the derived seed"
    );
}

#[tokio::test]
async fn repair_missing_wallets_resolves_via_callback() {
    // Wallet events consumed before the cutover never reached this DB — the
    // repair pass resolves those users through the injected callback (IAM
    // GetUserWallet in production). Ok(Some) fills the row + durable edge,
    // Ok(None) ("no wallet yet") and Err (IAM down) both leave NULL for the
    // next boot.
    let Some(url) = test_url() else {
        eprintln!("SKIP repair_missing_wallets_resolves_via_callback: METER_TEST_DATABASE_URL unset");
        return;
    };
    let pool = setup(&url, &["OWNREPO-RP1", "OWNREPO-RP2", "OWNREPO-RP3"]).await;
    let repo = OwnerReadModel::new(pool.clone());
    let (u_fixed, u_no_wallet, u_iam_down) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());

    repo.upsert_by_serial("OWNREPO-RP1", u_fixed, None).await.unwrap();
    repo.upsert_by_serial("OWNREPO-RP2", u_no_wallet, None).await.unwrap();
    repo.upsert_by_serial("OWNREPO-RP3", u_iam_down, None).await.unwrap();

    // The scan is table-wide (other tests' NULL rows may be in flight in the same
    // DB), so the stub answers by user_id and the assertions check rows, not counts.
    let (fixed, checked) = repo
        .repair_missing_wallets(|uid| async move {
            if uid == u_fixed {
                Ok(Some("  W-REPAIRED  ".to_string())) // resolver output gets normalized
            } else if uid == u_iam_down {
                Err(anyhow::anyhow!("iam unreachable"))
            } else {
                Ok(None)
            }
        })
        .await;
    assert!(checked >= 3, "all three NULL-wallet users must be scanned (got {checked})");
    assert!(fixed >= 1, "the resolvable user's row must be counted as fixed");

    assert_eq!(
        row(&pool, "OWNREPO-RP1").await.unwrap().1.as_deref(),
        Some("W-REPAIRED"),
        "resolved wallet must land trimmed"
    );
    assert!(
        row(&pool, "OWNREPO-RP2").await.unwrap().1.is_none(),
        "user without a wallet stays NULL"
    );
    assert!(
        row(&pool, "OWNREPO-RP3").await.unwrap().1.is_none(),
        "resolver failure leaves NULL (healed next boot)"
    );

    // The repair writes through update_wallet_by_user, so the durable edge is
    // recorded too — a later meter event for this user back-fills from it.
    let edge: Option<String> = sqlx::query_scalar(
        "SELECT wallet_address FROM user_wallet_read_model WHERE user_id = $1",
    )
    .bind(u_fixed)
    .fetch_one(&pool)
    .await
    .expect("edge row");
    assert_eq!(edge.as_deref(), Some("W-REPAIRED"));
}
