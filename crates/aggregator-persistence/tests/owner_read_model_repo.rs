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

    // A new owner + new wallet overwrites both (last-writer-wins on user_id, and a
    // non-null event wallet does replace).
    repo.upsert_by_serial("OWNREPO-1", u2, Some("W2")).await.unwrap();
    assert_eq!(row(&pool, "OWNREPO-1").await, Some((u2, Some("W2".to_string()))));
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
