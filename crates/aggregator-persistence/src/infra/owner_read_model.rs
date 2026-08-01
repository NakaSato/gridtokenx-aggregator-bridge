//! DB-per-service Phase 2 — owner/wallet read-model feed.
//!
//! Builds (but does NOT cut over) the machinery that keeps the local
//! `meter_owner_read_model(serial_number, user_id, wallet_address, updated_at)`
//! table current. At a later cutover (main thread) this table replaces the
//! aggregator's cross-domain read of IAM `users.wallet_address` via the
//! `meters ⋈ users` JOIN (`meter_registry.rs`, `pg_readings.rs`). See
//! `docs/db-split-phase2.md` and `migrations/0006_meter_owner_read_model.sql`.
//!
//! **This is machinery only, gated OFF by default** (`AGGREGATOR_OWNER_READMODEL_FEED`).
//! The existing `meters ⋈ users` reads are intentionally left in place with their
//! `TODO(db-split)` markers — nothing here removes them.
//!
//! Three parts, all degrade-safe (mirroring the other optional edges here):
//! 1. [`OwnerReadModel`] — Postgres repo: UPSERT by serial (meter events) + UPDATE
//!    wallet by user (IAM wallet events) + first-boot backfill from the current
//!    shared source. Uses the same `PgPool` the readings sink / registry use.
//! 2. [`OwnerReadModelConsumer`] — a Kafka `StreamConsumer` mirroring the existing
//!    dispatch listener (`infra::kafka::AggregatorKafkaConsumer`): same brokers /
//!    group-id / subscribe / recv-loop shape, subscribing to the user + meter
//!    event topics and dispatching a JSON [`FeedEvent`] envelope to the repo.
//!    Never panics — a bad payload is logged and skipped.
//! 3. [`spawn_owner_readmodel_feed`] — the gate: when enabled it runs the backfill
//!    once (idempotent) then spawns the consumer on the shared shutdown future;
//!    when disabled it does nothing.

use std::future::Future;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::Message,
    ClientConfig,
};
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// The JSON envelope every domain event ships in: `{id, event_type, timestamp,
/// source, data}`. We only need the discriminant and the free-form `data`
/// object; the rest (`id`/`timestamp`/`source`) is ignored so an envelope shape
/// change elsewhere can't break parsing here.
#[derive(Debug, Deserialize)]
struct FeedEvent {
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    data: serde_json::Value,
}

/// `data` payload for `MeterRegistered` / `MeterUpdated` (from meter-service).
/// Only the fields the read-model needs are pulled; extras (`meter_id`,
/// `zone_id`, `status`) are ignored.
#[derive(Debug, Deserialize)]
struct MeterEventData {
    serial_number: String,
    user_id: Uuid,
    #[serde(default)]
    wallet_address: Option<String>,
}

/// `data` payload for `UserWalletLinked` / `UserOnboarded` /
/// `UserWalletPrimaryChanged` (from IAM). Keyed by user, not serial.
#[derive(Debug, Deserialize)]
struct UserEventData {
    user_id: Uuid,
    #[serde(default)]
    wallet_address: Option<String>,
    /// Present on `UserWalletPrimaryChanged` (and any event that distinguishes a
    /// primary from a secondary wallet). Absent ⇒ treated as primary.
    #[serde(default)]
    is_primary: Option<bool>,
}

/// Normalize a wallet field: trim, and treat empty/whitespace as absent (`None`)
/// so a blank wallet is stored as SQL NULL rather than an empty string — the same
/// `filter(|w| !w.trim().is_empty())` guard the registry / mint path use.
fn wallet_or_none(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|w| !w.is_empty())
}

/// Postgres repo over `meter_owner_read_model`. Cheap to clone (`PgPool` is an
/// `Arc` internally).
#[derive(Clone)]
pub struct OwnerReadModel {
    pool: PgPool,
}

impl OwnerReadModel {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// UPSERT the serial → owner edge from a meter event.
    ///
    /// The serial is canonicalized by the DB `canonicalize_meter_serial()` — the
    /// SAME function the `meters` unique index uses — so a serial written here
    /// resolves identically to a `meters` lookup (no dash/case-variant split).
    ///
    /// The wallet on a meter event is a **stale registration-time snapshot** — it
    /// is set once at `MeterRegistered` (from the owner's wallet at that instant)
    /// and never re-emitted, while IAM is the authoritative source and updates the
    /// primary via `update_wallet_by_user`. Meter and IAM events ride separate
    /// topics with different partition keys, so there is NO cross-stream ordering:
    /// a Kafka redelivery (or the first-boot `earliest` replay) of an old
    /// `MeterRegistered` could arrive after IAM already moved the primary.
    ///
    /// So the wallet merge is **owner-aware**, not a blind `COALESCE`:
    /// - **same owner** — IAM's wallet wins; the meter event only *fills an absent*
    ///   wallet (`COALESCE(existing, EXCLUDED)`), never overrides a set one. This is
    ///   what stops a stale meter redelivery from reverting the IAM primary and
    ///   misdirecting a surplus mint to the wallet the user moved away from.
    /// - **owner changed** (re-registration to a different `user_id`) — take the
    ///   event's fresh snapshot; the previous owner's wallet is wrong for the new one.
    ///
    /// The `user_id` edge is always refreshed. Authoritative wallet writes / clears
    /// flow through the user-event path ([`update_wallet_by_user`](Self::update_wallet_by_user)).
    pub async fn upsert_by_serial(
        &self,
        serial: &str,
        user_id: Uuid,
        wallet: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
            VALUES (
                canonicalize_meter_serial($1), $2,
                -- A wallet event with no meter rows yet lands only in
                -- user_wallet_read_model; consult it so either arrival order of
                -- the two streams produces the same merged row.
                COALESCE($3, (SELECT wallet_address FROM user_wallet_read_model WHERE user_id = $2)),
                now()
            )
            ON CONFLICT (serial_number) DO UPDATE
            SET user_id        = EXCLUDED.user_id,
                wallet_address = CASE
                    WHEN meter_owner_read_model.user_id = EXCLUDED.user_id
                        -- same owner: IAM wallet authoritative; meter event fills only
                        THEN COALESCE(meter_owner_read_model.wallet_address, EXCLUDED.wallet_address)
                    -- owner changed: the old owner's wallet is stale for the new owner
                    ELSE EXCLUDED.wallet_address
                END,
                updated_at     = now()
            "#,
        )
        .bind(serial)
        .bind(user_id)
        .bind(wallet)
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    /// UPDATE the wallet for every meter owned by `user_id` (an IAM wallet event
    /// is keyed by user, not serial). Returns the number of meter rows touched.
    ///
    /// This is the authoritative wallet writer: it sets the wallet directly
    /// (including to NULL when a wallet is unlinked), unlike the serial upsert
    /// which only fills an absent wallet.
    ///
    /// The wallet is ALSO upserted into `user_wallet_read_model` — the durable
    /// user → wallet edge — so a wallet event consumed before the user's first
    /// `MeterRegistered` row exists (the two streams have no cross-topic
    /// ordering) is not lost: the later serial upsert back-fills from that edge.
    /// Both writes ride one transaction so a crash can't record the wallet on
    /// one side only.
    pub async fn update_wallet_by_user(
        &self,
        user_id: Uuid,
        wallet: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO user_wallet_read_model (user_id, wallet_address, updated_at)
            VALUES ($1, $2, now())
            ON CONFLICT (user_id) DO UPDATE
            SET wallet_address = EXCLUDED.wallet_address, updated_at = now()
            "#,
        )
        .bind(user_id)
        .bind(wallet)
        .execute(&mut *tx)
        .await?;
        let res = sqlx::query(
            r#"
            UPDATE meter_owner_read_model
            SET wallet_address = $1, updated_at = now()
            WHERE user_id = $2
            "#,
        )
        .bind(wallet)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(res.rows_affected())
    }

    /// Clear the wallet for `user_id` **only when it currently equals `wallet`** —
    /// the response to a `UserWalletUnlinked` event. Keyed by `(user_id, wallet)`
    /// so unlinking a NON-primary wallet (one the read-model never stored) is a
    /// no-op, and only the actual stored mint-recipient wallet is cleared. Without
    /// this, a mint keeps resolving to a wallet the user no longer controls.
    /// Returns the number of rows cleared.
    pub async fn clear_wallet_if_matches(
        &self,
        user_id: Uuid,
        wallet: &str,
    ) -> Result<u64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // Clear the durable user → wallet edge under the same match rule, so a
        // later meter event can't resurrect the unlinked wallet from it.
        sqlx::query(
            r#"
            UPDATE user_wallet_read_model
            SET wallet_address = NULL, updated_at = now()
            WHERE user_id = $1 AND wallet_address = $2
            "#,
        )
        .bind(user_id)
        .bind(wallet)
        .execute(&mut *tx)
        .await?;
        let res = sqlx::query(
            r#"
            UPDATE meter_owner_read_model
            SET wallet_address = NULL, updated_at = now()
            WHERE user_id = $1 AND wallet_address = $2
            "#,
        )
        .bind(user_id)
        .bind(wallet)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(res.rows_affected())
    }

    /// One-time, idempotent first-boot seed. `ON CONFLICT DO NOTHING` so a
    /// re-run (or a row already advanced by a live event) is never clobbered.
    /// Returns the number of rows seeded.
    ///
    /// Two sources, picked by what the connected DB actually contains:
    /// - **Pre-cutover** (shared DB): the legacy `meters ⋈ users(wallet_address)`
    ///   join — the same cross-domain read Phase 2 removes, used here only to seed.
    /// - **Post-cutover** (dedicated `gridtokenx_meter` DB): IAM's `users` table no
    ///   longer exists (SQLSTATE 42P01), so seed the serial → user edge from the
    ///   local `meters` table, taking any wallet already known to
    ///   `user_wallet_read_model`. Wallets IAM assigned before the cutover are NOT
    ///   here (their events pre-date the consumer group's offsets) — those rows
    ///   seed with a NULL wallet and are healed by
    ///   [`repair_missing_wallets`](Self::repair_missing_wallets) via IAM
    ///   `GetUserWallet`, not by this seed.
    pub async fn backfill(&self) -> Result<u64, sqlx::Error> {
        let legacy = sqlx::query(
            r#"
            INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
            SELECT canonicalize_meter_serial(m.serial_number), m.user_id, u.wallet_address, now()
            FROM meters m
            JOIN users u ON u.id = m.user_id
            ON CONFLICT (serial_number) DO NOTHING
            "#,
        )
        .execute(&self.pool)
        .await;
        let seeded = match legacy {
            Ok(res) => res.rows_affected(),
            Err(e) if is_undefined_table(&e) => {
                info!("owner read-model backfill: no IAM `users` table here (post-split DB) — seeding from local meters ⋈ user_wallet_read_model");
                self.backfill_local().await?
            }
            Err(e) => return Err(e),
        };
        // The seeds above only flow serial-ward. Complete the reverse edge, or
        // any owner whose wallet arrived via a backfill (rather than a live IAM
        // event) would have a wallet in `meter_owner_read_model` and NO row in
        // `user_wallet_read_model` — invisible to `repair_missing_wallets`,
        // which only targets NULL wallets. meter-service resolves every owner
        // wallet through the user edge alone (TD-004), so that hole would blank
        // the wallet on its meter list / map for exactly those owners.
        self.backfill_user_edge().await?;
        Ok(seeded)
    }

    /// Seed the durable user → primary-wallet edge from the wallets already
    /// known per serial. Idempotent and non-regressing: `DO NOTHING` on conflict
    /// so a row written by a live IAM event (authoritative) is never overwritten
    /// by this derived one. When a user owns several serials carrying different
    /// wallets, the most recently updated one wins.
    async fn backfill_user_edge(&self) -> Result<u64, sqlx::Error> {
        let res = sqlx::query(
            r#"
            INSERT INTO user_wallet_read_model (user_id, wallet_address, updated_at)
            SELECT DISTINCT ON (user_id) user_id, wallet_address, now()
            FROM meter_owner_read_model
            WHERE wallet_address IS NOT NULL AND wallet_address <> ''
            ORDER BY user_id, updated_at DESC
            ON CONFLICT (user_id) DO NOTHING
            "#,
        )
        .execute(&self.pool)
        .await?;
        let n = res.rows_affected();
        if n > 0 {
            info!(
                seeded = n,
                "owner read-model: seeded user→wallet edge from existing serial rows"
            );
        }
        Ok(n)
    }

    /// Post-cutover seed from tables that live in the metering DB itself:
    /// `meters` for the serial → user edge, `user_wallet_read_model` for any
    /// wallet the event feed has already recorded.
    async fn backfill_local(&self) -> Result<u64, sqlx::Error> {
        let res = sqlx::query(
            r#"
            INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
            SELECT canonicalize_meter_serial(m.serial_number), m.user_id, uw.wallet_address, now()
            FROM meters m
            LEFT JOIN user_wallet_read_model uw ON uw.user_id = m.user_id
            ON CONFLICT (serial_number) DO NOTHING
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Users whose read-model rows have no wallet — the mint-blocking state the
    /// repair pass resolves against IAM. Bounded so a huge fleet can't turn the
    /// one-shot repair into an unbounded IAM scan (leftovers heal next boot).
    async fn users_missing_wallet(&self) -> Result<Vec<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"
            SELECT DISTINCT user_id FROM meter_owner_read_model
            WHERE wallet_address IS NULL
            LIMIT 1000
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    /// One-shot repair: for every read-model row missing a wallet, ask `resolve`
    /// (IAM `GetUserWallet`, wired by the server layer) for the user's primary
    /// wallet and write it through [`update_wallet_by_user`](Self::update_wallet_by_user).
    /// Covers users whose IAM wallet events pre-date the feed's consumer offsets
    /// (e.g. registrations before the DB-per-service cutover), which neither the
    /// event stream nor the local seed can recover.
    ///
    /// `resolve` returns `Ok(None)` when the user simply has no wallet yet (kept
    /// NULL, retried next boot) and `Err` on transport failure (logged, skipped).
    /// Returns `(rows_fixed, users_checked)`.
    pub async fn repair_missing_wallets<F, Fut>(&self, resolve: F) -> (u64, u64)
    where
        F: Fn(Uuid) -> Fut,
        Fut: Future<Output = Result<Option<String>>>,
    {
        let users = match self.users_missing_wallet().await {
            Ok(u) => u,
            Err(e) => {
                warn!("owner read-model wallet repair: scan failed ({e}); skipping");
                return (0, 0);
            }
        };
        let checked = users.len() as u64;
        let mut fixed = 0u64;
        for user_id in users {
            match resolve(user_id).await {
                Ok(Some(raw)) => match wallet_or_none(Some(&raw)) {
                    Some(wallet) => match self.update_wallet_by_user(user_id, Some(wallet)).await {
                        Ok(n) => {
                            debug!("owner read-model wallet repair: filled {n} row(s) for user {user_id}");
                            fixed += n;
                        }
                        Err(e) => warn!(
                            "owner read-model wallet repair: write failed for user {user_id} ({e})"
                        ),
                    },
                    None => debug!(
                        "owner read-model wallet repair: IAM returned blank wallet for user {user_id}; leaving NULL"
                    ),
                },
                Ok(None) => debug!(
                    "owner read-model wallet repair: user {user_id} has no wallet yet; leaving NULL"
                ),
                Err(e) => warn!(
                    "owner read-model wallet repair: resolve failed for user {user_id} ({e}); leaving NULL"
                ),
            }
        }
        (fixed, checked)
    }
}

/// SQLSTATE 42P01 (`undefined_table`) — the discriminator between the shared
/// pre-cutover DB (IAM `users` present) and the dedicated metering DB (absent).
fn is_undefined_table(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("42P01"))
}

/// Whether the owner read-model feed is enabled (`AGGREGATOR_OWNER_READMODEL_FEED`).
/// Shared with the server layer so the wallet-repair pass rides the same gate.
pub fn feed_enabled() -> bool {
    std::env::var("AGGREGATOR_OWNER_READMODEL_FEED")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

/// Kafka consumer feeding the read-model. Mirrors
/// [`AggregatorKafkaConsumer`](super::kafka::AggregatorKafkaConsumer): a
/// `StreamConsumer` on the same broker list + a group-id, subscribed and drained
/// in a `recv()` loop — no new bus is introduced.
pub struct OwnerReadModelConsumer {
    consumer: StreamConsumer,
    /// Kept so the loop can rebuild the client after sustained fencing — see
    /// [`Self::run`].
    brokers: String,
    group_id: String,
    topics: Vec<String>,
}

/// Consecutive `recv()` failures tolerated before the client is rebuilt. At the
/// 500ms backoff below this is ~30s of unbroken failure, so a transient broker
/// blip (which recovers on its own) never triggers a rebuild.
const REBUILD_AFTER_CONSECUTIVE_ERRORS: u32 = 60;

impl OwnerReadModelConsumer {
    /// Build + subscribe. Mirrors the dispatch listener's builder, subscribing to
    /// multiple topics (user + meter events).
    pub fn new(brokers: &str, group_id: &str, topics: &[&str]) -> Result<Self> {
        let consumer = Self::connect(brokers, group_id, topics)?;
        Ok(Self {
            consumer,
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            topics: topics.iter().map(|t| (*t).to_string()).collect(),
        })
    }

    /// The client build + subscribe, factored out so [`Self::run`] can redo it.
    fn connect(brokers: &str, group_id: &str, topics: &[&str]) -> Result<StreamConsumer> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            // Unlike the tail-only dispatch listener (`latest`), a read-model must
            // not silently miss ownership updates published while it was down — so
            // a fresh group replays from the earliest retained offset. The lazy
            // per-serial DB re-resolve (MeterRegistry::backfill) + the first-boot
            // backfill are the belt-and-braces backstop if an event is still lost.
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| anyhow!("owner read-model consumer create failed: {:?}", e))?;

        consumer
            .subscribe(topics)
            .map_err(|e| anyhow!("owner read-model consumer subscribe failed: {:?}", e))?;
        info!("✅ Owner read-model consumer subscribed to {:?}", topics);
        Ok(consumer)
    }

    /// Drain the subscribed topics until `shutdown` resolves, dispatching each
    /// message into `repo`. Never panics: a consume error backs off briefly and
    /// continues; a malformed payload is logged and skipped.
    pub async fn run(mut self, repo: OwnerReadModel, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        info!("📡 Owner read-model feed started");
        let mut consecutive_errors: u32 = 0;
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("🛑 Owner read-model feed stopped");
                    break;
                }
                recv = self.consumer.recv() => {
                    match recv {
                        Ok(msg) => {
                            consecutive_errors = 0;
                            match msg.payload() {
                                Some(payload) => Self::dispatch(&repo, payload).await,
                                None => warn!("owner read-model: empty payload, skipping"),
                            }
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            error!("❌ Owner read-model consume error: {}", e);
                            // Avoid a hot spin if the broker is transiently down.
                            tokio::time::sleep(Duration::from_millis(500)).await;

                            // Most fencings self-heal: rdkafka rejoins the group and
                            // `recv()` starts succeeding again. But after a long host
                            // suspend (observed 2026-08-01: a 68-minute freeze blew
                            // `max.poll.interval.ms`) the client stayed out of the
                            // group indefinitely — the loop kept retrying a client
                            // that would never recover, so ownership events silently
                            // stopped applying and `meter_owner_read_model` drifted
                            // from `meters`. That is invisible downstream: readings
                            // keep attributing to the previous owner and surplus is
                            // dropped as `no registered owner`. Rebuilding the client
                            // is exactly what a container restart did to fix it, minus
                            // the restart — the group id is unchanged, so it resumes
                            // from the committed offsets rather than replaying.
                            if consecutive_errors >= REBUILD_AFTER_CONSECUTIVE_ERRORS {
                                let topics: Vec<&str> =
                                    self.topics.iter().map(String::as_str).collect();
                                match Self::connect(&self.brokers, &self.group_id, &topics) {
                                    Ok(consumer) => {
                                        self.consumer = consumer;
                                        consecutive_errors = 0;
                                        warn!(
                                            "♻️ Owner read-model consumer rebuilt after {} consecutive errors",
                                            REBUILD_AFTER_CONSECUTIVE_ERRORS
                                        );
                                    }
                                    // Keep the old client and keep trying; a rebuild
                                    // that fails must not take the feed down.
                                    Err(e) => {
                                        error!("❌ Owner read-model consumer rebuild failed: {e}");
                                        consecutive_errors = 0;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Parse one envelope and apply it. Pure log-and-continue on any bad payload —
    /// this is called from the recv loop and must never propagate/panic.
    async fn dispatch(repo: &OwnerReadModel, payload: &[u8]) {
        let event: FeedEvent = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                warn!("owner read-model: undecodable envelope JSON ({e}); skipping");
                return;
            }
        };

        match event.event_type.as_str() {
            // Meter events carry the serial → user edge (+ optional wallet).
            "MeterRegistered" | "MeterUpdated" => {
                let data: MeterEventData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(
                            "owner read-model: bad {} data ({e}); skipping",
                            event.event_type
                        );
                        return;
                    }
                };
                let wallet = wallet_or_none(data.wallet_address.as_deref());
                match repo
                    .upsert_by_serial(&data.serial_number, data.user_id, wallet)
                    .await
                {
                    Ok(()) => debug!(
                        "owner read-model: upserted serial {} → user {}",
                        data.serial_number, data.user_id
                    ),
                    Err(e) => warn!(
                        "owner read-model: upsert failed for serial {} ({e})",
                        data.serial_number
                    ),
                }
            }

            // IAM wallet events are keyed by user → touch every serial that user owns.
            //
            // `EmailVerified` belongs here too: IAM auto-provisions the custodial
            // wallet at e-mail verification and that event is the FIRST (and in the
            // common self-service flow, the ONLY) user event carrying the wallet —
            // `UserOnboarded`/`UserWalletLinked` fire only on the separate on-chain
            // onboarding path. Without it a verified user who claims a meter never
            // gets a wallet into the read-model and every surplus mint defers.
            // Its payload has no `is_primary` (⇒ treated primary) and a missing
            // `wallet_address` deserializes to None, which `wallet_or_none` keeps
            // as NULL — same contract as the other arms.
            "EmailVerified" | "UserWalletLinked" | "UserOnboarded" | "UserWalletPrimaryChanged" => {
                let data: UserEventData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(
                            "owner read-model: bad {} data ({e}); skipping",
                            event.event_type
                        );
                        return;
                    }
                };
                // Only the PRIMARY wallet feeds the read-model (it is the mint
                // recipient). `is_primary` is present on PrimaryChanged; when
                // absent (Linked/Onboarded) the wallet is treated as primary.
                if data.is_primary == Some(false) {
                    debug!(
                        "owner read-model: non-primary wallet change for user {}, skipping",
                        data.user_id
                    );
                    return;
                }
                let wallet = wallet_or_none(data.wallet_address.as_deref());
                // EmailVerified stamps the wallet with `unwrap_or("")` on the IAM
                // side — an empty wallet there means provisioning hadn't landed
                // yet (or a re-verify raced it), NOT an authoritative unlink. A
                // clear must come from the explicit wallet events only.
                if wallet.is_none() && event.event_type == "EmailVerified" {
                    debug!(
                        "owner read-model: EmailVerified without wallet for user {}, skipping",
                        data.user_id
                    );
                    return;
                }
                match repo.update_wallet_by_user(data.user_id, wallet).await {
                    Ok(n) => debug!(
                        "owner read-model: updated {n} meter row(s) for user {}",
                        data.user_id
                    ),
                    Err(e) => warn!(
                        "owner read-model: wallet update failed for user {} ({e})",
                        data.user_id
                    ),
                }
            }

            // A wallet unlink clears the read-model wallet ONLY if it was the
            // stored (mint-recipient) one — unlinking a different wallet is a
            // no-op. Prevents a surplus mint from targeting a wallet the user has
            // unlinked. `UserWalletUnlinked` carries no `is_primary`; the match on
            // the stored wallet value is what scopes it to the primary.
            "UserWalletUnlinked" => {
                let data: UserEventData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("owner read-model: bad UserWalletUnlinked data ({e}); skipping");
                        return;
                    }
                };
                match wallet_or_none(data.wallet_address.as_deref()) {
                    Some(w) => match repo.clear_wallet_if_matches(data.user_id, w).await {
                        Ok(n) => debug!(
                            "owner read-model: cleared {n} wallet row(s) for user {} (unlink)",
                            data.user_id
                        ),
                        Err(e) => warn!(
                            "owner read-model: wallet clear failed for user {} ({e})",
                            data.user_id
                        ),
                    },
                    None => debug!(
                        "owner read-model: UserWalletUnlinked with empty wallet for user {}, skipping",
                        data.user_id
                    ),
                }
            }

            other => debug!("owner read-model: ignoring event_type {other}"),
        }
    }
}

/// The gate. When `AGGREGATOR_OWNER_READMODEL_FEED` is truthy AND a Postgres pool
/// + `KAFKA_BOOTSTRAP_SERVERS` are present, run the backfill once (idempotent)
/// then spawn the consumer on the shared `shutdown` future. Disabled by default:
/// unset/false ⇒ this is a no-op and nothing about the existing service changes.
///
/// Degrade-safe like the other optional edges: a missing pool / missing brokers /
/// a consumer build error logs a `warn!`/`error!` and returns without spawning —
/// the service still starts.
pub async fn spawn_owner_readmodel_feed(
    pool: Option<PgPool>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) {
    if !feed_enabled() {
        info!("ℹ️ Owner read-model feed disabled (AGGREGATOR_OWNER_READMODEL_FEED != true)");
        return;
    }

    let Some(pool) = pool else {
        warn!("⚠️ AGGREGATOR_OWNER_READMODEL_FEED=true but no Postgres pool — read-model feed disabled");
        return;
    };
    let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") else {
        warn!("⚠️ AGGREGATOR_OWNER_READMODEL_FEED=true but KAFKA_BOOTSTRAP_SERVERS unset — read-model feed disabled");
        return;
    };

    let repo = OwnerReadModel::new(pool.clone());

    // First-boot backfill (idempotent, ON CONFLICT DO NOTHING). A failure here is
    // non-fatal: the live event feed + the lazy per-serial DB re-resolve keep the
    // model current, so log and continue rather than abort the feed.
    //
    // SCOPE: pre-cutover (shared DB) this seeds from `meters ⋈ users`; on the
    // dedicated `gridtokenx_meter` DB (no IAM `users` table) it falls back to the
    // local `meters ⋈ user_wallet_read_model` seed. Wallets whose IAM events
    // pre-date the consumer offsets are healed separately by the server-layer
    // wallet-repair pass (`repair_missing_wallets` over IAM `GetUserWallet`).
    match repo.backfill().await {
        Ok(n) => info!("📥 Owner read-model backfilled {n} row(s)"),
        Err(e) => warn!(
            "⚠️ Owner read-model backfill skipped/failed ({e}); feed continues (events + lazy re-resolve)"
        ),
    }

    // Defaults match the actual producers so the feed works out of the box:
    //   meter-service publishes to METER_EVENTS_TOPIC (default `meter_events`);
    //   IAM publishes to KafkaTopics `{prefix}.user.events` (default `iam.user.events`).
    // Override per deployment when the IAM topic prefix differs.
    let meter_topic =
        std::env::var("READMODEL_METER_TOPIC").unwrap_or_else(|_| "meter_events".to_string());
    let user_topic =
        std::env::var("READMODEL_IAM_TOPIC").unwrap_or_else(|_| "iam.user.events".to_string());

    // The two source streams can live on DIFFERENT Kafka clusters: meter-service
    // publishes `meter_events` to this service's KAFKA_BOOTSTRAP_SERVERS, while IAM
    // publishes `iam.user.events` to ITS broker (KAFKA_CMD in this deploy) — the same
    // one noti-service consumes from, so the topic can't just be moved. When
    // READMODEL_IAM_BROKERS names that other cluster we run a SECOND consumer there
    // for the user topic; unset ⇒ same broker as everything else (single consumer,
    // original behavior, zero change).
    let iam_brokers = std::env::var("READMODEL_IAM_BROKERS").unwrap_or_else(|_| brokers.clone());

    if iam_brokers == brokers {
        // One cluster carries both topics — single consumer, single repo.
        let consumer = match OwnerReadModelConsumer::new(
            &brokers,
            "aggregator-owner-readmodel-group",
            &[user_topic.as_str(), meter_topic.as_str()],
        ) {
            Ok(c) => c,
            Err(e) => {
                error!("❌ Owner read-model consumer init failed: {e}; feed disabled");
                return;
            }
        };
        info!("🗃️ Owner read-model feed ENABLED (broker {brokers}; topics: {user_topic}, {meter_topic})");
        tokio::spawn(async move {
            consumer.run(repo, shutdown).await;
        });
        return;
    }

    // Split brokers: `meter_events` on `brokers`, `iam.user.events` on `iam_brokers`.
    // Two consumers feed the same read-model (distinct group-ids). Both must observe
    // the one `shutdown` future, so fan it out over a watch channel.
    let meter_consumer = match OwnerReadModelConsumer::new(
        &brokers,
        "aggregator-owner-readmodel-meter-group",
        &[meter_topic.as_str()],
    ) {
        Ok(c) => c,
        Err(e) => {
            error!("❌ Owner read-model meter consumer init failed: {e}; feed disabled");
            return;
        }
    };
    let iam_consumer = match OwnerReadModelConsumer::new(
        &iam_brokers,
        "aggregator-owner-readmodel-iam-group",
        &[user_topic.as_str()],
    ) {
        Ok(c) => c,
        Err(e) => {
            error!("❌ Owner read-model IAM consumer init failed: {e}; feed disabled");
            return;
        }
    };

    let (tx, rx) = tokio::sync::watch::channel(false);
    let rx2 = rx.clone();
    tokio::spawn(async move {
        shutdown.await;
        let _ = tx.send(true);
    });
    // Resolve once the flag flips true; a dropped sender (changed() Err) also stops us.
    async fn wait_shutdown(mut rx: tokio::sync::watch::Receiver<bool>) {
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                break;
            }
        }
    }

    let repo_iam = OwnerReadModel::new(pool);
    info!(
        "🗃️ Owner read-model feed ENABLED (split brokers: {meter_topic}@{brokers}, {user_topic}@{iam_brokers})"
    );
    tokio::spawn(async move {
        meter_consumer.run(repo, wait_shutdown(rx)).await;
    });
    tokio::spawn(async move {
        iam_consumer.run(repo_iam, wait_shutdown(rx2)).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure parsing/routing tests — no Kafka/Postgres. The repo SQL + consumer wire
    // roundtrip need live infra (covered by the e2e stack), same as the other
    // edges here; these lock the envelope contract the dispatcher depends on.

    #[test]
    fn wallet_or_none_trims_and_blanks_to_none() {
        assert_eq!(wallet_or_none(Some("  ABC123  ")), Some("ABC123"));
        assert_eq!(wallet_or_none(Some("   ")), None);
        assert_eq!(wallet_or_none(Some("")), None);
        assert_eq!(wallet_or_none(None), None);
    }

    #[test]
    fn feed_event_parses_envelope_and_ignores_extra_fields() {
        let raw = br#"{
            "id": "evt-1",
            "event_type": "MeterRegistered",
            "timestamp": 1700000000,
            "source": "meter-service",
            "data": {"serial_number": "MTR-1", "user_id": "00000000-0000-0000-0000-000000000001"}
        }"#;
        let ev: FeedEvent = serde_json::from_slice(raw).unwrap();
        assert_eq!(ev.event_type, "MeterRegistered");
        assert!(ev.data.is_object());
    }

    #[test]
    fn feed_event_defaults_missing_data_and_type() {
        // Missing data ⇒ Null value (not an error); missing event_type ⇒ "".
        let ev: FeedEvent = serde_json::from_slice(br#"{"id":"x"}"#).unwrap();
        assert_eq!(ev.event_type, "");
        assert!(ev.data.is_null());
    }

    #[test]
    fn meter_event_data_parses_with_and_without_wallet() {
        let with_w: MeterEventData = serde_json::from_str(
            r#"{"serial_number":"MTR-1","meter_id":"m1","user_id":"00000000-0000-0000-0000-000000000001","zone_id":"z1","status":"active","wallet_address":"WALLET1"}"#,
        )
        .unwrap();
        assert_eq!(with_w.serial_number, "MTR-1");
        assert_eq!(with_w.wallet_address.as_deref(), Some("WALLET1"));

        let no_w: MeterEventData = serde_json::from_str(
            r#"{"serial_number":"MTR-2","user_id":"00000000-0000-0000-0000-000000000002"}"#,
        )
        .unwrap();
        assert_eq!(no_w.wallet_address, None);
    }

    #[test]
    fn user_event_data_is_primary_defaults_absent() {
        let linked: UserEventData = serde_json::from_str(
            r#"{"user_id":"00000000-0000-0000-0000-000000000001","wallet_address":"W"}"#,
        )
        .unwrap();
        assert_eq!(
            linked.is_primary, None,
            "absent is_primary ⇒ None ⇒ treated as primary"
        );

        let demoted: UserEventData = serde_json::from_str(
            r#"{"user_id":"00000000-0000-0000-0000-000000000001","wallet_address":"W","is_primary":false}"#,
        )
        .unwrap();
        assert_eq!(
            demoted.is_primary,
            Some(false),
            "non-primary change must be skippable"
        );
    }

    #[test]
    fn meter_event_data_rejects_bad_uuid() {
        // A bad user_id must fail parse (→ dispatcher logs + skips, never panics).
        assert!(serde_json::from_str::<MeterEventData>(
            r#"{"serial_number":"MTR-1","user_id":"not-a-uuid"}"#
        )
        .is_err());
    }

    #[test]
    fn meter_event_data_rejects_missing_serial() {
        // A meter event without a serial has no read-model key — it must fail
        // parse (→ dispatcher logs + skips), never land as an empty serial.
        assert!(serde_json::from_str::<MeterEventData>(
            r#"{"user_id":"00000000-0000-0000-0000-000000000001"}"#
        )
        .is_err());
    }

    #[test]
    fn user_event_data_rejects_missing_or_bad_user_id() {
        // The user path is keyed by user_id: absent or malformed must fail parse
        // (→ dispatcher logs + skips, never panics) — mirror of the meter guard.
        assert!(serde_json::from_str::<UserEventData>(r#"{"wallet_address":"W"}"#).is_err());
        assert!(serde_json::from_str::<UserEventData>(
            r#"{"user_id":"nope","wallet_address":"W"}"#
        )
        .is_err());
    }

    #[test]
    fn user_event_data_parses_explicit_is_primary_true() {
        // Some(true) must ride the same path as absent (only Some(false) skips).
        let promoted: UserEventData = serde_json::from_str(
            r#"{"user_id":"00000000-0000-0000-0000-000000000001","wallet_address":"W","is_primary":true}"#,
        )
        .unwrap();
        assert_eq!(promoted.is_primary, Some(true));
    }

    #[test]
    fn blank_wallets_normalize_to_none_after_parse() {
        // A whitespace/empty wallet on the wire parses as Some(..) but must
        // normalize to None before hitting SQL — meter path ⇒ fill-only treats it
        // as absent, user path ⇒ explicit NULL — never stored as an empty string.
        let meter: MeterEventData = serde_json::from_str(
            r#"{"serial_number":"MTR-3","user_id":"00000000-0000-0000-0000-000000000003","wallet_address":"   "}"#,
        )
        .unwrap();
        assert_eq!(wallet_or_none(meter.wallet_address.as_deref()), None);

        let user: UserEventData = serde_json::from_str(
            r#"{"user_id":"00000000-0000-0000-0000-000000000003","wallet_address":""}"#,
        )
        .unwrap();
        assert_eq!(wallet_or_none(user.wallet_address.as_deref()), None);
    }

    #[test]
    fn typed_payloads_reject_null_data() {
        // A recognised event_type with missing `data` (⇒ Null, per
        // feed_event_defaults_missing_data_and_type) must fail the typed parse —
        // the dispatcher logs + skips it, it is never treated as a valid event.
        assert!(serde_json::from_value::<MeterEventData>(serde_json::Value::Null).is_err());
        assert!(serde_json::from_value::<UserEventData>(serde_json::Value::Null).is_err());
    }
}
