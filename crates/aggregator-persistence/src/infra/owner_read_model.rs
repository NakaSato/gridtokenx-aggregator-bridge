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
    /// Last-writer-wins: `updated_at` is stamped `now()` on every write. On a
    /// conflict the `user_id` is refreshed, but the wallet is only overwritten by
    /// a **non-null** event wallet (`COALESCE(EXCLUDED, existing)`): meter events
    /// often carry no wallet (a meter registered before/without a wallet — see
    /// docs §3), and blanking a wallet already set by an authoritative IAM user
    /// event would regress it. Clearing a wallet is done via the user-event path
    /// ([`update_wallet_by_user`](Self::update_wallet_by_user)), which is the
    /// authoritative wallet source.
    pub async fn upsert_by_serial(
        &self,
        serial: &str,
        user_id: Uuid,
        wallet: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
            VALUES (canonicalize_meter_serial($1), $2, $3, now())
            ON CONFLICT (serial_number) DO UPDATE
            SET user_id        = EXCLUDED.user_id,
                wallet_address = COALESCE(EXCLUDED.wallet_address, meter_owner_read_model.wallet_address),
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
    /// is keyed by user, not serial). Returns the number of rows touched (0 when
    /// the user owns no meters yet — the meter event fills the edge later, and a
    /// subsequent wallet event / the backfill re-applies the wallet).
    ///
    /// This is the authoritative wallet writer: it sets the wallet directly
    /// (including to NULL when a wallet is unlinked), unlike the serial upsert
    /// which only fills an absent wallet.
    pub async fn update_wallet_by_user(
        &self,
        user_id: Uuid,
        wallet: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        let res = sqlx::query(
            r#"
            UPDATE meter_owner_read_model
            SET wallet_address = $1, updated_at = now()
            WHERE user_id = $2
            "#,
        )
        .bind(wallet)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// One-time, idempotent first-boot seed from the CURRENT shared source — the
    /// same `meters ⋈ users(wallet_address)` join the aggregator reads today
    /// (reachable pre-cutover on `DATABASE_URL`). `ON CONFLICT DO NOTHING` so a
    /// re-run (or a row already advanced by a live event) is never clobbered.
    /// Returns the number of rows seeded.
    ///
    /// NOTE: this JOIN reads IAM-owned `users` — that is the very cross-domain
    /// read Phase 2 removes. It is used HERE only to seed the read-model at
    /// cutover; the live path keeps using it until the read-model is verified,
    /// per the `TODO(db-split)` markers (not touched by this change).
    pub async fn backfill(&self) -> Result<u64, sqlx::Error> {
        let res = sqlx::query(
            r#"
            INSERT INTO meter_owner_read_model (serial_number, user_id, wallet_address, updated_at)
            SELECT canonicalize_meter_serial(m.serial_number), m.user_id, u.wallet_address, now()
            FROM meters m
            JOIN users u ON u.id = m.user_id
            ON CONFLICT (serial_number) DO NOTHING
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }
}

/// Kafka consumer feeding the read-model. Mirrors
/// [`AggregatorKafkaConsumer`](super::kafka::AggregatorKafkaConsumer): a
/// `StreamConsumer` on the same broker list + a group-id, subscribed and drained
/// in a `recv()` loop — no new bus is introduced.
pub struct OwnerReadModelConsumer {
    consumer: StreamConsumer,
}

impl OwnerReadModelConsumer {
    /// Build + subscribe. Mirrors the dispatch listener's builder, subscribing to
    /// multiple topics (user + meter events).
    pub fn new(brokers: &str, group_id: &str, topics: &[&str]) -> Result<Self> {
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
        Ok(Self { consumer })
    }

    /// Drain the subscribed topics until `shutdown` resolves, dispatching each
    /// message into `repo`. Never panics: a consume error backs off briefly and
    /// continues; a malformed payload is logged and skipped.
    pub async fn run(self, repo: OwnerReadModel, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        info!("📡 Owner read-model feed started");
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("🛑 Owner read-model feed stopped");
                    break;
                }
                recv = self.consumer.recv() => {
                    match recv {
                        Ok(msg) => match msg.payload() {
                            Some(payload) => Self::dispatch(&repo, payload).await,
                            None => warn!("owner read-model: empty payload, skipping"),
                        },
                        Err(e) => {
                            error!("❌ Owner read-model consume error: {}", e);
                            // Avoid a hot spin if the broker is transiently down.
                            tokio::time::sleep(Duration::from_millis(500)).await;
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
                        warn!("owner read-model: bad {} data ({e}); skipping", event.event_type);
                        return;
                    }
                };
                let wallet = wallet_or_none(data.wallet_address.as_deref());
                match repo.upsert_by_serial(&data.serial_number, data.user_id, wallet).await {
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
            "UserWalletLinked" | "UserOnboarded" | "UserWalletPrimaryChanged" => {
                let data: UserEventData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("owner read-model: bad {} data ({e}); skipping", event.event_type);
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
    let enabled = std::env::var("AGGREGATOR_OWNER_READMODEL_FEED")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    if !enabled {
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

    let repo = OwnerReadModel::new(pool);

    // First-boot backfill (idempotent, ON CONFLICT DO NOTHING). A failure here is
    // non-fatal: the live event feed + the lazy per-serial DB re-resolve keep the
    // model current, so log and continue rather than abort the feed.
    //
    // SCOPE: this seeds from `meters ⋈ users` on THIS pool. It only succeeds while
    // the pool still points at the shared DB (pre-cutover / dual-write window).
    // On the dedicated `gridtokenx_meter` DB there is no IAM `users` table, so the
    // backfill errors (logged, non-fatal) and the read-model is seeded by event
    // replay instead — a cross-DB seed (old shared DB → new metering DB) is the
    // cutover tooling's job, not this single-pool runtime path (docs §3 option 1/2).
    match repo.backfill().await {
        Ok(n) => info!("📥 Owner read-model backfilled {n} row(s) from meters ⋈ users"),
        Err(e) => warn!(
            "⚠️ Owner read-model backfill skipped/failed ({e}); feed continues (events + lazy re-resolve)"
        ),
    }

    let meter_topic =
        std::env::var("READMODEL_METER_TOPIC").unwrap_or_else(|_| "meter_events".to_string());
    let user_topic =
        std::env::var("READMODEL_IAM_TOPIC").unwrap_or_else(|_| "user_events".to_string());

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

    info!("🗃️ Owner read-model feed ENABLED (topics: {user_topic}, {meter_topic})");
    tokio::spawn(async move {
        consumer.run(repo, shutdown).await;
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
        assert_eq!(linked.is_primary, None, "absent is_primary ⇒ None ⇒ treated as primary");

        let demoted: UserEventData = serde_json::from_str(
            r#"{"user_id":"00000000-0000-0000-0000-000000000001","wallet_address":"W","is_primary":false}"#,
        )
        .unwrap();
        assert_eq!(demoted.is_primary, Some(false), "non-primary change must be skippable");
    }

    #[test]
    fn meter_event_data_rejects_bad_uuid() {
        // A bad user_id must fail parse (→ dispatcher logs + skips, never panics).
        assert!(serde_json::from_str::<MeterEventData>(
            r#"{"serial_number":"MTR-1","user_id":"not-a-uuid"}"#
        )
        .is_err());
    }
}
