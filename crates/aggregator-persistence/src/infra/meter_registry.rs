use anyhow::{anyhow, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Default seconds an *unattributed* meter serial is remembered as "not found" before
/// re-querying the backends. Override with `METER_REGISTRY_NEG_CACHE_TTL_SECS`.
/// `resolve_user_id` runs once per inbound reading; without this, a fleet of
/// unregistered meters re-queries Redis **and** Postgres on every reading, flooding
/// both. The TTL is kept short so a meter registered out-of-band (meter-service writes
/// Postgres directly — the bridge never sees `register_meter`) is picked up within ≤ TTL.
const METER_NEG_CACHE_TTL_SECS: u64 = 30;

fn neg_cache_ttl() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| {
        let secs = std::env::var("METER_REGISTRY_NEG_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(METER_NEG_CACHE_TTL_SECS);
        Duration::from_secs(secs)
    })
}

/// Cached meter-to-owner resolver.
///
/// Resolves meter_serial → user_id (+ owner wallet) using a three-tier lookup:
/// 1. Local in-memory HashMap (fastest)
/// 2. Redis at `gridtokenx:meters:{serial}:user_id` / `:wallet` (shared, hot cache)
/// 3. **Postgres** (`meters` JOIN `users`) — the durable source of truth, written by
///    the meter-service registration API. On a Postgres hit the result is backfilled
///    into Redis + the local cache, so Redis acts as a self-populating cache and a
///    flush/restart never loses ownership (the DB still has it).
///
/// If no backend has the mapping, returns `None` and the reading is unattributed
/// (the prosumer must register their meter first). When **no** registry backend is
/// configured at all (no Redis and no Postgres), `resolve_user_id` keeps the legacy
/// degraded fallback of attributing to the nil user.
pub struct MeterRegistry {
    redis: Option<ConnectionManager>,
    /// Durable owner source of truth (shared gridtokenx Postgres). Read-only.
    pg: Option<PgPool>,
    local_cache: RwLock<HashMap<String, Uuid>>,
    /// meter_serial → owner wallet address (mint recipient). Backfilled from Redis
    /// (`gridtokenx:meters:{serial}:wallet`) or Postgres on first resolve.
    wallet_cache: RwLock<HashMap<String, String>>,
    /// meter_serial → expiry for serials that missed *every* tier. Bounds the
    /// Redis+Postgres re-query rate for unattributed meters under sustained ingest.
    /// Entries expire after [`neg_cache_ttl`] and are dropped on a later positive
    /// resolution / registration.
    neg_cache: RwLock<HashMap<String, Instant>>,
}

impl MeterRegistry {
    pub fn new(redis: Option<ConnectionManager>, pg: Option<PgPool>) -> Self {
        Self {
            redis,
            pg,
            local_cache: RwLock::new(HashMap::new()),
            wallet_cache: RwLock::new(HashMap::new()),
            neg_cache: RwLock::new(HashMap::new()),
        }
    }

    /// True if `serial` has a live "not found" marker. Drops the entry when expired.
    async fn negatively_cached(&self, serial: &str) -> bool {
        {
            let cache = self.neg_cache.read().await;
            match cache.get(serial) {
                Some(exp) if *exp > Instant::now() => return true,
                Some(_) => {}      // expired — fall through to remove under write lock
                None => return false,
            }
        }
        self.neg_cache.write().await.remove(serial);
        false
    }

    /// Mark `serial` as "not found" for [`neg_cache_ttl`]. Opportunistically prunes
    /// expired entries so the map stays bounded to currently-unattributed serials.
    async fn cache_negative(&self, serial: &str) {
        let now = Instant::now();
        let mut cache = self.neg_cache.write().await;
        cache.retain(|_, exp| *exp > now);
        cache.insert(serial.to_string(), now + neg_cache_ttl());
    }

    /// Drop any "not found" marker for `serial` (it just resolved / registered).
    async fn clear_negative(&self, serial: &str) {
        self.neg_cache.write().await.remove(serial);
    }

    /// Fetch `(user_id, wallet)` for a serial from the durable Postgres source.
    /// Returns `Ok(None)` when Postgres is not configured or the serial is unknown.
    /// The wallet is `Option` because `users.wallet_address` may be NULL/empty.
    async fn fetch_owner_from_db(
        &self,
        meter_serial: &str,
    ) -> Result<Option<(Uuid, Option<String>)>> {
        let pool = match &self.pg {
            Some(p) => p,
            None => return Ok(None),
        };
        let row: Option<(Uuid, Option<String>)> = sqlx::query_as(
            "SELECT m.user_id, u.wallet_address \
             FROM meters m JOIN users u ON u.id = m.user_id \
             WHERE m.serial_number = $1",
        )
        .bind(meter_serial)
        .fetch_optional(pool)
        .await
        .map_err(|e| anyhow!("Postgres owner lookup failed for {}: {}", meter_serial, e))?;
        Ok(row)
    }

    /// Populate the local caches and (best-effort) Redis from a durable Postgres
    /// hit, so subsequent resolves are served from the hot tiers. Redis write
    /// failures are logged but never fail resolution — Redis is only a cache here.
    async fn backfill(&self, meter_serial: &str, user_id: Uuid, wallet: Option<&str>) {
        // A positive resolution supersedes any prior "not found" marker.
        self.clear_negative(meter_serial).await;
        self.local_cache
            .write()
            .await
            .insert(meter_serial.to_string(), user_id);
        let wallet = wallet.filter(|w| !w.trim().is_empty());
        if let Some(w) = wallet {
            self.wallet_cache
                .write()
                .await
                .insert(meter_serial.to_string(), w.to_string());
        }

        if let Some(conn) = &self.redis {
            let mut conn = conn.clone();
            let ukey = format!("gridtokenx:meters:{}:user_id", meter_serial);
            if let Err(e) = conn.set::<_, _, ()>(&ukey, user_id.to_string()).await {
                warn!(
                    "⚠️ Redis backfill (user_id) failed for {}: {}",
                    meter_serial, e
                );
            }
            if let Some(w) = wallet {
                let wkey = format!("gridtokenx:meters:{}:wallet", meter_serial);
                if let Err(e) = conn.set::<_, _, ()>(&wkey, w.to_string()).await {
                    warn!(
                        "⚠️ Redis backfill (wallet) failed for {}: {}",
                        meter_serial, e
                    );
                }
            }
        }
        debug!("📥 Backfilled meter {} owner from Postgres", meter_serial);
    }

    /// Resolve user_id for a given meter serial number.
    /// Returns None if no mapping is found in any configured backend.
    pub async fn resolve_user_id(&self, meter_serial: &str) -> Result<Option<Uuid>> {
        // 1. Check local cache first
        {
            let cache = self.local_cache.read().await;
            if let Some(uid) = cache.get(meter_serial) {
                return Ok(Some(*uid));
            }
        }

        // 1b. Recently-missed serial: skip the Redis + Postgres round-trips. Bounds
        //     the backend query rate for unattributed meters under sustained ingest.
        if self.negatively_cached(meter_serial).await {
            return Ok(None);
        }

        // 2. Check Redis (hot cache, shared across instances)
        if let Some(conn) = &self.redis {
            let mut conn = conn.clone();
            let key = format!("gridtokenx:meters:{}:user_id", meter_serial);
            let user_id_str: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| anyhow!("Redis lookup failed for {}: {}", key, e))?;
            if let Some(uid_str) = user_id_str {
                match Uuid::parse_str(&uid_str) {
                    Ok(uid) => {
                        self.local_cache
                            .write()
                            .await
                            .insert(meter_serial.to_string(), uid);
                        debug!("🔍 Resolved meter {} → user {} (redis)", meter_serial, uid);
                        return Ok(Some(uid));
                    }
                    Err(e) => {
                        warn!("⚠️ Invalid UUID in Redis for meter {}: {}", meter_serial, e);
                    }
                }
            }
        }

        // 3. Postgres — durable source of truth (meter-service registration). On a
        //    hit, backfill Redis + local cache so later resolves hit the hot tiers.
        if let Some((uid, wallet)) = self.fetch_owner_from_db(meter_serial).await? {
            self.backfill(meter_serial, uid, wallet.as_deref()).await;
            debug!(
                "🔍 Resolved meter {} → user {} (postgres)",
                meter_serial, uid
            );
            return Ok(Some(uid));
        }

        // 4. No registry backend configured at all → legacy degraded fallback:
        //    attribute to the nil user so ingest still flows in pure-local dev.
        if self.redis.is_none() && self.pg.is_none() {
            return Ok(Some(Uuid::nil()));
        }

        // Backend(s) configured but the serial is unknown everywhere — remember the
        // miss briefly so the next readings don't re-hit Redis + Postgres.
        self.cache_negative(meter_serial).await;
        Ok(None)
    }

    /// Resolve the owner wallet address for a meter serial (mint recipient).
    ///
    /// Two-tier cache mirroring [`resolve_user_id`](Self::resolve_user_id), reading
    /// `gridtokenx:meters:{serial}:wallet`. Returns `None` (skip the mint) when no
    /// wallet has been registered for the serial.
    pub async fn resolve_wallet(&self, meter_serial: &str) -> Result<Option<String>> {
        // 1. Local cache first.
        {
            let cache = self.wallet_cache.read().await;
            if let Some(w) = cache.get(meter_serial) {
                return Ok(Some(w.clone()));
            }
        }

        // 2. Redis lookup (hot cache). Skipped when Redis is not configured.
        if let Some(conn) = &self.redis {
            let mut conn = conn.clone();
            let key = format!("gridtokenx:meters:{}:wallet", meter_serial);
            let wallet: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| anyhow!("Redis wallet lookup failed for {}: {}", key, e))?;
            if let Some(w) = &wallet {
                if !w.trim().is_empty() {
                    self.wallet_cache
                        .write()
                        .await
                        .insert(meter_serial.to_string(), w.clone());
                    debug!("🔍 Resolved meter {} → wallet {} (redis)", meter_serial, w);
                    return Ok(Some(w.clone()));
                }
            }
        }

        // 3. Postgres — durable source of truth. Backfill caches (incl. user_id) on
        //    a hit; only return a wallet when the owner actually has a non-empty one.
        if let Some((uid, db_wallet)) = self.fetch_owner_from_db(meter_serial).await? {
            self.backfill(meter_serial, uid, db_wallet.as_deref()).await;
            if let Some(w) = db_wallet.filter(|w| !w.trim().is_empty()) {
                debug!(
                    "🔍 Resolved meter {} → wallet {} (postgres)",
                    meter_serial, w
                );
                return Ok(Some(w));
            }
        }

        Ok(None)
    }

    /// Register a meter → user mapping (called during meter registration flow).
    /// When `wallet` is provided it is also stored at
    /// `gridtokenx:meters:{serial}:wallet` so surplus mints can be credited.
    pub async fn register_meter(
        &self,
        meter_serial: &str,
        user_id: Uuid,
        wallet: Option<&str>,
    ) -> Result<()> {
        if let Some(conn) = &self.redis {
            let mut conn = conn.clone();
            let key = format!("gridtokenx:meters:{}:user_id", meter_serial);

            conn.set::<_, _, ()>(&key, user_id.to_string())
                .await
                .map_err(|e| anyhow!("Failed to register meter in Redis: {}", e))?;

            if let Some(w) = wallet.filter(|w| !w.trim().is_empty()) {
                let wkey = format!("gridtokenx:meters:{}:wallet", meter_serial);
                conn.set::<_, _, ()>(&wkey, w.to_string())
                    .await
                    .map_err(|e| anyhow!("Failed to register meter wallet in Redis: {}", e))?;
            }
        }

        // Update local caches
        self.local_cache
            .write()
            .await
            .insert(meter_serial.to_string(), user_id);
        if let Some(w) = wallet.filter(|w| !w.trim().is_empty()) {
            self.wallet_cache
                .write()
                .await
                .insert(meter_serial.to_string(), w.to_string());
        }

        // Newly registered — drop any stale "not found" marker so the next read
        // resolves immediately instead of waiting out the negative TTL.
        self.clear_negative(meter_serial).await;

        info!("📝 Registered meter {} → user {}", meter_serial, user_id);
        Ok(())
    }

    /// Get cache statistics
    pub async fn cache_size(&self) -> usize {
        self.local_cache.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Redis=None exercises the local-cache + skip paths without infra. The
    // Redis-backed branches need a live connection (integration coverage).

    #[tokio::test]
    async fn resolve_wallet_none_when_unregistered_and_no_redis() {
        let reg = MeterRegistry::new(None, None);
        // No wallet registered + no Redis ⇒ None ⇒ mint is skipped (bin still evicts).
        assert_eq!(reg.resolve_wallet("MTR-001").await.unwrap(), None);
    }

    #[tokio::test]
    async fn register_then_resolve_wallet_hits_local_cache() {
        let reg = MeterRegistry::new(None, None);
        reg.register_meter("MTR-001", Uuid::nil(), Some("WALLET123"))
            .await
            .unwrap();
        assert_eq!(
            reg.resolve_wallet("MTR-001").await.unwrap(),
            Some("WALLET123".to_string()),
            "registered wallet resolves from local cache"
        );
    }

    #[tokio::test]
    async fn empty_or_whitespace_wallet_is_not_cached() {
        let reg = MeterRegistry::new(None, None);
        // Blank wallet must not be stored — would otherwise mint to "" and fail.
        reg.register_meter("MTR-001", Uuid::nil(), Some("   "))
            .await
            .unwrap();
        assert_eq!(reg.resolve_wallet("MTR-001").await.unwrap(), None);

        reg.register_meter("MTR-002", Uuid::nil(), Some(""))
            .await
            .unwrap();
        assert_eq!(reg.resolve_wallet("MTR-002").await.unwrap(), None);
    }

    #[tokio::test]
    async fn register_without_wallet_resolves_user_but_not_wallet() {
        let reg = MeterRegistry::new(None, None);
        let uid = Uuid::from_u128(7);
        reg.register_meter("MTR-001", uid, None).await.unwrap();
        assert_eq!(reg.resolve_user_id("MTR-001").await.unwrap(), Some(uid));
        assert_eq!(
            reg.resolve_wallet("MTR-001").await.unwrap(),
            None,
            "no wallet ⇒ skip mint"
        );
    }

    #[tokio::test]
    async fn unregistered_user_id_defaults_to_nil_without_redis() {
        let reg = MeterRegistry::new(None, None);
        // No mapping + no Redis ⇒ attributed to nil user (documented fallback).
        assert_eq!(
            reg.resolve_user_id("UNKNOWN").await.unwrap(),
            Some(Uuid::nil())
        );
    }

    #[tokio::test]
    async fn negative_cache_marks_and_clears() {
        let reg = MeterRegistry::new(None, None);
        assert!(!reg.negatively_cached("MTR-X").await, "unknown serial not cached");
        reg.cache_negative("MTR-X").await;
        assert!(
            reg.negatively_cached("MTR-X").await,
            "missed serial must be negatively cached within TTL"
        );
        reg.clear_negative("MTR-X").await;
        assert!(
            !reg.negatively_cached("MTR-X").await,
            "clear_negative must drop the marker"
        );
    }

    #[tokio::test]
    async fn expired_negative_entry_misses_and_is_evicted() {
        let reg = MeterRegistry::new(None, None);
        // Insert an already-expired marker directly, bypassing cache_negative's TTL.
        reg.neg_cache
            .write()
            .await
            .insert("MTR-OLD".to_string(), Instant::now() - Duration::from_secs(1));
        assert!(!reg.negatively_cached("MTR-OLD").await, "expired marker must miss");
        assert!(
            !reg.neg_cache.read().await.contains_key("MTR-OLD"),
            "expired marker must be evicted on lookup"
        );
    }

    #[tokio::test]
    async fn register_clears_negative_marker() {
        let reg = MeterRegistry::new(None, None);
        reg.cache_negative("MTR-REG").await;
        reg.register_meter("MTR-REG", Uuid::from_u128(9), None)
            .await
            .unwrap();
        assert!(
            !reg.negatively_cached("MTR-REG").await,
            "registration must drop the stale not-found marker"
        );
    }

    #[test]
    fn neg_cache_ttl_is_positive() {
        assert!(neg_cache_ttl() > Duration::ZERO);
        assert!(METER_NEG_CACHE_TTL_SECS > 0);
    }

    /// With NO Redis, the Postgres tier alone resolves a registered meter to its
    /// owner (user_id + wallet) and backfills the local cache. Read-only: borrows
    /// an already-registered meter whose owner has a wallet; mutates nothing.
    /// DB-gated like the meter-service e2e suite — run with `--ignored` against a
    /// live stack that has at least one registered meter.
    #[tokio::test]
    #[ignore = "requires live Postgres with a registered meter"]
    async fn resolve_from_postgres_tier_and_backfills_cache() {
        use sqlx::postgres::PgPoolOptions;
        let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx".to_string()
        });
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&db)
            .await
            .expect("connect Postgres");

        // Borrow an existing meter whose owner has a non-empty wallet — exactly the
        // shape resolve_user_id/resolve_wallet query. Read-only.
        let row: Option<(String, Uuid, Option<String>)> = sqlx::query_as(
            "SELECT m.serial_number, m.user_id, u.wallet_address \
             FROM meters m JOIN users u ON u.id = m.user_id \
             WHERE u.wallet_address IS NOT NULL AND u.wallet_address <> '' LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .expect("query registered meter");
        let Some((serial, user_id, wallet)) = row else {
            eprintln!("SKIP: no registered meter with a wallet-bearing owner in DB");
            return;
        };

        // No Redis ⇒ resolution must come from the Postgres tier.
        let reg = MeterRegistry::new(None, Some(pool.clone()));
        let got_uid = reg.resolve_user_id(&serial).await.expect("resolve uid");
        let got_wallet = reg.resolve_wallet(&serial).await.expect("resolve wallet");

        assert_eq!(
            got_uid,
            Some(user_id),
            "user_id resolved from Postgres tier"
        );
        assert_eq!(
            got_wallet,
            wallet.filter(|w| !w.trim().is_empty()),
            "wallet resolved from Postgres tier"
        );
        // A Postgres hit backfills the local cache, so the serial is now cached.
        assert!(
            reg.cache_size().await >= 1,
            "Postgres hit should backfill the local cache"
        );
    }
}
