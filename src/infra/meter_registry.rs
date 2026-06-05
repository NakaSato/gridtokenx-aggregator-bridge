use anyhow::{anyhow, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Cached meter-to-user ID resolver.
///
/// Resolves meter_serial → user_id using a two-tier cache:
/// 1. Local in-memory HashMap (fastest)
/// 2. Redis lookup at `gridtokenx:meters:{serial}:user_id` (shared across instances)
///
/// If neither has the mapping, returns None and the settlement worker
/// will skip that bin (prosumer must register their meter first).
pub struct MeterRegistry {
    redis: Option<ConnectionManager>,
    local_cache: RwLock<HashMap<String, Uuid>>,
}

impl MeterRegistry {
    pub fn new(redis: Option<ConnectionManager>) -> Self {
        Self {
            redis,
            local_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Resolve user_id for a given meter serial number.
    /// Returns None if no mapping is found.
    pub async fn resolve_user_id(&self, meter_serial: &str) -> Result<Option<Uuid>> {
        // 1. Check local cache first
        {
            let cache = self.local_cache.read().await;
            if let Some(uid) = cache.get(meter_serial) {
                return Ok(Some(*uid));
            }
        }

        // 2. Check Redis
        let mut conn = match &self.redis {
            Some(conn) => conn.clone(),
            None => return Ok(Some(Uuid::nil())),
        };
        let key = format!("gridtokenx:meters:{}:user_id", meter_serial);

        let user_id_str: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| anyhow!("Redis lookup failed for {}: {}", key, e))?;

        if let Some(uid_str) = user_id_str {
            match Uuid::parse_str(&uid_str) {
                Ok(uid) => {
                    // Cache locally for future lookups
                    let mut cache = self.local_cache.write().await;
                    cache.insert(meter_serial.to_string(), uid);
                    debug!("🔍 Resolved meter {} → user {}", meter_serial, uid);
                    return Ok(Some(uid));
                }
                Err(e) => {
                    warn!("⚠️ Invalid UUID in Redis for meter {}: {}", meter_serial, e);
                }
            }
        }

        Ok(None)
    }

    /// Register a meter → user mapping (called during meter registration flow)
    pub async fn register_meter(&self, meter_serial: &str, user_id: Uuid) -> Result<()> {
        if let Some(conn) = &self.redis {
            let mut conn = conn.clone();
            let key = format!("gridtokenx:meters:{}:user_id", meter_serial);

            conn.set::<_, _, ()>(&key, user_id.to_string())
                .await
                .map_err(|e| anyhow!("Failed to register meter in Redis: {}", e))?;
        }

        // Update local cache
        let mut cache = self.local_cache.write().await;
        cache.insert(meter_serial.to_string(), user_id);

        info!("📝 Registered meter {} → user {}", meter_serial, user_id);
        Ok(())
    }

    /// Get cache statistics
    pub async fn cache_size(&self) -> usize {
        self.local_cache.read().await.len()
    }
}
