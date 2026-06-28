use anyhow::{anyhow, Context, Result};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Verifies Ed25519 telemetry signatures against device public keys stored in
/// Redis (`gridtokenx:devices:{meter_id}:pubkey`).
///
/// The Redis connection is established lazily and **rebuilt automatically** if
/// the Redis server restarts, so signed telemetry keeps verifying without a
/// bridge restart. When Redis is genuinely unreachable, verification returns a
/// loud `Err` (fail-closed but observable) instead of silently reporting an
/// invalid signature — a silent `Ok(false)` is indistinguishable from a forged
/// signature and previously masked a dead connection for hours.
pub struct SignatureVerifier {
    /// Source URL used to (re)build the connection manager after a failure.
    redis_url: Option<String>,
    /// Cached reconnecting manager; `None` until first use or after invalidation.
    conn: Arc<Mutex<Option<ConnectionManager>>>,
}

impl SignatureVerifier {
    /// Construct from a Redis URL. The connection is established on first use
    /// and transparently rebuilt if it drops (e.g. Redis restart).
    pub fn new(redis_url: Option<String>) -> Self {
        Self {
            redis_url,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Construct from an already-established connection manager. The manager
    /// auto-reconnects, but without a URL it cannot be fully rebuilt after a
    /// hard failure — prefer [`SignatureVerifier::new`].
    pub fn from_manager(conn: Option<ConnectionManager>) -> Self {
        Self {
            redis_url: None,
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    /// Return a live connection manager clone, building it from `redis_url` on
    /// first use or after [`invalidate`](Self::invalidate). Errors loudly when
    /// Redis is unavailable rather than treating it as a bad signature.
    async fn conn(&self) -> Result<ConnectionManager> {
        {
            let guard = self.conn.lock().await;
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let url = self.redis_url.as_ref().ok_or_else(|| {
            anyhow!("SignatureVerifier has no live Redis connection or URL; cannot verify telemetry signatures")
        })?;
        let client = redis::Client::open(url.clone())
            .map_err(|e| anyhow!("Failed to open Redis client {}: {}", url, e))?;
        let mgr = ConnectionManager::new(client)
            .await
            .map_err(|e| anyhow!("Failed to connect to Redis {}: {}", url, e))?;
        let mut guard = self.conn.lock().await;
        *guard = Some(mgr.clone());
        Ok(mgr)
    }

    /// Drop the cached manager so the next call rebuilds from the URL. Used
    /// after a hard error so a Redis restart is recovered without a process
    /// restart. No-op when there is no URL to rebuild from.
    async fn invalidate(&self) {
        if self.redis_url.is_some() {
            let mut guard = self.conn.lock().await;
            *guard = None;
        }
    }

    /// GET a key, rebuilding the connection and retrying once on a transport
    /// error (the recovery path after a Redis restart).
    async fn get_with_retry(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.conn().await?;
        match conn.get::<_, Option<String>>(key).await {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!(
                    "⚠️ Redis lookup error for {} ({}); rebuilding connection and retrying",
                    key, e
                );
                self.invalidate().await;
                let mut conn2 = self.conn().await?;
                conn2.get::<_, Option<String>>(key).await.map_err(|e2| {
                    anyhow!("Redis lookup failed for {} after reconnect: {}", key, e2)
                })
            }
        }
    }

    /// MGET keys, rebuilding the connection and retrying once on a transport error.
    async fn mget_with_retry(&self, keys: &[String]) -> Result<Vec<Option<String>>> {
        let mut conn = self.conn().await?;
        match conn.mget::<_, Vec<Option<String>>>(keys).await {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!(
                    "⚠️ Redis MGET error ({}); rebuilding connection and retrying",
                    e
                );
                self.invalidate().await;
                let mut conn2 = self.conn().await?;
                conn2
                    .mget::<_, Vec<Option<String>>>(keys)
                    .await
                    .map_err(|e2| anyhow!("Redis MGET failed after reconnect: {}", e2))
            }
        }
    }

    pub async fn verify_telemetry_signature(
        &self,
        meter_id: &str,
        payload: &[u8],
        signature_base58: &str,
    ) -> Result<bool> {
        // 1. Lookup device public key from registry (Redis)
        // Key format: gridtokenx:devices:{meter_id}:pubkey
        let key = format!("gridtokenx:devices:{}:pubkey", meter_id);

        let public_key_hex: Option<String> = self.get_with_retry(&key).await?;

        let hex_str = public_key_hex
            .ok_or_else(|| {
                anyhow!(
                    "Public key not found in Redis for meter: {} (Key: {})",
                    meter_id,
                    key
                )
            })?
            .trim()
            .to_string();

        let hex_len = hex_str.len();

        // Handle both raw binary and hex string (32 bytes raw or 64 chars hex)
        let public_key_bytes = if hex_len == 64 {
            hex::decode(&hex_str)
                .map_err(|e| anyhow!("Failed to decode hex public key for {}: {}", meter_id, e))?
        } else {
            hex_str.into_bytes()
        };

        if public_key_bytes.is_empty() {
            return Err(anyhow!(
                "Decoded public key is empty for meter: {} (Hex length: {})",
                meter_id,
                hex_len
            ));
        }

        // 2. Decode signature from base58
        let signature_bytes = bs58::decode(signature_base58)
            .into_vec()
            .map_err(|e| anyhow!("Invalid base58 signature: {}", e))?;

        if signature_bytes.len() != 64 {
            warn!(
                "🚫 Invalid signature length: {} (expected 64) for meter: {}",
                signature_bytes.len(),
                meter_id
            );
            return Ok(false);
        }

        let signature = Signature::from_slice(&signature_bytes)?;

        // 3. Verify signature
        let verifying_key = VerifyingKey::from_bytes(
            &public_key_bytes
                .clone()
                .try_into()
                .map_err(|_| anyhow!("Invalid key length"))?,
        )?;

        let is_valid = verifying_key.verify(payload, &signature).is_ok();

        if !is_valid {
            warn!(
                "🚫 Ed25519 signature verification FAILED for meter: {}",
                meter_id
            );
            debug!("   Payload (string): {}", String::from_utf8_lossy(payload));
            debug!("   Payload (hex): {}", hex::encode(payload));
            debug!("   Public Key (hex): {}", hex::encode(public_key_bytes));
            debug!("   Signature (base58): {}", signature_base58);
        }

        Ok(is_valid)
    }

    /// Batch version of signature verification using MGET for performance.
    pub async fn verify_telemetry_signature_batch(
        &self,
        meter_ids: &[String],
        payloads: &[Vec<u8>],
        signatures: &[[u8; 64]],
    ) -> Result<Vec<bool>> {
        if meter_ids.len() != payloads.len() || meter_ids.len() != signatures.len() {
            return Err(anyhow!("Mismatched batch lengths"));
        }

        // Empty batch ⇒ nothing to verify. Short-circuit BEFORE issuing MGET: Redis
        // rejects a zero-key MGET ("wrong number of arguments"), which would surface a
        // bulk batch whose every frame was decode-skipped as a hard INTERNAL error
        // instead of the fail-closed processed_count==0 the skip path intends.
        if meter_ids.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = meter_ids
            .iter()
            .map(|id| format!("gridtokenx:devices:{}:pubkey", id))
            .collect();

        // Fetch all public keys in one round-trip (rebuild + retry on failure).
        let public_keys_hex: Vec<Option<String>> = self.mget_with_retry(&keys).await?;

        let mut results = Vec::with_capacity(meter_ids.len());

        for i in 0..meter_ids.len() {
            let res = if let Some(hex_str) = &public_keys_hex[i] {
                let public_key_bytes = if hex_str.len() == 64 {
                    hex::decode(hex_str.trim()).unwrap_or_default()
                } else {
                    hex_str.as_bytes().to_vec()
                };

                if public_key_bytes.len() != 32 {
                    false
                } else {
                    let vk_res = VerifyingKey::from_bytes(&public_key_bytes.try_into().unwrap());
                    if let Ok(verifying_key) = vk_res {
                        let signature = Signature::from_bytes(&signatures[i]);
                        verifying_key.verify(&payloads[i], &signature).is_ok()
                    } else {
                        false
                    }
                }
            } else {
                false
            };
            results.push(res);
        }

        Ok(results)
    }
}

/// Decode a hex-encoded AES-256 key to 32 raw bytes. Errors loudly on invalid
/// hex or a wrong length — a truncated/padded symmetric key silently produces
/// garbage plaintext, so we reject rather than coerce.
fn decode_aes_key_hex(meter_id: &str, raw: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(raw.trim())
        .map_err(|e| anyhow!("Failed to decode hex enckey for {}: {}", meter_id, e))?;
    bytes.try_into().map_err(|v: Vec<u8>| {
        anyhow!(
            "enckey for {} has wrong length: {} bytes (expected 32)",
            meter_id,
            v.len()
        )
    })
}

/// Fetches per-device AES-256 symmetric keys from Redis
/// (`gridtokenx:devices:{meter_id}:enckey`, 64-char hex) for decrypting secure
/// UTT-S+ v4 binary DLMS frames.
///
/// Mirrors [`SignatureVerifier`]'s self-healing connection: the Redis URL is
/// owned (not a one-shot connection) and the manager is transparently rebuilt
/// after a transport error, so a Redis restart does not freeze decryption. A
/// genuinely-unreachable Redis returns a loud `Err` (fail-closed), never a
/// silent `Ok(None)` — an absent key and a dead connection must stay
/// distinguishable, exactly as for signature verification.
/// Default seconds a resolved unversioned enckey verdict is trusted before
/// re-reading Redis. Override with `DEVICE_ENCKEY_CACHE_TTL_SECS`. The legacy
/// unversioned key is effectively static (rotation goes through the *versioned*
/// path), so a moderate positive TTL turns a per-frame Redis GET into one GET per
/// device per TTL — the dominant decrypt-path flood under sustained ingest.
const ENCKEY_POSITIVE_TTL_SECS: u64 = 300;

/// Default seconds an *absent* enckey is remembered before re-reading Redis.
/// Override with `DEVICE_ENCKEY_NEG_CACHE_TTL_SECS`. Bounds the flood from frames
/// of an unkeyed device; kept short so a freshly-provisioned key is picked up soon.
const ENCKEY_NEGATIVE_TTL_SECS: u64 = 10;

fn enckey_positive_ttl() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| ttl_env("DEVICE_ENCKEY_CACHE_TTL_SECS", ENCKEY_POSITIVE_TTL_SECS))
}

fn enckey_negative_ttl() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| ttl_env("DEVICE_ENCKEY_NEG_CACHE_TTL_SECS", ENCKEY_NEGATIVE_TTL_SECS))
}

fn ttl_env(var: &'static str, default_secs: u64) -> Duration {
    let secs = std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

/// A cached enckey verdict: `Some(key)` present, `None` genuinely absent.
#[derive(Clone)]
struct CachedEncKey {
    key: Option<[u8; 32]>,
    expires: Instant,
}

pub struct DeviceKeyRegistry {
    redis_url: Option<String>,
    conn: Arc<Mutex<Option<ConnectionManager>>>,
    /// Vault Transit client for unwrapping versioned (rotated) GUEKs. `None`
    /// disables the versioned path (legacy unversioned key only).
    vault: Option<super::vault::VaultTransitClient>,
    /// Cache of unwrapped versioned keys, keyed by `(meter_id, kid)`. A wrapped
    /// GUEK version is immutable, so caching avoids re-hitting Vault per frame.
    versioned_cache: Arc<Mutex<std::collections::HashMap<(String, i64), [u8; 32]>>>,
    /// Hot cache of *unversioned* enckey verdicts (present/absent) with TTLs, so a
    /// keyed device isn't re-read from Redis on every frame. Positive verdicts last
    /// [`enckey_positive_ttl`], absences the shorter [`enckey_negative_ttl`].
    key_cache: Arc<Mutex<HashMap<String, CachedEncKey>>>,
}

impl DeviceKeyRegistry {
    /// Construct from a Redis URL; connection built on first use and rebuilt on
    /// failure.
    pub fn new(redis_url: Option<String>) -> Self {
        Self {
            redis_url,
            conn: Arc::new(Mutex::new(None)),
            vault: None,
            versioned_cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
            key_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Construct from an already-established manager (cannot fully rebuild after
    /// a hard failure without a URL — prefer [`DeviceKeyRegistry::new`]).
    pub fn from_manager(conn: Option<ConnectionManager>) -> Self {
        Self {
            redis_url: None,
            conn: Arc::new(Mutex::new(conn)),
            vault: None,
            versioned_cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
            key_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach a Vault Transit client, enabling versioned (rotated) key lookup.
    pub fn with_vault(mut self, vault: Option<super::vault::VaultTransitClient>) -> Self {
        self.vault = vault;
        self
    }

    async fn conn(&self) -> Result<ConnectionManager> {
        {
            let guard = self.conn.lock().await;
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let url = self.redis_url.as_ref().ok_or_else(|| {
            anyhow!("DeviceKeyRegistry has no live Redis connection or URL; cannot fetch device AES keys")
        })?;
        let client = redis::Client::open(url.clone())
            .map_err(|e| anyhow!("Failed to open Redis client {}: {}", url, e))?;
        let mgr = ConnectionManager::new(client)
            .await
            .map_err(|e| anyhow!("Failed to connect to Redis {}: {}", url, e))?;
        let mut guard = self.conn.lock().await;
        *guard = Some(mgr.clone());
        Ok(mgr)
    }

    async fn invalidate(&self) {
        if self.redis_url.is_some() {
            let mut guard = self.conn.lock().await;
            *guard = None;
        }
    }

    /// GET a key, rebuilding the connection and retrying once on transport error.
    async fn get_with_retry(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.conn().await?;
        match conn.get::<_, Option<String>>(key).await {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!(
                    "⚠️ Redis enckey lookup error for {} ({}); rebuilding connection and retrying",
                    key, e
                );
                self.invalidate().await;
                let mut conn2 = self.conn().await?;
                conn2.get::<_, Option<String>>(key).await.map_err(|e2| {
                    anyhow!(
                        "Redis enckey lookup failed for {} after reconnect: {}",
                        key,
                        e2
                    )
                })
            }
        }
    }

    /// MGET keys, rebuilding the connection and retrying once on transport error.
    async fn mget_with_retry(&self, keys: &[String]) -> Result<Vec<Option<String>>> {
        let mut conn = self.conn().await?;
        match conn.mget::<_, Vec<Option<String>>>(keys).await {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!(
                    "⚠️ Redis enckey MGET error ({}); rebuilding connection and retrying",
                    e
                );
                self.invalidate().await;
                let mut conn2 = self.conn().await?;
                conn2
                    .mget::<_, Vec<Option<String>>>(keys)
                    .await
                    .map_err(|e2| anyhow!("Redis enckey MGET failed after reconnect: {}", e2))
            }
        }
    }

    /// Look up a cached enckey verdict for `meter_id`, dropping it when expired.
    async fn key_cache_get(&self, meter_id: &str) -> Option<Option<[u8; 32]>> {
        let mut cache = self.key_cache.lock().await;
        match cache.get(meter_id) {
            Some(e) if e.expires > Instant::now() => Some(e.key),
            Some(_) => {
                cache.remove(meter_id); // expired — drop so the map stays bounded
                None
            }
            None => None,
        }
    }

    /// Store a verdict for `meter_id`. Present keys last the positive TTL, absences
    /// the shorter negative TTL. Opportunistically prunes expired entries.
    async fn key_cache_put(&self, meter_id: &str, key: Option<[u8; 32]>) {
        let now = Instant::now();
        let ttl = if key.is_some() {
            enckey_positive_ttl()
        } else {
            enckey_negative_ttl()
        };
        let mut cache = self.key_cache.lock().await;
        cache.retain(|_, v| v.expires > now);
        cache.insert(
            meter_id.to_string(),
            CachedEncKey {
                key,
                expires: now + ttl,
            },
        );
    }

    /// Fetch one device's AES-256 key. `Ok(None)` only when the key is genuinely
    /// absent; `Err` on Redis-unreachable (fail-closed) or a malformed/wrong-length
    /// key (never truncate or pad). A recently-resolved verdict (present or absent)
    /// is served from the hot cache, bounding per-frame Redis reads to one per device
    /// per TTL.
    pub async fn get_device_aes_key(&self, meter_id: &str) -> Result<Option<[u8; 32]>> {
        if let Some(cached) = self.key_cache_get(meter_id).await {
            return Ok(cached);
        }
        let key = format!("gridtokenx:devices:{}:enckey", meter_id);
        let resolved = match self.get_with_retry(&key).await? {
            Some(hex_str) => Some(decode_aes_key_hex(meter_id, &hex_str)?),
            None => None,
        };
        // Only cache after a successful Redis round-trip (a malformed key returns
        // Err above and is never cached, preserving fail-closed semantics).
        self.key_cache_put(meter_id, resolved).await;
        Ok(resolved)
    }

    /// Fetch a device's **versioned** AES-256 key (rotated GUEK) for `kid`.
    ///
    /// Reads the Vault-wrapped blob at `gridtokenx:devices:{id}:enckey:v{kid}`,
    /// unwraps it via Vault Transit, and caches the result by `(meter_id, kid)`
    /// (a wrapped version is immutable). `Ok(None)` when that version is absent
    /// (e.g. pruned past the grace window); `Err` when Redis is unreachable, no
    /// Vault client is configured, or the unwrap fails (fail-closed).
    pub async fn get_device_aes_key_versioned(
        &self,
        meter_id: &str,
        kid: i64,
    ) -> Result<Option<[u8; 32]>> {
        let cache_key = (meter_id.to_string(), kid);
        {
            let cache = self.versioned_cache.lock().await;
            if let Some(k) = cache.get(&cache_key) {
                return Ok(Some(*k));
            }
        }

        let redis_key = format!("gridtokenx:devices:{}:enckey:v{}", meter_id, kid);
        let wrapped = match self.get_with_retry(&redis_key).await? {
            Some(w) => w,
            None => return Ok(None),
        };

        let vault = self.vault.as_ref().ok_or_else(|| {
            anyhow!(
                "versioned key v{} present for {} but no Vault client configured to unwrap it",
                kid,
                meter_id
            )
        })?;
        let key = vault
            .unwrap(wrapped.trim())
            .await
            .with_context(|| format!("failed to unwrap GUEK v{} for {}", kid, meter_id))?;

        self.versioned_cache.lock().await.insert(cache_key, key);
        Ok(Some(key))
    }

    /// Batch fetch via a single MGET (same round-trip shape as
    /// [`SignatureVerifier::verify_telemetry_signature_batch`], so decrypt and
    /// sig-verify stay aligned). A genuinely-absent key ⇒ `None`. Unlike the
    /// single path, a *malformed* key for one meter is logged and yields `None`
    /// for that entry so one bad key cannot fail the whole batch; Redis-unreachable
    /// still fails the whole call loudly.
    pub async fn get_device_aes_keys(&self, meter_ids: &[String]) -> Result<Vec<Option<[u8; 32]>>> {
        // Start from the hot cache; only the cache-misses need a Redis MGET.
        let mut out: Vec<Option<[u8; 32]>> = Vec::with_capacity(meter_ids.len());
        let mut miss_idx: Vec<usize> = Vec::new();
        let mut miss_keys: Vec<String> = Vec::new();
        for (i, id) in meter_ids.iter().enumerate() {
            match self.key_cache_get(id).await {
                Some(verdict) => out.push(verdict),
                None => {
                    out.push(None); // placeholder, filled after MGET
                    miss_idx.push(i);
                    miss_keys.push(format!("gridtokenx:devices:{}:enckey", id));
                }
            }
        }
        if miss_keys.is_empty() {
            return Ok(out);
        }

        let raw: Vec<Option<String>> = self.mget_with_retry(&miss_keys).await?;
        for (slot, &i) in raw.into_iter().zip(miss_idx.iter()) {
            let parsed = match slot {
                Some(hex_str) => match decode_aes_key_hex(&meter_ids[i], &hex_str) {
                    Ok(k) => {
                        self.key_cache_put(&meter_ids[i], Some(k)).await;
                        Some(k)
                    }
                    Err(e) => {
                        // Malformed → None for this entry (don't fail the batch) and
                        // don't cache it as "absent": the data is present-but-bad.
                        warn!("🚫 Skipping malformed enckey in batch: {}", e);
                        None
                    }
                },
                None => {
                    self.key_cache_put(&meter_ids[i], None).await;
                    None
                }
            };
            out[i] = parsed;
        }
        Ok(out)
    }

    /// Atomically check-and-bump a meter's monotonic invocation counter, the
    /// anti-replay guard for encrypted telemetry. Returns `Ok(true)` when
    /// `counter` is strictly greater than the last accepted value (and stores it),
    /// `Ok(false)` when it is a replay (`counter <= last`). The compare-and-set is
    /// a single Redis Lua script so concurrent ticks for one meter cannot race a
    /// stale counter through. Redis-unreachable returns a loud `Err` (fail-closed),
    /// mirroring the key-fetch paths.
    ///
    /// Key: `gridtokenx:devices:{meter_id}:ic`.
    pub async fn check_and_bump_counter(&self, meter_id: &str, counter: i64) -> Result<bool> {
        let key = format!("gridtokenx:devices:{}:ic", meter_id);
        // KEYS[1] = ic key, ARGV[1] = incoming counter. Reject (0) when the stored
        // counter is >= incoming; otherwise store incoming and accept (1).
        let script = redis::Script::new(
            r"local cur = redis.call('GET', KEYS[1])
if cur and tonumber(cur) >= tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1",
        );
        let mut conn = self.conn().await?;
        let accepted: i64 = script
            .key(&key)
            .arg(counter)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| anyhow!("IC counter CAS failed for {}: {}", meter_id, e))?;
        Ok(accepted == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Signing primitives used only by the device-key test below; the production
    // verifier path needs verify-only (`Verifier`/`VerifyingKey`), so these are
    // scoped here rather than at crate level.
    use ed25519_dalek::{Signer, SigningKey};

    // A syntactically valid 64-char hex pubkey + base58 sig so the verifier
    // reaches the Redis lookup before any decode error — the lookup is what we
    // exercise here.
    const DUMMY_SIG_B58: &str =
        "11111111111111111111111111111111111111111111111111111111111111111111";

    /// Security regression guard for the 403-on-all-readings incident: when the
    /// verifier has no way to reach Redis it MUST return a loud `Err`, never a
    /// silent `Ok(false)` (indistinguishable from a forged signature).
    #[tokio::test]
    async fn verify_errors_loud_when_no_redis_url() {
        let v = SignatureVerifier::new(None);
        let res = v
            .verify_telemetry_signature("meter-1", b"payload", DUMMY_SIG_B58)
            .await;
        assert!(
            res.is_err(),
            "no-URL verifier must Err, got Ok({:?})",
            res.ok()
        );
    }

    /// Same guard for a manager-less verifier built via `from_manager(None)`.
    #[tokio::test]
    async fn verify_errors_loud_when_no_manager() {
        let v = SignatureVerifier::from_manager(None);
        let res = v
            .verify_telemetry_signature("meter-1", b"payload", DUMMY_SIG_B58)
            .await;
        assert!(
            res.is_err(),
            "manager-less verifier must Err, got Ok({:?})",
            res.ok()
        );
    }

    /// An empty batch must short-circuit to `Ok(vec![])` BEFORE any Redis call —
    /// a zero-key MGET errors ("wrong number of arguments"), which would turn a
    /// bulk batch whose every frame was decode-skipped into a hard error instead
    /// of the intended fail-closed `processed_count == 0`. Holds even with no Redis
    /// (the guard returns before the lookup).
    #[tokio::test]
    async fn verify_batch_empty_is_ok_without_redis() {
        let v = SignatureVerifier::new(None);
        let res = v.verify_telemetry_signature_batch(&[], &[], &[]).await;
        assert!(
            matches!(res.as_deref(), Ok(&[])),
            "empty batch must be Ok([]), got {:?}",
            res
        );
    }

    /// Device Ed25519 primitive — the exact checks `verify_telemetry_signature`
    /// performs once it has the pubkey: a 64-byte signature guard, key load, and
    /// verify against the right vs wrong key. Pure (no Redis): the
    /// pubkey-fetch-then-verify path is exercised end-to-end in the e2e suite;
    /// the Redis-unreachable fail-closed case is `verify_errors_loud_*` above.
    #[test]
    fn device_ed25519_primitive_valid_wrong_key_and_bad_len() {
        let signing = SigningKey::from_bytes(&[3u8; 32]);
        let vk = signing.verifying_key();
        let payload = b"E2E-METER:50:1750000000000";
        let sig = signing.sign(payload);

        // valid — correct key over the signed payload verifies.
        assert!(vk.verify(payload, &sig).is_ok());

        // wrong-key — a different device's pubkey must reject (forgery guard).
        let other_vk = SigningKey::from_bytes(&[9u8; 32]).verifying_key();
        assert!(other_vk.verify(payload, &sig).is_err());

        // tampered payload under the right key must reject.
        assert!(vk.verify(b"E2E-METER:9999:1750000000000", &sig).is_err());

        // bad-len — a signature that is not exactly 64 bytes is rejected before
        // any verify (mirrors the `signature_bytes.len() != 64` guard).
        assert!(Signature::from_slice(&[0u8; 63]).is_err());
        assert!(Signature::from_slice(&[0u8; 65]).is_err());
    }

    /// Fail-closed-loud guard for the AES key path, mirroring
    /// `verify_errors_loud_when_no_redis_url`: no Redis URL ⇒ `Err`, never a
    /// silent `Ok(None)` (a dead connection must not look like an absent key).
    #[tokio::test]
    async fn get_aes_key_errors_loud_when_no_redis_url() {
        let r = DeviceKeyRegistry::new(None);
        let res = r.get_device_aes_key("meter-1").await;
        assert!(
            res.is_err(),
            "no-URL registry must Err, got Ok({:?})",
            res.ok()
        );
    }

    /// Same guard for the batch path — Redis-unreachable fails the whole call.
    #[tokio::test]
    async fn get_aes_keys_batch_errors_loud_when_no_redis_url() {
        let r = DeviceKeyRegistry::new(None);
        let res = r.get_device_aes_keys(&["meter-1".to_string()]).await;
        assert!(
            res.is_err(),
            "no-URL registry must Err, got Ok({:?})",
            res.ok()
        );
    }

    /// A present-but-malformed key is a hard `Err` on the single path (never
    /// truncate/pad a symmetric key).
    #[test]
    fn decode_aes_key_rejects_bad_hex() {
        assert!(decode_aes_key_hex("m", "nothex!!").is_err());
    }

    /// Wrong-length (valid hex, 16 bytes) ⇒ `Err`, not a padded 32-byte key.
    #[test]
    fn decode_aes_key_rejects_wrong_length() {
        let half = "00".repeat(16); // 32 hex chars = 16 bytes
        assert!(decode_aes_key_hex("m", &half).is_err());
    }

    /// Exactly 64 hex chars ⇒ 32 raw bytes, trimmed.
    #[test]
    fn decode_aes_key_accepts_32_bytes() {
        let hexkey = "ab".repeat(32); // 64 chars
        let k = decode_aes_key_hex("m", &format!("  {}\n", hexkey)).unwrap();
        assert_eq!(k, [0xabu8; 32]);
    }

    /// Live Redis: missing enckey ⇒ `Ok(None)`; seeded key ⇒ `Ok(Some(32 bytes))`.
    #[tokio::test]
    #[ignore = "requires REDIS_URL (default redis://localhost:7010)"]
    async fn get_aes_key_against_real_redis() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:7010".to_string());
        let r = DeviceKeyRegistry::new(Some(url.clone()));

        // Missing key ⇒ Ok(None).
        let got = r
            .get_device_aes_key("__nonexistent__")
            .await
            .expect("get_device_aes_key should connect to Redis");
        assert!(got.is_none());

        // Seed a key, read it back as 32 raw bytes, then clean up.
        let meter = "__test_seeded__";
        let hexkey = "cd".repeat(32); // 64 chars ⇒ 32 bytes of 0xcd
        seed_enckey(&url, meter, &hexkey).await;
        let got = r
            .get_device_aes_key(meter)
            .await
            .expect("seeded key should read")
            .expect("seeded key should be present");
        assert_eq!(got, [0xcdu8; 32]);
        del_enckey(&url, meter).await;
    }

    /// Live Redis batch: mixed meters — seeded / absent / malformed ⇒
    /// `[Some(32 bytes), None, None]` (malformed never fails the whole batch).
    #[tokio::test]
    #[ignore = "requires REDIS_URL (default redis://localhost:7010)"]
    async fn get_aes_keys_batch_mixed_against_real_redis() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:7010".to_string());
        let r = DeviceKeyRegistry::new(Some(url.clone()));

        let good = "__batch_good__";
        let bad = "__batch_bad__";
        let absent = "__batch_absent__";
        seed_enckey(&url, good, &"ef".repeat(32)).await; // valid 32-byte key
        seed_enckey(&url, bad, "not-valid-hex").await; // malformed ⇒ None
        del_enckey(&url, absent).await; // ensure absent ⇒ None

        let got = r
            .get_device_aes_keys(&[good.to_string(), bad.to_string(), absent.to_string()])
            .await
            .expect("batch should connect to Redis");

        assert_eq!(got[0], Some([0xefu8; 32]));
        assert_eq!(got[1], None);
        assert_eq!(got[2], None);

        del_enckey(&url, good).await;
        del_enckey(&url, bad).await;
    }

    /// A cached *present* verdict is served without touching Redis — proven by
    /// using a registry with no URL/connection (a Redis hit would `Err`).
    #[tokio::test]
    async fn cached_enckey_served_without_redis() {
        let r = DeviceKeyRegistry::from_manager(None);
        r.key_cache_put("m-cached", Some([0x11u8; 32])).await;
        let got = r
            .get_device_aes_key("m-cached")
            .await
            .expect("cache hit must not reach Redis");
        assert_eq!(got, Some([0x11u8; 32]));
    }

    /// A cached *absent* verdict is also served from cache (negative caching).
    #[tokio::test]
    async fn cached_absent_enckey_served_without_redis() {
        let r = DeviceKeyRegistry::from_manager(None);
        r.key_cache_put("m-absent", None).await;
        let got = r
            .get_device_aes_key("m-absent")
            .await
            .expect("cached absence must not reach Redis");
        assert_eq!(got, None);
        // Uncached meter on a URL-less registry must still Err (fail-closed).
        assert!(r.get_device_aes_key("m-uncached").await.is_err());
    }

    /// An expired cache entry misses and is evicted on lookup.
    #[tokio::test]
    async fn expired_enckey_entry_evicted() {
        let r = DeviceKeyRegistry::from_manager(None);
        r.key_cache.lock().await.insert(
            "m-old".to_string(),
            CachedEncKey {
                key: Some([0x22u8; 32]),
                expires: Instant::now() - Duration::from_secs(1),
            },
        );
        assert!(r.key_cache_get("m-old").await.is_none(), "expired must miss");
        assert!(
            !r.key_cache.lock().await.contains_key("m-old"),
            "expired entry evicted on lookup"
        );
    }

    #[test]
    fn enckey_negative_ttl_shorter_than_positive() {
        assert!(enckey_negative_ttl() < enckey_positive_ttl());
        assert!(ENCKEY_NEGATIVE_TTL_SECS < ENCKEY_POSITIVE_TTL_SECS);
    }

    async fn seed_enckey(url: &str, meter: &str, hexkey: &str) {
        let client = redis::Client::open(url).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!("gridtokenx:devices:{}:enckey", meter);
        let _: () = conn.set(&key, hexkey).await.unwrap();
    }

    async fn del_enckey(url: &str, meter: &str) {
        let client = redis::Client::open(url).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!("gridtokenx:devices:{}:enckey", meter);
        let _: () = conn.del(&key).await.unwrap();
    }

    async fn del_ic(url: &str, meter: &str) {
        let client = redis::Client::open(url).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!("gridtokenx:devices:{}:ic", meter);
        let _: () = conn.del(&key).await.unwrap();
    }

    /// Live Redis: the invocation-counter CAS accepts a strictly-increasing
    /// sequence and rejects any counter <= the last accepted (replay guard).
    #[tokio::test]
    #[ignore = "requires REDIS_URL (default redis://localhost:7010)"]
    async fn check_and_bump_counter_accepts_increasing_rejects_replay() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:7010".to_string());
        let r = DeviceKeyRegistry::new(Some(url.clone()));
        let meter = "__ic_test__";
        del_ic(&url, meter).await; // clean slate

        // First counter accepted (no prior value).
        assert!(r.check_and_bump_counter(meter, 100).await.unwrap());
        // Strictly greater accepted.
        assert!(r.check_and_bump_counter(meter, 101).await.unwrap());
        // Equal rejected (replay).
        assert!(!r.check_and_bump_counter(meter, 101).await.unwrap());
        // Lower rejected (replay).
        assert!(!r.check_and_bump_counter(meter, 50).await.unwrap());
        // Jump ahead accepted.
        assert!(r.check_and_bump_counter(meter, 200).await.unwrap());

        del_ic(&url, meter).await;
    }

    /// Fail-closed: no Redis URL ⇒ the CAS errors loudly, never silently accepts.
    #[tokio::test]
    async fn check_and_bump_counter_errors_loud_when_no_redis_url() {
        let r = DeviceKeyRegistry::new(None);
        assert!(r.check_and_bump_counter("m", 1).await.is_err());
    }

    /// Live reconnect check — exercises `get_with_retry` against a real Redis.
    /// Ignored by default; run with `cargo test -- --ignored` and Redis up.
    #[tokio::test]
    #[ignore = "requires REDIS_URL (default redis://localhost:7010)"]
    async fn get_with_retry_reads_against_real_redis() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:7010".to_string());
        let v = SignatureVerifier::new(Some(url));
        // Missing key => Ok(None) proves the connection built and the round-trip
        // succeeded without panicking.
        let got = v
            .get_with_retry("gridtokenx:test:__nonexistent__:pubkey")
            .await
            .expect("get_with_retry should connect to Redis");
        assert!(got.is_none());
    }
}
