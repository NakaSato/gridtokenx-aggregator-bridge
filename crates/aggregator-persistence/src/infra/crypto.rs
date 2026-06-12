use anyhow::{anyhow, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
pub struct DeviceKeyRegistry {
    redis_url: Option<String>,
    conn: Arc<Mutex<Option<ConnectionManager>>>,
}

impl DeviceKeyRegistry {
    /// Construct from a Redis URL; connection built on first use and rebuilt on
    /// failure.
    pub fn new(redis_url: Option<String>) -> Self {
        Self {
            redis_url,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Construct from an already-established manager (cannot fully rebuild after
    /// a hard failure without a URL — prefer [`DeviceKeyRegistry::new`]).
    pub fn from_manager(conn: Option<ConnectionManager>) -> Self {
        Self {
            redis_url: None,
            conn: Arc::new(Mutex::new(conn)),
        }
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
                    anyhow!("Redis enckey lookup failed for {} after reconnect: {}", key, e2)
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

    /// Fetch one device's AES-256 key. `Ok(None)` only when the key is genuinely
    /// absent; `Err` on Redis-unreachable (fail-closed) or a malformed/wrong-length
    /// key (never truncate or pad).
    pub async fn get_device_aes_key(&self, meter_id: &str) -> Result<Option<[u8; 32]>> {
        let key = format!("gridtokenx:devices:{}:enckey", meter_id);
        match self.get_with_retry(&key).await? {
            Some(hex_str) => Ok(Some(decode_aes_key_hex(meter_id, &hex_str)?)),
            None => Ok(None),
        }
    }

    /// Batch fetch via a single MGET (same round-trip shape as
    /// [`SignatureVerifier::verify_telemetry_signature_batch`], so decrypt and
    /// sig-verify stay aligned). A genuinely-absent key ⇒ `None`. Unlike the
    /// single path, a *malformed* key for one meter is logged and yields `None`
    /// for that entry so one bad key cannot fail the whole batch; Redis-unreachable
    /// still fails the whole call loudly.
    pub async fn get_device_aes_keys(
        &self,
        meter_ids: &[String],
    ) -> Result<Vec<Option<[u8; 32]>>> {
        let keys: Vec<String> = meter_ids
            .iter()
            .map(|id| format!("gridtokenx:devices:{}:enckey", id))
            .collect();

        let raw: Vec<Option<String>> = self.mget_with_retry(&keys).await?;

        let mut out = Vec::with_capacity(meter_ids.len());
        for (i, slot) in raw.into_iter().enumerate() {
            let parsed = match slot {
                Some(hex_str) => match decode_aes_key_hex(&meter_ids[i], &hex_str) {
                    Ok(k) => Some(k),
                    Err(e) => {
                        warn!("🚫 Skipping malformed enckey in batch: {}", e);
                        None
                    }
                },
                None => None,
            };
            out.push(parsed);
        }
        Ok(out)
    }
}

pub struct SettlementSigner {
    signing_key: SigningKey,
}

impl SettlementSigner {
    pub fn new(private_key_bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; 32] = private_key_bytes
            .try_into()
            .map_err(|_| anyhow!("Invalid private key length"))?;
        let signing_key = SigningKey::from_bytes(&bytes);
        Ok(Self { signing_key })
    }

    pub fn sign_settlement<T: Serialize>(&self, data: &T) -> Result<String> {
        let payload = serde_json::to_vec(data)?;
        let signature = self.signing_key.sign(&payload);
        Ok(bs58::encode(signature.to_bytes()).into_string())
    }

    /// Sign a canonical message string matching the Trading Service's verification format:
    /// "{user_id}:{meter_serial}:{energy_generated_kwh}:{start_time}:{end_time}"
    pub fn sign_canonical(&self, message: &str) -> String {
        let signature = self.signing_key.sign(message.as_bytes());
        bs58::encode(signature.to_bytes()).into_string()
    }

    /// Get the public key as a bs58-encoded string (for AGGREGATOR_BRIDGE_PUBLIC_KEY config)
    pub fn public_key_bs58(&self) -> String {
        let verifying_key = self.signing_key.verifying_key();
        bs58::encode(verifying_key.as_bytes()).into_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementData {
    pub meter_serial: String,
    pub window_start: i64, // 15-min window start (ms)
    pub window_end: i64,   // 15-min window end (ms)
    pub energy_generated: f64,
    pub energy_consumed: f64,
    pub net_energy: f64,
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // A syntactically valid 64-char hex pubkey + base58 sig so the verifier
    // reaches the Redis lookup before any decode error — the lookup is what we
    // exercise here.
    const DUMMY_SIG_B58: &str = "11111111111111111111111111111111111111111111111111111111111111111111";

    /// Security regression guard for the 403-on-all-readings incident: when the
    /// verifier has no way to reach Redis it MUST return a loud `Err`, never a
    /// silent `Ok(false)` (indistinguishable from a forged signature).
    #[tokio::test]
    async fn verify_errors_loud_when_no_redis_url() {
        let v = SignatureVerifier::new(None);
        let res = v
            .verify_telemetry_signature("meter-1", b"payload", DUMMY_SIG_B58)
            .await;
        assert!(res.is_err(), "no-URL verifier must Err, got Ok({:?})", res.ok());
    }

    /// Same guard for a manager-less verifier built via `from_manager(None)`.
    #[tokio::test]
    async fn verify_errors_loud_when_no_manager() {
        let v = SignatureVerifier::from_manager(None);
        let res = v
            .verify_telemetry_signature("meter-1", b"payload", DUMMY_SIG_B58)
            .await;
        assert!(res.is_err(), "manager-less verifier must Err, got Ok({:?})", res.ok());
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
        assert!(matches!(res.as_deref(), Ok(&[])), "empty batch must be Ok([]), got {:?}", res);
    }

    /// `sign_canonical` must produce a base58 Ed25519 signature that verifies
    /// against the signer's own public key for the exact canonical string —
    /// the contract the Trading Service relies on.
    #[test]
    fn settlement_signer_canonical_roundtrip() {
        let signer = SettlementSigner::new(&[7u8; 32]).unwrap();
        let msg = "user:meter:12.5:1000:2000";

        let sig_bytes = bs58::decode(signer.sign_canonical(msg)).into_vec().unwrap();
        let signature = Signature::from_slice(&sig_bytes).unwrap();

        let pk_bytes = bs58::decode(signer.public_key_bs58()).into_vec().unwrap();
        let pk: [u8; 32] = pk_bytes.try_into().unwrap();
        let verifying_key = VerifyingKey::from_bytes(&pk).unwrap();

        assert!(verifying_key.verify(msg.as_bytes(), &signature).is_ok());
        // Tampered message must NOT verify.
        assert!(verifying_key
            .verify(b"user:meter:99.9:1000:2000", &signature)
            .is_err());
    }

    /// Fail-closed-loud guard for the AES key path, mirroring
    /// `verify_errors_loud_when_no_redis_url`: no Redis URL ⇒ `Err`, never a
    /// silent `Ok(None)` (a dead connection must not look like an absent key).
    #[tokio::test]
    async fn get_aes_key_errors_loud_when_no_redis_url() {
        let r = DeviceKeyRegistry::new(None);
        let res = r.get_device_aes_key("meter-1").await;
        assert!(res.is_err(), "no-URL registry must Err, got Ok({:?})", res.ok());
    }

    /// Same guard for the batch path — Redis-unreachable fails the whole call.
    #[tokio::test]
    async fn get_aes_keys_batch_errors_loud_when_no_redis_url() {
        let r = DeviceKeyRegistry::new(None);
        let res = r.get_device_aes_keys(&["meter-1".to_string()]).await;
        assert!(res.is_err(), "no-URL registry must Err, got Ok({:?})", res.ok());
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
