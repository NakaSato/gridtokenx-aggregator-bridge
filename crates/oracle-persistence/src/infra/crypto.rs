use anyhow::{anyhow, Result};
use ed25519_dalek::{SecretKey, Signature, Signer, SigningKey, Verifier, VerifyingKey};
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

    /// Get the public key as a bs58-encoded string (for ORACLE_BRIDGE_PUBLIC_KEY config)
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
