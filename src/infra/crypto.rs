use anyhow::{anyhow, Result};
use ed25519_dalek::{SecretKey, Signature, Signer, SigningKey, Verifier, VerifyingKey};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

pub struct SignatureVerifier {
    redis: ConnectionManager,
}

impl SignatureVerifier {
    pub fn new(redis: ConnectionManager) -> Self {
        Self { redis }
    }

    pub async fn verify_telemetry_signature(
        &self,
        meter_id: &str,
        payload: &[u8],
        signature_base58: &str,
    ) -> Result<bool> {
        // 1. Lookup device public key from registry (Redis)
        // Key format: gridtokenx:devices:{meter_id}:pubkey
        let mut conn = self.redis.clone();
        let key = format!("gridtokenx:devices:{}:pubkey", meter_id);

        let public_key_hex: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| anyhow!("Redis lookup failed for {}: {}", key, e))?;

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
