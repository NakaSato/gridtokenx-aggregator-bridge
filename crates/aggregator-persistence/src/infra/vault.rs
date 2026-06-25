use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Deserialize;

/// Minimal Vault Transit client for **unwrapping** per-meter GUEKs.
///
/// The simulator wraps each random GUEK with the Transit KEK
/// (`VAULT_METER_KEK_NAME`) and seeds only the `vault:v1:…` ciphertext into
/// Redis. The bridge calls [`unwrap`](Self::unwrap) to recover the 32-byte key
/// for AES-256-GCM frame decryption — the raw key never lives at rest.
///
/// Fail-closed: any transport/HTTP/parse/length error returns `Err`, so a
/// caller never decrypts with a truncated or empty key.
#[derive(Clone)]
pub struct VaultTransitClient {
    http: reqwest::Client,
    addr: String,
    token: String,
    kek_name: String,
}

#[derive(Deserialize)]
struct DecryptResponse {
    data: DecryptData,
}

#[derive(Deserialize)]
struct DecryptData {
    plaintext: String,
}

impl VaultTransitClient {
    /// Build from Vault address, token, and the Transit KEK key name.
    pub fn new(
        addr: impl Into<String>,
        token: impl Into<String>,
        kek_name: impl Into<String>,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            addr: addr.into().trim_end_matches('/').to_string(),
            token: token.into(),
            kek_name: kek_name.into(),
        }
    }

    /// Build from the standard env (`VAULT_ADDR`, `VAULT_TOKEN`,
    /// `VAULT_METER_KEK_NAME`). Returns `None` when `VAULT_ADDR` is unset — the
    /// caller then runs without key rotation (legacy unversioned key only).
    pub fn from_env() -> Option<Self> {
        let addr = std::env::var("VAULT_ADDR").ok().filter(|s| !s.is_empty())?;
        let token = std::env::var("VAULT_TOKEN").unwrap_or_default();
        let kek_name = std::env::var("VAULT_METER_KEK_NAME")
            .unwrap_or_else(|_| "gridtokenx-meter-kek".to_string());
        Some(Self::new(addr, token, kek_name))
    }

    /// Unwrap a `vault:v1:…` ciphertext back to the raw 32-byte AES-256 key.
    pub async fn unwrap(&self, ciphertext: &str) -> Result<[u8; 32]> {
        let url = format!("{}/v1/transit/decrypt/{}", self.addr, self.kek_name);
        let resp = self
            .http
            .post(&url)
            .header("X-Vault-Token", &self.token)
            .json(&serde_json::json!({ "ciphertext": ciphertext }))
            .send()
            .await
            .context("Vault Transit decrypt request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Vault decrypt returned {}: {}", status, body));
        }

        let parsed: DecryptResponse = resp
            .json()
            .await
            .context("Vault decrypt response was not the expected JSON")?;
        let raw = STANDARD
            .decode(parsed.data.plaintext.trim())
            .context("Vault plaintext was not valid base64")?;
        raw.try_into().map_err(|v: Vec<u8>| {
            anyhow!("unwrapped key has wrong length: {} (expected 32)", v.len())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrypt_response_parses_plaintext() {
        let body = r#"{"data":{"plaintext":"AQIDBA=="}}"#;
        let parsed: DecryptResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.data.plaintext, "AQIDBA==");
    }

    #[test]
    fn from_env_none_without_addr() {
        // No VAULT_ADDR in the test env -> None (rotation disabled).
        std::env::remove_var("VAULT_ADDR");
        assert!(VaultTransitClient::from_env().is_none());
    }

    /// Live Vault (dev): wrap a key out-of-band, then unwrap it back to 32 bytes.
    /// Ignored by default; run with Vault up + the KEK provisioned.
    #[tokio::test]
    #[ignore = "requires VAULT_ADDR (default http://localhost:13001) + provisioned KEK"]
    async fn unwrap_round_trips_against_real_vault() {
        let addr =
            std::env::var("VAULT_ADDR").unwrap_or_else(|_| "http://localhost:13001".to_string());
        let token = std::env::var("VAULT_TOKEN").unwrap_or_else(|_| "root".to_string());
        let kek = std::env::var("VAULT_METER_KEK_NAME")
            .unwrap_or_else(|_| "gridtokenx-meter-kek".to_string());

        // Wrap a known key via the same Transit endpoint the sim uses.
        let key = [0x42u8; 32];
        let http = reqwest::Client::new();
        let wrap: serde_json::Value = http
            .post(format!("{}/v1/transit/encrypt/{}", addr, kek))
            .header("X-Vault-Token", &token)
            .json(&serde_json::json!({ "plaintext": STANDARD.encode(key) }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ct = wrap["data"]["ciphertext"].as_str().unwrap();

        let client = VaultTransitClient::new(addr, token, kek);
        let got = client.unwrap(ct).await.unwrap();
        assert_eq!(got, key);
    }
}
