use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use super::vault::VaultTransitClient;

/// Redis key holding the Vault-wrapped fleet-wide stream encryption key (SEK).
const SEK_REDIS_KEY: &str = "gridtokenx:stream:sek";

/// AES-256-GCM cipher for at-rest encryption of Redis stream payloads.
///
/// One fleet-wide symmetric **SEK** encrypts every meter reading the bridge
/// disseminates to the zone/unified streams; the in-process zone ingester
/// decrypts it. The SEK never sits in Redis in the clear — only its
/// Vault-KEK-wrapped form — and it is stable across bridge restarts so entries
/// already in a stream stay decryptable.
#[derive(Clone)]
pub struct StreamCipher {
    sek: [u8; 32],
}

impl StreamCipher {
    pub fn new(sek: [u8; 32]) -> Self {
        Self { sek }
    }

    /// Encrypt `plaintext` (the serialized DeviceReading) with the SEK; returns
    /// `(nonce_b64, ciphertext_b64)`. `aad` binds the envelope's `event_type`.
    pub fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<(String, String)> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.sek));
        let mut nonce = [0u8; 12];
        getrandom::getrandom(&mut nonce).map_err(|e| anyhow!("nonce RNG failed: {}", e))?;
        let ct = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| anyhow!("stream encrypt failed: {}", e))?;
        Ok((STANDARD.encode(nonce), STANDARD.encode(ct)))
    }

    /// Decrypt a `(nonce_b64, ciphertext_b64)` pair back to plaintext bytes.
    pub fn decrypt(&self, nonce_b64: &str, ct_b64: &str, aad: &[u8]) -> Result<Vec<u8>> {
        let nonce = STANDARD
            .decode(nonce_b64.trim())
            .context("stream nonce not base64")?;
        if nonce.len() != 12 {
            return Err(anyhow!("stream nonce must be 12 bytes"));
        }
        let ct = STANDARD
            .decode(ct_b64.trim())
            .context("stream ciphertext not base64")?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.sek));
        cipher
            .decrypt(Nonce::from_slice(&nonce), Payload { msg: &ct, aad })
            .map_err(|e| anyhow!("stream GCM auth/decrypt failed: {}", e))
    }
}

/// Get-or-create the fleet-wide SEK, returning a ready [`StreamCipher`].
///
/// On first run: generate 32 random bytes, Vault-KEK-wrap them, and `SET NX`
/// the wrapped blob at `gridtokenx:stream:sek` (NX so concurrent bridge
/// instances converge on one SEK). On every run after: read the wrapped blob
/// and Vault-unwrap it — so the SEK is stable across restarts and old stream
/// entries remain decryptable. Fail-closed: Vault/Redis errors propagate,
/// EXCEPT an unwrappable stored blob (KEK destroyed, e.g. dev Vault restart),
/// which rotates to a fresh SEK instead of crash-looping the bridge.
pub async fn load_or_create_stream_cipher(
    vault: &VaultTransitClient,
    conn: &mut ConnectionManager,
) -> Result<StreamCipher> {
    // A stored blob wrapped by a KEK that no longer exists (dev Vault is
    // in-memory — a Vault restart destroys the transit key) can NEVER be
    // recovered; treat it as dead and rotate rather than crash-looping the
    // whole bridge on boot.
    let mut rotate_dead_blob = false;
    if let Some(wrapped) = conn
        .get::<_, Option<String>>(SEK_REDIS_KEY)
        .await
        .context("read stream SEK from Redis")?
    {
        match vault.unwrap(wrapped.trim()).await {
            Ok(sek) => return Ok(StreamCipher::new(sek)),
            Err(e) => {
                tracing::warn!(
                    "stored stream SEK is unwrappable ({e:#}); rotating to a fresh SEK — \
                     stream entries sealed under the old key stay undecryptable"
                );
                rotate_dead_blob = true;
            }
        }
    }

    let mut sek = [0u8; 32];
    getrandom::getrandom(&mut sek).map_err(|e| anyhow!("SEK RNG failed: {}", e))?;
    let wrapped = vault.wrap(&sek).await.context("wrap new stream SEK")?;
    let stored: bool = if rotate_dead_blob {
        // Overwrite the dead blob — SET NX would keep it forever. Concurrent
        // rotating instances are last-writer-wins: both hold valid ciphers for
        // what they seal, and mixed-key entries only cost per-entry decode
        // failures on the reader (decode_entry already tolerates them).
        conn.set::<_, _, ()>(SEK_REDIS_KEY, &wrapped)
            .await
            .context("SET rotated stream SEK")?;
        true
    } else {
        conn.set_nx(SEK_REDIS_KEY, &wrapped)
            .await
            .context("SET NX stream SEK")?
    };
    if stored {
        return Ok(StreamCipher::new(sek));
    }
    // Lost the race: another instance set it first — adopt theirs.
    let wrapped: String = conn
        .get(SEK_REDIS_KEY)
        .await
        .context("re-read stream SEK after NX race")?;
    let sek = vault
        .unwrap(wrapped.trim())
        .await
        .context("unwrap raced stream SEK")?;
    Ok(StreamCipher::new(sek))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_aad() {
        let cipher = StreamCipher::new([7u8; 32]);
        let (nonce, ct) = cipher.encrypt(b"{\"kwh\":1.5}", b"energy").unwrap();
        let pt = cipher.decrypt(&nonce, &ct, b"energy").unwrap();
        assert_eq!(pt, b"{\"kwh\":1.5}");
    }

    #[test]
    fn wrong_aad_or_key_fails() {
        let cipher = StreamCipher::new([7u8; 32]);
        let (nonce, ct) = cipher.encrypt(b"secret", b"aad-1").unwrap();
        // Tampered AAD.
        assert!(cipher.decrypt(&nonce, &ct, b"aad-2").is_err());
        // Wrong SEK.
        assert!(StreamCipher::new([9u8; 32])
            .decrypt(&nonce, &ct, b"aad-1")
            .is_err());
    }

    #[test]
    fn nonce_is_random_per_call() {
        let cipher = StreamCipher::new([1u8; 32]);
        let (n1, _) = cipher.encrypt(b"x", b"").unwrap();
        let (n2, _) = cipher.encrypt(b"x", b"").unwrap();
        assert_ne!(n1, n2);
    }
}
