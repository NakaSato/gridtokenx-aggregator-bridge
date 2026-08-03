//! Chain Bridge mint gateway (NATS request-reply).
//!
//! The aggregator mints surplus energy tokens for a completed 15-minute billing
//! window by sending *intent* to Chain Bridge over NATS — it carries no Solana /
//! blockchain-core dependency and mirrors the wire types locally.
//!
//! SECURITY: the mint envelope is **signed** with this service's mTLS client key
//! (P-256/ECDSA over the canonical bytes) and carries the matching cert PEM as an
//! `EnvelopeAuth`, so the bridge can bind the self-asserted `service_identity` to a
//! CA-issued cert and reject spoofed identities under `CHAIN_BRIDGE_REQUIRE_SIGNED_NATS`.
//! When the client cert/key is absent (insecure dev), the signer is `None` and the
//! envelope ships unsigned — accepted only while the bridge runs signing in log-only
//! mode. The signing scheme is mirrored from `gridtokenx-blockchain-core`'s
//! `rpc::envelope_auth` (the bridge's verifier) — the aggregator carries no
//! blockchain-core dependency, so the canonical layout below MUST stay byte-for-byte
//! identical to `canonical_mint_bytes` there or every signature fails verification.

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use uuid::Uuid;

// Send/sign wire types + canonical-bytes scheme now come from the shared light
// `gridtokenx-blockchain-types` crate (single source of truth with the Chain
// Bridge verifier) — replaces the byte-for-byte local mirror this file carried.
use gridtokenx_blockchain_types::envelope_auth::{
    canonical_mint_batch_bytes, canonical_mint_bytes, EnvelopeSigner,
};
use gridtokenx_blockchain_types::nats_schema::{
    BatchMintItem, MintEnergyBatchMessage, MintEnergyMessage,
};

// Test-only: signing/verify primitives + the scheme tag, used by the
// golden-vector + roundtrip tests that guard the shared wire scheme.
#[cfg(test)]
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
#[cfg(test)]
use gridtokenx_blockchain_types::envelope_auth::ENVELOPE_AUTH_SCHEME_V1;
#[cfg(test)]
use p256::ecdsa::{Signature, SigningKey};

/// Subject Chain Bridge consumes mint intents on.
const MINT_SUBJECT: &str = "chain.tx.mint";

/// Subject Chain Bridge consumes BATCHED mint intents on — must equal
/// `blockchain_core::rpc::nats_schema::MINT_BATCH_SUBJECT`. A single token under
/// `chain.tx.` so the bridge's `chain.tx.*` stream captures it without reconfig.
const MINT_BATCH_SUBJECT: &str = "chain.tx.mintbatch";

/// Stable per-(meter, window) idempotency key: `mint:{serial}:{window_start_ms}`.
/// The bridge dedups replays on this; the on-chain `(meter_id, window_start_ms)`
/// PDA is the ultimate backstop. The 15-min window must match the billing window.
fn mint_idempotency_key(meter_serial: &str, window_start_ms: i64) -> String {
    format!("mint:{meter_serial}:{window_start_ms}")
}

/// Whether `wallet` is a syntactically valid Solana address (Base58, 32 bytes
/// decoded). The bridge rejects anything else (`Pubkey::from_str`), so checking
/// here lets the mint retry path skip the NATS round-trip for a wallet that can
/// never mint — without pulling in a Solana dependency (this crate stays
/// chain-light; `bs58` alone matches `Pubkey`'s own encoding).
#[must_use]
pub fn wallet_is_valid(wallet: &str) -> bool {
    bs58::decode(wallet)
        .into_vec()
        .map(|b| b.len() == 32)
        .unwrap_or(false)
}

/// Connect to NATS honoring credentials embedded in the URL.
///
/// async-nats (0.37) ignores the userinfo component of a
/// `nats://user:pass@host` URL — auth is taken only from `ConnectOptions` — so
/// a broker with `authorization` enabled rejects a plain `async_nats::connect`
/// even when the URL carries valid credentials. Mirrors
/// `gridtokenx-blockchain-core`'s `rpc::nats_provider::connect_with_url_creds`
/// (this crate stays chain-light, so no shared dep). Credentials are used
/// verbatim (no percent-decoding).
async fn connect_with_url_creds(url: &str) -> Result<async_nats::Client, async_nats::ConnectError> {
    match url_userinfo(url) {
        Some((user, pass)) => {
            async_nats::ConnectOptions::with_user_and_password(user, pass)
                .connect(url)
                .await
        }
        None => async_nats::connect(url).await,
    }
}

/// `(user, password)` from a URL's userinfo, if present. Password defaults to
/// empty when the userinfo has no `:`.
fn url_userinfo(url: &str) -> Option<(String, String)> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?'])
        .next()
        .unwrap_or(after_scheme);
    let (userinfo, _host) = authority.rsplit_once('@')?;
    let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    Some((user.to_string(), pass.to_string()))
}

/// Interprets the bridge's reply: `success` requires a signature; otherwise the
/// reported error (or a default) surfaces as an `Err`.
fn parse_mint_result(result: MintEnergyResultMessage) -> Result<MintOutcome> {
    if result.success {
        let signature = result
            .signature
            .ok_or_else(|| anyhow!("mint succeeded without signature"))?;
        Ok(MintOutcome {
            signature,
            slot: result.slot,
        })
    } else {
        Err(anyhow!(result
            .error
            .unwrap_or_else(|| "mint failed".to_string())))
    }
}

/// Marker error: the mint intent was durably acked by JetStream but no reply
/// arrived within the request timeout. Unlike a hard bridge rejection, the mint
/// may still have landed on-chain — the retry is safe (idempotency key + PDA
/// dedup) but callers must count it distinctly so a lossy reply path is visible
/// on dashboards instead of masquerading as mint failures.
#[derive(Debug)]
pub struct MintReplyTimeout;

impl std::fmt::Display for MintReplyTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mint request timed out awaiting reply")
    }
}

impl std::error::Error for MintReplyTimeout {}

/// Result of a successful on-chain mint submission.
#[derive(Debug, Clone)]
pub struct MintOutcome {
    /// On-chain transaction signature.
    pub signature: String,
    /// Slot the mint transaction landed in.
    pub slot: u64,
}

/// Local Deserialize subset of `gridtokenx_blockchain_types::nats_schema::MintEnergyResultMessage`
/// (the bridge reply). Extra fields the bridge may send (`correlation_id`,
/// `deduplicated`) are intentionally ignored by serde.
#[derive(Deserialize)]
struct MintEnergyResultMessage {
    success: bool,
    signature: Option<String>,
    error: Option<String>,
    #[serde(default)]
    slot: u64,
}

/// One recipient's outcome from the bridge's batch reply. Minimal Deserialize
/// mirror (unknown fields, e.g. `deduplicated`, are ignored). `idempotency_key`
/// lets the caller match a result back to its outbox entry without ordering.
#[derive(Deserialize, Debug, Clone)]
pub struct MintBatchItemResult {
    /// Index into the request's items — informational; matching is by key.
    #[serde(default)]
    pub index: usize,
    pub idempotency_key: String,
    pub success: bool,
    pub signature: Option<String>,
    #[serde(default)]
    pub slot: u64,
    pub error: Option<String>,
}

/// Mirror of `blockchain_core::rpc::nats_schema::MintEnergyBatchResultMessage`
/// (reply side, Deserialize-only).
#[derive(Deserialize)]
struct MintEnergyBatchResultMessage {
    results: Vec<MintBatchItemResult>,
}

/// Input to [`NatsMintGateway::mint_batch`] — one recipient's mint intent. The
/// per-item idempotency key is derived as `mint:{meter_serial}:{window_start_ms}`,
/// identical to the single-recipient path, so the two paths dedup against the
/// same on-chain `(meter, window)` PDA.
#[derive(Debug, Clone)]
pub struct BatchMintInput {
    pub recipient_wallet: String,
    pub energy_kwh: f64,
    pub meter_id: [u8; 16],
    pub meter_serial: String,
    pub window_start_ms: i64,
}

/// Mint gateway. `Nats` talks to Chain Bridge; `Disabled` is wired when no
/// blockchain backend is configured so the flush loop can call `mint` uniformly.
// The `Nats` variant is intentionally the only non-trivial one; the gateway is
// long-lived (one per process), so the size asymmetry with `Disabled` is moot.
#[allow(clippy::large_enum_variant)]
pub enum MintGateway {
    /// Mints via Chain Bridge over NATS request-reply.
    Nats(NatsMintGateway),
    /// No backend configured — every `mint` is a no-op error.
    Disabled,
}

impl MintGateway {
    /// Builds the gateway from config. Mints via Chain Bridge when
    /// `mint_via_chain_bridge` is set and a NATS URL is given; otherwise
    /// disabled. A NATS connect failure degrades to `Disabled`.
    #[must_use]
    pub async fn connect(
        mint_via_chain_bridge: bool,
        nats_url: Option<&str>,
        service_identity: String,
    ) -> Self {
        match (mint_via_chain_bridge, nats_url) {
            (true, Some(url)) => match connect_with_url_creds(url).await {
                Ok(client) => Self::Nats(NatsMintGateway::new(
                    client,
                    service_identity,
                    // Reply-wait budget per mint request. Under a large settlement
                    // burst the chain-bridge queue wait can exceed a short budget,
                    // and every timeout re-enters the outbox → wholesale republish
                    // amplification (observed 2.3× at a 10k burst, congestion
                    // collapse at 25k). Raise via env for fleet-scale windows.
                    std::time::Duration::from_secs(
                        std::env::var("AGGREGATOR_MINT_REPLY_TIMEOUT_SECS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(30),
                    ),
                    EnvelopeSigner::from_env_paths(),
                )),
                Err(e) => {
                    tracing::warn!("mint backend NATS connect failed ({e}); minting disabled");
                    Self::Disabled
                }
            },
            (true, None) => {
                tracing::warn!("MINT_VIA_CHAIN_BRIDGE set but NATS_URL unset; minting disabled");
                Self::Disabled
            }
            (false, _) => Self::Disabled,
        }
    }

    /// Whether this gateway will actually attempt an on-chain mint.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Nats(_))
    }

    /// Mints `energy_kwh` to `recipient_wallet` for `(meter_id, window_start_ms)`.
    ///
    /// # Errors
    /// Returns an error when the backend is disabled, unreachable, or the bridge
    /// reports a failure.
    pub async fn mint(
        &self,
        recipient_wallet: &str,
        energy_kwh: f64,
        meter_id: [u8; 16],
        meter_serial: &str,
        window_start_ms: i64,
    ) -> Result<MintOutcome> {
        match self {
            Self::Nats(gw) => {
                gw.mint(
                    recipient_wallet,
                    energy_kwh,
                    meter_id,
                    meter_serial,
                    window_start_ms,
                )
                .await
            }
            Self::Disabled => Err(anyhow!("mint backend not configured")),
        }
    }

    /// Batched sibling of [`Self::mint`] — mints many recipients in one NATS
    /// round-trip and returns the bridge's per-recipient result vector.
    ///
    /// # Errors
    /// Returns an error only for envelope-level failures (backend disabled,
    /// encode, publish/ack, reply timeout, decode). Per-recipient failures ride
    /// in the returned results (`success = false`), not as an `Err`.
    pub async fn mint_batch(&self, inputs: &[BatchMintInput]) -> Result<Vec<MintBatchItemResult>> {
        match self {
            Self::Nats(gw) => gw.mint_batch(inputs).await,
            Self::Disabled => Err(anyhow!("mint backend not configured")),
        }
    }
}

/// Mints energy tokens by asking Chain Bridge to build, sign, and submit the
/// generation mint over NATS request-reply. Carries only intent — no Solana types.
pub struct NatsMintGateway {
    client: async_nats::Client,
    /// JetStream context over the same connection. Mint intents are published
    /// here (not core NATS) so the broker persists them in the `chain.tx.*`
    /// stream and returns a PubAck — closing the prior fire-and-forget gap where
    /// a mint could vanish if the consumer was momentarily absent.
    jetstream: async_nats::jetstream::Context,
    service_identity: String,
    request_timeout: std::time::Duration,
    /// Mint-envelope signer; `None` in insecure dev (no client cert) ⇒ unsigned.
    signer: Option<EnvelopeSigner>,
}

impl NatsMintGateway {
    /// Creates a gateway over a connected NATS client. Builds a JetStream
    /// context over the same connection for durable, acked mint publishes; the
    /// core client is retained for the reply-subject subscription.
    #[must_use]
    fn new(
        client: async_nats::Client,
        service_identity: String,
        request_timeout: std::time::Duration,
        signer: Option<EnvelopeSigner>,
    ) -> Self {
        let jetstream = async_nats::jetstream::new(client.clone());
        Self {
            client,
            jetstream,
            service_identity,
            request_timeout,
            signer,
        }
    }

    #[tracing::instrument(name = "nats_mint_publish", skip_all, fields(energy_kwh = energy_kwh))]
    async fn mint(
        &self,
        recipient_wallet: &str,
        energy_kwh: f64,
        meter_id: [u8; 16],
        meter_serial: &str,
        window_start_ms: i64,
    ) -> Result<MintOutcome> {
        let correlation_id = Uuid::new_v4().to_string();
        let reply_subject = format!("chain.mint.result.{correlation_id}");
        let created_at_ms = gridtokenx_telemetry::time::now()
            .timestamp_millis()
            .try_into()
            .unwrap_or(0u64);

        let mut msg = MintEnergyMessage {
            correlation_id,
            idempotency_key: mint_idempotency_key(meter_serial, window_start_ms),
            reply_subject: reply_subject.clone(),
            recipient_wallet: recipient_wallet.to_string(),
            energy_kwh,
            meter_id,
            window_start_ms,
            service_identity: self.service_identity.clone(),
            created_at_ms,
            auth: None,
        };
        // Sign over the canonical bytes (which exclude `auth`) before attaching it.
        if let Some(signer) = &self.signer {
            msg.auth = Some(signer.sign(&canonical_mint_bytes(&msg)));
        }
        let payload = serde_json::to_vec(&msg).context("encode mint intent")?;

        // Subscribe to the reply BEFORE publishing so the result can't be missed.
        // The reply rides core NATS (the bridge replies to this ephemeral
        // subject); only the durable mint *intent* below goes through JetStream.
        let mut sub = self
            .client
            .subscribe(reply_subject.clone())
            .await
            .map_err(|e| anyhow!("subscribe mint reply: {e}"))?;

        // Publish the mint intent to JetStream and AWAIT the PubAck — this
        // confirms the broker durably stored the message in the `chain.tx.*`
        // stream before we wait for a reply. A missing stream / broker fault
        // surfaces as `Err` here (caller logs; the bin still evicts and the
        // idempotency key dedups any later retry) rather than a silent drop.
        // Carry the current trace context on NATS headers so chain-bridge stitches
        // its mint-consume span onto this trace. Headers ride OUTSIDE the signed
        // envelope (`canonical_mint_bytes` excludes them), so signing is unaffected.
        let mut headers = async_nats::HeaderMap::new();
        gridtokenx_telemetry::inject_trace_context(|k, v| headers.insert(k, v.as_str()));

        let ack = self
            .jetstream
            .publish_with_headers(MINT_SUBJECT, headers, payload.into())
            .await
            .map_err(|e| anyhow!("publish mint intent (jetstream): {e}"))?;
        ack.await
            .map_err(|e| anyhow!("mint intent not acked by jetstream: {e}"))?;

        let reply = tokio::time::timeout(self.request_timeout, sub.next())
            .await
            .map_err(|_| anyhow::Error::new(MintReplyTimeout))?
            .ok_or_else(|| anyhow!("mint reply stream closed"))?;

        let result: MintEnergyResultMessage =
            serde_json::from_slice(&reply.payload).context("decode mint result")?;

        parse_mint_result(result)
    }

    /// Batched sibling of [`Self::mint`]: publishes ONE `chain.tx.mintbatch`
    /// envelope carrying every recipient, and returns the bridge's per-recipient
    /// result vector so the caller evicts exactly the confirmed ones and retries
    /// the rest. Amortises the NATS round-trip and the bridge's per-tx cost over
    /// up to `GENERATION_MINT_CHUNK` recipients per on-chain transaction.
    ///
    /// Each item's dedup/PDA key is `mint:{meter_serial}:{window_start_ms}` —
    /// identical to the single path, so the two paths are mutually idempotent (a
    /// recipient minted by either never double-mints via the other).
    ///
    /// # Errors
    /// Returns `Err` only for envelope-level failures (encode, JetStream
    /// publish/ack, reply timeout, decode). Per-recipient failures are carried in
    /// the returned `MintBatchItemResult`s (`success = false`), not as an `Err`.
    #[tracing::instrument(name = "nats_mint_batch_publish", skip_all, fields(items = inputs.len()))]
    pub async fn mint_batch(&self, inputs: &[BatchMintInput]) -> Result<Vec<MintBatchItemResult>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let correlation_id = Uuid::new_v4().to_string();
        let reply_subject = format!("chain.mint.batch.result.{correlation_id}");
        let created_at_ms = gridtokenx_telemetry::time::now()
            .timestamp_millis()
            .try_into()
            .unwrap_or(0u64);

        let items: Vec<BatchMintItem> = inputs
            .iter()
            .map(|i| BatchMintItem {
                idempotency_key: mint_idempotency_key(&i.meter_serial, i.window_start_ms),
                recipient_wallet: i.recipient_wallet.clone(),
                energy_kwh: i.energy_kwh,
                meter_id: i.meter_id,
                window_start_ms: i.window_start_ms,
            })
            .collect();

        let mut msg = MintEnergyBatchMessage {
            correlation_id,
            reply_subject: reply_subject.clone(),
            items,
            service_identity: self.service_identity.clone(),
            created_at_ms,
            auth: None,
        };
        if let Some(signer) = &self.signer {
            msg.auth = Some(signer.sign(&canonical_mint_batch_bytes(&msg)));
        }
        let payload = serde_json::to_vec(&msg).context("encode batch mint intent")?;

        // Subscribe to the reply BEFORE publishing so it can't be missed.
        let mut sub = self
            .client
            .subscribe(reply_subject.clone())
            .await
            .map_err(|e| anyhow!("subscribe batch mint reply: {e}"))?;

        let mut headers = async_nats::HeaderMap::new();
        gridtokenx_telemetry::inject_trace_context(|k, v| headers.insert(k, v.as_str()));

        let ack = self
            .jetstream
            .publish_with_headers(MINT_BATCH_SUBJECT, headers, payload.into())
            .await
            .map_err(|e| anyhow!("publish batch mint intent (jetstream): {e}"))?;
        ack.await
            .map_err(|e| anyhow!("batch mint intent not acked by jetstream: {e}"))?;

        let reply = tokio::time::timeout(self.request_timeout, sub.next())
            .await
            .map_err(|_| anyhow::Error::new(MintReplyTimeout))?
            .ok_or_else(|| anyhow!("batch mint reply stream closed"))?;

        let result: MintEnergyBatchResultMessage =
            serde_json::from_slice(&reply.payload).context("decode batch mint result")?;

        Ok(result.results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // --- NATS URL userinfo: auth-enabled broker needs creds via ConnectOptions ---

    #[test]
    fn url_userinfo_extracts_user_and_password() {
        assert_eq!(
            url_userinfo("nats://alice:s3cret@nats:4222"),
            Some(("alice".to_string(), "s3cret".to_string()))
        );
    }

    #[test]
    fn url_userinfo_absent_returns_none() {
        assert_eq!(url_userinfo("nats://nats:4222"), None);
        // '@' beyond the authority (path/query) must not be mistaken for userinfo
        assert_eq!(url_userinfo("nats://host:4222/path@x"), None);
    }

    // --- idempotency key: stable per (meter, window) so the bridge dedups replays ---

    #[test]
    fn idempotency_key_format_is_mint_serial_window() {
        assert_eq!(
            mint_idempotency_key("MTR-001", 1_700_000_000_000),
            "mint:MTR-001:1700000000000"
        );
    }

    // --- wallet pre-validation: must accept exactly what the bridge's Pubkey parse accepts ---

    #[test]
    fn real_solana_pubkey_is_valid() {
        // The SPL token program id — a canonical 32-byte Base58 address.
        assert!(wallet_is_valid(
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        ));
    }

    #[test]
    fn e2e_fake_wallet_with_non_base58_chars_is_invalid() {
        // The 30_settlement fixture shape: `Wa11et{meter}` padded with '1's.
        // Meter ids like I8438032 inject 'I'/'0' — not in the Base58 alphabet.
        assert!(!wallet_is_valid(
            "Wa11etI843803211111111111111111111111111111"
        ));
        assert!(!wallet_is_valid(
            "Wa11etS478130111111111111111111111111111111"
        ));
    }

    #[test]
    fn valid_base58_but_wrong_length_is_invalid() {
        assert!(!wallet_is_valid("abc")); // decodes fine, 2 bytes
        assert!(!wallet_is_valid("")); // empty
    }

    // --- mint intent wire shape: field names + types form the Chain Bridge contract ---

    #[test]
    fn mint_message_serializes_expected_wire_shape() {
        let msg = MintEnergyMessage {
            correlation_id: "cid".to_string(),
            idempotency_key: mint_idempotency_key("MTR-001", 900_000),
            reply_subject: "chain.mint.result.cid".to_string(),
            recipient_wallet: "WALLET".to_string(),
            energy_kwh: 12.5,
            meter_id: [7u8; 16],
            window_start_ms: 900_000,
            service_identity: "aggregator-bridge".to_string(),
            created_at_ms: 1_700_000_000_000,
            auth: None,
        };
        let v: Value = serde_json::to_value(&msg).unwrap();

        // Field names the bridge deserializes by — drift here breaks minting silently.
        assert_eq!(v["correlation_id"], "cid");
        assert_eq!(v["idempotency_key"], "mint:MTR-001:900000");
        assert_eq!(v["reply_subject"], "chain.mint.result.cid");
        assert_eq!(v["recipient_wallet"], "WALLET");
        assert_eq!(v["energy_kwh"], json!(12.5));
        assert_eq!(
            v["meter_id"],
            json!([7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7])
        );
        assert_eq!(v["window_start_ms"], json!(900_000));
        assert_eq!(v["service_identity"], "aggregator-bridge");
        assert_eq!(v["created_at_ms"], json!(1_700_000_000_000u64));
        // `auth` MUST be absent — unsigned envelope (dev). See SECURITY note.
        assert!(v.get("auth").is_none(), "auth must not be serialized");
    }

    // --- signing: the attached auth is a valid P-256 sig over the canonical bytes ---

    #[test]
    fn signed_envelope_verifies_over_canonical_bytes() {
        use p256::ecdsa::signature::Verifier as _;
        use p256::ecdsa::VerifyingKey;
        use p256::elliptic_curve::rand_core::OsRng;
        use p256::pkcs8::EncodePrivateKey as _;

        let secret = p256::SecretKey::random(&mut OsRng);
        let key_pem = secret
            .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string();
        let signer =
            EnvelopeSigner::from_pem("test-cert-pem".to_string(), &key_pem).expect("signer builds");

        let msg = MintEnergyMessage {
            correlation_id: "cid".to_string(),
            idempotency_key: mint_idempotency_key("MTR-001", 900_000),
            reply_subject: "chain.mint.result.cid".to_string(),
            recipient_wallet: "WALLET".to_string(),
            energy_kwh: 12.5,
            meter_id: [7u8; 16],
            window_start_ms: 900_000,
            service_identity: "aggregator-bridge".to_string(),
            created_at_ms: 1_700_000_000_000,
            auth: None,
        };
        let canonical = canonical_mint_bytes(&msg);
        let auth = signer.sign(&canonical);

        assert_eq!(auth.scheme, ENVELOPE_AUTH_SCHEME_V1);
        assert_eq!(auth.cert_pem, "test-cert-pem");

        // The DER signature verifies against the public key over the same canonical
        // bytes the bridge will recompute — proves the sign path matches the scheme.
        let sig_der = BASE64.decode(&auth.signature).expect("sig is base64");
        let sig = Signature::from_der(&sig_der).expect("sig is DER");
        let vk = VerifyingKey::from(&SigningKey::from(secret));
        vk.verify(&canonical, &sig)
            .expect("signature verifies over canonical bytes");
    }

    // --- drift guard: the mirror MUST match blockchain-core's wire scheme ---

    /// Golden vector — canonical mint bytes reconstructed independently of the
    /// `canonical_mint_bytes` under test, identical to
    /// `blockchain_core::rpc::envelope_auth::canonical_mint_golden_vector`.
    ///
    /// The roundtrip test above only proves the local signer agrees with the
    /// local `canonical_mint_bytes` — it would still pass if this mirror DRIFTED
    /// from blockchain-core (a field added/reordered, endianness flipped, tag
    /// changed). Because the aggregator keeps a private copy of the encoder (no
    /// blockchain-core dep), nothing else cross-checks it. This pins the mirror to
    /// that scheme byte-for-byte: drift fails here instead of silently breaking
    /// every mint signature at the bridge (the verifier recomputes the canonical
    /// bytes from blockchain-core's copy, so a mismatch rejects the signature).
    #[test]
    fn canonical_mint_bytes_matches_blockchain_core_golden() {
        let m = MintEnergyMessage {
            correlation_id: "c1".to_string(),
            idempotency_key: "ik".to_string(),
            reply_subject: "chain.mint.result.c1".to_string(),
            recipient_wallet: "Wa11et".to_string(),
            energy_kwh: 7.5,
            meter_id: [7u8; 16],
            window_start_ms: 1_700_000_000_000,
            service_identity: "spiffe://gridtokenx.th/prod/meter-service".to_string(),
            created_at_ms: 1_700_000_000_000,
            auth: None,
        };

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"gridtokenx-nats-envelope-v1\0mint\0");
        let mut field = |name: &str, bytes: &[u8]| {
            expected.extend_from_slice(name.as_bytes());
            expected.push(0);
            expected.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            expected.extend_from_slice(bytes);
        };
        field("correlation_id", b"c1");
        field("idempotency_key", b"ik");
        field("reply_subject", b"chain.mint.result.c1");
        field("recipient_wallet", b"Wa11et");
        field(
            "service_identity",
            b"spiffe://gridtokenx.th/prod/meter-service",
        );
        field("created_at_ms", &1_700_000_000_000u64.to_le_bytes());
        field("energy_kwh", &7.5f64.to_le_bytes());
        field("meter_id", &[7u8; 16]);
        field("window_start_ms", &1_700_000_000_000i64.to_le_bytes());

        assert_eq!(
            canonical_mint_bytes(&m),
            expected,
            "aggregator mint canonical bytes drifted from the blockchain-core wire \
             scheme (envelope_auth::canonical_mint_bytes) — every mint signature \
             would fail verification at the Chain Bridge"
        );
    }

    /// Golden vector for the BATCH canonical bytes — identical reconstruction to
    /// `blockchain_core::rpc::envelope_auth::canonical_mint_batch_golden_vector`.
    /// Pins the aggregator's private mirror to the bridge's wire scheme byte-for-
    /// byte (per-item framing + `item_count`); drift fails here instead of
    /// silently breaking every batch signature at the verifier.
    #[test]
    fn canonical_mint_batch_bytes_matches_blockchain_core_golden() {
        let m = MintEnergyBatchMessage {
            correlation_id: "c1".to_string(),
            reply_subject: "chain.mint.batch.result.c1".to_string(),
            items: vec![
                BatchMintItem {
                    idempotency_key: "mint:m1:1000".to_string(),
                    recipient_wallet: "Wa11etA".to_string(),
                    energy_kwh: 7.5,
                    meter_id: [7u8; 16],
                    window_start_ms: 1_700_000_000_000,
                },
                BatchMintItem {
                    idempotency_key: "mint:m2:1000".to_string(),
                    recipient_wallet: "Wa11etB".to_string(),
                    energy_kwh: 3.25,
                    meter_id: [9u8; 16],
                    window_start_ms: 1_700_000_000_000,
                },
            ],
            service_identity: "spiffe://gridtokenx.th/prod/aggregator-bridge".to_string(),
            created_at_ms: 1_700_000_000_000,
            auth: None,
        };

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"gridtokenx-nats-envelope-v1\0mint_batch\0");
        let mut field = |name: &str, bytes: &[u8]| {
            expected.extend_from_slice(name.as_bytes());
            expected.push(0);
            expected.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            expected.extend_from_slice(bytes);
        };
        let item_bytes = |ik: &str, wallet: &str, kwh: f64, meter: &[u8; 16], win: i64| {
            let mut b: Vec<u8> = Vec::new();
            let mut f = |name: &str, bytes: &[u8]| {
                b.extend_from_slice(name.as_bytes());
                b.push(0);
                b.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                b.extend_from_slice(bytes);
            };
            f("idempotency_key", ik.as_bytes());
            f("recipient_wallet", wallet.as_bytes());
            f("energy_kwh", &kwh.to_le_bytes());
            f("meter_id", meter);
            f("window_start_ms", &win.to_le_bytes());
            b
        };
        field("correlation_id", b"c1");
        field("reply_subject", b"chain.mint.batch.result.c1");
        field(
            "service_identity",
            b"spiffe://gridtokenx.th/prod/aggregator-bridge",
        );
        field("created_at_ms", &1_700_000_000_000u64.to_le_bytes());
        field("item_count", &2u64.to_le_bytes());
        field(
            "item",
            &item_bytes(
                "mint:m1:1000",
                "Wa11etA",
                7.5,
                &[7u8; 16],
                1_700_000_000_000,
            ),
        );
        field(
            "item",
            &item_bytes(
                "mint:m2:1000",
                "Wa11etB",
                3.25,
                &[9u8; 16],
                1_700_000_000_000,
            ),
        );

        assert_eq!(
            canonical_mint_batch_bytes(&m),
            expected,
            "aggregator BATCH canonical bytes drifted from the blockchain-core wire \
             scheme (envelope_auth::canonical_mint_batch_bytes) — every batch mint \
             signature would fail verification at the Chain Bridge"
        );
    }

    /// Reproduces the exact bridge path: sign → JSON serialize → JSON
    /// deserialize → recompute canonical from the decoded struct → verify.
    /// Sweeps many realistic surplus f64 values + varying meter_id/window so a
    /// value-dependent round-trip mismatch (the suspected cause of intermittent
    /// "envelope signature verification failed") would surface as a failure here.
    #[test]
    fn signed_envelope_survives_json_roundtrip_over_many_values() {
        use p256::ecdsa::signature::Verifier as _;
        use p256::ecdsa::VerifyingKey;
        use p256::elliptic_curve::rand_core::OsRng;
        use p256::pkcs8::EncodePrivateKey as _;

        let secret = p256::SecretKey::random(&mut OsRng);
        let key_pem = secret
            .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string();
        let vk = VerifyingKey::from(&SigningKey::from(secret.clone()));
        let signer =
            EnvelopeSigner::from_pem("test-cert-pem".to_string(), &key_pem).expect("signer builds");

        let mut failures = 0usize;
        for i in 0..5000u64 {
            // "Ugly" surplus floats like real billing deltas (0.06069600000000001).
            let energy_kwh = (i as f64 * 0.000_337) + (i as f64 / 7.0).fract() * 0.1;
            let mut meter_id = [0u8; 16];
            meter_id[..8].copy_from_slice(&i.to_le_bytes());
            meter_id[8..].copy_from_slice(&(i.wrapping_mul(2_654_435_761)).to_le_bytes());
            let window_start_ms = 1_700_000_000_000i64 + (i as i64) * 900_000;

            let mut msg = MintEnergyMessage {
                correlation_id: format!("cid-{i}"),
                idempotency_key: mint_idempotency_key("MTR", window_start_ms),
                reply_subject: format!("chain.mint.result.cid-{i}"),
                recipient_wallet: "So11111111111111111111111111111111111111112".to_string(),
                energy_kwh,
                meter_id,
                window_start_ms,
                service_identity: "spiffe://gridtokenx.th/prod/aggregator-bridge".to_string(),
                created_at_ms: 1_700_000_000_000u64 + i,
                auth: None,
            };
            // Sign exactly as production does.
            msg.auth = Some(signer.sign(&canonical_mint_bytes(&msg)));

            // The wire hop the bridge performs.
            let payload = serde_json::to_vec(&msg).unwrap();
            let decoded: MintEnergyMessage = serde_json::from_slice(&payload).unwrap();

            // Bridge recomputes canonical from the DECODED struct, then verifies.
            let canonical = canonical_mint_bytes(&decoded);
            let auth = decoded.auth.as_ref().unwrap();
            let sig_der = BASE64.decode(&auth.signature).unwrap();
            let sig = Signature::from_der(&sig_der).unwrap();
            if vk.verify(&canonical, &sig).is_err() {
                failures += 1;
            }
        }
        assert_eq!(
            failures, 0,
            "{failures}/5000 signed mint envelopes failed verify after a JSON round-trip"
        );
    }

    #[test]
    fn canonical_mint_bytes_start_with_domain_tag_and_kind() {
        let msg = MintEnergyMessage {
            correlation_id: "c".to_string(),
            idempotency_key: "k".to_string(),
            reply_subject: "r".to_string(),
            recipient_wallet: "w".to_string(),
            energy_kwh: 1.0,
            meter_id: [0u8; 16],
            window_start_ms: 0,
            service_identity: "s".to_string(),
            created_at_ms: 0,
            auth: None,
        };
        let bytes = canonical_mint_bytes(&msg);
        // Domain tag is private to -types; assert the literal wire prefix here.
        let mut expected_prefix = b"gridtokenx-nats-envelope-v1\0".to_vec();
        expected_prefix.extend_from_slice(b"mint");
        expected_prefix.push(0);
        assert!(
            bytes.starts_with(&expected_prefix),
            "canonical must begin with domain tag + kind (drift breaks bridge verify)"
        );
    }

    // --- result parsing: success needs a signature; failure surfaces the error ---

    #[test]
    fn parse_result_success_yields_outcome() {
        let r = MintEnergyResultMessage {
            success: true,
            signature: Some("SIG".to_string()),
            error: None,
            slot: 42,
        };
        let out = parse_mint_result(r).expect("success parses");
        assert_eq!(out.signature, "SIG");
        assert_eq!(out.slot, 42);
    }

    #[test]
    fn parse_result_success_without_signature_is_error() {
        let r = MintEnergyResultMessage {
            success: true,
            signature: None,
            error: None,
            slot: 0,
        };
        assert!(
            parse_mint_result(r).is_err(),
            "success without signature must error"
        );
    }

    #[test]
    fn parse_result_failure_surfaces_error() {
        let r = MintEnergyResultMessage {
            success: false,
            signature: None,
            error: Some("insufficient authority".to_string()),
            slot: 0,
        };
        let err = parse_mint_result(r).unwrap_err().to_string();
        assert!(
            err.contains("insufficient authority"),
            "bridge error propagates: {err}"
        );
    }

    #[test]
    fn result_slot_defaults_when_absent() {
        // The bridge may omit `slot`; serde default keeps the wire backward-compatible.
        let r: MintEnergyResultMessage =
            serde_json::from_value(json!({"success": true, "signature": "S"})).unwrap();
        assert_eq!(r.slot, 0, "missing slot defaults to 0");
    }

    // --- gateway config: minting is opt-in; missing prerequisites degrade to Disabled ---
    // (The (true, Some(url)) connect path touches NATS and is covered by integration tests.)

    #[tokio::test]
    async fn connect_disabled_when_flag_off() {
        // Flag off → never touches NATS even with a URL present.
        let gw =
            MintGateway::connect(false, Some("nats://localhost:4222"), "svc".to_string()).await;
        assert!(!gw.is_enabled(), "flag off ⇒ disabled");
    }

    #[tokio::test]
    async fn connect_disabled_when_url_missing() {
        // Flag on but no NATS_URL → disabled (logged warn), no connect attempt.
        let gw = MintGateway::connect(true, None, "svc".to_string()).await;
        assert!(!gw.is_enabled(), "flag on + no url ⇒ disabled");
    }

    #[tokio::test]
    async fn disabled_mint_errors_and_is_not_enabled() {
        let gw = MintGateway::Disabled;
        assert!(!gw.is_enabled());
        let err = gw
            .mint("WALLET", 5.0, [0u8; 16], "MTR-001", 900_000)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not configured"),
            "disabled mint surfaces config error: {err}"
        );
    }
}
