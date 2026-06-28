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
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures::StreamExt;
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::DecodePrivateKey as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Subject Chain Bridge consumes mint intents on.
const MINT_SUBJECT: &str = "chain.tx.mint";

/// Envelope-auth scheme tag — must equal `blockchain_core::rpc::envelope_auth::ENVELOPE_AUTH_SCHEME_V1`.
const ENVELOPE_AUTH_SCHEME_V1: &str = "ecdsa-p256-sha256-v1";

/// Domain separation tag — must equal `envelope_auth::DOMAIN_TAG` (trailing NUL included).
const DOMAIN_TAG: &[u8] = b"gridtokenx-nats-envelope-v1\0";

/// Length-prefixed canonical field encoder — mirror of `envelope_auth::push_field`:
/// `name` bytes, `0x00`, the value length as `u64` LE, then the value bytes.
fn push_field(buf: &mut Vec<u8>, name: &str, bytes: &[u8]) {
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Canonical bytes signed for a mint envelope — byte-for-byte mirror of
/// `blockchain_core::rpc::envelope_auth::canonical_mint_bytes`. The field order,
/// little-endian numeric encoding, and exclusion of `auth` are part of the wire
/// contract: any drift here makes the bridge reject every signature.
fn canonical_mint_bytes(m: &MintEnergyMessage) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(DOMAIN_TAG);
    buf.extend_from_slice(b"mint");
    buf.push(0);
    push_field(&mut buf, "correlation_id", m.correlation_id.as_bytes());
    push_field(&mut buf, "idempotency_key", m.idempotency_key.as_bytes());
    push_field(&mut buf, "reply_subject", m.reply_subject.as_bytes());
    push_field(&mut buf, "recipient_wallet", m.recipient_wallet.as_bytes());
    push_field(&mut buf, "service_identity", m.service_identity.as_bytes());
    push_field(&mut buf, "created_at_ms", &m.created_at_ms.to_le_bytes());
    push_field(&mut buf, "energy_kwh", &m.energy_kwh.to_le_bytes());
    push_field(&mut buf, "meter_id", &m.meter_id);
    push_field(
        &mut buf,
        "window_start_ms",
        &m.window_start_ms.to_le_bytes(),
    );
    buf
}

/// Mirror of `blockchain_core::rpc::nats_schema::EnvelopeAuth` — the auth block the
/// bridge's `check_envelope_auth` deserializes (scheme → cert → SAN → signature).
#[derive(Serialize, Deserialize, Clone)]
struct EnvelopeAuth {
    scheme: String,
    cert_pem: String,
    /// base64(ASN.1-DER ECDSA P-256/SHA-256 signature) over the canonical bytes.
    signature: String,
}

/// Signs canonical mint bytes with the service's mTLS client key and carries the
/// matching cert PEM. Built once at startup; cheap per message (one ECDSA sign).
/// Local mirror of `envelope_auth::EnvelopeSigner` (no blockchain-core dep).
struct EnvelopeSigner {
    key: SigningKey,
    cert_pem: String,
}

impl EnvelopeSigner {
    /// Pure constructor from in-memory PEM. The key must be ECDSA P-256 in SEC1
    /// ("EC PRIVATE KEY", what `gen-certs.sh` emits) or PKCS#8 form.
    fn from_pem(cert_pem: String, key_pem: &str) -> Result<Self> {
        let secret = p256::SecretKey::from_sec1_pem(key_pem)
            .or_else(|_| p256::SecretKey::from_pkcs8_pem(key_pem))
            .map_err(|e| anyhow!("client key is not a P-256 SEC1/PKCS#8 PEM: {e}"))?;
        Ok(Self {
            key: SigningKey::from(secret),
            cert_pem,
        })
    }

    /// Loads `CHAIN_BRIDGE_CLIENT_CERT` / `CHAIN_BRIDGE_CLIENT_KEY` (same paths the
    /// mTLS gRPC client uses). Returns `None` with a warning when the material is
    /// missing or unparseable, so insecure/dev setups keep publishing unsigned.
    fn from_env_paths() -> Option<Self> {
        let cert_path = std::env::var("CHAIN_BRIDGE_CLIENT_CERT")
            .unwrap_or_else(|_| "infra/certs/client.crt".to_string());
        let key_path = std::env::var("CHAIN_BRIDGE_CLIENT_KEY")
            .unwrap_or_else(|_| "infra/certs/client.key".to_string());
        let load = || -> Result<Self> {
            let cert_pem = std::fs::read_to_string(&cert_path)
                .with_context(|| format!("reading client cert at {cert_path}"))?;
            let key_pem = std::fs::read_to_string(&key_path)
                .with_context(|| format!("reading client key at {key_path}"))?;
            Self::from_pem(cert_pem, &key_pem)
        };
        match load() {
            Ok(signer) => Some(signer),
            Err(e) => {
                tracing::warn!(
                    "⚠️ NATS mint envelope signer unavailable ({e:#}) — mints will be UNSIGNED (dev only)"
                );
                None
            }
        }
    }

    /// Signs the canonical bytes; the `p256` `Signer` impl prehashes with SHA-256,
    /// matching the bridge's `verify_p256_signature`.
    fn sign(&self, canonical: &[u8]) -> EnvelopeAuth {
        let sig: Signature = self.key.sign(canonical);
        EnvelopeAuth {
            scheme: ENVELOPE_AUTH_SCHEME_V1.to_string(),
            cert_pem: self.cert_pem.clone(),
            signature: BASE64.encode(sig.to_der()),
        }
    }
}

/// Stable per-(meter, window) idempotency key: `mint:{serial}:{window_start_ms}`.
/// The bridge dedups replays on this; the on-chain `(meter_id, window_start_ms)`
/// PDA is the ultimate backstop. The 15-min window must match the billing window.
fn mint_idempotency_key(meter_serial: &str, window_start_ms: i64) -> String {
    format!("mint:{meter_serial}:{window_start_ms}")
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

/// Result of a successful on-chain mint submission.
#[derive(Debug, Clone)]
pub struct MintOutcome {
    /// On-chain transaction signature.
    pub signature: String,
    /// Slot the mint transaction landed in.
    pub slot: u64,
}

/// Mirror of `gridtokenx_blockchain_core::rpc::nats_schema::MintEnergyMessage`.
/// Duplicated here so the aggregator stays chain-light. Keep field names in sync.
#[derive(Serialize, Deserialize)]
struct MintEnergyMessage {
    correlation_id: String,
    idempotency_key: String,
    reply_subject: String,
    recipient_wallet: String,
    energy_kwh: f64,
    meter_id: [u8; 16],
    window_start_ms: i64,
    service_identity: String,
    created_at_ms: u64,
    /// Signed envelope auth; omitted from the wire when unsigned (no client cert).
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<EnvelopeAuth>,
}

/// Mirror of `MintEnergyResultMessage`.
#[derive(Deserialize)]
struct MintEnergyResultMessage {
    success: bool,
    signature: Option<String>,
    error: Option<String>,
    #[serde(default)]
    slot: u64,
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
            (true, Some(url)) => match async_nats::connect(url).await {
                Ok(client) => Self::Nats(NatsMintGateway::new(
                    client,
                    service_identity,
                    std::time::Duration::from_secs(30),
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
        let ack = self
            .jetstream
            .publish(MINT_SUBJECT, payload.into())
            .await
            .map_err(|e| anyhow!("publish mint intent (jetstream): {e}"))?;
        ack.await
            .map_err(|e| anyhow!("mint intent not acked by jetstream: {e}"))?;

        let reply = tokio::time::timeout(self.request_timeout, sub.next())
            .await
            .map_err(|_| anyhow!("mint request timed out"))?
            .ok_or_else(|| anyhow!("mint reply stream closed"))?;

        let result: MintEnergyResultMessage =
            serde_json::from_slice(&reply.payload).context("decode mint result")?;

        parse_mint_result(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // --- idempotency key: stable per (meter, window) so the bridge dedups replays ---

    #[test]
    fn idempotency_key_format_is_mint_serial_window() {
        assert_eq!(
            mint_idempotency_key("MTR-001", 1_700_000_000_000),
            "mint:MTR-001:1700000000000"
        );
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
        let mut expected_prefix = DOMAIN_TAG.to_vec();
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
