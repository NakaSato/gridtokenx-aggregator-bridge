use crate::models::{
    BatchPrivateNetworkPayload, DeviceReading, DeviceType, IngestResponse, PrivateNetworkPayload,
};
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

// =============================================================================
// Health Check
// =============================================================================

pub async fn health() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "service": "gridtokenx-iot-gateway",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

// =============================================================================
// Private Network Ingestion (B2B - Specialized Protocols)
// =============================================================================

/// Secure (locked-down / production) mode. When `AGGREGATOR_REQUIRE_SECURE=true`,
/// every ingest bypass is neutralized: the unverified-telemetry escape hatch is
/// ignored, the unsigned `simulator` protocol is refused, and the REST meter path
/// requires an authenticated `dlms-enc` frame (no plaintext downgrade). Default
/// off so dev/e2e keep their bypasses.
pub fn secure_mode_enabled() -> bool {
    std::env::var("AGGREGATOR_REQUIRE_SECURE").unwrap_or_default() == "true"
}

// Helper function to verify signature on REST ingestion paths
/// Whether telemetry signature enforcement is disabled (dev/test escape hatch).
///
/// Default (env unset) is fail-CLOSED: telemetry with an invalid or unverifiable
/// Ed25519 signature is rejected. Set `AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY=true`
/// only in trusted dev/test environments to accept unverified readings.
///
/// Secure mode ([`secure_mode_enabled`]) hard-overrides this to `false`: the
/// escape hatch cannot re-enable unverified telemetry in a locked-down deployment.
pub fn signature_enforcement_disabled() -> bool {
    !secure_mode_enabled()
        && std::env::var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY").unwrap_or_default() == "true"
}

/// Whether the unsigned `simulator` ingest bypass is permitted. Allowed only
/// outside secure mode; in secure mode a `simulator` frame falls through to
/// normal signature verification (and is rejected, being unsigned).
pub fn simulator_bypass_allowed() -> bool {
    !secure_mode_enabled()
}

/// Normalize a declared REST protocol to the stack it resolves to: an empty or
/// `auto` value resolves to `dlms` (the only meter protocol); anything else is
/// lowercased and honored verbatim (`simulator` is the unsigned dev bypass).
fn resolve_protocol(raw: &str) -> String {
    let p = raw.to_lowercase();
    if p.is_empty() || p == "auto" {
        "dlms".to_string()
    } else {
        p
    }
}

/// Whether a *resolved* protocol is one the ingest paths accept. `auto`/empty
/// must be passed through [`resolve_protocol`] first (they resolve to `dlms`).
fn is_supported_protocol(resolved: &str) -> bool {
    matches!(resolved, "dlms" | "simulator")
}

/// Secure-mode meter-path gate. Returns the status to reject with, or `None` to
/// proceed. `AGGREGATOR_REQUIRE_SECURE=true` requires an authenticated
/// `dlms-enc` frame, so any non-encrypted frame (plaintext `dlms`, `simulator`,
/// downgrade) is refused with `426 UPGRADE_REQUIRED` before it can reach a dev
/// bypass. The batch path carries no per-frame encryption, so it passes
/// `was_encrypted = false` (rejected wholesale in secure mode).
fn secure_mode_gate(secure_mode: bool, was_encrypted: bool) -> Option<StatusCode> {
    if secure_mode && !was_encrypted {
        Some(StatusCode::UPGRADE_REQUIRED)
    } else {
        None
    }
}

/// Status to return for a signature-verification outcome, or `None` to continue
/// processing (signature valid, OR enforcement disabled so unverified telemetry
/// is accepted in dev). Encodes the fail-closed default: an invalid signature is
/// `403 FORBIDDEN`, a verification *error* (e.g. Redis unreachable) is
/// `401 UNAUTHORIZED` — distinct so a transport failure is not reported as a
/// forged signature. Both are suppressed when `enforcement_disabled` is true.
fn sig_failure_status(
    verified: &anyhow::Result<bool>,
    enforcement_disabled: bool,
) -> Option<StatusCode> {
    if enforcement_disabled {
        return None;
    }
    match verified {
        Ok(true) => None,
        Ok(false) => Some(StatusCode::FORBIDDEN),
        Err(_) => Some(StatusCode::UNAUTHORIZED),
    }
}

/// The numeric value a DLMS/COSEM meter signs in its canonical
/// `{device_id}:{value}:{timestamp}` Ed25519 sign-target.
///
/// The value is consumption in kWh: it resolves `kwh` → `energy_consumed` →
/// `energy_generated` → OBIS active import (1.1.1.8.0.255, Wh/1000) → OBIS export
/// (1.1.2.8.0.255, Wh/1000), so a real OBIS-coded meter signs the same kWh the
/// decoder reconstructs. `protocol` is unused (DLMS/COSEM is the only stack) but
/// kept in the signature for call-site symmetry.
fn canonical_sign_value(_protocol: &str, payload: &serde_json::Value) -> String {
    fn num(payload: &serde_json::Value, key: &str) -> Option<f64> {
        payload.get(key).and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
    }
    // DLMS canonical value = consumption in kWh. Plain fields win first (existing
    // clients), then OBIS active import/export — which arrive in Wh, so /1000 to
    // match the decoder's `consumed_kwh`/`generated_kwh` (dlms.rs). Without this a
    // pure-OBIS meter (no `kwh` field) would sign `{id}:0:{ts}` — the signed value
    // would not match the decoded reading.
    let value = num(payload, "kwh")
        .or_else(|| num(payload, "energy_consumed"))
        .or_else(|| num(payload, "energy_generated"))
        // OBIS active import total (1.1.1.8.0.255) → consumed; export → generated.
        .or_else(|| num(payload, "1.1.1.8.0.255").map(|wh| wh / 1000.0))
        .or_else(|| num(payload, "1.1.2.8.0.255").map(|wh| wh / 1000.0))
        .unwrap_or(0.0);
    value.to_string()
}

/// The ordered set of `(label, bytes)` a REST telemetry signature is checked
/// against — newest canonical form first, legacy forms after. Extracted from
/// `verify_rest_signature` so the fallback ladder is unit-testable without a
/// live Ed25519 verifier (the verifier needs a device pubkey in Redis; that
/// fetch path is covered by e2e, the candidate construction is covered here).
///
/// 1. canonical `{device_id}:{kwh}:{timestamp_ms}` (ms-scale, current signers)
/// 2. second-scale `{device_id}:{kwh}:{timestamp_ms / 1000}` (legacy signers)
/// 3. the serialized JSON object with `signature` removed (keys sorted by
///    serde_json's default `BTreeMap`); only emitted for object payloads.
///
/// An empty label means "no extra log on match" (the caller logs the headline).
fn rest_sign_candidates(
    device_id: &str,
    kwh: &str,
    timestamp_ms: i64,
    payload_val: &serde_json::Value,
) -> Vec<(&'static str, Vec<u8>)> {
    let mut out = vec![
        (
            "",
            format!("{}:{}:{}", device_id, kwh, timestamp_ms).into_bytes(),
        ),
        (
            "second-scale",
            format!("{}:{}:{}", device_id, kwh, timestamp_ms / 1000).into_bytes(),
        ),
    ];
    if let serde_json::Value::Object(map) = payload_val {
        let mut sorted = map.clone();
        sorted.remove("signature");
        if let Ok(bytes) = serde_json::to_vec(&serde_json::Value::Object(sorted)) {
            out.push(("serialized JSON", bytes));
        }
    }
    out
}

async fn verify_rest_signature(
    state: &AppState,
    device_id: &str,
    payload_val: &serde_json::Value,
    signature: &str,
    kwh: &str,
    timestamp_ms: i64,
) -> anyhow::Result<bool> {
    // Try each canonical form in order; a verifier error is fatal (fail-closed),
    // a non-match falls through to the next candidate.
    for (label, target) in rest_sign_candidates(device_id, kwh, timestamp_ms, payload_val) {
        match state
            .signature_verifier
            .verify_telemetry_signature(device_id, &target, signature)
            .await
        {
            Ok(true) => {
                if !label.is_empty() {
                    info!(
                        "✅ Telemetry signature verified (REST, {}) for {}",
                        label, device_id
                    );
                }
                return Ok(true);
            }
            Err(e) => return Err(e),
            _ => {}
        }
    }

    Ok(false)
}

/// Outcome of decrypting an AES-256-GCM `dlms-enc` envelope.
enum DecryptOutcome {
    /// Recovered inner OBIS payload (replaces the encrypted envelope).
    Ok(serde_json::Value),
    /// Replayed / non-increasing invocation counter — reject (anti-replay).
    Replay,
    /// Malformed envelope, missing key, or GCM auth failure — reject.
    Bad(String),
}

/// Decrypt a `dlms-enc` envelope (`payload.enc = {counter, nonce, ciphertext}`)
/// with the meter's per-device AES-256 key, then enforce the monotonic
/// invocation counter for replay protection.
///
/// Order matters: authenticate (GCM, whose AAD binds `device_id:counter`) BEFORE
/// touching the counter store. A forged frame fails GCM and never advances the
/// stored counter, so it cannot lock out a meter by bumping the counter past its
/// legitimate sequence; a replayed *valid* frame authenticates but is then caught
/// by the `<= last` check. Fail-closed throughout (missing key / Redis error /
/// auth failure all reject).
async fn decrypt_dlms_envelope(
    state: &AppState,
    device_id: &str,
    payload: &serde_json::Value,
) -> DecryptOutcome {
    use aes_gcm::aead::{Aead, Payload};
    use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    let enc = match payload.get("enc") {
        Some(e) => e,
        None => return DecryptOutcome::Bad("missing 'enc' envelope".into()),
    };
    let counter = match enc.get("counter").and_then(|v| v.as_i64()) {
        Some(c) => c,
        None => return DecryptOutcome::Bad("missing/invalid counter".into()),
    };
    // `kid` selects a rotated key version; absent => Phase-2 legacy static key.
    let kid = enc.get("kid").and_then(|v| v.as_i64());
    let nonce_b64 = enc
        .get("nonce")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let ct_b64 = enc
        .get("ciphertext")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let nonce_bytes = match STANDARD.decode(nonce_b64) {
        Ok(b) if b.len() == 12 => b,
        _ => return DecryptOutcome::Bad("nonce must be 12 bytes base64".into()),
    };
    let ct = match STANDARD.decode(ct_b64) {
        Ok(b) => b,
        Err(_) => return DecryptOutcome::Bad("ciphertext not base64".into()),
    };

    // Per-device AES key: a `kid` selects the rotated (Vault-wrapped) version;
    // its absence uses the legacy unversioned key (Phase-2). Fail-closed: absent
    // key or Redis/Vault error both reject.
    let key_lookup = match kid {
        Some(v) => {
            state
                .device_key_registry
                .get_device_aes_key_versioned(device_id, v)
                .await
        }
        None => {
            state
                .device_key_registry
                .get_device_aes_key(device_id)
                .await
        }
    };
    let key_bytes = match key_lookup {
        Ok(Some(k)) => k,
        Ok(None) => {
            return DecryptOutcome::Bad(match kid {
                Some(v) => format!("no AES key v{} for {}", v, device_id),
                None => format!("no AES key for {}", device_id),
            })
        }
        Err(e) => return DecryptOutcome::Bad(format!("key lookup failed: {}", e)),
    };

    // Authenticate + decrypt first (AAD binds device_id:counter into the tag).
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let aad = format!("{}:{}", device_id, counter);
    let plaintext = match cipher.decrypt(
        Nonce::from_slice(&nonce_bytes),
        Payload {
            msg: &ct,
            aad: aad.as_bytes(),
        },
    ) {
        Ok(p) => p,
        Err(_) => return DecryptOutcome::Bad("GCM auth/decrypt failed".into()),
    };

    // Only an authenticated frame advances the replay counter.
    match state
        .device_key_registry
        .check_and_bump_counter(device_id, counter)
        .await
    {
        Ok(true) => {}
        Ok(false) => return DecryptOutcome::Replay,
        Err(e) => return DecryptOutcome::Bad(format!("counter check failed: {}", e)),
    }

    match serde_json::from_slice::<serde_json::Value>(&plaintext) {
        Ok(v) => DecryptOutcome::Ok(v),
        Err(e) => DecryptOutcome::Bad(format!("decrypted payload not JSON: {}", e)),
    }
}

pub async fn ingest_private_network(
    State(state): State<AppState>,
    Json(payload): Json<PrivateNetworkPayload>,
) -> impl IntoResponse {
    // 0. Decrypt an AES-256-GCM `dlms-enc` envelope up front, so the signature,
    // kwh and timestamp extraction below all run on the recovered OBIS payload
    // exactly as for a plaintext `dlms` frame.
    let mut payload = payload;
    let was_encrypted = payload.protocol.to_lowercase() == "dlms-enc";
    if was_encrypted {
        match decrypt_dlms_envelope(&state, &payload.device_id, &payload.payload).await {
            DecryptOutcome::Ok(obis) => {
                payload.payload = obis;
                payload.protocol = "dlms".to_string();
            }
            DecryptOutcome::Replay => {
                warn!("🚫 Replayed invocation counter for {}", payload.device_id);
                return (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "replayed invocation counter" })),
                )
                    .into_response();
            }
            DecryptOutcome::Bad(e) => {
                warn!(
                    "🚫 Rejecting encrypted frame for {}: {}",
                    payload.device_id, e
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("decrypt failed: {}", e) })),
                )
                    .into_response();
            }
        }
    }

    // 0b. Secure mode: the meter path must be an authenticated encrypted frame.
    // Reject any non-`dlms-enc` frame (plaintext `dlms`, `simulator`, downgrade)
    // before it can reach a bypass.
    if let Some(code) = secure_mode_gate(secure_mode_enabled(), was_encrypted) {
        warn!(
            "🚫 Secure mode: rejecting non-encrypted frame (protocol '{}') for {}",
            payload.protocol, payload.device_id
        );
        return (
            code,
            Json(json!({ "error": "secure mode requires an encrypted dlms-enc frame" })),
        )
            .into_response();
    }

    // 1. Signature Verification (Ed25519)
    let signature = payload
        .payload
        .get("signature")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    // Unify with gRPC canonical signing format: {meter_id}:{kwh}:{timestamp}
    // Note: 'kwh' in SmartMeterPayload is derived from energy_consumed or energy_generated
    let kwh = payload
        .payload
        .get("kwh")
        .and_then(|v| {
            if let Some(f) = v.as_f64() {
                Some(f)
            } else if let Some(s) = v.as_str() {
                s.parse::<f64>().ok()
            } else {
                None
            }
        })
        .or_else(|| {
            payload
                .payload
                .get("energy_consumed")
                .and_then(|v| v.as_f64())
        })
        .or_else(|| {
            payload
                .payload
                .get("energy_generated")
                .and_then(|v| v.as_f64())
        })
        .unwrap_or(0.0)
        .to_string();

    // We expect ISO-8601 string in REST, but we need timestamp_millis for canonical format
    let timestamp_str = payload
        .payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let timestamp_ms = chrono::DateTime::parse_from_rfc3339(timestamp_str)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0);

    // DLMS/COSEM is the only meter protocol. `auto` or an empty protocol field
    // resolves to `dlms` (no detection needed); `simulator` is the unsigned dev
    // ingest bypass handled below.
    let protocol = resolve_protocol(&payload.protocol);

    if !is_supported_protocol(&protocol) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Unsupported protocol: {}", protocol) })),
        )
            .into_response();
    }

    // The `simulator` protocol is an explicit unsigned dev ingest path (mirrors
    // the batch handler's simulator bypass); everything else must carry a valid
    // signature over the protocol-native canonical value. Fail-CLOSED by default:
    // reject invalid/unverifiable telemetry unless
    // AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY=true.
    let canonical_value = canonical_sign_value(&protocol, &payload.payload);
    // The `simulator` bypass is honored only outside secure mode (and is already
    // unreachable in secure mode — the require-encryption guard above rejects it).
    let is_simulator = protocol == "simulator" && simulator_bypass_allowed();
    let sig_verified = if is_simulator {
        warn!(
            "⚠️ Unsigned `simulator` ingest bypass used for {} (dev path)",
            payload.device_id
        );
        true
    } else {
        let result = verify_rest_signature(
            &state,
            &payload.device_id,
            &payload.payload,
            signature,
            &canonical_value,
            timestamp_ms,
        )
        .await;
        // Fail-closed status decision (403 invalid / 401 verify-error), unless
        // the dev escape hatch is on — see `sig_failure_status`.
        if let Some(code) = sig_failure_status(&result, signature_enforcement_disabled()) {
            let msg = match &result {
                Ok(false) => "Invalid Ed25519 signature".to_string(),
                Err(e) => format!("Verification failed: {}", e),
                Ok(true) => unreachable!("sig_failure_status returns None for Ok(true)"),
            };
            warn!("🚫 REST signature rejected for {}: {}", payload.device_id, msg);
            return (code, Json(json!({ "error": msg }))).into_response();
        }
        match result {
            Ok(true) => {
                info!(
                    "✅ Telemetry signature verified (REST) for {}",
                    payload.device_id
                );
                true
            }
            // Enforcement disabled (dev): proceed but mark the reading unverified.
            _ => false,
        }
    };

    // Special handling for simulator/json protocol to avoid DLMS stack failure
    if protocol == "simulator" {
        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: payload.device_id.clone(),
            device_type: DeviceType::SmartMeter,
            serial_number: payload.device_id.clone(),
            zone_code: payload
                .payload
                .get("zone_code")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            timestamp: chrono::DateTime::parse_from_rfc3339(timestamp_str)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| gridtokenx_telemetry::time::now()),
            metrics: crate::models::DeviceMetrics::Energy {
                generated_kwh: payload
                    .payload
                    .get("energy_generated")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                consumed_kwh: payload
                    .payload
                    .get("energy_consumed")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                net_kwh: kwh.parse::<f64>().unwrap_or(0.0),
            },
            metadata: payload
                .payload
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        };
        return disseminate_reading(&state, reading, sig_verified).await;
    }

    // Only DLMS/COSEM remains; `simulator` returned above, `auto`/empty mapped to dlms.
    let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> = state.dlms_stack.clone();

    // Serialize payload back to bytes for the stack handlers
    let raw_data = serde_json::to_vec(&payload.payload).unwrap_or_default();

    match stack.handle_message(&payload.device_id, &raw_data).await {
        Ok(Some(reading)) => disseminate_reading(&state, reading, sig_verified).await,
        Ok(None) => (
            StatusCode::OK,
            Json(json!({ "status": "processed", "message": "Message handled by stack, no reading generated" })),
        ).into_response(),
        Err(e) => {
            warn!("⚠️ Protocol stack error: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Protocol error: {}", e) })),
            ).into_response()
        }
    }
}

pub async fn ingest_legacy_batch(
    State(state): State<AppState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    info!("📥 Received legacy simulator batch ingestion");

    let readings = match payload.get("readings").and_then(|v| v.as_array()) {
        Some(r) => r,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Missing readings array"})),
            )
                .into_response()
        }
    };

    let mut responses = Vec::new();

    for item in readings {
        let device_id = item
            .get("meter_serial")
            .or_else(|| item.get("meter_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let kwh = item.get("kwh").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let timestamp_str = item.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| gridtokenx_telemetry::time::now());

        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: device_id.to_string(),
            zone_code: item
                .get("zone_code")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            timestamp,
            metrics: crate::models::DeviceMetrics::Energy {
                generated_kwh: item
                    .get("energy_generated")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(kwh),
                consumed_kwh: item
                    .get("energy_consumed")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                net_kwh: kwh,
            },
            metadata: item
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        };

        let reading_id = reading.reading_id.clone();
        let device_type = reading.device_type;

        state.router.disseminate(&reading).await.ok();

        responses.push(IngestResponse {
            status: "accepted",
            reading_id,
            device_type,
            stream: device_type.target_stream().to_string(),
        });
    }

    (StatusCode::OK, Json(responses)).into_response()
}

pub async fn ingest_private_network_batch(
    State(state): State<AppState>,
    Json(payload): Json<BatchPrivateNetworkPayload>,
) -> impl IntoResponse {
    // Secure mode: only the encrypted single-frame REST path is permitted. The
    // batch path carries no per-frame encryption, so reject it wholesale rather
    // than accept plaintext telemetry in a locked-down deployment.
    // Batch carries no per-frame encryption, so it is never "encrypted" — secure
    // mode rejects it wholesale (use the encrypted single-frame path instead).
    if let Some(code) = secure_mode_gate(secure_mode_enabled(), false) {
        warn!("🚫 Secure mode: rejecting plaintext batch ingest (use the encrypted REST path)");
        return (
            code,
            Json(json!({ "error": "secure mode: encrypted single-frame REST ingest only" })),
        )
            .into_response();
    }
    info!(
        "📥 Received private network batch ingestion: protocol={}",
        payload.protocol
    );
    let mut responses = Vec::new();
    // DLMS/COSEM is the only meter protocol; `auto`/empty resolve to `dlms`,
    // `simulator` is the unsigned dev bypass. Validate the declared top protocol
    // once: empty/`auto` are allowed (they resolve to dlms), else it must be a
    // supported resolved protocol.
    let top_protocol = payload.protocol.to_lowercase();
    let top_ok = top_protocol.is_empty()
        || top_protocol == "auto"
        || is_supported_protocol(&top_protocol);
    if !top_ok {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Unsupported protocol: {}", top_protocol) })),
        )
            .into_response();
    }

    for item in payload.readings {
        // `auto`/empty resolve to dlms; otherwise honor the declared protocol.
        let protocol = resolve_protocol(&top_protocol);
        let device_id = item
            .get("device_id")
            .or_else(|| item.get("meter_id"))
            .or_else(|| item.get("meter_serial"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let signature = item
            .get("signature")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        // Verification Logic (same as single ingest)
        let kwh = item
            .get("kwh")
            .and_then(|v| {
                if let Some(f) = v.as_f64() {
                    Some(f)
                } else if let Some(s) = v.as_str() {
                    s.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .or_else(|| item.get("energy_consumed").and_then(|v| v.as_f64()))
            .or_else(|| item.get("energy_generated").and_then(|v| v.as_f64()))
            .unwrap_or(0.0)
            .to_string();

        let timestamp_str = item.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp_ms = chrono::DateTime::parse_from_rfc3339(timestamp_str)
            .map(|dt| dt.timestamp_millis())
            .unwrap_or(0);

        let canonical_value = canonical_sign_value(&protocol, &item);
        // Simulator bypass honored only outside secure mode; in secure mode the
        // frame falls through to verification and is rejected below (unsigned).
        let is_verified = if protocol == "simulator" && simulator_bypass_allowed() {
            true // Skip signature for simulator mode (dev only)
        } else {
            match verify_rest_signature(
                &state,
                device_id,
                &item,
                signature,
                &canonical_value,
                timestamp_ms,
            )
            .await
            {
                Ok(true) => true,
                _ => false,
            }
        };

        if !is_verified && !signature_enforcement_disabled() {
            warn!("🚫 Invalid signature in batch for device {}", device_id);
            continue; // Skip invalid reading (fail-closed by default)
        }

        // Special handling for simulator/json protocol to avoid DLMS stack failure
        if protocol == "simulator" {
            let reading = DeviceReading {
                reading_id: Uuid::new_v4(),
                device_id: device_id.to_string(),
                device_type: DeviceType::SmartMeter,
                serial_number: device_id.to_string(),
                zone_code: item
                    .get("zone_code")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                timestamp: chrono::DateTime::parse_from_rfc3339(timestamp_str)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| gridtokenx_telemetry::time::now()),
                metrics: crate::models::DeviceMetrics::Energy {
                    generated_kwh: item
                        .get("energy_generated")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    consumed_kwh: item
                        .get("energy_consumed")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    net_kwh: kwh.parse::<f64>().unwrap_or(0.0),
                },
                metadata: item
                    .as_object()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            };

            let reading_id = reading.reading_id.clone();
            let device_type = reading.device_type;
            state.router.disseminate(&reading).await.ok();
            responses.push(IngestResponse {
                status: "accepted",
                reading_id,
                device_type,
                stream: device_type.target_stream().to_string(),
            });
            continue;
        }

        // Serialize payload back to bytes for the stack handlers
        let raw_data = serde_json::to_vec(&item).unwrap_or_default();

        // Only DLMS/COSEM remains; `simulator` handled above, `auto`/empty mapped to dlms.
        let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> = state.dlms_stack.clone();

        match stack.handle_message(device_id, &raw_data).await {
            Ok(Some(reading)) => {
                let reading_id = reading.reading_id.clone();
                let device_type = reading.device_type;

                // In batch mode, we handle dissemination sequentially for simplicity
                state.router.disseminate(&reading).await.ok();

                responses.push(IngestResponse {
                    status: "accepted",
                    reading_id,
                    device_type,
                    stream: device_type.target_stream().to_string(),
                });
            }
            _ => {}
        }
    }

    (StatusCode::OK, Json(responses)).into_response()
}

// =============================================================================
// Shared Dissemination
// =============================================================================

#[cfg(test)]
mod tests {
    use super::{
        canonical_sign_value, is_supported_protocol, resolve_protocol, rest_sign_candidates,
        secure_mode_enabled, secure_mode_gate, sig_failure_status, signature_enforcement_disabled,
        simulator_bypass_allowed,
    };
    use axum::http::StatusCode;
    use serde_json::json;
    use std::sync::Mutex;

    // Serialize the env-mutating secure-mode tests — process env is global, so
    // these cannot run concurrently with each other without flaking.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Secure mode hard-overrides every ingest bypass: the unverified-telemetry
    /// escape hatch is ignored and the unsigned `simulator` bypass is refused,
    /// regardless of the dev env vars.
    #[test]
    fn secure_mode_neutralizes_bypasses() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY", "true");

        // Without secure mode, the escape hatch + simulator bypass are honored.
        std::env::remove_var("AGGREGATOR_REQUIRE_SECURE");
        assert!(!secure_mode_enabled());
        assert!(signature_enforcement_disabled()); // hatch active
        assert!(simulator_bypass_allowed());

        // Secure mode forces fail-closed even with the hatch still set to "true".
        std::env::set_var("AGGREGATOR_REQUIRE_SECURE", "true");
        assert!(secure_mode_enabled());
        assert!(!signature_enforcement_disabled()); // hatch overridden
        assert!(!simulator_bypass_allowed());

        std::env::remove_var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY");
        std::env::remove_var("AGGREGATOR_REQUIRE_SECURE");
    }

    // --- REST route status decisions (G1: handler-level status mapping) ---

    #[test]
    fn resolve_protocol_maps_empty_and_auto_to_dlms() {
        assert_eq!(resolve_protocol(""), "dlms");
        assert_eq!(resolve_protocol("auto"), "dlms");
        assert_eq!(resolve_protocol("AUTO"), "dlms"); // case-insensitive
        assert_eq!(resolve_protocol("DLMS"), "dlms"); // lowercased
        assert_eq!(resolve_protocol("simulator"), "simulator");
        assert_eq!(resolve_protocol("modbus"), "modbus"); // unknown passes through verbatim
    }

    #[test]
    fn supported_protocols_are_dlms_and_simulator_only() {
        assert!(is_supported_protocol("dlms"));
        assert!(is_supported_protocol("simulator"));
        assert!(!is_supported_protocol("modbus"));
        assert!(!is_supported_protocol("auto")); // must be resolved first
        assert!(!is_supported_protocol(""));
    }

    #[test]
    fn secure_mode_gate_426s_only_unencrypted_in_secure_mode() {
        // Locked-down + non-encrypted frame → 426 UPGRADE_REQUIRED.
        assert_eq!(
            secure_mode_gate(true, false),
            Some(StatusCode::UPGRADE_REQUIRED)
        );
        // Locked-down but already encrypted → proceed.
        assert_eq!(secure_mode_gate(true, true), None);
        // Not locked-down → never gates, encrypted or not.
        assert_eq!(secure_mode_gate(false, false), None);
        assert_eq!(secure_mode_gate(false, true), None);
    }

    #[test]
    fn sig_failure_status_is_fail_closed_403_invalid_401_error() {
        // Enforcing (hatch off): invalid sig → 403, verify error → 401, valid → proceed.
        assert_eq!(sig_failure_status(&Ok(true), false), None);
        assert_eq!(
            sig_failure_status(&Ok(false), false),
            Some(StatusCode::FORBIDDEN)
        );
        assert_eq!(
            sig_failure_status(&Err(anyhow::anyhow!("redis down")), false),
            Some(StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn sig_failure_status_hatch_open_accepts_unverified() {
        // Enforcement disabled (dev hatch): every outcome proceeds (None).
        assert_eq!(sig_failure_status(&Ok(false), true), None);
        assert_eq!(
            sig_failure_status(&Err(anyhow::anyhow!("redis down")), true),
            None
        );
    }

    /// Outside secure mode, signature enforcement is on by default and only the
    /// explicit env hatch disables it.
    #[test]
    fn enforcement_on_by_default_off_only_via_hatch() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("AGGREGATOR_REQUIRE_SECURE");
        std::env::remove_var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY");
        assert!(!signature_enforcement_disabled()); // fail-closed default
        std::env::set_var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY", "true");
        assert!(signature_enforcement_disabled());
        std::env::remove_var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY");
    }

    // --- REST signature fallback ladder (canonical / sec-scale ts / JSON) ---

    #[test]
    fn sign_candidates_order_and_forms() {
        let payload = json!({"kwh": 10.0, "timestamp": "x", "signature": "SIG"});
        let cands = rest_sign_candidates("METER-1", "10", 1_700_000_000_000, &payload);

        assert_eq!(cands.len(), 3, "object payload yields all 3 forms");
        // 1. canonical ms-scale, no log label (caller logs the headline).
        assert_eq!(cands[0].0, "");
        assert_eq!(cands[0].1, b"METER-1:10:1700000000000");
        // 2. second-scale = ms / 1000.
        assert_eq!(cands[1].0, "second-scale");
        assert_eq!(cands[1].1, b"METER-1:10:1700000000");
        // 3. serialized JSON, signature stripped.
        assert_eq!(cands[2].0, "serialized JSON");
    }

    #[test]
    fn sign_candidates_json_strips_signature_keeps_rest() {
        let payload = json!({"kwh": 10.0, "signature": "SIG", "zone_code": "Z1"});
        let cands = rest_sign_candidates("M", "10", 1000, &payload);
        let json_bytes = &cands[2].1;
        let parsed: serde_json::Value = serde_json::from_slice(json_bytes).unwrap();
        assert!(
            parsed.get("signature").is_none(),
            "signature must be removed from signed bytes"
        );
        assert_eq!(
            parsed.get("kwh"),
            Some(&json!(10.0)),
            "other fields preserved"
        );
        assert_eq!(parsed.get("zone_code"), Some(&json!("Z1")));
    }

    #[test]
    fn sign_candidates_non_object_payload_omits_json_form() {
        // A non-object payload (e.g. bare array) has no signature to strip — only
        // the two string forms are emitted, never a panic.
        let payload = json!([1, 2, 3]);
        let cands = rest_sign_candidates("M", "10", 2000, &payload);
        assert_eq!(
            cands.len(),
            2,
            "non-object payload yields only the 2 string forms"
        );
        assert_eq!(cands[0].1, b"M:10:2000");
        assert_eq!(cands[1].1, b"M:10:2");
    }

    #[test]
    fn sign_candidates_sub_second_ts_floors_to_zero() {
        // timestamp_ms < 1000 → second-scale floors to :0 (parity with i64 /1000).
        let cands = rest_sign_candidates("M", "5", 999, &json!({}));
        assert_eq!(cands[1].1, b"M:5:0");
    }

    #[test]
    fn dlms_signs_kwh() {
        let p = json!({"kwh": 1.5, "energy_consumed": 1.0});
        assert_eq!(canonical_sign_value("dlms", &p), "1.5");
    }

    #[test]
    fn dlms_signs_obis_import_as_kwh() {
        // Real DLMS meter sends OBIS-coded active import (Wh), no `kwh` field.
        // Canonical value must derive from OBIS import / 1000 (kWh), matching the
        // decoder's `consumed_kwh` — NOT fall through to 0.
        let p = json!({"1.1.1.8.0.255": 10000.0});
        assert_eq!(canonical_sign_value("dlms", &p), "10");
    }

    #[test]
    fn dlms_obis_export_fallback_when_no_import() {
        // Export-only meter (e.g. pure generation): fall back to OBIS export / 1000.
        let p = json!({"1.1.2.8.0.255": 5000.0});
        assert_eq!(canonical_sign_value("dlms", &p), "5");
    }

    #[test]
    fn dlms_obis_import_wins_over_export() {
        let p = json!({"1.1.1.8.0.255": 7000.0, "1.1.2.8.0.255": 5000.0});
        assert_eq!(canonical_sign_value("dlms", &p), "7");
    }

    #[test]
    fn dlms_kwh_field_wins_over_obis() {
        // Explicit `kwh` field takes precedence over OBIS — existing clients unaffected.
        let p = json!({"kwh": 1.5, "1.1.1.8.0.255": 99000.0});
        assert_eq!(canonical_sign_value("dlms", &p), "1.5");
    }

    #[test]
    fn falls_back_to_energy_generated() {
        let p = json!({"energy_generated": 3.0});
        assert_eq!(canonical_sign_value("dlms", &p), "3");
    }

    #[test]
    fn parses_stringified_numbers() {
        let p = json!({"kwh": "1.5"});
        assert_eq!(canonical_sign_value("dlms", &p), "1.5");
    }
}

pub async fn disseminate_reading(
    state: &AppState,
    reading: DeviceReading,
    is_verified: bool,
) -> axum::response::Response {
    let response = IngestResponse {
        status: "accepted",
        reading_id: reading.reading_id,
        device_type: reading.device_type,
        stream: reading.device_type.target_stream().to_string(),
    };

    match state.router.disseminate(&reading).await {
        Ok(_) => {
            // --- Kafka Streaming (NEW ARCHITECTURE) ---
            if let Some(kafka) = &state.kafka_producer {
                let kafka_reading = reading.clone();
                let kafka_producer = kafka.clone();
                tokio::spawn(async move {
                    let (gen, con, net) = match kafka_reading.metrics {
                        crate::models::DeviceMetrics::Energy {
                            generated_kwh,
                            consumed_kwh,
                            net_kwh,
                        } => (generated_kwh, consumed_kwh, net_kwh),
                        _ => (0.0, 0.0, 0.0),
                    };

                    let event = crate::infra::kafka::MeterReadingEvent {
                        meter_id: kafka_reading.device_id,
                        timestamp: kafka_reading.timestamp.timestamp_millis(),
                        energy_generated: gen,
                        energy_consumed: con,
                        surplus: net,
                        voltage: kafka_reading
                            .metadata
                            .get("voltage_v")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(230.0),
                        frequency: kafka_reading
                            .metadata
                            .get("frequency_hz")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(50.0),
                        power_factor: kafka_reading
                            .metadata
                            .get("power_factor")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(1.0),
                        signature: kafka_reading
                            .metadata
                            .get("signature")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        verified: is_verified,
                        confidence_score: if is_verified { 1.0 } else { 0.0 },
                    };

                    if let Err(e) = kafka_producer.publish_meter_reading(&event).await {
                        error!("❌ Kafka publish failed: {}", e);
                    }
                });
            }

            // --- RabbitMQ Validation (NEW ARCHITECTURE) ---
            if let Some(rmq) = &state.rabbitmq_producer {
                let rmq_producer = rmq.clone();
                let meter_id = reading.device_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = rmq_producer.submit_validation_job(&meter_id).await {
                        error!("❌ RabbitMQ validation job failed: {}", e);
                    }
                });
            }

            (StatusCode::ACCEPTED, Json(json!(response))).into_response()
        }
        Err(e) => {
            error!("❌ Failed to disseminate reading: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to disseminate reading" })),
            )
                .into_response()
        }
    }
}
