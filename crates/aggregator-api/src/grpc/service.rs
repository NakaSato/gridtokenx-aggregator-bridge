use buffa::view::OwnedView;
use chrono::{TimeZone, Utc};
use connectrpc::{ConnectError, Context};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::handlers::disseminate_reading;
use crate::models::{DeviceMetrics, DeviceReading, DeviceType};
use crate::protocol::DlmsBinaryFrame;
use crate::state::AppState;
use serde_json::json;

/// Generated code from proto (Buffa / ConnectRPC), now in the aggregator-protocol crate.
pub use aggregator_protocol::oracle as proto;

use proto::{
    BulkRawRequestView, BulkRawResponse, IngestResponse, MeterReadingBatchRequestView,
    MeterReadingBatchResponse, MeterReadingView, OracleService,
};

/// Implementation of the Unified Aggregator gRPC Service (UTT)
pub struct AggregatorServiceImpl {
    pub state: AppState,
}

impl AggregatorServiceImpl {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Register the service with the ConnectRPC router
    pub fn register_service(self: Arc<Self>, router: connectrpc::Router) -> connectrpc::Router {
        use proto::OracleServiceExt;
        self.register(router)
    }

    /// Resolve the per-device AES key and decrypt+parse a secure v4 DLMS frame.
    ///
    /// Order (the decoder stays key-agnostic; the lookup lives here, not in `aggregator-stacks`):
    /// `parse_header` → LDN (`meter_id`) → `device_key_registry.get_device_aes_key` →
    /// `parse(bytes, Some(key))`.
    ///
    /// Policy mirrors the Ed25519 signature gate (skip-frame, not panic):
    /// - **production** (`ENVIRONMENT=production`): a missing/invalid `enckey` ⇒ frame **rejected**
    ///   (`None` + `warn!`), never silently decoded as plaintext — fail-closed.
    /// - **dev/legacy**: plaintext fallback runs **only** when `ALLOW_PLAINTEXT_DLMS=true`
    ///   (logged loud); otherwise the frame is skipped.
    /// - **Redis-unreachable / malformed key**: `get_device_aes_key` returns `Err`; we treat it as
    ///   fail-closed-loud (`error!` + `None`) — an unreachable key store must never decode telemetry.
    ///
    /// Returns `None` when the frame could not be securely decoded; callers skip it (or fall back to
    /// the request's standard signed fields, exactly as they did when the legacy `parse` errored).
    async fn decode_secure_frame(&self, frame_bytes: &[u8]) -> Option<DlmsBinaryFrame> {
        // Header first — it's plaintext and carries the meter identity (LDN) for the key lookup.
        let header = match DlmsBinaryFrame::parse_header(frame_bytes) {
            Ok(h) => h,
            Err(e) => {
                warn!("⚠️ DLMS header parse failed: {}", e);
                return None;
            }
        };
        let meter_id = header.logical_device_name.as_str();

        let is_production = std::env::var("ENVIRONMENT").unwrap_or_default() == "production";
        // Secure mode hard-overrides the dev plaintext fallback (see `plaintext_dlms_allowed`).
        let allow_plaintext = plaintext_dlms_allowed(
            std::env::var("ALLOW_PLAINTEXT_DLMS").unwrap_or_default() == "true",
            crate::handlers::secure_mode_enabled(),
        );

        let key = match self
            .state
            .device_key_registry
            .get_device_aes_key(meter_id)
            .await
        {
            Ok(k) => k,
            Err(e) => {
                // Fail-closed-loud: key store unreachable / key malformed — never decode unverified.
                error!(
                    "🚫 DLMS key fetch failed for meter={}: {} — frame skipped",
                    meter_id, e
                );
                return None;
            }
        };

        // Pure decision — Redis already resolved. Unit-tested without AppState/Redis.
        apply_dlms_key_policy(frame_bytes, meter_id, key, is_production, allow_plaintext)
    }
}

/// Pure decode-policy decision for a secure v4 DLMS frame, given the already-resolved per-device key.
///
/// Redis- and AppState-free so the full branch matrix is unit-testable. The caller
/// ([`AggregatorServiceImpl::decode_secure_frame`]) handles the fail-closed-loud Redis-error path
/// *before* this is reached; here `key` is either the genuinely-resolved enckey (`Some`) or a
/// genuinely-absent key (`None`).
///
/// - `Some(key)`: decrypt; GCM auth failure ⇒ `None` (skip, never garbage TLVs).
/// - `None` + production: reject (`None`) — never plaintext-decode an unkeyed frame.
/// - `None` + `allow_plaintext`: plaintext fallback (dev only, logged loud).
/// - `None` otherwise: skip.
fn apply_dlms_key_policy(
    frame_bytes: &[u8],
    meter_id: &str,
    key: Option<[u8; 32]>,
    is_production: bool,
    allow_plaintext: bool,
) -> Option<DlmsBinaryFrame> {
    match key {
        Some(k) => match DlmsBinaryFrame::parse(frame_bytes, Some(&k)) {
            Ok(f) => Some(f),
            Err(e) => {
                warn!(
                    "🚫 DLMS decrypt failed for meter={}: {} — frame skipped",
                    meter_id, e
                );
                None
            }
        },
        None if is_production => {
            warn!(
                "🚫 No enckey for meter={} under ENVIRONMENT=production — frame skipped (fail-closed)",
                meter_id
            );
            None
        }
        None if allow_plaintext => match DlmsBinaryFrame::parse(frame_bytes, None) {
            Ok(f) => {
                warn!(
                    "⚠️ ALLOW_PLAINTEXT_DLMS=true — meter={} decoded as PLAINTEXT (dev only)",
                    meter_id
                );
                Some(f)
            }
            Err(e) => {
                warn!(
                    "⚠️ DLMS plaintext parse failed for meter={}: {}",
                    meter_id, e
                );
                None
            }
        },
        None => {
            warn!(
                "🚫 No enckey for meter={} and ALLOW_PLAINTEXT_DLMS not set — frame skipped",
                meter_id
            );
            None
        }
    }
}

/// Effective plaintext-DLMS permission, given the `ALLOW_PLAINTEXT_DLMS` env flag and
/// secure mode. Secure mode hard-overrides the flag off — an unkeyed frame is skipped
/// (fail-closed), exactly as under `ENVIRONMENT=production`. Pure so the secure-mode
/// override is unit-testable without AppState/env.
fn plaintext_dlms_allowed(allow_plaintext_env: bool, secure_mode: bool) -> bool {
    !secure_mode && allow_plaintext_env
}

/// Effective bulk skip-verify permission, given the `SKIP_SIG_VERIFY` env flag and
/// secure mode. Secure mode hard-overrides the flag off so every bulk frame is
/// verified in a locked-down deployment. Pure so the override is unit-testable.
fn bulk_skip_verify_allowed(skip_verify_env: bool, secure_mode: bool) -> bool {
    !secure_mode && skip_verify_env
}

/// Split a packed bulk-raw payload into `(frame_bytes, signature)` pairs.
///
/// Wire layout per entry: `[frame_len: 1B][frame: frame_len B][signature: 64B]`.
/// Parsing stops (and drops the trailing bytes) at the first entry that would run
/// past the buffer — a truncated tail is ignored, never partially processed, so a
/// frame can never be paired with the wrong or a truncated signature. Pure (no
/// Redis/decrypt) so the framing + bounds matrix is unit-testable.
fn split_bulk_frames(payload: &[u8]) -> Vec<(&[u8], [u8; 64])> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while cursor < payload.len() {
        let frame_len = payload[cursor] as usize;
        cursor += 1;
        // Need the full frame AND its 64-byte signature, or stop.
        if cursor + frame_len + 64 > payload.len() {
            break;
        }
        let frame_bytes = &payload[cursor..cursor + frame_len];
        cursor += frame_len;
        let mut sig = [0u8; 64];
        sig.copy_from_slice(&payload[cursor..cursor + 64]);
        cursor += 64;
        out.push((frame_bytes, sig));
    }
    out
}

/// Canonical Ed25519 sign-target for a gRPC meter reading:
/// `{meter_id}:{kwh}:{timestamp}`. Mirrors the REST canonical
/// `{device_id}:{value}:{timestamp}` (see `handlers::canonical_sign_value`) so a
/// meter signs an identical target on either transport. Pure (one source of the
/// format) so a drift in the separator/order is caught.
fn grpc_sign_target(
    meter_id: impl std::fmt::Display,
    kwh: impl std::fmt::Display,
    timestamp: i64,
) -> String {
    format!("{meter_id}:{kwh}:{timestamp}")
}

// Unified Ingestion Pattern: all telemetry handled through a single verified entry point
impl OracleService for AggregatorServiceImpl {
    /// Bulk Raw Ingestion: Optimized for high-throughput simulators.
    /// Payload is packed binary frames: [FrameLen(1b)] + [ProtocolV4Frame] + [Ed25519Signature(64b)]
    async fn bulk_raw_ingest(
        &self,
        ctx: Context,
        request: OwnedView<BulkRawRequestView<'static>>,
    ) -> Result<(BulkRawResponse, Context), ConnectError> {
        let payload = &request.payload;
        let mut processed_count = 0;

        let mut meter_ids = Vec::with_capacity(request.meter_count as usize);
        let mut payloads_for_sig = Vec::with_capacity(request.meter_count as usize);
        let mut signatures = Vec::with_capacity(request.meter_count as usize);
        let mut frames = Vec::with_capacity(request.meter_count as usize);

        // 1. Unpack (pure framing/bounds in split_bulk_frames) and pre-parse frames.
        for (frame_bytes, sig_bytes) in split_bulk_frames(payload) {
            // Resolve per-device key, decrypt, parse (prod/dev policy in decode_secure_frame).
            // Bulk verifies against the binary payload for speed (see step 2).
            if let Some(frame) = self.decode_secure_frame(frame_bytes).await {
                let meter_id = frame.logical_device_name.clone();
                meter_ids.push(meter_id);
                payloads_for_sig.push(frame_bytes.to_vec());
                signatures.push(sig_bytes);
                frames.push(frame);
            }
        }

        // 2. Batch Signature Verification
        // Secure mode hard-overrides the SKIP_SIG_VERIFY bulk bypass (see `bulk_skip_verify_allowed`).
        let skip_verify = bulk_skip_verify_allowed(
            std::env::var("SKIP_SIG_VERIFY").unwrap_or_default() == "true",
            crate::handlers::secure_mode_enabled(),
        );
        let verification_results = if skip_verify {
            warn!(
                "⚠️ SKIP_SIG_VERIFY=true — bypassing Ed25519 verification for {} bulk frame(s); telemetry accepted UNVERIFIED (dev only)",
                meter_ids.len()
            );
            vec![true; meter_ids.len()]
        } else {
            self.state
                .signature_verifier
                .verify_telemetry_signature_batch(&meter_ids, &payloads_for_sig, &signatures)
                .await
                .map_err(|e| {
                    ConnectError::internal(format!("Batch signature verification failed: {}", e))
                })?
        };

        // 3. Process Verified Frames
        for (i, is_verified) in verification_results.into_iter().enumerate() {
            if !is_verified {
                warn!(
                    "🚫 Bulk ingest: Invalid signature for meter={}",
                    meter_ids[i]
                );
                continue;
            }

            let frame = &frames[i];
            let generated_kwh = frame.active_energy_export_wh.unwrap_or(0) as f64 / 1000.0;
            let consumed_kwh = frame.active_energy_import_wh.unwrap_or(0) as f64 / 1000.0;

            let mut metadata = HashMap::new();
            if let Some(cv) = frame.voltage_cv {
                metadata.insert("voltage_v".to_string(), json!((cv as f64) / 100.0));
            }
            if let Some(ma) = frame.current_ma {
                metadata.insert("current_a".to_string(), json!((ma as f64) / 1000.0));
            }
            if let Some(bps) = frame.battery_soc_bps {
                metadata.insert("battery_level_pct".to_string(), json!((bps as f64) / 100.0));
            }
            metadata.insert(
                "dlms_manufacturer_id".to_string(),
                json!(frame.manufacturer_id),
            );
            metadata.insert("ingest_mode".to_string(), json!("bulk_raw"));

            let reading = DeviceReading {
                reading_id: Uuid::new_v4(),
                device_id: meter_ids[i].clone(),
                device_type: DeviceType::SmartMeter,
                serial_number: meter_ids[i].clone(),
                zone_code: None, // Would need lookup or inclusion in frame
                timestamp: frame.timestamp,
                metrics: DeviceMetrics::Energy {
                    generated_kwh,
                    consumed_kwh,
                    net_kwh: generated_kwh - consumed_kwh,
                },
                metadata,
            };

            // Reached only after the per-frame verification gate above passed.
            let _ = disseminate_reading(&self.state, reading, true).await;
            processed_count += 1;
        }

        Ok((
            BulkRawResponse {
                processed_count,
                status: "success".to_string(),
                ..Default::default()
            },
            ctx,
        ))
    }

    /// Unified Ingest: Handles verified telemetry for VPP operations
    async fn ingest(
        &self,
        ctx: Context,
        request: OwnedView<MeterReadingView<'static>>,
    ) -> Result<(IngestResponse, Context), ConnectError> {
        info!(
            "📡 Unified Trusted Ingestion (UTT): meter={}",
            request.meter_id
        );

        // --- UTT-H hardened anti-replay verification ---
        // Fail-CLOSED by default: signed-but-invalid is always rejected; unsigned
        // is rejected unless AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY=true.
        let sig_verified = if let Some(signature) = request.signature.as_deref() {
            // Reconstruct canonical target with sequence/ms support (UTT-H Protocol)
            // Note: In a real deployment, sequence would be tracked in Redis to prevent reuse.
            let sign_target =
                grpc_sign_target(&request.meter_id, &request.kwh, request.timestamp);

            let is_verified = match self
                .state
                .signature_verifier
                .verify_telemetry_signature(&request.meter_id, sign_target.as_bytes(), signature)
                .await
            {
                Ok(true) => true,
                _ => {
                    // Fallback to binary payload verification (which now includes CRC-32 and Versioning)
                    if !request.raw_payload.is_empty() {
                        match self
                            .state
                            .signature_verifier
                            .verify_telemetry_signature(
                                &request.meter_id,
                                &request.raw_payload,
                                signature,
                            )
                            .await
                        {
                            Ok(true) => {
                                info!(
                                    "✅ UTT-H signature verified against binary payload for {}",
                                    request.meter_id
                                );
                                true
                            }
                            _ => false,
                        }
                    } else {
                        false
                    }
                }
            };

            if !is_verified {
                error!(
                    "🚫 UTT-H Integrity check failed for meter {}",
                    request.meter_id
                );
                return Err(ConnectError::permission_denied(
                    "UTT-H Signature Verification Failed",
                ));
            }
            true
        } else {
            warn!(
                "⚠️ Received unsigned telemetry from meter={}",
                request.meter_id
            );
            if !crate::handlers::signature_enforcement_disabled() {
                return Err(ConnectError::invalid_argument("Signature required"));
            }
            false
        };

        let mut generated_kwh = request
            .energy_generated
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let mut consumed_kwh = request
            .energy_consumed
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let mut timestamp = Utc
            .timestamp_opt(request.timestamp, 0)
            .single()
            .unwrap_or_else(|| gridtokenx_telemetry::time::now());
        let mut metadata = HashMap::new();

        // DLMS/COSEM (IEC 62056) Decoding
        if !request.raw_payload.is_empty() {
            // Decode the secure frame if it carries one; on skip (None) we keep the request's
            // standard signed fields (the signature gate above already passed) — same fallback
            // the legacy parse-error path used.
            match self.decode_secure_frame(&request.raw_payload).await {
                Some(frame) => {
                    timestamp = frame.timestamp;
                    if let Some(wh) = frame.active_energy_export_wh {
                        generated_kwh = (wh as f64) / 1000.0;
                    }
                    if let Some(wh) = frame.active_energy_import_wh {
                        consumed_kwh = (wh as f64) / 1000.0;
                    }
                    if let Some(cv) = frame.voltage_cv {
                        metadata.insert("voltage_v".to_string(), json!((cv as f64) / 100.0));
                    }
                    if let Some(ma) = frame.current_ma {
                        metadata.insert("current_a".to_string(), json!((ma as f64) / 1000.0));
                    }
                    if let Some(bps) = frame.battery_soc_bps {
                        metadata
                            .insert("battery_level_pct".to_string(), json!((bps as f64) / 100.0));
                    }
                    metadata.insert(
                        "dlms_manufacturer_id".to_string(),
                        json!(frame.manufacturer_id),
                    );
                }
                // Frame skipped (decode_secure_frame already logged why) — fall back to standard fields.
                None => {}
            }
        }

        let reading = DeviceReading {
            reading_id: request
                .reading_id
                .parse()
                .unwrap_or_else(|_| Uuid::new_v4()),
            device_id: request.meter_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: request.meter_serial.to_string(),
            zone_code: request.zone_code.as_deref().map(|s| s.to_string()),
            timestamp,
            metrics: DeviceMetrics::Energy {
                generated_kwh,
                consumed_kwh,
                net_kwh: request.kwh.parse().unwrap_or(generated_kwh - consumed_kwh),
            },
            metadata,
        };

        // Dissemination: fan out to VPP streams (Redis/Kafka) and local aggregation
        let res = disseminate_reading(&self.state, reading, sig_verified).await;

        if res.status().is_success() {
            Ok((
                IngestResponse {
                    receipt_id: Uuid::new_v4().to_string(),
                    status: "accepted".to_string(),
                    ..Default::default()
                },
                ctx,
            ))
        } else {
            error!("❌ Ingestion failed for meter={}", request.meter_id);
            Err(ConnectError::internal("Platform dissemination failed"))
        }
    }

    /// Batch Ingest: Optimized pipeline for large-scale meter deployments
    async fn ingest_batch(
        &self,
        ctx: Context,
        request: OwnedView<MeterReadingBatchRequestView<'static>>,
    ) -> Result<(MeterReadingBatchResponse, Context), ConnectError> {
        let mut receipt_ids = Vec::new();
        let mut accepted_count = 0;
        let mut rejected_count = 0;

        for tel in &request.readings {
            // Signature verification (required for UTT). Fail-CLOSED by default:
            // signed-but-invalid and unsigned are both rejected unless
            // AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY=true.
            let sig_verified = if let Some(signature) = tel.signature.as_deref() {
                let sign_target = grpc_sign_target(tel.meter_id, tel.kwh, tel.timestamp);
                let is_verified = match self
                    .state
                    .signature_verifier
                    .verify_telemetry_signature(&tel.meter_id, sign_target.as_bytes(), signature)
                    .await
                {
                    Ok(true) => true,
                    _ => false,
                };

                if !is_verified {
                    warn!("🚫 Invalid signature in batch for meter={}", tel.meter_id);
                    rejected_count += 1;
                    continue;
                }
                true
            } else if !crate::handlers::signature_enforcement_disabled() {
                rejected_count += 1;
                continue;
            } else {
                false
            };

            let mut generated_kwh = tel
                .energy_generated
                .as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let mut consumed_kwh = tel
                .energy_consumed
                .as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let mut timestamp = Utc
                .timestamp_opt(tel.timestamp, 0)
                .single()
                .unwrap_or_else(|| gridtokenx_telemetry::time::now());
            let mut metadata = HashMap::new();

            if !tel.raw_payload.is_empty() {
                if let Some(frame) = self.decode_secure_frame(&tel.raw_payload).await {
                    timestamp = frame.timestamp;
                    if let Some(wh) = frame.active_energy_export_wh {
                        generated_kwh = (wh as f64) / 1000.0;
                    }
                    if let Some(wh) = frame.active_energy_import_wh {
                        consumed_kwh = (wh as f64) / 1000.0;
                    }
                    metadata.insert(
                        "dlms_manufacturer_id".to_string(),
                        json!(frame.manufacturer_id),
                    );
                }
            }

            let reading = DeviceReading {
                reading_id: tel.reading_id.parse().unwrap_or_else(|_| Uuid::new_v4()),
                device_id: tel.meter_id.to_string(),
                device_type: DeviceType::SmartMeter,
                serial_number: tel.meter_serial.to_string(),
                zone_code: tel.zone_code.as_deref().map(|s| s.to_string()),
                timestamp,
                metrics: DeviceMetrics::Energy {
                    generated_kwh,
                    consumed_kwh,
                    net_kwh: tel.kwh.parse().unwrap_or(generated_kwh - consumed_kwh),
                },
                metadata,
            };

            let res = disseminate_reading(&self.state, reading, sig_verified).await;
            if res.status().is_success() {
                receipt_ids.push(Uuid::new_v4().to_string());
                accepted_count += 1;
            } else {
                rejected_count += 1;
            }
        }

        Ok((
            MeterReadingBatchResponse {
                receipt_ids,
                status: if rejected_count == 0 {
                    "all_accepted"
                } else {
                    "partially_accepted"
                }
                .to_string(),
                accepted_count,
                rejected_count,
                ..Default::default()
            },
            ctx,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::{aead::Aead, Aes256Gcm, Key, KeyInit, Nonce};
    use crc32fast::Hasher;

    const V4: u8 = 0x04;
    const METER: &str = "METER042";

    /// Build a well-formed v4 frame carrying one import-energy TLV (tag 1 = 5000 Wh).
    /// `key = Some` ⇒ AES-256-GCM encrypted block; `key = None` ⇒ plaintext TLV block.
    fn build_frame(key: Option<&[u8; 32]>, ldn: &str) -> Vec<u8> {
        let manuf = b"INC";
        let ts_bytes = 1700000000_u64.to_be_bytes();

        let mut tlv = Vec::new();
        tlv.push(1);
        tlv.push(8);
        tlv.extend_from_slice(&5000_u64.to_be_bytes());

        let block = match key {
            Some(k) => {
                let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(k));
                let mut nonce = [0u8; 12];
                nonce[0..3].copy_from_slice(manuf);
                nonce[3..11].copy_from_slice(&ts_bytes);
                nonce[11] = V4;
                cipher
                    .encrypt(Nonce::from_slice(&nonce), tlv.as_slice())
                    .unwrap()
            }
            None => tlv,
        };

        let mut ldn_bytes = [0u8; 8];
        let src = ldn.as_bytes();
        let n = src.len().min(8);
        ldn_bytes[..n].copy_from_slice(&src[..n]);

        let mut p = Vec::new();
        p.push(V4);
        p.push(0); // total-length placeholder
        p.extend_from_slice(manuf);
        p.extend_from_slice(&ldn_bytes);
        p.extend_from_slice(&ts_bytes);
        p.extend_from_slice(&block);
        p[1] = (p.len() - 2 + 4) as u8;
        let mut h = Hasher::new();
        h.update(&p);
        p.extend_from_slice(&h.finalize().to_be_bytes());
        p
    }

    // --- the resolved-key is present ---

    #[test]
    fn policy_decrypts_with_correct_key() {
        let key = [7u8; 32];
        let frame = build_frame(Some(&key), METER);
        let out = apply_dlms_key_policy(&frame, METER, Some(key), false, false);
        assert_eq!(out.unwrap().active_energy_import_wh, Some(5000));
    }

    #[test]
    fn policy_rejects_wrong_key() {
        let frame = build_frame(Some(&[7u8; 32]), METER);
        // GCM auth-tag failure ⇒ skip, never garbage TLVs.
        let out = apply_dlms_key_policy(&frame, METER, Some([9u8; 32]), false, false);
        assert!(out.is_none());
    }

    #[test]
    fn policy_keyed_device_must_encrypt() {
        // Device has an enckey but sent a plaintext frame ⇒ GCM decrypt fails ⇒ rejected.
        let frame = build_frame(None, METER);
        let out = apply_dlms_key_policy(&frame, METER, Some([7u8; 32]), false, false);
        assert!(out.is_none());
    }

    // --- no key provisioned (genuinely absent) ---

    #[test]
    fn policy_production_no_key_rejects() {
        // Fail-closed: encrypted frame, no enckey, production ⇒ never plaintext-decode.
        let frame = build_frame(Some(&[7u8; 32]), METER);
        let out = apply_dlms_key_policy(&frame, METER, None, true, false);
        assert!(out.is_none());
    }

    #[test]
    fn policy_dev_no_key_no_flag_rejects() {
        // Dev, no enckey, ALLOW_PLAINTEXT_DLMS off ⇒ skip (default-deny).
        let frame = build_frame(Some(&[7u8; 32]), METER);
        let out = apply_dlms_key_policy(&frame, METER, None, false, false);
        assert!(out.is_none());
    }

    #[test]
    fn policy_dev_plaintext_flag_decodes_plaintext_frame() {
        // Dev escape hatch: no enckey + flag on + genuinely-plaintext frame ⇒ decoded.
        let frame = build_frame(None, METER);
        let out = apply_dlms_key_policy(&frame, METER, None, false, true);
        assert_eq!(out.unwrap().active_energy_import_wh, Some(5000));
    }

    #[test]
    fn policy_production_overrides_plaintext_flag() {
        // Even if ALLOW_PLAINTEXT_DLMS is set, production + no key still rejects (prod wins).
        let frame = build_frame(None, METER);
        let out = apply_dlms_key_policy(&frame, METER, None, true, true);
        assert!(out.is_none());
    }

    // --- secure-mode hard-override of the gRPC bypasses ---

    #[test]
    fn secure_mode_forces_plaintext_dlms_off() {
        // Dev flag on but secure mode forces it off (fail-closed); off only via env when not secure.
        assert!(!plaintext_dlms_allowed(true, true));
        assert!(plaintext_dlms_allowed(true, false));
        assert!(!plaintext_dlms_allowed(false, false));
    }

    #[test]
    fn secure_mode_forces_bulk_verify_on() {
        // SKIP_SIG_VERIFY honored only outside secure mode; secure mode always verifies.
        assert!(!bulk_skip_verify_allowed(true, true));
        assert!(bulk_skip_verify_allowed(true, false));
        assert!(!bulk_skip_verify_allowed(false, false));
    }

    // --- bulk-raw payload framing (security: frame ↔ signature pairing + bounds) ---

    /// Pack one bulk entry: `[len][frame][64-byte sig]`.
    fn pack_entry(frame: &[u8], sig_fill: u8) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(frame.len() as u8);
        v.extend_from_slice(frame);
        v.extend_from_slice(&[sig_fill; 64]);
        v
    }

    #[test]
    fn split_bulk_frames_parses_each_entry_with_its_own_signature() {
        let mut payload = pack_entry(&[0xAA, 0xBB], 1);
        payload.extend(pack_entry(&[0xCC], 2));

        let out = split_bulk_frames(&payload);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, &[0xAA, 0xBB]);
        assert_eq!(out[0].1, [1u8; 64]);
        assert_eq!(out[1].0, &[0xCC]);
        assert_eq!(out[1].1, [2u8; 64], "each frame keeps its own signature");
    }

    #[test]
    fn split_bulk_frames_drops_truncated_tail_not_partial() {
        // One valid entry followed by a declared len that overruns the buffer.
        let mut payload = pack_entry(&[0x01], 7);
        payload.push(50); // claims a 50-byte frame; nowhere near present
        payload.extend_from_slice(&[0xFF, 0xFF]);

        let out = split_bulk_frames(&payload);
        assert_eq!(out.len(), 1, "the complete entry parses; the truncated tail is dropped");
        assert_eq!(out[0].0, &[0x01]);
    }

    #[test]
    fn split_bulk_frames_empty_payload_is_empty() {
        assert!(split_bulk_frames(&[]).is_empty());
    }

    #[test]
    fn split_bulk_frames_zero_length_frame_still_consumes_signature() {
        // A 0-length frame is legal framing: len byte + 0 frame bytes + 64 sig.
        let payload = pack_entry(&[], 9);
        let out = split_bulk_frames(&payload);
        assert_eq!(out.len(), 1);
        assert!(out[0].0.is_empty());
        assert_eq!(out[0].1, [9u8; 64]);
    }

    #[test]
    fn split_bulk_frames_missing_signature_bytes_is_dropped() {
        // Frame present but fewer than 64 signature bytes follow ⇒ entry dropped
        // (never pair a frame with a short/garbage signature).
        let mut payload = Vec::new();
        payload.push(1); // len = 1
        payload.push(0x42); // the frame byte
        payload.extend_from_slice(&[0u8; 10]); // only 10 of the 64 sig bytes
        assert!(split_bulk_frames(&payload).is_empty());
    }

    // --- canonical gRPC sign-target ---

    #[test]
    fn grpc_sign_target_is_meter_kwh_timestamp() {
        assert_eq!(grpc_sign_target("METER042", "12.5", 1_700_000_000), "METER042:12.5:1700000000");
        // Zero / negative timestamps render verbatim (no flooring on the gRPC path).
        assert_eq!(grpc_sign_target("M", "0", 0), "M:0:0");
    }
}
