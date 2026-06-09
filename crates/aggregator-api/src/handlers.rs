use crate::models::{
    BatchPrivateNetworkPayload, DeviceReading, DeviceType, IngestResponse, PrivateNetworkPayload,
};
use crate::protocol::stacks::ProtocolStack;
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

// Helper function to verify signature on REST ingestion paths
/// Whether telemetry signature enforcement is disabled (dev/test escape hatch).
///
/// Default (env unset) is fail-CLOSED: telemetry with an invalid or unverifiable
/// Ed25519 signature is rejected. Set `AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY=true`
/// only in trusted dev/test environments to accept unverified readings.
pub fn signature_enforcement_disabled() -> bool {
    std::env::var("AGGREGATOR_ALLOW_UNVERIFIED_TELEMETRY").unwrap_or_default() == "true"
}

/// The numeric field a device of `protocol` signs in its canonical
/// `{device_id}:{value}:{timestamp}` Ed25519 sign-target.
///
/// DLMS-family meters sign consumed energy (kWh); other stacks sign their native
/// primary measure — OCPP `energy_wh`, SunSpec `WH`, OpenADR `actual_kw`. The DLMS
/// value resolves `kwh` → `energy_consumed` → `energy_generated` → OBIS active
/// import (1.1.1.8.0.255, Wh/1000) → OBIS export (1.1.2.8.0.255, Wh/1000), so a
/// real OBIS-coded meter signs the same kWh the decoder reconstructs. Falls back
/// to these DLMS fields so an unrecognized protocol still has a value to verify.
fn canonical_sign_value(protocol: &str, payload: &serde_json::Value) -> String {
    fn num(payload: &serde_json::Value, key: &str) -> Option<f64> {
        payload
            .get(key)
            .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
    }
    // DLMS canonical value = consumption in kWh. Plain fields win first (existing
    // clients), then OBIS active import/export — which arrive in Wh, so /1000 to
    // match the decoder's `consumed_kwh`/`generated_kwh` (dlms.rs). Without this a
    // pure-OBIS meter (no `kwh` field) would sign `{id}:0:{ts}` — the signed value
    // would not match the decoded reading.
    let dlms_energy = || {
        num(payload, "kwh")
            .or_else(|| num(payload, "energy_consumed"))
            .or_else(|| num(payload, "energy_generated"))
            // OBIS active import total (1.1.1.8.0.255) → consumed; export → generated.
            .or_else(|| num(payload, "1.1.1.8.0.255").map(|wh| wh / 1000.0))
            .or_else(|| num(payload, "1.1.2.8.0.255").map(|wh| wh / 1000.0))
            .unwrap_or(0.0)
    };
    let value = match protocol {
        "ocpp" => num(payload, "energy_wh").unwrap_or_else(dlms_energy),
        "sunspec" => num(payload, "WH")
            .or_else(|| num(payload, "W"))
            .unwrap_or_else(dlms_energy),
        "openadr" => num(payload, "actual_kw")
            .or_else(|| num(payload, "baseload_kw"))
            .unwrap_or_else(dlms_energy),
        _ => dlms_energy(),
    };
    value.to_string()
}

async fn verify_rest_signature(
    state: &AppState,
    device_id: &str,
    payload_val: &serde_json::Value,
    signature: &str,
    kwh: &str,
    timestamp_ms: i64,
) -> anyhow::Result<bool> {
    // 1. Standard verification against device_id:kwh:timestamp_ms
    let sign_target = format!("{}:{}:{}", device_id, kwh, timestamp_ms);
    match state
        .signature_verifier
        .verify_telemetry_signature(device_id, sign_target.as_bytes(), signature)
        .await
    {
        Ok(true) => return Ok(true),
        Err(e) => return Err(e),
        _ => {}
    }

    // Fallback 1: Try with second-scale timestamp (timestamp_ms / 1000)
    let timestamp_s = timestamp_ms / 1000;
    let sign_target_s = format!("{}:{}:{}", device_id, kwh, timestamp_s);
    match state
        .signature_verifier
        .verify_telemetry_signature(device_id, sign_target_s.as_bytes(), signature)
        .await
    {
        Ok(true) => {
            info!("✅ Telemetry signature verified (REST, second-scale) for {}", device_id);
            return Ok(true);
        }
        Err(e) => return Err(e),
        _ => {}
    }

    // Fallback 2: Try directly against the serialized JSON bytes after removing signature and sorting keys alphabetically
    let mut sorted_payload = payload_val.clone();
    if let serde_json::Value::Object(ref mut map) = sorted_payload {
        map.remove("signature");
        if let Ok(raw_payload_bytes) = serde_json::to_vec(&sorted_payload) {
            match state
                .signature_verifier
                .verify_telemetry_signature(device_id, &raw_payload_bytes, signature)
                .await
            {
                Ok(true) => {
                    info!("✅ Telemetry signature verified (REST, serialized JSON) for {}", device_id);
                    return Ok(true);
                }
                Err(e) => return Err(e),
                _ => {}
            }
        }
    }

    Ok(false)
}

pub async fn ingest_private_network(
    State(state): State<AppState>,
    Json(payload): Json<PrivateNetworkPayload>,
) -> impl IntoResponse {
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

    // Resolve the protocol first. `auto` (or an empty protocol field) triggers
    // field-based detection so signature verification can use the protocol-native
    // canonical measure (OCPP `energy_wh`, SunSpec `WH`, …) rather than assuming
    // DLMS energy fields.
    let mut protocol = payload.protocol.to_lowercase();
    if protocol.is_empty() || protocol == "auto" {
        protocol = crate::protocol::stacks::detect_protocol(&payload.payload).to_string();
        info!(
            "🔎 Auto-detected protocol '{}' for {}",
            protocol, payload.device_id
        );
    }

    const SUPPORTED: [&str; 5] = ["dlms", "ocpp", "openadr", "sunspec", "simulator"];
    if !SUPPORTED.contains(&protocol.as_str()) {
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
    let is_simulator = protocol == "simulator";
    let sig_verified = if is_simulator {
        true
    } else {
        match verify_rest_signature(
            &state,
            &payload.device_id,
            &payload.payload,
            signature,
            &canonical_value,
            timestamp_ms,
        )
        .await
        {
            Ok(true) => {
                info!(
                    "✅ Telemetry signature verified (REST) for {}",
                    payload.device_id
                );
                true
            }
            Ok(false) => {
                warn!(
                    "🚫 Invalid telemetry signature (REST) for {}",
                    payload.device_id
                );
                if !signature_enforcement_disabled() {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(json!({ "error": "Invalid Ed25519 signature" })),
                    )
                        .into_response();
                }
                false
            }
            Err(e) => {
                warn!("⚠️ Verification error for {}: {}", payload.device_id, e);
                if !signature_enforcement_disabled() {
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(json!({ "error": format!("Verification failed: {}", e) })),
                    )
                        .into_response();
                }
                false
            }
        }
    };

    // Special handling for simulator/json protocol to avoid DLMS stack failure
    if protocol == "simulator" {
        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: payload.device_id.clone(),
            device_type: DeviceType::SmartMeter,
            serial_number: payload.device_id.clone(),
            zone_code: payload.payload.get("zone_code").and_then(|v| v.as_str()).map(|s| s.to_string()),
            timestamp: chrono::DateTime::parse_from_rfc3339(timestamp_str)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now()),
            metrics: crate::models::DeviceMetrics::Energy {
                generated_kwh: payload.payload.get("energy_generated").and_then(|v| v.as_f64()).unwrap_or(0.0),
                consumed_kwh: payload.payload.get("energy_consumed").and_then(|v| v.as_f64()).unwrap_or(0.0),
                net_kwh: kwh.parse::<f64>().unwrap_or(0.0),
            },
            metadata: payload.payload.as_object().cloned().unwrap_or_default().into_iter().collect(),
        };
        return disseminate_reading(&state, reading, sig_verified).await;
    }

    let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> = match protocol.as_str() {
        "dlms" => state.dlms_stack.clone(),
        "ocpp" => Arc::new(crate::protocol::stacks::ocpp::OcppStack::new()),
        "openadr" => Arc::new(crate::protocol::stacks::openadr::OpenAdrStack::new()),
        "sunspec" => Arc::new(crate::protocol::stacks::sunspec::SunSpecStack::new()),
        _ => unreachable!(),
    };

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
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "Missing readings array"}))).into_response(),
    };

    let mut responses = Vec::new();

    for item in readings {
        let device_id = item
            .get("meter_serial")
            .or_else(|| item.get("meter_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let kwh = item
            .get("kwh")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let timestamp_str = item.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());

        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: device_id.to_string(),
            zone_code: item.get("zone_code").and_then(|v| v.as_str()).map(|s| s.to_string()),
            timestamp,
            metrics: crate::models::DeviceMetrics::Energy {
                generated_kwh: item.get("energy_generated").and_then(|v| v.as_f64()).unwrap_or(kwh),
                consumed_kwh: item.get("energy_consumed").and_then(|v| v.as_f64()).unwrap_or(0.0),
                net_kwh: kwh,
            },
            metadata: item.as_object().cloned().unwrap_or_default().into_iter().collect(),
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
    info!("📥 Received private network batch ingestion: protocol={}", payload.protocol);
    let mut responses = Vec::new();
    // `auto` (or an empty top-level protocol) detects each reading's protocol
    // from its own fields, so a batch may mix device types.
    let top_protocol = payload.protocol.to_lowercase();
    const SUPPORTED: [&str; 6] = ["dlms", "ocpp", "openadr", "sunspec", "simulator", "auto"];
    if !top_protocol.is_empty() && !SUPPORTED.contains(&top_protocol.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Unsupported protocol: {}", top_protocol) })),
        )
            .into_response();
    }

    for item in payload.readings {
        // Resolve this reading's protocol (per-item detection under `auto`).
        let protocol = if top_protocol.is_empty() || top_protocol == "auto" {
            crate::protocol::stacks::detect_protocol(&item).to_string()
        } else {
            top_protocol.clone()
        };
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
        let is_verified = if protocol == "simulator" {
            true // Skip signature for simulator mode
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
                zone_code: item.get("zone_code").and_then(|v| v.as_str()).map(|s| s.to_string()),
                timestamp: chrono::DateTime::parse_from_rfc3339(timestamp_str)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now()),
                metrics: crate::models::DeviceMetrics::Energy {
                    generated_kwh: item.get("energy_generated").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    consumed_kwh: item.get("energy_consumed").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    net_kwh: kwh.parse::<f64>().unwrap_or(0.0),
                },
                metadata: item.as_object().cloned().unwrap_or_default().into_iter().collect(),
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

        let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> = match protocol.as_str() {
            "dlms" => state.dlms_stack.clone(),
            "ocpp" => Arc::new(crate::protocol::stacks::ocpp::OcppStack::new()),
            "openadr" => Arc::new(crate::protocol::stacks::openadr::OpenAdrStack::new()),
            "sunspec" => Arc::new(crate::protocol::stacks::sunspec::SunSpecStack::new()),
            _ => continue,
        };

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
    use super::canonical_sign_value;
    use serde_json::json;

    #[test]
    fn ocpp_signs_energy_wh() {
        let p = json!({"energy_wh": 15000.0, "kwh": 1.0});
        assert_eq!(canonical_sign_value("ocpp", &p), "15000");
    }

    #[test]
    fn sunspec_signs_wh_not_watts() {
        let p = json!({"W": 5000.0, "WH": 12345.0, "St": 4});
        assert_eq!(canonical_sign_value("sunspec", &p), "12345");
    }

    #[test]
    fn openadr_signs_actual_kw() {
        let p = json!({"baseload_kw": 3.0, "actual_kw": 2.4});
        assert_eq!(canonical_sign_value("openadr", &p), "2.4");
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
    fn unknown_protocol_falls_back_to_dlms_energy() {
        let p = json!({"energy_generated": 3.0});
        assert_eq!(canonical_sign_value("mystery", &p), "3");
    }

    #[test]
    fn ocpp_without_energy_wh_falls_back_to_dlms_energy() {
        let p = json!({"energy_consumed": 2.0});
        assert_eq!(canonical_sign_value("ocpp", &p), "2");
    }

    #[test]
    fn parses_stringified_numbers() {
        let p = json!({"energy_wh": "15000"});
        assert_eq!(canonical_sign_value("ocpp", &p), "15000");
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
