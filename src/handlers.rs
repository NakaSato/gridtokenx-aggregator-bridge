use crate::models::{
    BatchPrivateNetworkPayload, DeviceReading, DeviceType, IngestResponse, PrivateNetworkPayload,
};
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info, warn};

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

    match verify_rest_signature(
        &state,
        &payload.device_id,
        &payload.payload,
        signature,
        &kwh,
        timestamp_ms,
    )
    .await
    {
        Ok(true) => info!(
            "✅ Telemetry signature verified (REST) for {}",
            payload.device_id
        ),
        Ok(false) => {
            warn!(
                "🚫 Invalid telemetry signature (REST) for {}",
                payload.device_id
            );
            if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "Invalid Ed25519 signature" })),
                )
                    .into_response();
            }
        }
        Err(e) => {
            warn!("⚠️ Verification error for {}: {}", payload.device_id, e);
            if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "error": format!("Verification failed: {}", e) })),
                )
                    .into_response();
            }
        }
    }

    // Dynamic routing to specialized protocol stacks
    let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> =
        match payload.protocol.to_lowercase().as_str() {
            "ocpp" => state.ocpp_stack.clone(),
            "sunspec" => state.sunspec_stack.clone(),
            "dlms" => state.dlms_stack.clone(),
            "openadr" => state.openadr_stack.clone(),
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("Unsupported protocol: {}", payload.protocol) })),
                )
                    .into_response();
            }
        };

    // Serialize payload back to bytes for the stack handlers
    let raw_data = serde_json::to_vec(&payload.payload).unwrap_or_default();

    match stack.handle_message(&payload.device_id, &raw_data).await {
        Ok(Some(reading)) => disseminate_reading(&state, reading).await,
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

pub async fn ingest_private_network_batch(
    State(state): State<AppState>,
    Json(payload): Json<BatchPrivateNetworkPayload>,
) -> impl IntoResponse {
    let mut responses = Vec::new();
    let protocol = payload.protocol.to_lowercase();

    // Dynamic routing to specialized protocol stacks
    let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> = match protocol.as_str() {
        "ocpp" => state.ocpp_stack.clone(),
        "sunspec" => state.sunspec_stack.clone(),
        "dlms" => state.dlms_stack.clone(),
        "openadr" => state.openadr_stack.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Unsupported protocol: {}", payload.protocol) })),
            )
                .into_response();
        }
    };

    for item in payload.readings {
        let device_id = item
            .get("device_id")
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

        let is_verified = match verify_rest_signature(
            &state,
            device_id,
            &item,
            signature,
            &kwh,
            timestamp_ms,
        )
        .await
        {
            Ok(true) => true,
            _ => false,
        };

        if !is_verified && std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
            warn!("🚫 Invalid signature in batch for device {}", device_id);
            continue; // Skip invalid reading
        }

        // Serialize payload back to bytes for the stack handlers
        let raw_data = serde_json::to_vec(&item).unwrap_or_default();

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

pub async fn disseminate_reading(
    state: &AppState,
    reading: DeviceReading,
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
                        verified: true,
                        confidence_score: 1.0,
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
