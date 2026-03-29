use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use serde_json::json;
use tracing::{error, info, warn};
use std::time::Instant;

use crate::models::{DeviceReading, DeviceType, SmartMeterPayload, BatchSmartMeterPayload, EvChargerPayload, BatteryPayload, GenericIngestPayload, PrivateNetworkPayload, IngestResponse};
use crate::protocol::{DeviceProtocol, RawPayload};
use crate::state::AppState;
use crate::metrics;

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

pub async fn get_metrics(
    State(state): State<AppState>,
) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    let m = &state.metrics;
    
    let total = m.total_requests.load(Ordering::Relaxed);
    let avg_latency = if total > 0 {
        m.total_grpc_latency_us.load(Ordering::Relaxed) as f64 / total as f64
    } else {
        0.0
    };

    Json(json!({
        "total_requests": total,
        "authorized_requests": m.authorized_requests.load(Ordering::Relaxed),
        "failed_requests": m.failed_requests.load(Ordering::Relaxed),
        "on_chain_syncs": m.on_chain_syncs.load(Ordering::Relaxed),
        "last_grpc_latency_us": m.last_grpc_latency_us.load(Ordering::Relaxed),
        "avg_grpc_latency_us": avg_latency,
    }))
}

// =============================================================================
// Smart Meter Ingestion
// =============================================================================

pub async fn ingest_smart_meter(
    State(state): State<AppState>,
    Json(payload): Json<SmartMeterPayload>,
) -> impl IntoResponse {
    let start = Instant::now();
    info!("📡 Smart meter reading from: {}", payload.device_id);

    if payload.device_id.is_empty() {
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::record_ingestion_request("smart_meter", false, duration_ms);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "device_id is required" })),
        ).into_response();
    }

    let raw = RawPayload {
        device_type: DeviceType::SmartMeter,
        body: serde_json::to_value(&payload).unwrap_or_default(),
    };

    match state.smart_meter_adapter.parse(&raw) {
        Ok(reading) => {
            let result = disseminate_reading(&state, reading).await;
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            let success = result.status().is_success();
            metrics::record_ingestion_request("smart_meter", success, duration_ms);
            if success {
                metrics::record_meter_reading(true, duration_ms);
            }
            result
        },
        Err(e) => {
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_ingestion_request("smart_meter", false, duration_ms);
            warn!("⚠️ Failed to parse smart meter payload: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid smart meter payload: {}", e) })),
            )
                .into_response()
        }
    }
}

pub async fn ingest_batch_smart_meter(
    State(state): State<AppState>,
    Json(payload): Json<BatchSmartMeterPayload>,
) -> impl IntoResponse {
    info!("📡 Batch smart meter ingestion: {} readings", payload.readings.len());

    let mut results = Vec::new();
    for item in payload.readings {
        let raw = RawPayload {
            device_type: DeviceType::SmartMeter,
            body: serde_json::to_value(&item).unwrap_or_default(),
        };

        match state.smart_meter_adapter.parse(&raw) {
            Ok(reading) => {
                // For simplicity in batch, we disseminate but don't stop on single error
                // In production, we'd probably want a more robust partial success response
                let _ = state.router.disseminate(&reading).await;
                results.push(reading.reading_id);
            }
            Err(e) => {
                warn!("⚠️ Failed to parse batch item: {}", e);
            }
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "batch_accepted",
            "count": results.len(),
            "reading_ids": results
        })),
    )
}

// =============================================================================
// EV Charger Ingestion
// =============================================================================

pub async fn ingest_ev_charger(
    State(state): State<AppState>,
    Json(payload): Json<EvChargerPayload>,
) -> impl IntoResponse {
    let start = Instant::now();
    info!("🔌 EV charger reading from: {}", payload.device_id);

    if payload.device_id.is_empty() || payload.session_id.is_empty() {
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::record_ingestion_request("ev_charger", false, duration_ms);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "device_id and session_id are required" })),
        ).into_response();
    }

    let raw = RawPayload {
        device_type: DeviceType::EvCharger,
        body: serde_json::to_value(&payload).unwrap_or_default(),
    };

    match state.ev_charger_adapter.parse(&raw) {
        Ok(reading) => {
            let result = disseminate_reading(&state, reading).await;
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            let success = result.status().is_success();
            metrics::record_ingestion_request("ev_charger", success, duration_ms);
            if success {
                metrics::record_ev_charger_data(true, duration_ms);
            }
            result
        },
        Err(e) => {
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_ingestion_request("ev_charger", false, duration_ms);
            warn!("⚠️ Failed to parse EV charger payload: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid EV charger payload: {}", e) })),
            )
                .into_response()
        }
    }
}

// =============================================================================
// Battery Ingestion
// =============================================================================

pub async fn ingest_battery(
    State(state): State<AppState>,
    Json(payload): Json<BatteryPayload>,
) -> impl IntoResponse {
    let start = Instant::now();
    info!("🔋 Battery reading from: {}", payload.device_id);

    if payload.device_id.is_empty() {
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::record_ingestion_request("battery", false, duration_ms);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "device_id is required" })),
        ).into_response();
    }

    let raw = RawPayload {
        device_type: DeviceType::Battery,
        body: serde_json::to_value(&payload).unwrap_or_default(),
    };

    match state.battery_adapter.parse(&raw) {
        Ok(reading) => {
            let result = disseminate_reading(&state, reading).await;
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            let success = result.status().is_success();
            metrics::record_ingestion_request("battery", success, duration_ms);
            if success {
                metrics::record_battery_data(true, duration_ms);
            }
            result
        },
        Err(e) => {
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_ingestion_request("battery", false, duration_ms);
            warn!("⚠️ Failed to parse battery payload: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid battery payload: {}", e) })),
            )
                .into_response()
        }
    }
}

// =============================================================================
// Auto-detect Ingestion
// =============================================================================

pub async fn ingest_auto(
    State(state): State<AppState>,
    Json(payload): Json<GenericIngestPayload>,
) -> impl IntoResponse {
    info!("🔄 Auto-detect ingestion: {:?}", payload.device_type);

    let raw = RawPayload {
        device_type: payload.device_type,
        body: payload.data,
    };

    let parse_result = match raw.device_type {
        DeviceType::SmartMeter => state.smart_meter_adapter.parse(&raw),
        DeviceType::EvCharger => state.ev_charger_adapter.parse(&raw),
        DeviceType::Battery => state.battery_adapter.parse(&raw),
    };

    match parse_result {
        Ok(reading) => {
            let start = Instant::now();
            let result = disseminate_reading(&state, reading).await;
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            let success = result.status().is_success();
            metrics::record_ingestion_request("auto", success, duration_ms);
            result
        },
        Err(e) => {
            warn!("⚠️ Failed to parse {:?} payload: {}", raw.device_type, e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid payload: {}", e) })),
            )
                .into_response()
        }
    }
}

// =============================================================================
// Private Network Ingestion (OCPP, SunSpec, DLMS, OpenADR)
// =============================================================================

pub async fn ingest_private_network(
    State(state): State<AppState>,
    Json(payload): Json<PrivateNetworkPayload>,
) -> impl IntoResponse {
    info!("🔗 Private Network ingestion: protocol={}, device={}", payload.protocol, payload.device_id);

    // Dynamic routing to specialized protocol stacks
    let stack: Arc<dyn crate::protocol::stacks::ProtocolStack> = match payload.protocol.to_lowercase().as_str() {
        "ocpp" => state.ocpp_stack.clone(),
        "sunspec" => state.sunspec_stack.clone(),
        "dlms" => state.dlms_stack.clone(),
        "openadr" => state.openadr_stack.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Unsupported protocol: {}", payload.protocol) })),
            ).into_response();
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

// =============================================================================
// Shared Dissemination
// =============================================================================

async fn disseminate_reading(
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
        Ok(_) => (StatusCode::ACCEPTED, Json(json!(response))).into_response(),
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
