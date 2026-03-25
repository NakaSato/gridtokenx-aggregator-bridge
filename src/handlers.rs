use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use serde_json::json;
use tracing::{error, info, warn};

use crate::models::*;
use crate::protocol::{DeviceProtocol, RawPayload};
use crate::state::AppState;
use crate::protocol::battery::BatteryAdapter;
use crate::protocol::stacks::ocpp::OcppStack;

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
// Smart Meter Ingestion
// =============================================================================

pub async fn ingest_smart_meter(
    State(state): State<AppState>,
    Json(payload): Json<SmartMeterPayload>,
) -> impl IntoResponse {
    info!("📡 Smart meter reading from: {}", payload.device_id);

    if payload.device_id.is_empty() {
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
        Ok(reading) => disseminate_reading(&state, reading).await,
        Err(e) => {
            warn!("⚠️ Failed to parse smart meter payload: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid smart meter payload: {}", e) })),
            )
                .into_response()
        }
    }
}

// =============================================================================
// EV Charger Ingestion
// =============================================================================

pub async fn ingest_ev_charger(
    State(state): State<AppState>,
    Json(payload): Json<EvChargerPayload>,
) -> impl IntoResponse {
    info!("🔌 EV charger reading from: {}", payload.device_id);

    if payload.device_id.is_empty() || payload.session_id.is_empty() {
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
        Ok(reading) => disseminate_reading(&state, reading).await,
        Err(e) => {
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
    info!("🔋 Battery reading from: {}", payload.device_id);

    if payload.device_id.is_empty() {
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
        Ok(reading) => disseminate_reading(&state, reading).await,
        Err(e) => {
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
        Ok(reading) => disseminate_reading(&state, reading).await,
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
