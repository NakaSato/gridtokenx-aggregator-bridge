use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use serde_json::json;
use tracing::{error, info, warn};
use crate::models::{DeviceReading, PrivateNetworkPayload, IngestResponse, DeviceType};
use crate::state::AppState;

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

    // --- Neural Edge Intelligence (NILM) Hook ---
    if reading.device_type == DeviceType::SmartMeter {
        let nilm_engine = state.nilm_engine.clone();
        let accumulator = state.gradient_accumulator.clone();
        let meter_id = reading.device_id.clone();
        
        // Try to determine the active power for disaggregation
        let power_w = reading.metadata.get("total_active_power_w")
            .and_then(|v| v.as_f64())
            .or_else(|| {
                // Heuristic fallback: V * I
                let v = reading.metadata.get("voltage_v").and_then(|v| v.as_f64())?;
                let i = reading.metadata.get("current_a").and_then(|i| i.as_f64())?;
                Some(v * i)
            });

        if let Some(w) = power_w {
            tokio::spawn(async move {
                match nilm_engine.disaggregate(&meter_id, w).await {
                    Ok(result) => {
                        info!("⚡ NILM Disaggregation: meter={}, experts={:?}, appliances={:?}", 
                            meter_id, 
                            result.flexibility_scores.iter().map(|s| &s.app_name).collect::<Vec<_>>(),
                            result.appliances
                        );
                        
                        // Simulate local gradient updates for Federated Learning
                        let mut acc = accumulator.lock().await;
                        acc.accumulate("sparse_moe_output", &[0.01, 0.05, 0.02, 0.08]);
                    }
                    Err(e) => warn!("⚠️ NILM Engine error for {}: {}", meter_id, e),
                }
            });
        }
    }

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
