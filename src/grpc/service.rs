use connectrpc::{Context, ConnectError};
use buffa::view::OwnedView;
use tracing::{info, error, warn};
use std::sync::Arc;
use uuid::Uuid;
use chrono::{Utc, TimeZone};
use std::collections::HashMap;

use crate::models::{DeviceReading, DeviceType, DeviceMetrics};
use crate::state::AppState;
use crate::handlers::disseminate_reading;
use crate::protocol::DlmsBinaryFrame;
use serde_json::json;

/// Generated code from proto using industrial standard (Buffa / ConnectRPC)
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/_oracle_include.rs"));
    pub use gridtokenx::oracle::v1::*;
}

use proto::{
    OracleService, 
    TelemetryRequestView, 
    TelemetryBatchRequestView,
    TelemetryResponse, 
    TelemetryBatchResponse,
};

/// Implementation of the Oracle gRPC Service (Industrial Standard)
pub struct OracleServiceImpl {
    pub state: AppState,
}

impl OracleServiceImpl {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Register the service with the ConnectRPC router using the generated extension trait.
    /// Renamed to avoid collision with the extension trait's method.
    pub fn register_service(self: Arc<Self>, router: connectrpc::Router) -> connectrpc::Router {
        use proto::OracleServiceExt;
        self.register(router)
    }
}

// Global Standard: Native async methods (as seen in IAM service)
impl OracleService for OracleServiceImpl {
    /// Path A: High-frequency telemetry ingestion (VPP operations)
    async fn submit_telemetry(
        &self,
        ctx: Context,
        request: OwnedView<TelemetryRequestView<'static>>,
    ) -> Result<(TelemetryResponse, Context), ConnectError> {
        info!("📡 Unified B2C/B2B Industrial Ingestion (IEC 62056): meter={}", request.meter_id);
        
        // --- Signature Verification ---
        if let Some(signature) = request.signature.as_deref() {
            // Reconstruct canonical signed payload: meter_id:kwh:timestamp
            let sign_target = format!("{}:{}:{}", request.meter_id, request.kwh, request.timestamp);
            match self.state.signature_verifier.verify_telemetry_signature(
                &request.meter_id,
                sign_target.as_bytes(),
                signature
            ).await {
                Ok(true) => info!("✅ Telemetry signature verified for {}", request.meter_id),
                Ok(false) => {
                    error!("🚫 Invalid telemetry signature for {}", request.meter_id);
                    return Err(ConnectError::permission_denied("Invalid telemetry signature"));
                }
                Err(e) => {
                    error!("⚠️ Verification error for {}: {}", request.meter_id, e);
                    // In production, we'd fail here. For mixed-mode dev, we might log and continue.
                    if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                        return Err(ConnectError::unauthenticated(format!("Verification failed: {}", e)));
                    }
                }
            }
        } else {
            tracing::warn!("⚠️ Received unsigned telemetry from meter={}", request.meter_id);
            if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                return Err(ConnectError::invalid_argument("Signature required in production"));
            }
        }

        let mut generated_kwh = request.energy_generated.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let mut consumed_kwh = request.energy_consumed.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let mut timestamp = Utc.timestamp_opt(request.timestamp, 0).single().unwrap_or_else(Utc::now);
        let mut metadata = HashMap::new();

        if !request.raw_payload.is_empty() {
            info!("📦 Received raw DLMS/binary payload ({} bytes)", request.raw_payload.len());
            match DlmsBinaryFrame::parse(&request.raw_payload) {
                Ok(frame) => {
                    info!("✅ Successfully decoded DLMS payload: {:?}", frame);
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
                        metadata.insert("battery_level_pct".to_string(), json!((bps as f64) / 100.0));
                    }
                    metadata.insert("dlms_manufacturer_id".to_string(), json!(frame.manufacturer_id));
                    metadata.insert("dlms_logical_device_name".to_string(), json!(frame.logical_device_name));
                }
                Err(e) => {
                    tracing::warn!("⚠️ Failed to parse raw_payload as DLMS block: {}. Falling back to standard gRPC fields.", e);
                }
            }
        }

        // Standardized energy mapping for DLMS/COSEM (IEC 62056) compliant telemetry
        let reading = DeviceReading {
            reading_id: request.reading_id.parse().unwrap_or_else(|_| Uuid::new_v4()),
            device_id: request.meter_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: request.meter_serial.to_string(),
            zone_id: request.zone_id,
            timestamp,
            metrics: DeviceMetrics::Energy {
                generated_kwh,
                consumed_kwh,
                net_kwh: generated_kwh - consumed_kwh,
            },
            metadata,
        };

        // Reuse unified dissemination engine for Path A
        let res = disseminate_reading(&self.state, reading).await;
        
        if res.status().is_success() {
            Ok((TelemetryResponse {
                receipt_id: Uuid::new_v4().to_string(),
                status: "accepted".to_string(),
                ..Default::default()
            }, ctx))
        } else {
            error!("❌ Telemetry dissemination failed for meter={}", request.meter_id);
            Err(ConnectError::internal("Platform dissemination failed"))
        }
    }

    /// Path A Batch: Optimized ingestion for professional aggregators
    async fn submit_telemetry_batch(
        &self,
        ctx: Context,
        request: OwnedView<TelemetryBatchRequestView<'static>>,
    ) -> Result<(TelemetryBatchResponse, Context), ConnectError> {
        let mut receipt_ids = Vec::new();
        let mut accepted_count = 0;
        let mut rejected_count = 0;

        for tel in &request.readings {
            // --- Signature Verification for Batch Item ---
            if let Some(signature) = tel.signature.as_deref() {
                let sign_target = format!("{}:{}:{}", tel.meter_id, tel.kwh, tel.timestamp);
                let is_verified = match self.state.signature_verifier.verify_telemetry_signature(
                    &tel.meter_id,
                    sign_target.as_bytes(),
                    signature
                ).await {
                    Ok(true) => true,
                    _ => false,
                };

                if !is_verified {
                    warn!("🚫 Invalid signature in batch for meter={}", tel.meter_id);
                    rejected_count += 1;
                    continue; // Skip this reading
                }
            } else if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                warn!("⚠️ Missing signature in batch for meter={}", tel.meter_id);
                rejected_count += 1;
                continue;
            }

            let mut generated_kwh = tel.energy_generated.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let mut consumed_kwh = tel.energy_consumed.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let mut timestamp = Utc.timestamp_opt(tel.timestamp, 0).single().unwrap_or_else(Utc::now);
            let mut metadata = HashMap::new();

            if !tel.raw_payload.is_empty() {
                info!("📦 Received batched raw DLMS payload ({} bytes)", tel.raw_payload.len());
                match DlmsBinaryFrame::parse(&tel.raw_payload) {
                    Ok(frame) => {
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
                            metadata.insert("battery_level_pct".to_string(), json!((bps as f64) / 100.0));
                        }
                        metadata.insert("dlms_manufacturer_id".to_string(), json!(frame.manufacturer_id));
                        metadata.insert("dlms_logical_device_name".to_string(), json!(frame.logical_device_name));
                    }
                    Err(e) => {
                        tracing::warn!("⚠️ Failed to parse raw_payload as DLMS block: {}. Falling back to standard fields.", e);
                    }
                }
            }

            let reading = DeviceReading {
                reading_id: tel.reading_id.parse().unwrap_or_else(|_| Uuid::new_v4()),
                device_id: tel.meter_id.to_string(),
                device_type: DeviceType::SmartMeter,
                serial_number: tel.meter_serial.to_string(),
                zone_id: tel.zone_id,
                timestamp,
                metrics: DeviceMetrics::Energy {
                    generated_kwh,
                    consumed_kwh,
                    net_kwh: generated_kwh - consumed_kwh,
                },
                metadata,
            };

            let res = disseminate_reading(&self.state, reading).await;
            if res.status().is_success() {
                receipt_ids.push(Uuid::new_v4().to_string());
                accepted_count += 1;
            } else {
                rejected_count += 1;
            }
        }

        Ok((TelemetryBatchResponse {
            receipt_ids,
            status: if rejected_count == 0 { "all_accepted" } else { "partially_accepted" }.to_string(),
            accepted_count,
            rejected_count,
            ..Default::default()
        }, ctx))
    }

    /// Federated Learning: Receive locally trained model gradients
    async fn push_gradients(
        &self,
        ctx: Context,
        request: OwnedView<proto::PushGradientsRequestView<'static>>,
    ) -> Result<(proto::PushGradientsResponse, Context), ConnectError> {
        info!("📥 FL: Received NILM gradients from meter: {} (base_version: {})", 
              request.meter_id, request.base_version);
        
        let mut layers = HashMap::new();
        for (layer_name, layer_data) in &request.layers {
            layers.insert(layer_name.to_string(), layer_data.values.to_vec());
        }

        let domain_update = crate::nilm::federated::PushGradientsRequest {
            meter_id: request.meter_id.to_string(),
            base_version: request.base_version.to_string(),
            layers,
            sample_count: request.sample_count as usize,
        };

        let mut aggregator = self.state.global_aggregator.lock().await;
        let accepted = aggregator.add_update(domain_update).map_err(|e| {
            error!("❌ FL Aggregation failed: {}", e);
            ConnectError::internal("Aggregation failed")
        })?;

        if accepted {
            if let Err(e) = aggregator.save_to_file(&self.state.model_state_path) {
                error!("❌ FL Persistence failed: {}", e);
            }
        }

        Ok((proto::PushGradientsResponse {
            status: if accepted { "aggregated" } else { "queued" }.to_string(),
            accepted,
            ..Default::default()
        }, ctx))
    }

    /// Federated Learning: provide latest global model weights
    async fn pull_global_model(
        &self,
        ctx: Context,
        request: OwnedView<proto::PullGlobalModelRequestView<'static>>,
    ) -> Result<(proto::PullGlobalModelResponse, Context), ConnectError> {
        let aggregator = self.state.global_aggregator.lock().await;
        
        info!("📤 FL: Serving Global Model version {} to meter: {}", 
              aggregator.current_version, request.meter_id);

        // In production, this would load the actual .tflite / .rknn binary 
        // from storage based on the aggregated weights.
        Ok((proto::PullGlobalModelResponse {
            version: aggregator.current_version.clone(),
            model_payload: vec![0, 0, 0, 0, b'T', b'F', b'L', b'3'], // Placeholder
            model_type: "MOE_1D_CNN_INT8".to_string(),
            timestamp: Utc::now().timestamp(),
            ..Default::default()
        }, ctx))
    }
}

