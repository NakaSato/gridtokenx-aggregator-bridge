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

/// Generated code from proto using industrial standard (Buffa / ConnectRPC)
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/_oracle_include.rs"));
    pub use gridtokenx::oracle::v1::*;
}

use proto::{
    OracleService, TelemetryBatchRequestView, TelemetryBatchResponse, TelemetryRequestView,
    TelemetryResponse,
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
        info!(
            "📡 Unified B2C/B2B Industrial Ingestion (IEC 62056): meter={}",
            request.meter_id
        );

        // --- Signature Verification ---
        if let Some(signature) = request.signature.as_deref() {
            // Reconstruct canonical signed payload: meter_id:kwh:timestamp
            let sign_target = format!("{}:{}:{}", request.meter_id, request.kwh, request.timestamp);
            let mut is_verified = match self
                .state
                .signature_verifier
                .verify_telemetry_signature(&request.meter_id, sign_target.as_bytes(), signature)
                .await
            {
                Ok(true) => true,
                _ => false,
            };

            // Fallback: Verify against raw binary payload directly (authentic devices)
            if !is_verified && !request.raw_payload.is_empty() {
                is_verified = match self
                    .state
                    .signature_verifier
                    .verify_telemetry_signature(&request.meter_id, &request.raw_payload, signature)
                    .await
                {
                    Ok(true) => {
                        info!("✅ Telemetry signature verified against raw binary payload for {}", request.meter_id);
                        true
                    }
                    _ => false,
                };
            }

            if !is_verified {
                error!("🚫 Invalid telemetry signature for {}", request.meter_id);
                return Err(ConnectError::permission_denied(
                    "Invalid telemetry signature",
                ));
            }
        } else {
            tracing::warn!(
                "⚠️ Received unsigned telemetry from meter={}",
                request.meter_id
            );
            if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                return Err(ConnectError::invalid_argument(
                    "Signature required in production",
                ));
            }
        }

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
            .unwrap_or_else(Utc::now);
        let mut metadata = HashMap::new();

        if !request.raw_payload.is_empty() {
            info!(
                "📦 Received raw DLMS/binary payload ({} bytes)",
                request.raw_payload.len()
            );
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
                        metadata
                            .insert("battery_level_pct".to_string(), json!((bps as f64) / 100.0));
                    }
                    metadata.insert(
                        "dlms_manufacturer_id".to_string(),
                        json!(frame.manufacturer_id),
                    );
                    metadata.insert(
                        "dlms_logical_device_name".to_string(),
                        json!(frame.logical_device_name),
                    );
                }
                Err(e) => {
                    tracing::warn!("⚠️ Failed to parse raw_payload as DLMS block: {}. Falling back to standard gRPC fields.", e);
                }
            }
        }

        // Standardized energy mapping for DLMS/COSEM (IEC 62056) compliant telemetry
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

        // Reuse unified dissemination engine for Path A
        let res = disseminate_reading(&self.state, reading).await;

        if res.status().is_success() {
            Ok((
                TelemetryResponse {
                    receipt_id: Uuid::new_v4().to_string(),
                    status: "accepted".to_string(),
                    ..Default::default()
                },
                ctx,
            ))
        } else {
            error!(
                "❌ Telemetry dissemination failed for meter={}",
                request.meter_id
            );
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
                let mut is_verified = match self
                    .state
                    .signature_verifier
                    .verify_telemetry_signature(&tel.meter_id, sign_target.as_bytes(), signature)
                    .await
                {
                    Ok(true) => true,
                    _ => false,
                };

                // Fallback: Verify against raw binary payload directly (authentic devices)
                if !is_verified && !tel.raw_payload.is_empty() {
                    is_verified = match self
                        .state
                        .signature_verifier
                        .verify_telemetry_signature(&tel.meter_id, &tel.raw_payload, signature)
                        .await
                    {
                        Ok(true) => {
                            info!("✅ Telemetry signature verified against raw binary payload for batch item {}", tel.meter_id);
                            true
                        }
                        _ => false,
                    };
                }

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
                .unwrap_or_else(Utc::now);
            let mut metadata = HashMap::new();

            if !tel.raw_payload.is_empty() {
                info!(
                    "📦 Received batched raw DLMS payload ({} bytes)",
                    tel.raw_payload.len()
                );
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
                            metadata.insert(
                                "battery_level_pct".to_string(),
                                json!((bps as f64) / 100.0),
                            );
                        }
                        metadata.insert(
                            "dlms_manufacturer_id".to_string(),
                            json!(frame.manufacturer_id),
                        );
                        metadata.insert(
                            "dlms_logical_device_name".to_string(),
                            json!(frame.logical_device_name),
                        );
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
                zone_code: tel.zone_code.as_deref().map(|s| s.to_string()),
                timestamp,
                metrics: DeviceMetrics::Energy {
                    generated_kwh,
                    consumed_kwh,
                    net_kwh: tel.kwh.parse().unwrap_or(generated_kwh - consumed_kwh),
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

        Ok((
            TelemetryBatchResponse {
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

    /// Path B: Settlement Attestations
    async fn submit_attestation(
        &self,
        ctx: Context,
        request: OwnedView<proto::AttestationRequestView<'static>>,
    ) -> Result<(proto::AttestationResponse, Context), ConnectError> {
        info!(
            "🔐 Path B: Received Settlement Attestation for meter={}",
            request.meter_id
        );

        // --- Signature Verification for Path B ---
        let signature = request.signature.as_ref();
        // The exact payload format to sign should be defined by the architecture.
        // Assuming `{meter_id}:{total_kwh}:{start_time}:{end_time}` for attestations.
        let sign_target = format!(
            "{}:{}:{}:{}",
            request.meter_id, request.total_kwh, request.start_time, request.end_time
        );

        match self
            .state
            .signature_verifier
            .verify_telemetry_signature(&request.meter_id, sign_target.as_bytes(), signature)
            .await
        {
            Ok(true) => info!("✅ Attestation signature verified for {}", request.meter_id),
            Ok(false) => {
                error!("🚫 Invalid attestation signature for {}", request.meter_id);
                return Err(ConnectError::permission_denied(
                    "Invalid attestation signature",
                ));
            }
            Err(e) => {
                error!(
                    "⚠️ Attestation verification error for {}: {}",
                    request.meter_id, e
                );
                if std::env::var("ENVIRONMENT").unwrap_or_default() == "production" {
                    return Err(ConnectError::unauthenticated(format!(
                        "Verification failed: {}",
                        e
                    )));
                }
            }
        }

        // Placeholder for ZK-Rollup Accumulation logic (Path B)
        info!("📦 Attestation added to ZK Batch [{}]", request.batch_id);

        Ok((
            proto::AttestationResponse {
                batch_id: request.batch_id.to_string(),
                status: "queued_for_zk_proof".to_string(),
                verifier_tx_id: Uuid::new_v4().to_string(), // Placeholder tx id
                ..Default::default()
            },
            ctx,
        ))
    }

    /// Path B Batch: Settlement Attestations
    async fn submit_attestation_batch(
        &self,
        ctx: Context,
        request: OwnedView<proto::AttestationBatchRequestView<'static>>,
    ) -> Result<(proto::AttestationBatchResponse, Context), ConnectError> {
        info!(
            "🔐 Path B: Received Batch Settlement Attestations (count={})",
            request.attestations.len()
        );

        let mut accepted_count = 0;
        let mut rejected_count = 0;

        for att in &request.attestations {
            let signature = att.signature.as_ref();
            let sign_target = format!(
                "{}:{}:{}:{}",
                att.meter_id, att.total_kwh, att.start_time, att.end_time
            );

            let is_verified = match self
                .state
                .signature_verifier
                .verify_telemetry_signature(&att.meter_id, sign_target.as_bytes(), signature)
                .await
            {
                Ok(true) => true,
                _ => false,
            };

            if is_verified {
                accepted_count += 1;
            } else {
                warn!(
                    "🚫 Invalid attestation signature in batch for meter={}",
                    att.meter_id
                );
                rejected_count += 1;
            }
        }

        Ok((
            proto::AttestationBatchResponse {
                status: if rejected_count == 0 {
                    "all_queued"
                } else {
                    "partially_queued"
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
