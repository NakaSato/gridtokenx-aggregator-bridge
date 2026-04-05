//! Oracle Bridge metrics instrumentation
//!
//! This module provides metrics for:
//! - IoT ingestion endpoints
//! - Redis stream processing
//! - Oracle/bridge operations
//! - Device connectivity

use metrics::{counter, gauge, histogram};
use std::time::Instant;

/// Records IoT ingestion request metrics
#[allow(dead_code)]
pub fn record_ingestion_request(
    device_type: &str,
    success: bool,
    duration_ms: f64,
) {
    counter!("oracle_ingestion_requests_total",
        "device_type" => device_type.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_ingestion_request_duration_ms",
        "device_type" => device_type.to_string()
    ).record(duration_ms);
}

/// Records meter reading ingestion metrics
pub fn record_meter_reading(success: bool, duration_ms: f64) {
    counter!("oracle_meter_readings_total",
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_meter_reading_duration_ms").record(duration_ms);

    if !success {
        counter!("oracle_meter_reading_failures_total").increment(1);
    }
}

/// Records EV charger data ingestion metrics
pub fn record_ev_charger_data(success: bool, duration_ms: f64) {
    counter!("oracle_ev_charger_data_total",
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_ev_charger_data_duration_ms").record(duration_ms);
}

/// Records battery data ingestion metrics
pub fn record_battery_data(success: bool, duration_ms: f64) {
    counter!("oracle_battery_data_total",
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_battery_data_duration_ms").record(duration_ms);
}

/// Records Redis stream processing metrics
#[allow(dead_code)]
pub fn record_redis_stream_processing(
    stream_name: &str,
    messages_processed: u64,
    success: bool,
    duration_ms: f64,
) {
    counter!("oracle_redis_stream_processed_total",
        "stream" => stream_name.to_string(),
        "success" => success.to_string()
    ).increment(messages_processed);

    histogram!("oracle_redis_stream_processing_duration_ms",
        "stream" => stream_name.to_string()
    ).record(duration_ms);

    if !success {
        counter!("oracle_redis_stream_processing_failures_total",
            "stream" => stream_name.to_string()
        ).increment(1);
    }
}

/// Records Oracle forwarding to API Gateway
pub fn record_oracle_forwarding(success: bool, duration_ms: f64) {
    counter!("oracle_forwarding_total",
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_forwarding_duration_ms").record(duration_ms);

    if !success {
        counter!("oracle_forwarding_failures_total").increment(1);
    }
}

/// Records blockchain submission metrics
#[allow(dead_code)]
pub fn record_blockchain_submission(
    operation: &str,
    success: bool,
    duration_ms: f64,
) {
    counter!("oracle_blockchain_submissions_total",
        "operation" => operation.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_blockchain_submission_duration_ms",
        "operation" => operation.to_string()
    ).record(duration_ms);

    if !success {
        counter!("oracle_blockchain_submission_failures_total",
            "operation" => operation.to_string()
        ).increment(1);
    }
}

/// Records device connectivity metrics
#[allow(dead_code)]
pub fn record_device_connection(device_type: &str, connected: bool) {
    if connected {
        counter!("oracle_device_connections_total",
            "device_type" => device_type.to_string()
        ).increment(1);
    } else {
        counter!("oracle_device_disconnections_total",
            "device_type" => device_type.to_string()
        ).increment(1);
    }
}

/// Records active device count
#[allow(dead_code)]
pub fn record_active_devices(device_type: &str, count: u64) {
    gauge!("oracle_active_devices", "device_type" => device_type.to_string()).set(count as f64);
}

/// Records data validation metrics
#[allow(dead_code)]
pub fn record_data_validation(valid: bool, reason: &str) {
    counter!("oracle_data_validations_total",
        "valid" => valid.to_string(),
        "reason" => reason.to_string()
    ).increment(1);

    if !valid {
        counter!("oracle_data_validation_failures_total",
            "reason" => reason.to_string()
        ).increment(1);
    }
}

/// Records protocol adapter metrics
#[allow(dead_code)]
pub fn record_protocol_adapter(
    protocol: &str,
    operation: &str,
    success: bool,
    duration_ms: f64,
) {
    counter!("oracle_protocol_adapter_operations_total",
        "protocol" => protocol.to_string(),
        "operation" => operation.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_protocol_adapter_operation_duration_ms",
        "protocol" => protocol.to_string(),
        "operation" => operation.to_string()
    ).record(duration_ms);
}

/// Records gRPC client metrics for IAM
#[allow(dead_code)]
pub fn record_grpc_client_call(service: &str, method: &str, success: bool, duration_ms: f64) {
    counter!("oracle_grpc_client_calls_total",
        "service" => service.to_string(),
        "method" => method.to_string(),
        "success" => success.to_string()
    ).increment(1);

    histogram!("oracle_grpc_client_call_duration_ms",
        "service" => service.to_string(),
        "method" => method.to_string()
    ).record(duration_ms);
}

/// Records HTTP request metrics for Oracle Bridge
#[allow(dead_code)]
pub struct HttpMetricsTimer {
    start: Instant,
    method: String,
    path: String,
}

#[allow(dead_code)]
impl HttpMetricsTimer {
    pub fn new(method: &str, path: &str) -> Self {
        let start = Instant::now();
        gauge!("oracle_http_requests_in_flight",
            "method" => method.to_string(),
            "path" => path.to_string()
        ).increment(1.0);
        Self {
            start,
            method: method.to_string(),
            path: path.to_string(),
        }
    }

    pub fn finish(self, status: u16) {
        let duration = self.start.elapsed();
        let duration_secs = duration.as_secs_f64();
        let duration_ms = duration.as_secs_f64() * 1000.0;

        gauge!("oracle_http_requests_in_flight",
            "method" => self.method.clone(),
            "path" => self.path.clone()
        ).decrement(1.0);

        counter!("oracle_http_requests_total",
            "method" => self.method.clone(),
            "path" => self.path.clone(),
            "status" => status.to_string()
        ).increment(1);

        histogram!("oracle_http_request_duration_seconds",
            "method" => self.method.clone(),
            "path" => self.path.clone(),
            "status" => status.to_string()
        ).record(duration_secs);

        // Also record in milliseconds for easier dashboard display
        histogram!("oracle_http_request_duration_ms",
            "method" => self.method.clone(),
            "path" => self.path.clone(),
            "status" => status.to_string()
        ).record(duration_ms);

        if status >= 500 {
            counter!("oracle_http_errors_total",
                "method" => self.method.clone(),
                "path" => self.path.clone(),
                "status" => status.to_string()
            ).increment(1);
        }
    }
}

/// Records API key authentication metrics
#[allow(dead_code)]
pub fn record_api_key_auth(success: bool, duration_ms: f64, source: &str) {
    counter!("oracle_api_key_authentications_total",
        "success" => success.to_string(),
        "source" => source.to_string()
    ).increment(1);

    histogram!("oracle_api_key_authentication_duration_ms",
        "source" => source.to_string()
    ).record(duration_ms);

    if !success {
        counter!("oracle_api_key_authentication_failures_total",
            "source" => source.to_string()
        ).increment(1);
    }
}

/// Records energy reading metrics
#[allow(dead_code)]
pub fn record_energy_reading(
    reading_type: &str,
    value_kwh: f64,
    device_id: &str,
) {
    counter!("oracle_energy_readings_total",
        "reading_type" => reading_type.to_string(),
        "device_id" => device_id.to_string()
    ).increment(1);

    histogram!("oracle_energy_reading_value_kwh",
        "reading_type" => reading_type.to_string(),
        "device_id" => device_id.to_string()
    ).record(value_kwh);
}

// =============================================================================
// Batch Forwarding Metrics
// =============================================================================

/// Records batch forwarding operations
pub fn record_batch_forward(batch_size: usize, accepted: usize, rejected: usize, duration_ms: f64) {
    counter!("oracle_batch_forwarded_total",
        "success" => "true"
    ).increment(accepted as u64);

    counter!("oracle_batch_forwarded_total",
        "success" => "false"
    ).increment(rejected as u64);

    counter!("oracle_batch_flushes_total").increment(1);

    histogram!("oracle_batch_size").record(batch_size as f64);

    histogram!("oracle_batch_forward_duration_ms").record(duration_ms);
}

/// Records batch forwarding failures
pub fn record_batch_failure(reason: &str) {
    counter!("oracle_batch_failures_total",
        "reason" => reason.to_string()
    ).increment(1);
}
