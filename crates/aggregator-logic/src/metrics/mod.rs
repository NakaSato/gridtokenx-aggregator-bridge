//! Aggregator Bridge metrics instrumentation
//!
//! This module provides metrics for:
//! - IoT ingestion endpoints
//! - Redis stream processing
//! - Aggregator/bridge operations
//! - Device connectivity

use metrics::{counter, gauge, histogram};
use std::time::Instant;

/// Records IoT ingestion request metrics
#[allow(dead_code)]
pub fn record_ingestion_request(device_type: &str, success: bool, duration_ms: f64) {
    counter!("aggregator_ingestion_requests_total",
        "device_type" => device_type.to_string(),
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_ingestion_request_duration_ms",
        "device_type" => device_type.to_string()
    )
    .record(duration_ms);
}

/// Records meter reading ingestion metrics
pub fn record_meter_reading(success: bool, duration_ms: f64) {
    counter!("aggregator_meter_readings_total",
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_meter_reading_duration_ms").record(duration_ms);

    if !success {
        counter!("aggregator_meter_reading_failures_total").increment(1);
    }
}

/// Records EV charger data ingestion metrics
pub fn record_ev_charger_data(success: bool, duration_ms: f64) {
    counter!("aggregator_ev_charger_data_total",
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_ev_charger_data_duration_ms").record(duration_ms);
}

/// Records battery data ingestion metrics
pub fn record_battery_data(success: bool, duration_ms: f64) {
    counter!("aggregator_battery_data_total",
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_battery_data_duration_ms").record(duration_ms);
}

/// Records Redis stream processing metrics
#[allow(dead_code)]
pub fn record_redis_stream_processing(
    stream_name: &str,
    messages_processed: u64,
    success: bool,
    duration_ms: f64,
) {
    counter!("aggregator_redis_stream_processed_total",
        "stream" => stream_name.to_string(),
        "success" => success.to_string()
    )
    .increment(messages_processed);

    histogram!("aggregator_redis_stream_processing_duration_ms",
        "stream" => stream_name.to_string()
    )
    .record(duration_ms);

    if !success {
        counter!("aggregator_redis_stream_processing_failures_total",
            "stream" => stream_name.to_string()
        )
        .increment(1);
    }
}

/// Records Aggregator forwarding to API Gateway
pub fn record_aggregator_forwarding(success: bool, duration_ms: f64) {
    counter!("aggregator_forwarding_total",
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_forwarding_duration_ms").record(duration_ms);

    if !success {
        counter!("aggregator_forwarding_failures_total").increment(1);
    }
}

/// Records device connectivity metrics
#[allow(dead_code)]
pub fn record_device_connection(device_type: &str, connected: bool) {
    if connected {
        counter!("aggregator_device_connections_total",
            "device_type" => device_type.to_string()
        )
        .increment(1);
    } else {
        counter!("aggregator_device_disconnections_total",
            "device_type" => device_type.to_string()
        )
        .increment(1);
    }
}

/// Records active device count
#[allow(dead_code)]
pub fn record_active_devices(device_type: &str, count: u64) {
    gauge!("aggregator_active_devices", "device_type" => device_type.to_string()).set(count as f64);
}

/// Records data validation metrics
#[allow(dead_code)]
pub fn record_data_validation(valid: bool, reason: &str) {
    counter!("aggregator_data_validations_total",
        "valid" => valid.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);

    if !valid {
        counter!("aggregator_data_validation_failures_total",
            "reason" => reason.to_string()
        )
        .increment(1);
    }
}

/// Records protocol adapter metrics
#[allow(dead_code)]
pub fn record_protocol_adapter(protocol: &str, operation: &str, success: bool, duration_ms: f64) {
    counter!("aggregator_protocol_adapter_operations_total",
        "protocol" => protocol.to_string(),
        "operation" => operation.to_string(),
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_protocol_adapter_operation_duration_ms",
        "protocol" => protocol.to_string(),
        "operation" => operation.to_string()
    )
    .record(duration_ms);
}

/// Records gRPC client metrics for IAM
#[allow(dead_code)]
pub fn record_grpc_client_call(service: &str, method: &str, success: bool, duration_ms: f64) {
    counter!("aggregator_grpc_client_calls_total",
        "service" => service.to_string(),
        "method" => method.to_string(),
        "success" => success.to_string()
    )
    .increment(1);

    histogram!("aggregator_grpc_client_call_duration_ms",
        "service" => service.to_string(),
        "method" => method.to_string()
    )
    .record(duration_ms);
}

/// Records HTTP request metrics for Aggregator Bridge
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
        gauge!("aggregator_http_requests_in_flight",
            "method" => method.to_string(),
            "path" => path.to_string()
        )
        .increment(1.0);
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

        gauge!("aggregator_http_requests_in_flight",
            "method" => self.method.clone(),
            "path" => self.path.clone()
        )
        .decrement(1.0);

        counter!("aggregator_http_requests_total",
            "method" => self.method.clone(),
            "path" => self.path.clone(),
            "status" => status.to_string()
        )
        .increment(1);

        histogram!("aggregator_http_request_duration_seconds",
            "method" => self.method.clone(),
            "path" => self.path.clone(),
            "status" => status.to_string()
        )
        .record(duration_secs);

        // Also record in milliseconds for easier dashboard display
        histogram!("aggregator_http_request_duration_ms",
            "method" => self.method.clone(),
            "path" => self.path.clone(),
            "status" => status.to_string()
        )
        .record(duration_ms);

        if status >= 500 {
            counter!("aggregator_http_errors_total",
                "method" => self.method.clone(),
                "path" => self.path.clone(),
                "status" => status.to_string()
            )
            .increment(1);
        }
    }
}

/// Records API key authentication metrics
#[allow(dead_code)]
pub fn record_api_key_auth(success: bool, duration_ms: f64, source: &str) {
    counter!("aggregator_api_key_authentications_total",
        "success" => success.to_string(),
        "source" => source.to_string()
    )
    .increment(1);

    histogram!("aggregator_api_key_authentication_duration_ms",
        "source" => source.to_string()
    )
    .record(duration_ms);

    if !success {
        counter!("aggregator_api_key_authentication_failures_total",
            "source" => source.to_string()
        )
        .increment(1);
    }
}

/// Records energy reading metrics
#[allow(dead_code)]
pub fn record_energy_reading(reading_type: &str, value_kwh: f64, device_id: &str) {
    counter!("aggregator_energy_readings_total",
        "reading_type" => reading_type.to_string(),
        "device_id" => device_id.to_string()
    )
    .increment(1);

    histogram!("aggregator_energy_reading_value_kwh",
        "reading_type" => reading_type.to_string(),
        "device_id" => device_id.to_string()
    )
    .record(value_kwh);
}

// =============================================================================
// Dispatch / OpenADR Metrics
// =============================================================================

/// Records a dispatch-engine outcome: "fired", "suppressed" (cooldown), or
/// "failed" (adapter error — retried on the next grid-status message).
pub fn record_dispatch_outcome(action: &str, adapter: &str, outcome: &str) {
    counter!("aggregator_dispatch_total",
        "action" => action.to_string(),
        "adapter" => adapter.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

/// Records an OpenADR VEN listener outcome per polled event:
/// "executed", "dispatch_failed" (retried next poll), or "report_failed"
/// (best-effort report did not reach the VTN; dispatch already done).
pub fn record_ven_event(outcome: &str) {
    counter!("aggregator_openleadr_ven_events_total",
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

// =============================================================================
// Settlement / Surplus-Mint Metrics
// =============================================================================

/// Records a surplus-mint settlement outcome per completed billing bin:
/// "settled" (minted on-chain via Chain Bridge), "skipped" (the bin aggregated
/// but no token was issued — most often an unregistered meter with no recipient
/// wallet, which otherwise fails silently), "failed" (the bridge/NATS mint call
/// errored), or "no_surplus" (net consumption — nothing to mint). `reason`
/// qualifies the outcome ("no_wallet", "resolve_err", "mint_err", "ok").
pub fn record_mint_outcome(outcome: &str, reason: &str) {
    counter!("aggregator_mint_total",
        "outcome" => outcome.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

/// Records which settlement path is active for surplus-mint dissemination:
/// "nats" (signed `chain.tx.mint` over NATS request-reply) or "disabled"
/// (minting off — `MINT_VIA_CHAIN_BRIDGE` unset or NATS unreachable). Set once
/// at startup; a future direct-gRPC mint path would report "grpc". Emits the
/// active path as `1` and the others as `0` so a single
/// `aggregator_settlement_path{path="..."} == 1` series names the mode without a
/// stale label lingering at 1 after a mode change.
pub fn record_settlement_path(path: &str) {
    for candidate in ["nats", "grpc", "disabled"] {
        gauge!("aggregator_settlement_path", "path" => candidate.to_string())
            .set(if candidate == path { 1.0 } else { 0.0 });
    }
}

// =============================================================================
// Batch Forwarding Metrics
// =============================================================================

/// Records batch forwarding operations
pub fn record_batch_forward(batch_size: usize, accepted: usize, rejected: usize, duration_ms: f64) {
    counter!("aggregator_batch_forwarded_total",
        "success" => "true"
    )
    .increment(accepted as u64);

    counter!("aggregator_batch_forwarded_total",
        "success" => "false"
    )
    .increment(rejected as u64);

    counter!("aggregator_batch_flushes_total").increment(1);

    histogram!("aggregator_batch_size").record(batch_size as f64);

    histogram!("aggregator_batch_forward_duration_ms").record(duration_ms);
}

/// Records batch forwarding failures
pub fn record_batch_failure(reason: &str) {
    counter!("aggregator_batch_failures_total",
        "reason" => reason.to_string()
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    // These recorders emit into the global `metrics` facade. With no recorder
    // installed (the unit-test default) every emit is a no-op — the contract
    // being asserted is that they never panic and accept their full label set,
    // for both success and failure branches. A live recorder + scrape is an
    // integration concern (the running service's /metrics endpoint).

    #[test]
    fn ingestion_recorders_do_not_panic() {
        record_ingestion_request("smart_meter", true, 1.0);
        record_ingestion_request("ev_charger", false, 2.0);
        record_meter_reading(true, 1.0);
        record_meter_reading(false, 1.0); // failure branch
        record_ev_charger_data(true, 1.0);
        record_battery_data(true, 1.0);
        record_energy_reading("generated", 12.5, "dev-1");
    }

    #[test]
    fn pipeline_recorders_do_not_panic() {
        record_redis_stream_processing("zone:1", 10, true, 5.0);
        record_redis_stream_processing("zone:1", 0, false, 5.0); // failure branch
        record_aggregator_forwarding(true, 3.0);
        record_aggregator_forwarding(false, 3.0); // failure branch
        record_batch_forward(50, 48, 2, 7.0);
        record_batch_failure("grpc_error");
        record_protocol_adapter("dlms", "decode", true, 1.0);
        record_grpc_client_call("iam", "verify", false, 2.0);
    }

    #[test]
    fn device_and_validation_recorders_do_not_panic() {
        record_device_connection("smart_meter", true);
        record_device_connection("smart_meter", false); // disconnect branch
        record_active_devices("battery", 3);
        record_data_validation(true, "ok");
        record_data_validation(false, "bad_sig"); // failure branch
        record_api_key_auth(true, 1.0, "iam");
        record_api_key_auth(false, 1.0, "static"); // failure branch
    }

    #[test]
    fn dispatch_recorders_do_not_panic() {
        record_dispatch_outcome("FLEX_UP", "openleadr", "fired");
        record_ven_event("executed");
    }

    #[test]
    fn settlement_recorders_do_not_panic() {
        record_mint_outcome("settled", "ok");
        record_mint_outcome("skipped", "no_wallet"); // unregistered meter
        record_mint_outcome("skipped", "resolve_err");
        record_mint_outcome("failed", "mint_err");
        record_mint_outcome("no_surplus", "ok");
        record_settlement_path("nats");
        record_settlement_path("disabled");
    }

    #[test]
    fn http_metrics_timer_full_lifecycle_does_not_panic() {
        HttpMetricsTimer::new("POST", "/v1/ingest/telemetry").finish(200);
        HttpMetricsTimer::new("GET", "/health").finish(500); // 5xx error branch
    }
}
