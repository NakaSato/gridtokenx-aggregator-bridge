pub mod dlms;
pub mod ocpp;
pub mod openadr;
pub mod sunspec;

use aggregator_core::models::DeviceReading;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

/// Auto-detect the device protocol from a decoded JSON payload by inspecting
/// its discriminating field set.
///
/// Returns one of `"ocpp" | "openadr" | "sunspec" | "dlms"`. Falls back to
/// `"dlms"` — the dominant smart-meter path (OBIS-coded or plain energy fields)
/// — when no stronger signal is present. Used when an ingest request declares
/// `protocol = "auto"` (or omits it) so callers don't have to hard-code the
/// stack per device.
pub fn detect_protocol(payload: &Value) -> &'static str {
    let obj = match payload.as_object() {
        Some(o) => o,
        None => return "dlms",
    };
    let has = |k: &str| obj.contains_key(k);

    // SunSpec Model 103 inverter registers (scale-factored W/WH + status).
    if has("W") && has("WH") && has("St") {
        return "sunspec";
    }
    // OCPP EVSE MeterValues / StatusNotification.
    if has("connector_id") && (has("energy_wh") || has("transaction_id")) {
        return "ocpp";
    }
    // OpenADR EiReport demand-response telemetry.
    if has("actual_kw") && (has("baseload_kw") || has("available_capacity_kw")) {
        return "openadr";
    }
    // DLMS/COSEM: OBIS-coded keys (e.g. "1.1.1.8.0.255") or plain energy fields.
    "dlms"
}

/// Unified trait for "Private Network" protocol stacks.
/// These are more complex than simple stateless adapters and
/// may involve state machines or multi-step handshake logic.
#[async_trait]
pub trait ProtocolStack: Send + Sync {
    /// Handle a raw incoming message from a private network device.
    /// This may return a canonical `DeviceReading` if the message
    /// contains enough information to update the ledger.
    async fn handle_message(
        &self,
        device_id: &str,
        raw_data: &[u8],
    ) -> Result<Option<DeviceReading>>;
}

#[cfg(test)]
mod tests {
    use super::detect_protocol;
    use serde_json::json;

    #[test]
    fn detects_sunspec_model_103() {
        let p = json!({"W": 5000.0, "W_SF": -1, "WH": 12345.0, "WH_SF": 0, "St": 4});
        assert_eq!(detect_protocol(&p), "sunspec");
    }

    #[test]
    fn detects_ocpp_evse() {
        let p = json!({"energy_wh": 1500.0, "power_w": 7200.0, "status": "Charging", "connector_id": 1});
        assert_eq!(detect_protocol(&p), "ocpp");
    }

    #[test]
    fn detects_openadr_report() {
        let p = json!({"baseload_kw": 3.0, "actual_kw": 2.4, "available_capacity_kw": 5.0, "request_id": "r1", "status": "active"});
        assert_eq!(detect_protocol(&p), "openadr");
    }

    #[test]
    fn detects_dlms_obis_keys() {
        let p = json!({"1.1.1.8.0.255": 1000.0, "1.1.2.8.0.255": 2000.0});
        assert_eq!(detect_protocol(&p), "dlms");
    }

    #[test]
    fn falls_back_to_dlms_for_plain_energy_fields() {
        let p = json!({"kwh": 1.5, "energy_generated": 2.5, "energy_consumed": 1.0});
        assert_eq!(detect_protocol(&p), "dlms");
    }

    #[test]
    fn falls_back_to_dlms_for_non_object() {
        assert_eq!(detect_protocol(&json!("not-an-object")), "dlms");
    }
}
