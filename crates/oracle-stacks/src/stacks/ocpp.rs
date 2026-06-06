use super::ProtocolStack;
use oracle_core::models::{DeviceMetrics, DeviceReading, DeviceType, EvStatus};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

/// OCPP (Open Charge Point Protocol) Stack.
/// Supports MeterValues and StatusNotification for EVSEs.
pub struct OcppStack;

#[derive(serde::Deserialize)]
struct OcppTelemetry {
    #[serde(rename = "energy_wh")]
    energy_wh: f64,
    #[serde(rename = "power_w")]
    power_w: f64,
    #[serde(rename = "status")]
    status: String,
    #[serde(rename = "connector_id")]
    connector_id: u32,
    #[serde(rename = "transaction_id")]
    transaction_id: Option<String>,
}

impl OcppStack {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolStack for OcppStack {
    async fn handle_message(
        &self,
        device_id: &str,
        raw_data: &[u8],
    ) -> Result<Option<DeviceReading>> {
        // Parse OCPP telemetry from JSON representation
        let tel: OcppTelemetry =
            serde_json::from_slice(raw_data).context("Failed to parse OCPP telemetry data")?;

        let status = match tel.status.as_str() {
            "Available" => EvStatus::Available,
            "Preparing" => EvStatus::Charging,
            "Charging" => EvStatus::Charging,
            "SuspendedEV" => EvStatus::Charging,
            "SuspendedEVSE" => EvStatus::Charging,
            "Finishing" => EvStatus::Charging,
            "Reserved" => EvStatus::Available,
            "Unavailable" => EvStatus::Faulted,
            "Faulted" => EvStatus::Faulted,
            _ => EvStatus::Available,
        };

        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::EvCharger,
            serial_number: device_id.to_string(),
            zone_code: None,
            timestamp: Utc::now() - chrono::Duration::hours(12),
            metrics: DeviceMetrics::EvSession {
                energy_delivered_kwh: tel.energy_wh / 1000.0,
                session_id: tel.transaction_id.unwrap_or_else(|| "NONE".to_string()),
                connector_id: tel.connector_id,
                status,
            },
            metadata: {
                let mut map = std::collections::HashMap::new();
                map.insert("power_w".to_string(), Value::from(tel.power_w));
                map.insert("ocpp_status_raw".to_string(), Value::from(tel.status));
                map
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ocpp_handle_valid_meter_values() {
        let stack = OcppStack::new();
        let payload = r#"{
            "energy_wh": 15000.0,
            "power_w": 3600.0,
            "status": "Charging",
            "connector_id": 1,
            "transaction_id": "TX-999"
        }"#;

        let result = stack
            .handle_message("CP-001", payload.as_bytes())
            .await
            .unwrap();
        let reading = result.unwrap();

        assert_eq!(reading.device_id, "CP-001");
        if let DeviceMetrics::EvSession {
            energy_delivered_kwh,
            session_id,
            connector_id,
            status,
        } = reading.metrics
        {
            assert_eq!(energy_delivered_kwh, 15.0);
            assert_eq!(session_id, "TX-999");
            assert_eq!(connector_id, 1);
            assert_eq!(status, EvStatus::Charging);
        } else {
            panic!("Wrong metric type");
        }
    }

    #[tokio::test]
    async fn test_ocpp_status_mapping() {
        let stack = OcppStack::new();
        let statuses = vec![
            ("Available", EvStatus::Available),
            ("Faulted", EvStatus::Faulted),
            ("SuspendedEV", EvStatus::Charging),
            ("UnknownStatus", EvStatus::Available),
        ];

        for (raw, expected) in statuses {
            let payload = format!(
                r#"{{
                "energy_wh": 0.0,
                "power_w": 0.0,
                "status": "{}",
                "connector_id": 1
            }}"#,
                raw
            );

            let result = stack
                .handle_message("CP-001", payload.as_bytes())
                .await
                .unwrap();
            let reading = result.unwrap();

            if let DeviceMetrics::EvSession { status, .. } = reading.metrics {
                assert_eq!(status, expected, "Failed for raw status: {}", raw);
            }
        }
    }
}
