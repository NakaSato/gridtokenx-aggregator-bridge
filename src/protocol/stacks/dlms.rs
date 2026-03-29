use async_trait::async_trait;
use crate::models::{DeviceMetrics, DeviceReading, DeviceType};
use super::ProtocolStack;
use anyhow::{Result, Context};
use chrono::Utc;
use uuid::Uuid;

/// DLMS/COSEM Protocol Stack.
/// Handles commercial and industrial smart meter protocol packets.
pub struct DlmsStack;

#[derive(serde::Deserialize)]
struct DlmsTelemetry {
    #[serde(rename = "active_energy_import_wh")]
    active_energy_import_wh: f64,
    #[serde(rename = "active_energy_export_wh")]
    active_energy_export_wh: f64,
    #[serde(rename = "instantaneous_voltage_v")]
    voltage: Option<f64>,
    #[serde(rename = "instantaneous_current_a")]
    current: Option<f64>,
}

impl DlmsStack {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolStack for DlmsStack {
    async fn handle_message(&self, device_id: &str, raw_data: &[u8]) -> Result<Option<DeviceReading>> {
        // Parse DLMS telemetry from JSON representation
        let tel: DlmsTelemetry = serde_json::from_slice(raw_data)
            .context("Failed to parse DLMS telemetry data")?;

        let generated_kwh = tel.active_energy_export_wh / 1000.0;
        let consumed_kwh = tel.active_energy_import_wh / 1000.0;
        
        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: device_id.to_string(),
            zone_id: None,
            timestamp: Utc::now() - chrono::Duration::hours(12), // Adjust for localnet drift if needed
            metrics: DeviceMetrics::Energy {
                generated_kwh,
                consumed_kwh,
                net_kwh: generated_kwh - consumed_kwh,
            },
            metadata: {
                let mut map = std::collections::HashMap::new();
                if let Some(v) = tel.voltage {
                    map.insert("voltage_v".to_string(), serde_json::to_value(v)?);
                }
                if let Some(a) = tel.current {
                    map.insert("current_a".to_string(), serde_json::to_value(a)?);
                }
                map
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dlms_handle_valid_message() {
        let stack = DlmsStack::new();
        let payload = r#"{
            "active_energy_import_wh": 1000.0,
            "active_energy_export_wh": 500.0,
            "instantaneous_voltage_v": 230.5,
            "instantaneous_current_a": 4.2
        }"#;

        let result = stack.handle_message("MTR-X", payload.as_bytes()).await.unwrap();
        let reading = result.unwrap();

        assert_eq!(reading.device_id, "MTR-X");
        if let DeviceMetrics::Energy { generated_kwh, consumed_kwh, net_kwh } = reading.metrics {
            assert_eq!(generated_kwh, 0.5);
            assert_eq!(consumed_kwh, 1.0);
            assert_eq!(net_kwh, -0.5);
        } else {
            panic!("Wrong metric type");
        }

        assert_eq!(reading.metadata.get("voltage_v").unwrap(), &serde_json::json!(230.5));
        assert_eq!(reading.metadata.get("current_a").unwrap(), &serde_json::json!(4.2));
    }

    #[tokio::test]
    async fn test_dlms_handle_invalid_json() {
        let stack = DlmsStack::new();
        let payload = r#"{ "bad": "json" "#;
        let result = stack.handle_message("MTR-X", payload.as_bytes()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dlms_handle_missing_optional_fields() {
        let stack = DlmsStack::new();
        let payload = r#"{
            "active_energy_import_wh": 1000.0,
            "active_energy_export_wh": 0.0
        }"#;

        let result = stack.handle_message("MTR-X", payload.as_bytes()).await.unwrap();
        let reading = result.unwrap();

        assert_eq!(reading.metadata.get("voltage_v"), None);
        assert_eq!(reading.metadata.get("current_a"), None);
    }
}
