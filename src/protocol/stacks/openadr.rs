use super::ProtocolStack;
use crate::models::{DeviceMetrics, DeviceReading, DeviceType};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

/// OpenADR (Automated Demand Response) Protocol Stack.
/// Supports EiEvent and EiReport for demand side management.
pub struct OpenAdrStack;

#[derive(serde::Deserialize)]
struct OpenAdrReport {
    #[serde(rename = "baseload_kw")]
    baseload_kw: f64,
    #[serde(rename = "actual_kw")]
    actual_kw: f64,
    #[serde(rename = "available_capacity_kw")]
    available_capacity_kw: f64,
    #[serde(rename = "request_id")]
    request_id: String,
    #[serde(rename = "status")]
    status: String,
}

impl OpenAdrStack {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolStack for OpenAdrStack {
    async fn handle_message(
        &self,
        device_id: &str,
        raw_data: &[u8],
    ) -> Result<Option<DeviceReading>> {
        // Parse OpenADR EiReport from JSON representation
        let report: OpenAdrReport =
            serde_json::from_slice(raw_data).context("Failed to parse OpenADR report data")?;

        // OpenADR is often about controlling consumption,
        // but here we report current available capacity for "lending" or response.
        // We map baseload to consumed_kwh (simplified over 1h context)

        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter, // Typically mapped to aggregate meter
            serial_number: device_id.to_string(),
            zone_code: None,
            timestamp: Utc::now() - chrono::Duration::hours(12),
            metrics: DeviceMetrics::Energy {
                generated_kwh: 0.0,
                consumed_kwh: report.actual_kw, // Current demand as consumed energy
                net_kwh: -report.actual_kw,
            },
            metadata: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "openadr_status".to_string(),
                    serde_json::json!(report.status),
                );
                map.insert(
                    "available_capacity_kw".to_string(),
                    serde_json::json!(report.available_capacity_kw),
                );
                map.insert(
                    "openadr_request_id".to_string(),
                    serde_json::json!(report.request_id),
                );
                map.insert(
                    "baseload_kw".to_string(),
                    serde_json::json!(report.baseload_kw),
                );
                map
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_openadr_handle_valid_report() {
        let stack = OpenAdrStack::new();
        let payload = r#"{
            "baseload_kw": 50.0,
            "actual_kw": 45.5,
            "available_capacity_kw": 4.5,
            "request_id": "REQ-123",
            "status": "active"
        }"#;

        let result = stack
            .handle_message("VEN-001", payload.as_bytes())
            .await
            .unwrap();
        let reading = result.unwrap();

        assert_eq!(reading.device_id, "VEN-001");
        if let DeviceMetrics::Energy {
            generated_kwh,
            consumed_kwh,
            net_kwh,
        } = reading.metrics
        {
            assert_eq!(generated_kwh, 0.0);
            assert_eq!(consumed_kwh, 45.5);
            assert_eq!(net_kwh, -45.5);
        } else {
            panic!("Wrong metric type");
        }

        assert_eq!(
            reading.metadata.get("openadr_status").unwrap(),
            &serde_json::json!("active")
        );
        assert_eq!(
            reading.metadata.get("available_capacity_kw").unwrap(),
            &serde_json::json!(4.5)
        );
    }

    #[tokio::test]
    async fn test_openadr_handle_invalid_json() {
        let stack = OpenAdrStack::new();
        let payload = r#"{ "not": "openadr" }"#;
        let result = stack.handle_message("VEN-001", payload.as_bytes()).await;
        assert!(result.is_err());
    }
}
