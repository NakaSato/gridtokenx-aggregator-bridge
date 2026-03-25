use async_trait::async_trait;
use crate::models::{DeviceMetrics, DeviceReading, DeviceType};
use super::ProtocolStack;
use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

/// OpenADR (Automated Demand Response) Protocol Stack.
/// Supports EiEvent and EiReport for demand side management.
pub struct OpenAdrStack;

impl OpenAdrStack {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolStack for OpenAdrStack {
    fn name(&self) -> &'static str {
        "OpenADR 2.0b VEN"
    }

    async fn handle_message(&self, device_id: &str, _raw_data: &[u8]) -> Result<Option<DeviceReading>> {
        // Placeholder: Handle VEN (Virtual End Node) logic
        // OpenADR is often about controlling consumption, 
        // but here we report current available capacity for "lending" or response.
        
        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter, // Typically mapped to aggregate meter
            serial_number: device_id.to_string(),
            zone_id: Some(1),
            timestamp: Utc::now(),
            metrics: DeviceMetrics::Energy {
                generated_kwh: 0.0,
                consumed_kwh: 500.0, // Baseline consumption
                net_kwh: -500.0,
            },
            metadata: [
                ("openadr_status".to_string(), serde_json::json!("idle")),
                ("available_capacity_kw".to_string(), serde_json::json!(150.0)),
            ].into_iter().collect(),
        }))
    }
}
