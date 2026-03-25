use async_trait::async_trait;
use crate::models::{DeviceMetrics, DeviceReading, DeviceType};
use super::ProtocolStack;
use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

/// SunSpec (Modbus TCP) Protocol Stack.
/// Maps IEEE 1547 inverter models to canonical battery/solar models.
pub struct SunSpecStack;

impl SunSpecStack {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProtocolStack for SunSpecStack {
    fn name(&self) -> &'static str {
        "SunSpec / Modbus-TCP"
    }

    async fn handle_message(&self, device_id: &str, _raw_data: &[u8]) -> Result<Option<DeviceReading>> {
        // Placeholder: Map Modbus registers to canonical metrics
        
        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::Battery,
            serial_number: device_id.to_string(),
            zone_id: Some(1),
            timestamp: Utc::now(),
            metrics: DeviceMetrics::BatteryState {
                soc_percent: 85.0,
                power_kw: 5.2,
                temperature_c: 25.0,
                mode: crate::models::BatteryMode::Discharging,
            },
            metadata: std::collections::HashMap::new(),
        }))
    }
}
