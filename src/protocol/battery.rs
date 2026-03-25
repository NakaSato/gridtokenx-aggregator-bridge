use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::models::{BatteryMode, BatteryPayload, DeviceMetrics, DeviceReading, DeviceType};
use super::{DeviceProtocol, RawPayload};

/// Battery storage protocol adapter.
pub struct BatteryAdapter;

impl BatteryAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DeviceProtocol for BatteryAdapter {
    fn protocol_name(&self) -> &str {
        "Battery Storage (REST/JSON)"
    }

    fn device_types(&self) -> Vec<DeviceType> {
        vec![DeviceType::Battery]
    }

    fn parse(&self, raw: &RawPayload) -> Result<DeviceReading> {
        let payload: BatteryPayload = serde_json::from_value(raw.body.clone())?;

        let mode = payload.mode.unwrap_or_else(|| {
            if payload.power_kw > 0.0 {
                BatteryMode::Charging
            } else if payload.power_kw < 0.0 {
                BatteryMode::Discharging
            } else {
                BatteryMode::Idle
            }
        });

        Ok(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: payload.device_id.clone(),
            device_type: DeviceType::Battery,
            serial_number: payload.serial_number.unwrap_or_else(|| payload.device_id),
            zone_id: payload.zone_id,
            timestamp: payload.timestamp.unwrap_or_else(Utc::now),
            metrics: DeviceMetrics::BatteryState {
                soc_percent: payload.soc_percent,
                power_kw: payload.power_kw,
                temperature_c: payload.temperature_c.unwrap_or(25.0),
                mode,
            },
            metadata: payload.metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_battery() {
        let adapter = BatteryAdapter::new();
        let raw = RawPayload {
            device_type: DeviceType::Battery,
            body: json!({
                "device_id": "BAT-001",
                "soc_percent": 85.0,
                "power_kw": -10.0,
                "temperature_c": 28.0
            }),
        };

        let result = adapter.parse(&raw).unwrap();
        assert_eq!(result.device_id, "BAT-001");
        
        if let DeviceMetrics::BatteryState { soc_percent, power_kw, mode, .. } = result.metrics {
            assert_eq!(soc_percent, 85.0);
            assert_eq!(power_kw, -10.0);
            assert_eq!(mode, BatteryMode::Discharging);
        } else {
            panic!("Wrong metrics type");
        }
    }
}
