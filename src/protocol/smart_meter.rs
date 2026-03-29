use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::models::{DeviceMetrics, DeviceReading, DeviceType, SmartMeterPayload};
use super::{DeviceProtocol, RawPayload};

/// Smart Meter protocol adapter.
/// Supports REST/JSON format used by GridTokenX simulators
/// and compatible with DLMS/COSEM-style field naming.
pub struct SmartMeterAdapter;

impl SmartMeterAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DeviceProtocol for SmartMeterAdapter {
    fn parse(&self, raw: &RawPayload) -> Result<DeviceReading> {
        let payload: SmartMeterPayload = serde_json::from_value(raw.body.clone())?;

        let generated = payload.energy_generated;
        let consumed = payload.energy_consumed;

        Ok(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: payload.device_id.clone(),
            device_type: DeviceType::SmartMeter,
            serial_number: payload.serial_number.unwrap_or_else(|| payload.device_id),
            zone_id: payload.zone_id,
            timestamp: payload.timestamp.unwrap_or_else(Utc::now),
            metrics: DeviceMetrics::Energy {
                generated_kwh: generated,
                consumed_kwh: consumed,
                net_kwh: generated - consumed,
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
    fn test_parse_smart_meter() {
        let adapter = SmartMeterAdapter::new();
        let raw = RawPayload {
            device_type: DeviceType::SmartMeter,
            body: json!({
                "device_id": "MTR-001",
                "energy_generated": 100.0,
                "energy_consumed": 50.0,
                "zone_id": 1
            }),
        };

        let result = adapter.parse(&raw).unwrap();
        assert_eq!(result.device_id, "MTR-001");
        assert_eq!(result.serial_number, "MTR-001");
        assert_eq!(result.zone_id, Some(1));
        
        if let DeviceMetrics::Energy { generated_kwh, consumed_kwh, net_kwh } = result.metrics {
            assert_eq!(generated_kwh, 100.0);
            assert_eq!(consumed_kwh, 50.0);
            assert_eq!(net_kwh, 50.0);
        } else {
            panic!("Wrong metrics type");
        }
    }
}
