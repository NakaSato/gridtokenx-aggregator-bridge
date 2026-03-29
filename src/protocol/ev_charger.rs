use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::models::{DeviceMetrics, DeviceReading, DeviceType, EvChargerPayload, EvStatus};
use super::{DeviceProtocol, RawPayload};

/// EV Charger protocol adapter.
/// Compatible with OCPP 1.6/2.0 MeterValues JSON format.
pub struct EvChargerAdapter;

impl EvChargerAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DeviceProtocol for EvChargerAdapter {
    fn parse(&self, raw: &RawPayload) -> Result<DeviceReading> {
        let payload: EvChargerPayload = serde_json::from_value(raw.body.clone())?;

        Ok(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: payload.device_id.clone(),
            device_type: DeviceType::EvCharger,
            serial_number: payload.serial_number.unwrap_or_else(|| payload.device_id),
            zone_id: payload.zone_id,
            timestamp: payload.timestamp.unwrap_or_else(Utc::now),
            metrics: DeviceMetrics::EvSession {
                energy_delivered_kwh: payload.energy_delivered_kwh,
                session_id: payload.session_id,
                connector_id: payload.connector_id,
                status: payload.status.unwrap_or(EvStatus::Charging),
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
    fn test_parse_ev_charger() {
        let adapter = EvChargerAdapter::new();
        let raw = RawPayload {
            device_type: DeviceType::EvCharger,
            body: json!({
                "device_id": "EV-101",
                "energy_delivered_kwh": 25.5,
                "session_id": "sess-999",
                "connector_id": 1,
                "status": "charging"
            }),
        };

        let result = adapter.parse(&raw).unwrap();
        assert_eq!(result.device_id, "EV-101");
        assert_eq!(result.serial_number, "EV-101");
        
        if let DeviceMetrics::EvSession { energy_delivered_kwh, session_id, .. } = result.metrics {
            assert_eq!(energy_delivered_kwh, 25.5);
            assert_eq!(session_id, "sess-999");
        } else {
            panic!("Wrong metrics type");
        }
    }
}
