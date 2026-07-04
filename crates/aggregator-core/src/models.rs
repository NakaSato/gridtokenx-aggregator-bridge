use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// =============================================================================
// Device Types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    SmartMeter,
    EvCharger,
    Battery,
}

impl DeviceType {
    pub fn target_stream(&self) -> &str {
        match self {
            DeviceType::SmartMeter => "gridtokenx:events:v1",
            DeviceType::EvCharger => "gridtokenx:ev:v1",
            DeviceType::Battery => "gridtokenx:battery:v1",
        }
    }
}

// =============================================================================
// Canonical Device Reading
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceReading {
    pub reading_id: Uuid,
    pub device_id: String,
    pub device_type: DeviceType,
    pub serial_number: String,
    pub zone_code: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub metrics: DeviceMetrics,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeviceMetrics {
    Energy {
        generated_kwh: f64,
        consumed_kwh: f64,
        net_kwh: f64,
    },
    EvSession {
        energy_delivered_kwh: f64,
        session_id: String,
        connector_id: u32,
        status: EvStatus,
    },
    BatteryState {
        soc_percent: f64,
        power_kw: f64,
        temperature_c: f64,
        mode: BatteryMode,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvStatus {
    Available,
    Charging,
    SuspendedEv,
    SuspendedEvse,
    Finishing,
    Faulted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatteryMode {
    Idle,
    Charging,
    Discharging,
}

// =============================================================================
// Private Network Ingestion (DLMS/COSEM)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PrivateNetworkPayload {
    pub protocol: String, // "dlms" ("auto"/empty → dlms; "simulator" = unsigned dev bypass)
    pub device_id: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct BatchPrivateNetworkPayload {
    pub protocol: String,
    pub readings: Vec<serde_json::Value>,
}

// =============================================================================
// API Response
// =============================================================================

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub status: &'static str,
    pub reading_id: Uuid,
    pub device_type: DeviceType,
    pub stream: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_stream_maps_each_device_type() {
        assert_eq!(
            DeviceType::SmartMeter.target_stream(),
            "gridtokenx:events:v1"
        );
        assert_eq!(DeviceType::EvCharger.target_stream(), "gridtokenx:ev:v1");
        assert_eq!(DeviceType::Battery.target_stream(), "gridtokenx:battery:v1");
    }

    #[test]
    fn device_type_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&DeviceType::SmartMeter).unwrap(),
            r#""smart_meter""#
        );
        assert_eq!(
            serde_json::to_string(&DeviceType::EvCharger).unwrap(),
            r#""ev_charger""#
        );
    }

    #[test]
    fn device_metrics_energy_is_internally_tagged() {
        let m = DeviceMetrics::Energy {
            generated_kwh: 5.0,
            consumed_kwh: 2.0,
            net_kwh: 3.0,
        };
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(v["type"], "energy");
        assert_eq!(v["net_kwh"], 3.0);
    }

    #[test]
    fn device_metrics_roundtrips_through_tag() {
        let m = DeviceMetrics::BatteryState {
            soc_percent: 80.0,
            power_kw: -1.5,
            temperature_c: 25.0,
            mode: BatteryMode::Discharging,
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: DeviceMetrics = serde_json::from_str(&json).unwrap();
        match back {
            DeviceMetrics::BatteryState {
                soc_percent, mode, ..
            } => {
                assert_eq!(soc_percent, 80.0);
                assert_eq!(mode, BatteryMode::Discharging);
            }
            _ => panic!("expected BatteryState"),
        }
    }

    #[test]
    fn ev_status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&EvStatus::SuspendedEvse).unwrap(),
            r#""suspended_evse""#
        );
    }
}
