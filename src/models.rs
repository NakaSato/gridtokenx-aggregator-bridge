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
// Private Network Ingestion (OCPP, SunSpec, DLMS, OpenADR)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PrivateNetworkPayload {
    pub protocol: String, // "ocpp", "sunspec", "dlms", "openadr"
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
