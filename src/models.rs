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
    pub zone_id: Option<i32>,
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
// Ingest Request Models (per-device-type input payloads)
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct SmartMeterPayload {
    pub device_id: String,
    pub serial_number: Option<String>,
    pub zone_id: Option<i32>,
    pub timestamp: Option<DateTime<Utc>>,
    pub energy_generated: f64,
    pub energy_consumed: f64,
    pub reading_value: Option<f64>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EvChargerPayload {
    pub device_id: String,
    pub serial_number: Option<String>,
    pub zone_id: Option<i32>,
    pub timestamp: Option<DateTime<Utc>>,
    pub energy_delivered_kwh: f64,
    pub session_id: String,
    pub connector_id: u32,
    pub status: Option<EvStatus>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatteryPayload {
    pub device_id: String,
    pub serial_number: Option<String>,
    pub zone_id: Option<i32>,
    pub timestamp: Option<DateTime<Utc>>,
    pub soc_percent: f64,
    pub power_kw: f64,
    pub temperature_c: Option<f64>,
    pub mode: Option<BatteryMode>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

// =============================================================================
// Generic Ingest Request (auto-detect)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct GenericIngestPayload {
    pub device_type: DeviceType,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct PrivateNetworkPayload {
    pub protocol: String, // "ocpp", "sunspec", "dlms", "openadr"
    pub device_id: String,
    pub payload: serde_json::Value,
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
