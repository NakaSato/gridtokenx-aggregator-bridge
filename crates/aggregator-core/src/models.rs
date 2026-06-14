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
// Mint Forward (aggregator → meter-service over NATS)
// =============================================================================

/// Reading forwarded to meter-service for the on-chain energy-token mint.
///
/// This payload IS the data that becomes on-chain mint provenance, so it carries
/// exactly what the mint needs: a stable idempotency key (so a NATS redelivery or
/// replay cannot double-mint the same energy), the meter identity, the net energy
/// to mint, and when it was measured. The recipient wallet is intentionally NOT
/// on the wire — meter-service derives it from the registered meter owner, so an
/// untrusted forward cannot redirect minted tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintForwardReading {
    /// Stable reading id — the idempotency key. meter-service uses it as the
    /// reading primary key, so a duplicate delivery is a no-op insert and the
    /// mint's `minted` guard prevents a second on-chain mint.
    pub reading_id: Uuid,
    /// Aggregator device id (diagnostic / correlation only).
    pub device_id: String,
    /// Physical meter serial — device identity / provenance for the mint and the
    /// key meter-service resolves the owning user + wallet from.
    pub meter_serial: String,
    /// Net surplus energy in kWh to mint.
    pub energy_kwh: f64,
    /// Reading timestamp as epoch milliseconds.
    pub timestamp_ms: i64,
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
