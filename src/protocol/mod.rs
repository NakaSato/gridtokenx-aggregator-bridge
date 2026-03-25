use async_trait::async_trait;
use anyhow::Result;

pub mod smart_meter;
pub mod ev_charger;
pub mod battery;
pub mod stacks;

use crate::models::{DeviceReading, DeviceType};

/// Raw incoming payload before protocol-specific parsing.
pub struct RawPayload {
    pub device_type: DeviceType,
    pub body: serde_json::Value,
}

/// Trait for protocol adapters that normalize device-specific
/// payloads into the canonical `DeviceReading` format.
#[async_trait]
pub trait DeviceProtocol: Send + Sync {
    /// Human-readable protocol name (e.g., "DLMS/COSEM", "OCPP 1.6").
    fn protocol_name(&self) -> &str;

    /// Which device types this adapter handles.
    fn device_types(&self) -> Vec<DeviceType>;

    /// Parse raw payload into a canonical `DeviceReading`.
    fn parse(&self, raw: &RawPayload) -> Result<DeviceReading>;
}
