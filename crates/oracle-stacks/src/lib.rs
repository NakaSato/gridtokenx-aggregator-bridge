//! oracle-stacks — device protocol stack implementations.
//! Each stack implements `stacks::ProtocolStack`, decoding raw device frames into `oracle_core::models` readings.

pub mod binary_decoder;
pub mod stacks;

use oracle_core::models::DeviceType;
pub use binary_decoder::DlmsBinaryFrame;

/// Raw incoming payload before protocol-specific parsing.
pub struct RawPayload {
    pub device_type: DeviceType,
    pub body: serde_json::Value,
}
