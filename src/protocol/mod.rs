pub mod binary_decoder;
pub mod stacks;

use crate::models::DeviceType;
pub use binary_decoder::DlmsBinaryFrame;

/// Raw incoming payload before protocol-specific parsing.
pub struct RawPayload {
    pub device_type: DeviceType,
    pub body: serde_json::Value,
}
