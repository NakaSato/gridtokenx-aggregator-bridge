pub mod dlms;
pub mod ocpp;
pub mod openadr;
pub mod sunspec;

use crate::models::DeviceReading;
use anyhow::Result;
use async_trait::async_trait;

/// Unified trait for "Private Network" protocol stacks.
/// These are more complex than simple stateless adapters and
/// may involve state machines or multi-step handshake logic.
#[async_trait]
pub trait ProtocolStack: Send + Sync {
    /// Handle a raw incoming message from a private network device.
    /// This may return a canonical `DeviceReading` if the message
    /// contains enough information to update the ledger.
    async fn handle_message(
        &self,
        device_id: &str,
        raw_data: &[u8],
    ) -> Result<Option<DeviceReading>>;
}
