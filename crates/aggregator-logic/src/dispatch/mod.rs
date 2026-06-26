pub mod engine;
pub mod grpc_client;

pub use crate::dispatch::grpc_client::DispatchType;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait DispatchAdapter: Send + Sync {
    async fn execute_dispatch(&self, action: DispatchType, capacity_kw: f64) -> Result<()>;

    /// Whether `execute_dispatch` only simulates actuation (logs, no real
    /// downstream command). The VEN listener uses this to avoid attesting a
    /// dispatch that never physically happened: execution reports are
    /// suppressed for a simulated adapter unless `OPENLEADR_VEN_REPORTS=true`
    /// is set explicitly.
    fn is_simulation(&self) -> bool {
        false
    }
}
