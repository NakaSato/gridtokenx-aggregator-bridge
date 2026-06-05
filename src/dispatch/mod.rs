pub mod engine;
pub mod grpc_client;

use anyhow::Result;
use async_trait::async_trait;
pub use crate::dispatch::grpc_client::DispatchType;

#[async_trait]
pub trait DispatchAdapter: Send + Sync {
    async fn execute_dispatch(&self, action: DispatchType, capacity_kw: f64) -> Result<()>;
}
