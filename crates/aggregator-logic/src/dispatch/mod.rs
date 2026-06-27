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

#[cfg(test)]
mod tests {
    use super::*;

    /// Adapter that doesn't override `is_simulation` — must inherit the
    /// trait default of `false` (a real adapter attests actuation, so the
    /// VEN listener will send execution reports). Counterpart to the
    /// `Ieee2030_5Adapter` stub which overrides it to `true`.
    struct RealAdapter;

    #[async_trait]
    impl DispatchAdapter for RealAdapter {
        async fn execute_dispatch(&self, _action: DispatchType, _capacity_kw: f64) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn is_simulation_defaults_to_false() {
        assert!(!RealAdapter.is_simulation());
    }
}
