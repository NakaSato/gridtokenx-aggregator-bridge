use crate::dispatch::grpc_client::DispatchType;
use crate::dispatch::DispatchAdapter;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
pub struct DerControlPayload {
    pub control_type: String,
    pub capacity_kw: f64,
    pub timestamp: i64,
}

/// Map a flex action to its IEEE 2030.5 DERControl type. FLEX_UP (grid needs
/// the DER to back off) reduces power; FLEX_DOWN increases it. Pure so the
/// mapping can be asserted without the (stubbed) HTTP transmission.
fn der_control_type(action: DispatchType) -> &'static str {
    match action {
        DispatchType::FLEX_UP => "DERControl_ReducePower",
        DispatchType::FLEX_DOWN => "DERControl_IncreasePower",
    }
}

/// Adapter for IEEE 2030.5 DERControl mapping logic
#[derive(Default)]
pub struct Ieee2030_5Adapter;

impl Ieee2030_5Adapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DispatchAdapter for Ieee2030_5Adapter {
    async fn execute_dispatch(&self, action: DispatchType, capacity_kw: f64) -> Result<()> {
        let control_type = der_control_type(action);

        let payload = DerControlPayload {
            control_type: control_type.to_string(),
            capacity_kw,
            timestamp: gridtokenx_telemetry::time::now().timestamp(),
        };

        info!("Sending IEEE 2030.5 payload: {:?}", payload);

        // Simulation of IEEE 2030.5 transmission via HTTP (reqwest)
        // In a real environment, this would target the DER Gateway URL
        let _client = reqwest::Client::new();

        info!("IEEE 2030.5 payload transmitted successfully.");
        Ok(())
    }

    fn is_simulation(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn der_control_type_maps_flex_actions() {
        // FLEX_UP ⇒ DER reduces power; FLEX_DOWN ⇒ DER increases power.
        assert_eq!(
            der_control_type(DispatchType::FLEX_UP),
            "DERControl_ReducePower"
        );
        assert_eq!(
            der_control_type(DispatchType::FLEX_DOWN),
            "DERControl_IncreasePower"
        );
    }

    #[test]
    fn adapter_reports_simulation() {
        // VEN listener relies on this to suppress execution reports for the stub.
        assert!(Ieee2030_5Adapter::new().is_simulation());
    }

    #[tokio::test]
    async fn execute_dispatch_is_ok_for_both_actions() {
        // Stubbed transmission (no real HTTP target) — must not error or panic.
        let a = Ieee2030_5Adapter::new();
        a.execute_dispatch(DispatchType::FLEX_UP, 10.0)
            .await
            .unwrap();
        a.execute_dispatch(DispatchType::FLEX_DOWN, 5.0)
            .await
            .unwrap();
    }
}
