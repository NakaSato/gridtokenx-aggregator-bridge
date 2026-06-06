//! Oracle Bridge domain services & orchestration logic.
//!
//! AppState-free building blocks consumed by the API/binary layer:
//! - `aggregator`: 15-minute energy aggregation windows
//! - `router`: reading dispatch routing
//! - `dispatch`: VPP flex dispatch engine + ConnectRPC dispatch client
//! - `standards`: IEEE 2030.5 (and related) standard handling
//! - `zk`: zero-knowledge energy attestation (circuit/prover scaffolding)
//! - `metrics`: Prometheus metrics wiring

pub mod aggregator;
pub mod dispatch;
pub mod metrics;
pub mod router;
pub mod standards;
pub mod zk;
