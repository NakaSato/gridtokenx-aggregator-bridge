//! Aggregator Bridge domain services & orchestration logic.
//!
//! AppState-free building blocks consumed by the API/binary layer:
//! - `aggregator`: 15-minute energy aggregation windows
//! - `router`: reading dispatch routing
//! - `dispatch`: VPP flex dispatch engine + ConnectRPC dispatch client
//! - `grid_status`: rolling grid-frequency window from meter telemetry
//! - `standards`: IEEE 2030.5 / OpenADR 3 (OpenLEADR) standard handling
//! - `metrics`: Prometheus metrics wiring

pub mod aggregator;
pub mod billing_sink;
pub mod dispatch;
pub mod grid_status;
pub mod metrics;
pub mod router;
pub mod standards;
