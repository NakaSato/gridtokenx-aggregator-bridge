//! Aggregator Bridge domain services & orchestration logic.
//!
//! AppState-free building blocks consumed by the API/binary layer:
//! - `aggregator`: 15-minute energy aggregation windows
//! - `bin_store`: durable (Redis) billing-bin store for crash recovery
//! - `mint_outbox`: durable (Redis) outbox of unsettled surplus mints (retry until on-chain)
//! - `router`: reading dispatch routing
//! - `dispatch`: VPP flex dispatch engine + ConnectRPC dispatch client
//! - `grid_status`: rolling grid-frequency window from meter telemetry
//! - `standards`: IEEE 2030.5 / OpenADR 3 (OpenLEADR) standard handling
//! - `metrics`: Prometheus metrics wiring

pub mod aggregator;
pub mod billing_sink;
pub mod bin_store;
pub mod dispatch;
pub mod grid_status;
pub mod metrics;
pub mod mint_outbox;
pub mod router;
pub mod standards;
