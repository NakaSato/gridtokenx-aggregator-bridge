//! Aggregator Bridge API / application layer.
//!
//! HTTP + ConnectRPC transport, ingestion pipeline, auth/middleware, telemetry,
//! and the `AppState` wiring hub. Depends inward on the logic/persistence/
//! protocol/stacks/core crates.
//!
//! The re-exports below let the modules in this crate keep using their original
//! `crate::{models,protocol,infra,aggregator,...}` paths unchanged after the
//! workspace split.

pub use aggregator_core::models;
pub use aggregator_logic::{
    aggregator, billing_sink, dispatch, grid_status, metrics, router, standards,
};
pub use aggregator_persistence::infra;
pub use aggregator_stacks as protocol;

pub mod auth;
pub mod grpc;
pub mod handlers;
pub mod ingester;
pub mod middleware;
pub mod state;
pub mod telemetry;
pub mod utils;
