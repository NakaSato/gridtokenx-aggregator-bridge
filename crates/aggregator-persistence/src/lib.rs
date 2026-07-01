//! Aggregator Bridge persistence & external adapters.
//!
//! - `infra`: external system adapters (Kafka, RabbitMQ, Redis, crypto signer/verifier,
//!   meter registry, ConnectRPC platform client).
//! - `storage`: local time-series buffering & sync (circular buffer, sqlite sync manager).

pub mod infra;
pub mod metrics;
pub mod storage;
