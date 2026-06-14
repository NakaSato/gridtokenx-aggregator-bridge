//! Telemetry for the Aggregator Bridge.
//!
//! Thin re-export of the shared [`gridtokenx_telemetry`] crate so existing
//! `telemetry::init_telemetry(...)` / `telemetry::shutdown_telemetry(...)` call
//! sites keep resolving after the per-service copies were unified.

pub use gridtokenx_telemetry::{init_telemetry, shutdown_telemetry, TelemetryGuard};

/// NTP time source (Cloudflare/Google primary) — see [`gridtokenx_telemetry::time`].
pub use gridtokenx_telemetry::time;
