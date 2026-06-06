//! oracle-core — pure domain layer for the Oracle Bridge.
//!
//! No async, no I/O, no transport. Holds device/reading domain types and
//! fixed-point numeric helpers shared across the edge/persistence/logic crates.

pub mod models;
pub mod numeric;
