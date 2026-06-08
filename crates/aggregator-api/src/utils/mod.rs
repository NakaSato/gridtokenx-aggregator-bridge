//! `numeric` now lives in the `aggregator-core` crate; re-exported so existing
//! `crate::utils::numeric::*` paths keep resolving during the workspace split.
pub use aggregator_core::numeric;
