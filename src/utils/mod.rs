//! `numeric` now lives in the `oracle-core` crate; re-exported so existing
//! `crate::utils::numeric::*` paths keep resolving during the workspace split.
pub use oracle_core::numeric;
