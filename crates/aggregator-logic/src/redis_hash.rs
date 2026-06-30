//! Shared `HGETALL`-decode helper for the durable Redis hash stores
//! ([`crate::bin_store`], [`crate::mint_outbox`]) — both decode a
//! `HashMap<String, String>` into a `Vec<T>`, skipping (with a `warn!`) any
//! value that fails to deserialize rather than aborting the whole restore.

use std::collections::HashMap;
use std::fmt::Display;

use serde::de::DeserializeOwned;
use tracing::warn;

/// Decodes an `HGETALL` map into `T`, dropping unparsable entries. `label`
/// names the store in the warn log (e.g. `"durable bin store"`,
/// `"mint outbox"`). Pure — unit-testable without a live Redis.
pub fn parse_redis_hash<T: DeserializeOwned>(
    map: HashMap<String, String>,
    label: impl Display,
) -> Vec<T> {
    map.into_iter()
        .filter_map(|(field, json)| match serde_json::from_str::<T>(&json) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!("{label}: dropping unparsable entry {field}: {e}");
                None
            }
        })
        .collect()
}
