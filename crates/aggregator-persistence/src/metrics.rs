//! Shared cache-lookup metric — a single hit/miss counter emitter used by
//! every hot-cache in this crate (pubkey, enckey, meter_owner) plus the
//! API-key cache in `aggregator-api` (which depends on this crate), so the
//! `aggregator_cache_lookups_total{cache,result}` label shape only exists in
//! one place instead of drifting across independent inline copies.

/// Emit a hot-cache lookup outcome. `cache` names the cache (`"pubkey"`,
/// `"enckey"`, `"meter_owner"`, `"apikey"`, ...); `hit` = served from cache,
/// `miss` = fell through to the backing store. Feeds
/// `aggregator_cache_lookups_total{cache,result}` — a falling hit-rate flags
/// key churn, revocation activity, or a flood of distinct/bad ids.
pub fn record_cache_lookup(cache: &'static str, hit: bool) {
    metrics::counter!(
        "aggregator_cache_lookups_total",
        "cache" => cache,
        "result" => if hit { "hit" } else { "miss" },
    )
    .increment(1);
}
