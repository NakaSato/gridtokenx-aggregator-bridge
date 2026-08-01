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

/// Readings the `meter_readings` sink could not attribute to an owner, and so did
/// not persist at all.
///
/// The insert joins the owner projection and keeps only rows with a wallet, which
/// means telemetry from a meter with no registry row is accepted at ingest, counted
/// nowhere, and silently dropped at write time — the reading leaves no trace but a
/// log line. That is how 16 unregistered simulator meters streamed for days while
/// only their (unmintable) surplus bins hinted anything was wrong.
///
/// `aggregator_readings_unattributed_total` is the signal to alert on: sustained
/// non-zero means meters are producing telemetry nobody owns. It is a counter, not a
/// log, precisely because the condition persists until a human registers the meter.
pub fn record_unattributed_readings(dropped: u64) {
    if dropped > 0 {
        metrics::counter!("aggregator_readings_unattributed_total").increment(dropped);
    }
}
