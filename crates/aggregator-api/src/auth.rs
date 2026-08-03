use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use tracing::{info, warn};

use crate::state::{
    identity::{ApiKeyRequest, ApiKeyResponse},
    AppState,
};

/// Default seconds a *successful* API-key verification is trusted before re-checking
/// IAM. Override with `API_KEY_POS_CACHE_TTL_SECS`. Sustained ingest (a meter fleet at
/// N readings/window) would otherwise call IAM `VerifyApiKey` once per request — each
/// triggering a Redis event + a DB write — and saturate IAM, timing out unrelated
/// callers. A short positive cache bounds that to one IAM round-trip per key per TTL.
/// Trade-off: a revoked/rotated key stays accepted until its entry expires (≤ TTL).
const API_KEY_POSITIVE_TTL_SECS: u64 = 60;

/// Default seconds a *definitive IAM reject* is trusted before re-checking IAM.
/// Override with `API_KEY_NEG_CACHE_TTL_SECS`. A wrong/rotated key replayed on every
/// reading is the symmetric flood vector to a good key: each request misses the positive
/// cache and hits IAM `VerifyApiKey`. A short negative cache bounds repeated rejects to
/// one IAM round-trip per key per TTL. Kept much shorter than the positive TTL so a key
/// authorized right after a failed first attempt is picked up quickly. Only *definitive
/// IAM rejects* are cached negatively — an IAM connection error is transient and MUST
/// still fall through to the static-key fallback, so it is never cached.
const API_KEY_NEGATIVE_TTL_SECS: u64 = 10;

/// Resolve a TTL from `var` (seconds), falling back to `default_secs` when unset or
/// unparseable. Read once at first use and memoized — the cache TTLs don't change at
/// runtime, and this keeps the hot path allocation-free.
fn ttl_from_env(var: &'static str, default_secs: u64) -> Duration {
    let secs = std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

fn positive_ttl() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| ttl_from_env("API_KEY_POS_CACHE_TTL_SECS", API_KEY_POSITIVE_TTL_SECS))
}

fn negative_ttl() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| ttl_from_env("API_KEY_NEG_CACHE_TTL_SECS", API_KEY_NEGATIVE_TTL_SECS))
}

/// Optional circuit breaker over IAM `VerifyApiKey`. The per-key cache already
/// bounds IAM load while IAM is *up*; the breaker covers IAM being *down* — after
/// a run of connection failures it opens, so cache-missing requests fast-fall to
/// the static-key fallback instead of each eating the gRPC timeout. Opt-in
/// (`AGGREGATOR_IAM_CIRCUIT_BREAKER_ENABLED=true`, default off) so existing
/// behaviour is unchanged until it's canaried on. Only *connection errors* count
/// as failures — a definitive IAM reject means IAM is up and closes the breaker.
fn iam_breaker() -> Option<&'static gridtokenx_telemetry::breaker::CircuitBreaker> {
    static CB: OnceLock<Option<gridtokenx_telemetry::breaker::CircuitBreaker>> = OnceLock::new();
    CB.get_or_init(|| {
        let enabled = std::env::var("AGGREGATOR_IAM_CIRCUIT_BREAKER_ENABLED")
            .map(|v| matches!(v.trim(), "1" | "true" | "TRUE"))
            .unwrap_or(false);
        if !enabled {
            return None;
        }
        let threshold = std::env::var("AGGREGATOR_IAM_CB_FAILURE_THRESHOLD")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(5);
        let cooldown_secs = std::env::var("AGGREGATOR_IAM_CB_COOLDOWN_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(5);
        info!("⚡ IAM circuit breaker enabled (threshold={threshold}, cooldown={cooldown_secs}s)");
        Some(gridtokenx_telemetry::breaker::CircuitBreaker::new(
            threshold,
            Duration::from_secs(cooldown_secs),
        ))
    })
    .as_ref()
}

/// Normalized result of the IAM `VerifyApiKey` round-trip, collapsing the
/// gRPC `Ok`/`Err` + `valid` flag into the three cases the auth policy cares
/// about. Keeping this separate from the action keeps the reject-vs-error
/// security invariant a single, pure, unit-testable decision.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum IamVerdict {
    /// IAM affirmatively authorized the key.
    Authorized,
    /// IAM affirmatively *rejected* the key — definitive, NOT a transport
    /// failure. Must NOT fall through to the static-key fallback.
    Rejected,
    /// IAM could not be reached (connection error) or no client is configured.
    /// Transient: MUST fall through to the static-key fallback and is never
    /// cached (the next request re-checks once IAM is back).
    Unavailable,
}

/// What the middleware does after consulting IAM, before any static-key check.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum PostIam {
    /// Cache positive, run the request.
    Allow,
    /// Cache negative, return 401 — no static fallback.
    Deny,
    /// Do not cache; fall through to the static-key check.
    TryStatic,
}

/// Map an IAM verdict to the next action. Encodes the security invariant: only a
/// *connection failure* (`Unavailable`) reaches the static-key fallback — a
/// definitive IAM reject is final (401, no fallback).
fn post_iam_action(verdict: IamVerdict) -> PostIam {
    match verdict {
        IamVerdict::Authorized => PostIam::Allow,
        IamVerdict::Rejected => PostIam::Deny,
        IamVerdict::Unavailable => PostIam::TryStatic,
    }
}

/// Whether a post-IAM action should be written to the verdict cache, and with
/// what validity. `None` = don't cache (transient IAM failure must re-check).
fn cache_verdict_for(action: PostIam) -> Option<bool> {
    match action {
        PostIam::Allow => Some(true),
        PostIam::Deny => Some(false),
        PostIam::TryStatic => None,
    }
}

struct CachedAuth {
    expires: Instant,
    valid: bool,
}

fn api_key_cache() -> &'static Mutex<HashMap<String, CachedAuth>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CachedAuth>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns the cached verdict for `key` (`Some(true)` = authorized, `Some(false)` =
/// rejected) when a non-expired entry exists, else `None`.
fn cache_lookup(key: &str) -> Option<bool> {
    let mut cache = match api_key_cache().lock() {
        Ok(c) => c,
        Err(p) => p.into_inner(), // poisoned: recover, the map is still usable
    };
    let out = match cache.get(key) {
        Some(entry) if entry.expires > Instant::now() => Some(entry.valid),
        Some(_) => {
            cache.remove(key); // expired — drop it so the map doesn't grow unbounded
            None
        }
        None => None,
    };
    // Track cache effectiveness — shares aggregator_cache_lookups_total with
    // aggregator-persistence's pubkey/enckey/meter_owner caches. A falling
    // hit-rate flags key churn or a flood of distinct/bad keys hammering IAM.
    aggregator_persistence::metrics::record_cache_lookup("apikey", out.is_some());
    out
}

/// Record a verdict for `key`. Positive verdicts last `API_KEY_POSITIVE_TTL`,
/// negative verdicts the shorter `API_KEY_NEGATIVE_TTL`.
fn cache_store(key: &str, valid: bool) {
    let now = Instant::now();
    let ttl = if valid {
        positive_ttl()
    } else {
        negative_ttl()
    };
    let mut cache = match api_key_cache().lock() {
        Ok(c) => c,
        Err(p) => p.into_inner(),
    };
    // Opportunistic prune of expired entries — cheap, keeps the map bounded to the
    // set of currently-active keys (small) rather than every key ever seen.
    cache.retain(|_, v| v.expires > now);
    cache.insert(
        key.to_string(),
        CachedAuth {
            expires: now + ttl,
            valid,
        },
    );
}

pub async fn api_key_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // 1. Extract API Key (Header: X-API-KEY)
    let api_key = req
        .headers()
        .get("X-API-KEY")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let api_key = match api_key {
        Some(key) => key,
        None => {
            warn!("🚫 Missing API Key in request to: {:?}", req.uri());
            state.metrics.record_request(false, 0);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // 1b. Fast path: a recently-decided key skips the IAM round-trip entirely.
    // A cached reject short-circuits to 401 so a wrong/rotated key replayed every
    // reading can't flood IAM (symmetric to the positive fast-path below).
    match cache_lookup(&api_key) {
        Some(true) => {
            state.metrics.record_request(true, 0);
            return Ok(next.run(req).await);
        }
        Some(false) => {
            state.metrics.record_request(false, 0);
            return Err(StatusCode::UNAUTHORIZED);
        }
        None => {}
    }

    // 2. Verify with IAM Service (gRPC) if available, normalizing the outcome to
    //    an `IamVerdict` so the reject-vs-error policy is one pure, tested
    //    decision (`post_iam_action`) rather than tangled control flow.
    let breaker = iam_breaker();
    let verdict = if breaker.is_some_and(|cb| !cb.should_allow()) {
        // Breaker OPEN: IAM has been failing — skip the round-trip and fall
        // through to the static-key fallback instead of eating the gRPC timeout.
        warn!("⚡ IAM circuit breaker OPEN — skipping VerifyApiKey, using static-key fallback");
        metrics::counter!("aggregator_iam_circuit_breaker_skips_total").increment(1);
        IamVerdict::Unavailable
    } else if let Some(ref identity_client) = state.identity_client {
        let start = std::time::Instant::now();
        let request = ApiKeyRequest {
            key: api_key.clone(),
            ..Default::default()
        };

        match identity_client.verify_api_key(request).await {
            Ok(response) => {
                // IAM responded (authorized OR rejected) — it's up; close the breaker.
                if let Some(cb) = breaker {
                    cb.record_success();
                }
                let latency_us = start.elapsed().as_micros() as u64;
                let res: ApiKeyResponse = response.into_owned();
                if res.valid {
                    info!(
                        "✅ API Key authorized via IAM (Role: {}) [{}us]",
                        res.role, latency_us
                    );
                    state.metrics.record_request(true, latency_us);
                    IamVerdict::Authorized
                } else {
                    warn!(
                        "🚫 API Key rejected by IAM: {} [{}us]",
                        res.error_message, latency_us
                    );
                    state.metrics.record_request(false, latency_us);
                    IamVerdict::Rejected
                }
            }
            Err(e) => {
                // Connection error — IAM unreachable; count toward opening the breaker.
                if let Some(cb) = breaker {
                    cb.record_failure();
                }
                let latency_us = start.elapsed().as_micros() as u64;
                warn!(
                    "⚠️ IAM Service error: {} [{}us]. Falling back to static keys.",
                    e, latency_us
                );
                state.metrics.record_request(false, latency_us);
                IamVerdict::Unavailable
            }
        }
    } else {
        // No IAM client configured behaves like an unreachable IAM: fall through
        // to the static-key fallback (never a silent deny).
        IamVerdict::Unavailable
    };

    let action = post_iam_action(verdict);
    // Cache the decision per `cache_verdict_for` — positive/negative are cached
    // (bound IAM load on replays), a transient `TryStatic` is never cached.
    if let Some(valid) = cache_verdict_for(action) {
        cache_store(&api_key, valid);
    }
    match action {
        PostIam::Allow => return Ok(next.run(req).await),
        PostIam::Deny => return Err(StatusCode::UNAUTHORIZED),
        PostIam::TryStatic => {} // fall through to the static-key check below
    }

    // 3. Fallback to static keys
    if state.api_keys.iter().any(|k| k == &api_key) {
        info!("✅ API Key authorized via static fallback");
        state.metrics.record_request(true, 0);
        cache_store(&api_key, true);
        return Ok(next.run(req).await);
    }

    // Don't log the key itself — a mistyped real credential would land in logs.
    warn!("🚫 API Key not authorized (len={})", api_key.len());
    state.metrics.record_request(false, 0);
    Err(StatusCode::UNAUTHORIZED)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cache is a process-global static shared across all tests in this binary,
    // which run concurrently — each test MUST use a unique key so they don't observe
    // each other's entries.

    #[test]
    fn stored_key_is_a_cache_hit() {
        let key = "test-stored-key-unique-1";
        assert_eq!(
            cache_lookup(key),
            None,
            "unknown key must miss before store"
        );
        cache_store(key, true);
        assert_eq!(
            cache_lookup(key),
            Some(true),
            "key must hit positive immediately after store"
        );
    }

    #[test]
    fn unknown_key_is_a_miss() {
        assert_eq!(cache_lookup("test-never-stored-key-unique-2"), None);
    }

    #[test]
    fn rejected_key_is_cached_negative() {
        let key = "test-rejected-key-unique-neg";
        assert_eq!(
            cache_lookup(key),
            None,
            "unknown key must miss before store"
        );
        cache_store(key, false);
        assert_eq!(
            cache_lookup(key),
            Some(false),
            "rejected key must hit negative within TTL so it doesn't re-flood IAM"
        );
    }

    #[test]
    fn negative_ttl_is_shorter_than_positive() {
        // A rotated/authorized-late key must be re-checked sooner than a trusted one.
        // Defaults (env unset in tests) must preserve this ordering.
        assert!(
            negative_ttl() < positive_ttl(),
            "negative cache must expire faster than positive"
        );
        assert!(API_KEY_NEGATIVE_TTL_SECS < API_KEY_POSITIVE_TTL_SECS);
    }

    #[test]
    fn expired_entry_is_evicted_and_misses() {
        let key = "test-expired-key-unique-3";
        // Insert an already-expired entry directly, bypassing cache_store's TTL.
        {
            let mut cache = api_key_cache().lock().expect("cache lock");
            cache.insert(
                key.to_string(),
                CachedAuth {
                    expires: Instant::now() - Duration::from_secs(1),
                    valid: true,
                },
            );
        }
        assert_eq!(cache_lookup(key), None, "expired entry must miss");
        // And the lookup must have removed it (no unbounded growth of stale keys).
        let cache = api_key_cache().lock().expect("cache lock");
        assert!(
            !cache.contains_key(key),
            "expired entry must be evicted on lookup"
        );
    }

    // --- reject-vs-error policy (the documented security invariant) ---
    // The static-key fallback exists ONLY for a transient IAM outage. A
    // definitive IAM reject must be final (401, no fallback), or a key IAM
    // explicitly revoked could still be honored by a stale static key.

    #[test]
    fn iam_authorized_allows() {
        assert_eq!(post_iam_action(IamVerdict::Authorized), PostIam::Allow);
    }

    #[test]
    fn iam_reject_denies_and_never_tries_static() {
        let action = post_iam_action(IamVerdict::Rejected);
        assert_eq!(action, PostIam::Deny);
        assert_ne!(
            action,
            PostIam::TryStatic,
            "a definitive IAM reject must NOT fall through to the static-key fallback"
        );
    }

    #[test]
    fn iam_unavailable_falls_through_to_static() {
        // Connection error OR no client configured → both map to Unavailable,
        // which is the ONLY verdict permitted to reach the static-key fallback.
        assert_eq!(post_iam_action(IamVerdict::Unavailable), PostIam::TryStatic);
    }

    #[test]
    fn reject_is_cached_but_transient_failure_is_not() {
        // Allow/Deny are cached (bound IAM load on replays); a transient
        // TryStatic must NOT be cached, or one IAM blip would pin a key to the
        // static path until the (uncached) entry it never wrote expired.
        assert_eq!(cache_verdict_for(PostIam::Allow), Some(true));
        assert_eq!(cache_verdict_for(PostIam::Deny), Some(false));
        assert_eq!(
            cache_verdict_for(PostIam::TryStatic),
            None,
            "a transient IAM failure must never be cached"
        );
    }

    #[test]
    fn store_prunes_expired_entries() {
        let live = "test-live-key-unique-4";
        let stale = "test-stale-key-unique-4";
        {
            let mut cache = api_key_cache().lock().expect("cache lock");
            cache.insert(
                stale.to_string(),
                CachedAuth {
                    expires: Instant::now() - Duration::from_secs(1),
                    valid: true,
                },
            );
        }
        cache_store(live, true); // retain() in cache_store should drop the stale entry
        let cache = api_key_cache().lock().expect("cache lock");
        assert!(cache.contains_key(live));
        assert!(
            !cache.contains_key(stale),
            "cache_store must prune expired entries"
        );
    }
}
