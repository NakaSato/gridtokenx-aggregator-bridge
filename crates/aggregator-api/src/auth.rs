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

/// How long a *successful* API-key verification is trusted before re-checking IAM.
/// Sustained ingest (e.g. a meter fleet at N readings/window) would otherwise call
/// IAM `VerifyApiKey` once per request — each triggering a Redis event + a DB write —
/// and saturate IAM, timing out unrelated callers. A short positive cache bounds
/// that to one IAM round-trip per key per TTL. Trade-off: a revoked/rotated key stays
/// accepted until its entry expires (≤ TTL).
const API_KEY_POSITIVE_TTL: Duration = Duration::from_secs(60);

/// How long a *definitive IAM reject* is trusted before re-checking IAM.
/// A wrong/rotated key replayed on every reading is the symmetric flood vector to a
/// good key: each request misses the positive cache and hits IAM `VerifyApiKey`. A
/// short negative cache bounds repeated rejects to one IAM round-trip per key per TTL.
/// Kept much shorter than the positive TTL so a key that gets authorized right after a
/// failed first attempt is picked up quickly. Only *definitive IAM rejects* are cached
/// negatively — an IAM connection error is transient and MUST still fall through to the
/// static-key fallback, so it is never cached.
const API_KEY_NEGATIVE_TTL: Duration = Duration::from_secs(10);

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
    match cache.get(key) {
        Some(entry) if entry.expires > Instant::now() => Some(entry.valid),
        Some(_) => {
            cache.remove(key); // expired — drop it so the map doesn't grow unbounded
            None
        }
        None => None,
    }
}

/// Record a verdict for `key`. Positive verdicts last `API_KEY_POSITIVE_TTL`,
/// negative verdicts the shorter `API_KEY_NEGATIVE_TTL`.
fn cache_store(key: &str, valid: bool) {
    let now = Instant::now();
    let ttl = if valid {
        API_KEY_POSITIVE_TTL
    } else {
        API_KEY_NEGATIVE_TTL
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

    // 2. Verify with IAM Service (gRPC) if available
    if let Some(ref identity_client) = state.identity_client {
        let start = std::time::Instant::now();
        let request = ApiKeyRequest {
            key: api_key.clone(),
            ..Default::default()
        };

        match identity_client.verify_api_key(request).await {
            Ok(response) => {
                let latency_us = start.elapsed().as_micros() as u64;
                let res: ApiKeyResponse = response.into_owned();
                if res.valid {
                    info!(
                        "✅ API Key authorized via IAM (Role: {}) [{}us]",
                        res.role, latency_us
                    );
                    state.metrics.record_request(true, latency_us);
                    cache_store(&api_key, true);
                    return Ok(next.run(req).await);
                } else {
                    warn!(
                        "🚫 API Key rejected by IAM: {} [{}us]",
                        res.error_message, latency_us
                    );
                    state.metrics.record_request(false, latency_us);
                    // Definitive IAM reject: cache it briefly so a replayed bad key
                    // doesn't re-hit IAM every reading. NOT cached on the Err branch
                    // below — that is a transient connection failure that must still
                    // fall through to the static-key fallback.
                    cache_store(&api_key, false);
                    return Err(StatusCode::UNAUTHORIZED);
                }
            }
            Err(e) => {
                let latency_us = start.elapsed().as_micros() as u64;
                warn!(
                    "⚠️ IAM Service error: {} [{}us]. Falling back to static keys.",
                    e, latency_us
                );
                state.metrics.record_request(false, latency_us);
                // Fall through to static key check
            }
        }
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
        assert_eq!(cache_lookup(key), None, "unknown key must miss before store");
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
        assert_eq!(cache_lookup(key), None, "unknown key must miss before store");
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
        assert!(
            API_KEY_NEGATIVE_TTL < API_KEY_POSITIVE_TTL,
            "negative cache must expire faster than positive"
        );
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
