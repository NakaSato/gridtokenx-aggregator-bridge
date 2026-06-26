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
/// and saturate IAM, timing out unrelated callers. A short positive-only cache bounds
/// that to one IAM round-trip per key per TTL. Trade-off: a revoked/rotated key stays
/// accepted until its entry expires (≤ TTL). Rejections are NEVER cached, so a freshly
/// authorized key is picked up immediately.
const API_KEY_CACHE_TTL: Duration = Duration::from_secs(60);

struct CachedAuth {
    expires: Instant,
}

fn api_key_cache() -> &'static Mutex<HashMap<String, CachedAuth>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CachedAuth>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns true if `key` has a non-expired positive verdict cached.
fn cache_hit(key: &str) -> bool {
    let mut cache = match api_key_cache().lock() {
        Ok(c) => c,
        Err(p) => p.into_inner(), // poisoned: recover, the map is still usable
    };
    match cache.get(key) {
        Some(entry) if entry.expires > Instant::now() => true,
        Some(_) => {
            cache.remove(key); // expired — drop it so the map doesn't grow unbounded
            false
        }
        None => false,
    }
}

/// Record a positive verdict for `key`, valid for `API_KEY_CACHE_TTL`.
fn cache_store(key: &str) {
    let now = Instant::now();
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
            expires: now + API_KEY_CACHE_TTL,
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

    // 1b. Fast path: a recently-verified key skips the IAM round-trip entirely.
    if cache_hit(&api_key) {
        state.metrics.record_request(true, 0);
        return Ok(next.run(req).await);
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
                    cache_store(&api_key);
                    return Ok(next.run(req).await);
                } else {
                    warn!(
                        "🚫 API Key rejected by IAM: {} [{}us]",
                        res.error_message, latency_us
                    );
                    state.metrics.record_request(false, latency_us);
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
        cache_store(&api_key);
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
        assert!(!cache_hit(key), "unknown key must miss before store");
        cache_store(key);
        assert!(cache_hit(key), "key must hit immediately after store");
    }

    #[test]
    fn unknown_key_is_a_miss() {
        assert!(!cache_hit("test-never-stored-key-unique-2"));
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
                },
            );
        }
        assert!(!cache_hit(key), "expired entry must miss");
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
                },
            );
        }
        cache_store(live); // retain() in cache_store should drop the stale entry
        let cache = api_key_cache().lock().expect("cache lock");
        assert!(cache.contains_key(live));
        assert!(
            !cache.contains_key(stale),
            "cache_store must prune expired entries"
        );
    }
}
