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

#[allow(dead_code)]
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
        return Ok(next.run(req).await);
    }

    warn!("🚫 API Key not authorized: {}", api_key);
    state.metrics.record_request(false, 0);
    Err(StatusCode::UNAUTHORIZED)
}
