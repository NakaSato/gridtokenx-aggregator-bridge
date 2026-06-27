use crate::storage::CircularBuffer;
use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Replay endpoint for unsynced telemetry, derived from the gateway base URL.
/// Pure so the path can be pinned in tests without an HTTP round-trip.
fn replay_url(base: &str) -> String {
    format!("{}/api/v1/telemetry/replay", base)
}

/// JSON body for a single replayed reading. Pure — kept in sync with the
/// gateway's `/api/v1/telemetry/replay` contract.
fn replay_body(meter_id: &str, timestamp: chrono::DateTime<chrono::Utc>, payload: &Value) -> Value {
    serde_json::json!({
        "meter_id": meter_id,
        "timestamp": timestamp,
        "payload": payload,
    })
}

/// SyncManager handles the replay of unsynced telemetry from the local buffer to the API Gateway.
pub struct SyncManager {
    buffer: Arc<Mutex<CircularBuffer>>,
    api_services_url: String,
    client: Client,
}

impl SyncManager {
    pub fn new(buffer: Arc<Mutex<CircularBuffer>>, api_services_url: &str) -> Self {
        Self {
            buffer,
            api_services_url: api_services_url.to_string(),
            client: Client::new(),
        }
    }

    /// Run the sync loop to periodically check for unsynced data and replay it.
    pub async fn run(&self) -> Result<()> {
        info!("🔄 Starting Sync Manager replay loop...");

        loop {
            let unsynced = {
                let buffer = self.buffer.lock().await;
                buffer.get_unsynced(50)? // Sync in batches of 50
            };

            if unsynced.is_empty() {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }

            info!(
                "📤 Syncing {} unsynced telemetry records...",
                unsynced.len()
            );

            let mut synced_ids = Vec::new();
            for (id, meter_id, ts, payload) in unsynced {
                match self.send_to_gateway(&meter_id, ts, &payload).await {
                    Ok(_) => synced_ids.push(id),
                    Err(e) => {
                        warn!("⚠️ Sync failed for record {}: {}. Retrying later...", id, e);
                        break; // Stop batch and retry later
                    }
                }
            }

            if !synced_ids.is_empty() {
                let mut buffer = self.buffer.lock().await;
                buffer.mark_as_synced(&synced_ids)?;
            }
        }
    }

    async fn send_to_gateway(
        &self,
        meter_id: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
        payload: &Value,
    ) -> Result<()> {
        let url = replay_url(&self.api_services_url);

        let response = self
            .client
            .post(&url)
            .json(&replay_body(meter_id, timestamp, payload))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Gateway returned unexpected status: {}",
                response.status()
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    // `run`/`send_to_gateway` drive a real HTTP gateway, so the unit tests cover
    // the pure request-shaping (`replay_url`/`replay_body`) that defines the
    // gateway contract. The send path itself is exercised by the superproject e2e.

    #[test]
    fn replay_url_appends_endpoint_path() {
        assert_eq!(
            replay_url("http://gateway:4000"),
            "http://gateway:4000/api/v1/telemetry/replay"
        );
    }

    #[test]
    fn replay_body_has_expected_contract_shape() {
        let ts = chrono::Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let payload = json!({"kwh": 2.5, "nested": {"a": 1}});
        let body = replay_body("METER-1", ts, &payload);

        assert_eq!(body["meter_id"], "METER-1");
        assert_eq!(body["payload"], payload);
        // timestamp serializes to RFC3339 (chrono's serde repr).
        assert_eq!(body["timestamp"], "2023-11-14T22:13:20Z");
    }

    #[test]
    fn replay_body_preserves_arbitrary_payload_json() {
        let ts = chrono::Utc.timestamp_opt(0, 0).unwrap();
        let payload = json!([1, 2, {"x": true}]);
        let body = replay_body("m", ts, &payload);
        assert_eq!(body["payload"], payload);
    }
}
