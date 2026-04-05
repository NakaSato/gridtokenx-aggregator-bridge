use anyhow::Result;
use crate::storage::CircularBuffer;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};
use reqwest::Client;
use serde_json::Value;

/// SyncManager handles the replay of unsynced telemetry from the local buffer to the API Gateway.
pub struct SyncManager {
    buffer: Arc<Mutex<CircularBuffer>>,
    api_gateway_url: String,
    client: Client,
}

impl SyncManager {
    pub fn new(buffer: Arc<Mutex<CircularBuffer>>, api_gateway_url: &str) -> Self {
        Self {
            buffer,
            api_gateway_url: api_gateway_url.to_string(),
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

            info!("📤 Syncing {} unsynced telemetry records...", unsynced.len());
            
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

    async fn send_to_gateway(&self, meter_id: &str, timestamp: chrono::DateTime<chrono::Utc>, payload: &Value) -> Result<()> {
        let url = format!("{}/api/v1/telemetry/replay", self.api_gateway_url);
        
        let response = self.client.post(&url)
            .json(&serde_json::json!({
                "meter_id": meter_id,
                "timestamp": timestamp,
                "payload": payload,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Gateway returned unexpected status: {}", response.status()));
        }

        Ok(())
    }
}
