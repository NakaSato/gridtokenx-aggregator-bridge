use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::streams::StreamMaxlen;
use redis::AsyncCommands;
use tracing::info;

use crate::models::DeviceReading;

/// Default maximum number of entries per Redis stream.
const DEFAULT_MAX_STREAM_LEN: usize = 100_000;

/// Routes normalized `DeviceReading` events to the appropriate
/// Redis Stream based on `device_type`.
pub struct Router {
    connection_manager: ConnectionManager,
    /// Approximate cap for each stream (MAXLEN ~).
    max_stream_len: usize,
}

impl Router {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        let max_stream_len: usize = std::env::var("REDIS_STREAM_MAXLEN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_STREAM_LEN);

        info!("📏 Redis stream MAXLEN cap: ~{}", max_stream_len);

        Ok(Self {
            connection_manager,
            max_stream_len,
        })
    }

    /// Publish a normalized reading to the correct downstream stream.
    pub async fn disseminate(&self, reading: &DeviceReading) -> Result<String> {
        let mut conn = self.connection_manager.clone();
        let stream_name = reading.device_type.target_stream();

        // Wrap in an event envelope matching the existing EventBus format
        let event_envelope = serde_json::json!({
            "event_type": self.event_type_name(&reading),
            "payload": reading,
        });

        let json = serde_json::to_string(&event_envelope)
            .context("Failed to serialize reading")?;

        let stream_id: String = conn
            .xadd_maxlen(
                stream_name,
                StreamMaxlen::Approx(self.max_stream_len),
                "*",
                &[("event", &json)],
            )
            .await
            .context("Failed to publish to Redis Stream")?;

        info!(
            "📤 Disseminated {:?} {} → {} (ID: {})",
            reading.device_type,
            reading.serial_number,
            stream_name,
            stream_id
        );

        Ok(stream_id)
    }

    fn event_type_name(&self, reading: &DeviceReading) -> &'static str {
        match reading.device_type {
            crate::models::DeviceType::SmartMeter => "SmartMeterReading",
            crate::models::DeviceType::EvCharger => "EvChargingEvent",
            crate::models::DeviceType::Battery => "BatteryStateUpdate",
        }
    }
}
