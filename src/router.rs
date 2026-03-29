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
        let event_type = self.event_type_name(&reading);
        
        let event_envelope = if reading.device_type == crate::models::DeviceType::SmartMeter {
            let (gen, con, net) = match reading.metrics {
                crate::models::DeviceMetrics::Energy { generated_kwh, consumed_kwh, net_kwh } => 
                    (Some(generated_kwh), Some(consumed_kwh), net_kwh),
                _ => (None, None, 0.0),
            };

            serde_json::json!({
                "event_type": event_type,
                "payload": {
                    "reading_id": reading.reading_id,
                    "meter_id": uuid::Uuid::nil(), // Placeholder for bridge readings
                    "meter_serial": reading.serial_number,
                    "user_id": uuid::Uuid::nil(), // Placeholder
                    "wallet_address": reading.metadata.get("wallet_address").and_then(|v| v.as_str()).unwrap_or(""),
                    "zone_id": reading.zone_id,
                    "kwh": net,
                    "energy_generated": gen,
                    "energy_consumed": con,
                    "voltage": reading.metadata.get("voltage").and_then(|v| v.as_f64()),
                    "current": reading.metadata.get("current").and_then(|v| v.as_f64()),
                    "battery_level": reading.metadata.get("battery_level").and_then(|v| v.as_f64()),
                    "temperature": reading.metadata.get("temperature").and_then(|v| v.as_f64()),
                    "timestamp": reading.timestamp,
                }
            })
        } else {
            serde_json::json!({
                "event_type": event_type,
                "payload": reading,
            })
        };

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
            crate::models::DeviceType::SmartMeter => "MeterReadingCreated",
            crate::models::DeviceType::EvCharger => "EvChargingEvent",
            crate::models::DeviceType::Battery => "BatteryStateUpdate",
        }
    }
}
