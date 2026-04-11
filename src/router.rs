use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::streams::StreamMaxlen;
use redis::AsyncCommands;
use tracing::info;

use crate::models::DeviceReading;

/// Default maximum number of entries per Redis stream.
const DEFAULT_MAX_STREAM_LEN: usize = 100_000;

/// Routes normalized `DeviceReading` events to zone-partitioned Redis Streams.
pub struct Router {
    connection_manager: ConnectionManager,
    /// Approximate cap for each stream (MAXLEN ~).
    max_stream_len: usize,
    /// Number of zone partitions
    num_zones: usize,
}

impl Router {
    pub async fn new(redis_url: &str, num_zones: usize) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        let max_stream_len: usize = std::env::var("REDIS_STREAM_MAXLEN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_STREAM_LEN);

        info!("📏 Redis stream MAXLEN cap: ~{}", max_stream_len);
        info!("🔢 Zone partitions: {}", num_zones);

        Ok(Self {
            connection_manager,
            max_stream_len,
            num_zones,
        })
    }

    /// Determine zone index for a reading
    fn get_zone_index(&self, reading: &DeviceReading) -> usize {
        match reading.zone_id {
            Some(zid) if zid >= 0 && (zid as usize) < self.num_zones => zid as usize,
            _ => {
                // Hash serial number to zone for consistent partitioning
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                reading.serial_number.hash(&mut hasher);
                hasher.finish() as usize % self.num_zones
            }
        }
    }

    /// Publish a normalized reading to a zone-partitioned stream.
    pub async fn disseminate(&self, reading: &DeviceReading) -> Result<String> {
        let mut conn = self.connection_manager.clone();
        
        // Route to zone-specific stream
        let zone_idx = self.get_zone_index(reading);
        let stream_name = format!("gridtokenx:events:zone_{}", zone_idx);

        // Map DeviceMetrics to flattened payload for api-services Compatibility
        let (generated, consumed, net) = match reading.metrics {
            crate::models::DeviceMetrics::Energy { generated_kwh, consumed_kwh, net_kwh } => (Some(generated_kwh), Some(consumed_kwh), net_kwh),
            _ => (None, None, 0.0),
        };

        // Construct payload matching api-services::domain::events::MeterReadingPayload
        let event_payload = serde_json::json!({
            "reading_id": reading.reading_id,
            "meter_id": reading.device_id,
            "meter_serial": reading.serial_number,
            "user_id": "00000000-0000-0000-0000-000000000000", // Placeholder for persistence worker
            "wallet_address": reading.serial_number, 
            "zone_id": reading.zone_id,
            "kwh": net,
            "energy_generated": generated,
            "energy_consumed": consumed,
            "voltage": reading.metadata.get("voltage_v"),
            "current": reading.metadata.get("current_a"),
            "battery_level": reading.metadata.get("battery_level_pct"),
            "temperature": reading.metadata.get("temperature_c"),
            "metadata": reading.metadata,
            "timestamp": reading.timestamp,
        });

        let event_envelope = serde_json::json!({
            "event_type": "MeterReadingCreated",
            "payload": event_payload,
        });

        let json = serde_json::to_string(&event_envelope)
            .context("Failed to serialize reading")?;

        let stream_id: String = conn
            .xadd_maxlen(
                &stream_name,
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
            crate::models::DeviceType::EvCharger => "EvCharging",
            crate::models::DeviceType::Battery => "BatteryStateUpdate",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DeviceType, DeviceMetrics};
    use chrono::Utc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_router_hashing_consistency() {
        let router_10 = Router {
            connection_manager: redis::aio::ConnectionManager::new(redis::Client::open("redis://127.0.0.1").unwrap()).await.unwrap(),
            max_stream_len: 1000,
            num_zones: 10,
        };

        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: "DEV-123".to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: "SN-999".to_string(),
            zone_id: None, // Force hashing
            timestamp: Utc::now(),
            metrics: DeviceMetrics::Energy { generated_kwh: 10.0, consumed_kwh: 5.0, net_kwh: 5.0 },
            metadata: std::collections::HashMap::new(),
        };

        let idx_10 = router_10.get_zone_index(&reading);
        assert!(idx_10 < 10);

        let router_20 = Router {
            connection_manager: router_10.connection_manager.clone(),
            max_stream_len: 1000,
            num_zones: 20,
        };

        let idx_20 = router_20.get_zone_index(&reading);
        assert!(idx_20 < 20);
        
        // Note: Hashing results might differ between 10 and 20, which is expected.
        // The test ensures they are within bounds.
    }

    #[tokio::test]
    async fn test_router_explicit_zone_id() {
        let router = Router {
            connection_manager: redis::aio::ConnectionManager::new(redis::Client::open("redis://127.0.0.1").unwrap()).await.unwrap(),
            max_stream_len: 1000,
            num_zones: 10,
        };

        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: "DEV-123".to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: "SN-999".to_string(),
            zone_id: Some(5),
            timestamp: Utc::now(),
            metrics: DeviceMetrics::Energy { generated_kwh: 10.0, consumed_kwh: 5.0, net_kwh: 5.0 },
            metadata: std::collections::HashMap::new(),
        };

        assert_eq!(router.get_zone_index(&reading), 5);
        
        // If zone_id is out of bounds, it should fallback to hashing
        let reading_out_of_bounds = DeviceReading {
            zone_id: Some(15),
            ..reading
        };
        assert!(router.get_zone_index(&reading_out_of_bounds) < 10);
    }
}
