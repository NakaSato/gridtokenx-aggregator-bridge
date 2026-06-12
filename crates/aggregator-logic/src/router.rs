use anyhow::{anyhow, Context, Result};
use redis::aio::ConnectionManager;
use redis::streams::StreamMaxlen;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use aggregator_core::models::DeviceReading;

/// Default maximum number of entries per Redis stream.
const DEFAULT_MAX_STREAM_LEN: usize = 100_000;

use gridtokenx_blockchain_core::rpc::nats_schema::MeterReadingMessage;

/// Routes normalized `DeviceReading` events to zone-partitioned Redis Streams.
pub struct Router {
    /// Redis URL used to rebuild the connection after a server restart.
    redis_url: String,
    /// Cached reconnecting manager; rebuilt on transport error so a Redis
    /// restart is recovered inline rather than failing the first request.
    conn: Arc<Mutex<Option<ConnectionManager>>>,
    /// Approximate cap for each stream (MAXLEN ~).
    max_stream_len: usize,
    /// Number of zone partitions
    num_zones: usize,
    /// Optional NATS client for forwarding direct chain-bridge ingestion messages
    nats_client: Option<async_nats::Client>,
}

impl Router {
    pub async fn new(redis_url: &str, num_zones: usize, nats_client: Option<async_nats::Client>) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        let max_stream_len: usize = std::env::var("REDIS_STREAM_MAXLEN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_STREAM_LEN);

        info!("📏 Redis stream MAXLEN cap: ~{}", max_stream_len);
        info!("🔢 Zone partitions: {}", num_zones);

        Ok(Self {
            redis_url: redis_url.to_string(),
            conn: Arc::new(Mutex::new(Some(connection_manager))),
            max_stream_len,
            num_zones,
            nats_client,
        })
    }

    /// Return a live connection manager clone, rebuilding from `redis_url` after
    /// an [`invalidate`](Self::invalidate). Errors loudly when Redis is down.
    async fn conn(&self) -> Result<ConnectionManager> {
        {
            let guard = self.conn.lock().await;
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let client = redis::Client::open(self.redis_url.as_str())
            .map_err(|e| anyhow!("Failed to open Redis client {}: {}", self.redis_url, e))?;
        let mgr = ConnectionManager::new(client)
            .await
            .map_err(|e| anyhow!("Failed to connect to Redis {}: {}", self.redis_url, e))?;
        let mut guard = self.conn.lock().await;
        *guard = Some(mgr.clone());
        Ok(mgr)
    }

    /// Drop the cached manager so the next [`conn`](Self::conn) rebuilds it.
    async fn invalidate(&self) {
        let mut guard = self.conn.lock().await;
        *guard = None;
    }

    /// Determine zone index for a reading
    fn get_zone_index(&self, reading: &DeviceReading) -> usize {
        calculate_zone_index(self.num_zones, reading)
    }

    /// Publish a normalized reading to a zone-partitioned stream.
    pub async fn disseminate(&self, reading: &DeviceReading) -> Result<String> {
        let mut conn = self.conn().await?;

        // Route to zone-specific stream
        let zone_idx = self.get_zone_index(reading);
        let stream_name = format!("gridtokenx:events:zone_{}", zone_idx);

        let event_envelope = serde_json::json!({
            "event_type": self.event_type_name(reading),
            "payload": reading,
        });

        let json = serde_json::to_string(&event_envelope).context("Failed to serialize reading")?;

        let stream_id: String = match conn
            .xadd_maxlen(
                &stream_name,
                StreamMaxlen::Approx(self.max_stream_len),
                "*",
                &[("event", &json)],
            )
            .await
        {
            Ok(id) => id,
            Err(e) => {
                // Transport error (e.g. Redis restarted) — rebuild and retry once
                // so the request succeeds inline instead of returning HTTP 500.
                warn!(
                    "⚠️ Redis XADD error for {} ({}); rebuilding connection and retrying",
                    stream_name, e
                );
                self.invalidate().await;
                let mut conn2 = self.conn().await?;
                let id: String = conn2
                    .xadd_maxlen(
                        &stream_name,
                        StreamMaxlen::Approx(self.max_stream_len),
                        "*",
                        &[("event", &json)],
                    )
                    .await
                    .context("Failed to publish to Redis Stream after reconnect")?;
                conn = conn2;
                id
            }
        };

        // Also publish to unified stream for general consumers (e.g. trading-service)
        let unified_stream = reading.device_type.target_stream();
        if unified_stream != stream_name {
            let _: Result<String, _> = conn
                .xadd_maxlen(
                    unified_stream,
                    StreamMaxlen::Approx(self.max_stream_len),
                    "*",
                    &[("event", &json)],
                )
                .await;
        }

        info!(
            "📤 Disseminated {:?} {} → {} (ID: {})",
            reading.device_type, reading.serial_number, stream_name, stream_id
        );

        // Option A: Forward telemetry directly to NATS for chain-bridge ingestion
        if let Some(nats) = &self.nats_client {
            if reading.device_type == aggregator_core::models::DeviceType::SmartMeter {
                let net = match reading.metrics {
                    aggregator_core::models::DeviceMetrics::Energy { net_kwh, .. } => net_kwh,
                    _ => 0.0,
                };
                let nats_payload = MeterReadingMessage {
                    device_id: reading.device_id.clone(),
                    wallet_address: reading.serial_number.clone(), // using serial_number as wallet/device correlation
                    energy_kwh: net,
                    timestamp_ms: reading.timestamp.timestamp_millis() as u64,
                };
                
                match serde_json::to_vec(&nats_payload) {
                    Ok(payload_bytes) => {
                        if let Err(e) = nats.publish("meter.reading.mint".to_string(), payload_bytes.into()).await {
                            tracing::error!("Failed to publish to NATS meter.reading.mint: {}", e);
                        } else {
                            let _ = nats.flush().await;
                            tracing::info!("📤 Also forwarded to NATS stream: meter.reading.mint");
                        }
                    }
                    Err(e) => tracing::error!("Failed to serialize NATS payload: {}", e),
                }
            }
        }

        Ok(stream_id)
    }

    fn event_type_name(&self, reading: &DeviceReading) -> &'static str {
        match reading.device_type {
            aggregator_core::models::DeviceType::SmartMeter => "SmartMeterReading",
            aggregator_core::models::DeviceType::EvCharger => "EvCharging",
            aggregator_core::models::DeviceType::Battery => "BatteryStateUpdate",
        }
    }
}

/// Determine zone index for a reading based on zone_code or serial_number hashing
fn calculate_zone_index(num_zones: usize, reading: &DeviceReading) -> usize {
    match &reading.zone_code {
        Some(zcode) => {
            // Try to parse numerical suffix from zone code (e.g. "ZONE5" -> 5)
            let suffix: String = zcode.chars().skip_while(|c| !c.is_ascii_digit()).collect();
            if !suffix.is_empty() {
                if let Ok(idx) = suffix.parse::<usize>() {
                    if idx < num_zones {
                        return idx;
                    }
                }
            }

            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            zcode.hash(&mut hasher);
            hasher.finish() as usize % num_zones
        }
        _ => {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            reading.serial_number.hash(&mut hasher);
            hasher.finish() as usize % num_zones
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aggregator_core::models::{DeviceMetrics, DeviceType};
    use chrono::Utc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_router_hashing_consistency() {
        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: "DEV-123".to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: "SN-999".to_string(),
            zone_code: None, // Force hashing
            timestamp: Utc::now(),
            metrics: DeviceMetrics::Energy {
                generated_kwh: 10.0,
                consumed_kwh: 5.0,
                net_kwh: 5.0,
            },
            metadata: std::collections::HashMap::new(),
        };

        let idx_10 = calculate_zone_index(10, &reading);
        assert!(idx_10 < 10);

        let idx_20 = calculate_zone_index(20, &reading);
        assert!(idx_20 < 20);

        // Note: Hashing results might differ between 10 and 20, which is expected.
        // The test ensures they are within bounds.
    }

    #[tokio::test]
    async fn test_router_explicit_zone_id() {
        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: "DEV-123".to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: "SN-999".to_string(),
            zone_code: Some("ZONE5".to_string()),
            timestamp: Utc::now(),
            metrics: DeviceMetrics::Energy {
                generated_kwh: 10.0,
                consumed_kwh: 5.0,
                net_kwh: 5.0,
            },
            metadata: std::collections::HashMap::new(),
        };

        assert_eq!(calculate_zone_index(10, &reading), 5);

        // If zone_id is out of bounds, it should fallback to hashing
        let reading_out_of_bounds = DeviceReading {
            zone_code: Some("ZONE15".to_string()),
            ..reading
        };
        assert!(calculate_zone_index(10, &reading_out_of_bounds) < 10);
    }
}
