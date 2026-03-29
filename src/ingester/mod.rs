use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::aggregator::Aggregator;
use std::sync::atomic::AtomicI64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeterReadingPayload {
    pub reading_id: Uuid,
    pub meter_id: Uuid,
    pub meter_serial: String,
    pub user_id: Uuid,
    pub wallet_address: String,
    pub zone_id: Option<i32>,
    pub kwh: Decimal,
    pub energy_generated: Option<Decimal>,
    pub energy_consumed: Option<Decimal>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "payload")]
pub enum Event {
    // Legacy/Core Meter Reading (from API Gateway)
    MeterReadingCreated(MeterReadingPayload),
    // New IoT Gateway Events (from Router)
    SmartMeterReading(crate::models::DeviceReading),
    EvChargingEvent(crate::models::DeviceReading),
    BatteryStateUpdate(crate::models::DeviceReading),

    OrderMatched(serde_json::Value),
    OrderUpdate(serde_json::Value),
    SettlementRequested(serde_json::Value),
    PeakPriceUpdate(serde_json::Value),
    TriggerExecution(serde_json::Value),
    OrderCreated(serde_json::Value),
    #[serde(other)]
    Unknown,
}

pub struct EventIngester {
    connection_manager: ConnectionManager,
    streams: Vec<String>,
    group_name: String,
    consumer_name: String,
    api_gateway_url: String,
    http_client: reqwest::Client,
    aggregator: Arc<Mutex<Aggregator>>,
    #[allow(dead_code)] // Reserved for future clock synchronization features
    clock_offset: Arc<AtomicI64>,
    metrics: Arc<crate::state::Metrics>,
}

impl EventIngester {
    pub async fn new(
        redis_url: &str,
        api_gateway_url: &str,
        aggregator: Arc<Mutex<Aggregator>>,
        metrics: Arc<crate::state::Metrics>,
    ) -> Result<Self> {
        let client = Client::open(redis_url)?;
        
        // Try to create connection manager with retry
        let connection_manager = Self::create_connection_manager(&client).await?;

        let streams = vec![
            std::env::var("EVENT_STREAM_NAME")
                .unwrap_or_else(|_| "gridtokenx:events:v1".to_string()),
            "gridtokenx:ev:v1".to_string(),
            "gridtokenx:battery:v1".to_string(),
        ];
        let group_name = "oracle_bridge_group".to_string();
        let consumer_name = format!("consumer_{}", Uuid::new_v4());

        Ok(Self {
            connection_manager,
            streams,
            group_name,
            consumer_name,
            api_gateway_url: api_gateway_url.to_string(),
            http_client: reqwest::Client::new(),
            aggregator,
            clock_offset: Arc::new(AtomicI64::new(0)),
            metrics,
        })
    }

    async fn create_connection_manager(client: &Client) -> Result<ConnectionManager> {
        use tokio::time::{sleep, Duration};
        
        for attempt in 1..=10 {
            match ConnectionManager::new(client.clone()).await {
                Ok(cm) => {
                    info!("✅ Redis connection manager created");
                    return Ok(cm);
                }
                Err(e) => {
                    if attempt <= 5 {
                        warn!("⚠️ Redis connection attempt {} failed: {}. Retrying in {}s...", 
                              attempt, e, attempt);
                        sleep(Duration::from_secs(attempt)).await;
                    } else {
                        return Err(anyhow::anyhow!("Failed to connect to Redis after {} attempts: {}", attempt, e));
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!("Failed to connect to Redis"))
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.setup_consumer_groups().await?;

        info!("👂 Listening to streams: {:?} (group: {})", self.streams, self.group_name);

        // Start background clock sync
        let ingester_clone = self.clone();
        tokio::spawn(async move {
            ingester_clone.sync_clock_loop().await;
        });

        loop {
            if let Err(e) = self.process_next_batch().await {
                error!("⚠️ Error processing batch: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    async fn sync_clock_loop(&self) {
        // Clock sync no longer needed since we're not submitting directly to blockchain
        // Keep this as a no-op for future use
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
        }
    }

    async fn setup_consumer_groups(&self) -> Result<()> {
        let mut conn = self.connection_manager.clone();

        for stream in &self.streams {
            // Ensure stream exists and create group (ignore error if group already exists)
            let result: redis::RedisResult<()> = conn.xgroup_create_mkstream(stream, &self.group_name, "$").await;
            if let Err(e) = result {
                let err_str = e.to_string();
                if !err_str.contains("BUSYGROUP") {
                    // Only log error if it's not "group already exists"
                    warn!("⚠️ Could not create consumer group for {}: {}", stream, e);
                }
            }
        }

        Ok(())
    }

    async fn process_next_batch(&self) -> Result<()> {
        let mut conn = self.connection_manager.clone();

        let options = StreamReadOptions::default()
            .group(&self.group_name, &self.consumer_name)
            .block(2000)
            .count(10);

        // Read from all configured streams
        let stream_keys: Vec<&str> = self.streams.iter().map(|s| s.as_str()).collect();
        let ids: Vec<&str> = vec![">"; self.streams.len()];

        let reply: StreamReadReply = conn.xread_options(&stream_keys, &ids, &options)
            .await
            .context("Failed to read from Redis Streams")?;

        let mut futures = Vec::new();

        for stream in reply.keys {
            let stream_name = stream.key.clone();
            for entry in stream.ids {
                let stream_name_clone = stream_name.clone();
                let entry_clone = entry.clone();
                let ingester_ref = self; // self is already Arc-wrapped or passed by ref

                futures.push(async move {
                    if let Some(event_value) = entry_clone.map.get("event") {
                        if let Ok(json) = redis::from_redis_value::<String>(event_value) {
                            match serde_json::from_str::<Event>(&json) {
                                Ok(Event::MeterReadingCreated(payload)) => {
                                    let _ = ingester_ref.handle_meter_reading(payload).await;
                                }
                                Ok(Event::SmartMeterReading(reading)) => {
                                    let _ = ingester_ref.handle_iot_reading(reading).await;
                                }
                                Ok(Event::EvChargingEvent(reading)) => {
                                    let _ = ingester_ref.handle_iot_reading(reading).await;
                                }
                                Ok(Event::BatteryStateUpdate(reading)) => {
                                    let _ = ingester_ref.handle_iot_reading(reading).await;
                                }
                                Ok(_) => {
                                    debug!("⏭️ Ignoring non-ingestion event type in {}", stream_name_clone);
                                }
                                Err(e) => {
                                    warn!("⚠️ Failed to deserialize event from {}: {}", stream_name_clone, e);
                                }
                            }
                        }
                    }

                    // Return the pair to ACK
                    (stream_name_clone, entry_clone.id)
                });
            }
        }

        if !futures.is_empty() {
            let results = futures::future::join_all(futures).await;

            // Batch ACK
            for (stream_name, entry_id) in results {
                let _: redis::RedisResult<()> = conn.xack(&stream_name, &self.group_name, &[&entry_id]).await;
            }
        }

        Ok(())
    }

    async fn handle_meter_reading(&self, payload: MeterReadingPayload) -> Result<()> {
        info!("📈 Received MeterReadingCreated: {} ({} kWh)", payload.meter_serial, payload.kwh);

        // 1. Update Aggregator (local stats)
        {
            let mut agg = self.aggregator.lock().await;
            agg.handle_reading(payload.clone());
        }

        // 2. Forward to API Gateway for blockchain submission
        self.forward_to_api_gateway(&payload).await?;

        Ok(())
    }

    async fn handle_iot_reading(&self, reading: crate::models::DeviceReading) -> Result<()> {
        use crate::models::DeviceMetrics;
        use rust_decimal::Decimal;

        info!(
            "🌐 Received {:?} reading: {} ({})",
            reading.device_type, reading.serial_number, reading.device_id
        );

        // Map DeviceReading to MeterReadingPayload for API Gateway
        let (energy_generated, energy_consumed) = match reading.metrics {
            DeviceMetrics::Energy { generated_kwh, consumed_kwh, .. } => {
                (Some(generated_kwh), Some(consumed_kwh))
            }
            DeviceMetrics::EvSession { energy_delivered_kwh, .. } => {
                // EV charging is "consumed" energy from the grid's perspective
                (Some(0.0), Some(energy_delivered_kwh))
            }
            DeviceMetrics::BatteryState { power_kw, mode, .. } => {
                match mode {
                    crate::models::BatteryMode::Charging => (Some(0.0), Some(power_kw.abs())),
                    crate::models::BatteryMode::Discharging => (Some(power_kw.abs()), Some(0.0)),
                    crate::models::BatteryMode::Idle => (Some(0.0), Some(0.0)),
                }
            }
        };

        let payload = MeterReadingPayload {
            reading_id: Uuid::new_v4(),
            meter_id: Uuid::new_v4(), // DeviceReading doesn't have meter_id
            meter_serial: reading.serial_number,
            user_id: Uuid::nil(), // DeviceReading doesn't have user_id
            wallet_address: String::new(), // DeviceReading doesn't have wallet_address
            zone_id: reading.zone_id,
            kwh: Decimal::ZERO, // Not used for IoT readings
            energy_generated: energy_generated.and_then(|v| Decimal::from_f64_retain(v)),
            energy_consumed: energy_consumed.and_then(|v| Decimal::from_f64_retain(v)),
            timestamp: reading.timestamp,
        };

        // Forward to API Gateway
        self.forward_to_api_gateway(&payload).await?;

        Ok(())
    }

    /// Forward meter reading to API Gateway for blockchain submission
    async fn forward_to_api_gateway(&self, payload: &MeterReadingPayload) -> Result<()> {
        let url = format!("{}/api/v1/oracle/submit-reading", self.api_gateway_url);

        // Convert timestamp to i64
        let timestamp = payload.timestamp.timestamp();

        let response = self.http_client
            .post(&url)
            .json(&serde_json::json!({
                "reading_id": payload.reading_id,
                "meter_id": payload.meter_id,
                "meter_serial": payload.meter_serial,
                "user_id": payload.user_id,
                "wallet_address": payload.wallet_address,
                "zone_id": payload.zone_id,
                "kwh": payload.kwh.to_string(),
                "energy_generated": payload.energy_generated.map(|d| d.to_string()),
                "energy_consumed": payload.energy_consumed.map(|d| d.to_string()),
                "timestamp": timestamp,
            }))
            .send()
            .await
            .context("Failed to send request to API Gateway")?;

        if response.status().is_success() {
            let result: serde_json::Value = response.json().await
                .context("Failed to parse API Gateway response")?;
            
            if let Some(signature) = result.get("signature").and_then(|v| v.as_str()) {
                info!("⛓️ API Gateway submitted to blockchain: TX: {}", signature);
                self.metrics.record_sync();
            } else {
                warn!("⚠️ API Gateway response missing signature");
            }
        } else {
            let error_text = response.text().await.unwrap_or_default();
            error!("❌ API Gateway rejected reading: {}", error_text);
            return Err(anyhow::anyhow!("API Gateway error: {}", error_text));
        }

        Ok(())
    }
}
