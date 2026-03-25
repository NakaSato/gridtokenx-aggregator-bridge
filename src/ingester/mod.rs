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
use crate::blockchain::BlockchainClient;
use crate::aggregator::Aggregator;

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
    blockchain_client: Arc<BlockchainClient>,
    aggregator: Arc<Mutex<Aggregator>>,
}

impl EventIngester {
    pub async fn new(
        redis_url: &str,
        blockchain_client: Arc<BlockchainClient>,
        aggregator: Arc<Mutex<Aggregator>>,
    ) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;
        
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
            blockchain_client,
            aggregator,
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.setup_consumer_groups().await?;
        
        info!("👂 Listening to streams: {:?} (group: {})", self.streams, self.group_name);
        
        loop {
            if let Err(e) = self.process_next_batch().await {
                error!("⚠️ Error processing batch: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    async fn setup_consumer_groups(&self) -> Result<()> {
        let mut conn = self.connection_manager.clone();
        
        for stream in &self.streams {
            // Ensure stream exists and create group
            let _: redis::RedisResult<()> = conn.xgroup_create_mkstream(stream, &self.group_name, "$").await;
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
        
        // 1. Update Aggregator
        {
            let mut agg = self.aggregator.lock().await;
            agg.handle_reading(payload.clone());
        }

        // 2. Submit to Blockchain
        let produced = (payload.energy_generated.unwrap_or(Decimal::ZERO) * Decimal::from(1000)).to_string().parse::<u64>().unwrap_or(0);
        let consumed = (payload.energy_consumed.unwrap_or(Decimal::ZERO) * Decimal::from(1000)).to_string().parse::<u64>().unwrap_or(0);
        let timestamp = Utc::now().timestamp();

        match self.blockchain_client.submit_meter_reading(
            payload.meter_serial.clone(),
            produced,
            consumed,
            timestamp
        ).await {
            Ok(sig) => info!("⛓️ On-Chain Update Success: [Meter] {} - TX: {}", payload.meter_serial, sig),
            Err(e) => error!("❌ On-Chain Update Failed: [Meter] {} - {}", payload.meter_serial, e),
        }

        Ok(())
    }

    async fn handle_iot_reading(&self, reading: crate::models::DeviceReading) -> Result<()> {
        use crate::models::DeviceMetrics;
        
        info!(
            "🌐 Received {:?} reading: {} ({})",
            reading.device_type, reading.serial_number, reading.device_id
        );

        // Map DeviceReading to Blockchain submission
        // We use the same submit_meter_reading but map metrics accordingly
        let (produced, consumed) = match reading.metrics {
            DeviceMetrics::Energy { generated_kwh, consumed_kwh, .. } => {
                ((generated_kwh * 1000.0) as u64, (consumed_kwh * 1000.0) as u64)
            }
            DeviceMetrics::EvSession { energy_delivered_kwh, .. } => {
                // EV charging is "consumed" energy from the grid's perspective
                (0, (energy_delivered_kwh * 1000.0) as u64)
            }
            DeviceMetrics::BatteryState { power_kw, mode, .. } => {
                // Approximate energy based on power if available, or just use 0 if state-only
                // For now, let's treat power_kw as the "rate"
                match mode {
                    crate::models::BatteryMode::Charging => (0, (power_kw.abs() * 1000.0) as u64),
                    crate::models::BatteryMode::Discharging => ((power_kw.abs() * 1000.0) as u64, 0),
                    crate::models::BatteryMode::Idle => (0, 0),
                }
            }
        };

        let timestamp = reading.timestamp.timestamp();

        match self.blockchain_client.submit_meter_reading(
            reading.serial_number.clone(),
            produced,
            consumed,
            timestamp
        ).await {
            Ok(sig) => info!("⛓️ On-Chain Update Success: [{:?}] {} - TX: {}", reading.device_type, reading.serial_number, sig),
            Err(e) => error!("❌ On-Chain Update Failed: [{:?}] {} - {}", reading.device_type, reading.serial_number, e),
        }

        Ok(())
    }
}
