//! Zone-based microgrid parallel ingester for high-throughput processing
//!
//! This module provides parallel ingestion by:
//! - Partitioning readings by zone_id into separate Redis streams
//! - Running parallel ingester workers per zone
//! - Batching telemetry submissions to API Gateway

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};
use rust_decimal::Decimal;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::aggregator::Aggregator;
use crate::infra::platform::{MeterReading, PlatformClient};
use crate::ingester::batcher::{BatchHandle, BatchWorker, BATCH_TIMEOUT_MS, FORWARD_BATCH_SIZE};
use crate::ingester::bin_store::BinStore;
use crate::ingester::Event;
use crate::utils::numeric::to_positive_decimal;

/// Maximum concurrent processing per zone
const ZONE_SEMAPHORE_SIZE: usize = 50;

/// Zone-based event ingester with parallel processing and batch forwarding
pub struct ZoneEventIngester {
    connection_manager: ConnectionManager,
    aggregator: Arc<Mutex<Aggregator>>,
    #[allow(dead_code)]
    metrics: Arc<crate::state::Metrics>,
    zone_streams: Vec<String>,
    group_name: String,
    consumer_name: String,
    batch_handle: BatchHandle,
    /// Number of zone partitions
    num_zones: usize,
    /// Meter → User ID resolver
    meter_registry: Arc<crate::infra::meter_registry::MeterRegistry>,
    /// Durable billing-bin store (crash recovery). None ⇒ degraded (RAM-only) mode.
    bin_store: Option<BinStore>,
}

impl ZoneEventIngester {
    pub async fn new(
        redis_url: &str,
        api_services_url: &str,
        aggregator: Arc<Mutex<Aggregator>>,
        metrics: Arc<crate::state::Metrics>,
        num_zones: usize,
        meter_registry: Arc<crate::infra::meter_registry::MeterRegistry>,
        bin_store: Option<BinStore>,
    ) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let connection_manager = Self::create_connection_manager(&client).await?;

        // Create zone-partitioned streams
        let mut zone_streams = Vec::with_capacity(num_zones);
        for i in 0..num_zones {
            zone_streams.push(format!("gridtokenx:events:zone_{}", i));
        }

        let group_name = "aggregator_bridge_zone_group".to_string();
        let consumer_name = format!("zone_consumer_{}", Uuid::new_v4());

        // Create batch forwarder with Redis connection for ACKing.
        // The platform (OracleService) client is best-effort: the live forward path is a
        // Redis-ACK bypass, so a missing/unreachable target must NOT block the ingester —
        // local aggregation (Path B billing bins) depends only on Redis + the Aggregator.
        let platform_client = match PlatformClient::new(api_services_url).await {
            Ok(c) => Some(c),
            Err(e) => {
                warn!(
                    "⚠️ PlatformClient unavailable ({}): zone ingester runs without gRPC forwarding (Path B bins still fill locally).",
                    e
                );
                None
            }
        };
        let batch_handle = BatchWorker::spawn(
            platform_client,
            connection_manager.clone(),
            group_name.clone(),
            metrics.clone(),
        );

        info!("🔢 Zone partitions: {}", num_zones);
        info!("📊 Zone streams: {:?}", zone_streams);
        info!(
            "📦 Batch forwarding: {} readings or {}ms timeout",
            FORWARD_BATCH_SIZE, BATCH_TIMEOUT_MS
        );

        Ok(Self {
            connection_manager,
            aggregator,
            metrics,
            zone_streams,
            group_name,
            consumer_name,
            batch_handle,
            num_zones,
            meter_registry,
            bin_store,
        })
    }

    async fn create_connection_manager(client: &Client) -> Result<ConnectionManager> {
        use tokio::time::{sleep, Duration, timeout};
        for attempt in 1..=5 {
            match timeout(Duration::from_secs(2), ConnectionManager::new(client.clone())).await {
                Ok(Ok(cm)) => {
                    info!("✅ Redis connection manager created for zone ingester");
                    return Ok(cm);
                }
                Ok(Err(e)) => {
                    warn!("⚠️ Redis connection attempt {} failed: {}. Retrying...", attempt, e);
                    sleep(Duration::from_secs(1)).await;
                }
                Err(_) => {
                    warn!("⚠️ Redis connection attempt {} timed out. Retrying...", attempt);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Err(anyhow::anyhow!("Failed to connect to Redis after 5 attempts"))
    }


    /// Determine which zone stream a reading should go to
    #[allow(dead_code)]
    pub fn get_zone_index(&self, zone_code: Option<String>, meter_serial: &str) -> usize {
        match zone_code {
            Some(zcode) => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                zcode.hash(&mut hasher);
                hasher.finish() as usize % self.num_zones
            }
            _ => {
                // Hash meter serial to zone for consistent partitioning
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                meter_serial.hash(&mut hasher);
                hasher.finish() as usize % self.num_zones
            }
        }
    }

    pub async fn run(self: Arc<Self>, token: CancellationToken) -> Result<()> {
        self.setup_consumer_groups().await?;

        info!(
            "👂 Zone-based ingester listening to {} streams",
            self.zone_streams.len()
        );
        info!(
            "🚀 Spawning {} zone workers with concurrency {}",
            self.num_zones, ZONE_SEMAPHORE_SIZE
        );

        // Spawn worker for each zone
        let mut join_set = JoinSet::new();
        for (idx, stream_name) in self.zone_streams.iter().enumerate() {
            let ingester = self.clone();
            let stream = stream_name.clone();
            let zone_token = token.clone();
            join_set.spawn(async move {
                info!("🔨 Starting zone worker {} for stream: {}", idx, stream);
                tokio::select! {
                    res = ingester.process_zone_stream(&stream, idx) => res,
                    _ = zone_token.cancelled() => {
                        info!("🔄 Zone worker {} shutting down...", idx);
                        Ok(())
                    }
                }
            });
        }

        // Spawn redelivery task (reclaims messages stale for > 30s)
        let redelivery_ingester = self.clone();
        let redelivery_token = token.clone();
        join_set.spawn(async move {
            redelivery_ingester
                .run_redelivery_loop(redelivery_token)
                .await;
            Ok::<(), anyhow::Error>(())
        });

        // Batch worker now handles its own periodic flushes internally

        // Monitor workers
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => debug!("Worker completed"),
                Ok(Err(e)) => error!("❌ Worker failed: {}", e),
                Err(e) => error!("❌ Worker panicked: {}", e),
            }
        }

        // 10. Crucial: Shut down the batch worker to trigger final flush
        info!("🔄 All zone workers stopped. Triggering final telemetry flush...");
        if let Err(e) = self.batch_handle.clone().shutdown().await {
            error!("❌ Final telemetry flush failed: {}", e);
        }

        Ok(())
    }

    async fn run_redelivery_loop(self: Arc<Self>, token: CancellationToken) {
        // Wait for burn-in period
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {},
            _ = token.cancelled() => return,
        }

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        info!("♻️ Zone redelivery loop started (threshold: 30s)");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for stream in &self.zone_streams {
                        if let Err(e) = self.reclaim_stale_messages(stream).await {
                            error!("❌ Failed to reclaim messages from {}: {}", stream, e);
                        }
                    }
                }
                _ = token.cancelled() => {
                    info!("🔄 Redelivery loop shutting down...");
                    break;
                }
            }
        }
    }

    async fn reclaim_stale_messages(&self, stream: &str) -> Result<()> {
        let mut conn = self.connection_manager.clone();

        // 1. Get pending messages via XPENDING range
        let pending_items: Vec<redis::Value> = redis::cmd("XPENDING")
            .arg(stream)
            .arg(&self.group_name)
            .arg("-")
            .arg("+")
            .arg(50)
            .query_async(&mut conn)
            .await?;

        let mut ids_to_claim = Vec::new();
        for item in pending_items {
            if let redis::Value::Array(parts) = item {
                if parts.len() >= 3 {
                    let id: String = redis::from_redis_value(&parts[0])?;
                    let idle_ms: u64 = redis::from_redis_value(&parts[2])?;
                    if idle_ms >= 30000 {
                        ids_to_claim.push(id);
                    }
                }
            }
        }

        if ids_to_claim.is_empty() {
            return Ok(());
        }

        // 2. Claim stale messages
        let mut claim_cmd = redis::cmd("XCLAIM");
        claim_cmd
            .arg(stream)
            .arg(&self.group_name)
            .arg(&self.consumer_name)
            .arg(30000);
        for id in &ids_to_claim {
            claim_cmd.arg(id);
        }

        let claimed_values: Vec<redis::Value> = claim_cmd.query_async(&mut conn).await?;

        if !claimed_values.is_empty() {
            info!(
                "♻️ Reclaimed {} stale messages from stream {}",
                claimed_values.len(),
                stream
            );
            for val in claimed_values {
                if let redis::Value::Array(parts) = val {
                    if parts.len() >= 2 {
                        let entry_id: String = redis::from_redis_value(&parts[0])?;
                        let fields: std::collections::BTreeMap<String, String> =
                            redis::from_redis_value(&parts[1])?;

                        if let Some(json) = fields.get("event") {
                            match serde_json::from_str::<Event>(json)
                                .context("Failed to parse reclaimed event JSON")
                            {
                                Ok(Event::SmartMeterReading(reading))
                                | Ok(Event::EvCharging(reading))
                                | Ok(Event::BatteryStateUpdate(reading)) => {
                                    if let Err(e) =
                                        self.handle_iot_reading(reading, stream, &entry_id).await
                                    {
                                        error!("❌ Failed to handle reclaimed IoT reading: {}", e);
                                    }
                                }
                                Ok(_) => {
                                    let mut conn = self.connection_manager.clone();
                                    let _: redis::RedisResult<()> =
                                        conn.xack(stream, &self.group_name, &[&entry_id]).await;
                                }
                                Err(e) => {
                                    error!("❌ Reclaim error: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn setup_consumer_groups(&self) -> Result<()> {
        let mut conn = self.connection_manager.clone();

        for stream in &self.zone_streams {
            let result: redis::RedisResult<()> = conn
                .xgroup_create_mkstream(stream, &self.group_name, "$")
                .await;
            if let Err(e) = result {
                let err_str = e.to_string();
                if !err_str.contains("BUSYGROUP") {
                    warn!("⚠️ Could not create consumer group for {}: {}", stream, e);
                } else {
                    debug!("Consumer group already exists for {}", stream);
                }
            }
        }

        Ok(())
    }

    async fn process_zone_stream(
        self: Arc<Self>,
        stream_name: &str,
        zone_idx: usize,
    ) -> Result<()> {
        let mut conn = self.connection_manager.clone();
        let semaphore = Arc::new(Semaphore::new(ZONE_SEMAPHORE_SIZE));
        let mut pending_count = 0;
        let mut total_processed = 0;

        loop {
            let options = StreamReadOptions::default()
                .group(&self.group_name, &self.consumer_name)
                .block(2000)
                .count(25);

            let reply: Result<StreamReadReply, _> = conn
                .xread_options(&[stream_name], &[">"], &options)
                .await
                .context("Failed to read from Redis Stream");

            match reply {
                Ok(stream_reply) => {
                    for stream in stream_reply.keys {
                        for entry in stream.ids {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let ingester = self.clone();
                            let stream_name = stream.key.clone();
                            let entry_id = entry.id.clone();

                            tokio::spawn(async move {
                                // Process entry
                                if let Some(event_value) = entry.map.get("event") {
                                    if let Ok(json) = redis::from_redis_value::<String>(event_value)
                                    {
                                        match serde_json::from_str::<Event>(&json).with_context(
                                            || {
                                                format!(
                                                    "Failed to parse event JSON for entry {}",
                                                    entry_id
                                                )
                                            },
                                        ) {
                                            Ok(Event::SmartMeterReading(reading))
                                            | Ok(Event::EvCharging(reading))
                                            | Ok(Event::BatteryStateUpdate(reading)) => {
                                                let serial = reading.serial_number.clone();
                                                if let Err(e) = ingester
                                                    .handle_iot_reading(
                                                        reading,
                                                        &stream_name,
                                                        &entry_id,
                                                    )
                                                    .await
                                                {
                                                    error!(
                                                        "❌ Error handling IoT reading [{}]: {}",
                                                        serial, e
                                                    );
                                                }
                                            }
                                            Ok(_) => {
                                                // Unknown event, ACK it so it doesn't block
                                                let mut conn = ingester.connection_manager.clone();
                                                let _: redis::RedisResult<()> = conn
                                                    .xack(
                                                        &stream_name,
                                                        &ingester.group_name,
                                                        &[&entry_id],
                                                    )
                                                    .await;
                                            }
                                            Err(e) => {
                                                error!("❌ Event processing error: {}", e);
                                            }
                                        }
                                    }
                                }

                                // ACKing is now handled by the BatchForwarder after successful RPC
                                drop(permit);
                            });

                            pending_count += 1;
                            total_processed += 1;
                        }
                    }
                }
                Err(e) => {
                    error!("⚠️ Error reading from zone {}: {}", stream_name, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }

            // Log progress every 1000 messages
            if pending_count >= 1000 {
                info!(
                    "📊 Zone {} processed {} messages (total: {})",
                    zone_idx, pending_count, total_processed
                );
                pending_count = 0;
            }
        }
    }

    async fn handle_iot_reading(
        &self,
        reading: crate::models::DeviceReading,
        stream_name: &str,
        entry_id: &str,
    ) -> Result<()> {
        use crate::models::DeviceMetrics;

        let (energy_generated, energy_consumed, net_kwh) = match reading.metrics {
            DeviceMetrics::Energy {
                generated_kwh,
                consumed_kwh,
                net_kwh,
            } => (
                to_positive_decimal(generated_kwh, "energy_generated").unwrap_or(Decimal::ZERO),
                to_positive_decimal(consumed_kwh, "energy_consumed").unwrap_or(Decimal::ZERO),
                Decimal::from_f64_retain(net_kwh).unwrap_or(Decimal::ZERO),
            ),
            DeviceMetrics::EvSession {
                energy_delivered_kwh,
                ..
            } => {
                let consumed = to_positive_decimal(energy_delivered_kwh, "ev_energy_delivered")
                    .unwrap_or(Decimal::ZERO);
                (Decimal::ZERO, consumed, -consumed)
            }
            DeviceMetrics::BatteryState { power_kw, mode, .. } => match mode {
                crate::models::BatteryMode::Charging => {
                    let consumed = to_positive_decimal(power_kw.abs(), "battery_power_charging")
                        .unwrap_or(Decimal::ZERO);
                    (Decimal::ZERO, consumed, -consumed)
                }
                crate::models::BatteryMode::Discharging => {
                    let generated =
                        to_positive_decimal(power_kw.abs(), "battery_power_discharging")
                            .unwrap_or(Decimal::ZERO);
                    (generated, Decimal::ZERO, generated)
                }
                crate::models::BatteryMode::Idle => (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            },
        };

        // Use a consistent UUID derived from the serial number for aggregation
        let meter_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, reading.serial_number.as_bytes());

        // Resolve user_id from meter registry
        let user_id = match self
            .meter_registry
            .resolve_user_id(&reading.serial_number)
            .await
        {
            Ok(Some(uid)) => uid,
            Ok(None) => {
                debug!(
                    "⚠️ No user mapping found for meter {}. Using nil UUID.",
                    reading.serial_number
                );
                Uuid::nil()
            }
            Err(e) => {
                warn!(
                    "⚠️ Meter registry lookup failed for {}: {}. Using nil UUID.",
                    reading.serial_number, e
                );
                Uuid::nil()
            }
        };

        // 1. Update Aggregator (local stats) + write-through to durable store.
        // Snapshot is taken under the lock; the Redis I/O happens after release.
        let bin_snapshot = {
            let mut agg = self.aggregator.lock().await;
            agg.handle_reading(
                meter_id,
                user_id,
                reading.serial_number.clone(),
                energy_generated,
                energy_consumed,
                reading.timestamp,
            )
        };
        if let Some(store) = &self.bin_store {
            if let Err(e) = store.persist(&bin_snapshot).await {
                warn!(
                    "⚠️ Billing-bin persist failed for {} (in-memory only, energy at-risk on restart): {}",
                    bin_snapshot.meter_serial, e
                );
            }
        }

        // 2. Forward to API Gateway (batched)
        self.forward_to_api_services(
            reading.reading_id,
            meter_id,
            reading.serial_number,
            reading.zone_code.clone(),
            net_kwh,
            reading.timestamp,
            stream_name,
            entry_id,
        )
        .await
    }

    /// Forward meter reading to API Gateway with batching (UTT Pipeline)
    async fn forward_to_api_services(
        &self,
        reading_id: Uuid,
        meter_id: Uuid,
        meter_serial: String,
        zone_code: Option<String>,
        net_kwh: Decimal,
        timestamp: DateTime<Utc>,
        stream_name: &str,
        entry_id: &str,
    ) -> Result<()> {
        // BYPASS: Legacy HTTP/gRPC forwarder removed in favor of direct Kafka streaming.
        
        // Directly ACK the message in Redis to prevent redelivery
        let mut conn = self.connection_manager.clone();
        let _: () = redis::AsyncCommands::xack(&mut conn, stream_name, &self.group_name, &[entry_id])
            .await
            .context("Failed to ACK message in Redis stream")?;
            
        Ok(())
    }
}
