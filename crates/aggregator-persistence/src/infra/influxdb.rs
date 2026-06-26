//! Independent InfluxDB v2 time-series sink for realtime telemetry history.
//!
//! This sink is **dedicated to the Aggregator Bridge only** — it owns its own
//! `INFLUXDB_*` connection (URL / org / bucket / token) and shares nothing with
//! any other service's InfluxDB. Point it at a standalone instance.
//!
//! Write path is **async fire-and-forget**: [`InfluxWriter::record`] enqueues a
//! [`TelemetryPoint`] onto a bounded channel and returns immediately; a
//! background task batches points and flushes them to InfluxDB. A slow or
//! unreachable InfluxDB therefore never blocks the realtime ingest path — it is
//! degraded-by-design, matching the rest of this service's edges.

use std::time::Duration;

use anyhow::Result;
use futures::stream;
use influxdb2::models::DataPoint;
use influxdb2::Client;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{error, info, warn};

/// Bounded queue capacity. Once full, new points are dropped (and logged)
/// rather than applying backpressure to the realtime ingest path.
const CHANNEL_CAPACITY: usize = 10_000;
/// Max points buffered before a flush is forced.
const BATCH_SIZE: usize = 500;
/// Max time a partial batch waits before being flushed.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// A protocol-neutral telemetry sample destined for InfluxDB.
///
/// Defined here (not in `aggregator-core`) so this persistence edge stays free
/// of domain deps; the caller (the router, which owns `DeviceReading`) maps a
/// reading into this shape.
#[derive(Debug, Clone)]
pub struct TelemetryPoint {
    /// InfluxDB measurement name (e.g. `"energy"`, `"ev_session"`, `"battery"`).
    pub measurement: &'static str,
    /// Numeric tags identifying the source device.
    pub device_id: String,
    pub device_type: String,
    pub serial_number: String,
    pub zone_code: Option<String>,
    /// Additional categorical tags (e.g. EV status, battery mode).
    pub extra_tags: Vec<(&'static str, String)>,
    /// Numeric fields (the actual measured values).
    pub fields: Vec<(&'static str, f64)>,
    /// Event time in nanoseconds since the Unix epoch.
    pub timestamp_ns: i64,
}

impl TelemetryPoint {
    fn into_data_point(self) -> Result<DataPoint> {
        let mut builder = DataPoint::builder(self.measurement)
            .tag("device_id", self.device_id)
            .tag("device_type", self.device_type)
            .tag("serial_number", self.serial_number);

        if let Some(zone) = self.zone_code {
            builder = builder.tag("zone_code", zone);
        }
        for (k, v) in self.extra_tags {
            builder = builder.tag(k, v);
        }
        for (k, v) in self.fields {
            builder = builder.field(k, v);
        }
        builder = builder.timestamp(self.timestamp_ns);

        Ok(builder.build()?)
    }
}

/// Handle to the background InfluxDB writer task.
#[derive(Clone)]
pub struct InfluxWriter {
    tx: mpsc::Sender<TelemetryPoint>,
}

impl InfluxWriter {
    /// Build a writer from `INFLUXDB_*` env vars and start the background task.
    ///
    /// Returns `Ok(None)` (disabled) when `INFLUXDB_URL` is unset — InfluxDB is
    /// optional. Returns `Err` only on genuinely malformed config; callers
    /// degrade to `None` on `Err` too, so a missing/unreachable InfluxDB never
    /// stops the service from starting.
    pub async fn connect() -> Result<Option<Self>> {
        let url = match std::env::var("INFLUXDB_URL") {
            Ok(u) if !u.is_empty() => u,
            _ => {
                info!("ℹ️ InfluxDB disabled (INFLUXDB_URL not set)");
                return Ok(None);
            }
        };
        let org = std::env::var("INFLUXDB_ORG").unwrap_or_else(|_| "gridtokenx".to_string());
        let bucket =
            std::env::var("INFLUXDB_BUCKET").unwrap_or_else(|_| "aggregator_telemetry".to_string());
        let token = std::env::var("INFLUXDB_TOKEN").unwrap_or_default();

        let client = Client::new(url.clone(), org.clone(), token);

        // Health probe so an unreachable InfluxDB degrades loudly at boot
        // instead of silently dropping every point later.
        match client.health().await {
            Ok(_) => info!(
                "🗄️ InfluxDB connected (independent telemetry history): url={} org={} bucket={}",
                url, org, bucket
            ),
            Err(e) => {
                warn!(
                    "⚠️ InfluxDB health check failed ({}). Realtime history persistence disabled.",
                    e
                );
                return Ok(None);
            }
        }

        let (tx, rx) = mpsc::channel::<TelemetryPoint>(CHANNEL_CAPACITY);
        tokio::spawn(run_writer(client, bucket, rx));

        Ok(Some(Self { tx }))
    }

    /// Enqueue a point for asynchronous write. Never blocks; drops (and logs)
    /// when the queue is full so the realtime path is never throttled.
    pub fn record(&self, point: TelemetryPoint) {
        if let Err(e) = self.tx.try_send(point) {
            // `warn` not `error`: dropping history under overload is degraded,
            // not a failure of the operational (Redis) path.
            warn!(
                "⚠️ InfluxDB queue full or closed; dropping telemetry point: {}",
                e
            );
        }
    }
}

/// Background batcher: drains the channel, flushing on size or interval.
async fn run_writer(client: Client, bucket: String, mut rx: mpsc::Receiver<TelemetryPoint>) {
    let mut batch: Vec<TelemetryPoint> = Vec::with_capacity(BATCH_SIZE);
    let mut ticker = interval(FLUSH_INTERVAL);

    loop {
        tokio::select! {
            maybe_point = rx.recv() => {
                match maybe_point {
                    Some(point) => {
                        batch.push(point);
                        if batch.len() >= BATCH_SIZE {
                            flush(&client, &bucket, &mut batch).await;
                        }
                    }
                    None => {
                        // Channel closed (shutdown): flush remainder and exit.
                        flush(&client, &bucket, &mut batch).await;
                        info!("🛑 InfluxDB writer task stopped");
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&client, &bucket, &mut batch).await;
                }
            }
        }
    }
}

/// Write the current batch, draining it regardless of outcome (a write failure
/// drops that batch loudly rather than retrying forever and stalling the queue).
async fn flush(client: &Client, bucket: &str, batch: &mut Vec<TelemetryPoint>) {
    if batch.is_empty() {
        return;
    }
    let count = batch.len();
    let mut points = Vec::with_capacity(count);
    for tp in batch.drain(..) {
        match tp.into_data_point() {
            Ok(dp) => points.push(dp),
            Err(e) => warn!("⚠️ Skipping malformed InfluxDB point: {}", e),
        }
    }

    if points.is_empty() {
        return;
    }

    match client.write(bucket, stream::iter(points)).await {
        Ok(()) => {}
        Err(e) => error!("❌ InfluxDB write failed ({} points dropped): {}", count, e),
    }
}
