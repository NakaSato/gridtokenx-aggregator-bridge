use anyhow::{anyhow, Context, Result};
use redis::aio::ConnectionManager;
use redis::streams::StreamMaxlen;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use aggregator_core::models::{DeviceMetrics, DeviceReading, DeviceType};
use aggregator_persistence::infra::influxdb::{InfluxWriter, TelemetryPoint};

/// Default maximum number of entries per Redis stream.
const DEFAULT_MAX_STREAM_LEN: usize = 100_000;

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
    /// Optional independent InfluxDB sink for realtime telemetry history.
    influx: Option<Arc<InfluxWriter>>,
    /// Optional at-rest encryption of stream payloads. When set, the serialized
    /// DeviceReading is AES-256-GCM sealed before XADD (the in-process zone
    /// ingester decrypts); when None, payloads are written in the clear.
    stream_cipher: Option<Arc<aggregator_persistence::infra::stream_cipher::StreamCipher>>,
}

impl Router {
    pub async fn new(
        redis_url: &str,
        num_zones: usize,
        influx: Option<Arc<InfluxWriter>>,
    ) -> Result<Self> {
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
            influx,
            stream_cipher: None,
        })
    }

    /// Attach a stream cipher to encrypt payloads at rest in the Redis streams.
    pub fn with_stream_cipher(
        mut self,
        cipher: Option<Arc<aggregator_persistence::infra::stream_cipher::StreamCipher>>,
    ) -> Self {
        self.stream_cipher = cipher;
        self
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

        let event_type = self.event_type_name(reading);

        // When a stream cipher is configured, the reading is AES-256-GCM sealed
        // so the at-rest stream entry carries no plaintext registers — the
        // in-process zone ingester decrypts it. Otherwise the payload is written
        // in the clear (backward-compatible). The `event_type` stays cleartext
        // (routing/observability) and is bound as the GCM AAD.
        let json = if let Some(cipher) = &self.stream_cipher {
            let payload_bytes = serde_json::to_vec(reading)
                .context("Failed to serialize reading for encryption")?;
            let (nonce, ciphertext) = cipher
                .encrypt(&payload_bytes, event_type.as_bytes())
                .context("Failed to encrypt stream payload")?;
            serde_json::to_string(&serde_json::json!({
                "event_type": event_type,
                "enc": { "nonce": nonce, "ciphertext": ciphertext },
            }))
            .context("Failed to serialize encrypted reading")?
        } else {
            serde_json::to_string(&serde_json::json!({
                "event_type": event_type,
                "payload": reading,
            }))
            .context("Failed to serialize reading")?
        };

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

        // Persist realtime history to the independent InfluxDB sink (async,
        // fire-and-forget — a slow/down InfluxDB never blocks dissemination).
        if let Some(influx) = &self.influx {
            if let Some(point) = reading_to_point(reading) {
                influx.record(point);
            }
        }

        // Surplus minting is NOT done here. Readings are persisted (Redis zone +
        // unified streams, InfluxDB); the on-chain mint happens in the settlement
        // sink when a 15-min billing window closes with net surplus, via the
        // MintGateway (Chain Bridge over NATS). See src/main.rs + infra/mint.rs.

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

/// Map a normalized `DeviceReading` into a protocol-neutral InfluxDB point.
///
/// Measurement is keyed by device type; categorical state (EV/battery) becomes
/// tags, numeric metrics become fields. Returns `None` only if the event time
/// is unrepresentable as nanoseconds.
fn reading_to_point(reading: &DeviceReading) -> Option<TelemetryPoint> {
    let timestamp_ns = reading.timestamp.timestamp_nanos_opt()?;

    type PointTags = Vec<(&'static str, String)>;
    type PointFields = Vec<(&'static str, f64)>;
    let (measurement, extra_tags, mut fields): (&'static str, PointTags, PointFields) =
        match &reading.metrics {
            DeviceMetrics::Energy {
                generated_kwh,
                consumed_kwh,
                net_kwh,
            } => (
                "energy",
                vec![],
                vec![
                    ("generated_kwh", *generated_kwh),
                    ("consumed_kwh", *consumed_kwh),
                    ("net_kwh", *net_kwh),
                ],
            ),
            DeviceMetrics::EvSession {
                energy_delivered_kwh,
                session_id,
                connector_id,
                status,
            } => (
                "ev_session",
                vec![
                    ("session_id", session_id.clone()),
                    ("status", format!("{:?}", status)),
                ],
                vec![
                    ("energy_delivered_kwh", *energy_delivered_kwh),
                    ("connector_id", *connector_id as f64),
                ],
            ),
            DeviceMetrics::BatteryState {
                soc_percent,
                power_kw,
                temperature_c,
                mode,
            } => (
                "battery",
                vec![("mode", format!("{:?}", mode))],
                vec![
                    ("soc_percent", *soc_percent),
                    ("power_kw", *power_kw),
                    ("temperature_c", *temperature_c),
                ],
            ),
        };

    // Promote the decoded residential OBIS registers from metadata onto the
    // InfluxDB point so the full set (not just energy) is queryable/plottable.
    // Each is emitted only when present and numeric; the bridge's DlmsStack
    // decodes these names (see dlms.rs). f64 fields only — `active_tariff` is an
    // int register surfaced as a float for InfluxDB.
    if measurement == "energy" {
        const EXTRA_FIELDS: &[&str] = &[
            "sum_active_power_kw",
            "max_demand_import_kw",
            "active_tariff",
            "active_import_rate1_kwh",
            "active_import_rate2_kwh",
            "active_export_rate1_kwh",
            "active_export_rate2_kwh",
            "reactive_energy_import_kvarh",
            "reactive_energy_export_kvarh",
            "voltage_l1_v",
            "frequency_hz",
            "power_factor",
        ];
        for &name in EXTRA_FIELDS {
            if let Some(v) = reading.metadata.get(name).and_then(|v| v.as_f64()) {
                fields.push((name, v));
            }
        }
    }

    let device_type = match reading.device_type {
        DeviceType::SmartMeter => "smart_meter",
        DeviceType::EvCharger => "ev_charger",
        DeviceType::Battery => "battery",
    };

    Some(TelemetryPoint {
        measurement,
        device_id: reading.device_id.clone(),
        device_type: device_type.to_string(),
        serial_number: reading.serial_number.clone(),
        zone_code: reading.zone_code.clone(),
        extra_tags,
        fields,
        timestamp_ns,
    })
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
            timestamp: gridtokenx_telemetry::time::now(),
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

    fn energy_reading_with_metadata(
        meta: std::collections::HashMap<String, serde_json::Value>,
    ) -> DeviceReading {
        DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: "DEV-1".to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: "SN-1".to_string(),
            zone_code: Some("ZONE1".to_string()),
            timestamp: Utc::now(),
            metrics: DeviceMetrics::Energy {
                generated_kwh: 10.0,
                consumed_kwh: 5.0,
                net_kwh: 5.0,
            },
            metadata: meta,
        }
    }

    fn field(point: &TelemetryPoint, name: &str) -> Option<f64> {
        point
            .fields
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| *v)
    }

    #[test]
    fn energy_point_promotes_numeric_obis_metadata_fields() {
        let mut meta = std::collections::HashMap::new();
        // Numeric registers — must be promoted onto the point.
        meta.insert("active_tariff".to_string(), serde_json::json!(1));
        meta.insert("max_demand_import_kw".to_string(), serde_json::json!(32.0));
        meta.insert("sum_active_power_kw".to_string(), serde_json::json!(-3.944));
        meta.insert(
            "active_import_rate1_kwh".to_string(),
            serde_json::json!(10.0),
        );
        // Non-numeric / unknown — must NOT be promoted.
        meta.insert("dr_status".to_string(), serde_json::json!("active"));
        meta.insert("not_a_listed_field".to_string(), serde_json::json!(99.0));

        let point = reading_to_point(&energy_reading_with_metadata(meta)).expect("representable");

        assert_eq!(point.measurement, "energy");
        // Base energy fields stay.
        assert_eq!(field(&point, "generated_kwh"), Some(10.0));
        assert_eq!(field(&point, "consumed_kwh"), Some(5.0));
        assert_eq!(field(&point, "net_kwh"), Some(5.0));
        // Listed numeric OBIS registers promoted (int coerced to f64).
        assert_eq!(field(&point, "active_tariff"), Some(1.0));
        assert_eq!(field(&point, "max_demand_import_kw"), Some(32.0));
        assert_eq!(field(&point, "sum_active_power_kw"), Some(-3.944));
        assert_eq!(field(&point, "active_import_rate1_kwh"), Some(10.0));
        // Non-numeric string and unlisted keys are dropped.
        assert_eq!(field(&point, "dr_status"), None);
        assert_eq!(field(&point, "not_a_listed_field"), None);
    }

    #[test]
    fn non_energy_measurement_skips_obis_promotion() {
        let mut meta = std::collections::HashMap::new();
        // Even if an OBIS-named numeric field is present, a battery point ignores it.
        meta.insert("max_demand_import_kw".to_string(), serde_json::json!(32.0));
        let reading = DeviceReading {
            device_type: DeviceType::Battery,
            metrics: DeviceMetrics::BatteryState {
                soc_percent: 80.0,
                power_kw: 1.0,
                temperature_c: 25.0,
                mode: aggregator_core::models::BatteryMode::Idle,
            },
            ..energy_reading_with_metadata(meta)
        };

        let point = reading_to_point(&reading).expect("representable");
        assert_eq!(point.measurement, "battery");
        assert_eq!(field(&point, "max_demand_import_kw"), None);
    }

    #[tokio::test]
    async fn test_router_explicit_zone_id() {
        let reading = DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: "DEV-123".to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: "SN-999".to_string(),
            zone_code: Some("ZONE5".to_string()),
            timestamp: gridtokenx_telemetry::time::now(),
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
