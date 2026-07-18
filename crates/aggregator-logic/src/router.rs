use anyhow::{anyhow, Context, Result};
use redis::aio::ConnectionManager;
use redis::streams::StreamMaxlen;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use aggregator_core::models::{DeviceMetrics, DeviceReading, DeviceType};
use aggregator_persistence::infra::influxdb::{InfluxWriter, TelemetryPoint};
use aggregator_persistence::infra::pg_readings::{PgReadingsWriter, ReadingRow};

/// Default maximum number of entries per Redis stream.
const DEFAULT_MAX_STREAM_LEN: usize = 100_000;

/// Borrowed wire envelope for a plaintext stream entry (`{event_type, payload}`).
///
/// Serialized directly from borrowed fields on the per-reading hot path — this
/// avoids the throwaway `serde_json::Value` tree that `json!({..})` builds (a
/// full nested-map clone of the whole reading) before re-serializing it to a
/// string. One pass, no intermediate allocation. The consumer
/// (`zone_ingester::decode_entry` → `from_str::<Event>`) deserializes by key,
/// so field order is irrelevant to it.
#[derive(serde::Serialize)]
struct StreamEnvelope<'a> {
    event_type: &'a str,
    payload: &'a DeviceReading,
}

/// Borrowed wire envelope for an at-rest-encrypted stream entry
/// (`{event_type, enc:{nonce, ciphertext}}`). Same zero-intermediate rationale
/// as [`StreamEnvelope`].
#[derive(serde::Serialize)]
struct EncStreamEnvelope<'a> {
    event_type: &'a str,
    enc: EncBody<'a>,
}

#[derive(serde::Serialize)]
struct EncBody<'a> {
    nonce: &'a str,
    ciphertext: &'a str,
}

/// Self-healing cache of a reconnecting resource (here a Redis `ConnectionManager`).
///
/// Holds the last-built handle; [`invalidate`](Self::invalidate) drops it so the
/// next [`get_or_build`](Self::get_or_build) rebuilds via the supplied async
/// builder. This is the state machine behind `Router`'s retry-once self-heal: on a
/// transport error the caller `invalidate`s then `get_or_build`s, so a Redis
/// restart is recovered inline instead of freezing the bridge. Generic + builder-
/// injected so the rebuild/cache/error semantics are unit-testable without Redis.
struct ReconnectCache<T: Clone> {
    cached: Mutex<Option<T>>,
}

impl<T: Clone> ReconnectCache<T> {
    /// New cache, optionally pre-seeded with an already-built handle.
    fn new(initial: Option<T>) -> Self {
        Self {
            cached: Mutex::new(initial),
        }
    }

    /// Return the cached handle if present, else build one via `build`, cache it,
    /// and return it. The builder runs **only** on a cache miss; a build error
    /// propagates and leaves the cache empty (so the next call retries).
    async fn get_or_build<F, Fut, E>(&self, build: F) -> std::result::Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
    {
        {
            let guard = self.cached.lock().await;
            if let Some(c) = guard.as_ref() {
                return Ok(c.clone());
            }
        }
        let built = build().await?;
        let mut guard = self.cached.lock().await;
        *guard = Some(built.clone());
        Ok(built)
    }

    /// Drop the cached handle so the next [`get_or_build`](Self::get_or_build) rebuilds.
    async fn invalidate(&self) {
        let mut guard = self.cached.lock().await;
        *guard = None;
    }
}

/// Routes normalized `DeviceReading` events to zone-partitioned Redis Streams.
pub struct Router {
    /// Redis URL used to rebuild the connection after a server restart.
    redis_url: String,
    /// Self-healing connection cache; rebuilt on transport error so a Redis
    /// restart is recovered inline rather than failing the first request.
    cache: ReconnectCache<ConnectionManager>,
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
    /// Optional Postgres `meter_readings` sink so the dashboard (meter-service,
    /// read-only) can list Recent Readings. Fire-and-forget; owner+wallet
    /// resolved inside the INSERT. None ⇒ disabled (default).
    pg_readings: Option<Arc<PgReadingsWriter>>,
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
            cache: ReconnectCache::new(Some(connection_manager)),
            max_stream_len,
            num_zones,
            influx,
            stream_cipher: None,
            pg_readings: None,
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

    /// Attach the optional Postgres `meter_readings` sink (dashboard history).
    pub fn with_pg_readings(mut self, writer: Option<Arc<PgReadingsWriter>>) -> Self {
        self.pg_readings = writer;
        self
    }

    /// Return a live connection manager clone, rebuilding from `redis_url` after
    /// an [`invalidate`](Self::invalidate). Errors loudly when Redis is down.
    async fn conn(&self) -> Result<ConnectionManager> {
        let url = self.redis_url.clone();
        self.cache
            .get_or_build(|| async move {
                let client = redis::Client::open(url.as_str())
                    .map_err(|e| anyhow!("Failed to open Redis client {}: {}", url, e))?;
                ConnectionManager::new(client)
                    .await
                    .map_err(|e| anyhow!("Failed to connect to Redis {}: {}", url, e))
            })
            .await
    }

    /// Drop the cached manager so the next [`conn`](Self::conn) rebuilds it.
    async fn invalidate(&self) {
        self.cache.invalidate().await;
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
            // Serialize the envelope in one pass from borrowed fields — no
            // intermediate `serde_json::Value` tree (see `EncStreamEnvelope`).
            serde_json::to_string(&EncStreamEnvelope {
                event_type,
                enc: EncBody {
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                },
            })
            .context("Failed to serialize encrypted reading")?
        } else {
            // Borrow the reading straight into the envelope — no throwaway Value
            // clone of the whole reading (see `StreamEnvelope`).
            serde_json::to_string(&StreamEnvelope {
                event_type,
                payload: reading,
            })
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

        // Mirror energy readings into the shared Postgres `meter_readings` table
        // so the dashboard's Recent Readings list is populated (owner+wallet
        // resolved inside the INSERT). Same fire-and-forget contract as InfluxDB.
        if let Some(pg) = &self.pg_readings {
            if let Some(row) = reading_to_row(reading) {
                pg.record(row);
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

/// Map a `DeviceReading` into a Postgres `meter_readings` row.
///
/// Only smart-meter **energy** readings are mirrored (EV/battery have no place in
/// the meter dashboard's energy history). `voltage`/`power_factor`/`frequency` are
/// pulled from the decoded OBIS metadata when present. Returns `None` for
/// non-energy metrics so nothing is written for them.
fn reading_to_row(reading: &DeviceReading) -> Option<ReadingRow> {
    let DeviceMetrics::Energy {
        generated_kwh,
        consumed_kwh,
        net_kwh,
    } = &reading.metrics
    else {
        return None;
    };
    let meta = |name: &str| reading.metadata.get(name).and_then(serde_json::Value::as_f64);
    Some(ReadingRow {
        serial_number: reading.serial_number.clone(),
        timestamp_ms: reading.timestamp.timestamp_millis(),
        generated_kwh: *generated_kwh,
        consumed_kwh: *consumed_kwh,
        surplus_kwh: net_kwh.max(0.0),
        deficit_kwh: (-net_kwh).max(0.0),
        voltage: meta("voltage_l1_v"),
        power_factor: meta("power_factor"),
        frequency: meta("frequency_hz"),
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

    // --- ReconnectCache: the Redis self-heal state machine (no Redis needed) ---

    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    #[tokio::test]
    async fn reconnect_cache_serves_seeded_handle_without_building() {
        let cache = ReconnectCache::new(Some("initial".to_string()));
        let builds = AtomicUsize::new(0);
        let got = cache
            .get_or_build(|| {
                builds.fetch_add(1, SeqCst);
                async { Ok::<_, ()>("rebuilt".to_string()) }
            })
            .await
            .unwrap();
        assert_eq!(got, "initial", "cached handle is served as-is");
        assert_eq!(builds.load(SeqCst), 0, "builder must not run on a cache hit");
    }

    #[tokio::test]
    async fn reconnect_cache_rebuilds_exactly_once_after_invalidate_then_caches() {
        let cache = ReconnectCache::new(Some(1u32));
        cache.invalidate().await; // simulate a transport error dropping the conn
        let builds = AtomicUsize::new(0);

        let v = cache
            .get_or_build(|| {
                builds.fetch_add(1, SeqCst);
                async { Ok::<_, ()>(42u32) }
            })
            .await
            .unwrap();
        assert_eq!(v, 42, "rebuilt handle returned");
        assert_eq!(builds.load(SeqCst), 1, "rebuilt exactly once");

        // A subsequent call hits the freshly-cached handle — no second rebuild.
        let v2 = cache
            .get_or_build(|| {
                builds.fetch_add(1, SeqCst);
                async { Ok::<_, ()>(99u32) }
            })
            .await
            .unwrap();
        assert_eq!(v2, 42, "second call serves the cached rebuild, not a new build");
        assert_eq!(builds.load(SeqCst), 1, "no rebuild on the cached path");
    }

    #[tokio::test]
    async fn reconnect_cache_propagates_build_error_and_stays_empty_for_retry() {
        let cache = ReconnectCache::<u32>::new(None);
        // First build fails (Redis still down) → error surfaces, cache stays empty.
        let err = cache
            .get_or_build(|| async { Err::<u32, &str>("redis down") })
            .await;
        assert_eq!(err, Err("redis down"));
        // Because the cache stayed empty, the next attempt retries the builder
        // (a failed rebuild must never poison the cache with a stale/None hit).
        let ok = cache
            .get_or_build(|| async { Ok::<u32, &str>(7) })
            .await
            .unwrap();
        assert_eq!(ok, 7);
    }

    /// The borrowed `StreamEnvelope` serialization must be byte-for-value
    /// identical to the old `json!({"event_type", "payload": reading})` path —
    /// the optimization only removes the intermediate Value tree, never changes
    /// the wire contract the zone ingester deserializes.
    #[test]
    fn stream_envelope_matches_legacy_json_macro_output() {
        let reading = energy_reading_with_metadata(std::collections::HashMap::new());
        let event_type = "SmartMeterReading";

        let new_str = serde_json::to_string(&StreamEnvelope {
            event_type,
            payload: &reading,
        })
        .unwrap();
        let legacy_str = serde_json::to_string(&serde_json::json!({
            "event_type": event_type,
            "payload": &reading,
        }))
        .unwrap();

        // Compare as parsed Values (key order is irrelevant to the consumer,
        // which deserializes by key) — proves identical semantic content.
        let new_val: serde_json::Value = serde_json::from_str(&new_str).unwrap();
        let legacy_val: serde_json::Value = serde_json::from_str(&legacy_str).unwrap();
        assert_eq!(new_val, legacy_val);
    }

    /// Same equivalence guarantee for the encrypted `enc` envelope branch.
    #[test]
    fn enc_stream_envelope_matches_legacy_json_macro_output() {
        let (nonce, ciphertext) = ("bm9uY2U=".to_string(), "Y2lwaGVy".to_string());
        let event_type = "SmartMeterReading";

        let new_str = serde_json::to_string(&EncStreamEnvelope {
            event_type,
            enc: EncBody {
                nonce: &nonce,
                ciphertext: &ciphertext,
            },
        })
        .unwrap();
        let legacy_str = serde_json::to_string(&serde_json::json!({
            "event_type": event_type,
            "enc": { "nonce": nonce, "ciphertext": ciphertext },
        }))
        .unwrap();

        let new_val: serde_json::Value = serde_json::from_str(&new_str).unwrap();
        let legacy_val: serde_json::Value = serde_json::from_str(&legacy_str).unwrap();
        assert_eq!(new_val, legacy_val);
    }

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

    /// Live XADD + self-heal end-to-end. Disseminates once on the seeded
    /// connection, then `invalidate`s and disseminates again — exercising the
    /// `ReconnectCache` rebuild path (the same recovery used after a Redis
    /// restart) against a real server, and verifies both entries land in the
    /// zone stream. Complements the Redis-free `reconnect_cache_*` unit tests.
    #[tokio::test]
    #[ignore = "requires a running Redis (default redis://127.0.0.1:6379, override REDIS_URL)"]
    async fn disseminate_and_self_heal_against_real_redis() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        // 4 zones; ZONE1 (from the helper) resolves to zone index 1 deterministically.
        let router = Router::new(&url, 4, None)
            .await
            .expect("router connects to Redis");
        let reading = energy_reading_with_metadata(std::collections::HashMap::new());
        let zone_stream = "gridtokenx:events:zone_1";

        let before: usize = {
            let mut conn = router.conn().await.expect("conn");
            conn.xlen(zone_stream).await.unwrap_or(0)
        };

        // 1) XADD on the seeded connection.
        let id1 = router.disseminate(&reading).await.expect("first disseminate");
        assert!(!id1.is_empty());

        // 2) Drop the cached manager, then disseminate again — must rebuild from
        //    the URL and succeed (the self-heal that survives a Redis restart).
        router.invalidate().await;
        let id2 = router
            .disseminate(&reading)
            .await
            .expect("disseminate after invalidate must rebuild the connection");
        assert!(!id2.is_empty());

        // Both readings actually landed in the zone stream.
        let mut conn = router.conn().await.expect("conn");
        let after: usize = conn.xlen(zone_stream).await.expect("xlen");
        assert!(
            after >= before + 2,
            "both disseminated readings present in {zone_stream} (before={before}, after={after})"
        );
    }
}
