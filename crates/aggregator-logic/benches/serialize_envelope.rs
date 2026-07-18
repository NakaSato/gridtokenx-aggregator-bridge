//! Micro-benchmark for the per-reading stream-envelope serialization on the
//! ingest hot path (`Router::disseminate`, `crates/aggregator-logic/src/router.rs`).
//!
//! Isolates exactly the change made under "Chapter 3 — Performance Mindset":
//! replacing the `json!({..})` path (which materializes a throwaway
//! `serde_json::Value` tree — a full nested-map clone of the whole reading —
//! before re-serializing it) with a borrowed `#[derive(Serialize)]` envelope
//! that serializes straight to the string in one pass.
//!
//! The two functions below are byte-for-value identical (proven by the unit
//! tests `stream_envelope_matches_legacy_json_macro_output` in router.rs); this
//! bench measures only their cost. Run:
//!   cargo bench -p aggregator-logic --bench serialize_envelope

use std::collections::HashMap;

use aggregator_core::models::{DeviceMetrics, DeviceReading, DeviceType};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;
use uuid::Uuid;

/// OLD path: build a `serde_json::Value` tree via `json!`, then serialize it.
fn serialize_legacy(event_type: &str, reading: &DeviceReading) -> String {
    serde_json::to_string(&json!({
        "event_type": event_type,
        "payload": reading,
    }))
    .unwrap()
}

/// NEW path: serialize a borrowed envelope directly — no intermediate Value.
#[derive(serde::Serialize)]
struct StreamEnvelope<'a> {
    event_type: &'a str,
    payload: &'a DeviceReading,
}

fn serialize_borrowed(event_type: &str, reading: &DeviceReading) -> String {
    serde_json::to_string(&StreamEnvelope {
        event_type,
        payload: reading,
    })
    .unwrap()
}

/// A representative smart-meter reading carrying the full decoded residential
/// OBIS register set in metadata — the realistic hot-path payload (see
/// `router::reading_to_point` `EXTRA_FIELDS` and `dlms.rs`).
fn sample_reading() -> DeviceReading {
    let mut metadata: HashMap<String, serde_json::Value> = HashMap::new();
    for (k, v) in [
        ("sum_active_power_kw", json!(-3.944)),
        ("max_demand_import_kw", json!(32.0)),
        ("active_tariff", json!(1)),
        ("active_import_rate1_kwh", json!(120.5)),
        ("active_import_rate2_kwh", json!(80.25)),
        ("active_export_rate1_kwh", json!(10.0)),
        ("active_export_rate2_kwh", json!(5.0)),
        ("reactive_energy_import_kvarh", json!(2.5)),
        ("reactive_energy_export_kvarh", json!(1.25)),
        ("voltage_l1_v", json!(230.4)),
        ("frequency_hz", json!(49.98)),
        ("power_factor", json!(0.97)),
        ("signature", json!("3b9f...ed25519sig...base64")),
        ("zone_code", json!("ZONE3")),
    ] {
        metadata.insert(k.to_string(), v);
    }

    DeviceReading {
        reading_id: Uuid::new_v4(),
        device_id: "MTR-000123456789".to_string(),
        device_type: DeviceType::SmartMeter,
        serial_number: "MTR-000123456789".to_string(),
        zone_code: Some("ZONE3".to_string()),
        timestamp: gridtokenx_telemetry::time::now(),
        metrics: DeviceMetrics::Energy {
            generated_kwh: 12.5,
            consumed_kwh: 8.25,
            net_kwh: 4.25,
        },
        metadata,
    }
}

fn bench_serialize(c: &mut Criterion) {
    let reading = sample_reading();
    let event_type = "SmartMeterReading";

    let mut group = c.benchmark_group("disseminate_serialize");
    group.bench_function("legacy_json_macro", |b| {
        b.iter(|| serialize_legacy(black_box(event_type), black_box(&reading)))
    });
    group.bench_function("borrowed_envelope", |b| {
        b.iter(|| serialize_borrowed(black_box(event_type), black_box(&reading)))
    });
    group.finish();
}

criterion_group!(benches, bench_serialize);
criterion_main!(benches);
