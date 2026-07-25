use super::ProtocolStack;
use aggregator_core::models::{DeviceMetrics, DeviceReading, DeviceType};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

/// DLMS/COSEM Protocol Stack.
/// Handles commercial and industrial smart meter protocol packets.
#[derive(Default)]
pub struct DlmsStack;

use serde_json::Value;
use std::collections::HashMap;

/// Common OBIS (Object Identification System) codes for IEC 62056.
/// Format: A.B.C.D.E.F
mod obis {
    // Electricity (Medium 1)
    pub const ELEC_ACTIVE_IMPORT_TOTAL: &str = "1.1.1.8.0.255";
    pub const ELEC_ACTIVE_EXPORT_TOTAL: &str = "1.1.2.8.0.255";
    // Tariff-split active energy (residential TOU): rate 1 = peak, rate 2 = off-peak.
    // The total registers above stay the settlement energy; these are billing detail.
    pub const ELEC_ACTIVE_IMPORT_RATE1: &str = "1.1.1.8.1.255";
    pub const ELEC_ACTIVE_IMPORT_RATE2: &str = "1.1.1.8.2.255";
    pub const ELEC_ACTIVE_EXPORT_RATE1: &str = "1.1.2.8.1.255";
    pub const ELEC_ACTIVE_EXPORT_RATE2: &str = "1.1.2.8.2.255";
    pub const ELEC_MAX_DEMAND_IMPORT: &str = "1.1.1.6.0.255";
    // Sum active instantaneous power (A+ − A−), signed — the simulator's net power.
    // Distinct from ELEC_TOTAL_ACTIVE_POWER (1.1.1.7.0.255), which is positive-only.
    pub const ELEC_SUM_ACTIVE_POWER: &str = "1.1.16.7.0.255";
    pub const ELEC_REACTIVE_IMPORT_TOTAL: &str = "1.1.3.8.0.255";
    pub const ELEC_REACTIVE_EXPORT_TOTAL: &str = "1.1.4.8.0.255";
    pub const ELEC_VOLTAGE_L1: &str = "1.1.32.7.0.255";
    pub const ELEC_VOLTAGE_L2: &str = "1.1.52.7.0.255";
    pub const ELEC_VOLTAGE_L3: &str = "1.1.72.7.0.255";
    pub const ELEC_CURRENT_L1: &str = "1.1.31.7.0.255";
    pub const ELEC_CURRENT_L2: &str = "1.1.51.7.0.255";
    pub const ELEC_CURRENT_L3: &str = "1.1.71.7.0.255";
    pub const ELEC_TOTAL_ACTIVE_POWER: &str = "1.1.1.7.0.255";
    pub const ELEC_FREQUENCY: &str = "1.1.14.7.0.255";
    pub const ELEC_POWER_FACTOR: &str = "1.1.13.7.0.255";

    // Gas (Medium 7)
    pub const GAS_VOLUME_TOTAL: &str = "7.0.11.0.0.255";
    // Kept for OBIS-dictionary completeness even while no decoder reads it.
    #[allow(dead_code)]
    pub const GAS_TEMPERATURE: &str = "7.0.41.0.0.255";

    // Water (Medium 8)
    pub const WATER_VOLUME_TOTAL: &str = "8.0.11.0.0.255";

    // Demand Response / Abstract
    pub const DR_STATUS: &str = "0.0.96.10.0.255";
    pub const ACTIVE_TARIFF: &str = "0.0.96.14.0.255";

    // Grid-asset state (BESS / EV), abstract manufacturer-specific range. Metadata
    // only — they do not drive settlement energy. Values arrive in engineering
    // units (SoC %, kW) and are stored as-is.
    pub const BATTERY_SOC: &str = "0.0.96.130.0.255";
    pub const BATTERY_DISPATCH: &str = "0.0.96.131.0.255";
    pub const EV_CHARGE_POWER: &str = "0.0.96.132.0.255";
}

impl DlmsStack {
    pub fn new() -> Self {
        Self
    }

    /// Internal mapper to translate OBIS codes to platform metrics.
    fn map_payload(&self, payload: &HashMap<String, Value>) -> (f64, f64, HashMap<String, Value>) {
        let mut generated_wh = 0.0;
        let mut consumed_wh = 0.0;
        let mut metadata = HashMap::new();
        // Plain JSON energy keys (kWh). REST meters sign over these via
        // `canonical_sign_value` (aggregator-api handlers.rs), so they must land in
        // the same metrics or the signed value diverges from the stored one. OBIS
        // registers take precedence when both forms appear (HashMap iteration order
        // is random — resolve after the loop, not by last-write-wins).
        let mut plain_generated_kwh: Option<f64> = None;
        let mut plain_consumed_kwh: Option<f64> = None;
        let mut obis_generated = false;
        let mut obis_consumed = false;

        for (key, val) in payload {
            match key.as_str() {
                // Electricity - Active Energy
                obis::ELEC_ACTIVE_IMPORT_TOTAL => {
                    consumed_wh = val.as_f64().unwrap_or(0.0);
                    obis_consumed = true;
                    metadata.insert("obis_active_import".to_string(), val.clone());
                }
                obis::ELEC_ACTIVE_EXPORT_TOTAL => {
                    generated_wh = val.as_f64().unwrap_or(0.0);
                    obis_generated = true;
                    metadata.insert("obis_active_export".to_string(), val.clone());
                }
                "energy_generated" => {
                    plain_generated_kwh = val.as_f64();
                    metadata.insert(key.clone(), val.clone());
                }
                "energy_consumed" => {
                    plain_consumed_kwh = val.as_f64();
                    metadata.insert(key.clone(), val.clone());
                }

                // Electricity - Tariff-split active energy (TOU). Metadata only —
                // these do NOT drive consumed/generated (the totals above own that)
                // to avoid double-counting. Stored as kWh for symmetry with the
                // settlement energy.
                obis::ELEC_ACTIVE_IMPORT_RATE1 => {
                    metadata.insert(
                        "active_import_rate1_kwh".to_string(),
                        Value::from(val.as_f64().unwrap_or(0.0) / 1000.0),
                    );
                }
                obis::ELEC_ACTIVE_IMPORT_RATE2 => {
                    metadata.insert(
                        "active_import_rate2_kwh".to_string(),
                        Value::from(val.as_f64().unwrap_or(0.0) / 1000.0),
                    );
                }
                obis::ELEC_ACTIVE_EXPORT_RATE1 => {
                    metadata.insert(
                        "active_export_rate1_kwh".to_string(),
                        Value::from(val.as_f64().unwrap_or(0.0) / 1000.0),
                    );
                }
                obis::ELEC_ACTIVE_EXPORT_RATE2 => {
                    metadata.insert(
                        "active_export_rate2_kwh".to_string(),
                        Value::from(val.as_f64().unwrap_or(0.0) / 1000.0),
                    );
                }
                obis::ELEC_MAX_DEMAND_IMPORT => {
                    metadata.insert("max_demand_import_kw".to_string(), val.clone());
                }
                obis::ELEC_SUM_ACTIVE_POWER => {
                    metadata.insert("sum_active_power_kw".to_string(), val.clone());
                }

                // Electricity - Reactive Energy
                obis::ELEC_REACTIVE_IMPORT_TOTAL => {
                    metadata.insert(
                        "reactive_energy_import_kvarh".to_string(),
                        Value::from(val.as_f64().unwrap_or(0.0) / 1000.0),
                    );
                }
                obis::ELEC_REACTIVE_EXPORT_TOTAL => {
                    metadata.insert(
                        "reactive_energy_export_kvarh".to_string(),
                        Value::from(val.as_f64().unwrap_or(0.0) / 1000.0),
                    );
                }

                // Electricity - Grid Metrics
                obis::ELEC_VOLTAGE_L1 => {
                    metadata.insert("voltage_l1_v".to_string(), val.clone());
                    metadata.insert("voltage_v".to_string(), val.clone()); // Legacy compatibility
                }
                obis::ELEC_VOLTAGE_L2 => {
                    metadata.insert("voltage_l2_v".to_string(), val.clone());
                }
                obis::ELEC_VOLTAGE_L3 => {
                    metadata.insert("voltage_l3_v".to_string(), val.clone());
                }
                obis::ELEC_CURRENT_L1 => {
                    metadata.insert("current_l1_a".to_string(), val.clone());
                    metadata.insert("current_a".to_string(), val.clone()); // Legacy compatibility
                }
                obis::ELEC_CURRENT_L2 => {
                    metadata.insert("current_l2_a".to_string(), val.clone());
                }
                obis::ELEC_CURRENT_L3 => {
                    metadata.insert("current_l3_a".to_string(), val.clone());
                }
                obis::ELEC_TOTAL_ACTIVE_POWER => {
                    metadata.insert("total_active_power_w".to_string(), val.clone());
                }
                obis::ELEC_FREQUENCY => {
                    metadata.insert("frequency_hz".to_string(), val.clone());
                }
                obis::ELEC_POWER_FACTOR => {
                    metadata.insert("power_factor".to_string(), val.clone());
                }

                // Gas & Water (Normalized to consumed energy/volume for platform tracking)
                obis::GAS_VOLUME_TOTAL => {
                    consumed_wh = val.as_f64().unwrap_or(0.0); // Simplified mapping for tracking
                    metadata.insert("utility_type".to_string(), Value::from("gas"));
                    metadata.insert("volume_m3".to_string(), val.clone());
                }
                obis::WATER_VOLUME_TOTAL => {
                    consumed_wh = val.as_f64().unwrap_or(0.0);
                    metadata.insert("utility_type".to_string(), Value::from("water"));
                    metadata.insert("volume_m3".to_string(), val.clone());
                }

                // Demand Response
                obis::DR_STATUS => {
                    metadata.insert("demand_response_status".to_string(), val.clone());
                }
                obis::ACTIVE_TARIFF => {
                    metadata.insert("active_tariff".to_string(), val.clone());
                }

                // Grid-asset state (BESS / EV). Named so the Influx/Kafka
                // named-key sinks can promote them; without a name they would sit
                // in metadata under their raw OBIS code and stay invisible.
                obis::BATTERY_SOC => {
                    metadata.insert("battery_soc_percent".to_string(), val.clone());
                }
                obis::BATTERY_DISPATCH => {
                    metadata.insert("battery_dispatch_kw".to_string(), val.clone());
                }
                obis::EV_CHARGE_POWER => {
                    metadata.insert("ev_charging_kw".to_string(), val.clone());
                }

                // Fallback for metadata (non-OBIS extra fields)
                _ => {
                    metadata.insert(key.clone(), val.clone());
                }
            }
        }

        if !obis_generated {
            if let Some(kwh) = plain_generated_kwh {
                generated_wh = kwh * 1000.0;
            }
        }
        if !obis_consumed {
            if let Some(kwh) = plain_consumed_kwh {
                consumed_wh = kwh * 1000.0;
            }
        }

        (generated_wh / 1000.0, consumed_wh / 1000.0, metadata)
    }
}

#[async_trait]
impl ProtocolStack for DlmsStack {
    async fn handle_message(
        &self,
        device_id: &str,
        raw_data: &[u8],
    ) -> Result<Option<DeviceReading>> {
        // Parse DLMS telemetry from dynamic JSON representation
        let payload: HashMap<String, Value> = serde_json::from_slice(raw_data)
            .context("Failed to parse dynamic DLMS telemetry data")?;

        let (generated_kwh, consumed_kwh, mut metadata) = self.map_payload(&payload);

        // Add IEC 62056 compliance flag and COSEM modeling info
        metadata.insert(
            "protocol_standard".to_string(),
            Value::from("IEC 62056 (DLMS/COSEM)"),
        );
        metadata.insert(
            "cosem_modeling".to_string(),
            Value::from("active_attribute_proxy"),
        );

        // Meter-reported timestamp drives the 15-minute billing windows (offline
        // meters batch backdated readings); fall back to arrival time when the
        // field is absent or unparseable.
        let timestamp = payload
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(gridtokenx_telemetry::time::now);

        // Optional microgrid zone tag from the meter payload. The router parses
        // its numeric suffix to pick the zone_<n> stream; accept a string code
        // ("2", "zone_2") or a bare JSON number. Absent => router falls back to
        // serial-number hashing.
        let zone_code = payload.get("zone_code").and_then(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        });

        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: device_id.to_string(),
            zone_code,
            timestamp,
            metrics: DeviceMetrics::Energy {
                generated_kwh,
                consumed_kwh,
                net_kwh: generated_kwh - consumed_kwh,
            },
            metadata,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_dlms_obis_code_mapping() {
        let stack = DlmsStack::new();
        // Payload using formal OBIS codes including multi-phase and power
        let payload = json!({
            "1.1.1.8.0.255": 10000.0, // Active Import Wh
            "1.1.2.8.0.255": 5000.0,  // Active Export Wh
            "1.1.72.7.0.255": 232.5,  // Voltage L3
            "1.1.1.7.0.255": 1500.0,  // Total Active Power W
            "0.0.96.10.0.255": "load_shedding"
        });
        let raw = serde_json::to_vec(&payload).unwrap();

        let result = stack.handle_message("OBIS-MTR", &raw).await.unwrap();
        let reading = result.unwrap();

        if let DeviceMetrics::Energy {
            generated_kwh,
            consumed_kwh,
            ..
        } = reading.metrics
        {
            assert_eq!(generated_kwh, 5.0);
            assert_eq!(consumed_kwh, 10.0);
        }
        assert_eq!(reading.metadata.get("voltage_l3_v").unwrap(), &json!(232.5));
        assert_eq!(
            reading.metadata.get("total_active_power_w").unwrap(),
            &json!(1500.0)
        );
        assert_eq!(
            reading.metadata.get("demand_response_status").unwrap(),
            &json!("load_shedding")
        );
    }

    #[tokio::test]
    async fn test_dlms_bess_ev_registers_decode_to_named_metadata() {
        let stack = DlmsStack::new();
        // Grid-asset registers must decode to named metadata keys so the Influx /
        // Kafka named-key sinks can promote them (raw OBIS codes are invisible).
        let payload = json!({
            "1.1.1.8.0.255": 10000.0,     // keep it a valid energy reading
            "1.1.2.8.0.255": 5000.0,
            "0.0.96.130.0.255": 57.5,     // battery SoC %
            "0.0.96.131.0.255": 160.0,    // battery dispatch kW (discharge)
            "0.0.96.132.0.255": 128.4     // EV charging kW
        });
        let raw = serde_json::to_vec(&payload).unwrap();

        let reading = stack.handle_message("OBIS-BESS", &raw).await.unwrap().unwrap();

        assert_eq!(
            reading.metadata.get("battery_soc_percent").unwrap(),
            &json!(57.5)
        );
        assert_eq!(
            reading.metadata.get("battery_dispatch_kw").unwrap(),
            &json!(160.0)
        );
        assert_eq!(
            reading.metadata.get("ev_charging_kw").unwrap(),
            &json!(128.4)
        );
        // Raw OBIS codes must NOT leak through once named.
        assert!(reading.metadata.get("0.0.96.130.0.255").is_none());
    }

    #[tokio::test]
    async fn test_dlms_plain_energy_keys_and_meter_timestamp() {
        let stack = DlmsStack::new();
        // REST meters sign over plain energy keys (canonical_sign_value) and
        // report their own timestamp — both must survive decoding.
        let payload = json!({
            "device_id": "PLAIN-MTR",
            "timestamp": "2026-06-11T10:07:30.000Z",
            "energy_generated": 20.0,
            "energy_consumed": 3.5,
            "signature": "ignored-here"
        });
        let raw = serde_json::to_vec(&payload).unwrap();

        let reading = stack
            .handle_message("PLAIN-MTR", &raw)
            .await
            .unwrap()
            .unwrap();

        if let DeviceMetrics::Energy {
            generated_kwh,
            consumed_kwh,
            net_kwh,
        } = reading.metrics
        {
            assert_eq!(generated_kwh, 20.0);
            assert_eq!(consumed_kwh, 3.5);
            assert_eq!(net_kwh, 16.5);
        } else {
            panic!("expected Energy metrics");
        }
        assert_eq!(
            reading.timestamp,
            chrono::DateTime::parse_from_rfc3339("2026-06-11T10:07:30.000Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[tokio::test]
    async fn test_dlms_obis_overrides_plain_energy_keys() {
        let stack = DlmsStack::new();
        let payload = json!({
            "1.1.2.8.0.255": 5000.0,   // OBIS export 5 kWh — wins
            "energy_generated": 99.0,  // plain key — ignored when OBIS present
        });
        let raw = serde_json::to_vec(&payload).unwrap();

        let reading = stack
            .handle_message("MIX-MTR", &raw)
            .await
            .unwrap()
            .unwrap();
        if let DeviceMetrics::Energy { generated_kwh, .. } = reading.metrics {
            assert_eq!(generated_kwh, 5.0);
        } else {
            panic!("expected Energy metrics");
        }
    }

    #[tokio::test]
    async fn test_dlms_zone_code_drives_device_reading_zone() {
        let stack = DlmsStack::new();

        // String zone code (as the simulator sends it) -> routed by zone.
        let raw = serde_json::to_vec(&json!({
            "1.1.1.8.0.255": 1000.0,
            "zone_code": "2",
        }))
        .unwrap();
        let reading = stack.handle_message("Z-MTR", &raw).await.unwrap().unwrap();
        assert_eq!(reading.zone_code.as_deref(), Some("2"));

        // Bare JSON number is also accepted (stringified).
        let raw = serde_json::to_vec(&json!({
            "1.1.1.8.0.255": 1000.0,
            "zone_code": 3,
        }))
        .unwrap();
        let reading = stack.handle_message("Z-MTR", &raw).await.unwrap().unwrap();
        assert_eq!(reading.zone_code.as_deref(), Some("3"));

        // Absent -> None (router falls back to serial-number hashing).
        let raw = serde_json::to_vec(&json!({ "1.1.1.8.0.255": 1000.0 })).unwrap();
        let reading = stack.handle_message("Z-MTR", &raw).await.unwrap().unwrap();
        assert_eq!(reading.zone_code, None);
    }

    #[tokio::test]
    async fn test_dlms_residential_tou_registers_are_metadata_not_double_counted() {
        let stack = DlmsStack::new();
        // Peak interval: total import 10 kWh, all in rate 1; plus max demand and
        // the active-tariff indicator. Totals must equal the *_8_0 registers only.
        let payload = json!({
            "1.1.1.8.0.255": 10000.0, // total import 10 kWh
            "1.1.2.8.0.255": 5000.0,  // total export 5 kWh
            "1.1.1.8.1.255": 10000.0, // import rate 1 (peak) Wh
            "1.1.2.8.1.255": 5000.0,  // export rate 1 (peak) Wh
            "1.1.1.6.0.255": 32.0,    // max demand kW
            "1.1.16.7.0.255": -3.944, // sum active power (net, exporting) kW
            "0.0.96.14.0.255": 1,     // active tariff = peak
        });
        let raw = serde_json::to_vec(&payload).unwrap();
        let reading = stack
            .handle_message("TOU-MTR", &raw)
            .await
            .unwrap()
            .unwrap();

        if let DeviceMetrics::Energy {
            generated_kwh,
            consumed_kwh,
            ..
        } = reading.metrics
        {
            // Rate registers must NOT add to the totals.
            assert_eq!(consumed_kwh, 10.0);
            assert_eq!(generated_kwh, 5.0);
        } else {
            panic!("expected Energy metrics");
        }
        assert_eq!(
            reading.metadata.get("active_import_rate1_kwh").unwrap(),
            &json!(10.0)
        );
        assert_eq!(
            reading.metadata.get("active_export_rate1_kwh").unwrap(),
            &json!(5.0)
        );
        assert_eq!(
            reading.metadata.get("max_demand_import_kw").unwrap(),
            &json!(32.0)
        );
        assert_eq!(
            reading.metadata.get("sum_active_power_kw").unwrap(),
            &json!(-3.944)
        );
        assert_eq!(reading.metadata.get("active_tariff").unwrap(), &json!(1));
    }

    #[tokio::test]
    async fn test_dlms_multi_utility_gas() {
        let stack = DlmsStack::new();
        let payload = json!({
            "7.0.11.0.0.255": 123.456, // Gas Volume Corrected
            "utility_serial": "GAS-001"
        });
        let raw = serde_json::to_vec(&payload).unwrap();

        let result = stack.handle_message("GAS-MTR", &raw).await.unwrap();
        let reading = result.unwrap();

        assert_eq!(reading.metadata.get("utility_type").unwrap(), &json!("gas"));
        assert_eq!(reading.metadata.get("volume_m3").unwrap(), &json!(123.456));
    }

    #[tokio::test]
    async fn test_dlms_handle_invalid_json() {
        let stack = DlmsStack::new();
        let payload = r#"{ "bad": "json" "#;
        let result = stack.handle_message("MTR-X", payload.as_bytes()).await;
        assert!(result.is_err());
    }
}
