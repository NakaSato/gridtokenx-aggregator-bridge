use super::ProtocolStack;
use aggregator_core::models::{DeviceMetrics, DeviceReading, DeviceType};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

/// DLMS/COSEM Protocol Stack.
/// Handles commercial and industrial smart meter protocol packets.
pub struct DlmsStack;

use serde_json::Value;
use std::collections::HashMap;

/// Common OBIS (Object Identification System) codes for IEC 62056.
/// Format: A.B.C.D.E.F
mod obis {
    // Electricity (Medium 1)
    pub const ELEC_ACTIVE_IMPORT_TOTAL: &str = "1.1.1.8.0.255";
    pub const ELEC_ACTIVE_EXPORT_TOTAL: &str = "1.1.2.8.0.255";
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
    pub const GAS_TEMPERATURE: &str = "7.0.41.0.0.255";

    // Water (Medium 8)
    pub const WATER_VOLUME_TOTAL: &str = "8.0.11.0.0.255";

    // Demand Response / Abstract
    pub const DR_STATUS: &str = "0.0.96.10.0.255";
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

        for (key, val) in payload {
            match key.as_str() {
                // Electricity - Active Energy
                obis::ELEC_ACTIVE_IMPORT_TOTAL => {
                    consumed_wh = val.as_f64().unwrap_or(0.0);
                    metadata.insert("obis_active_import".to_string(), val.clone());
                }
                obis::ELEC_ACTIVE_EXPORT_TOTAL => {
                    generated_wh = val.as_f64().unwrap_or(0.0);
                    metadata.insert("obis_active_export".to_string(), val.clone());
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

                // Fallback for metadata (non-OBIS extra fields)
                _ => {
                    metadata.insert(key.clone(), val.clone());
                }
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

        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::SmartMeter,
            serial_number: device_id.to_string(),
            zone_code: None,
            timestamp: Utc::now(),
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
