use super::ProtocolStack;
use crate::models::{DeviceMetrics, DeviceReading, DeviceType};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

/// SunSpec (Modbus TCP) Protocol Stack.
/// Maps IEEE 1547 inverter models to canonical battery/solar models.
pub struct SunSpecStack;

#[derive(serde::Deserialize)]
struct SunSpecModel103 {
    #[serde(rename = "W")]
    watts: f64,
    #[serde(rename = "W_SF")]
    watts_sf: i8,
    #[serde(rename = "WH")]
    watt_hours: f64,
    #[serde(rename = "WH_SF")]
    watt_hours_sf: i8,
    #[serde(rename = "St")]
    status: u16,
}

impl SunSpecStack {
    pub fn new() -> Self {
        Self
    }

    fn apply_sf(value: f64, sf: i8) -> f64 {
        value * 10.0f64.powi(sf as i32)
    }
}

#[async_trait]
impl ProtocolStack for SunSpecStack {
    async fn handle_message(
        &self,
        device_id: &str,
        raw_data: &[u8],
    ) -> Result<Option<DeviceReading>> {
        // In this implementation, we expect a JSON represention of SunSpec registers
        // from Model 103 (Three Phase Inverter).
        let model: SunSpecModel103 =
            serde_json::from_slice(raw_data).context("Failed to parse SunSpec Model 103 data")?;

        let power_kw = Self::apply_sf(model.watts, model.watts_sf) / 1000.0;
        let energy_kwh = Self::apply_sf(model.watt_hours, model.watt_hours_sf) / 1000.0;

        // Map SunSpec Status to BatteryMode/DeviceType context
        let mode = match model.status {
            2 => crate::models::BatteryMode::Discharging, // 2 = Producing
            4 => crate::models::BatteryMode::Charging,    // 4 = Charging (for Storage)
            _ => crate::models::BatteryMode::Idle,
        };

        Ok(Some(DeviceReading {
            reading_id: Uuid::new_v4(),
            device_id: device_id.to_string(),
            device_type: DeviceType::Battery, // Model 103 is generic, but often used for Storage in this system
            serial_number: device_id.to_string(),
            zone_code: None, // Will be filled from request if provided
            timestamp: Utc::now() - chrono::Duration::hours(12),
            metrics: DeviceMetrics::BatteryState {
                soc_percent: 0.0, // Model 103 doesn't have SoC, would need Model 802
                power_kw,
                temperature_c: 0.0,
                mode,
            },
            metadata: {
                let mut map = std::collections::HashMap::new();
                map.insert("total_energy_kwh".to_string(), Value::from(energy_kwh));
                map.insert("sunspec_status".to_string(), Value::from(model.status));
                map
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sunspec_apply_sf() {
        assert_eq!(SunSpecStack::apply_sf(100.0, 0), 100.0);
        assert_eq!(SunSpecStack::apply_sf(123.0, -1), 12.3);
        assert_eq!(SunSpecStack::apply_sf(5.0, 2), 500.0);
    }

    #[tokio::test]
    async fn test_sunspec_handle_valid_message() {
        let stack = SunSpecStack::new();
        let payload = r#"{
            "W": 5000,
            "W_SF": -1,
            "WH": 100000,
            "WH_SF": -2,
            "St": 2
        }"#;

        let result = stack
            .handle_message("INV-001", payload.as_bytes())
            .await
            .unwrap();
        let reading = result.unwrap();

        assert_eq!(reading.device_id, "INV-001");
        if let DeviceMetrics::BatteryState { power_kw, mode, .. } = reading.metrics {
            assert_eq!(power_kw, 0.5); // (5000 * 10^-1) / 1000 = 0.5
            assert_eq!(mode, crate::models::BatteryMode::Discharging);
        } else {
            panic!("Wrong metric type");
        }

        let energy_kwh = reading
            .metadata
            .get("total_energy_kwh")
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!(energy_kwh, 1.0); // (100000 * 10^-2) / 1000 = 1.0
    }

    #[tokio::test]
    async fn test_sunspec_mode_mapping() {
        let stack = SunSpecStack::new();
        let test_cases = vec![
            (2, crate::models::BatteryMode::Discharging),
            (4, crate::models::BatteryMode::Charging),
            (1, crate::models::BatteryMode::Idle),
        ];

        for (st, expected_mode) in test_cases {
            let payload = format!(
                r#"{{
                "W": 0, "W_SF": 0, "WH": 0, "WH_SF": 0, "St": {}
            }}"#,
                st
            );

            let result = stack
                .handle_message("INV-001", payload.as_bytes())
                .await
                .unwrap();
            let reading = result.unwrap();
            if let DeviceMetrics::BatteryState { mode, .. } = reading.metrics {
                assert_eq!(mode, expected_mode);
            }
        }
    }
}
