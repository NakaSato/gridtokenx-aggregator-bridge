use std::collections::HashMap;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;
use tracing::info;
use crate::ingester::MeterReadingPayload;

pub struct Aggregator {
    // Per-meter state: (last_reading, count)
    meter_stats: HashMap<Uuid, (MeterReadingPayload, u64)>,
    // Per-zone state: (total_produced, total_consumed, count)
    zone_stats: HashMap<i32, (Decimal, Decimal, u64)>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            meter_stats: HashMap::new(),
            zone_stats: HashMap::new(),
        }
    }

    pub fn handle_reading(&mut self, payload: MeterReadingPayload) {
        // 1. Update Meter Stats
        let entry = self.meter_stats.entry(payload.meter_id).or_insert((payload.clone(), 0));
        entry.0 = payload.clone();
        entry.1 += 1;

        // 2. Update Zone Stats
        if let Some(zone_id) = payload.zone_id {
            let z_entry = self.zone_stats.entry(zone_id).or_insert((Decimal::ZERO, Decimal::ZERO, 0));
            z_entry.0 += payload.energy_generated.unwrap_or(Decimal::ZERO);
            z_entry.1 += payload.energy_consumed.unwrap_or(Decimal::ZERO);
            z_entry.2 += 1;
        }

        info!("🧮 Aggregated reading for meter {} in zone {:?}", payload.meter_serial, payload.zone_id);
    }

    pub fn get_zone_summary(&self, zone_id: i32) -> Option<(Decimal, Decimal, u64)> {
        self.zone_stats.get(&zone_id).copied()
    }
}
