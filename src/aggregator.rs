use chrono::{DateTime, Utc, Timelike};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;
use tracing::{debug, info};

/// Duration of a billing window in minutes
const WINDOW_MINUTES: u32 = 15;

#[derive(Debug, Clone)]
pub struct BillingBin {
    pub meter_id: Uuid,
    pub user_id: Uuid,
    pub meter_serial: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub energy_generated: Decimal,
    pub energy_consumed: Decimal,
    pub reading_count: u64,
}

pub struct Aggregator {
    /// active_bins: (meter_id, window_start_time) -> BillingBin
    active_bins: HashMap<(Uuid, DateTime<Utc>), BillingBin>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            active_bins: HashMap::new(),
        }
    }

    /// Handles a new meter reading and updates or creates the corresponding billing bin
    pub fn handle_reading(
        &mut self,
        meter_id: Uuid,
        user_id: Uuid,
        meter_serial: String,
        generated: Decimal,
        consumed: Decimal,
        timestamp: DateTime<Utc>,
    ) {
        let start_time = self.get_window_start(timestamp);
        let end_time = start_time + chrono::Duration::minutes(WINDOW_MINUTES as i64);

        let bin = self.active_bins.entry((meter_id, start_time)).or_insert_with(|| {
            debug!("🆕 Creating new billing bin for {} starting at {}", meter_serial, start_time);
            BillingBin {
                meter_id,
                user_id,
                meter_serial,
                start_time,
                end_time,
                energy_generated: Decimal::ZERO,
                energy_consumed: Decimal::ZERO,
                reading_count: 0,
            }
        });

        bin.energy_generated += generated;
        bin.energy_consumed += consumed;
        bin.reading_count += 1;
    }

    /// Returns a list of all billing bins that have reached their end time and removes them from the aggregator
    pub fn take_completed_bins(&mut self) -> Vec<BillingBin> {
        let now = Utc::now();
        let mut completed = Vec::new();
        let mut to_remove = Vec::new();

        for (key, bin) in &self.active_bins {
            if bin.end_time <= now {
                completed.push(bin.clone());
                to_remove.push(*key);
            }
        }

        for key in to_remove {
            self.active_bins.remove(&key);
        }

        if !completed.is_empty() {
            info!("📊 Aggregated {} completed billing bins", completed.len());
        }

        completed
    }

    /// Helper to calculate the start of the 15-minute window for a given timestamp
    fn get_window_start(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let minute = (time.minute() / WINDOW_MINUTES) * WINDOW_MINUTES;
        time.with_minute(minute)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
    }
}

impl Default for Aggregator {
    fn default() -> Self {
        Self::new()
    }
}
