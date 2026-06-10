use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;
use uuid::Uuid;

/// Duration of a billing window in minutes
const WINDOW_MINUTES: u32 = 15;

/// Stable identity of a billing bin: (meter, window start). Used as the
/// in-memory map key and as the durable-store field key.
pub type BinKey = (Uuid, DateTime<Utc>);

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl BillingBin {
    /// Stable key for this bin: (meter_id, window_start).
    pub fn key(&self) -> BinKey {
        (self.meter_id, self.start_time)
    }
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

    /// Handles a new meter reading and updates or creates the corresponding billing
    /// bin. Returns a snapshot (clone) of the updated bin so the async edge can
    /// write it through to the durable store (crash-recovery of accumulated energy).
    pub fn handle_reading(
        &mut self,
        meter_id: Uuid,
        user_id: Uuid,
        meter_serial: String,
        generated: Decimal,
        consumed: Decimal,
        timestamp: DateTime<Utc>,
    ) -> BillingBin {
        let start_time = self.get_window_start(timestamp);
        let end_time = start_time + chrono::Duration::minutes(WINDOW_MINUTES as i64);

        let bin = self
            .active_bins
            .entry((meter_id, start_time))
            .or_insert_with(|| {
                debug!(
                    "🆕 Creating new billing bin for {} starting at {}",
                    meter_serial, start_time
                );
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
        bin.clone()
    }

    /// Returns clones of all billing bins past their end time WITHOUT removing them.
    /// Non-destructive on purpose: the settlement engine evicts a bin only after the
    /// mint is confirmed submitted (see `remove_bins`), so a failed/crashed mint
    /// retries on the next tick instead of silently losing the energy.
    pub fn peek_completed_bins(&self) -> Vec<BillingBin> {
        let now = Utc::now();
        self.active_bins
            .values()
            .filter(|bin| bin.end_time <= now)
            .cloned()
            .collect()
    }

    /// Removes the given bins (settled & evicted from the durable store by the caller).
    pub fn remove_bins(&mut self, keys: &[BinKey]) {
        for key in keys {
            self.active_bins.remove(key);
        }
    }

    /// Reloads bins from the durable store on boot. Existing in-memory bins win
    /// (a live reading already created them); only missing keys are inserted.
    pub fn rehydrate(&mut self, bins: Vec<BillingBin>) {
        for bin in bins {
            self.active_bins.entry(bin.key()).or_insert(bin);
        }
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
