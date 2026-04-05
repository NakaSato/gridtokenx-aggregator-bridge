use std::collections::HashMap;
use rust_decimal::Decimal;
use uuid::Uuid;
use tracing::{info, debug};
use chrono::{DateTime, Utc, Duration, Timelike};

/// Data for a single meter's performance in a window
#[derive(Debug, Clone)]
pub struct WindowedStats {
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
    // Current active window state: Map<MeterId, WindowedStats>
    active_windows: HashMap<Uuid, WindowedStats>,
    // Completed windows ready for attestation: Vec<WindowedStats>
    completed_windows: Vec<WindowedStats>,
    // Last processed window timestamp (aligned to 15-min)
    current_window_start: DateTime<Utc>,
}

impl Aggregator {
    pub fn new() -> Self {
        let now = Utc::now();
        let current_window_start = Self::align_to_window(now);
        
        Self {
            active_windows: HashMap::new(),
            completed_windows: Vec::new(),
            current_window_start,
        }
    }

    /// Aligns a timestamp to the start of the 15-minute window (00, 15, 30, 45)
    pub fn align_to_window(ts: DateTime<Utc>) -> DateTime<Utc> {
        let minute = (ts.minute() / 15) * 15;
        ts.with_minute(minute)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
    }

    pub fn handle_reading(
        &mut self,
        meter_id: Uuid,
        user_id: Uuid,
        meter_serial: String,
        energy_generated: Decimal,
        energy_consumed: Decimal,
        timestamp: DateTime<Utc>,
    ) {
        let window_start = Self::align_to_window(timestamp);

        // Check if we've moved to a new window
        if window_start > self.current_window_start {
            info!("🔔 New 15-minute window detected: {} (previous: {})", 
                  window_start, self.current_window_start);
            self.rotate_windows(window_start);
        }

        // Update active window for this meter
        let stats = self.active_windows.entry(meter_id).or_insert_with(|| WindowedStats {
            meter_id,
            user_id,
            meter_serial: meter_serial.clone(),
            start_time: window_start,
            end_time: window_start + Duration::minutes(15),
            energy_generated: Decimal::ZERO,
            energy_consumed: Decimal::ZERO,
            reading_count: 0,
        });

        stats.energy_generated += energy_generated;
        stats.energy_consumed += energy_consumed;
        stats.reading_count += 1;

        debug!("🧮 Aggregated reading for {} in window {}", meter_serial, window_start);
    }

    /// Moves all active windows to completed and updates current window start
    fn rotate_windows(&mut self, new_window_start: DateTime<Utc>) {
        let mut finished = std::mem::take(&mut self.active_windows);
        
        for (_, stats) in finished.drain() {
            self.completed_windows.push(stats);
        }

        self.current_window_start = new_window_start;
        info!("📦 Rotating windows. {} meter summaries ready for attestation", self.completed_windows.len());
    }

    pub fn take_completed_windows(&mut self) -> Vec<WindowedStats> {
        std::mem::take(&mut self.completed_windows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_align_to_window() {
        let ts = Utc.with_ymd_and_hms(2026, 3, 30, 10, 7, 30).unwrap();
        let aligned = Aggregator::align_to_window(ts);
        assert_eq!(aligned, Utc.with_ymd_and_hms(2026, 3, 30, 10, 0, 0).unwrap());

        let ts2 = Utc.with_ymd_and_hms(2026, 3, 30, 10, 16, 0).unwrap();
        let aligned2 = Aggregator::align_to_window(ts2);
        assert_eq!(aligned2, Utc.with_ymd_and_hms(2026, 3, 30, 10, 15, 0).unwrap());
    }

    #[test]
    fn test_window_rotation() {
        let mut agg = Aggregator::new();
        let meter_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        
        // Window 1: 10:00 - 10:15
        let ts1 = Utc.with_ymd_and_hms(2026, 3, 30, 10, 5, 0).unwrap();
        agg.current_window_start = Aggregator::align_to_window(ts1);
        
        agg.handle_reading(
            meter_id,
            user_id,
            "MTR-001".to_string(),
            Decimal::from(5),
            Decimal::from(2),
            ts1,
        );

        assert_eq!(agg.active_windows.len(), 1);
        assert_eq!(agg.completed_windows.len(), 0);

        // Window 2: 10:15 - 10:30 (triggers rotation)
        let ts2 = Utc.with_ymd_and_hms(2026, 3, 30, 10, 20, 0).unwrap();
        agg.handle_reading(
            meter_id,
            user_id,
            "MTR-001".to_string(),
            Decimal::from(6),
            Decimal::from(3),
            ts2,
        );

        assert_eq!(agg.completed_windows.len(), 1);
        assert_eq!(agg.active_windows.len(), 1);
        
        let completed = agg.take_completed_windows();
        assert_eq!(completed[0].energy_generated, Decimal::from(5));
        assert_eq!(completed[0].energy_consumed, Decimal::from(2));
    }
}
