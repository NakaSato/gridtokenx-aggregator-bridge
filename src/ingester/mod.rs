pub mod zone_ingester;
pub mod batcher;
pub mod settlement_worker;


use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "payload")]
pub enum Event {
    // New IoT Gateway Events (from Router)
    SmartMeterReading(crate::models::DeviceReading),
    EvCharging(crate::models::DeviceReading),
    BatteryStateUpdate(crate::models::DeviceReading),

    OrderMatched(serde_json::Value),
    OrderUpdate(serde_json::Value),
    SettlementRequested(serde_json::Value),
    PeakPriceUpdate(serde_json::Value),
    TriggerExecution(serde_json::Value),
    OrderCreated(serde_json::Value),
    #[serde(other)]
    Unknown,
}
