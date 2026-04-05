use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

/// An appliance-level power profile detected via NILM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplianceProfile {
    pub name: String,
    pub power_w: f64,
    pub state_id: u32, // 0: Off, 1: On, etc.
}

/// A load flexibility score (0.0 to 1.0) for a VPP-optimized dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlexibilityScore {
    pub app_name: String,
    pub score: f64,
}

/// The result of a NILM disaggregation cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NilmResult {
    pub meter_id: String,
    pub timestamp: DateTime<Utc>,
    pub total_power_w: f64,
    pub appliances: Vec<ApplianceProfile>,
    pub flexibility_scores: Vec<FlexibilityScore>,
    pub anomalies: Vec<String>,
}
