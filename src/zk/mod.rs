pub mod circuit;
pub mod prover;

use serde::{Deserialize, Serialize};

/// A ZK-Proof artifact for a settlement batch (UTT-ZK Specification)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementProof {
    pub zone_id: String,
    pub window_start: i64,
    pub window_end: i64,
    pub merkle_root: String,
    pub total_energy_kwh: f64,
    pub proof_bytes: Vec<u8>,
    pub prover_version: String,
}
