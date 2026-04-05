use anyhow::{Result, Context};
use crate::nilm::models::{NilmResult, ApplianceProfile, FlexibilityScore};
use crate::nilm::rknn_ffi::RknnEngine;
use chrono::Utc;
use tracing::{info, debug, warn};

/// The NILM Engine handles the disaggregation of raw power signals using a Sparse MoE model.
/// It interfaces securely with the Rockchip NPU (RK3566) via our FFI wrapper.
pub struct NilmEngine {
    num_experts: usize,
    active_experts: usize,
    rknn: tokio::sync::Mutex<RknnEngine>,
}

impl NilmEngine {
    pub async fn new() -> Result<Self> {
        let mut rknn_engine = RknnEngine::new();
        // Load the quantized INT8 Sparse-MoE model mapped to the NPU
        rknn_engine.load_model("/opt/rknn/models/nilm_sparse_moe_v1.rknn")
            .context("Failed to load NILM NPU model")?;

        Ok(Self {
            num_experts: 8,
            active_experts: 2,
            rknn: tokio::sync::Mutex::new(rknn_engine),
        })
    }

    /// Run disaggregation on raw power data.
    /// Safely wraps the underlying NPU C-API Call.
    pub async fn disaggregate(&self, meter_id: &str, power_w: f64) -> Result<NilmResult> {
        debug!("🧠 Disaggregating load for meter {} ({} W)", meter_id, power_w);
        
        let engine = self.rknn.lock().await;

        // Construct a synthetic 1-second sampled tensor window (Placeholder for real data buffer)
        // In reality, this would be a sliding window of historical voltage/current matrices.
        let mut mock_input_tensor = vec![0.0_f32; 120]; 
        mock_input_tensor[119] = power_w as f32; // Put the latest power reading at the end
        
        // Execute the Neural Net Model Layer
        let start = std::time::Instant::now();
        let inference_result = engine.run_inference(&mock_input_tensor).await?;
        let elapsed = start.elapsed();
        
        debug!("⚡ NPU Inference completed in {:?} ms. Activated Experts: {:?}", elapsed.as_millis(), inference_result.top_experts);

        let appliances = self.map_inference_to_appliances(&inference_result.appliance_powers);
        let flexibility_scores = self.estimate_flexibility(&appliances);
        
        Ok(NilmResult {
            meter_id: meter_id.to_string(),
            timestamp: Utc::now(),
            total_power_w: power_w,
            appliances,
            flexibility_scores,
            anomalies: vec![],
        })
    }

    fn map_inference_to_appliances(&self, power_distribution: &[f32]) -> Vec<ApplianceProfile> {
        let mut apps = Vec::new();

        // Specific expert mapping to appliance categories as defined by our sparse architecture
        let labels = [
            "Baseline/General",
            "Lighting",
            "Cooking / Range",
            "Water Heater",
            "EV Charger L2",
            "Pool Pump",
            "Washer/Dryer",
            "HVAC / Central AC",
        ];

        for (idx, &power) in power_distribution.iter().enumerate() {
            if power > 10.0 { // 10W Threshold filter
                apps.push(ApplianceProfile {
                    name: labels.get(idx).unwrap_or(&"Unknown").to_string(),
                    power_w: power as f64,
                    state_id: 1, // On
                });
            }
        }
        
        // Sort descending by power_w
        apps.sort_by(|a, b| b.power_w.partial_cmp(&a.power_w).unwrap_or(std::cmp::Ordering::Equal));
        apps
    }

    fn estimate_flexibility(&self, apps: &[ApplianceProfile]) -> Vec<FlexibilityScore> {
        apps.iter().map(|app| {
            let score = match app.name.as_str() {
                "EV Charger L2" => 0.95, // Highly flexible/deferrable
                "Pool Pump" => 0.85,  
                "Water Heater" => 0.70, // Moderately flexible through thermal inertia
                "HVAC / Central AC" => 0.60, 
                _ => 0.05,            // Inflexible (Cooking, Lighting)
            };
            FlexibilityScore { app_name: app.name.clone(), score }
        }).collect()
    }
}
