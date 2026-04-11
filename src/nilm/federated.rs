use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::time::Duration;
use tracing::{info, debug, warn};

/// Configuration for Differential Privacy (DP)
pub struct PrivacyConfig {
    pub epsilon: f64,
    pub delta: f64,
    pub noise_scale: f64, 
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        Self {
            epsilon: 1.0,
            delta: 1e-5,
            noise_scale: 1.0 / 1.0, // scale = sensitivity / epsilon. Assuming sensitivity=1 for normalized gradients
        }
    }
}

/// A structure to hold locally accumulated gradients before pushing to the cloud
pub struct LocalGradientAccumulator {
    gradients: HashMap<String, Vec<f32>>,
    sample_count: usize,
    privacy_cfg: PrivacyConfig,
}

impl LocalGradientAccumulator {
    pub fn new() -> Self {
        Self {
            gradients: HashMap::new(),
            sample_count: 0,
            privacy_cfg: PrivacyConfig::default(),
        }
    }

    /// Simulate accumulating local training metrics from the Sparse MoE model.
    /// In production, these gradients are retrieved directly from the NPU backward-pass buffers.
    pub fn accumulate(&mut self, layer_name: &str, layer_gradients: &[f32]) {
        let entry = self.gradients.entry(layer_name.to_string()).or_insert(vec![0.0; layer_gradients.len()]);
        for (i, &val) in layer_gradients.iter().enumerate() {
            entry[i] += val;
        }
        self.sample_count += 1;
    }

    /// Apply Differential Privacy Laplace noise using the Probability Transform Method.
    pub fn apply_differential_privacy(&mut self) {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let b = self.privacy_cfg.noise_scale as f32;

        for (_layer, gradients) in self.gradients.iter_mut() {
            for val in gradients.iter_mut() {
                // Generate U in (-0.5, 0.5]
                let u: f32 = rng.gen_range(-0.5..0.5);
                // Laplace(0, b) = -b * sgn(U) * ln(1 - 2|U|)
                let noise = -b * u.signum() * (1.0 - 2.0 * u.abs()).ln();
                
                if noise.is_finite() {
                    *val += noise;
                }
            }
        }
        debug!("🔐 Applied DP Laplace Noise (ε={}, δ={}) to gradients", self.privacy_cfg.epsilon, self.privacy_cfg.delta);
    }

    /// Package and compress the gradients for IoT transport.
    /// Simulates compressing gradients to ~500 KB per the architectural spec.
    pub async fn upload_gradients(&mut self) -> Result<()> {
        if self.sample_count == 0 {
            debug!("No gradients to upload this cycle.");
            return Ok(());
        }

        info!("🔄 Preparing local Federated Learning gradients for cloud sync.");
        
        // 1. Apply DP Math
        self.apply_differential_privacy();

        // 2. Compress (Simulated)
        let total_size_bytes = self.gradients.values().map(|v| v.len() * 4).sum::<usize>();
        let compressed_size = total_size_bytes / 4; // Simulated 4x compression

        info!("📦 Compressed gradient size: {} bytes -> {} bytes", total_size_bytes, compressed_size);

        // 3. Upload over gRPC (Placeholder log - replacing MQTT as per unified stack)
        // In real execution, passing this to standard streaming gRPC client.
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("☁️ Successfully uploaded gradients to global aggregator.");

        // Clear local buffer
        self.gradients.clear();
        self.sample_count = 0;

        Ok(())
    }
}

/// Global Aggregator (Running on Oracle Bridge)
/// Orchestrates the collective intelligence by averaging gradients from multiple Edge Meters.
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalModelAggregator {
    pub current_version: String,
    pub global_weights: HashMap<String, Vec<f32>>,
    #[serde(skip)]
    pub pending_updates: Vec<PushGradientsRequest>,
    pub aggregation_threshold: usize,
    #[serde(skip)]
    pub active_model_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone)]
pub struct PushGradientsRequest {
    pub meter_id: String,
    pub base_version: String,
    pub layers: HashMap<String, Vec<f32>>,
    pub sample_count: usize,
}

impl GlobalModelAggregator {
    pub fn new(threshold: usize) -> Self {
        Self {
            current_version: "1.0.0".to_string(),
            global_weights: HashMap::new(),
            pending_updates: Vec::new(),
            aggregation_threshold: threshold,
            active_model_path: None,
        }
    }

    /// Export the current global model as a TFLM-compatible binary.
    /// In production, this would trigger a quantization (f32 -> int8) and flatbuffer serialization.
    pub fn export_binary(&mut self, output_dir: &Path) -> Result<std::path::PathBuf> {
        let filename = format!("nilm_model_v{}.tflite", self.current_version.replace(".", "_"));
        let full_path = output_dir.join(filename);

        // Simulated TFLite generation (writing a header + weight buffer)
        let mut binary_content = Vec::new();
        binary_content.extend_from_slice(b"TFL3"); // TFLite Magic
        
        // Mock weights serialization
        for (name, weights) in &self.global_weights {
            binary_content.extend_from_slice(name.as_bytes());
            for &w in weights {
                binary_content.extend_from_slice(&w.to_le_bytes());
            }
        }

        fs::write(&full_path, binary_content).context("Failed to write TFLite binary")?;
        info!("💎 Exported global model binary: {:?}", full_path);
        
        self.active_model_path = Some(full_path.clone());
        Ok(full_path)
    }

    /// Add a new gradient update to the global pending pool.
    pub fn add_update(&mut self, update: PushGradientsRequest) -> Result<bool> {
        if update.base_version != self.current_version {
            warn!("⚠️ Refusing gradient update based on stale version: {} (Current: {})", 
                  update.base_version, self.current_version);
            return Ok(false);
        }

        info!("📥 Received NILM gradients from meter: {} (samples: {})", update.meter_id, update.sample_count);
        self.pending_updates.push(update);

        if self.pending_updates.len() >= self.aggregation_threshold {
            self.aggregate()?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Perform Federated Averaging (FedAvg) on the accumulated pool.
    pub fn aggregate(&mut self) -> Result<()> {
        if self.pending_updates.is_empty() {
            return Ok(());
        }

        info!("🤖 Performing Federated Averaging (FedAvg) over {} nodes.", self.pending_updates.len());

        let total_samples: usize = self.pending_updates.iter().map(|u| u.sample_count).sum();
        let mut new_weights: HashMap<String, Vec<f32>> = HashMap::new();

        for update in &self.pending_updates {
            let weight = update.sample_count as f32 / total_samples as f32;
            
            for (layer_name, gradients) in &update.layers {
                let entry = new_weights.entry(layer_name.clone()).or_insert(vec![0.0; gradients.len()]);
                for (i, &grad) in gradients.iter().enumerate() {
                    entry[i] += grad * weight;
                }
            }
        }

        // Update the global weights (In production, this triggers a re-quantization to TFLite INT8)
        self.global_weights = new_weights;
        
        // Bump version
        let parts: Vec<&str> = self.current_version.split('.').collect();
        let minor: i32 = parts[2].parse().unwrap_or(0);
        self.current_version = format!("{}.{}.{}", parts[0], parts[1], minor + 1);

        info!("🚀 Global NILM Model refined to version: {}", self.current_version);

        // Clear pool
        self.pending_updates.clear();
        Ok(())
    }

    /// Save the current aggregator state to a persistent file.
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize aggregator state")?;
        
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("Failed to create storage directory")?;
        }

        fs::write(path, json).context("Failed to write aggregator state to disk")?;
        debug!("💾 Persistent model state saved: {:?}", path);
        Ok(())
    }

    /// Load the aggregator state from a persistent file.
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .context("Failed to read aggregator state file")?;
        let mut state: Self = serde_json::from_str(&content)
            .context("Failed to deserialize aggregator state")?;
        
        // Ensure pending updates is initialized (it's skipped during serde)
        state.pending_updates = Vec::new();
        
        info!("📖 Resumed Collective Intelligence at version: {}", state.current_version);
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fed_avg_math() {
        let mut aggregator = GlobalModelAggregator::new(2);
        let mut layers1 = HashMap::new();
        layers1.insert("layer1".to_string(), vec![1.0, 2.0, 3.0]);

        let mut layers2 = HashMap::new();
        layers2.insert("layer1".to_string(), vec![2.0, 4.0, 6.0]);

        // Push update 1 (100 samples)
        let _ = aggregator.add_update(PushGradientsRequest {
            meter_id: "m1".to_string(),
            base_version: "1.0.0".to_string(),
            layers: layers1,
            sample_count: 100,
        }).unwrap();

        // Push update 2 (300 samples)
        // Expected value for layer1[0] = (1.0 * 100 + 2.0 * 300) / 400 = (100 + 600) / 400 = 1.75
        let accepted = aggregator.add_update(PushGradientsRequest {
            meter_id: "m2".to_string(),
            base_version: "1.0.0".to_string(),
            layers: layers2,
            sample_count: 300,
        }).unwrap();

        assert!(accepted);
        assert_eq!(aggregator.current_version, "1.0.1");
        
        let weight = aggregator.global_weights.get("layer1").unwrap();
        assert!((weight[0] - 1.75).abs() < 1e-6);
        assert!((weight[1] - 3.5).abs() < 1e-6);
        assert!((weight[2] - 5.25).abs() < 1e-6);
    }

    #[test]
    fn test_stale_version_rejection() {
        let mut aggregator = GlobalModelAggregator::new(5);
        let layers = HashMap::new();
        
        let accepted = aggregator.add_update(PushGradientsRequest {
            meter_id: "m1".to_string(),
            base_version: "0.9.0".to_string(), // Stale version
            layers,
            sample_count: 100,
        }).unwrap();

        assert!(!accepted);
        assert_eq!(aggregator.pending_updates.len(), 0);
    }

    #[test]
    fn test_persistence_round_trip() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("nilm_test.json");

        let mut aggregator = GlobalModelAggregator::new(1);
        aggregator.current_version = "2.0.5".to_string();
        aggregator.global_weights.insert("layer1".to_string(), vec![0.1, 0.2, 0.3]);

        // Save
        aggregator.save_to_file(&path).unwrap();

        // Load
        let loaded = GlobalModelAggregator::load_from_file(&path).unwrap();

        assert_eq!(loaded.current_version, "2.0.5");
        assert_eq!(loaded.aggregation_threshold, 1);
        let w = loaded.global_weights.get("layer1").unwrap();
        assert_eq!(w[0], 0.1);
        assert_eq!(w[1], 0.2);
        assert_eq!(w[2], 0.3);

        // Cleanup
        let _ = fs::remove_file(path);
    }
}
