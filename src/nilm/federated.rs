use anyhow::{Result, Context};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, debug, warn};
use rand::thread_rng;

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
