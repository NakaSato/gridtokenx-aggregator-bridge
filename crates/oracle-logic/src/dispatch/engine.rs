use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::aggregator::Aggregator;
use crate::dispatch::grpc_client::{DispatchClient, DispatchType};
use crate::dispatch::DispatchAdapter;
use crate::standards::ieee2030_5::Ieee2030_5Adapter;

pub struct DispatchEngine {
    aggregator: Arc<Mutex<Aggregator>>,
    adapters: std::collections::HashMap<String, Arc<dyn DispatchAdapter>>,
}

impl DispatchEngine {
    pub fn new(aggregator: Arc<Mutex<Aggregator>>, grpc_client: DispatchClient) -> Self {
        let mut adapters = std::collections::HashMap::new();
        adapters.insert("grpc".to_string(), Arc::new(grpc_client) as Arc<dyn DispatchAdapter>);
        adapters.insert("ieee".to_string(), Arc::new(Ieee2030_5Adapter::new()) as Arc<dyn DispatchAdapter>);
        
        Self {
            aggregator,
            adapters,
        }
    }

    /// Evaluates grid status and decides whether to dispatch flex commands
    pub async fn evaluate_and_dispatch(&mut self, frequency: f64) -> Result<()> {
        info!("Evaluating grid frequency: {} Hz", frequency);

        let adapter = self.adapters.get("ieee").unwrap().clone(); // Simplified for now

        if frequency < 49.8 {
            warn!("Frequency low! Dispatching FLEX_UP command.");
            self.dispatch_action(adapter, DispatchType::FLEX_UP, 100.0).await?;
        } else if frequency > 50.2 {
            info!("Frequency high! Dispatching FLEX_DOWN command.");
            self.dispatch_action(adapter, DispatchType::FLEX_DOWN, 100.0).await?;
        } else {
            info!("Frequency stable.");
        }

        Ok(())
    }

    async fn dispatch_action(&mut self, adapter: Arc<dyn DispatchAdapter>, action: DispatchType, capacity_kw: f64) -> Result<()> {
        // Query aggregator state
        let mut aggregator = self.aggregator.lock().await;
        let bins = aggregator.take_completed_bins();
        
        // Calculate total capacity
        let total_capacity: rust_decimal::Decimal = bins.iter().map(|b| b.energy_generated).sum();
        
        if total_capacity <= rust_decimal::Decimal::ZERO {
            return Err(anyhow!("No available capacity for dispatch"));
        }

        // Execute dispatch via trait
        adapter.execute_dispatch(action, capacity_kw).await
    }
}

