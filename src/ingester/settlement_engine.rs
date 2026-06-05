use crate::aggregator::{Aggregator, BillingBin};
use crate::infra::crypto::SettlementSigner;
use crate::infra::platform::PlatformClient;
use crate::zk::prover::ZkProver;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct SettlementEngine {
    aggregator: Arc<Mutex<Aggregator>>,
    api_services_url: String,
    signer: Option<Arc<SettlementSigner>>,
    prover: Arc<ZkProver>,
}

impl SettlementEngine {
    pub fn new(
        aggregator: Arc<Mutex<Aggregator>>,
        api_services_url: String,
        signer: Option<Arc<SettlementSigner>>,
    ) -> Self {
        Self {
            aggregator,
            api_services_url,
            signer,
            prover: Arc::new(ZkProver::new()),
        }
    }

    /// Run the settlement engine loop (Path B Sink)
    pub async fn run(&self, token: CancellationToken) {
        info!("💰 UTT Settlement Engine started (UTT Path B + ZK-Rollup)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.process_completed_bins().await {
                        error!("❌ Error in settlement processing: {}", e);
                    }
                }
                _ = token.cancelled() => {
                    info!("🔄 Settlement Worker shutting down...");
                    break;
                }
            }
        }
    }

    /// Scans the aggregator for completed bins and submits them to the API
    async fn process_completed_bins(&self) -> anyhow::Result<()> {
        let bins = {
            let mut agg = self.aggregator.lock().await;
            agg.take_completed_bins()
        };

        if bins.is_empty() {
            return Ok(());
        }

        info!(
            "🧾 Found {} completed billing bins ready for ZK-Aggregation",
            bins.len()
        );

        // We can reuse a single PlatformClient for these requests
        let platform_client = PlatformClient::new(&self.api_services_url).await?;

        let mut settlement_payloads = Vec::new();

        for bin in bins {
            match self.prepare_settlement_payload(bin).await {
                Ok(payload) => settlement_payloads.push(payload),
                Err(e) => error!("❌ Failed to prepare settlement payload: {}", e),
            }
        }

        if !settlement_payloads.is_empty() {
            if let Err(e) = platform_client.settle_generation_mint_batch(&self.api_services_url, settlement_payloads).await {
                error!("❌ Batched settlement submission failed: {}", e);
            }
        }

        Ok(())
    }

    /// Prepares a settlement payload for a billing bin
    async fn prepare_settlement_payload(&self, bin: BillingBin) -> anyhow::Result<serde_json::Value> {
        let start_time = bin.start_time.timestamp();
        let end_time = bin.end_time.timestamp();

        info!("🛡️ Generating Plonky2 ZK-Proof for settlement [Zone: {}]", bin.user_id);
        
        let proof = self.prover.prove_batch(
            &bin.user_id.to_string(), 
            start_time,
            end_time,
            bin.energy_generated.to_f64().unwrap_or(0.0)
        ).await?;

        info!("✅ ZK-Proof generated ({} bytes) for {}", proof.proof_bytes.len(), bin.user_id);

        Ok(serde_json::json!({
            "user_id": bin.user_id,
            "meter_serial": bin.meter_serial,
            "start_time": start_time,
            "end_time": end_time,
            "energy_generated_kwh": bin.energy_generated,
            "zk_proof": hex::encode(&proof.proof_bytes),
            "prover_version": "gridtokenx-plonky2-v0.2",
            "signature": if let Some(signer) = &self.signer {
                signer.sign_canonical(&format!("{}:{}:{}:{}:{}", bin.user_id, bin.meter_serial, bin.energy_generated, start_time, end_time))
            } else {
                String::new()
            }
        }))
    }
}
