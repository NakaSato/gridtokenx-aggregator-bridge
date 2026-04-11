use std::sync::Arc;
use tokio::sync::Mutex;
use crate::aggregator::{Aggregator, BillingBin};
use crate::infra::crypto::SettlementSigner;
use crate::infra::platform::PlatformClient;
use tracing::{info, error, warn};
use tokio_util::sync::CancellationToken;
// removed unused Decimal import

pub struct SettlementWorker {
    aggregator: Arc<Mutex<Aggregator>>,
    api_services_url: String,
    signer: Option<Arc<SettlementSigner>>,
}

impl SettlementWorker {
    pub fn new(
        aggregator: Arc<Mutex<Aggregator>>, 
        api_services_url: String,
        signer: Option<Arc<SettlementSigner>>,
    ) -> Self {
        Self {
            aggregator,
            api_services_url,
            signer,
        }
    }

    /// Run the settlement worker loop
    pub async fn run(&self, token: CancellationToken) {
        info!("💰 Settlement Worker started (Interval: 60s)");
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

        info!("🧾 Found {} completed billing bins ready for settlement", bins.len());

        // We can reuse a single PlatformClient for these requests
        let platform_client = PlatformClient::new(&self.api_services_url).await?;

        for bin in bins {
            if let Err(e) = self.settle_bin(&platform_client, bin).await {
                error!("❌ Settlement failed for bin: {}", e);
                // Implementation Note: In a production environment, failed settlements 
                // should be pushed to a RabbitMQ Dead Letter Queue or Retry Queue.
            }
        }

        Ok(())
    }

    /// Prepares, signs, and submits a single billing bin to the API Gateway
    async fn settle_bin(&self, client: &PlatformClient, bin: BillingBin) -> anyhow::Result<()> {
        let mut payload = serde_json::json!({
            "meter_id": bin.meter_id,
            "meter_serial": bin.meter_serial,
            "user_id": bin.user_id,
            "start_time": bin.start_time,
            "end_time": bin.end_time,
            "energy_generated_kwh": bin.energy_generated,
            "energy_consumed_kwh": bin.energy_consumed,
            "reading_count": bin.reading_count,
        });

        // 1. Sign the payload if a signer is available
        if let Some(signer) = &self.signer {
            match signer.sign_settlement(&payload) {
                Ok(sig) => {
                    payload["signature"] = serde_json::json!(sig);
                }
                Err(e) => {
                    warn!("⚠️ Failed to sign settlement for {}: {}", bin.meter_serial, e);
                    payload["signature"] = serde_json::json!("");
                }
            }
        } else {
            payload["signature"] = serde_json::json!("");
        }

        // 2. Submit via PlatformClient
        client.settle_generation_mint(&self.api_services_url, &payload).await?;

        Ok(())
    }
}
