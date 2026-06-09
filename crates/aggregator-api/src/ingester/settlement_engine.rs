use crate::aggregator::{Aggregator, BillingBin};
use crate::infra::crypto::SettlementSigner;
use crate::infra::platform::PlatformClient;
use crate::state::{identity, IdentityServiceClient};
use crate::zk::prover::ZkProver;
use connectrpc::client::SharedHttp2Connection;
use gridtokenx_blockchain_core::{BlockchainService, NoopMetrics, Pubkey, SolanaProgramsConfig};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Builds the shared `BlockchainService` used for the chain-bridge generation-mint
/// path. Reads `CHAIN_BRIDGE_URL`, `SOLANA_CLUSTER`/`SOLANA_RPC_URL`, and the
/// `SOLANA_*_PROGRAM_ID` set (env overrides over baked-in defaults). When `NATS_URL`
/// is set the service auto-selects the NATS chain-bridge provider; tx are submitted
/// UNSIGNED so Vault (`platform_admin`) signs — no local keypair here.
pub async fn build_blockchain_service() -> anyhow::Result<Arc<BlockchainService>> {
    let bridge_url =
        std::env::var("CHAIN_BRIDGE_URL").unwrap_or_else(|_| "http://127.0.0.1:5040".to_string());
    let cluster = std::env::var("SOLANA_CLUSTER")
        .or_else(|_| std::env::var("SOLANA_RPC_URL"))
        .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());

    let d = SolanaProgramsConfig::default();
    let programs = SolanaProgramsConfig {
        registry_program_id: std::env::var("SOLANA_REGISTRY_PROGRAM_ID")
            .unwrap_or(d.registry_program_id),
        oracle_program_id: std::env::var("SOLANA_ORACLE_PROGRAM_ID").unwrap_or(d.oracle_program_id),
        governance_program_id: std::env::var("SOLANA_GOVERNANCE_PROGRAM_ID")
            .unwrap_or(d.governance_program_id),
        energy_token_program_id: std::env::var("SOLANA_ENERGY_TOKEN_PROGRAM_ID")
            .unwrap_or(d.energy_token_program_id),
        trading_program_id: std::env::var("SOLANA_TRADING_PROGRAM_ID")
            .unwrap_or(d.trading_program_id),
        trading_market_id: std::env::var("SOLANA_TRADING_MARKET_ID")
            .unwrap_or(d.trading_market_id),
    };

    let svc =
        BlockchainService::new(bridge_url, cluster, programs, Arc::new(NoopMetrics {})).await?;
    Ok(Arc::new(svc))
}

pub struct SettlementEngine {
    aggregator: Arc<Mutex<Aggregator>>,
    api_services_url: String,
    signer: Option<Arc<SettlementSigner>>,
    prover: Arc<ZkProver>,
    /// When true, mint GRID directly via Chain Bridge instead of POSTing to
    /// trading-service. Gated by `MINT_VIA_CHAIN_BRIDGE`. Requires `blockchain`
    /// and `identity_client` to be present.
    mint_via_chain_bridge: bool,
    blockchain: Option<Arc<BlockchainService>>,
    identity_client: Option<Arc<IdentityServiceClient<SharedHttp2Connection>>>,
}

impl SettlementEngine {
    pub fn new(
        aggregator: Arc<Mutex<Aggregator>>,
        api_services_url: String,
        signer: Option<Arc<SettlementSigner>>,
        mint_via_chain_bridge: bool,
        blockchain: Option<Arc<BlockchainService>>,
        identity_client: Option<Arc<IdentityServiceClient<SharedHttp2Connection>>>,
    ) -> Self {
        Self {
            aggregator,
            api_services_url,
            signer,
            prover: Arc::new(ZkProver::new()),
            mint_via_chain_bridge,
            blockchain,
            identity_client,
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

        // Generation-mint path: aggregator owns issuance — build + submit the GRID
        // mint tx directly via Chain Bridge (Vault signs), bypassing trading-service.
        if self.mint_via_chain_bridge {
            return self.mint_bins_via_chain_bridge(bins).await;
        }

        // Legacy path: POST settlement payloads to trading-service over REST.
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

    /// Generation-mint via Chain Bridge: resolve each bin's recipient wallet through
    /// IAM (`GetUserWallet`), convert generated kWh → atomic GRID units, and submit a
    /// single batched, UNSIGNED mint transaction. Chain Bridge (Vault `platform_admin`)
    /// is the sole signer; the PolicyEngine must allowlist ENERGY_TOKEN + SPL-ATA for
    /// the Aggregator Bridge identity.
    async fn mint_bins_via_chain_bridge(&self, bins: Vec<BillingBin>) -> anyhow::Result<()> {
        let blockchain = self.blockchain.as_ref().ok_or_else(|| {
            anyhow::anyhow!("MINT_VIA_CHAIN_BRIDGE is set but BlockchainService is not wired")
        })?;
        let identity_client = self.identity_client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("MINT_VIA_CHAIN_BRIDGE is set but IAM identity client is not connected")
        })?;

        // 1e9 = GRID has 9 decimals (atomic units).
        let scale = Decimal::from(1_000_000_000i64);
        let mut inputs: Vec<(Pubkey, u64)> = Vec::with_capacity(bins.len());

        for bin in bins {
            let amount_atomic =
                ToPrimitive::to_u64(&(bin.energy_generated * scale).trunc()).unwrap_or(0);
            if amount_atomic == 0 {
                warn!(
                    "⏭️ Skipping mint for user {} — zero generated energy",
                    bin.user_id
                );
                continue;
            }

            // Resolve the recipient's primary on-chain wallet via IAM.
            let request = identity::GetUserWalletRequest {
                user_id: bin.user_id.to_string(),
                ..Default::default()
            };
            let wallet_str = match identity_client.get_user_wallet(request).await {
                Ok(resp) => resp.into_owned().wallet_address,
                Err(e) => {
                    error!(
                        "❌ Failed to resolve wallet for user {} via IAM: {} — skipping",
                        bin.user_id, e
                    );
                    continue;
                }
            };

            let wallet = match BlockchainService::parse_pubkey(&wallet_str) {
                Ok(pk) => pk,
                Err(e) => {
                    error!(
                        "❌ Invalid wallet '{}' for user {}: {} — skipping",
                        wallet_str, bin.user_id, e
                    );
                    continue;
                }
            };

            inputs.push((wallet, amount_atomic));
        }

        if inputs.is_empty() {
            info!("ℹ️ No mintable bins after wallet resolution");
            return Ok(());
        }

        let count = inputs.len();
        info!("💰 [Chain Bridge] Submitting batched generation mint for {count} recipient(s)");
        match blockchain.execute_generation_mint_batch(inputs).await {
            Ok(signature) => {
                info!("✅ Generation mint batch submitted ({count} recipients): {signature}");
                Ok(())
            }
            Err(e) => {
                error!("❌ Generation mint batch failed: {e}");
                Err(e)
            }
        }
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
