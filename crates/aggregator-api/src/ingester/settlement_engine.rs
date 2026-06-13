use crate::aggregator::{Aggregator, BillingBin, BinKey};
use crate::infra::crypto::SettlementSigner;
use crate::infra::platform::PlatformClient;
use crate::ingester::bin_store::BinStore;
use crate::state::{identity, IdentityServiceClient};
use crate::zk::prover::ZkProver;
use connectrpc::client::SharedHttp2Connection;
use gridtokenx_blockchain_core::{
    BlockchainService, MintRecipient, NoopMetrics, SolanaProgramsConfig,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashSet;
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
    /// Durable bin store; settled bins are evicted here only after confirmed submit.
    bin_store: Option<BinStore>,
}

impl SettlementEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregator: Arc<Mutex<Aggregator>>,
        api_services_url: String,
        signer: Option<Arc<SettlementSigner>>,
        mint_via_chain_bridge: bool,
        blockchain: Option<Arc<BlockchainService>>,
        identity_client: Option<Arc<IdentityServiceClient<SharedHttp2Connection>>>,
        bin_store: Option<BinStore>,
    ) -> Self {
        Self {
            aggregator,
            api_services_url,
            signer,
            prover: Arc::new(ZkProver::new()),
            mint_via_chain_bridge,
            blockchain,
            identity_client,
            bin_store,
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

    /// Scans the aggregator for completed bins and submits them to the API.
    ///
    /// Bins are *peeked* (non-destructive), submitted, and only then *evicted* —
    /// from both the in-memory map and the durable store. A bin is evicted ONLY if
    /// its energy was minted/settled (or is structurally unmintable, e.g. zero
    /// generation). Anything that failed for a transient reason (mint batch error,
    /// IAM/wallet lookup down) is left in place and retried on the next tick, so
    /// no real energy is silently dropped.
    async fn process_completed_bins(&self) -> anyhow::Result<()> {
        let bins = {
            let agg = self.aggregator.lock().await;
            agg.peek_completed_bins()
        };

        if bins.is_empty() {
            return Ok(());
        }

        info!(
            "🧾 Found {} completed billing bins ready for ZK-Aggregation",
            bins.len()
        );

        // Quarantine bins from unregistered meters (nil owner). There is no wallet
        // to mint GRID to, so these can NEVER settle — resolving their wallet via IAM
        // always returns `not_found`. Previously they were treated as a transient
        // failure and retried every tick (logging a wallet-not-found flood) and, on
        // the HTTP path, POSTed to the gateway with no JWT (a stream of 401s). Evict
        // them up front (dropping the unattributable energy) and warn once per window.
        let (orphan_bins, bins): (Vec<BillingBin>, Vec<BillingBin>) =
            bins.into_iter().partition(|b| b.user_id.is_nil());
        if !orphan_bins.is_empty() {
            let orphan_keys: Vec<BinKey> = orphan_bins.iter().map(|b| b.key()).collect();
            for b in &orphan_bins {
                warn!(
                    "🚫 Quarantining settlement bin for meter {} — no registered owner (nil user); \
                     dropping {} kWh generated. Register the meter (gridtokenx:meters:{}:user_id) to settle its energy.",
                    b.meter_serial, b.energy_generated, b.meter_serial
                );
            }
            self.evict_settled(&orphan_keys).await;
        }
        if bins.is_empty() {
            return Ok(());
        }

        // Generation-mint path: aggregator owns issuance — build + submit the GRID
        // mint tx directly via Chain Bridge (Vault signs), bypassing trading-service.
        let evict: Vec<BinKey> = if self.mint_via_chain_bridge {
            self.mint_bins_via_chain_bridge(bins).await?
        } else {
            self.settle_bins_via_http(bins).await?
        };

        self.evict_settled(&evict).await;
        Ok(())
    }

    /// Remove settled bins from the in-memory aggregator and the durable store.
    /// Durable-store removal is best-effort: a failure leaves a stale entry that
    /// `rehydrate` would reload, but the in-memory removal already happened, so it
    /// only risks a one-time replay after a crash — never lost energy.
    async fn evict_settled(&self, keys: &[BinKey]) {
        if keys.is_empty() {
            return;
        }
        {
            let mut agg = self.aggregator.lock().await;
            agg.remove_bins(keys);
        }
        if let Some(store) = &self.bin_store {
            if let Err(e) = store.remove(keys).await {
                warn!(
                    "⚠️ Failed to evict {} settled bins from durable store: {}",
                    keys.len(),
                    e
                );
            }
        }
    }

    /// Legacy path: POST settlement payloads to trading-service over REST.
    /// Returns the keys of bins that were accepted (safe to evict).
    async fn settle_bins_via_http(&self, bins: Vec<BillingBin>) -> anyhow::Result<Vec<BinKey>> {
        let platform_client = PlatformClient::new(&self.api_services_url).await?;

        let mut settlement_payloads = Vec::new();
        let mut keys = Vec::new();
        for bin in bins {
            let key = bin.key();
            match self.prepare_settlement_payload(bin).await {
                Ok(payload) => {
                    settlement_payloads.push(payload);
                    keys.push(key);
                }
                Err(e) => error!("❌ Failed to prepare settlement payload: {}", e),
            }
        }

        if settlement_payloads.is_empty() {
            return Ok(vec![]);
        }

        match platform_client
            .settle_generation_mint_batch(&self.api_services_url, settlement_payloads)
            .await
        {
            Ok(_) => Ok(keys),
            Err(e) => {
                // Keep bins for retry on the next tick — do not evict.
                error!("❌ Batched settlement submission failed: {}", e);
                Ok(vec![])
            }
        }
    }

    /// Generation-mint via Chain Bridge: resolve each bin's recipient wallet through
    /// IAM (`GetUserWallet`), convert generated kWh → atomic GRID units, and submit a
    /// single batched, UNSIGNED mint transaction. Chain Bridge (Vault `platform_admin`)
    /// is the sole signer; the PolicyEngine must allowlist ENERGY_TOKEN + SPL-ATA for
    /// the Aggregator Bridge identity.
    ///
    /// Returns the keys of bins safe to evict: those included in a successfully
    /// submitted batch, plus zero-generation bins (nothing to mint). Bins whose
    /// wallet could not be resolved (transient IAM outage) or whose batch submit
    /// failed are NOT returned — they stay in the aggregator and retry next tick.
    async fn mint_bins_via_chain_bridge(
        &self,
        bins: Vec<BillingBin>,
    ) -> anyhow::Result<Vec<BinKey>> {
        let blockchain = self.blockchain.as_ref().ok_or_else(|| {
            anyhow::anyhow!("MINT_VIA_CHAIN_BRIDGE is set but BlockchainService is not wired")
        })?;
        let identity_client = self.identity_client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("MINT_VIA_CHAIN_BRIDGE is set but IAM identity client is not connected")
        })?;

        // 1e9 = GRID has 9 decimals (atomic units).
        let scale = Decimal::from(1_000_000_000i64);
        let mut inputs: Vec<MintRecipient> = Vec::with_capacity(bins.len());
        // Bins included in the mint batch — evicted only after a confirmed submit.
        let mut minted_keys: Vec<BinKey> = Vec::with_capacity(bins.len());
        // Zero-generation bins — nothing to mint, safe to evict regardless of outcome.
        let mut zero_keys: Vec<BinKey> = Vec::new();

        for bin in bins {
            let key = bin.key();
            let amount_atomic =
                ToPrimitive::to_u64(&(bin.energy_generated * scale).trunc()).unwrap_or(0);
            if amount_atomic == 0 {
                warn!(
                    "⏭️ Skipping mint for user {} — zero generated energy",
                    bin.user_id
                );
                zero_keys.push(key);
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
                    // Transient (IAM down) — retain bin for retry, do NOT evict.
                    error!(
                        "❌ Failed to resolve wallet for user {} via IAM: {} — retrying next tick",
                        bin.user_id, e
                    );
                    continue;
                }
            };

            let wallet = match BlockchainService::parse_pubkey(&wallet_str) {
                Ok(pk) => pk,
                Err(e) => {
                    // Retain for retry; an invalid wallet logs loud each tick until fixed.
                    error!(
                        "❌ Invalid wallet '{}' for user {}: {} — retrying next tick",
                        wallet_str, bin.user_id, e
                    );
                    continue;
                }
            };

            // Carry the (meter, window) identity so the mint is idempotent on-chain:
            // key.0 = meter UUID, key.1 = 15-min window start. The energy-token
            // `gen_mint` record PDA is keyed by these, so a replay of this window is a
            // no-op mint regardless of bridge-side state.
            inputs.push(MintRecipient {
                wallet,
                amount: amount_atomic,
                meter_id: *key.0.as_bytes(),
                window_start_ms: key.1.timestamp_millis(),
            });
            minted_keys.push(key);
        }

        if inputs.is_empty() {
            info!("ℹ️ No mintable bins after wallet resolution");
            // Zero-gen bins are still safe to evict; wallet-failed bins were retained.
            return Ok(zero_keys);
        }

        let count = inputs.len();
        info!("💰 [Chain Bridge] Submitting batched generation mint for {count} recipient(s)");
        match blockchain.execute_generation_mint_batch(inputs).await {
            Ok(outcome) => {
                // The batch is chunked into multiple transactions that submit
                // independently — a failed chunk is skipped, so submitted bins are an
                // arbitrary subset (not a prefix). Evict exactly the bins covered by
                // submitted chunks (`submitted_indices` into `minted_keys`, which is
                // lockstep with `inputs`) and retain the rest for retry — a poison
                // chunk no longer stalls the bins behind it.
                // Exactly-once is enforced ON-CHAIN: each recipient carries its
                // (meter, window) identity, which keys the energy-token `gen_mint`
                // record PDA, so a replay (crash between submit and eviction, or a
                // Redis outage that defeats the MINTED_SET marker) re-runs as a no-op
                // mint. The MINTED_SET guard below is now only a fast path that avoids
                // re-submitting a tx that would no-op anyway.
                let submitted = outcome.submitted_indices;
                if let Some(e) = &outcome.error {
                    error!(
                        "❌ Generation mint batch partially failed ({}/{count} recipient(s) \
                         submitted): {e} — retaining unsubmitted bins for retry",
                        submitted.len()
                    );
                } else {
                    info!(
                        "✅ Generation mint batch submitted ({count} recipients, {} tx): {:?}",
                        outcome.signatures.len(),
                        outcome.signatures
                    );
                }
                // The bins whose mint actually went out this tick.
                let minted_now = evict_submitted(minted_keys, &submitted);
                // Durably mark them BEFORE eviction so a crash in the submit→evict
                // window does not replay the mint on rehydrate. Best-effort: a failed
                // mark only shrinks the guard, it never blocks settlement.
                if let Some(store) = &self.bin_store {
                    if let Err(e) = store.mark_minted(&minted_now).await {
                        warn!(
                            "⚠️ Failed to mark {} minted bin(s) — replay guard degraded: {}",
                            minted_now.len(),
                            e
                        );
                    }
                }
                // Evict submitted bins; retained (unsubmitted) keys are dropped here,
                // leaving their bins in the store for the next tick.
                zero_keys.extend(minted_now);
                Ok(zero_keys)
            }
            Err(e) => {
                // Batch failed before any submit — retain ALL minted bins, evict zero-gen.
                error!("❌ Generation mint batch failed: {e}");
                Ok(zero_keys)
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

/// Select the minted-bin keys the batch actually submitted (by index into the lockstep
/// `minted_keys`/`inputs` lists) for eviction; unsubmitted keys are retained for retry.
/// Pure and order-independent so a split/out-of-order index set works, and any index
/// out of range is ignored rather than panicking.
fn evict_submitted(minted_keys: Vec<BinKey>, submitted_indices: &[usize]) -> Vec<BinKey> {
    let submitted: HashSet<usize> = submitted_indices.iter().copied().collect();
    minted_keys
        .into_iter()
        .enumerate()
        .filter_map(|(i, key)| submitted.contains(&i).then_some(key))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    fn key(n: u128) -> BinKey {
        let ts: DateTime<Utc> = DateTime::from_timestamp(0, 0).unwrap();
        (Uuid::from_u128(n), ts)
    }

    #[test]
    fn evicts_all_when_every_index_submitted() {
        let keys = vec![key(0), key(1), key(2)];
        let evicted = evict_submitted(keys.clone(), &[0, 1, 2]);
        assert_eq!(evicted, keys);
    }

    #[test]
    fn evicts_none_when_nothing_submitted() {
        let keys = vec![key(0), key(1), key(2)];
        assert!(evict_submitted(keys, &[]).is_empty());
    }

    #[test]
    fn evicts_only_submitted_subset() {
        // A poison chunk at index 1 left unsubmitted: indices 0 and 2 evicted, 1 retained.
        let keys = vec![key(10), key(11), key(12)];
        let evicted = evict_submitted(keys, &[0, 2]);
        assert_eq!(evicted, vec![key(10), key(12)]);
    }

    #[test]
    fn index_order_does_not_matter() {
        // The adaptive split can submit ranges out of order (e.g. high half first).
        let keys = vec![key(0), key(1), key(2), key(3)];
        let evicted = evict_submitted(keys, &[3, 0, 2]);
        assert_eq!(evicted, vec![key(0), key(2), key(3)]);
    }

    #[test]
    fn out_of_range_index_is_ignored() {
        let keys = vec![key(0), key(1)];
        // Index 5 has no corresponding key — must not panic, just evict the valid one.
        let evicted = evict_submitted(keys, &[0, 5]);
        assert_eq!(evicted, vec![key(0)]);
    }
}
