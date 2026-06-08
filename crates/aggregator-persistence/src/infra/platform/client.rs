use anyhow::Result;
use connectrpc::client::{ClientConfig, Http2Connection, SharedHttp2Connection};
use connectrpc::Protocol;
use tracing::{error, info};

// Generated ConnectRPC code now lives in the aggregator-protocol crate.
pub use aggregator_protocol::oracle::{
    IngestResponse, MeterReading, MeterReadingBatchRequest, MeterReadingBatchResponse,
    OracleServiceClient,
};

#[derive(Clone)]
pub struct PlatformClient {
    client: OracleServiceClient<SharedHttp2Connection>,
}

impl PlatformClient {
    pub async fn new(base_url: &str) -> anyhow::Result<Self> {
        let uri: http::Uri = base_url.parse()?;
        let conn = Http2Connection::connect_plaintext(uri.clone())
            .await?
            .shared(1024);
        let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
        let client = OracleServiceClient::new(conn, config);

        Ok(Self { client })
    }

    /// Submit a batch of verified meter readings (UTT Path A/B)
    pub async fn submit_meter_reading_batch(
        &self,
        requests: Vec<MeterReading>,
    ) -> Result<MeterReadingBatchResponse> {
        let count = requests.len();
        info!(
            "📤 [UTT] Submitting batch of {} verified readings to platform",
            count
        );

        let batch_req = MeterReadingBatchRequest {
            readings: requests,
            ..Default::default()
        };

        match self.client.ingest_batch(batch_req).await {
            Ok(res) => {
                let view = res.view();
                info!(
                    "✅ Unified ingestion successful. Accepted: {}, Rejected: {}",
                    view.accepted_count, view.rejected_count
                );

                Ok(MeterReadingBatchResponse {
                    accepted_count: view.accepted_count,
                    rejected_count: view.rejected_count,
                    ..Default::default()
                })
            }
            Err(e) => {
                error!("❌ Unified ingestion failed: {}", e);
                Err(anyhow::anyhow!("RPC error: {}", e))
            }
        }
    }

    /// Execute Generation Mint for a verified billing bin via REST API
    pub async fn settle_generation_mint(
        &self,
        base_url: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/settlement/generation-mint", base_url);

        info!(
            "💰 [REST] Submitting settlement for {} ({} - {})",
            payload
                .get("meter_serial")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown"),
            payload
                .get("start_time")
                .and_then(|v| v.as_str())
                .unwrap_or("?"),
            payload
                .get("end_time")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
        );

        let request_builder = client.post(&url).json(payload);
        let response = request_builder.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            error!("❌ Settlement failed with status {}: {}", status, error_text);
            return Err(anyhow::anyhow!("Settlement failed: {}", error_text));
        }

        info!("✅ Settlement successful for {}", payload.get("meter_serial").and_then(|v| v.as_str()).unwrap_or(""));
        Ok(())
    }

    /// Execute Batched Generation Mint for verified billing bins via REST API
    pub async fn settle_generation_mint_batch(
        &self,
        base_url: &str,
        payloads: Vec<serde_json::Value>,
    ) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/settlement/generation-mint/batch", base_url);

        info!(
            "💰 [REST] Submitting batched settlement for {} records",
            payloads.len()
        );

        let batch_payload = serde_json::json!({
            "requests": payloads
        });

        let request_builder = client.post(&url).json(&batch_payload);
        let response = request_builder.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            error!("❌ Batched settlement failed with status {}: {}", status, error_text);
            return Err(anyhow::anyhow!("Batched settlement failed: {}", error_text));
        }

        info!("✅ Batched settlement successful");
        Ok(())
    }
}
