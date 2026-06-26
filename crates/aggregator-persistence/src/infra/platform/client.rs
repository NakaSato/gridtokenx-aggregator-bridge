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

    /// Submit a batch of verified meter readings (operational telemetry path)
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
}
