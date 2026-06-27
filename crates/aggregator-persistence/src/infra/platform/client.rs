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

#[cfg(test)]
mod tests {
    use super::*;

    // `PlatformClient::new` opens a real HTTP/2 socket (`connect_plaintext`), so
    // it can't be unit-tested without a live OracleService. Both construction and
    // the batch submit are exercised here as `#[ignore]` integration tests.

    #[tokio::test]
    #[ignore = "requires a live OracleService gRPC server (PLATFORM_GRPC_URL)"]
    async fn new_connects_to_real_oracle_service() {
        let url = std::env::var("PLATFORM_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:5030".to_string());
        assert!(PlatformClient::new(&url).await.is_ok());
    }

    #[tokio::test]
    #[ignore = "requires a live OracleService gRPC server (PLATFORM_GRPC_URL)"]
    async fn submit_batch_against_real_oracle_service() {
        let url = std::env::var("PLATFORM_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:5030".to_string());
        let client = PlatformClient::new(&url).await.expect("connect");
        let reading = MeterReading {
            meter_serial: "__test_meter__".to_string(),
            ..Default::default()
        };
        // Smoke: a reachable server returns a response (accept/reject), not a panic.
        let _ = client.submit_meter_reading_batch(vec![reading]).await;
    }
}
