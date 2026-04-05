use anyhow::Result;
use tracing::{info, error};
use connectrpc::client::{Http2Connection, SharedHttp2Connection, ClientConfig};
use connectrpc::Protocol;

// Include the generated ConnectRPC code
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/_oracle_include.rs"));
}

pub use proto::gridtokenx::oracle::v1::{
    OracleServiceClient, TelemetryRequest,
    TelemetryBatchRequest, TelemetryBatchResponse,
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
        
        Ok(Self {
            client,
        })
    }

    /// Submit a batch of telemetry readings (High performance)
    pub async fn submit_telemetry_batch(&self, requests: Vec<TelemetryRequest>) -> Result<TelemetryBatchResponse> {
        let count = requests.len();
        info!("📤 [ConnectRPC] Submitting batch of {} telemetry readings to platform", count);

        let batch_req = TelemetryBatchRequest {
            readings: requests,
            ..Default::default()
        };

        match self.client.submit_telemetry_batch(batch_req).await {
            Ok(res) => {
                let view = res.view();
                info!("✅ Batch telemetry ingested. Accepted: {}, Rejected: {}", 
                      view.accepted_count, view.rejected_count);
                
                // Return a fresh TelemetryBatchResponse based on the view
                Ok(TelemetryBatchResponse {
                    accepted_count: view.accepted_count,
                    rejected_count: view.rejected_count,
                    ..Default::default()
                })
            }
            Err(e) => {
                error!("❌ Batch telemetry submission failed: {}", e);
                Err(anyhow::anyhow!("RPC error: {}", e))
            }
        }
    }
}
