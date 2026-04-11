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

    /// Execute Generation Mint for a verified billing bin via REST API
    /// (Currently using REST for settlement as it involves complex on-chain coordinator)
    pub async fn settle_generation_mint(&self, base_url: &str, payload: &serde_json::Value) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/settlement/generation-mint", base_url);
        
        info!("💰 [REST] Submitting settlement for {} ({} - {})", 
            payload.get("meter_serial").and_then(|v| v.as_str()).unwrap_or("unknown"),
            payload.get("start_time").and_then(|v| v.as_str()).unwrap_or("?"),
            payload.get("end_time").and_then(|v| v.as_str()).unwrap_or("?")
        );

        // 1. Inject distributed tracing context (traceparent)
        let mut request_builder = client.post(&url).json(payload);
        
        use opentelemetry::global;
        use opentelemetry::propagation::TextMapPropagator;
        
        struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);
        impl<'a> opentelemetry::propagation::Injector for HeaderInjector<'a> {
            fn set(&mut self, key: &str, value: String) {
                if let Ok(name) = reqwest::header::HeaderName::from_bytes(key.as_bytes()) {
                    if let Ok(val) = reqwest::header::HeaderValue::from_str(&value) {
                        self.0.insert(name, val);
                    }
                }
            }
        }

        let mut headers = reqwest::header::HeaderMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&opentelemetry::Context::current(), &mut HeaderInjector(&mut headers));
        });
        
        request_builder = request_builder.headers(headers);

        // 2. Send Request
        let response = request_builder.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            error!("❌ Settlement failed with status {}: {}", status, error_text);
            return Err(anyhow::anyhow!("Settlement failed: {}", error_text));
        }

        info!("✅ Settlement successful for {}", payload.get("meter_serial").and_then(|v| v.as_str()).unwrap_or(""));
        Ok(())
    }
}
