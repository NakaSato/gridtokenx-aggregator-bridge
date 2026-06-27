use crate::dispatch::DispatchAdapter;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use connectrpc::client::{ClientConfig, HttpClient as Client};
use http::Uri;

// Generated code now lives in the aggregator-protocol crate.
pub use aggregator_protocol::dispatch::{DispatchControllerClient, DispatchType, FlexCommand};
use buffa::EnumValue;

pub struct DispatchClient {
    client: DispatchControllerClient<Client>,
}

#[async_trait]
impl DispatchAdapter for DispatchClient {
    async fn execute_dispatch(&self, action: DispatchType, capacity_kw: f64) -> Result<()> {
        let request = FlexCommand {
            cluster_id: "default-cluster".to_string(),
            dispatch_type: EnumValue::Known(action),
            capacity_kw,
            timestamp: gridtokenx_telemetry::time::now().timestamp(),
            __buffa_cached_size: Default::default(),
            __buffa_unknown_fields: Default::default(),
        };

        // Simple retry logic
        let mut retries = 3;
        while retries > 0 {
            match self.client.execute_flex_dispatch(request.clone()).await {
                Ok(response) => {
                    let owned = response.into_owned();
                    if owned.success {
                        return Ok(());
                    } else {
                        return Err(anyhow!("Dispatch failed: {}", owned.message));
                    }
                }
                Err(e) => {
                    retries -= 1;
                    if retries == 0 {
                        return Err(anyhow!("gRPC error: {}", e));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        Ok(())
    }
}

impl DispatchClient {
    pub async fn new(addr: String) -> Result<Self> {
        let uri = addr.parse::<Uri>()?;
        let config = ClientConfig::new(uri);
        let client = DispatchControllerClient::new(Client::plaintext(), config);
        Ok(Self { client })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_accepts_valid_uri() {
        // connectrpc client is lazy — no socket opened at construction, so a
        // syntactically valid URI succeeds without a live server.
        assert!(DispatchClient::new("http://localhost:5999".to_string())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn new_rejects_malformed_uri() {
        assert!(DispatchClient::new("".to_string()).await.is_err());
        assert!(DispatchClient::new("not a uri".to_string()).await.is_err());
    }

    #[tokio::test]
    #[ignore = "requires a live DispatchController gRPC server (DISPATCH_GRPC_URL)"]
    async fn execute_dispatch_against_real_server() {
        let url = std::env::var("DISPATCH_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:5030".to_string());
        let client = DispatchClient::new(url).await.expect("construct client");
        // Smoke: a reachable server returns Ok/Err, not a panic. Either outcome
        // exercises the request-build + retry path.
        let _ = client.execute_dispatch(DispatchType::FLEX_UP, 5.0).await;
    }
}
