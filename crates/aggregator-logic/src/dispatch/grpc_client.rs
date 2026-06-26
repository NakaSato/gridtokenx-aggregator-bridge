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
