use std::sync::Arc;

use crate::protocol::stacks::dlms::DlmsStack;
use crate::protocol::stacks::ocpp::OcppStack;
use crate::protocol::stacks::openadr::OpenAdrStack;
use crate::protocol::stacks::sunspec::SunSpecStack;
use crate::router::Router;

// Generated identity ConnectRPC code now lives in the oracle-protocol crate.
// Re-exported here so existing `crate::state::identity::*` paths keep resolving.
pub use oracle_protocol::identity;

pub use identity::IdentityServiceClient;

use connectrpc::client::SharedHttp2Connection;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct Metrics {
    pub total_requests: AtomicU64,
    pub authorized_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub last_grpc_latency_us: AtomicU64,
    pub total_grpc_latency_us: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            authorized_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            last_grpc_latency_us: AtomicU64::new(0),
            total_grpc_latency_us: AtomicU64::new(0),
        }
    }

    pub fn record_request(&self, authorized: bool, latency_us: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if authorized {
            self.authorized_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_requests.fetch_add(1, Ordering::Relaxed);
        }
        self.last_grpc_latency_us
            .store(latency_us, Ordering::Relaxed);
        self.total_grpc_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }
}

/// Shared application state, injected into Axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub router: Arc<Router>,

    // Private Network Protocol Stacks
    pub ocpp_stack: Arc<OcppStack>,
    pub sunspec_stack: Arc<SunSpecStack>,
    pub dlms_stack: Arc<DlmsStack>,
    pub openadr_stack: Arc<OpenAdrStack>,

    pub api_keys: Vec<String>,
    pub identity_client: Option<Arc<IdentityServiceClient<SharedHttp2Connection>>>,
    pub metrics: Arc<Metrics>,

    // Infrastructure
    pub kafka_producer: Option<Arc<crate::infra::kafka::OracleKafkaProducer>>,
    pub rabbitmq_producer: Option<Arc<crate::infra::rabbitmq::OracleRabbitMQProducer>>,
    pub signature_verifier: Arc<crate::infra::crypto::SignatureVerifier>,
    pub settlement_signer: Option<Arc<crate::infra::crypto::SettlementSigner>>,
    pub meter_registry: Arc<crate::infra::meter_registry::MeterRegistry>,
}
