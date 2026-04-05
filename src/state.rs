use std::sync::Arc;

use crate::protocol::stacks::ocpp::OcppStack;
use crate::protocol::stacks::sunspec::SunSpecStack;
use crate::protocol::stacks::dlms::DlmsStack;
use crate::protocol::stacks::openadr::OpenAdrStack;
use crate::router::Router;

pub mod identity {
    include!(concat!(env!("OUT_DIR"), "/_identity_include.rs"));
    pub use identity::*;
}

pub use identity::IdentityServiceClient;

use std::sync::atomic::{AtomicU64, Ordering};
use connectrpc::client::SharedHttp2Connection;

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
        self.last_grpc_latency_us.store(latency_us, Ordering::Relaxed);
        self.total_grpc_latency_us.fetch_add(latency_us, Ordering::Relaxed);
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
    // Edge-AI (Neural Inference Load Monitoring)
    pub nilm_engine: Arc<crate::nilm::NilmEngine>,
    pub gradient_accumulator: Arc<tokio::sync::Mutex<crate::nilm::LocalGradientAccumulator>>,
}
