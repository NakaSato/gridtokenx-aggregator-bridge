use std::sync::Arc;

use crate::protocol::stacks::dlms::DlmsStack;
use crate::router::Router;

// Generated identity ConnectRPC code now lives in the aggregator-protocol crate.
// Re-exported here so existing `crate::state::identity::*` paths keep resolving.
pub use aggregator_protocol::identity;

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

    // Private Network Protocol Stack: DLMS/COSEM is the only meter protocol.
    pub dlms_stack: Arc<DlmsStack>,

    pub api_keys: Vec<String>,
    pub identity_client: Option<Arc<IdentityServiceClient<SharedHttp2Connection>>>,
    pub metrics: Arc<Metrics>,

    // Infrastructure
    pub kafka_producer: Option<Arc<crate::infra::kafka::AggregatorKafkaProducer>>,
    pub rabbitmq_producer: Option<Arc<crate::infra::rabbitmq::AggregatorRabbitMQProducer>>,
    pub signature_verifier: Arc<crate::infra::crypto::SignatureVerifier>,
    // Per-device AES-256 key registry for decrypting secure v4 DLMS frames.
    // Self-healing Redis (mirrors signature_verifier); reads `…:enckey`.
    pub device_key_registry: Arc<crate::infra::crypto::DeviceKeyRegistry>,
    pub meter_registry: Arc<crate::infra::meter_registry::MeterRegistry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_new_is_zeroed() {
        let m = Metrics::new();
        assert_eq!(m.total_requests.load(Ordering::Relaxed), 0);
        assert_eq!(m.authorized_requests.load(Ordering::Relaxed), 0);
        assert_eq!(m.failed_requests.load(Ordering::Relaxed), 0);
        assert_eq!(m.last_grpc_latency_us.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_grpc_latency_us.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn record_request_authorized_bumps_total_and_authorized() {
        let m = Metrics::new();
        m.record_request(true, 120);
        assert_eq!(m.total_requests.load(Ordering::Relaxed), 1);
        assert_eq!(m.authorized_requests.load(Ordering::Relaxed), 1);
        assert_eq!(m.failed_requests.load(Ordering::Relaxed), 0);
        assert_eq!(m.last_grpc_latency_us.load(Ordering::Relaxed), 120);
    }

    #[test]
    fn record_request_unauthorized_bumps_failed() {
        let m = Metrics::new();
        m.record_request(false, 50);
        assert_eq!(m.total_requests.load(Ordering::Relaxed), 1);
        assert_eq!(m.authorized_requests.load(Ordering::Relaxed), 0);
        assert_eq!(m.failed_requests.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_request_accumulates_latency_and_tracks_last() {
        let m = Metrics::new();
        m.record_request(true, 100);
        m.record_request(false, 300);
        assert_eq!(m.total_requests.load(Ordering::Relaxed), 2);
        assert_eq!(m.authorized_requests.load(Ordering::Relaxed), 1);
        assert_eq!(m.failed_requests.load(Ordering::Relaxed), 1);
        // total accumulates, last reflects the most recent call.
        assert_eq!(m.total_grpc_latency_us.load(Ordering::Relaxed), 400);
        assert_eq!(m.last_grpc_latency_us.load(Ordering::Relaxed), 300);
    }
}
