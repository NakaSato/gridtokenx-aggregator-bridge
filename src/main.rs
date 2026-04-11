use std::sync::Arc;

use anyhow::{Result, Context as _};
use axum::{routing::{get, post}, Router as AxumRouter};
use dotenvy::dotenv;
use tracing::{error, info, warn};

mod telemetry;
mod ingester;
mod aggregator;
mod handlers;
mod infra;
mod models;
mod protocol;
mod router;
mod state;
mod auth;
mod metrics;
mod utils;
mod middleware;
mod grpc;
mod storage;
mod nilm;

use tokio_util::sync::CancellationToken;
use tokio::signal;


use protocol::stacks::ocpp::OcppStack;
use protocol::stacks::sunspec::SunSpecStack;
use protocol::stacks::dlms::DlmsStack;
use protocol::stacks::openadr::OpenAdrStack;
use state::AppState;
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize
    dotenv().ok();
    // Initialize OpenTelemetry tracing (sets up global subscriber)
    let telemetry_guard = telemetry::init_telemetry("gridtokenx-oracle-bridge");

    info!("🚀 Starting GridTokenX Oracle Bridge (Zone-Based Microgrid Mode)");

    // 2. Configuration
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let api_services_url = std::env::var("API_SERVICES_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4000".to_string());
    let api_services_grpc_url = std::env::var("API_SERVICES_GRPC_URL")
        .unwrap_or_else(|_| api_services_url.clone());
    let gateway_port: u16 = std::env::var("IOT_GATEWAY_PORT")
        .unwrap_or_else(|_| "4010".to_string())
        .parse()
        .unwrap_or(4010);
    let iam_service_url = std::env::var("IAM_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let num_zones: usize = std::env::var("IOT_NUM_ZONES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    info!("🔗 Redis: {}", redis_url);
    info!("🔗 API Services (REST): {}", api_services_url);
    info!("🔗 API Services (gRPC): {}", api_services_grpc_url);
    info!("🔗 IAM Service: {}", iam_service_url);
    info!("🌐 IoT Gateway port: {}", gateway_port);
    info!("🔢 Zone partitions: {}", num_zones);

    // 3. Initialize Shared Metrics
    let metrics = Arc::new(state::Metrics::new());

    // 4. Initialize Aggregator (local stats only, no blockchain submission)
    let aggregator = Arc::new(tokio::sync::Mutex::new(aggregator::Aggregator::new()));

    // 5. Initialize Zone-based Event Ingester (parallel processing by microgrid zone)
    info!("🔷 Zone-based ingester ENABLED");
    let zone_ingester = match ingester::zone_ingester::ZoneEventIngester::new(
        &redis_url,
        &api_services_grpc_url,
        aggregator.clone(),
        metrics.clone(),
        num_zones,
    ).await {
        Ok(zi) => Some(Arc::new(zi)),
        Err(e) => {
            warn!("⚠️ Zone-based ingester initialization deferred: {}. Direct gRPC ingestion path remains active.", e);
            None
        }
    };

    // Lifecycle coordination
    let shutdown_token = CancellationToken::new();

    // Run zone ingester in background if initialized
    if let Some(zi) = zone_ingester {
        let zone_shutdown = shutdown_token.clone();
        let _zone_handle = tokio::spawn(async move {
            if let Err(e) = zi.run(zone_shutdown).await {
                error!("❌ Zone ingester failed: {}", e);
            }
        });
    }


    info!("👂 Zone-based Oracle Bridge listening on {} zone streams", num_zones);



    // 6. Initialize IoT Gateway components
    let iot_router = Arc::new(router::Router::new(&redis_url, num_zones).await.context("Failed to initialize IoT router")?);
    let api_keys_raw = std::env::var("GRIDTOKENX_API_KEYS").unwrap_or_default();
    let api_keys: Vec<String> = api_keys_raw.split(',').map(|s| s.to_string()).filter(|s| !s.is_empty()).collect();

    // 7. Initialize New Architecture Infrastructure
    info!("🏗️ Initializing New Architecture Infrastructure...");
    
    // Kafka Producer
    let kafka_producer = if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
        let topic = std::env::var("KAFKA_TOPIC_METER_READINGS").unwrap_or_else(|_| "meter.readings".to_string());
        match crate::infra::kafka::OracleKafkaProducer::new(&brokers, &topic) {
            Ok(p) => Some(Arc::new(p)),
            Err(e) => {
                warn!("⚠️ Kafka initialization failed: {}. High-throughput streaming disabled.", e);
                None
            }
        }
    } else {
        info!("ℹ️ Kafka disabled (KAFKA_BOOTSTRAP_SERVERS not set)");
        None
    };

    // RabbitMQ Producer
    let rabbitmq_producer = if let Ok(url) = std::env::var("RABBITMQ_URL") {
        match crate::infra::rabbitmq::OracleRabbitMQProducer::new(&url).await {
            Ok(p) => Some(Arc::new(p)),
            Err(e) => {
                warn!("⚠️ RabbitMQ initialization failed: {}. Task queues disabled.", e);
                None
            }
        }
    } else {
        info!("ℹ️ RabbitMQ disabled (RABBITMQ_URL not set)");
        None
    };

    // Crypto: Signature Verifier
    let redis_client = redis::Client::open(redis_url.clone())?;
    let redis_conn_manager = redis::aio::ConnectionManager::new(redis_client).await?;
    let signature_verifier = Arc::new(crate::infra::crypto::SignatureVerifier::new(redis_conn_manager));

    // Crypto: Settlement Signer
    let settlement_signer = if let Ok(key_path) = std::env::var("ORACLE_BRIDGE_SIGNING_KEY") {
        match std::fs::read(&key_path) {
            Ok(bytes) => match crate::infra::crypto::SettlementSigner::new(&bytes) {
                Ok(s) => Some(Arc::new(s)),
                Err(e) => {
                    warn!("⚠️ Failed to initialize settlement signer: {}. Settlements will use placeholders.", e);
                    None
                }
            },
            Err(e) => {
                warn!("⚠️ Could not read Oracle Bridge signing key at {}: {}. Settlements will use placeholders.", key_path, e);
                None
            }
        }
    } else {
        info!("ℹ️ Settlement signer disabled (ORACLE_BRIDGE_SIGNING_KEY not set)");
        None
    };
    // 7c. Start Settlement Worker in background
    let settlement_agg = aggregator.clone();
    let settlement_shutdown = shutdown_token.clone();
    let settlement_api_url = api_services_url.clone();
    let settlement_signer_task = settlement_signer.clone();
    let settlement_handle = tokio::spawn(async move {
        let worker = crate::ingester::settlement_worker::SettlementWorker::new(
            settlement_agg, 
            settlement_api_url,
            settlement_signer_task
        );
        worker.run(settlement_shutdown).await;
    });

    // 7. Initialize IAM gRPC Client (optional - auth falls back to static API keys)
    use connectrpc::client::{Http2Connection, ClientConfig};
    use connectrpc::Protocol;

    let identity_client = async {
        let uri: http::Uri = iam_service_url.parse().ok()?;
        let conn = Http2Connection::connect_plaintext(uri.clone())
            .await
            .map_err(|e| warn!("⚠️  IAM gRPC connection failed (IAM feature might be degraded): {}", e))
            .ok()?
            .shared(1024);
        let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
        let client = crate::state::IdentityServiceClient::new(conn, config);
        info!("✅ IAM gRPC client connected to {}", iam_service_url);
        Some(Arc::new(client))
    }.await;

    // 7b. Initialize Prometheus metrics exporter
    let metrics_recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_quantiles(&[0.0, 0.5, 0.9, 0.95, 0.99, 1.0])
        .context("Failed to set metrics quantiles")?
        .build_recorder();
    let metrics_handle = metrics_recorder.handle();
    ::metrics::set_global_recorder(metrics_recorder).context("Failed to set metrics recorder")?;
    info!("✅ Prometheus metrics exporter initialized");

    // 8. Initialize NILM Engine and Federated learning (Neural Edge Intelligence)
    info!("🧠 Initializing NILM Neural Engine (Sparse MoE)...");
    let nilm_engine = Arc::new(crate::nilm::NilmEngine::new().await.context("Failed to initialize NILM Engine")?);
    let gradient_accumulator = Arc::new(tokio::sync::Mutex::new(crate::nilm::LocalGradientAccumulator::new()));
    
    // 8a. Bootstrap Global Aggregator from persistent state
    let model_state_path_raw = std::env::var("NILM_MODEL_STATE_PATH")
        .unwrap_or_else(|_| "storage/nilm_model_state.json".to_string());
    let model_state_path = std::path::Path::new(&model_state_path_raw);
    
    let global_aggregator_inner = if model_state_path.exists() {
        match crate::nilm::GlobalModelAggregator::load_from_file(model_state_path) {
            Ok(agg) => agg,
            Err(e) => {
                warn!("⚠️ Failed to load persistent model state: {}. Defaulting to v1.0.0", e);
                crate::nilm::GlobalModelAggregator::new(5)
            }
        }
    } else {
        info!("🆕 No persistent model state found. Starting fresh collective intelligence (v1.0.0)");
        crate::nilm::GlobalModelAggregator::new(5) // Threshold of 5 nodes
    };
    
    let global_aggregator = Arc::new(tokio::sync::Mutex::new(global_aggregator_inner));

    // 8b. Spawn Federated Learning Sync Loop (Cloud Persistence)
    let federated_shutdown = shutdown_token.clone();
    let federated_accumulator = gradient_accumulator.clone();
    let _federated_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut acc = federated_accumulator.lock().await;
                    if let Err(e) = acc.upload_gradients().await {
                        error!("❌ Federated gradient upload failed: {}", e);
                    }
                }
                _ = federated_shutdown.cancelled() => {
                    info!("🔄 Federated learning sync loop shutting down...");
                    break;
                }
            }
        }
    });

    let app_state = AppState {
        router: iot_router,
        ocpp_stack: Arc::new(OcppStack::new()),
        sunspec_stack: Arc::new(SunSpecStack::new()),
        dlms_stack: Arc::new(DlmsStack::new()),
        openadr_stack: Arc::new(OpenAdrStack::new()),
        api_keys,
        identity_client,
        metrics,
        nilm_engine,
        gradient_accumulator,
        global_aggregator,
        model_state_path: model_state_path.to_path_buf(),
        kafka_producer,
        rabbitmq_producer,
        signature_verifier,
        settlement_signer,
    };

    // 8. Build IoT Gateway HTTP routes
    let api_routes = AxumRouter::new()
        .route("/private-network/ingest", post(handlers::ingest_private_network));

    // Metrics endpoint that returns Prometheus format
    let metrics_handle = Arc::new(metrics_handle);

    let app = AxumRouter::new()
        .route("/health", get(handlers::health))
        .route("/v1/private-network/ingest", post(handlers::ingest_private_network))
        .route("/v1/private-network/ingest/batch", post(handlers::ingest_private_network_batch))
        .layer(axum::middleware::from_fn(middleware::otel_tracing::otel_tracing_middleware))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", gateway_port))
        .await
        .context("Failed to bind IoT Gateway listener")?;

    info!("✅ Oracle Bridge + IoT Gateway initialized");
    info!("   📡 IoT Gateway accepting connections on 0.0.0.0:{}", gateway_port);
    info!("   👂 Zone-based Oracle Bridge listening on {} zone streams", num_zones);
    info!("   ➡️  Forwarding readings to Kong Gateway: {}", api_services_url);

    // 9. Run HTTP and gRPC Servers concurrently (Industrial Standard)
    let grpc_port = std::env::var("GRPC_PORT").unwrap_or_else(|_| "50051".to_string());
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port)
        .parse()
        .context("Failed to parse gRPC address")?;
    
    info!("🚀 Starting Industrial gRPC ingestion service on {}...", grpc_addr);

    // Initialize gRPC service with platform-standard registration
    let oracle_grpc = Arc::new(crate::grpc::OracleServiceImpl::new(app_state.clone()));
    let grpc_router = oracle_grpc.register_service(connectrpc::Router::new());
    let grpc_server = connectrpc::Server::new(grpc_router);

    // 10. Start gRPC server in background
    let grpc_shutdown = shutdown_token.clone();
    let _grpc_handle = tokio::spawn(async move {
        tokio::select! {
            res = grpc_server.serve(grpc_addr) => {
                if let Err(e) = res {
                    error!("❌ gRPC server failed: {}", e);
                }
            }
            _ = grpc_shutdown.cancelled() => {
                info!("🔄 Oracle Industrial gRPC Service shutting down...");
            }
        }
    });

    info!("🚀 Starting HTTP gateway...");
    let server_shutdown = shutdown_token.clone();
    let server_result: anyhow::Result<()> = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            // Wait for SIGINT, SIGTERM, or common cancellation
            let ctrl_c = async {
                signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl+C handler");
            };

            #[cfg(unix)]
            let terminate = async {
                signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("failed to install signal handler")
                    .recv()
                    .await;
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {
                    info!("🛑 SIGINT received, triggering shutdown...");
                },
                _ = terminate => {
                    info!("🛑 SIGTERM received, triggering shutdown...");
                },
            }
            
            // Signal all background tasks to stop
            server_shutdown.cancel();
        })
        .await
        .context("IoT Gateway HTTP server failed");

    // Clean up gRPC server on shutdown (grpc_server doesn't currently support graceful signal directly in this pattern)
    // In production, we'd wrap this in a tokio task as well.
    let _ = grpc_server; 

    // 10. Wait for background tasks to complete flushes
    info!("⏳ Waiting for background tasks to exit...");
    // Tasks are signaled via shutdown_token.cancel() above

    info!("👋 Shutdown complete. Cleaning up telemetry...");
    telemetry::shutdown_telemetry(&telemetry_guard);
    server_result
}
