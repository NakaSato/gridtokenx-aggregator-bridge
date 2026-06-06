use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{Context as _, Result};
use axum::{
    routing::{get, post},
    Router as AxumRouter,
};
use dotenvy::dotenv;
use tracing::{error, info, warn};

// The entire application now lives in the `oracle-api` crate (which itself layers over
// oracle-logic / oracle-persistence / oracle-protocol / oracle-stacks / oracle-core).
// This binary is a thin entrypoint that wires the components and runs the servers.
use oracle_api::{
    aggregator, dispatch, grpc, handlers, infra, ingester, protocol, router, state, telemetry,
};

use tokio::signal;
use tokio_util::sync::CancellationToken;

use protocol::stacks::dlms::DlmsStack;
use protocol::stacks::ocpp::OcppStack;
use protocol::stacks::openadr::OpenAdrStack;
use protocol::stacks::sunspec::SunSpecStack;
use state::AppState;

fn expand_env(s: &str) -> String {
    let mut result = s.to_string();
    while let Some(start) = result.find("${") {
        if let Some(rest) = result.get(start + 2..) {
            if let Some(end_offset) = rest.find('}') {
                let end = start + 2 + end_offset;
                let var_name = &result[start + 2..end];
                let value = std::env::var(var_name).unwrap_or_else(|_| "".to_string());
                result.replace_range(start..end + 1, &value);
                continue;
            }
        }
        break;
    }
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize
    dotenv().ok();
    // Initialize OpenTelemetry tracing (sets up global subscriber)
    let _telemetry_guard = telemetry::init_telemetry("gridtokenx-oracle-bridge");

    info!("🚀 Starting GridTokenX Oracle Bridge (Zone-Based Microgrid Mode)");

    // 2. Configuration
    let redis_url = expand_env(
        &std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
    );
    let api_services_url = expand_env(
        &std::env::var("API_SERVICES_URL").unwrap_or_else(|_| "http://127.0.0.1:4000".to_string()),
    );
    let api_services_grpc_url = expand_env(
        &std::env::var("API_SERVICES_GRPC_URL").unwrap_or_else(|_| api_services_url.clone()),
    );
    let gateway_port: u16 = std::env::var("IOT_GATEWAY_PORT")
        .unwrap_or_else(|_| "4010".to_string())
        .parse()
        .unwrap_or(4010);
    let iam_service_url = expand_env(
        &std::env::var("IAM_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string()),
    );
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

    // 4b. Initialize Meter Registry (meter_serial → user_id resolver)
    let early_redis_client = redis::Client::open(redis_url.clone())?;
    let early_redis_conn_result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        redis::aio::ConnectionManager::new(early_redis_client)
    ).await;

    let early_redis_conn = match early_redis_conn_result {
        Ok(Ok(conn)) => Some(conn),
        _ => {
            warn!("⚠️ Redis connection timed out or failed. Running in degraded mode without Redis.");
            None
        }
    };

    let meter_registry = Arc::new(infra::meter_registry::MeterRegistry::new(
        early_redis_conn.clone(),
    ));

    // 5. Initialize Zone-based Event Ingester (parallel processing by microgrid zone)
    info!("🔷 Zone-based ingester ENABLED");
    let zone_ingester = match ingester::zone_ingester::ZoneEventIngester::new(
        &redis_url,
        &api_services_grpc_url,
        aggregator.clone(),
        metrics.clone(),
        num_zones,
        meter_registry.clone(),
    )
    .await
    {
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

    info!(
        "👂 Zone-based Oracle Bridge listening on {} zone streams",
        num_zones
    );

    // 6. Initialize IoT Gateway components
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let nats_client = async_nats::connect(&nats_url).await.ok();
    if nats_client.is_some() {
        info!("🔗 Connected to NATS for telemetry forwarding");
    } else {
        warn!("⚠️ Could not connect to NATS. Telemetry forwarding disabled.");
    }

    let iot_router = Arc::new(
        router::Router::new(&redis_url, num_zones, nats_client)
            .await
            .context("Failed to initialize IoT router")?,
    );
    let api_keys_raw = std::env::var("GRIDTOKENX_API_KEYS").unwrap_or_default();
    let api_keys: Vec<String> = api_keys_raw
        .split(',')
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // 7. Initialize New Architecture Infrastructure
    info!("🏗️ Initializing New Architecture Infrastructure...");

    // Kafka Producer
    let kafka_producer = if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
        let topic = std::env::var("KAFKA_TOPIC_METER_READINGS")
            .unwrap_or_else(|_| "meter.readings".to_string());
        match infra::kafka::OracleKafkaProducer::new(&brokers, &topic) {
            Ok(p) => Some(Arc::new(p)),
            Err(e) => {
                warn!(
                    "⚠️ Kafka initialization failed: {}. High-throughput streaming disabled.",
                    e
                );
                None
            }
        }
    } else {
        info!("ℹ️ Kafka disabled (KAFKA_BOOTSTRAP_SERVERS not set)");
        None
    };

    // 8. Initialize Dispatch Engine
    let aggregator = Arc::new(Mutex::new(aggregator::Aggregator::default()));
    let grpc_client = dispatch::grpc_client::DispatchClient::new("http://127.0.0.1:50051".to_string()).await?;
    let mut dispatch_engine = dispatch::engine::DispatchEngine::new(aggregator.clone(), grpc_client);
    
    // Kafka Consumer for Dispatch
    let kafka_consumer = if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
        let topic = std::env::var("KAFKA_TOPIC_GRID_STATUS")
            .unwrap_or_else(|_| "gridtokenx.oracle.grid_status".to_string());
        match infra::kafka::OracleKafkaConsumer::new(&brokers, "oracle-bridge-group", &topic) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                error!("❌ Kafka consumer init failed: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Spawn Kafka Dispatch Listener
    if let Some(consumer) = kafka_consumer {
        tokio::spawn(async move {
            info!("📡 Dispatch Engine Kafka listener started");
            loop {
                match consumer.consume_grid_status().await {
                    Ok(event) => {
                        if let Err(e) = dispatch_engine.evaluate_and_dispatch(event.frequency).await {
                            error!("❌ Dispatch Engine error: {}", e);
                        }
                    }
                    Err(e) => error!("❌ Kafka consume error: {}", e),
                }
            }
        });
    }

    // RabbitMQ Producer
    let rabbitmq_producer = if let Ok(url) = std::env::var("RABBITMQ_URL") {
        match infra::rabbitmq::OracleRabbitMQProducer::new(&url).await {
            Ok(p) => Some(Arc::new(p)),
            Err(e) => {
                warn!(
                    "⚠️ RabbitMQ initialization failed: {}. Task queues disabled.",
                    e
                );
                None
            }
        }
    } else {
        info!("ℹ️ RabbitMQ disabled (RABBITMQ_URL not set)");
        None
    };

    // Crypto: Signature Verifier
    let signature_verifier = Arc::new(infra::crypto::SignatureVerifier::new(
        early_redis_conn.clone(),
    ));

    // Crypto: Settlement Signer
    let settlement_signer = if let Ok(key_path) = std::env::var("ORACLE_BRIDGE_SIGNING_KEY") {
        match std::fs::read(&key_path) {
            Ok(bytes) => match infra::crypto::SettlementSigner::new(&bytes) {
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
    // 7c. Start UTT Settlement Engine (Path B)
    let settlement_agg = aggregator.clone();
    let settlement_shutdown = shutdown_token.clone();
    let settlement_api_url = std::env::var("SETTLEMENT_API_URL")
        .or_else(|_| std::env::var("TRADING_HTTP_URL"))
        .unwrap_or_else(|_| "http://trading-service:8093".to_string());
    
    info!("💰 UTT Path B Settlement target: {}", settlement_api_url);
    let settlement_signer_task = settlement_signer.clone();
    let _settlement_handle = tokio::spawn(async move {
        let engine = ingester::settlement_engine::SettlementEngine::new(
            settlement_agg,
            settlement_api_url,
            settlement_signer_task,
        );
        engine.run(settlement_shutdown).await;
    });

    // 7. Initialize IAM gRPC Client (optional - auth falls back to static API keys)
    use connectrpc::client::{ClientConfig, Http2Connection};
    use connectrpc::Protocol;

    let identity_client = async {
        let uri: http::Uri = iam_service_url.parse().ok()?;
        let conn = Http2Connection::connect_plaintext(uri.clone())
            .await
            .map_err(|e| {
                warn!(
                    "⚠️  IAM gRPC connection failed (IAM feature might be degraded): {}",
                    e
                )
            })
            .ok()?
            .shared(1024);
        let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
        let client = state::IdentityServiceClient::new(conn, config);
        info!("✅ IAM gRPC client connected to {}", iam_service_url);
        Some(Arc::new(client))
    }
    .await;

    // 7b. Initialize Prometheus metrics exporter
    let metrics_recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_quantiles(&[0.0, 0.5, 0.9, 0.95, 0.99, 1.0])
        .context("Failed to set metrics quantiles")?
        .build_recorder();
    let metrics_handle = metrics_recorder.handle();
    ::metrics::set_global_recorder(metrics_recorder).context("Failed to set metrics recorder")?;
    info!("✅ Prometheus metrics exporter initialized");

    let app_state = AppState {
        router: iot_router,
        ocpp_stack: Arc::new(OcppStack::new()),
        sunspec_stack: Arc::new(SunSpecStack::new()),
        dlms_stack: Arc::new(DlmsStack::new()),
        openadr_stack: Arc::new(OpenAdrStack::new()),
        api_keys,
        identity_client,
        metrics,
        kafka_producer,
        rabbitmq_producer,
        signature_verifier,
        settlement_signer,
        meter_registry,
    };

    // 8. Build IoT Gateway HTTP routes
    let api_routes = AxumRouter::new().route(
        "/private-network/ingest",
        post(handlers::ingest_private_network),
    );

    // Metrics endpoint that returns Prometheus format
    let metrics_handle = Arc::new(metrics_handle);

    let app = AxumRouter::new()
        .route("/health", get(handlers::health))
        .route(
            "/v1/private-network/ingest",
            post(handlers::ingest_private_network),
        )
        .route(
            "/v1/private-network/ingest/batch",
            post(handlers::ingest_private_network_batch),
        )
        .route(
            "/v1/ingest/telemetry",
            post(handlers::ingest_legacy_batch),
        )
        .route(
            "/v1/ingest/telemetry/batch",
            post(handlers::ingest_legacy_batch),
        )
        // .layer(axum::middleware::from_fn(middleware::otel_tracing::otel_tracing_middleware))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", gateway_port))
        .await
        .context("Failed to bind IoT Gateway listener")?;

    info!("✅ Oracle Bridge + IoT Gateway initialized");
    info!(
        "   📡 IoT Gateway accepting connections on 0.0.0.0:{}",
        gateway_port
    );
    info!(
        "   👂 Zone-based Oracle Bridge listening on {} zone streams",
        num_zones
    );
    info!(
        "   ➡️  Forwarding readings to Kong Gateway: {}",
        api_services_url
    );

    // 9. Run HTTP and gRPC Servers concurrently (Industrial Standard)
    // Default to the canonical Oracle Bridge gRPC port (:5030 per the README port
    // table / Envoy mesh route); override with GRPC_PORT.
    let grpc_port = std::env::var("GRPC_PORT").unwrap_or_else(|_| "5030".to_string());
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port)
        .parse()
        .context("Failed to parse gRPC address")?;

    info!(
        "🚀 Starting Industrial gRPC ingestion service on {}...",
        grpc_addr
    );

    // Initialize gRPC service with platform-standard registration
    let oracle_grpc = Arc::new(grpc::OracleServiceImpl::new(app_state.clone()));
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
    // telemetry::shutdown_telemetry(&telemetry_guard);
    server_result
}
