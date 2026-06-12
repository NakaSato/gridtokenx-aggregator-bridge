use std::sync::Arc;

use anyhow::{Context as _, Result};
use axum::{
    routing::{get, post},
    Router as AxumRouter,
};
use dotenvy::dotenv;
use tracing::{error, info, warn};

// The entire application now lives in the `aggregator-api` crate (which itself layers over
// aggregator-logic / aggregator-persistence / aggregator-protocol / aggregator-stacks / aggregator-core).
// This binary is a thin entrypoint that wires the components and runs the servers.
use aggregator_api::{
    aggregator, auth, dispatch, grid_status, grpc, handlers, infra, ingester, protocol, router,
    standards, state, telemetry,
};

use tokio::signal;
use tokio_util::sync::CancellationToken;

use protocol::stacks::dlms::DlmsStack;
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
    // Install default crypto provider for rustls (required for rustls 0.23+ when both
    // ring and aws-lc-rs are in the dependency graph; the mTLS Chain Bridge client
    // panics on provider ambiguity otherwise).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    // 1. Initialize
    dotenv().ok();
    // Initialize OpenTelemetry tracing (sets up global subscriber)
    let _telemetry_guard = telemetry::init_telemetry("gridtokenx-aggregator-bridge");

    info!("🚀 Starting GridTokenX Aggregator Bridge (Zone-Based Microgrid Mode)");

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

    // 4c. Durable billing-bin store (crash recovery for unsettled energy). Restore
    // any bins persisted before a previous restart BEFORE settlement starts, so a
    // bounce mid-window doesn't silently drop the GRID those readings should mint.
    let bin_store = early_redis_conn
        .clone()
        .map(ingester::bin_store::BinStore::new);
    if let Some(store) = &bin_store {
        match store.load_all().await {
            Ok(bins) if !bins.is_empty() => {
                let n = bins.len();
                aggregator.lock().await.rehydrate(bins);
                info!("♻️ Restored {} unsettled billing bins from durable store", n);
            }
            Ok(_) => info!("♻️ Durable bin store empty — fresh start"),
            Err(e) => warn!(
                "⚠️ Failed to restore billing bins from Redis: {} — starting empty",
                e
            ),
        }
    } else {
        warn!("⚠️ No Redis — billing bins are RAM-only (energy lost on restart)");
    }

    // 5. Initialize Zone-based Event Ingester (parallel processing by microgrid zone)
    info!("🔷 Zone-based ingester ENABLED");

    // Rolling grid-frequency window, fed by the zone ingester from reading
    // metadata and drained by the grid-status publisher below. Only useful
    // when Kafka is configured (the dispatch trigger path).
    let frequency_monitor = std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok().map(|_| {
        let window_secs = std::env::var("GRID_FREQ_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60);
        Arc::new(grid_status::FrequencyMonitor::new(
            std::time::Duration::from_secs(window_secs),
        ))
    });

    let zone_ingester = match ingester::zone_ingester::ZoneEventIngester::new(
        &redis_url,
        &api_services_grpc_url,
        aggregator.clone(),
        metrics.clone(),
        num_zones,
        meter_registry.clone(),
        bin_store.clone(),
        frequency_monitor.clone(),
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
        "👂 Zone-based Aggregator Bridge listening on {} zone streams",
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
        match infra::kafka::AggregatorKafkaProducer::new(&brokers, &topic) {
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

    // Grid-status publisher: periodically turn the rolling frequency window
    // (fed by the zone ingester from meter telemetry) into GridStatusEvents on
    // the dispatch topic. The fleet itself is the frequency sensor — no
    // external SCADA feed needed for frequency-driven dispatch.
    if let (Some(monitor), Some(producer)) = (frequency_monitor.clone(), kafka_producer.clone()) {
        let topic = std::env::var("KAFKA_TOPIC_GRID_STATUS")
            .unwrap_or_else(|_| "gridtokenx.aggregator.grid_status".to_string());
        let publish_secs = std::env::var("GRID_STATUS_PUBLISH_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30)
            .max(1);
        let publisher_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            info!(
                "📶 Grid-status publisher started (every {}s → {})",
                publish_secs, topic
            );
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(publish_secs));
            loop {
                tokio::select! {
                    _ = publisher_shutdown.cancelled() => {
                        info!("🔄 Grid-status publisher shutting down...");
                        return;
                    }
                    _ = ticker.tick() => {
                        let Some(frequency) = monitor.mean() else { continue };
                        ::metrics::gauge!("aggregator_grid_frequency_hz").set(frequency);
                        let event = infra::kafka::GridStatusEvent {
                            frequency,
                            load_kw: 0.0,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or_default(),
                        };
                        if let Err(e) = producer.publish_grid_status(&topic, &event).await {
                            warn!("⚠️ Grid-status publish failed: {}", e);
                        }
                    }
                }
            }
        });
    }

    // 8. Initialize Dispatch Engine — reuses the SINGLE shared aggregator (the one
    // the zone ingester fills). Must NOT create a second instance: a separate
    // aggregator here would never see any readings, and previously caused settlement
    // (which clones this binding) to scan an always-empty map → zero mints.
    let dispatch_grpc_url = std::env::var("DISPATCH_GRPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let grpc_client = match dispatch::grpc_client::DispatchClient::new(dispatch_grpc_url.clone()).await {
        Ok(client) => client,
        Err(e) => {
            warn!(
                "⚠️ DISPATCH_GRPC_URL '{}' invalid ({}); falling back to http://127.0.0.1:50051",
                dispatch_grpc_url, e
            );
            dispatch::grpc_client::DispatchClient::new("http://127.0.0.1:50051".to_string()).await?
        }
    };
    let mut dispatch_engine = dispatch::engine::DispatchEngine::new(aggregator.clone(), grpc_client);
    
    // Kafka Consumer for Dispatch
    let kafka_consumer = if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
        let topic = std::env::var("KAFKA_TOPIC_GRID_STATUS")
            .unwrap_or_else(|_| "gridtokenx.aggregator.grid_status".to_string());
        match infra::kafka::AggregatorKafkaConsumer::new(&brokers, "aggregator-bridge-group", &topic) {
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

    // VEN-side OpenADR listener: poll a utility VTN for DISPATCH_SETPOINT events
    // and execute them downstream. The downstream adapter is independent of the
    // dispatch engine's (BL-side) adapter — never "openleadr", or events would
    // loop back to a VTN.
    if let Ok(ven_vtn_url) = std::env::var("OPENLEADR_VEN_VTN_URL") {
        // Self-consumption guard: BL publishes to and VEN polls the SAME VTN.
        // Without a program/target filter the VEN would execute the bridge's own
        // outbound dispatch events — double actuation.
        if std::env::var("OPENLEADR_VTN_URL").as_deref() == Ok(ven_vtn_url.as_str())
            && std::env::var("OPENLEADR_VEN_PROGRAM_ID").is_err()
            && std::env::var("OPENLEADR_VEN_TARGET").is_err()
        {
            warn!(
                "⚠️ OPENLEADR_VEN_VTN_URL matches OPENLEADR_VTN_URL with no \
                 OPENLEADR_VEN_PROGRAM_ID/OPENLEADR_VEN_TARGET filter — the VEN \
                 will execute this bridge's own outbound dispatch events"
            );
        }
        let ven_adapter: Option<Arc<dyn dispatch::DispatchAdapter>> =
            match std::env::var("OPENLEADR_VEN_DISPATCH_ADAPTER").as_deref() {
                Ok("grpc") => {
                    let addr = std::env::var("DISPATCH_GRPC_URL")
                        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
                    match dispatch::grpc_client::DispatchClient::new(addr.clone()).await {
                        Ok(client) => Some(Arc::new(client)),
                        Err(e) => {
                            warn!(
                                "⚠️ OpenADR VEN listener disabled: DISPATCH_GRPC_URL '{}' invalid: {}",
                                addr, e
                            );
                            None
                        }
                    }
                }
                _ => {
                    warn!(
                        "⚠️ OpenADR VEN downstream adapter is the IEEE 2030.5 SIMULATION \
                         stub — dispatches are logged, not actuated. Execution reports are \
                         suppressed (so the VTN is not told a simulated dispatch happened) \
                         unless OPENLEADR_VEN_REPORTS=true is set. Set \
                         OPENLEADR_VEN_DISPATCH_ADAPTER=grpc for production."
                    );
                    Some(Arc::new(standards::ieee2030_5::Ieee2030_5Adapter::new()))
                }
            };
        if let Some(ven_adapter) = ven_adapter {
            match standards::openleadr_ven::OpenLeadrVenListener::from_env(ven_adapter) {
                Ok(Some(listener)) => {
                    let ven_shutdown = shutdown_token.clone();
                    tokio::spawn(async move {
                        listener.run(ven_shutdown.cancelled_owned()).await;
                    });
                }
                Ok(None) => {}
                Err(e) => warn!("⚠️ OpenADR VEN listener disabled: {}", e),
            }
        }
    }

    // RabbitMQ Producer
    let rabbitmq_producer = if let Ok(url) = std::env::var("RABBITMQ_URL") {
        match infra::rabbitmq::AggregatorRabbitMQProducer::new(&url).await {
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

    // Crypto: Signature Verifier.
    // Pass the Redis URL (not the one-shot `early_redis_conn`) so the verifier
    // owns a self-healing connection — it rebuilds transparently after a Redis
    // restart instead of silently rejecting all signed telemetry until the
    // bridge process is restarted.
    let signature_verifier = Arc::new(infra::crypto::SignatureVerifier::new(Some(
        redis_url.clone(),
    )));

    // Crypto: Per-device AES key registry (decrypts secure v4 DLMS frames).
    // Same self-healing Redis URL ownership as the verifier — survives a Redis
    // restart without freezing decryption.
    let device_key_registry = Arc::new(infra::crypto::DeviceKeyRegistry::new(Some(
        redis_url.clone(),
    )));

    // Crypto: Settlement Signer
    let settlement_signer = if let Ok(key_path) = std::env::var("AGGREGATOR_BRIDGE_SIGNING_KEY") {
        match std::fs::read(&key_path) {
            Ok(bytes) => match infra::crypto::SettlementSigner::new(&bytes) {
                Ok(s) => Some(Arc::new(s)),
                Err(e) => {
                    warn!("⚠️ Failed to initialize settlement signer: {}. Settlements will use placeholders.", e);
                    None
                }
            },
            Err(e) => {
                warn!("⚠️ Could not read Aggregator Bridge signing key at {}: {}. Settlements will use placeholders.", key_path, e);
                None
            }
        }
    } else {
        info!("ℹ️ Settlement signer disabled (AGGREGATOR_BRIDGE_SIGNING_KEY not set)");
        None
    };
    // 7. Initialize IAM gRPC Client (optional - auth falls back to static API keys).
    // Built before the Settlement Engine so the generation-mint path can resolve
    // recipient wallets via `GetUserWallet`.
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
        // IAM gates GetUserWallet behind ServiceRole::AggregatorBridge. The role is
        // derived from the `x-gridtokenx-role` header (see ServiceRole::from_headers in
        // gridtokenx-blockchain-core); without it the call is denied as Unknown.
        let config = ClientConfig::new(uri)
            .protocol(Protocol::Grpc)
            .default_header("x-gridtokenx-role", "aggregator-bridge");
        let client = state::IdentityServiceClient::new(conn, config);
        info!("✅ IAM gRPC client connected to {}", iam_service_url);
        Some(Arc::new(client))
    }
    .await;

    // 7c. Start UTT Settlement Engine (Path B)
    let settlement_agg = aggregator.clone();
    let settlement_shutdown = shutdown_token.clone();
    let settlement_api_url = std::env::var("SETTLEMENT_API_URL")
        .or_else(|_| std::env::var("TRADING_HTTP_URL"))
        .unwrap_or_else(|_| "http://trading-service:8093".to_string());

    info!("💰 UTT Path B Settlement target: {}", settlement_api_url);

    // Generation-mint routing: when MINT_VIA_CHAIN_BRIDGE=true the aggregator mints
    // GRID directly via Chain Bridge (Vault signs) instead of POSTing to trading-service.
    // Requires a working BlockchainService AND the IAM client; otherwise fall back to HTTP.
    let mint_via_chain_bridge_requested = std::env::var("MINT_VIA_CHAIN_BRIDGE")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    let settlement_blockchain = if mint_via_chain_bridge_requested {
        match ingester::settlement_engine::build_blockchain_service().await {
            Ok(svc) => Some(svc),
            Err(e) => {
                error!(
                    "❌ MINT_VIA_CHAIN_BRIDGE requested but BlockchainService build failed: {} — using HTTP path",
                    e
                );
                None
            }
        }
    } else {
        None
    };
    let mint_via_chain_bridge =
        mint_via_chain_bridge_requested && settlement_blockchain.is_some() && identity_client.is_some();
    if mint_via_chain_bridge {
        info!("⚡ Generation mint path: Chain Bridge (Vault-signed)");
    } else if mint_via_chain_bridge_requested {
        warn!("⚠️ MINT_VIA_CHAIN_BRIDGE requested but prerequisites missing (blockchain/IAM) — using HTTP settlement path");
    }

    let settlement_signer_task = settlement_signer.clone();
    let settlement_identity_client = identity_client.clone();
    let settlement_bin_store = bin_store.clone();
    let _settlement_handle = tokio::spawn(async move {
        let engine = ingester::settlement_engine::SettlementEngine::new(
            settlement_agg,
            settlement_api_url,
            settlement_signer_task,
            mint_via_chain_bridge,
            settlement_blockchain,
            settlement_identity_client,
            settlement_bin_store,
        );
        engine.run(settlement_shutdown).await;
    });

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
        dlms_stack: Arc::new(DlmsStack::new()),
        api_keys,
        identity_client,
        metrics,
        kafka_producer,
        rabbitmq_producer,
        signature_verifier,
        device_key_registry,
        meter_registry,
    };

    // 8. Build IoT Gateway HTTP routes
    let metrics_handle = Arc::new(metrics_handle);

    // Ingest routes require an API key (IAM gRPC when available, else the
    // static GRIDTOKENX_API_KEYS fallback); /health and /metrics stay open.
    if app_state.api_keys.is_empty() && app_state.identity_client.is_none() {
        warn!(
            "⚠️ No GRIDTOKENX_API_KEYS configured and IAM is unavailable — \
             every ingest request will be rejected with 401"
        );
    }
    let ingest_routes = AxumRouter::new()
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
        .route_layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            auth::api_key_auth,
        ));

    let app = AxumRouter::new()
        .route("/health", get(handlers::health))
        .route(
            "/metrics",
            get(move || {
                let handle = metrics_handle.clone();
                async move { handle.render() }
            }),
        )
        .merge(ingest_routes)
        // .layer(axum::middleware::from_fn(middleware::otel_tracing::otel_tracing_middleware))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", gateway_port))
        .await
        .context("Failed to bind IoT Gateway listener")?;

    info!("✅ Aggregator Bridge + IoT Gateway initialized");
    info!(
        "   📡 IoT Gateway accepting connections on 0.0.0.0:{}",
        gateway_port
    );
    info!(
        "   👂 Zone-based Aggregator Bridge listening on {} zone streams",
        num_zones
    );
    info!(
        "   ➡️  Forwarding readings to Kong Gateway: {}",
        api_services_url
    );

    // 9. Run HTTP and gRPC Servers concurrently (Industrial Standard)
    // Default to the canonical Aggregator Bridge gRPC port (:5030 per the README port
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
    let aggregator_grpc = Arc::new(grpc::AggregatorServiceImpl::new(app_state.clone()));
    let grpc_router = aggregator_grpc.register_service(connectrpc::Router::new());
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
                info!("🔄 Aggregator Industrial gRPC Service shutting down...");
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
