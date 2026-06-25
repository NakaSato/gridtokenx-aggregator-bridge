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

    // Make an external NTP server (Cloudflare primary, Google fallback) the primary
    // wall-clock source. Background poller; `telemetry::time::now()` is non-blocking
    // and degrades to the OS clock until the first sync. See gridtokenx_telemetry::time.
    telemetry::time::init_default();

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
        // IAM's gRPC (ConnectRPC) endpoint for API-key verification — host-mapped
        // to 5010 (see README port table), NOT this service's own GRPC_PORT. The
        // deployed compose overrides this with http://iam-service:8090.
        &std::env::var("IAM_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:5010".to_string()),
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
        redis::aio::ConnectionManager::new(early_redis_client),
    )
    .await;

    let early_redis_conn = match early_redis_conn_result {
        Ok(Ok(conn)) => Some(conn),
        _ => {
            warn!(
                "⚠️ Redis connection timed out or failed. Running in degraded mode without Redis."
            );
            None
        }
    };

    // Durable meter-owner source of truth: the shared gridtokenx Postgres, written
    // by the meter-service registration API. The MeterRegistry uses it as tier-3
    // (after local cache + Redis) and backfills Redis on a hit. Degraded-safe like
    // every other edge here: unreachable at boot ⇒ warn + None (Redis-only).
    let database_url = expand_env(&std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx".to_string()
    }));
    let pg_pool = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(std::time::Duration::from_secs(3))
            .connect(&database_url),
    )
    .await
    {
        Ok(Ok(pool)) => {
            info!("🔗 Postgres meter-owner registry: connected");
            Some(pool)
        }
        _ => {
            warn!("⚠️ Postgres unreachable; meter-owner DB tier disabled (Redis-only). Readings for unseeded meters will be unattributed.");
            None
        }
    };

    let meter_registry = Arc::new(infra::meter_registry::MeterRegistry::new(
        early_redis_conn.clone(),
        pg_pool,
    ));

    // NOTE: the on-chain settlement/mint path (former "Path B") was removed. The
    // aggregator now accumulates billing bins purely to feed the dispatch engine's
    // completed-window capacity query. With settlement gone, nothing evicts bins —
    // `active_bins` grows unbounded over time (known leak; acceptable for the
    // operational/dispatch role, revisit with a time-based reaper if needed).

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
    // Independent InfluxDB telemetry-history sink (own INFLUXDB_* connection;
    // shared with no other service). Degrades to None when unset/unreachable.
    let influx_writer = match infra::influxdb::InfluxWriter::connect().await {
        Ok(w) => w.map(Arc::new),
        Err(e) => {
            warn!(
                "⚠️ InfluxDB init failed: {}. Realtime history persistence disabled.",
                e
            );
            None
        }
    };

    // Keep a handle for the billing sink before the writer moves into the router.
    // Both share the same async fire-and-forget queue.
    let billing_influx = influx_writer.clone();

    let iot_router = Arc::new(
        router::Router::new(&redis_url, num_zones, influx_writer)
            .await
            .context("Failed to initialize IoT router")?,
    );

    // Chain Bridge mint gateway for surplus settlement. Degrades to Disabled when
    // MINT_VIA_CHAIN_BRIDGE is off or NATS is unreachable, so the settlement loop
    // still runs (and still evicts bins) regardless of mint availability.
    let nats_url = std::env::var("NATS_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let mint_via_chain_bridge =
        std::env::var("MINT_VIA_CHAIN_BRIDGE").is_ok_and(|v| v.eq_ignore_ascii_case("true"));
    let mint_service_identity = std::env::var("CHAIN_BRIDGE_SERVICE_IDENTITY")
        .unwrap_or_else(|_| "spiffe://gridtokenx.th/prod/aggregator-bridge".to_string());
    let mint_gateway = Arc::new(
        infra::mint::MintGateway::connect(
            mint_via_chain_bridge,
            nats_url.as_deref(),
            mint_service_identity,
        )
        .await,
    );
    if mint_gateway.is_enabled() {
        info!("⚡ Surplus mint via Chain Bridge ENABLED (NATS request-reply → chain.tx.mint)");
    } else {
        info!("⚡ Surplus mint DISABLED (needs MINT_VIA_CHAIN_BRIDGE=true + NATS_URL)");
    }

    // Settlement sink: periodically drains completed 15-minute billing bins. For
    // each bin it (1) writes the TOU/demand `billing` point to InfluxDB (if
    // enabled) and (2) mints the net surplus to the meter owner via Chain Bridge
    // (if enabled), then evicts the bin — the eviction bounds the otherwise-
    // unbounded `active_bins` map. Runs whenever InfluxDB OR minting is enabled.
    if billing_influx.is_some() || mint_gateway.is_enabled() {
        let billing_agg = aggregator.clone();
        let billing_shutdown = shutdown_token.clone();
        let settle_mint = mint_gateway.clone();
        let settle_registry = meter_registry.clone();
        let billing_influx = billing_influx;
        let interval_secs = std::env::var("BILLING_FLUSH_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let grace_secs = std::env::var("BILLING_FLUSH_GRACE_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(120);
        info!(
            "🧾 Settlement sink ENABLED (interval={}s, grace={}s; influxdb={}, mint={})",
            interval_secs,
            grace_secs,
            billing_influx.is_some(),
            settle_mint.is_enabled()
        );
        tokio::spawn(async move {
            let grace = chrono::Duration::seconds(grace_secs);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = billing_shutdown.cancelled() => {
                        info!("🛑 Settlement sink stopped");
                        break;
                    }
                    _ = ticker.tick() => {
                        // Drain and evict under the lock, then process the owned
                        // snapshot without holding the mutex (mint may be slow).
                        let bins = {
                            let mut agg = billing_agg.lock().await;
                            let bins = agg.peek_completed_bins(grace);
                            if bins.is_empty() {
                                continue;
                            }
                            let keys: Vec<_> = bins.iter().map(|b| b.key()).collect();
                            agg.remove_bins(&keys);
                            bins
                        };

                        let mut written = 0usize;
                        for bin in &bins {
                            // (1) InfluxDB billing point.
                            if let Some(influx) = billing_influx.as_ref() {
                                if let Some(point) =
                                    aggregator_api::billing_sink::bin_to_billing_point(bin)
                                {
                                    influx.record(point);
                                    written += 1;
                                }
                            }

                            // (2) Surplus mint — fire-and-forget so a slow bridge
                            // never stalls the sweep. The bridge idempotency key
                            // mint:{serial}:{window_start_ms} + on-chain PDA dedup
                            // any replay (e.g. a crash before eviction).
                            if settle_mint.is_enabled() {
                                if let Some(kwh) = bin.net_surplus_kwh() {
                                    let gw = settle_mint.clone();
                                    let reg = settle_registry.clone();
                                    let serial = bin.meter_serial.clone();
                                    let meter_id = *bin.meter_id.as_bytes();
                                    let window_start_ms = bin.window_start_ms();
                                    tokio::spawn(async move {
                                        let wallet = match reg.resolve_wallet(&serial).await {
                                            Ok(Some(w)) => w,
                                            Ok(None) => {
                                                warn!("surplus mint skipped: no wallet registered for meter {serial}");
                                                return;
                                            }
                                            Err(e) => {
                                                warn!("surplus mint skipped: wallet lookup failed for {serial}: {e}");
                                                return;
                                            }
                                        };
                                        match gw
                                            .mint(&wallet, kwh, meter_id, &serial, window_start_ms)
                                            .await
                                        {
                                            Ok(out) => info!(
                                                "⚡ minted {kwh} kWh surplus for meter {serial} (sig={}, slot={})",
                                                out.signature, out.slot
                                            ),
                                            Err(e) => {
                                                warn!("surplus mint failed for meter {serial}: {e}")
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        if written > 0 {
                            info!(
                                "🧾 Settlement sink: flushed {} completed bin(s) to InfluxDB",
                                written
                            );
                        }
                    }
                }
            }
        });
    }
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
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(publish_secs));
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
    // aggregator here would never see any readings, so dispatch would scan an
    // always-empty map and report zero available capacity.
    let dispatch_grpc_url =
        std::env::var("DISPATCH_GRPC_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let grpc_client = match dispatch::grpc_client::DispatchClient::new(dispatch_grpc_url.clone())
        .await
    {
        Ok(client) => client,
        Err(e) => {
            warn!(
                "⚠️ DISPATCH_GRPC_URL '{}' invalid ({}); falling back to http://127.0.0.1:50051",
                dispatch_grpc_url, e
            );
            dispatch::grpc_client::DispatchClient::new("http://127.0.0.1:50051".to_string()).await?
        }
    };
    let mut dispatch_engine =
        dispatch::engine::DispatchEngine::new(aggregator.clone(), grpc_client);

    // Kafka Consumer for Dispatch
    let kafka_consumer = if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
        let topic = std::env::var("KAFKA_TOPIC_GRID_STATUS")
            .unwrap_or_else(|_| "gridtokenx.aggregator.grid_status".to_string());
        match infra::kafka::AggregatorKafkaConsumer::new(
            &brokers,
            "aggregator-bridge-group",
            &topic,
        ) {
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
                        if let Err(e) = dispatch_engine.evaluate_and_dispatch(event.frequency).await
                        {
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

    // 7. Initialize IAM gRPC Client (optional - auth falls back to static API keys).
    // Used by the ingest auth middleware to resolve API keys via IAM.
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
        // IAM derives the caller's ServiceRole from the `x-gridtokenx-role` header
        // (see ServiceRole::from_headers in gridtokenx-blockchain-core); without it
        // role-gated calls are denied as Unknown.
        let config = ClientConfig::new(uri)
            .protocol(Protocol::Grpc)
            .default_header("x-gridtokenx-role", "aggregator-bridge");
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
        .route("/v1/ingest/telemetry", post(handlers::ingest_legacy_batch))
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

    // Shared graceful-shutdown trigger: SIGINT/SIGTERM cancels the shutdown token,
    // which both the HTTP/HTTPS server path and all background tasks observe.
    let server_shutdown = shutdown_token.clone();
    let shutdown_signal = async move {
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
    };

    // Optional TLS termination for the IoT gateway: when both IOT_GATEWAY_TLS_CERT
    // and IOT_GATEWAY_TLS_KEY are set, serve HTTPS so DLMS/COSEM telemetry is
    // encrypted in transit. Otherwise serve plain HTTP (backward-compatible default).
    let tls_cert = std::env::var("IOT_GATEWAY_TLS_CERT")
        .ok()
        .filter(|s| !s.is_empty());
    let tls_key = std::env::var("IOT_GATEWAY_TLS_KEY")
        .ok()
        .filter(|s| !s.is_empty());

    let server_result: anyhow::Result<()> = if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        info!("🔐 Starting HTTPS gateway (TLS) using cert {}...", cert);
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
            .await
            .context("Failed to load IoT Gateway TLS cert/key")?;

        // axum_server binds its own socket; release the plain listener bound above
        // so the port is free for the TLS acceptor.
        let addr = listener
            .local_addr()
            .context("Failed to read IoT Gateway listener address")?;
        drop(listener);

        let handle = axum_server::Handle::new();
        let watcher = handle.clone();
        tokio::spawn(async move {
            shutdown_signal.await;
            watcher.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
        });

        axum_server::bind_rustls(addr, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context("IoT Gateway HTTPS server failed")
    } else {
        info!("🚀 Starting HTTP gateway (plaintext; set IOT_GATEWAY_TLS_CERT/KEY for TLS)...");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .await
            .context("IoT Gateway HTTP server failed")
    };

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
