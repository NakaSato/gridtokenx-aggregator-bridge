use std::sync::Arc;

use anyhow::Result;
use axum::{routing::{get, post}, middleware, Router as AxumRouter};
use dotenvy::dotenv;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod ingester;
mod aggregator;
mod handlers;
mod models;
mod protocol;
mod router;
mod state;
mod auth;
mod metrics;

mod buffa_utils;

use protocol::smart_meter::SmartMeterAdapter;
use protocol::ev_charger::EvChargerAdapter;
use protocol::battery::BatteryAdapter;
use protocol::stacks::ocpp::OcppStack;
use protocol::stacks::sunspec::SunSpecStack;
use protocol::stacks::dlms::DlmsStack;
use protocol::stacks::openadr::OpenAdrStack;
use state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("🚀 Starting GridTokenX Oracle Bridge + IoT Gateway");

    // 2. Configuration
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let api_gateway_url = std::env::var("API_GATEWAY_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4000".to_string());
    let gateway_port: u16 = std::env::var("IOT_GATEWAY_PORT")
        .unwrap_or_else(|_| "4010".to_string())
        .parse()
        .unwrap_or(4010);
    let iam_service_url = std::env::var("IAM_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    info!("🔗 Redis: {}", redis_url);
    info!("🔗 API Gateway: {}", api_gateway_url);
    info!("🔗 IAM Service: {}", iam_service_url);
    info!("🌐 IoT Gateway port: {}", gateway_port);

    // 3. Initialize Shared Metrics
    let metrics = Arc::new(state::Metrics::new());

    // 4. Initialize Aggregator (local stats only, no blockchain submission)
    let aggregator = Arc::new(tokio::sync::Mutex::new(aggregator::Aggregator::new()));

    // 5. Initialize Event Ingester (Oracle Bridge — Redis consumer, forwards to API Gateway)
    let ingester = Arc::new(ingester::EventIngester::new(
        &redis_url,
        &api_gateway_url,
        aggregator,
        metrics.clone()
    ).await?);

    // 6. Initialize IoT Gateway components
    let iot_router = Arc::new(router::Router::new(&redis_url).await?);
    let api_keys_raw = std::env::var("GRIDTOKENX_API_KEYS").unwrap_or_default();
    let api_keys: Vec<String> = api_keys_raw.split(',').map(|s| s.to_string()).filter(|s| !s.is_empty()).collect();

    // 7. Initialize IAM gRPC Client (optional - auth falls back to static API keys)
    use state::identity::IdentityServiceClient;
    use connectrpc::client::{Http2Connection, ClientConfig};
    use connectrpc::Protocol;

    let identity_client = async {
        let uri: http::Uri = iam_service_url.parse().ok()?;
        let conn = Http2Connection::connect_plaintext(uri.clone())
            .await
            .map_err(|e| warn!("⚠️  IAM gRPC connection failed: {}", e))
            .ok()?
            .shared(1024);
        let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
        let client = IdentityServiceClient::new(conn, config);
        info!("✅ IAM gRPC client connected to {}", iam_service_url);
        Some(Arc::new(client))
    }.await;

    // 7b. Initialize Prometheus metrics exporter
    let metrics_recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_quantiles(&[0.0, 0.5, 0.9, 0.95, 0.99, 1.0])
        .expect("Failed to set quantiles")
        .build_recorder();
    let metrics_handle = metrics_recorder.handle();
    ::metrics::set_global_recorder(metrics_recorder).expect("Failed to set metrics recorder");
    info!("✅ Prometheus metrics exporter initialized");

    let app_state = AppState {
        router: iot_router,
        smart_meter_adapter: Arc::new(SmartMeterAdapter::new()),
        ev_charger_adapter: Arc::new(EvChargerAdapter::new()),
        battery_adapter: Arc::new(BatteryAdapter::new()),
        ocpp_stack: Arc::new(OcppStack::new()),
        sunspec_stack: Arc::new(SunSpecStack::new()),
        dlms_stack: Arc::new(DlmsStack::new()),
        openadr_stack: Arc::new(OpenAdrStack::new()),
        api_keys,
        identity_client,
        metrics,
    };

    // Store metrics handle in state for the metrics endpoint
    let metrics_handle = Arc::new(metrics_handle);
    let app_state_with_metrics = app_state.clone();

    // 8. Build IoT Gateway HTTP routes
    let api_routes = AxumRouter::new()
        .route("/ingest/smart-meter", post(handlers::ingest_smart_meter))
        .route("/ingest/batch/smart-meter", post(handlers::ingest_batch_smart_meter))
        .route("/ingest/ev-charger", post(handlers::ingest_ev_charger))
        .route("/ingest/battery", post(handlers::ingest_battery))
        .route("/ingest", post(handlers::ingest_auto))
        .route("/private-network/ingest", post(handlers::ingest_private_network))
        .layer(middleware::from_fn_with_state(app_state.clone(), auth::api_key_auth));

    // Metrics endpoint that returns Prometheus format
    let app = AxumRouter::new()
        .route("/health", get(handlers::health))
        .route("/metrics", {
            let metrics_handle = metrics_handle.clone();
            get(move || {
                let metrics_handle = metrics_handle.clone();
                async move { metrics_handle.render() }
            })
        })
        .nest("/api/v1", api_routes)
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", gateway_port)).await?;

    info!("✅ Oracle Bridge + IoT Gateway initialized");
    info!("   📡 IoT Gateway accepting connections on 0.0.0.0:{}", gateway_port);
    info!("   👂 Oracle Bridge listening on Redis streams");
    info!("   ➡️  Forwarding readings to API Gateway: {}", api_gateway_url);

    // 9. Run both services concurrently
    tokio::select! {
        result = ingester.run() => {
            if let Err(e) = result {
                error!("❌ Oracle Bridge ingester failed: {}", e);
            }
        }
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                error!("❌ IoT Gateway HTTP server failed: {}", e);
            }
        }
    }

    Ok(())
}
