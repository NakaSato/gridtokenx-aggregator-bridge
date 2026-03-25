use std::sync::Arc;

use anyhow::Result;
use axum::{routing::{get, post}, Router as AxumRouter};
use dotenvy::dotenv;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod blockchain;
mod ingester;
mod aggregator;
mod handlers;
mod models;
mod protocol;
mod router;
mod state;

use protocol::smart_meter::SmartMeterAdapter;
use protocol::ev_charger::EvChargerAdapter;
use protocol::battery::BatteryAdapter;
use protocol::stacks::ocpp::OcppStack;
use protocol::stacks::sunspec::SunSpecStack;
use protocol::stacks::dlms::DlmsStack;
use protocol::stacks::openadr::OpenAdrStack;
use router::Router;
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
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
    let gateway_port: u16 = std::env::var("IOT_GATEWAY_PORT")
        .unwrap_or_else(|_| "4010".to_string())
        .parse()
        .unwrap_or(4010);

    info!("🔗 Redis: {}", redis_url);
    info!("🔗 Solana RPC: {}", rpc_url);
    info!("🌐 IoT Gateway port: {}", gateway_port);

    // 3. Initialize Blockchain Client (for Oracle Bridge)
    let blockchain_client = Arc::new(blockchain::BlockchainClient::new(&rpc_url)?);

    // 4. Initialize Aggregator
    let aggregator = Arc::new(tokio::sync::Mutex::new(aggregator::Aggregator::new()));

    // 5. Initialize Event Ingester (Oracle Bridge — Redis consumer)
    let ingester = ingester::EventIngester::new(&redis_url, blockchain_client, aggregator).await?;

    // 6. Initialize IoT Gateway components
    let iot_router = Arc::new(Router::new(&redis_url).await?);
    let app_state = AppState {
        router: iot_router,
        smart_meter_adapter: Arc::new(SmartMeterAdapter::new()),
        ev_charger_adapter: Arc::new(EvChargerAdapter::new()),
        battery_adapter: Arc::new(BatteryAdapter::new()),
        ocpp_stack: Arc::new(OcppStack::new()),
        sunspec_stack: Arc::new(SunSpecStack::new()),
        dlms_stack: Arc::new(DlmsStack::new()),
        openadr_stack: Arc::new(OpenAdrStack::new()),
    };

    // 7. Build IoT Gateway HTTP routes
    let app = AxumRouter::new()
        .route("/health", get(handlers::health))
        .route("/api/v1/ingest/smart-meter", post(handlers::ingest_smart_meter))
        .route("/api/v1/ingest/ev-charger", post(handlers::ingest_ev_charger))
        .route("/api/v1/ingest/battery", post(handlers::ingest_battery))
        .route("/api/v1/ingest", post(handlers::ingest_auto))
        .route("/api/v1/private-network/ingest", post(handlers::ingest_private_network))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", gateway_port)).await?;

    info!("✅ Oracle Bridge + IoT Gateway initialized");
    info!("   📡 IoT Gateway accepting connections on 0.0.0.0:{}", gateway_port);
    info!("   👂 Oracle Bridge listening on Redis stream");

    // 8. Run both services concurrently
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
