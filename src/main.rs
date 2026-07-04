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
    aggregator, auth, dispatch, grid_status, grpc, handlers, infra, ingester, metrics,
    mint_settlement, protocol, router, standards, state, telemetry,
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

/// Parse the comma-separated `GRIDTOKENX_API_KEYS` value into the static
/// fallback key list. Empty entries are dropped — critical, because the seeded
/// IAM key is a placeholder (empty-string hash) and an empty static key would
/// otherwise authorize a request that sends a blank `X-API-KEY`. Splitting an
/// empty string yields one empty element, so an unset/blank var ⇒ no keys.
fn parse_api_keys(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Build a rustls `ServerConfig` that requires and verifies client certificates
/// (mTLS) against `client_ca_path`, presenting the server identity from
/// `cert_path`/`key_path`. Used when `IOT_GATEWAY_TLS_CLIENT_CA` is set so the
/// IoT gateway authenticates the meter simulator at the transport layer, not
/// just by API key. Uses the process-default crypto provider (installed in main).
fn build_mtls_server_config(
    cert_path: &str,
    key_path: &str,
    client_ca_path: &str,
) -> Result<rustls::ServerConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::io::BufReader;

    let mut cert_rd = BufReader::new(
        std::fs::File::open(cert_path)
            .with_context(|| format!("open server cert {}", cert_path))?,
    );
    let cert_chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_rd)
        .collect::<std::result::Result<_, _>>()
        .context("parse server cert chain")?;

    let mut key_rd = BufReader::new(
        std::fs::File::open(key_path).with_context(|| format!("open server key {}", key_path))?,
    );
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_rd)
        .context("parse server key")?
        .ok_or_else(|| anyhow::anyhow!("no private key in {}", key_path))?;

    let mut ca_rd = BufReader::new(
        std::fs::File::open(client_ca_path)
            .with_context(|| format!("open client CA {}", client_ca_path))?,
    );
    let mut roots = rustls::RootCertStore::empty();
    for ca in rustls_pemfile::certs(&mut ca_rd) {
        roots
            .add(ca.context("parse client CA cert")?)
            .context("add client CA root")?;
    }

    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("build client cert verifier")?;

    rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, key)
        .context("build mTLS server config")
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

    // 4c. Durable billing-bin store (crash recovery of in-flight 15-min windows).
    // Enabled when Redis is reachable AND DURABLE_BINS != "false" (default on).
    // Degrade-safe: no Redis ⇒ None ⇒ the aggregator runs memory-only exactly as
    // before. The ingest edge write-throughs each updated bin; this loop's
    // eviction deletes the durable entry; startup restores the rest.
    let durable_bins_enabled =
        std::env::var("DURABLE_BINS").map_or(true, |v| !v.eq_ignore_ascii_case("false"));
    let bin_store = match (durable_bins_enabled, early_redis_conn.clone()) {
        (true, Some(conn)) => Some(Arc::new(aggregator_api::bin_store::BinStore::new(conn))),
        (true, None) => {
            warn!("⚠️ DURABLE_BINS requested but Redis unavailable — billing bins are memory-only (a crash mid-window loses the partial bin).");
            None
        }
        (false, _) => {
            info!("ℹ️ Durable billing bins DISABLED (DURABLE_BINS=false) — memory-only.");
            None
        }
    };

    // Restore any in-flight bins persisted before a restart, so the settlement
    // loop sees windows that were accumulating when the process last stopped.
    if let Some(store) = &bin_store {
        match store.load_all().await {
            Ok(bins) if !bins.is_empty() => {
                let n = bins.len();
                aggregator.lock().await.restore_bins(bins);
                info!("♻️ Restored {n} in-flight billing bin(s) from durable store");
            }
            Ok(_) => info!("✅ Durable billing bins ENABLED (no in-flight bins to restore)"),
            Err(e) => warn!("⚠️ Could not restore durable billing bins ({e}); starting empty (memory-only until next write)."),
        }
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

    // At-rest stream encryption: bootstrap the fleet-wide SEK (Vault-KEK-wrapped,
    // Redis-persisted, stable across restarts) when AGGREGATOR_ENCRYPT_STREAMS is
    // set. Shared by the disseminator (encrypt) and the zone ingester (decrypt).
    // Fail-closed: if encryption is requested but Vault/Redis is unavailable, bail
    // rather than silently fall back to writing plaintext.
    let stream_cipher: Option<Arc<infra::stream_cipher::StreamCipher>> =
        if std::env::var("AGGREGATOR_ENCRYPT_STREAMS").unwrap_or_default() == "true" {
            match (
                infra::vault::VaultTransitClient::from_env(),
                early_redis_conn.clone(),
            ) {
                (Some(vault), Some(mut conn)) => {
                    // The SEK unwrap below hits Vault Transit immediately, so the
                    // KEK must exist first. The dev Vault is in-memory: after a
                    // restart the transit engine itself is gone and the unwrap
                    // fails with a route-not-found 404 — self-provision here
                    // (idempotent) instead of relying on the meter-key path's
                    // later ensure_kek, which boots after this point.
                    vault
                        .ensure_kek()
                        .await
                        .context("Failed to ensure Vault Transit KEK before SEK bootstrap")?;
                    let cipher =
                        infra::stream_cipher::load_or_create_stream_cipher(&vault, &mut conn)
                            .await
                            .context("Failed to bootstrap stream encryption key (SEK)")?;
                    info!("🔐 At-rest stream encryption ENABLED (SEK bootstrapped)");
                    Some(Arc::new(cipher))
                }
                _ => anyhow::bail!(
                    "AGGREGATOR_ENCRYPT_STREAMS=true but Vault (VAULT_ADDR) or Redis is unavailable"
                ),
            }
        } else {
            None
        };

    let zone_ingester = match ingester::zone_ingester::ZoneEventIngester::new(
        &redis_url,
        &api_services_grpc_url,
        aggregator.clone(),
        metrics.clone(),
        num_zones,
        meter_registry.clone(),
        frequency_monitor.clone(),
        stream_cipher.clone(),
        bin_store.clone(),
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
            .context("Failed to initialize IoT router")?
            .with_stream_cipher(stream_cipher.clone()),
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
    // NOTE: the settlement-path gauge is emitted in step 7b, AFTER the global metrics
    // recorder is installed — emitting here (before set_global_recorder) is a no-op.

    // Durable mint outbox: guarantees a settled surplus survives a mint that
    // can't land on-chain yet (bridge/validator down, sim rejection, lost
    // reply). The settlement loop enqueues here instead of fire-and-forget
    // minting; a drain loop retries every sweep until the mint is confirmed, and
    // the entry survives a restart. Enabled only when minting is on AND Redis is
    // reachable; otherwise None ⇒ the loop falls back to the prior best-effort
    // immediate mint (no durability, no regression). Retries are safe: the
    // bridge dedups on mint:{serial}:{window_start_ms} + the on-chain PDA.
    let mint_outbox = match (mint_gateway.is_enabled(), early_redis_conn.clone()) {
        (true, Some(conn)) => {
            info!("📤 Durable mint outbox ENABLED (unsettled surplus retried until on-chain)");
            Some(Arc::new(aggregator_api::mint_outbox::MintOutbox::new(conn)))
        }
        (true, None) => {
            warn!("⚠️ Mint enabled but Redis unavailable — mint outbox OFF; a mint that can't land on-chain is best-effort only (surplus may be lost on failure).");
            None
        }
        (false, _) => None,
    };

    // Bound on concurrent in-flight mint request-replies toward Chain Bridge,
    // shared by the settlement sink's immediate attempts and the outbox drain
    // loop. An unbounded fan-out (e.g. 10k bins completing in the same window)
    // floods the bridge's NATS consumer: every envelope is timestamped at
    // publish, the queue drains at the bridge's rate, and anything waiting past
    // the bridge's staleness cutoff (55s) is rejected — then times out client-
    // side and re-fires on the next drain tick, a livelock that throttled the
    // measured mint rate to ~40/s while the bridge could absorb ~160/s. The cap
    // keeps bridge queue wait well under the cutoff; size it near the bridge's
    // CHAIN_BRIDGE_MINT_CONCURRENCY.
    let mint_inflight = Arc::new(tokio::sync::Semaphore::new(
        std::env::var("MINT_INFLIGHT_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(128),
    ));
    // Companion to the semaphore: the semaphore bounds *how many* attempts run
    // at once; this keyset stops the sweep and the drain tick from publishing
    // the *same* entry twice while its reply is pending (see MintInFlight).
    let mint_inflight_keys = Arc::new(mint_settlement::MintInFlight::default());

    // Settlement sink: periodically drains completed 15-minute billing bins. For
    // each bin it (1) writes the TOU/demand `billing` point to InfluxDB (if
    // enabled) and (2) mints the net surplus to the meter owner via Chain Bridge
    // (if enabled), then evicts the bin — the eviction bounds the otherwise-
    // unbounded `active_bins` map. Runs whenever InfluxDB OR minting is enabled.
    if billing_influx.is_some() || mint_gateway.is_enabled() {
        let sink_inflight = mint_inflight.clone();
        let sink_inflight_keys = mint_inflight_keys.clone();
        let billing_agg = aggregator.clone();
        let billing_shutdown = shutdown_token.clone();
        let settle_mint = mint_gateway.clone();
        let settle_registry = meter_registry.clone();
        let billing_influx = billing_influx;
        let settle_bin_store = bin_store.clone();
        let settle_outbox = mint_outbox.clone();
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
                        // Drain and evict from the in-memory aggregator under the
                        // lock, then process the owned snapshot without holding the
                        // mutex (mint may be slow). The DURABLE (Redis) bin-store
                        // entry for each bin is intentionally NOT removed here — only
                        // after that bin's surplus has been durably handed off below,
                        // per-bin. Deleting it earlier (e.g. once for the whole batch,
                        // up front) would reopen the crash-loss window this store
                        // exists to close: a crash between an up-front bulk delete and
                        // a later enqueue would drop the surplus from every durable
                        // copy at once.
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

                            // (2) Surplus mint. When the durable outbox is on, the
                            // surplus is ENQUEUED (persisted) first, then an immediate
                            // best-effort mint is attempted so a healthy bridge still
                            // settles within ~1s — the outbox + drain loop remain the
                            // durable backstop if that immediate attempt fails (retried
                            // until confirmed, never lost; idempotent on
                            // mint:{serial}:{window_start_ms} + the on-chain PDA, so a
                            // retry of a mint that already landed never double-mints).
                            // Without the outbox (no Redis) it falls back to a
                            // best-effort fire-and-forget mint (prior behavior).
                            //
                            // `handoff` records how (or whether) the surplus was
                            // durably handed off, and gates the durable bin-store
                            // removal below via the pure `bin_durably_settled` — see
                            // its doc for why `Lost` is the only outcome that must
                            // retain the bin's durable entry instead of evicting it.
                            use aggregator_api::billing_sink::MintHandoff;
                            let mut handoff = MintHandoff::NotApplicable;
                            match aggregator_api::billing_sink::plan_mint(bin, settle_mint.is_enabled()) {
                                aggregator_api::billing_sink::MintDecision::Surplus(kwh) => {
                                    let pending = aggregator_api::mint_outbox::PendingMint::new(
                                        bin.meter_serial.clone(),
                                        bin.meter_id,
                                        bin.window_start_ms(),
                                        kwh,
                                    );
                                    match &settle_outbox {
                                        Some(outbox) => match outbox.enqueue(&pending).await {
                                            Ok(()) => {
                                                metrics::record_mint_outcome("queued", "ok");
                                                handoff = MintHandoff::Enqueued;
                                                let gw = settle_mint.clone();
                                                let reg = settle_registry.clone();
                                                let outbox = outbox.clone();
                                                let field = pending.field();
                                                let inflight = sink_inflight.clone();
                                                let inflight_keys = sink_inflight_keys.clone();
                                                tokio::spawn(async move {
                                                    // Gate on the shared in-flight cap so a huge
                                                    // window doesn't flood the bridge into
                                                    // staleness rejections. Closed semaphore ⇒
                                                    // skip; the outbox drain retries durably.
                                                    let Ok(_permit) = inflight.acquire_owned().await else {
                                                        return;
                                                    };
                                                    if mint_settlement::attempt_mint(&gw, &reg, &inflight_keys, &pending).await {
                                                        if let Err(e) = outbox.remove(&field).await {
                                                            warn!("mint outbox remove failed for {field} ({e}); a stale entry only causes one idempotent retry");
                                                        }
                                                    }
                                                });
                                            }
                                            Err(e) => {
                                                // Outbox write failed — retry it once before
                                                // falling back, instead of silently relying on
                                                // a single best-effort mint attempt.
                                                warn!("mint outbox enqueue failed for {}: {e}; retrying enqueue once", pending.meter_serial);
                                                match outbox.enqueue(&pending).await {
                                                    Ok(()) => {
                                                        metrics::record_mint_outcome("queued", "ok");
                                                        handoff = MintHandoff::Enqueued;
                                                    }
                                                    Err(e2) => {
                                                        error!(
                                                            "mint outbox enqueue failed twice for meter {} window {}: {e2}; attempting immediate mint as last resort",
                                                            pending.meter_serial, pending.window_start_ms
                                                        );
                                                        if mint_settlement::attempt_mint(&settle_mint, &settle_registry, &sink_inflight_keys, &pending).await {
                                                            handoff = MintHandoff::MintedDirectly;
                                                        } else {
                                                            error!(
                                                                "surplus mint LOST for meter {} window {}: outbox enqueue failed twice and immediate mint failed; durable bin entry retained for manual recovery",
                                                                pending.meter_serial, pending.window_start_ms
                                                            );
                                                            // Distinct from attempt_mint's own
                                                            // "failed"/"mint_err" (one failed
                                                            // call) — this counts the end-to-end
                                                            // outcome (no durable home anywhere),
                                                            // so an alert can page on it directly
                                                            // instead of relying on a log grep.
                                                            metrics::record_mint_outcome("lost", "outbox_and_mint_failed");
                                                            handoff = MintHandoff::Lost;
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        None => {
                                            // No durable outbox (Redis off): best-effort mint.
                                            // No outbox here ⇒ no durable bin store either
                                            // (both gate on the same Redis connection), so
                                            // there is nothing for `handoff` to protect.
                                            handoff = MintHandoff::BestEffortOnly;
                                            let gw = settle_mint.clone();
                                            let reg = settle_registry.clone();
                                            let inflight = sink_inflight.clone();
                                            let inflight_keys = sink_inflight_keys.clone();
                                            tokio::spawn(async move {
                                                let Ok(_permit) = inflight.acquire_owned().await else {
                                                    return;
                                                };
                                                let _ = mint_settlement::attempt_mint(&gw, &reg, &inflight_keys, &pending).await;
                                            });
                                        }
                                    }
                                }
                                aggregator_api::billing_sink::MintDecision::NoSurplus => {
                                    // Net consumption — nothing to mint. Counted as the
                                    // denominator so "skipped" rates are interpretable.
                                    metrics::record_mint_outcome("no_surplus", "ok");
                                }
                                aggregator_api::billing_sink::MintDecision::Disabled => {}
                            }

                            if aggregator_api::billing_sink::bin_durably_settled(handoff) {
                                if let Some(store) = &settle_bin_store {
                                    if let Err(e) = store.remove(&[bin.key()]).await {
                                        warn!("durable bin eviction failed for {} (stale entry remains, deduped by idempotency key): {e}", bin.meter_serial);
                                    }
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

    // Mint outbox drain loop: retries every persisted unsettled surplus until it
    // is confirmed on-chain, then removes it. This is what makes "the mint can't
    // land yet" non-lossy — a bridge/validator outage or a sim rejection just
    // leaves the entry queued for the next tick (and it survives a restart, since
    // load_all reads Redis). Retries are idempotent (bridge dedup + on-chain PDA).
    if let Some(outbox) = mint_outbox.clone() {
        let drain_gw = mint_gateway.clone();
        let drain_reg = meter_registry.clone();
        let drain_shutdown = shutdown_token.clone();
        let drain_inflight = mint_inflight.clone();
        let drain_inflight_keys = mint_inflight_keys.clone();
        let retry_secs = std::env::var("MINT_RETRY_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        info!("📤 Mint outbox drain loop ENABLED (retry every {retry_secs}s until on-chain)");
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(retry_secs));
            loop {
                tokio::select! {
                    _ = drain_shutdown.cancelled() => {
                        info!("🛑 Mint outbox drain loop stopped");
                        break;
                    }
                    _ = ticker.tick() => {
                        let pending = match outbox.load_all().await {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("mint outbox load failed ({e}); will retry next tick");
                                continue;
                            }
                        };
                        if pending.is_empty() {
                            continue;
                        }
                        info!("📤 Mint outbox: draining {} pending surplus mint(s)", pending.len());
                        // Attempt pending entries concurrently — sequential awaits
                        // would let one slow/unreachable Chain Bridge call serialize
                        // and starve the rest of the backlog for the whole tick —
                        // but bounded by the shared in-flight cap: each mint
                        // envelope is timestamped when it is published, so firing a
                        // 10k backlog at once parks most of it in the bridge's NATS
                        // queue past the 55s staleness cutoff (mass rejection +
                        // client reply-timeouts + re-fire next tick). The permit
                        // makes publish time ≈ processing time.
                        let mut attempts = tokio::task::JoinSet::new();
                        for p in pending {
                            let gw = drain_gw.clone();
                            let reg = drain_reg.clone();
                            let outbox = outbox.clone();
                            let inflight = drain_inflight.clone();
                            let inflight_keys = drain_inflight_keys.clone();
                            attempts.spawn(async move {
                                let Ok(_permit) = inflight.acquire_owned().await else {
                                    return;
                                };
                                // Confirmed on-chain ⇒ remove; otherwise keep for the
                                // next tick (no_wallet / bridge down / sim rejection).
                                if mint_settlement::attempt_mint(&gw, &reg, &inflight_keys, &p).await {
                                    let field = p.field();
                                    if let Err(e) = outbox.remove(&field).await {
                                        warn!("mint outbox remove failed for {field} ({e}); a stale entry only causes one idempotent retry");
                                    }
                                }
                            });
                        }
                        while let Some(res) = attempts.join_next().await {
                            if let Err(e) = res {
                                warn!("mint outbox drain task panicked: {e}");
                            }
                        }
                    }
                }
            }
        });
    }

    let api_keys_raw = std::env::var("GRIDTOKENX_API_KEYS").unwrap_or_default();
    let api_keys = parse_api_keys(&api_keys_raw);

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
    // Attach a Vault Transit client (from VAULT_ADDR/VAULT_TOKEN/VAULT_METER_KEK_NAME)
    // so rotated, KEK-wrapped GUEK versions can be unwrapped. None when Vault is
    // unset — the registry then serves only the legacy unversioned key.
    let meter_vault = infra::vault::VaultTransitClient::from_env();
    if let Some(v) = meter_vault.as_ref() {
        info!("🔐 Vault Transit enabled for per-meter key rotation (versioned GUEK unwrap)");
        // Self-heal the KEK on boot (dev Vault is in-memory). Best-effort: a
        // failure here just means rotation can't unwrap until the key exists, so
        // log and continue rather than blocking startup.
        if let Err(e) = v.ensure_kek().await {
            warn!(
                "⚠️ Could not ensure Vault meter KEK (rotation unwrap may fail): {}",
                e
            );
        }
    }
    let device_key_registry = Arc::new(
        infra::crypto::DeviceKeyRegistry::new(Some(redis_url.clone())).with_vault(meter_vault),
    );

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

    // Publish the active settlement path now that the recorder is installed. Emitted
    // once at startup (a no-op if done earlier, before set_global_recorder above), so a
    // silent degrade to mint-disabled is observable on /metrics.
    metrics::record_settlement_path(if mint_gateway.is_enabled() {
        "nats"
    } else {
        "disabled"
    });

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

    // Optional mTLS: when IOT_GATEWAY_TLS_CLIENT_CA is set, require + verify a
    // client cert (the simulator's) against that CA — transport-layer auth on top
    // of the API key. Unset => server-auth TLS only (backward-compatible).
    let client_ca = std::env::var("IOT_GATEWAY_TLS_CLIENT_CA")
        .ok()
        .filter(|s| !s.is_empty());

    let server_result: anyhow::Result<()> = if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        let tls_config = match client_ca.as_ref() {
            Some(ca) => {
                info!(
                    "🔐 Starting HTTPS gateway (mTLS) cert {} — verifying client certs against {}",
                    cert, ca
                );
                let server_config = build_mtls_server_config(&cert, &key, ca)
                    .context("Failed to build IoT Gateway mTLS config")?;
                axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(server_config))
            }
            None => {
                info!("🔐 Starting HTTPS gateway (TLS) using cert {}...", cert);
                axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
                    .await
                    .context("Failed to load IoT Gateway TLS cert/key")?
            }
        };

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

#[cfg(test)]
mod tests {
    use super::{build_mtls_server_config, expand_env, parse_api_keys};

    // expand_env reads process env, so these tests use uniquely-named vars to
    // avoid colliding with anything real or each other under parallel runs.

    #[test]
    fn expands_a_set_variable() {
        std::env::set_var("AGG_TEST_EXPAND_HOST", "redis-host");
        assert_eq!(
            expand_env("tcp://${AGG_TEST_EXPAND_HOST}:6379"),
            "tcp://redis-host:6379"
        );
        std::env::remove_var("AGG_TEST_EXPAND_HOST");
    }

    #[test]
    fn unset_variable_expands_to_empty() {
        std::env::remove_var("AGG_TEST_EXPAND_UNSET");
        assert_eq!(expand_env("a${AGG_TEST_EXPAND_UNSET}b"), "ab");
    }

    #[test]
    fn expands_multiple_variables() {
        std::env::set_var("AGG_TEST_EXPAND_A", "1");
        std::env::set_var("AGG_TEST_EXPAND_B", "2");
        assert_eq!(
            expand_env("${AGG_TEST_EXPAND_A}-${AGG_TEST_EXPAND_B}"),
            "1-2"
        );
        std::env::remove_var("AGG_TEST_EXPAND_A");
        std::env::remove_var("AGG_TEST_EXPAND_B");
    }

    #[test]
    fn no_placeholder_is_returned_verbatim() {
        assert_eq!(expand_env("plain-string:6379"), "plain-string:6379");
    }

    #[test]
    fn unterminated_placeholder_is_left_untouched() {
        // No closing brace ⇒ the loop breaks, returning the input as-is (no hang).
        assert_eq!(expand_env("prefix${UNCLOSED"), "prefix${UNCLOSED");
    }

    // --- parse_api_keys: static-fallback key list (G5 config) ---

    #[test]
    fn parse_api_keys_unset_or_blank_yields_no_keys() {
        // Splitting "" gives one empty element, which is filtered → no keys. This
        // is why a blank GRIDTOKENX_API_KEYS can't authorize a blank X-API-KEY.
        assert!(parse_api_keys("").is_empty());
        assert!(parse_api_keys(",").is_empty());
        assert!(parse_api_keys(",,").is_empty());
    }

    #[test]
    fn parse_api_keys_splits_and_drops_empty_entries() {
        assert_eq!(parse_api_keys("a,b,c"), vec!["a", "b", "c"]);
        // Empty entries between/around commas are dropped.
        assert_eq!(parse_api_keys("a,,b"), vec!["a", "b"]);
        assert_eq!(parse_api_keys("a,"), vec!["a"]);
        assert_eq!(parse_api_keys(",a"), vec!["a"]);
    }

    #[test]
    fn parse_api_keys_does_not_trim_whitespace() {
        // No trimming: keys are matched verbatim, so a stray space is its own key
        // (documents the behavior — keep commas tight in config).
        assert_eq!(parse_api_keys(" a , b "), vec![" a ", " b "]);
    }

    // --- build_mtls_server_config: fail-loud on bad cert material (G5 startup) ---
    // The happy path needs a valid CA-signed chain (exercised by e2e); these lock
    // the error branches — a misconfigured mTLS gateway must Err, never panic.
    // All three bail before the rustls builder, so no crypto provider is needed.

    fn tmp_file(name: &str, content: &[u8]) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("agg_mtls_test_{}_{}", std::process::id(), name));
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn build_mtls_rejects_missing_cert_file() {
        let mut missing = std::env::temp_dir();
        missing.push(format!("agg_mtls_absent_{}.pem", std::process::id()));
        let r = build_mtls_server_config(
            missing.to_str().unwrap(),
            missing.to_str().unwrap(),
            missing.to_str().unwrap(),
        );
        assert!(r.is_err(), "missing cert file must Err, not panic");
    }

    #[test]
    fn build_mtls_rejects_missing_key_file() {
        // Cert present (non-PEM ⇒ empty chain, no open error) but key file absent.
        let cert = tmp_file("cert_for_missing_key.pem", b"not a real pem\n");
        let mut absent_key = std::env::temp_dir();
        absent_key.push(format!("agg_mtls_nokey_{}.pem", std::process::id()));
        let r = build_mtls_server_config(
            cert.to_str().unwrap(),
            absent_key.to_str().unwrap(),
            cert.to_str().unwrap(),
        );
        let _ = std::fs::remove_file(&cert);
        assert!(r.is_err(), "missing key file must Err");
    }

    #[test]
    fn build_mtls_rejects_garbage_pem_with_no_private_key() {
        // All files exist but contain no PEM key ⇒ "no private key" Err.
        let cert = tmp_file("garbage_cert.pem", b"garbage\n");
        let key = tmp_file("garbage_key.pem", b"also garbage\n");
        let r =
            build_mtls_server_config(cert.to_str().unwrap(), key.to_str().unwrap(), cert.to_str().unwrap());
        let _ = std::fs::remove_file(&cert);
        let _ = std::fs::remove_file(&key);
        assert!(r.is_err(), "no parseable private key must Err");
    }
}
