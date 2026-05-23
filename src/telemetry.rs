use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug)]
pub struct TelemetryGuard {}

impl TelemetryGuard {
    pub fn shutdown(&self) {}
}

pub fn init_telemetry(_service_name_default: &'static str) -> TelemetryGuard {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    TelemetryGuard {}
}

pub fn shutdown_telemetry(_guard: &TelemetryGuard) {}
