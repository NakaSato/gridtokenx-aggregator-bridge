//! OpenTelemetry initialization for GridTokenX services (SigNoz via OTEL Collector).
//!
//! Uses the pattern established in api-services' telemetry.rs: filter → otel → fmt.

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, MetricExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Holds the telemetry provider so it can be shut down explicitly.
#[derive(Debug)]
pub struct TelemetryGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    /// Flush pending spans and shut down the tracer provider.
    pub fn shutdown(&self) {
        let _ = (&self.tracer_provider).shutdown();
        if let Some(ref mp) = self.meter_provider {
            let _ = mp.shutdown();
        }
    }
}

/// Initialize OpenTelemetry tracing and set up the global subscriber.
///
/// Returns a `TelemetryGuard` that should be held for the lifetime of the
/// application. Call `TelemetryGuard::shutdown()` before dropping to flush
/// any pending spans.
///
/// `service_name_default`: e.g. `"gridtokenx-iam"`, `"gridtokenx-trading"`, etc.
pub fn init_telemetry(service_name_default: &'static str) -> TelemetryGuard {
    let otel_enabled = std::env::var("OTEL_ENABLED")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(true);

    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://otel-collector:4317".to_string());

    let service_name = std::env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| service_name_default.to_string());

    let environment = std::env::var("ENVIRONMENT")
        .unwrap_or_else(|_| "development".to_string());

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Always set up the fmt subscriber so that local logging works
    // even when OTel is disabled or the exporter fails.
    if !otel_enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
        eprintln!("[INFO] OTel tracing DISABLED (OTEL_ENABLED=false) — fmt-only subscriber active.");
        let trace_provider = SdkTracerProvider::builder().build();
        return TelemetryGuard { tracer_provider: trace_provider, meter_provider: None };
    }

    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .with_attributes(vec![
            opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            opentelemetry::KeyValue::new("deployment.environment", environment.clone()),
        ])
        .build();

    let mut exporter_builder = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_endpoint);

    let exporter_result = exporter_builder.build();

    match exporter_result {
        Ok(exp) => {
            let trace_provider = SdkTracerProvider::builder()
                .with_batch_exporter(exp)
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(resource.clone())
                .build();

            global::set_tracer_provider(trace_provider.clone());

            let tracer = trace_provider.tracer(service_name.clone());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(filter)
                .with(otel_layer)
                .with(tracing_subscriber::fmt::layer().json())
                .init();

            tracing::info!(
                service = %service_name,
                endpoint = %otlp_endpoint,
                "OTel tracing initialized"
            );

            let meter_provider = init_metrics(&service_name, &otlp_endpoint, &resource);

            TelemetryGuard { tracer_provider: trace_provider, meter_provider }
        }
        Err(err) => {
            eprintln!("[WARN] OTLP exporter failed ({err}), falling back to fmt-only.");
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
            let trace_provider = SdkTracerProvider::builder().build();
            TelemetryGuard { tracer_provider: trace_provider, meter_provider: None }
        }
    }
}

/// Graceful shutdown — flushes batched spans before exit.
pub fn shutdown_telemetry(guard: &TelemetryGuard) {
    tracing::info!("Shutting down OpenTelemetry tracer...");
    guard.shutdown();
}

/// Initialize OpenTelemetry Metrics.
fn init_metrics(_service_name: &str, otlp_endpoint: &str, resource: &Resource) -> Option<SdkMeterProvider> {
    let mut exporter_builder = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint);

    let exporter = exporter_builder.build().map_err(|e| {
        eprintln!("[WARN] OTLP metrics exporter failed: {e}");
        e
    }).ok()?;

    let reader = PeriodicReader::builder(exporter).build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(reader)
        .build();

    global::set_meter_provider(provider.clone());
    Some(provider)
}
