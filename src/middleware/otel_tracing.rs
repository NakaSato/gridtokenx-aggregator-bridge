//! Axum middleware for OpenTelemetry distributed tracing
//!
//! This middleware automatically creates spans for incoming HTTP requests,
//! capturing standard attributes like HTTP method, status code, duration, etc.

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use opentelemetry::trace::{Span, SpanKind, Tracer};
use opentelemetry::{global, KeyValue};
use std::time::Instant;

/// Middleware that creates OpenTelemetry spans for HTTP requests
pub async fn otel_tracing_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();

    let method = request.method().clone();
    let uri = request.uri().clone();
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_string());

    let headers = request.headers();
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let client_ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let tracer = global::tracer("gridtokenx-oracle-bridge");
    let mut span = tracer
        .span_builder(format!("{} {}", method, matched_path))
        .with_kind(SpanKind::Server)
        .with_attributes(vec![
            KeyValue::new("http.method", method.to_string()),
            KeyValue::new("http.route", matched_path.clone()),
            KeyValue::new("http.url", uri.to_string()),
            KeyValue::new("http.user_agent", user_agent.to_string()),
            KeyValue::new("client.address", client_ip.to_string()),
            KeyValue::new("network.protocol.name", "http"),
        ])
        .start(&tracer);

    let response = next.run(request).await;

    let status = response.status();
    let duration = start.elapsed();

    span.add_event_with_timestamp(
        "response",
        std::time::SystemTime::now(),
        vec![KeyValue::new("http.status_code", status.as_u16() as i64)],
    );

    span.add_event_with_timestamp(
        "duration",
        std::time::SystemTime::now(),
        vec![KeyValue::new("http.duration_ms", duration.as_millis() as i64)],
    );

    if status.is_server_error() {
        span.add_event_with_timestamp(
            "error",
            std::time::SystemTime::now(),
            vec![
                KeyValue::new("error.type", "http_5xx"),
                KeyValue::new("http.status_code", status.as_u16() as i64),
            ],
        );
    }

    span.end();

    response
}
