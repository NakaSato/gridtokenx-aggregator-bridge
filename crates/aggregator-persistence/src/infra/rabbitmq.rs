use anyhow::Result;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Build the meter-validation job payload. Pure (timestamp aside) so the wire
/// shape can be asserted without a broker — kept in sync with the queue consumer.
fn build_validation_payload(meter_id: &str) -> serde_json::Value {
    json!({
        "meter_id": meter_id,
        "timestamp": gridtokenx_telemetry::time::now().timestamp_millis(),
        "priority": 5
    })
}

/// RabbitMQ producer for meter-validation jobs.
///
/// Self-healing, like the `SignatureVerifier` / `Router::disseminate` publishers: it
/// owns the `amqp_url` (not just a one-shot channel) and rebuilds the channel + retries
/// once on transport error. A single `lapin::Channel` is not recoverable — once it goes
/// to `invalid channel state`, every publish fails forever — so a RabbitMQ restart (or a
/// dropped channel) must not wedge the producer permanently.
pub struct AggregatorRabbitMQProducer {
    amqp_url: String,
    channel: Mutex<Channel>,
}

impl AggregatorRabbitMQProducer {
    pub async fn new(amqp_url: &str) -> Result<Self> {
        let channel = Self::connect(amqp_url).await?;
        info!("✅ RabbitMQ Producer initialized and exchanges/queues declared");
        Ok(Self {
            amqp_url: amqp_url.to_string(),
            channel: Mutex::new(channel),
        })
    }

    /// Open a connection, create a channel, and (idempotently) declare the
    /// exchange/queue/binding. Reused for the initial connect and the self-heal
    /// rebuild after a transport error.
    ///
    /// The `Connection` handle is dropped on return — lapin keeps the socket alive via
    /// the `Channel`'s internal references, so there is no need to leak it with
    /// `mem::forget` (which would strand one connection per reconnect).
    async fn connect(amqp_url: &str) -> Result<Channel> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        // Declare exchange
        channel
            .exchange_declare(
                "aggregator",
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // Declare queue
        channel
            .queue_declare(
                "meter.validation",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // Bind queue to exchange
        channel
            .queue_bind(
                "meter.validation",
                "aggregator",
                "meter.validate",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(channel)
    }

    pub async fn submit_validation_job(&self, meter_id: &str) -> Result<()> {
        let body = serde_json::to_string(&build_validation_payload(meter_id))?;

        // Publish, retrying once on transport error by rebuilding the channel — mirrors
        // the `get_with_retry` self-heal so a RabbitMQ restart (or a channel gone to
        // "invalid channel state") no longer wedges the producer forever.
        match self.publish(body.as_bytes()).await {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!("⚠️ RabbitMQ publish failed ({e}); rebuilding channel and retrying once");
                let fresh = Self::connect(&self.amqp_url).await?;
                *self.channel.lock().await = fresh;
                self.publish(body.as_bytes()).await
            }
        }
    }

    /// Single publish attempt against the current channel. A dead/invalid channel
    /// surfaces here as `Err`, which drives the rebuild-and-retry in
    /// [`Self::submit_validation_job`].
    async fn publish(&self, body: &[u8]) -> Result<()> {
        let channel = self.channel.lock().await;
        channel
            .basic_publish(
                "aggregator",
                "meter.validate",
                BasicPublishOptions::default(),
                body,
                BasicProperties::default().with_delivery_mode(2), // Persistent
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The producer needs a live lapin Channel (real broker) — `new`/`connect`/
    // `publish` can't be unit-tested without RabbitMQ. The pure tests pin the
    // wire payload; the self-heal rebuild-and-retry is exercised by the
    // `#[ignore]` broker test (default amqp://...:9030, the compose host port).

    fn rabbit_url() -> String {
        std::env::var("RABBITMQ_URL")
            .unwrap_or_else(|_| "amqp://gridtokenx:rabbitmq_secret_2025@localhost:9030".to_string())
    }

    #[test]
    fn payload_has_expected_wire_shape() {
        let p = build_validation_payload("METER-42");
        assert_eq!(p["meter_id"], "METER-42");
        assert_eq!(p["priority"], 5);
        // timestamp is epoch-millis, present and positive.
        assert!(p["timestamp"].is_i64());
        assert!(p["timestamp"].as_i64().unwrap() > 0);
    }

    #[test]
    fn payload_serializes_to_compact_json_object() {
        let body = serde_json::to_string(&build_validation_payload("M1")).unwrap();
        // Round-trips back to an object carrying the three known keys.
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v.is_object());
        assert_eq!(v.as_object().unwrap().len(), 3);
        assert_eq!(v["meter_id"], "M1");
    }

    #[tokio::test]
    #[ignore = "requires RABBITMQ_URL (default amqp://gridtokenx:...@localhost:9030)"]
    async fn submit_and_self_heal_against_real_broker() {
        let producer = AggregatorRabbitMQProducer::new(&rabbit_url())
            .await
            .expect("connect to RabbitMQ");

        // First publish on the live channel.
        producer
            .submit_validation_job("__test_meter__")
            .await
            .expect("initial publish should succeed");

        // Force the channel dead, then publish again: submit_validation_job must
        // rebuild the channel and retry once (self-heal), not wedge forever.
        producer.channel.lock().await.close(200, "test").await.ok();
        producer
            .submit_validation_job("__test_meter__")
            .await
            .expect("publish should self-heal after channel close");
    }
}
