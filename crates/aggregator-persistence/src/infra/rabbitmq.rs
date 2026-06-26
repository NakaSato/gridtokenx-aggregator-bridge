use anyhow::Result;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{info, warn};

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
        let payload = json!({
            "meter_id": meter_id,
            "timestamp": gridtokenx_telemetry::time::now().timestamp_millis(),
            "priority": 5
        });
        let body = serde_json::to_string(&payload)?;

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
