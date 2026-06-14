use anyhow::Result;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use serde_json::json;
use tracing::info;

pub struct AggregatorRabbitMQProducer {
    channel: Channel,
}

impl AggregatorRabbitMQProducer {
    pub async fn new(amqp_url: &str) -> Result<Self> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        // Declare exchanges
        channel
            .exchange_declare(
                "aggregator",
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // Declare queues
        channel
            .queue_declare(
                "meter.validation",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        channel
            .queue_bind(
                "meter.validation",
                "aggregator",
                "meter.validate",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        info!("✅ RabbitMQ Producer initialized and exchanges/queues declared");

        Ok(Self { channel })
    }

    pub async fn submit_validation_job(&self, meter_id: &str) -> Result<()> {
        let payload = json!({
            "meter_id": meter_id,
            "timestamp": gridtokenx_telemetry::time::now().timestamp_millis(),
            "priority": 5
        });

        self.channel
            .basic_publish(
                "aggregator",
                "meter.validate",
                BasicPublishOptions::default(),
                serde_json::to_string(&payload)?.as_bytes(),
                BasicProperties::default().with_delivery_mode(2), // Persistent
            )
            .await?;

        Ok(())
    }
}
