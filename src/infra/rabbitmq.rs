use anyhow::Result;
use lapin::{
    options::*, types::FieldTable, Connection, ConnectionProperties, 
    BasicProperties, ExchangeKind, Channel,
};
use serde_json::json;
use tracing::info;

pub struct OracleRabbitMQProducer {
    channel: Channel,
}

impl OracleRabbitMQProducer {
    pub async fn new(amqp_url: &str) -> Result<Self> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        // Declare exchanges
        channel.exchange_declare(
            "oracle",
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        ).await?;

        channel.exchange_declare(
            "trading",
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        ).await?;

        // Declare queues
        channel.queue_declare(
            "meter.validation",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ).await?;

        channel.queue_bind(
            "meter.validation",
            "oracle",
            "meter.validate",
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await?;

        // Declare settlement retry queue
        channel.queue_declare(
            "settlement.retry",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ).await?;

        channel.queue_bind(
            "settlement.retry",
            "trading",
            "settlement.retry",
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await?;

        info!("✅ RabbitMQ Producer initialized and exchanges/queues declared");

        Ok(Self { channel })
    }

    pub async fn submit_validation_job(&self, meter_id: &str) -> Result<()> {
        let payload = json!({
            "meter_id": meter_id,
            "timestamp": chrono::Utc::now().timestamp_millis(),
            "priority": 5
        });
        
        self.channel.basic_publish(
            "oracle",
            "meter.validate",
            BasicPublishOptions::default(),
            serde_json::to_string(&payload)?.as_bytes(),
            BasicProperties::default()
                .with_delivery_mode(2), // Persistent
        ).await?;
        
        Ok(())
    }
    
    pub async fn retry_settlement(&self, settlement_id: &str, retry_count: u32) -> Result<()> {
        let payload = json!({
            "settlement_id": settlement_id,
            "retry_count": retry_count,
            "next_retry_at": (chrono::Utc::now() + chrono::Duration::minutes(5 * retry_count as i64)).timestamp_millis()
        });
        
        let priority = std::cmp::min(10, retry_count + 1);
        
        self.channel.basic_publish(
            "trading",
            "settlement.retry",
            BasicPublishOptions::default(),
            serde_json::to_string(&payload)?.as_bytes(),
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_priority(priority as u8),
        ).await?;
        
        Ok(())
    }
}
