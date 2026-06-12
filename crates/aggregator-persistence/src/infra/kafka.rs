use anyhow::{Result, anyhow};
use rdkafka::{
    consumer::{StreamConsumer, Consumer},
    message::{OwnedHeaders, Message},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeterReadingEvent {
    pub meter_id: String,
    pub timestamp: i64,
    pub energy_generated: f64,
    pub energy_consumed: f64,
    pub surplus: f64,
    pub voltage: f64,
    pub frequency: f64,
    pub power_factor: f64,
    pub signature: String,
    pub verified: bool,
    pub confidence_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridStatusEvent {
    pub frequency: f64,
    pub load_kw: f64,
    pub timestamp: i64,
}

pub struct AggregatorKafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl AggregatorKafkaProducer {
    pub fn new(bootstrap_servers: &str, topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .create()?;

        info!("✅ Kafka Producer initialized for topic: {}", topic);

        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    pub async fn publish_meter_reading(&self, reading: &MeterReadingEvent) -> Result<()> {
        let payload = serde_json::to_string(reading)?;
        let verified_str = reading.verified.to_string();

        let record = FutureRecord::to(&self.topic)
            .key(&reading.meter_id)
            .payload(&payload)
            .headers(
                OwnedHeaders::new()
                    .insert(rdkafka::message::Header {
                        key: "signature",
                        value: Some(&reading.signature),
                    })
                    .insert(rdkafka::message::Header {
                        key: "verified",
                        value: Some(&verified_str),
                    }),
            );

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {:?}", e))?;

        Ok(())
    }

    /// Publish a grid status event to the given topic (the producer's default
    /// topic is the meter-readings stream, so the topic is explicit here).
    pub async fn publish_grid_status(&self, topic: &str, event: &GridStatusEvent) -> Result<()> {
        let payload = serde_json::to_string(event)?;
        let record = FutureRecord::to(topic).key("grid_status").payload(&payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {:?}", e))?;

        Ok(())
    }
}

pub struct AggregatorKafkaConsumer {
    consumer: StreamConsumer,
}

impl AggregatorKafkaConsumer {
    pub fn new(bootstrap_servers: &str, group_id: &str, topic: &str) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "latest")
            .create()?;

        consumer.subscribe(&[topic])?;
        info!("✅ Kafka Consumer initialized for topic: {}", topic);

        Ok(Self { consumer })
    }

    pub async fn consume_grid_status(&self) -> Result<GridStatusEvent> {
        let message = self.consumer.recv().await?;
        let payload = message.payload().ok_or_else(|| anyhow!("Empty payload"))?;
        let event: GridStatusEvent = serde_json::from_slice(payload)?;
        Ok(event)
    }
}
