use anyhow::Result;
use rdkafka::{
    message::OwnedHeaders,
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

pub struct OracleKafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl OracleKafkaProducer {
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
}
