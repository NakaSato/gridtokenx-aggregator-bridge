use anyhow::{anyhow, Result};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::{Message, OwnedHeaders},
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
            // Detect dead/black-holed peers. Without TCP keepalive (librdkafka
            // default OFF), a connection silently dropped by the broker's idle
            // reap (`connections.max.idle.ms`) or the container overlay during an
            // ingest lull is never noticed — the producer keeps writing to a dead
            // socket and every message expires at `message.timeout.ms`, forever,
            // with no auto-recovery. Keepalive lets librdkafka tear down the stale
            // socket and reconnect.
            .set("socket.keepalive.enable", "true")
            // Tolerate transient broker slowness (e.g. CPU contention) instead of
            // dropping the reading after only 5s. This is a fire-and-forget stream
            // already made durable via Redis + InfluxDB, so a longer delivery
            // window is cheap insurance against spurious MessageTimedOut floods.
            .set("message.timeout.ms", "30000")
            // Bound reconnect latency once a dead socket is detected.
            .set("reconnect.backoff.ms", "500")
            .set("reconnect.backoff.max.ms", "10000")
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

#[cfg(test)]
mod tests {
    use super::*;

    // Producer/Consumer wrap rdkafka clients that need a live broker, so the
    // wire roundtrip is an `#[ignore]` integration test (default localhost:29001,
    // the compose PLAINTEXT_HOST listener). The pure tests pin the event
    // (de)serialization — the exact step `consume_grid_status` performs after
    // `recv()`, and the shape `publish_*` puts on the wire.

    fn kafka_brokers() -> String {
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:29001".to_string())
    }

    fn sample_reading() -> MeterReadingEvent {
        MeterReadingEvent {
            meter_id: "METER-1".to_string(),
            timestamp: 1_700_000_000_000,
            energy_generated: 12.5,
            energy_consumed: 4.25,
            surplus: 8.25,
            voltage: 230.1,
            frequency: 49.98,
            power_factor: 0.97,
            signature: "sig-abc".to_string(),
            verified: true,
            confidence_score: 0.91,
        }
    }

    #[test]
    fn meter_reading_event_serde_roundtrip() {
        let r = sample_reading();
        let json = serde_json::to_string(&r).unwrap();
        let back: MeterReadingEvent = serde_json::from_str(&json).unwrap();

        // All 11 fields survive the wire.
        assert_eq!(back.meter_id, r.meter_id);
        assert_eq!(back.timestamp, r.timestamp);
        assert_eq!(back.energy_generated, r.energy_generated);
        assert_eq!(back.energy_consumed, r.energy_consumed);
        assert_eq!(back.surplus, r.surplus);
        assert_eq!(back.voltage, r.voltage);
        assert_eq!(back.frequency, r.frequency);
        assert_eq!(back.power_factor, r.power_factor);
        assert_eq!(back.signature, r.signature);
        assert_eq!(back.verified, r.verified);
        assert_eq!(back.confidence_score, r.confidence_score);
    }

    #[test]
    fn grid_status_event_serde_roundtrip() {
        let e = GridStatusEvent {
            frequency: 50.02,
            load_kw: 1234.5,
            timestamp: 1_700_000_000_001,
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: GridStatusEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.frequency, e.frequency);
        assert_eq!(back.load_kw, e.load_kw);
        assert_eq!(back.timestamp, e.timestamp);
    }

    #[test]
    fn grid_status_parses_from_byte_slice() {
        // Mirrors the post-recv() decode in `consume_grid_status`.
        let bytes = br#"{"frequency":49.9,"load_kw":10.0,"timestamp":42}"#;
        let e: GridStatusEvent = serde_json::from_slice(bytes).unwrap();
        assert_eq!(e.frequency, 49.9);
        assert_eq!(e.timestamp, 42);
    }

    #[test]
    fn grid_status_rejects_malformed_and_empty_payload() {
        assert!(serde_json::from_slice::<GridStatusEvent>(b"not json").is_err());
        assert!(serde_json::from_slice::<GridStatusEvent>(b"").is_err());
        // Missing required field also fails (no serde defaults on the type).
        assert!(serde_json::from_slice::<GridStatusEvent>(br#"{"frequency":50.0}"#).is_err());
    }

    #[tokio::test]
    #[ignore = "requires KAFKA_BOOTSTRAP_SERVERS (default localhost:29001)"]
    async fn publish_and_consume_grid_status_against_real_kafka() {
        use std::time::Duration;

        let brokers = kafka_brokers();
        // Unique topic + group so the latest-offset consumer isn't shadowed by
        // stale data from prior runs.
        let topic = "gridtokenx.test.grid_status.selfcheck";
        let group = "aggregator-test-grid-status";

        let producer = AggregatorKafkaProducer::new(&brokers, topic).expect("producer");
        let consumer = AggregatorKafkaConsumer::new(&brokers, group, topic).expect("consumer");

        // Give the consumer group time to join + get its partition assignment;
        // with auto.offset.reset=latest a message produced before assignment is lost.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let sent = GridStatusEvent {
            frequency: 50.05,
            load_kw: 777.0,
            timestamp: 1_700_000_000_123,
        };

        // Produce repeatedly and poll until one lands (covers assignment races).
        let mut got = None;
        for _ in 0..20 {
            producer
                .publish_grid_status(topic, &sent)
                .await
                .expect("publish");
            if let Ok(Ok(ev)) =
                tokio::time::timeout(Duration::from_millis(500), consumer.consume_grid_status())
                    .await
            {
                got = Some(ev);
                break;
            }
        }

        let ev = got.expect("should consume a grid status event within timeout");
        assert_eq!(ev.frequency, sent.frequency);
        assert_eq!(ev.load_kw, sent.load_kw);
        assert_eq!(ev.timestamp, sent.timestamp);
    }
}
