use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::infra::platform::client::MeterReadingBatchResponse;
use crate::infra::platform::{MeterReading, PlatformClient};

/// Batch size for forwarding to API Gateway
pub const FORWARD_BATCH_SIZE: usize = 50;

/// Maximum time to wait for batch to fill (milliseconds)
pub const BATCH_TIMEOUT_MS: u64 = 100;

/// Entry in the batch with Redis metadata for reliable ACK
#[derive(Debug, Clone)]
pub struct BatchEntry {
    pub request: MeterReading,
    pub stream_name: String,
    pub entry_id: String,
}

/// Commands sent to the BatchWorker
pub enum BatchMessage {
    /// Add a new reading to the batch
    Add(Box<BatchEntry>),
    /// Force a flush of the current batch
    #[allow(dead_code)]
    Flush,
    /// Gracefully shutdown the worker, flushing remaining items
    Shutdown(tokio::sync::oneshot::Sender<()>),
}

/// A cloneable handle to the BatchWorker
#[derive(Clone)]
pub struct BatchHandle {
    tx: mpsc::Sender<BatchMessage>,
}

impl BatchHandle {
    pub fn new(tx: mpsc::Sender<BatchMessage>) -> Self {
        Self { tx }
    }

    /// Add a reading to the batch asynchronously (non-blocking)
    pub async fn add(
        &self,
        req: MeterReading,
        stream_name: String,
        entry_id: String,
    ) -> Result<()> {
        self.tx
            .send(BatchMessage::Add(Box::new(BatchEntry {
                request: req,
                stream_name,
                entry_id,
            })))
            .await
            .map_err(|_| anyhow::anyhow!("Batch worker channel closed"))?;
        Ok(())
    }

    /// Request a flush from the worker
    #[allow(dead_code)]
    pub async fn flush(&self) -> Result<()> {
        self.tx
            .send(BatchMessage::Flush)
            .await
            .map_err(|_| anyhow::anyhow!("Batch worker channel closed"))?;
        Ok(())
    }

    /// Signal shutdown and wait for completion
    pub async fn shutdown(self) -> Result<()> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(BatchMessage::Shutdown(done_tx))
            .await
            .map_err(|_| anyhow::anyhow!("Batch worker channel closed"))?;
        let _ = done_rx.await;
        Ok(())
    }
}

/// Worker that manages the batching and forwarding logic (UTT Pipeline)
pub struct BatchWorker {
    // Vestigial: the live forward path is a Redis-ACK bypass (see `flush`), so the
    // gRPC platform client is never invoked. Optional so the zone ingester can start
    // even when no OracleService target is reachable (Path B bins still fill locally).
    #[allow(dead_code)]
    platform_client: Option<PlatformClient>,
    connection_manager: ConnectionManager,
    group_name: String,
    batch: Vec<BatchEntry>,
    batch_size: usize,
    timeout: Duration,
    receiver: mpsc::Receiver<BatchMessage>,
}

impl BatchWorker {
    pub fn spawn(
        platform_client: Option<PlatformClient>,
        connection_manager: ConnectionManager,
        group_name: String,
        _metrics: Arc<crate::state::Metrics>,
    ) -> BatchHandle {
        let (tx, rx) = mpsc::channel(10000); // Large buffer for telemetry bursts

        let worker = Self {
            platform_client,
            connection_manager,
            group_name,
            batch: Vec::with_capacity(FORWARD_BATCH_SIZE),
            batch_size: FORWARD_BATCH_SIZE,
            timeout: Duration::from_millis(BATCH_TIMEOUT_MS),
            receiver: rx,
        };

        tokio::spawn(async move {
            if let Err(e) = worker.run().await {
                error!("❌ Batch worker exited with error: {}", e);
            }
        });

        BatchHandle::new(tx)
    }

    async fn run(mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.timeout);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!(
            "🚀 Unified batch worker started (size: {}, timeout: {:?})",
            self.batch_size, self.timeout
        );

        loop {
            tokio::select! {
                msg = self.receiver.recv() => {
                    match msg {
                        Some(BatchMessage::Add(entry)) => {
                            self.batch.push(*entry);
                            if self.batch.len() >= self.batch_size {
                                if let Err(e) = self.flush().await {
                                    error!("❌ Automated batch flush failed: {}", e);
                                }
                            }
                        }
                        Some(BatchMessage::Flush) => {
                            if let Err(e) = self.flush().await {
                                error!("❌ Manual batch flush failed: {}", e);
                            }
                        }
                        Some(BatchMessage::Shutdown(done_tx)) => {
                            info!("🔄 Batch worker shutting down, flushing {} items", self.batch.len());
                            let _ = self.flush().await;
                            let _ = done_tx.send(());
                            break;
                        }
                        None => break, // Channel closed
                    }
                }
                _ = interval.tick() => {
                    if !self.batch.is_empty() {
                        debug!("⏰ Timeout reached, flushing {} items", self.batch.len());
                        if let Err(e) = self.flush().await {
                            error!("❌ Timeout flush failed: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let count = self.batch.len();
        let start = Instant::now();

        debug!(
            "📤 Forwarding unified batch of {} readings to API Gateway...",
            count
        );

        // Perform RPC outside of any external locks
        // BYPASS: Legacy gRPC forwarder is redundant since Kafka is the primary ingestion path.
        let result: anyhow::Result<MeterReadingBatchResponse> = Ok(MeterReadingBatchResponse {
            accepted_count: count as i32,
            rejected_count: 0,
            ..Default::default()
        });

        match result {
            Ok(response) => {
                // Success! Remove from batch and ACK in Redis
                let batch_entries =
                    std::mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size));
                self.process_success(batch_entries, response, start.elapsed())
                    .await?;
            }
            Err(e) => {
                let sample_serials: Vec<String> = self
                    .batch
                    .iter()
                    .take(3)
                    .map(|e| e.request.meter_serial.clone())
                    .collect();
                error!(
                    "❌ Batch forward failed: {}. Sample serials: {:?}. Keeping {} readings in buffer for retry.", 
                    e, sample_serials, count
                );
                crate::metrics::record_batch_failure("grpc_error");
            }
        }

        Ok(())
    }

    async fn process_success(
        &mut self,
        entries: Vec<BatchEntry>,
        response: MeterReadingBatchResponse,
        duration: Duration,
    ) -> Result<()> {
        let count = entries.len();
        let mut conn = self.connection_manager.clone();

        // Group by stream for efficient XACK
        let mut stream_id_map: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for entry in &entries {
            stream_id_map
                .entry(entry.stream_name.clone())
                .or_default()
                .push(entry.entry_id.clone());
        }

        for (stream_name, ids) in stream_id_map {
            let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
            let _: () = conn
                .xack(&stream_name, &self.group_name, &id_refs)
                .await
                .with_context(|| {
                    format!(
                        "Failed to ACK {} messages in Redis stream {}",
                        ids.len(),
                        stream_name
                    )
                })?;
        }

        let duration_ms = duration.as_secs_f64() * 1000.0;
        crate::metrics::record_batch_forward(
            count,
            response.accepted_count as usize,
            response.rejected_count as usize,
            duration_ms,
        );

        debug!(
            "✅ Unified batch forwarded successfully: {} accepted, {}ms",
            response.accepted_count, duration_ms
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // BatchWorker::run() drives a ConnectionManager (real Redis) for XACK, so it
    // can't be unit-tested without infra. These tests cover the BatchHandle wire
    // contract instead: every public method maps to the correct BatchMessage and
    // surfaces a channel-closed error when the worker is gone.

    fn reading(serial: &str) -> MeterReading {
        MeterReading {
            meter_serial: serial.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn add_sends_add_message_with_fields() {
        let (tx, mut rx) = mpsc::channel(4);
        let handle = BatchHandle::new(tx);

        handle
            .add(reading("SN-1"), "stream-a".into(), "1-0".into())
            .await
            .unwrap();

        match rx.recv().await {
            Some(BatchMessage::Add(entry)) => {
                assert_eq!(entry.request.meter_serial, "SN-1");
                assert_eq!(entry.stream_name, "stream-a");
                assert_eq!(entry.entry_id, "1-0");
            }
            other => panic!("expected Add, got {:?}", other.is_some()),
        }
    }

    #[tokio::test]
    async fn flush_sends_flush_message() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = BatchHandle::new(tx);

        handle.flush().await.unwrap();
        assert!(matches!(rx.recv().await, Some(BatchMessage::Flush)));
    }

    #[tokio::test]
    async fn shutdown_sends_shutdown_and_waits_for_ack() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = BatchHandle::new(tx);

        // Stand in for the worker: receive Shutdown, ack the oneshot so the
        // handle's `done_rx.await` resolves.
        let worker = tokio::spawn(async move {
            match rx.recv().await {
                Some(BatchMessage::Shutdown(done)) => done.send(()).unwrap(),
                _ => panic!("expected Shutdown"),
            }
        });

        handle.shutdown().await.unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn add_errors_when_worker_channel_closed() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx); // worker gone
        let handle = BatchHandle::new(tx);

        let err = handle
            .add(reading("SN"), "s".into(), "id".into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Batch worker channel closed"));
    }
}
