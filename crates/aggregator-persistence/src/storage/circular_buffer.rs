use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use serde_json::Value;
use std::path::Path;
use tracing::{debug, info};

/// An unsynced telemetry row: `(id, meter_id, timestamp, payload)`.
pub type UnsyncedRecord = (i64, String, DateTime<Utc>, Value);

/// A local, resilient telemetry ring-buffer using SQLite.
/// Ensures 24-hour data retention for offline operation.
pub struct CircularBuffer {
    conn: Connection,
    retention_hours: u32,
    push_count: u64,
}

impl CircularBuffer {
    /// Initialize the buffer at the specified path.
    pub fn new(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )?;

        info!(
            "🗄️ Initializing SQLite Circular Buffer at: {}",
            path.display()
        );

        // Ensure table exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meter_id TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                payload TEXT NOT NULL,
                synced INTEGER DEFAULT 0
            )",
            [],
        )?;

        // Index for faster cleanup/sync
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(timestamp)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_telemetry_synced ON telemetry(synced)",
            [],
        )?;

        Ok(Self {
            conn,
            retention_hours: 24,
            push_count: 0,
        })
    }

    /// Push a new reading into the buffer.
    pub fn push(
        &mut self,
        meter_id: &str,
        timestamp: DateTime<Utc>,
        payload: &Value,
    ) -> Result<()> {
        let payload_str = serde_json::to_string(payload)?;

        self.conn.execute(
            "INSERT INTO telemetry (meter_id, timestamp, payload) VALUES (?1, ?2, ?3)",
            params![meter_id, timestamp, payload_str],
        )?;

        self.push_count += 1;

        // Perform cleanup every 100 pushes to reduce SQL overhead in the critical path
        if self.push_count.is_multiple_of(100) {
            self.cleanup()?;
        }

        Ok(())
    }

    /// Mark records as synced with the platform.
    pub fn mark_as_synced(&mut self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        // Optimize: Use single UPDATE with IN clause for O(1) database interactions instead of O(N)
        let placeholders: String = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            "UPDATE telemetry SET synced = 1 WHERE id IN ({})",
            placeholders
        );

        self.conn.execute(&query, rusqlite::params_from_iter(ids))?;
        Ok(())
    }

    /// Fetch unsynced records to replay to the API Gateway.
    pub fn get_unsynced(&self, limit: u32) -> Result<Vec<UnsyncedRecord>> {
        let mut stmt = self.conn.prepare("SELECT id, meter_id, timestamp, payload FROM telemetry WHERE synced = 0 ORDER BY id LIMIT ?")?;

        let rows = stmt.query_map(params![limit], |row| {
            let id: i64 = row.get(0)?;
            let meter_id: String = row.get(1)?;
            let timestamp: DateTime<Utc> = row.get(2)?;
            let payload_str: String = row.get(3)?;
            let payload: Value = serde_json::from_str(&payload_str).unwrap_or_default();

            Ok((id, meter_id, timestamp, payload))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Cleanup old data beyond retention period (default 24h).
    fn cleanup(&mut self) -> Result<()> {
        let cutoff = gridtokenx_telemetry::time::now()
            - chrono::Duration::hours(self.retention_hours as i64);
        let deleted = self.conn.execute(
            "DELETE FROM telemetry WHERE timestamp < ?1 AND synced = 1",
            params![cutoff],
        )?;

        if deleted > 0 {
            debug!("🧹 Cleaned up {} old telemetry records", deleted);
        }
        Ok(())
    }
}
