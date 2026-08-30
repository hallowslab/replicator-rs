use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordStatus {
    Pending,
    InFlight,
    Done,
    Failed,
}

impl RecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordStatus::Pending => "pending",
            RecordStatus::InFlight => "in_flight",
            RecordStatus::Done => "done",
            RecordStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationRecord {
    pub id: i64,
    pub source_path: String,
    pub dest_path: String,
    pub dest_key: String,
    pub status: RecordStatus,
    pub attempts: u32,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Durable job store shared by the rftps writer and the replicator consumer.
///
/// SQLite with WAL so the writer (rftps process) and reader (replicator process)
/// can coexist. `dest_key` is the final remote key, computed once at record time
/// so retries/resume never mint a new UUID.
pub struct ReplicationStore {
    conn: Mutex<Connection>,
}

impl ReplicationStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create store directory '{}': {}", parent.display(), e))?;
        }
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS replication_records (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 source_path TEXT NOT NULL,
                 dest_path TEXT NOT NULL,
                 dest_key TEXT NOT NULL,
                 status TEXT NOT NULL DEFAULT 'pending',
                 attempts INTEGER NOT NULL DEFAULT 0,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_status ON replication_records(status);",
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Appends a new pending record. Returns the assigned id.
    pub fn insert(&self, source_path: &str, dest_path: &str, dest_key: &str) -> Result<i64, String> {
        let conn = self.conn.lock().unwrap();
        let now = now_unix();
        conn.execute(
            "INSERT INTO replication_records
                (source_path, dest_path, dest_key, status, attempts, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'pending', 0, ?4, ?4)",
            params![source_path, dest_path, dest_key, now],
        )
        .map_err(|e| e.to_string())?;
        Ok(conn.last_insert_rowid())
    }

    /// Atomically claims the next pending record (marks it `in_flight` and bumps
    /// its attempt count) and returns it, or `None` if the queue is empty.
    pub fn next_pending(&self) -> Result<Option<ReplicationRecord>, String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let rec = tx
            .query_row(
                "SELECT id, source_path, dest_path, dest_key, attempts
                 FROM replication_records
                 WHERE status = 'pending'
                 ORDER BY id ASC LIMIT 1",
                [],
                |row| {
                    Ok(ReplicationRecord {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        dest_path: row.get(2)?,
                        dest_key: row.get(3)?,
                        status: RecordStatus::InFlight,
                        attempts: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(|e| e.to_string())?;

        let claimed = if let Some(rec) = rec {
            tx.execute(
                "UPDATE replication_records SET status = 'in_flight', attempts = attempts + 1, updated_at = ?1 WHERE id = ?2",
                params![now_unix(), rec.id],
            )
            .map_err(|e| e.to_string())?;
            Some(rec)
        } else {
            None
        };

        tx.commit().map_err(|e| e.to_string())?;
        Ok(claimed)
    }

    pub fn mark_done(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE replication_records SET status = 'done', updated_at = ?1 WHERE id = ?2",
            params![now_unix(), id],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Marks a failed attempt. If attempts >= `max_retries` the record is
    /// finalized as `failed`; otherwise it is returned to `pending` for retry.
    pub fn mark_failed(&self, id: i64, max_retries: u32) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let attempts: u32 = conn
            .query_row(
                "SELECT attempts FROM replication_records WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        let new_status = if attempts >= max_retries {
            RecordStatus::Failed
        } else {
            RecordStatus::Pending
        };
        conn.execute(
            "UPDATE replication_records SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![new_status.as_str(), now_unix(), id],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Crash recovery: any record left `in_flight` by a previous run is returned
    /// to `pending` so it is re-uploaded on next start.
    pub fn resume_inflight(&self) -> Result<usize, String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE replication_records SET status = 'pending', updated_at = ?1 WHERE status = 'in_flight'",
            params![now_unix()],
        )
        .map_err(|e| e.to_string())
    }

    pub fn pending_count(&self) -> Result<i64, String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM replication_records WHERE status = 'pending'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())
    }
}
