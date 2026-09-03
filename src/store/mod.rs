use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordStatus {
    Pending,
    InFlight,
    Failed,
}

impl RecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordStatus::Pending => "pending",
            RecordStatus::InFlight => "in_flight",
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
    pub key_finalized: bool,
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
/// can coexist. Records are deleted once replication succeeds (or the source file
/// is gone), so the store only ever holds work-in-progress:
///   - `pending` (new or retry-scheduled records),
///   - `in_flight` (claimed, being uploaded).
///
/// `dest_path` is the logical path recorded by rftps; `dest_key` is the final
/// remote key, computed once at first claim so retries/resume never mint a new
/// UUID (`key_finalized` marks that it has been resolved).
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
                 key_finalized INTEGER NOT NULL DEFAULT 0,
                 status TEXT NOT NULL DEFAULT 'pending',
                 attempts INTEGER NOT NULL DEFAULT 0,
                 next_retry_at INTEGER NOT NULL DEFAULT 0,
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

    /// Atomically claims the next due `pending` record (marks it `in_flight` and
    /// bumps its attempt count) and returns it, or `None` if nothing is ready.
    /// Records whose `next_retry_at` is in the future are skipped.
    pub fn next_pending(&self) -> Result<Option<ReplicationRecord>, String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let rec = tx
            .query_row(
                "SELECT id, source_path, dest_path, dest_key, key_finalized, attempts
                 FROM replication_records
                 WHERE status = 'pending' AND next_retry_at <= ?1
                 ORDER BY id ASC LIMIT 1",
                [now_unix()],
                |row| {
                    Ok(ReplicationRecord {
                        id: row.get(0)?,
                        source_path: row.get(1)?,
                        dest_path: row.get(2)?,
                        dest_key: row.get(3)?,
                        key_finalized: row.get(4)?,
                        status: RecordStatus::InFlight,
                        attempts: row.get(5)?,
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

    /// Deletes a successfully-replicated record (or one whose source file is
    /// gone). Deleting keeps the store bounded.
    pub fn remove(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM replication_records WHERE id = ?1", params![id])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Returns a failed record to `pending`, scheduling the next attempt with
    /// exponential backoff (`retry_delay_secs * 2^(attempts-1)`, capped at 1h).
    ///
    /// If the record is older than `max_age_secs` (measured from `created_at`),
    /// it is instead marked `failed` as a dead-letter — kept visible so the
    /// operator can see what failed, and resettable via [`Self::reset_failed`].
    pub fn mark_failed(&self, id: i64, retry_delay_secs: u64, max_age_secs: u64) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let (attempts, created_at): (u32, i64) = conn
            .query_row(
                "SELECT attempts, created_at FROM replication_records WHERE id = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|e| e.to_string())?;
        let now = now_unix();

        if max_age_secs > 0 && now.saturating_sub(created_at) >= max_age_secs as i64 {
            conn.execute(
                "UPDATE replication_records SET status = 'failed', updated_at = ?1 WHERE id = ?2",
                params![now, id],
            )
            .map_err(|e| e.to_string())?;
            return Ok(());
        }

        let exp = attempts.saturating_sub(1).min(10);
        let delay = retry_delay_secs
            .saturating_mul(1u64 << exp)
            .min(3600);
        let next = now + delay as i64;
        conn.execute(
            "UPDATE replication_records SET status = 'pending', next_retry_at = ?1, updated_at = ?2 WHERE id = ?3",
            params![next, now, id],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Resets dead-lettered records (`failed`) back to `pending` with a fresh
    /// attempt count, so they are retried on the next run.
    pub fn reset_failed(&self) -> Result<usize, String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE replication_records SET status = 'pending', next_retry_at = 0, attempts = 0, updated_at = ?1 WHERE status = 'failed'",
            params![now_unix()],
        )
        .map_err(|e| e.to_string())
    }

    /// Persists the final remote key for a record, computed once at first claim
    /// (so a UUID generated by `immutable_naming` is never re-minted on retry).
    pub fn finalize_key(&self, id: i64, dest_key: &str) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE replication_records SET dest_key = ?1, key_finalized = 1, updated_at = ?2 WHERE id = ?3",
            params![dest_key, now_unix(), id],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Crash recovery: any record left `in_flight` by a previous run is returned
    /// to `pending` (immediately due) so it is re-uploaded on next start.
    pub fn resume_inflight(&self) -> Result<usize, String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE replication_records SET status = 'pending', next_retry_at = 0, updated_at = ?1 WHERE status = 'in_flight'",
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
