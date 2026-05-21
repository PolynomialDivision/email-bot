use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::task::spawn_blocking;

pub struct RouteRecord {
    pub email_message_id: String,
    pub thread_root_email_message_id: Option<String>,
    pub subject: Option<String>,
    pub origin: String,
}

pub struct SmtpRetryItem {
    pub matrix_event_id: String,
    pub smtp_payload: String,
    pub attempts: i64,
}

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).context("Failed to open SQLite database")?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS seen_emails (
                uid INTEGER NOT NULL,
                mailbox TEXT NOT NULL,
                message_id TEXT,
                processed_at INTEGER NOT NULL,
                PRIMARY KEY (uid, mailbox)
            );

            CREATE TABLE IF NOT EXISTS thread_map (
                message_id TEXT PRIMARY KEY,
                matrix_event_id TEXT NOT NULL,
                thread_root_id TEXT NOT NULL,
                mailbox TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS retry_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Bidirectional bridge routing table.
            -- origin='email': an email was posted to Matrix (smtp_* columns are NULL).
            -- origin='matrix': a Matrix reply is being sent as email.
            --   smtp_status: 'pending' | 'sent' | 'failed'
            CREATE TABLE IF NOT EXISTS message_routes (
                matrix_event_id TEXT PRIMARY KEY,
                email_message_id TEXT NOT NULL,
                thread_root_email_message_id TEXT,
                subject TEXT,
                origin TEXT NOT NULL,
                smtp_status TEXT,
                smtp_attempts INTEGER NOT NULL DEFAULT 0,
                smtp_next_retry_at INTEGER,
                smtp_payload TEXT,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_routes_email_id
                ON message_routes(email_message_id);
            ",
        )?;
        // Add new columns to existing databases that were created before these were introduced.
        // SQLite has no ADD COLUMN IF NOT EXISTS, so we silently swallow "duplicate column" errors.
        for ddl in &[
            "ALTER TABLE message_routes ADD COLUMN smtp_status TEXT",
            "ALTER TABLE message_routes ADD COLUMN smtp_attempts INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE message_routes ADD COLUMN smtp_next_retry_at INTEGER",
            "ALTER TABLE message_routes ADD COLUMN smtp_payload TEXT",
        ] {
            let _ = conn.execute(ddl, []);
        }
        Ok(())
    }

    pub async fn uid_seen(&self, mailbox: &str, uid: u32) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let mailbox = mailbox.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM seen_emails WHERE uid = ?1 AND mailbox = ?2",
                params![uid, mailbox],
                |row| row.get(0),
            )?;
            Ok::<bool, anyhow::Error>(count > 0)
        })
        .await
        .context("spawn_blocking uid_seen")?
    }

    pub async fn mark_uid_seen(
        &self,
        mailbox: &str,
        uid: u32,
        message_id: Option<&str>,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let mailbox = mailbox.to_owned();
        let message_id = message_id.map(str::to_owned);
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO seen_emails (uid, mailbox, message_id, processed_at) VALUES (?1, ?2, ?3, ?4)",
                params![uid, mailbox, message_id, now],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking mark_uid_seen")?
    }

    pub async fn get_thread_root(
        &self,
        message_id: &str,
    ) -> Result<Option<(String, String)>> {
        let conn = Arc::clone(&self.conn);
        let message_id = message_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT matrix_event_id, thread_root_id FROM thread_map WHERE message_id = ?1",
            )?;
            let result = stmt.query_row(params![message_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            });
            match result {
                Ok(row) => Ok(Some(row)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(anyhow::anyhow!(e)),
            }
        })
        .await
        .context("spawn_blocking get_thread_root")?
    }

    pub async fn store_thread(
        &self,
        message_id: &str,
        matrix_event_id: &str,
        thread_root_id: &str,
        mailbox: &str,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let message_id = message_id.to_owned();
        let matrix_event_id = matrix_event_id.to_owned();
        let thread_root_id = thread_root_id.to_owned();
        let mailbox = mailbox.to_owned();
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO thread_map (message_id, matrix_event_id, thread_root_id, mailbox, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![message_id, matrix_event_id, thread_root_id, mailbox, now],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking store_thread")?
    }

    pub async fn get_last_uid(&self, mailbox: &str) -> Result<u32> {
        let conn = Arc::clone(&self.conn);
        let key = format!("last_uid:{}", mailbox);
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let result = conn.query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![key],
                |row| row.get::<_, String>(0),
            );
            match result {
                Ok(val) => Ok(val.parse::<u32>().unwrap_or(0)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
                Err(e) => Err(anyhow::anyhow!(e)),
            }
        })
        .await
        .context("spawn_blocking get_last_uid")?
    }

    pub async fn set_last_uid(&self, mailbox: &str, uid: u32) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let key = format!("last_uid:{}", mailbox);
        let value = uid.to_string();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
                params![key, value],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking set_last_uid")?
    }

    pub async fn push_retry(&self, payload_json: &str) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let payload = payload_json.to_owned();
        let now = chrono::Utc::now().timestamp();
        // first retry after 60 seconds
        let next_retry_at = now + 60;
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT INTO retry_queue (payload, attempts, next_retry_at, created_at) VALUES (?1, 0, ?2, ?3)",
                params![payload, next_retry_at, now],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking push_retry")?
    }

    pub async fn pop_retry(&self, limit: usize) -> Result<Vec<(i64, String, i64)>> {
        let conn = Arc::clone(&self.conn);
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT id, payload, attempts FROM retry_queue WHERE next_retry_at <= ?1 ORDER BY next_retry_at ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![now, limit as i64], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok::<Vec<(i64, String, i64)>, anyhow::Error>(results)
        })
        .await
        .context("spawn_blocking pop_retry")?
    }

    pub async fn ack_retry(&self, id: i64) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute("DELETE FROM retry_queue WHERE id = ?1", params![id])?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking ack_retry")?
    }

    pub async fn fail_retry(&self, id: i64, next_retry_at: i64) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE retry_queue SET attempts = attempts + 1, next_retry_at = ?1 WHERE id = ?2",
                params![next_retry_at, id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking fail_retry")?
    }

    pub async fn store_route(
        &self,
        matrix_event_id: &str,
        email_message_id: &str,
        thread_root_email_message_id: Option<&str>,
        subject: Option<&str>,
        origin: &str,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        let email_message_id = email_message_id.to_owned();
        let thread_root_email_message_id = thread_root_email_message_id.map(str::to_owned);
        let subject = subject.map(str::to_owned);
        let origin = origin.to_owned();
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO message_routes \
                 (matrix_event_id, email_message_id, thread_root_email_message_id, subject, origin, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![matrix_event_id, email_message_id, thread_root_email_message_id, subject, origin, now],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking store_route")?
    }

    pub async fn get_route_by_matrix_event(
        &self,
        matrix_event_id: &str,
    ) -> Result<Option<RouteRecord>> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT email_message_id, thread_root_email_message_id, subject, origin \
                 FROM message_routes WHERE matrix_event_id = ?1",
            )?;
            let result = stmt.query_row(params![matrix_event_id], |row| {
                Ok(RouteRecord {
                    email_message_id: row.get(0)?,
                    thread_root_email_message_id: row.get(1)?,
                    subject: row.get(2)?,
                    origin: row.get(3)?,
                })
            });
            match result {
                Ok(r) => Ok(Some(r)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(anyhow::anyhow!(e)),
            }
        })
        .await
        .context("spawn_blocking get_route_by_matrix_event")?
    }

    /// Insert a pending Matrix→Email route before attempting SMTP.
    /// Uses INSERT OR IGNORE so duplicate events from Matrix sync are harmless.
    pub async fn store_matrix_reply_pending(
        &self,
        matrix_event_id: &str,
        our_message_id: &str,
        thread_root_email_message_id: Option<&str>,
        subject: Option<&str>,
        smtp_payload: &str,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        let our_message_id = our_message_id.to_owned();
        let thread_root_email_message_id = thread_root_email_message_id.map(str::to_owned);
        let subject = subject.map(str::to_owned);
        let smtp_payload = smtp_payload.to_owned();
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO message_routes \
                 (matrix_event_id, email_message_id, thread_root_email_message_id, \
                  subject, origin, smtp_status, smtp_attempts, smtp_payload, created_at) \
                 VALUES (?1, ?2, ?3, ?4, 'matrix', 'pending', 0, ?5, ?6)",
                params![
                    matrix_event_id,
                    our_message_id,
                    thread_root_email_message_id,
                    subject,
                    smtp_payload,
                    now
                ],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking store_matrix_reply_pending")?
    }

    pub async fn mark_route_sent(&self, matrix_event_id: &str) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE message_routes SET smtp_status = 'sent' \
                 WHERE matrix_event_id = ?1",
                params![matrix_event_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking mark_route_sent")?
    }

    /// Increment attempt counter and schedule next retry.
    /// Records with smtp_attempts ≥ 10 are no longer returned by get_failed_smtp_routes.
    pub async fn mark_route_failed(
        &self,
        matrix_event_id: &str,
        next_retry_at: i64,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE message_routes \
                 SET smtp_status = 'failed', \
                     smtp_attempts = smtp_attempts + 1, \
                     smtp_next_retry_at = ?1 \
                 WHERE matrix_event_id = ?2",
                params![next_retry_at, matrix_event_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking mark_route_failed")?
    }

    /// Fetch SMTP routes eligible for (re)delivery: status is 'pending' or 'failed',
    /// attempts < 10, and next_retry_at is either NULL (pending, never attempted)
    /// or already past due. Covers crash-recovery of PENDING records.
    pub async fn get_failed_smtp_routes(&self, limit: usize) -> Result<Vec<SmtpRetryItem>> {
        let conn = Arc::clone(&self.conn);
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT matrix_event_id, smtp_payload, smtp_attempts \
                 FROM message_routes \
                 WHERE origin = 'matrix' \
                   AND (smtp_status = 'pending' OR smtp_status = 'failed') \
                   AND smtp_attempts < 10 \
                   AND (smtp_next_retry_at IS NULL OR smtp_next_retry_at <= ?1) \
                 ORDER BY smtp_next_retry_at ASC \
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![now, limit as i64], |row| {
                Ok(SmtpRetryItem {
                    matrix_event_id: row.get(0)?,
                    smtp_payload: row.get(1)?,
                    attempts: row.get(2)?,
                })
            })?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok::<Vec<SmtpRetryItem>, anyhow::Error>(results)
        })
        .await
        .context("spawn_blocking get_failed_smtp_routes")?
    }

    /// Return the earliest next_retry_at across all retriable routes (attempts < 10).
    /// NULL next_retry_at (pending, never attempted) is coalesced to 0 — treated as
    /// immediately due. Returns None when there are no eligible routes at all.
    pub async fn get_next_smtp_retry_at(&self) -> Result<Option<i64>> {
        let conn = Arc::clone(&self.conn);
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let result = conn.query_row(
                "SELECT MIN(COALESCE(smtp_next_retry_at, 0)) \
                 FROM message_routes \
                 WHERE origin = 'matrix' \
                   AND (smtp_status = 'pending' OR smtp_status = 'failed') \
                   AND smtp_attempts < 10",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )?;
            Ok::<Option<i64>, anyhow::Error>(result)
        })
        .await
        .context("spawn_blocking get_next_smtp_retry_at")?
    }

    /// Returns true only for emails that were successfully delivered by the bridge (smtp_status='sent').
    /// Used to break the email→Matrix echo loop.
    pub async fn is_bridge_sent_email(&self, email_message_id: &str) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let email_message_id = email_message_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM message_routes \
                 WHERE email_message_id = ?1 AND origin = 'matrix' AND smtp_status = 'sent'",
                params![email_message_id],
                |row| row.get(0),
            )?;
            Ok::<bool, anyhow::Error>(count > 0)
        })
        .await
        .context("spawn_blocking is_bridge_sent_email")?
    }

    pub async fn cleanup_old_threads(&self, older_than_secs: i64) -> Result<usize> {
        let conn = Arc::clone(&self.conn);
        let cutoff = chrono::Utc::now().timestamp() - older_than_secs;
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let deleted = conn.execute(
                "DELETE FROM thread_map WHERE created_at < ?1",
                params![cutoff],
            )?;
            Ok::<usize, anyhow::Error>(deleted)
        })
        .await
        .context("spawn_blocking cleanup_old_threads")?
    }
}
