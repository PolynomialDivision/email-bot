use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
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

pub struct ConfirmationMatch {
    pub matrix_event_id: String,
    pub previous_status: String,
}

pub struct DeliveryNotice {
    pub matrix_event_id: String,
    pub room_id: String,
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
            --   smtp_status: 'pending' | 'failed' | 'smtp_accepted' |
            --                'list_confirmed' | 'delivery_unconfirmed'
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
                room_id TEXT,
                smtp_accepted_at INTEGER,
                list_confirmed_at INTEGER,
                delivery_unconfirmed_at INTEGER,
                delivery_notice_claimed_at INTEGER,
                delivery_notice_sent_at INTEGER,
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
            "ALTER TABLE message_routes ADD COLUMN room_id TEXT",
            "ALTER TABLE message_routes ADD COLUMN smtp_accepted_at INTEGER",
            "ALTER TABLE message_routes ADD COLUMN list_confirmed_at INTEGER",
            "ALTER TABLE message_routes ADD COLUMN delivery_unconfirmed_at INTEGER",
            "ALTER TABLE message_routes ADD COLUMN delivery_notice_claimed_at INTEGER",
            "ALTER TABLE message_routes ADD COLUMN delivery_notice_sent_at INTEGER",
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
        room_id: &str,
        our_message_id: &str,
        thread_root_email_message_id: Option<&str>,
        subject: Option<&str>,
        smtp_payload: &str,
    ) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        let room_id = room_id.to_owned();
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
                  subject, origin, smtp_status, smtp_attempts, smtp_payload, room_id, created_at) \
                 VALUES (?1, ?2, ?3, ?4, 'matrix', 'pending', 0, ?5, ?6, ?7)",
                params![
                    matrix_event_id,
                    our_message_id,
                    thread_root_email_message_id,
                    subject,
                    smtp_payload,
                    room_id,
                    now
                ],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking store_matrix_reply_pending")?
    }

    pub async fn mark_route_smtp_accepted(&self, matrix_event_id: &str) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE message_routes \
                 SET smtp_status = 'smtp_accepted', smtp_accepted_at = ?1, smtp_next_retry_at = NULL \
                 WHERE matrix_event_id = ?2 AND smtp_status IN ('pending', 'failed', 'sent')",
                params![now, matrix_event_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking mark_route_smtp_accepted")?
    }

    /// Confirm a mailing-list echo by exact Message-ID, falling back to the
    /// Matrix event correlation header when the list rewrites Message-ID.
    pub async fn confirm_list_delivery(
        &self,
        email_message_id: &str,
        matrix_event_id: Option<&str>,
    ) -> Result<Option<ConfirmationMatch>> {
        let conn = Arc::clone(&self.conn);
        let email_message_id = email_message_id.to_owned();
        let matrix_event_id = matrix_event_id
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let matched = tx
                .query_row(
                    "SELECT matrix_event_id, smtp_status \
                     FROM message_routes \
                     WHERE origin = 'matrix' \
                       AND (email_message_id = ?1 OR (?2 IS NOT NULL AND matrix_event_id = ?2)) \
                       AND smtp_status IN ('pending', 'sent', 'smtp_accepted', 'delivery_unconfirmed') \
                     LIMIT 1",
                    params![email_message_id, matrix_event_id],
                    |row| {
                        Ok(ConfirmationMatch {
                            matrix_event_id: row.get(0)?,
                            previous_status: row.get(1)?,
                        })
                    },
                )
                .optional()?;
            if let Some(ref matched) = matched {
                tx.execute(
                    "UPDATE message_routes \
                     SET smtp_status = 'list_confirmed', list_confirmed_at = ?1, \
                         delivery_notice_claimed_at = NULL \
                     WHERE matrix_event_id = ?2",
                    params![now, matched.matrix_event_id],
                )?;
            }
            tx.commit()?;
            Ok::<Option<ConfirmationMatch>, anyhow::Error>(matched)
        })
        .await
        .context("spawn_blocking confirm_list_delivery")?
    }

    /// Claim one overdue delivery notice. The lease makes this idempotent
    /// across restarts and concurrent bot instances sharing the database.
    pub async fn claim_unconfirmed_delivery(
        &self,
        timeout_secs: i64,
        lease_secs: i64,
    ) -> Result<Option<DeliveryNotice>> {
        let conn = Arc::clone(&self.conn);
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - timeout_secs;
        let stale_claim = now - lease_secs;
        spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let candidate = tx
                .query_row(
                    "SELECT matrix_event_id, room_id \
                     FROM message_routes \
                     WHERE origin = 'matrix' AND room_id IS NOT NULL \
                       AND delivery_notice_sent_at IS NULL \
                       AND (delivery_notice_claimed_at IS NULL OR delivery_notice_claimed_at <= ?1) \
                       AND ((smtp_status = 'smtp_accepted' AND smtp_accepted_at <= ?2) \
                            OR smtp_status = 'delivery_unconfirmed') \
                     ORDER BY COALESCE(smtp_accepted_at, created_at) ASC \
                     LIMIT 1",
                    params![stale_claim, cutoff],
                    |row| {
                        Ok(DeliveryNotice {
                            matrix_event_id: row.get(0)?,
                            room_id: row.get(1)?,
                        })
                    },
                )
                .optional()?;
            if let Some(ref candidate) = candidate {
                tx.execute(
                    "UPDATE message_routes \
                     SET smtp_status = 'delivery_unconfirmed', \
                         delivery_unconfirmed_at = COALESCE(delivery_unconfirmed_at, ?1), \
                         delivery_notice_claimed_at = ?1 \
                     WHERE matrix_event_id = ?2",
                    params![now, candidate.matrix_event_id],
                )?;
            }
            tx.commit()?;
            Ok::<Option<DeliveryNotice>, anyhow::Error>(candidate)
        })
        .await
        .context("spawn_blocking claim_unconfirmed_delivery")?
    }

    pub async fn mark_delivery_notice_sent(&self, matrix_event_id: &str) -> Result<()> {
        self.finish_delivery_notice(matrix_event_id, true).await
    }

    pub async fn delivery_notice_is_active(&self, matrix_event_id: &str) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM message_routes \
                 WHERE matrix_event_id = ?1 AND smtp_status = 'delivery_unconfirmed' \
                   AND delivery_notice_claimed_at IS NOT NULL \
                   AND delivery_notice_sent_at IS NULL",
                params![matrix_event_id],
                |row| row.get(0),
            )?;
            Ok::<bool, anyhow::Error>(count > 0)
        })
        .await
        .context("spawn_blocking delivery_notice_is_active")?
    }

    pub async fn release_delivery_notice(&self, matrix_event_id: &str) -> Result<()> {
        self.finish_delivery_notice(matrix_event_id, false).await
    }

    async fn finish_delivery_notice(&self, matrix_event_id: &str, sent: bool) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        let now = chrono::Utc::now().timestamp();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            if sent {
                conn.execute(
                    "UPDATE message_routes \
                     SET delivery_notice_sent_at = ?1, delivery_notice_claimed_at = NULL \
                     WHERE matrix_event_id = ?2 AND smtp_status = 'delivery_unconfirmed'",
                    params![now, matrix_event_id],
                )?;
            } else {
                conn.execute(
                    "UPDATE message_routes SET delivery_notice_claimed_at = NULL \
                     WHERE matrix_event_id = ?1 AND smtp_status = 'delivery_unconfirmed'",
                    params![matrix_event_id],
                )?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("spawn_blocking finish_delivery_notice")?
    }

    /// Increment attempt counter and schedule next retry.
    /// Records with smtp_attempts ≥ 10 are no longer returned by get_failed_smtp_routes.
    pub async fn mark_route_failed(&self, matrix_event_id: &str, next_retry_at: i64) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let matrix_event_id = matrix_event_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "UPDATE message_routes \
                 SET smtp_status = 'failed', \
                     smtp_attempts = smtp_attempts + 1, \
                     smtp_next_retry_at = ?1 \
                 WHERE matrix_event_id = ?2 AND smtp_status IN ('pending', 'failed')",
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

    /// Returns true for emails accepted by SMTP, including confirmed and
    /// unconfirmed list-delivery states. Used to break the echo loop.
    pub async fn is_bridge_sent_email(&self, email_message_id: &str) -> Result<bool> {
        let conn = Arc::clone(&self.conn);
        let email_message_id = email_message_id.to_owned();
        spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM message_routes \
                 WHERE email_message_id = ?1 AND origin = 'matrix' \
                   AND smtp_status IN ('sent', 'smtp_accepted', 'list_confirmed', 'delivery_unconfirmed')",
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::Db;

    fn test_db() -> (TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("email-bot.sqlite3")).unwrap();
        (dir, db)
    }

    async fn accepted_route(db: &Db, event_id: &str, message_id: &str) {
        db.store_matrix_reply_pending(
            event_id,
            "!room:example.org",
            message_id,
            Some(message_id),
            Some("subject"),
            "{}",
        )
        .await
        .unwrap();
        db.mark_route_smtp_accepted(event_id).await.unwrap();
    }

    fn make_overdue(db: &Db, event_id: &str) {
        let conn = db.conn.lock().unwrap();
        conn.execute(
            "UPDATE message_routes SET smtp_accepted_at = ?1 WHERE matrix_event_id = ?2",
            rusqlite::params![chrono::Utc::now().timestamp() - 7200, event_id],
        )
        .unwrap();
    }

    fn route_state(db: &Db, event_id: &str) -> (String, i64, String) {
        let conn = db.conn.lock().unwrap();
        conn.query_row(
            "SELECT smtp_status, smtp_attempts, email_message_id \
             FROM message_routes WHERE matrix_event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn list_copy_received_before_timeout() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "message@example.org").await;

        let matched = db
            .confirm_list_delivery("message@example.org", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(matched.matrix_event_id, "$event");
        assert_eq!(matched.previous_status, "smtp_accepted");
        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delayed_list_copy_received_before_notice_claim() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "message@example.org").await;
        make_overdue(&db, "$event");

        db.confirm_list_delivery("message@example.org", None)
            .await
            .unwrap();
        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn fast_list_copy_is_not_overwritten_by_smtp_acceptance_update() {
        let (_dir, db) = test_db();
        db.store_matrix_reply_pending(
            "$event",
            "!room:example.org",
            "message@example.org",
            None,
            Some("subject"),
            "{}",
        )
        .await
        .unwrap();

        db.confirm_list_delivery("message@example.org", Some("$event"))
            .await
            .unwrap();
        db.mark_route_smtp_accepted("$event").await.unwrap();

        assert_eq!(route_state(&db, "$event").0, "list_confirmed");
    }

    #[tokio::test]
    async fn list_copy_after_claim_suppresses_notice() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "message@example.org").await;
        make_overdue(&db, "$event");

        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_some());
        db.confirm_list_delivery("message@example.org", None)
            .await
            .unwrap();
        assert!(!db.delivery_notice_is_active("$event").await.unwrap());
    }

    #[tokio::test]
    async fn duplicate_scheduler_execution_claims_once() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "message@example.org").await;
        make_overdue(&db, "$event");

        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_some());
        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_none());
        db.mark_delivery_notice_sent("$event").await.unwrap();
        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn waiting_period_survives_restart() {
        let (dir, db) = test_db();
        let path = dir.path().join("email-bot.sqlite3");
        accepted_route(&db, "$event", "message@example.org").await;
        make_overdue(&db, "$event");
        drop(db);

        let reopened = Db::open(path).unwrap();
        assert!(reopened
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn rewritten_message_id_uses_matrix_event_header() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "original@example.org").await;

        let matched = db
            .confirm_list_delivery("rewritten@list.example", Some("$event"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(matched.matrix_event_id, "$event");
        assert_eq!(route_state(&db, "$event").0, "list_confirmed");
    }

    #[tokio::test]
    async fn stripped_custom_header_falls_back_to_message_id() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "original@example.org").await;

        assert!(db
            .confirm_list_delivery("original@example.org", None)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn no_correlation_marks_unconfirmed_without_resending() {
        let (_dir, db) = test_db();
        accepted_route(&db, "$event", "original@example.org").await;
        make_overdue(&db, "$event");

        assert!(db
            .confirm_list_delivery("rewritten@list.example", None)
            .await
            .unwrap()
            .is_none());
        assert!(db
            .claim_unconfirmed_delivery(3600, 300)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            route_state(&db, "$event"),
            (
                "delivery_unconfirmed".to_owned(),
                0,
                "original@example.org".to_owned()
            )
        );
    }
}
