use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use mxbot_common::supervise::Alive;
use native_tls::TlsConnector as NativeTlsConnector;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_native_tls::TlsConnector;
use tracing::{debug, error, info, warn};

use crate::config::ImapConfig;
use crate::db::Db;
use crate::email::RawEmail;

/// What the send loop did with a fetched email.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// Posted, put into the retry queue, or deliberately not posted (not from the list, a
    /// bridge echo, unparseable): the UID may be marked seen.
    Done,
    /// Neither posted nor queued: the UID stays unseen and is fetched again next cycle.
    Again,
}

/// A fetched email on its way to the send loop, with the channel for its outcome.
pub struct Fetched {
    pub raw: RawEmail,
    pub handled: oneshot::Sender<Handled>,
}

pub async fn imap_loop(
    config: ImapConfig,
    imap_password: String,
    tx: mpsc::Sender<Fetched>,
    db: Db,
    alive: Alive,
) {
    let mut reconnect_delay = Duration::from_secs(5);
    let mut attempt = 0u32;
    // A poll cycle ends with ok or failed (below) every poll interval.
    alive.expect_every(Duration::from_secs(config.effective_poll_interval_secs()));

    loop {
        alive.tick();
        attempt += 1;
        info!(
            attempt = attempt,
            host = %config.host,
            port = config.port,
            reconnect_delay_secs = reconnect_delay.as_secs(),
            "IMAP: connecting"
        );
        match connect_and_sync(&config, &imap_password, &tx, &db, &alive).await {
            Ok(()) => {
                // Normally connect_and_sync loops forever; Ok(()) means a clean exit.
                info!(attempt = attempt, "IMAP: sync loop exited cleanly");
                reconnect_delay = Duration::from_secs(5);
                attempt = 0;
            }
            Err(e) => {
                error!(
                    attempt = attempt,
                    host = %config.host,
                    port = config.port,
                    reconnect_delay_secs = reconnect_delay.as_secs(),
                    error = %e,
                    error_chain = ?e,
                    "IMAP: connection/sync failed, will reconnect in {}s", reconnect_delay.as_secs()
                );
                // A failure that keeps coming back (a wrong password) turns the container
                // unhealthy after the loop's time; a short outage does not.
                alive.failed(health_reason(&e, &imap_password));
                sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(300));
            }
        }
    }
}

async fn connect_and_sync(
    config: &ImapConfig,
    password: &str,
    tx: &mpsc::Sender<Fetched>,
    db: &Db,
    alive: &Alive,
) -> Result<()> {
    let addr = format!("{}:{}", config.host, config.port);

    // TCP connect
    debug!(addr = %addr, "IMAP: opening TCP connection");
    let t_tcp = Instant::now();
    let tcp = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("IMAP TCP connect to {} failed", addr))?;
    info!(
        addr = %addr,
        elapsed_ms = t_tcp.elapsed().as_millis(),
        "IMAP: TCP connection established"
    );

    // TLS handshake: IMAPS uses implicit TLS (no STARTTLS negotiation)
    debug!(
        host = %config.host,
        tls_mode = "IMAPS (implicit TLS)",
        "IMAP: initiating TLS handshake"
    );
    let t_tls = Instant::now();
    let tls_connector =
        NativeTlsConnector::new().context("IMAP: failed to create native TLS connector")?;
    let tls_connector = TlsConnector::from(tls_connector);
    let tls_stream = tls_connector
        .connect(&config.host, tcp)
        .await
        .with_context(|| {
            format!(
                "IMAP TLS handshake failed with {}: check server certificate and hostname",
                config.host
            )
        })?;
    info!(
        host = %config.host,
        tls_mode = "IMAPS (implicit TLS)",
        elapsed_ms = t_tls.elapsed().as_millis(),
        "IMAP: TLS handshake successful"
    );

    // IMAP login
    let client = async_imap::Client::new(tls_stream);
    info!(
        username = %config.username,
        host = %config.host,
        "IMAP: authenticating (LOGIN)"
    );
    let t_login = Instant::now();
    let mut session = client
        .login(&config.username, password)
        .await
        .map_err(|(e, _)| {
            // The server's answer stays the error's source: `health_reason` shows only its
            // short NO/BAD text.
            anyhow::Error::new(e).context(format!(
                "IMAP LOGIN failed for user '{}' on {} (check IMAP username and IMAP_PASSWORD)",
                config.username, config.host
            ))
        })?;
    info!(
        username = %config.username,
        host = %config.host,
        elapsed_ms = t_login.elapsed().as_millis(),
        "IMAP: login successful"
    );

    let poll_secs = config.effective_poll_interval_secs();
    let mailbox = config.effective_mailbox();
    info!(
        mailbox = %mailbox,
        poll_interval_secs = poll_secs,
        "IMAP: starting poll loop"
    );

    let mut poll_count = 0u64;
    loop {
        alive.tick();
        poll_count += 1;
        debug!(poll = poll_count, mailbox = %mailbox, "IMAP: poll cycle start");
        let t_poll = Instant::now();
        match sync_once(config, &mut session, tx, db, alive).await? {
            None => alive.ok(),
            Some(e) => alive.failed(health_reason(&e, password)),
        }
        debug!(
            poll = poll_count,
            mailbox = %mailbox,
            elapsed_ms = t_poll.elapsed().as_millis(),
            "IMAP: poll cycle complete, sleeping {}s", poll_secs
        );
        sleep(Duration::from_secs(poll_secs)).await;
    }
}

/// One poll cycle. `Ok(Some(error))`: the cycle went on, but a message could not be fetched,
/// or was fetched but neither posted nor queued for a retry (the last such error); it is
/// fetched again next cycle, and the cycle counts as failed for the health check.
///
/// A message that fails like this every time keeps the container unhealthy, and `last_uid`
/// stays before it, by design: it is not posted, and nothing skips it silently. The reason
/// names its UID and the step. What an admin does: for a failed fetch, look at that UID on
/// the IMAP server and move it out of the mailbox (to another folder) with a mail client;
/// the next cycle no longer finds it, gets past it and turns healthy again. (Marking it
/// \Seen in the mail client does not help: the bot keeps its own seen list.) For "neither
/// posted nor queued", fix the bot's database first (disk full, read-only volume: see the
/// log); the message then goes out by itself.
async fn sync_once(
    config: &ImapConfig,
    session: &mut async_imap::Session<tokio_native_tls::TlsStream<TcpStream>>,
    tx: &mpsc::Sender<Fetched>,
    db: &Db,
    alive: &Alive,
) -> Result<Option<anyhow::Error>> {
    let mailbox = config.effective_mailbox();

    debug!(mailbox = %mailbox, "IMAP: SELECT mailbox");
    let t_select = Instant::now();
    let mailbox_status = session
        .select(mailbox)
        .await
        .with_context(|| format!("IMAP SELECT {} failed", mailbox))?;
    info!(
        mailbox = %mailbox,
        exists = mailbox_status.exists,
        recent = mailbox_status.recent,
        unseen = ?mailbox_status.unseen,
        elapsed_ms = t_select.elapsed().as_millis(),
        "IMAP: mailbox selected"
    );

    let last_uid = db
        .get_last_uid(mailbox)
        .await
        .context("IMAP: get_last_uid from DB failed")?;
    debug!(mailbox = %mailbox, last_uid = last_uid, "IMAP: last seen UID from DB");

    let uids_to_fetch: Vec<u32> = if last_uid == 0 {
        if config.effective_skip_initial_history() {
            info!(
                mailbox = %mailbox,
                "IMAP: first run, skip_initial_history=true, marking all existing messages as seen without fetching"
            );
            let t = Instant::now();
            let all_uids = fetch_all_uids(session).await?;
            info!(
                mailbox = %mailbox,
                uid_count = all_uids.len(),
                elapsed_ms = t.elapsed().as_millis(),
                "IMAP: UID SEARCH ALL complete"
            );
            for &uid in &all_uids {
                db.mark_uid_seen(mailbox, uid, None)
                    .await
                    .context("IMAP: mark_uid_seen during initial skip failed")?;
            }
            if let Some(&max_uid) = all_uids.iter().max() {
                db.set_last_uid(mailbox, max_uid)
                    .await
                    .context("IMAP: set_last_uid during initial skip failed")?;
                info!(
                    mailbox = %mailbox,
                    uid_count = all_uids.len(),
                    max_uid = max_uid,
                    "IMAP: initial skip complete, marked all existing messages as seen"
                );
            } else {
                info!(mailbox = %mailbox, "IMAP: mailbox empty on first run");
            }
            return Ok(None);
        } else {
            let limit = config.effective_initial_fetch_limit();
            info!(
                mailbox = %mailbox,
                fetch_limit = limit,
                "IMAP: first run, skip_initial_history=false, fetching last {} messages", limit
            );
            let t = Instant::now();
            let all_uids = fetch_all_uids(session).await?;
            info!(
                mailbox = %mailbox,
                total_uids = all_uids.len(),
                fetch_limit = limit,
                elapsed_ms = t.elapsed().as_millis(),
                "IMAP: UID SEARCH ALL complete"
            );
            if all_uids.len() > limit {
                all_uids[all_uids.len() - limit..].to_vec()
            } else {
                all_uids
            }
        }
    } else {
        let since = last_uid + 1;
        debug!(
            mailbox = %mailbox,
            since_uid = since,
            "IMAP: UID SEARCH for new messages since {}", since
        );
        let t = Instant::now();
        let uids = fetch_uids_since(session, last_uid).await?;
        debug!(
            mailbox = %mailbox,
            new_uid_count = uids.len(),
            elapsed_ms = t.elapsed().as_millis(),
            "IMAP: UID SEARCH complete"
        );
        uids
    };

    if uids_to_fetch.is_empty() {
        debug!(mailbox = %mailbox, "IMAP: no new messages");
        return Ok(None);
    }

    info!(
        mailbox = %mailbox,
        count = uids_to_fetch.len(),
        uids = ?uids_to_fetch,
        "IMAP: fetching new messages"
    );

    let mut sorted_uids = uids_to_fetch;
    sorted_uids.sort_unstable();

    let mut cursor = UidCursor::default();
    let mut fetch_failed = None;

    for uid in sorted_uids {
        if db.uid_seen(mailbox, uid).await.unwrap_or(false) {
            debug!(uid = uid, mailbox = %mailbox, "IMAP: UID already seen, skipping");
            // Seen after a UID that failed earlier: last_uid catches up once that is done.
            if let Some(uid) = cursor.done(uid) {
                db.set_last_uid(mailbox, uid)
                    .await
                    .context("IMAP: set_last_uid for a seen UID failed")?;
            }
            continue;
        }

        debug!(uid = uid, mailbox = %mailbox, "IMAP: fetching RFC822 body");
        let t_fetch = Instant::now();
        match fetch_raw_email(session, uid).await {
            Ok(Some(raw_bytes)) => {
                let byte_count = raw_bytes.len();
                info!(
                    uid = uid,
                    mailbox = %mailbox,
                    bytes = byte_count,
                    elapsed_ms = t_fetch.elapsed().as_millis(),
                    "IMAP: message fetched, queuing for processing"
                );
                let raw = RawEmail {
                    uid,
                    mailbox: mailbox.to_owned(),
                    raw: raw_bytes,
                };

                // Seen only once it was posted or queued for a retry: a restart (or a failed
                // post that could not be queued) before that fetches it again. Reposting it is
                // harmless, the post's transaction IDs come from the email.
                let Some(handled) = hand_over(tx, raw).await else {
                    warn!(
                        uid = uid,
                        mailbox = %mailbox,
                        "IMAP: processing channel closed, matrix_send task may have exited"
                    );
                    return Ok(fetch_failed);
                };
                alive.tick();
                if let Some(e) = record_handled(db, mailbox, uid, handled, &mut cursor).await? {
                    fetch_failed = Some(e);
                }
            }
            Ok(None) => {
                warn!(
                    uid = uid,
                    mailbox = %mailbox,
                    "IMAP: UID not found in fetch response (message may have been expunged)"
                );
            }
            Err(e) => {
                warn!(
                    uid = uid,
                    mailbox = %mailbox,
                    error = %e,
                    error_chain = ?e,
                    "IMAP: failed to fetch message body, retrying it next cycle"
                );
                // Continue with next UID rather than aborting the whole sync.
                cursor.failed(uid);
                fetch_failed = Some(e);
            }
        }
    }

    Ok(fetch_failed)
}

/// What the health check shows for a failed IMAP step: our own description of the step,
/// and from the cause only its kind and the server's short NO/BAD text. Never the password
/// (cut out should a server echo it) and never mail content (an unreadable answer is named,
/// not shown).
fn health_reason(error: &anyhow::Error, password: &str) -> String {
    let mut reason = error.to_string();
    if let Some(detail) = error.chain().find_map(cause_detail) {
        reason = format!("{reason}: {detail}");
    }
    if !password.is_empty() {
        reason = reason.replace(password, "***");
    }
    let mut reason = reason.lines().next().unwrap_or_default().trim().to_owned();
    if reason.chars().count() > 200 {
        reason = reason.chars().take(199).collect::<String>() + "…";
    }
    reason
}

fn cause_detail(cause: &(dyn std::error::Error + 'static)) -> Option<String> {
    use async_imap::error::Error as Imap;
    if let Some(e) = cause.downcast_ref::<Imap>() {
        return Some(match e {
            Imap::No(text) => format!("NO {}", text.trim()),
            Imap::Bad(text) => format!("BAD {}", text.trim()),
            Imap::Io(io) => io.kind().to_string(),
            Imap::ConnectionLost => "connection lost".to_owned(),
            _ => "unreadable answer from the server".to_owned(),
        });
    }
    if let Some(io) = cause.downcast_ref::<std::io::Error>() {
        return Some(io.kind().to_string());
    }
    // Certificate or handshake problems, in the TLS library's words.
    cause
        .downcast_ref::<native_tls::Error>()
        .map(|e| e.to_string())
}

/// Gives a fetched email to the send loop and waits until it was posted or queued.
/// `None` when the send loop is gone.
async fn hand_over(tx: &mpsc::Sender<Fetched>, raw: RawEmail) -> Option<Handled> {
    let (handled, outcome) = oneshot::channel();
    tx.send(Fetched { raw, handled }).await.ok()?;
    outcome.await.ok()
}

/// Marks a handed-over UID seen (and moves `last_uid`) when it is done; otherwise leaves it
/// for the next cycle, like a failed fetch, and returns the failure the cycle reports to the
/// health check (UID and step only, nothing from the email).
async fn record_handled(
    db: &Db,
    mailbox: &str,
    uid: u32,
    handled: Handled,
    cursor: &mut UidCursor,
) -> Result<Option<anyhow::Error>> {
    match handled {
        Handled::Done => {
            debug!(uid = uid, mailbox = %mailbox, "IMAP: message handled, marking it seen");
            db.mark_uid_seen(mailbox, uid, None)
                .await
                .context("IMAP: mark_uid_seen after posting failed")?;
            if let Some(uid) = cursor.done(uid) {
                db.set_last_uid(mailbox, uid)
                    .await
                    .context("IMAP: set_last_uid after posting failed")?;
            }
        }
        Handled::Again => {
            warn!(
                uid = uid,
                mailbox = %mailbox,
                "IMAP: message neither posted nor queued, fetching it again next cycle"
            );
            cursor.failed(uid);
            return Ok(Some(anyhow::anyhow!(
                "IMAP UID {uid} in {mailbox}: fetched, but neither posted to Matrix nor put \
                 into the retry queue (check the bot's database); fetching it again next cycle"
            )));
        }
    }
    Ok(None)
}

/// How far `last_uid` may move in one sync cycle (UIDs in ascending order): up to the first
/// UID whose fetch failed, so the next cycle's search finds that one again. (It used to move
/// past it with the next message and the failed one was never fetched.) The messages after
/// it are marked seen, so they are not posted twice.
#[derive(Default)]
struct UidCursor {
    first_failed: Option<u32>,
}

impl UidCursor {
    /// `uid` is done (posted or seen before): the `last_uid` to store, if it may move.
    fn done(&self, uid: u32) -> Option<u32> {
        self.first_failed.is_none().then_some(uid)
    }

    fn failed(&mut self, uid: u32) {
        self.first_failed.get_or_insert(uid);
    }
}

async fn fetch_all_uids(
    session: &mut async_imap::Session<tokio_native_tls::TlsStream<TcpStream>>,
) -> Result<Vec<u32>> {
    debug!("IMAP: UID SEARCH ALL");
    let uids = session
        .uid_search("ALL")
        .await
        .context("IMAP UID SEARCH ALL failed")?;
    let mut result: Vec<u32> = uids.into_iter().collect();
    result.sort_unstable();
    debug!(
        uid_count = result.len(),
        "IMAP: UID SEARCH ALL returned {} UIDs",
        result.len()
    );
    Ok(result)
}

async fn fetch_uids_since(
    session: &mut async_imap::Session<tokio_native_tls::TlsStream<TcpStream>>,
    last_uid: u32,
) -> Result<Vec<u32>> {
    let query = format!("UID {}:*", last_uid + 1);
    debug!(query = %query, "IMAP: UID SEARCH for new messages");
    let uids = session
        .uid_search(&query)
        .await
        .context("IMAP UID SEARCH since last failed")?;
    let result: Vec<u32> = uids.into_iter().filter(|&uid| uid > last_uid).collect();
    debug!(
        uid_count = result.len(),
        last_uid = last_uid,
        "IMAP: UID SEARCH returned {} new UIDs",
        result.len()
    );
    Ok(result)
}

async fn fetch_raw_email(
    session: &mut async_imap::Session<tokio_native_tls::TlsStream<TcpStream>>,
    uid: u32,
) -> Result<Option<Vec<u8>>> {
    let uid_str = uid.to_string();
    debug!(uid = uid, "IMAP: UID FETCH RFC822");
    let stream = session
        .uid_fetch(&uid_str, "RFC822")
        .await
        .with_context(|| format!("IMAP UID FETCH {} RFC822 failed", uid))?;

    tokio::pin!(stream);

    while let Some(result) = stream.next().await {
        let fetch = result.with_context(|| format!("IMAP fetch stream error for UID {}", uid))?;
        if let Some(body) = fetch.body() {
            debug!(uid = uid, bytes = body.len(), "IMAP: RFC822 body received");
            return Ok(Some(body.to_vec()));
        }
    }

    debug!(uid = uid, "IMAP: fetch stream ended with no body");
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::{hand_over, health_reason, record_handled, Fetched, Handled, UidCursor};
    use crate::db::Db;
    use crate::email::RawEmail;
    use tokio::sync::mpsc;

    fn raw(uid: u32) -> RawEmail {
        RawEmail {
            uid,
            mailbox: "INBOX".to_owned(),
            raw: b"Subject: x\r\n\r\nbody".to_vec(),
        }
    }

    /// A send loop that answers every email with `outcome` (None: drops it unanswered).
    fn send_loop(outcome: Option<Handled>) -> mpsc::Sender<Fetched> {
        let (tx, mut rx) = mpsc::channel::<Fetched>(4);
        tokio::spawn(async move {
            while let Some(Fetched { handled, .. }) = rx.recv().await {
                if let Some(outcome) = outcome {
                    let _ = handled.send(outcome);
                }
            }
        });
        tx
    }

    #[tokio::test]
    async fn uid_is_seen_only_after_it_was_handled() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("email-bot.sqlite3")).unwrap();
        let mut cursor = UidCursor::default();

        // Posted (or queued): seen, last_uid moves.
        let handled = hand_over(&send_loop(Some(Handled::Done)), raw(10)).await;
        assert_eq!(handled, Some(Handled::Done));
        let failure = record_handled(&db, "INBOX", 10, Handled::Done, &mut cursor)
            .await
            .unwrap();
        assert!(failure.is_none());
        assert!(db.uid_seen("INBOX", 10).await.unwrap());
        assert_eq!(db.get_last_uid("INBOX").await.unwrap(), 10);

        // Neither posted nor queued: not seen, last_uid stays before it, so the next
        // cycle's search (UID 11:*) finds it again; later UIDs no longer move last_uid.
        let handled = hand_over(&send_loop(Some(Handled::Again)), raw(11)).await;
        assert_eq!(handled, Some(Handled::Again));
        let failure = record_handled(&db, "INBOX", 11, Handled::Again, &mut cursor)
            .await
            .unwrap();
        assert!(failure.is_some());
        assert!(!db.uid_seen("INBOX", 11).await.unwrap());
        let failure = record_handled(&db, "INBOX", 12, Handled::Done, &mut cursor)
            .await
            .unwrap();
        assert!(failure.is_none());
        assert!(db.uid_seen("INBOX", 12).await.unwrap());
        assert_eq!(db.get_last_uid("INBOX").await.unwrap(), 10);
    }

    #[tokio::test]
    async fn an_email_neither_posted_nor_queued_fails_the_cycle() {
        // A broken retry queue used to leave the poll cycle ok: the container stayed healthy
        // while the email was fetched again and again.
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("email-bot.sqlite3")).unwrap();
        let mut cursor = UidCursor::default();
        let failure = record_handled(&db, "Lists/house", 42, Handled::Again, &mut cursor)
            .await
            .unwrap()
            .expect("Again fails the cycle");
        let reason = health_reason(&failure, "hunter2");
        // Names the UID, the mailbox and the step, so an admin can find it; nothing from the
        // email itself (record_handled never sees it).
        assert!(
            reason.starts_with("IMAP UID 42 in Lists/house: "),
            "{reason}"
        );
        assert!(reason.contains("retry queue"), "{reason}");
        assert!(!reason.contains("Subject"), "{reason}");
        assert!(reason.chars().count() <= 200, "{reason}");
        assert!(!db.uid_seen("Lists/house", 42).await.unwrap());
    }

    #[tokio::test]
    async fn email_dropped_by_the_send_loop_is_not_handled() {
        // The send loop ended mid-way (or never answered): no outcome, nothing marked.
        assert_eq!(hand_over(&send_loop(None), raw(7)).await, None);
        let (tx, rx) = mpsc::channel::<Fetched>(1);
        drop(rx);
        assert_eq!(hand_over(&tx, raw(8)).await, None);
    }

    #[test]
    fn last_uid_stops_before_a_failed_fetch() {
        let mut cursor = UidCursor::default();
        assert_eq!(cursor.done(10), Some(10));
        cursor.failed(11);
        assert_eq!(cursor.done(12), None);
        cursor.failed(13);
        assert_eq!(cursor.first_failed, Some(11));
        // next cycle: 11 succeeds, 12 is seen already, last_uid catches up
        let next = UidCursor::default();
        assert_eq!(next.done(11), Some(11));
        assert_eq!(next.done(12), Some(12));
    }

    #[test]
    fn the_health_reason_names_the_step_and_the_servers_answer() {
        let refused = anyhow::Error::new(async_imap::error::Error::No(
            "[AUTHENTICATIONFAILED] Invalid credentials (Failure)".into(),
        ))
        .context("IMAP LOGIN failed for user 'list' on imap.example.org (check IMAP username and IMAP_PASSWORD)");
        assert_eq!(
            health_reason(&refused, "hunter2"),
            "IMAP LOGIN failed for user 'list' on imap.example.org (check IMAP username and \
             IMAP_PASSWORD): NO [AUTHENTICATIONFAILED] Invalid credentials (Failure)"
        );

        let io = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        let unreachable =
            anyhow::Error::new(io).context("IMAP TCP connect to imap.example.org:993 failed");
        assert_eq!(
            health_reason(&unreachable, "hunter2"),
            "IMAP TCP connect to imap.example.org:993 failed: connection refused"
        );
    }

    #[test]
    fn the_health_reason_never_shows_the_password_or_mail_content() {
        let echoed = anyhow::Error::new(async_imap::error::Error::Bad(
            "Unknown command LOGIN list hunter2\r\nsecond line".into(),
        ))
        .context("IMAP LOGIN failed for user 'list' on imap.example.org");
        let reason = health_reason(&echoed, "hunter2");
        assert!(!reason.contains("hunter2"), "{reason}");
        assert!(!reason.contains("second line"), "{reason}");

        let mail = b"Subject: private\r\n\r\nthe body \xff".to_vec();
        let utf8 = std::str::from_utf8(&mail).unwrap_err();
        let unreadable = anyhow::Error::new(async_imap::error::Error::Parse(
            async_imap::error::ParseError::DataNotUtf8(mail, utf8),
        ))
        .context("IMAP fetch stream error for UID 7");
        assert_eq!(
            health_reason(&unreadable, "hunter2"),
            "IMAP fetch stream error for UID 7: unreadable answer from the server"
        );

        let long = anyhow::Error::new(async_imap::error::Error::No("x".repeat(500)))
            .context("IMAP SELECT INBOX failed");
        assert_eq!(health_reason(&long, "").chars().count(), 200);
    }
}
