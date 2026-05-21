use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use native_tls::TlsConnector as NativeTlsConnector;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_native_tls::TlsConnector;
use tracing::{debug, error, info, warn};

use crate::config::ImapConfig;
use crate::db::Db;
use crate::email::RawEmail;

pub async fn imap_loop(
    config: ImapConfig,
    imap_password: String,
    tx: mpsc::Sender<RawEmail>,
    db: Db,
) {
    let mut reconnect_delay = Duration::from_secs(5);
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        info!(
            attempt = attempt,
            host = %config.host,
            port = config.port,
            reconnect_delay_secs = reconnect_delay.as_secs(),
            "IMAP: connecting"
        );
        match connect_and_sync(&config, &imap_password, &tx, &db).await {
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
                    "IMAP: connection/sync failed — will reconnect in {}s", reconnect_delay.as_secs()
                );
                sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(300));
            }
        }
    }
}

async fn connect_and_sync(
    config: &ImapConfig,
    password: &str,
    tx: &mpsc::Sender<RawEmail>,
    db: &Db,
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

    // TLS handshake — IMAPS uses implicit TLS (no STARTTLS negotiation)
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
                "IMAP TLS handshake failed with {} — check server certificate and hostname",
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
            anyhow::anyhow!(
                "IMAP LOGIN failed for user '{}' on {}: {} — check IMAP username and IMAP_PASSWORD",
                config.username,
                config.host,
                e
            )
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
        poll_count += 1;
        debug!(poll = poll_count, mailbox = %mailbox, "IMAP: poll cycle start");
        let t_poll = Instant::now();
        sync_once(config, &mut session, tx, db).await?;
        debug!(
            poll = poll_count,
            mailbox = %mailbox,
            elapsed_ms = t_poll.elapsed().as_millis(),
            "IMAP: poll cycle complete — sleeping {}s", poll_secs
        );
        sleep(Duration::from_secs(poll_secs)).await;
    }
}

async fn sync_once(
    config: &ImapConfig,
    session: &mut async_imap::Session<tokio_native_tls::TlsStream<TcpStream>>,
    tx: &mpsc::Sender<RawEmail>,
    db: &Db,
) -> Result<()> {
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
                "IMAP: first run, skip_initial_history=true — marking all existing messages as seen without fetching"
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
                    "IMAP: initial skip complete — marked all existing messages as seen"
                );
            } else {
                info!(mailbox = %mailbox, "IMAP: mailbox empty on first run");
            }
            return Ok(());
        } else {
            let limit = config.effective_initial_fetch_limit();
            info!(
                mailbox = %mailbox,
                fetch_limit = limit,
                "IMAP: first run, skip_initial_history=false — fetching last {} messages", limit
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
        return Ok(());
    }

    info!(
        mailbox = %mailbox,
        count = uids_to_fetch.len(),
        uids = ?uids_to_fetch,
        "IMAP: fetching new messages"
    );

    let mut sorted_uids = uids_to_fetch;
    sorted_uids.sort_unstable();

    for uid in sorted_uids {
        if db.uid_seen(mailbox, uid).await.unwrap_or(false) {
            debug!(uid = uid, mailbox = %mailbox, "IMAP: UID already seen — skipping");
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
                    "IMAP: message fetched — queuing for processing"
                );
                let raw = RawEmail {
                    uid,
                    mailbox: mailbox.to_owned(),
                    raw: raw_bytes,
                };

                if tx.send(raw).await.is_err() {
                    warn!(
                        uid = uid,
                        mailbox = %mailbox,
                        "IMAP: processing channel closed — matrix_send task may have exited"
                    );
                    return Ok(());
                }
                debug!(uid = uid, mailbox = %mailbox, "IMAP: message queued for Matrix posting");

                db.mark_uid_seen(mailbox, uid, None)
                    .await
                    .context("IMAP: mark_uid_seen after fetch failed")?;
                db.set_last_uid(mailbox, uid)
                    .await
                    .context("IMAP: set_last_uid after fetch failed")?;
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
                    "IMAP: failed to fetch message body — skipping UID"
                );
                // Continue with next UID rather than aborting the whole sync.
            }
        }
    }

    Ok(())
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
    debug!(uid_count = result.len(), "IMAP: UID SEARCH ALL returned {} UIDs", result.len());
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
    debug!(uid_count = result.len(), last_uid = last_uid, "IMAP: UID SEARCH returned {} new UIDs", result.len());
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
        let fetch = result
            .with_context(|| format!("IMAP fetch stream error for UID {}", uid))?;
        if let Some(body) = fetch.body() {
            debug!(uid = uid, bytes = body.len(), "IMAP: RFC822 body received");
            return Ok(Some(body.to_vec()));
        }
    }

    debug!(uid = uid, "IMAP: fetch stream ended with no body");
    Ok(None)
}
