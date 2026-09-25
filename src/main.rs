use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
use mxbot_common::{
    admin::Dispatch,
    matrix_sdk::{
        deserialized_responses::EncryptionInfo,
        ruma::{
            events::room::message::{OriginalSyncRoomMessageEvent, RoomMessageEventContent},
            OwnedRoomId, OwnedTransactionId,
        },
        Client, Room, RoomState,
    },
    Bot,
};
use serde_json;
use tokio::{fs, signal, sync::mpsc, time::sleep};
use tracing::{debug, error, info, warn};

mod config;
mod db;
mod email;
mod format;
mod html_clean;
mod imap_sync;
mod matrix_post;
mod matrix_reply;
mod smtp_send;

use config::{parse_allowed_repliers, Config, Secrets};
use db::Db;
use email::{ParsedEmail, RawEmail};

fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "unknown location".to_owned());
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "(non-string panic payload)".to_owned()
        };
        // Use eprintln as fallback in case tracing itself is broken.
        eprintln!("PANIC at {location}: {message}");
        tracing::error!(
            panic.location = %location,
            panic.message = %message,
            "PANIC: task crashed — set RUST_BACKTRACE=1 for stack trace"
        );
    }));
}

fn spawn_task<F>(name: &'static str, fut: F) -> tokio::task::JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        info!(task = name, "Task started");
        fut.await;
        // Background tasks are expected to loop forever; reaching here is abnormal.
        warn!(
            task = name,
            "Task exited — this task should run indefinitely"
        );
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    // Env-filter subscriber: RUST_LOG controls verbosity.
    // Examples:
    //   RUST_LOG=info                          — default, recommended for production
    //   RUST_LOG=debug                         — verbose, includes all internal state
    //   RUST_LOG=email_bot=debug,warn          — debug this crate, quiet external crates
    //   RUST_LOG=email_bot=debug,matrix_sdk=info,warn
    // RUST_LOG overrides the default (email_bot=info, SDK at warn), e.g.
    //   RUST_LOG=email_bot=debug,warn
    mxbot_common::logging::init("email_bot");

    install_panic_hook();

    info!(version = env!("CARGO_PKG_VERSION"), "email-bot starting");
    info!(
        "Runtime debugging:\n  \
         RUST_LOG=debug                  — verbose output from all crates\n  \
         RUST_LOG=email_bot=debug,warn   — debug this bot, quiet external crates\n  \
         RUST_BACKTRACE=1               — print stack traces on panic"
    );

    let config_path = mxbot_common::config::config_path_from_args();
    info!(config_path = %config_path.display(), "Loading config");
    let config: Config = mxbot_common::config::load_toml(&config_path)?;
    info!(config_path = %config_path.display(), "Config loaded successfully");

    let secrets = Secrets::from_env().context("Loading secrets from environment")?;
    debug!(
        imap_password_set = !secrets.imap_password.is_empty(),
        smtp_password_set = secrets.smtp_password.is_some(),
        "Secrets loaded from environment"
    );

    // Init store directory and database
    let store_path = mxbot_common::config::store_path_from_env();
    debug!(store_path = %store_path.display(), "Ensuring store directory exists");
    fs::create_dir_all(&store_path)
        .await
        .with_context(|| format!("Creating store directory {}", store_path.display()))?;

    let db_path = store_path.join("email-bot.sqlite3");
    info!(db_path = %db_path.display(), "Opening SQLite database");
    let db = Db::open(&db_path).context("Opening SQLite database")?;
    info!(db_path = %db_path.display(), "Database opened (WAL mode, migrations applied)");

    // Print startup diagnostics before touching Matrix
    print_startup_diagnostics(&config, &secrets, &db_path);

    let allowed_repliers = parse_allowed_repliers(&config.security)
        .context("Invalid allowed_repliers in [security] config")?;
    info!(strategy = ?config.security.common.encryption_strategy, "Encryption strategy configured");
    if config.security.common.allowed_rooms.is_none() {
        warn!("allowed_rooms = [] — bot will not operate in any room");
    }

    info!(
        homeserver = %config.matrix.homeserver,
        user_id = %config.matrix.user_id,
        device_id = %config.matrix.device_id,
        store_path = %store_path.display(),
        "Building Matrix client"
    );
    let bot = Bot::builder("email-bot", env!("CARGO_PKG_VERSION"))
        .store_path(&store_path)
        .start(&config.matrix, &config.security.common)
        .await
        .context("Matrix client setup failed")?;

    // Clone smtp config so both the reply handler and the retry worker can own a copy.
    let smtp_config_opt = config.smtp.clone();
    let list_confirmation_timeout_secs = smtp_config_opt
        .as_ref()
        .and_then(|smtp| smtp.list_confirmation_timeout_secs)
        .map(|seconds| seconds.max(300));

    let mut reply_state = None;
    if let Some(smtp_config) = config.smtp {
        match secrets.smtp_password {
            Some(ref smtp_password) => {
                let tls_mode = if smtp_config.require_smtps {
                    "SMTPS (implicit TLS, port 465)"
                } else {
                    "STARTTLS (port 587)"
                };
                info!(
                    smtp_host = %smtp_config.host,
                    smtp_port = smtp_config.port,
                    tls_mode = tls_mode,
                    smtp_username = %smtp_config.username,
                    from_address = %smtp_config.from_address,
                    list_address = %smtp_config.list_address,
                    allow_new_threads_from_matrix = smtp_config.allow_new_threads_from_matrix,
                    list_confirmation_timeout_secs = ?list_confirmation_timeout_secs,
                    allowed_replier_count = allowed_repliers.len(),
                    "Matrix→Email reply bridging: enabled"
                );
                if allowed_repliers.is_empty() {
                    info!(
                        "All room members may send email replies (no allowed_repliers restriction)"
                    );
                }
                reply_state = Some(matrix_reply::ReplyState {
                    bot_user_id: bot.user_id.clone(),
                    allowed_repliers,
                    smtp_config,
                    smtp_password: smtp_password.clone(),
                    db: db.clone(),
                    allowed_rooms: config.security.common.allowed_rooms.clone(),
                });
            }
            None => {
                warn!(
                    "[smtp] section is present in config but SMTP_PASSWORD env var is not set \
                     — Matrix→Email reply bridging is DISABLED. \
                     Set SMTP_PASSWORD to enable it."
                );
            }
        }
    } else {
        info!("No [smtp] section in config — Matrix→Email reply bridging disabled");
    }

    // Room messages: admin console first; everything else outside admin
    // direct chats may be a reply to bridge to email.
    bot.client.add_event_handler({
        let bot = bot.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, encryption: Option<EncryptionInfo>| {
            let bot = bot.clone();
            let reply_state = reply_state.clone();
            async move {
                if room.state() == RoomState::Joined
                    && bot.admin.handle(&room, &ev, encryption.as_ref()).await == Dispatch::Handled
                {
                    return;
                }
                if bot.admin.is_admin_dm(&room) {
                    return;
                }
                if let Some(state) = reply_state {
                    matrix_reply::handle_possible_reply(state, room, ev).await;
                }
            }
        }
    });

    // Initial sync to populate room state before processing emails (also
    // processes invites received while offline).
    info!("Performing initial Matrix sync (lazy loading)...");
    let t_sync0 = std::time::Instant::now();
    bot.initial_sync().await;
    info!(
        elapsed_ms = t_sync0.elapsed().as_millis(),
        "Initial Matrix sync complete"
    );

    {
        let rooms = bot.client.joined_rooms();
        info!(room_count = rooms.len(), "Joined rooms after initial sync");
        for room in &rooms {
            debug!(
                room_id = %room.room_id(),
                name = ?room.name(),
                "Joined room"
            );
        }
        if rooms.is_empty() {
            warn!(
                "Bot is not in any rooms — emails will be fetched but cannot be posted. \
                 Invite the bot to a Matrix room."
            );
        }
        if !config.security.common.allowed_rooms.is_all() {
            for room in &rooms {
                if !config.security.common.allowed_rooms.allows(room.room_id()) {
                    warn!(
                        room_id = %room.room_id(),
                        name = ?room.name(),
                        "Bot is joined to a room not in allowed_rooms — consider leaving it"
                    );
                }
            }
        }
    }

    // IMAP → parse → post channel
    let (tx, rx) = mpsc::channel::<RawEmail>(100);
    debug!("Email processing channel created (capacity=100)");

    let imap_config = config.imap;
    let imap_password = secrets.imap_password;
    let mailing_list_config = config.mailing_list;
    let limits_config = config.limits;

    let db_imap = db.clone();
    let _imap_handle = spawn_task("imap_sync", async move {
        imap_sync::imap_loop(imap_config, imap_password, tx, db_imap).await;
    });

    let db_sender = db.clone();
    let bot_sender = bot.clone();
    let limits_sender = limits_config.clone();
    let mailing_list_sender = mailing_list_config.clone();
    let _send_handle = spawn_task("matrix_send", async move {
        matrix_send_loop(
            bot_sender,
            rx,
            db_sender,
            mailing_list_sender,
            limits_sender,
        )
        .await;
    });

    let db_cleanup = db.clone();
    let _cleanup_handle = spawn_task("cleanup", async move {
        cleanup_loop(db_cleanup).await;
    });

    let db_retry = db.clone();
    let bot_retry = bot.clone();
    let limits_retry = limits_config.clone();
    let _retry_handle = spawn_task("email_retry", async move {
        retry_loop(bot_retry, db_retry, limits_retry).await;
    });

    let _smtp_retry_handle =
        if let (Some(smtp_cfg), Some(smtp_pw)) = (smtp_config_opt, secrets.smtp_password.clone()) {
            info!("Spawning SMTP retry worker");
            let db_smtp_retry = db.clone();
            Some(spawn_task("smtp_retry", async move {
                smtp_retry_loop(db_smtp_retry, smtp_cfg, smtp_pw).await;
            }))
        } else {
            debug!("SMTP retry worker not started (no SMTP config or SMTP_PASSWORD)");
            None
        };

    let _delivery_confirmation_handle = if let Some(timeout_secs) = list_confirmation_timeout_secs {
        info!(
            timeout_secs = timeout_secs,
            automatic_retry = false,
            "Spawning mailing-list confirmation monitor"
        );
        let db_confirmation = db.clone();
        let client_confirmation = bot.client.clone();
        Some(spawn_task("delivery_confirmation", async move {
            delivery_confirmation_loop(client_confirmation, db_confirmation, timeout_secs).await;
        }))
    } else {
        info!("Mailing-list confirmation notices disabled");
        None
    };

    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            info!("Received SIGINT — shutting down");
            std::process::exit(0);
        }
    });

    info!("All background tasks spawned — entering Matrix continuous sync loop");
    bot.sync_forever().await
}

fn print_startup_diagnostics(config: &Config, secrets: &Secrets, db_path: &std::path::Path) {
    info!("=== Startup Diagnostics ===");
    info!(
        homeserver = %config.matrix.homeserver,
        user_id = %config.matrix.user_id,
        device_id = %config.matrix.device_id,
        has_recovery_key = config.matrix.recovery_key.is_some(),
        "Matrix"
    );
    info!(
        host = %config.imap.host,
        port = config.imap.port,
        tls_mode = "IMAPS (implicit TLS)",
        username = %config.imap.username,
        mailbox = config.imap.effective_mailbox(),
        poll_interval_secs = config.imap.effective_poll_interval_secs(),
        initial_fetch_limit = config.imap.effective_initial_fetch_limit(),
        skip_initial_history = config.imap.effective_skip_initial_history(),
        imap_password_set = !secrets.imap_password.is_empty(),
        "IMAP"
    );
    match &config.smtp {
        Some(smtp) => {
            let tls_mode = if smtp.require_smtps {
                "SMTPS"
            } else {
                "STARTTLS"
            };
            info!(
                host = %smtp.host,
                port = smtp.port,
                tls_mode = tls_mode,
                username = %smtp.username,
                from_address = %smtp.from_address,
                list_address = %smtp.list_address,
                allow_new_threads_from_matrix = smtp.allow_new_threads_from_matrix,
                smtp_password_set = secrets.smtp_password.is_some(),
                "SMTP"
            );
        }
        None => info!("SMTP: not configured — Matrix→Email bridging disabled"),
    }
    let list_cfg = &config.mailing_list;
    info!(
        list_id = ?list_cfg.list_id,
        sender_domains = ?list_cfg.sender_domains,
        sender_emails = ?list_cfg.sender_emails,
        subject_prefix = ?list_cfg.subject_prefix,
        "Mailing list filters"
    );
    info!(
        db_path = %db_path.display(),
        max_body_bytes = config.limits.effective_max_body_bytes(),
        max_attachment_bytes = config.limits.effective_max_attachment_bytes(),
        max_quote_lines = config.limits.effective_max_quote_lines(),
        "Limits and storage"
    );
    info!("=== End Startup Diagnostics ===");
}

async fn matrix_send_loop(
    bot: Bot,
    mut rx: mpsc::Receiver<RawEmail>,
    db: Db,
    mailing_list_config: config::MailingListConfig,
    limits: config::LimitsConfig,
) {
    info!("Matrix send loop: ready, waiting for emails");
    while let Some(raw) = rx.recv().await {
        let uid = raw.uid;
        let mailbox = raw.mailbox.clone();
        debug!(uid = uid, mailbox = %mailbox, bytes = raw.raw.len(), "Matrix send loop: received raw email");

        match email::parse(&raw, &limits) {
            Err(e) => {
                warn!(uid = uid, mailbox = %mailbox, error = %e, "Failed to parse email — skipping");
                continue;
            }
            Ok(mut parsed) => {
                debug!(
                    uid = uid,
                    message_id = %parsed.message_id,
                    subject = %parsed.subject,
                    from = %parsed.from_email,
                    "Email parsed"
                );

                let is_list_email = email::is_mailing_list_email(&parsed, &mailing_list_config);
                if is_list_email {
                    match db
                        .confirm_list_delivery(
                            &parsed.message_id,
                            parsed.matrix_event_id.as_deref(),
                        )
                        .await
                    {
                        Ok(Some(confirmation)) => info!(
                            uid = uid,
                            message_id = %parsed.message_id,
                            matrix_event_id = %confirmation.matrix_event_id,
                            previous_status = %confirmation.previous_status,
                            matched_by = if parsed.matrix_event_id.as_deref() == Some(confirmation.matrix_event_id.as_str()) {
                                "matrix_event_id"
                            } else {
                                "message_id"
                            },
                            "Mailing-list delivery confirmed"
                        ),
                        Ok(None) => debug!(
                            uid = uid,
                            message_id = %parsed.message_id,
                            matrix_event_id = ?parsed.matrix_event_id,
                            "Mailing-list message did not match an awaiting delivery"
                        ),
                        Err(e) => warn!(
                            uid = uid,
                            message_id = %parsed.message_id,
                            error = %e,
                            "Failed to check mailing-list delivery correlation"
                        ),
                    }
                }

                // Layer 1 loop guard: X-Bridge-Origin header set by this bridge on send.
                if parsed.bridge_origin.as_deref() == Some("matrix") {
                    info!(
                        uid = uid,
                        message_id = %parsed.message_id,
                        "Loop guard (L1): skipping bridge-echo (X-Bridge-Origin: matrix)"
                    );
                    continue;
                }

                // Layer 2 loop guard: check DB for emails we sent (catches header-stripped bounces).
                match db.is_bridge_sent_email(&parsed.message_id).await {
                    Ok(true) => {
                        info!(
                            uid = uid,
                            message_id = %parsed.message_id,
                            "Loop guard (L2): skipping bridge-echo (found in message_routes as sent)"
                        );
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            uid = uid,
                            message_id = %parsed.message_id,
                            error = %e,
                            "DB error on bridge-echo check — proceeding to avoid lost content"
                        );
                    }
                    Ok(false) => {
                        debug!(uid = uid, message_id = %parsed.message_id, "Loop guard passed");
                    }
                }

                if !is_list_email {
                    info!(
                        uid = uid,
                        message_id = %parsed.message_id,
                        from = %parsed.from_email,
                        list_id = ?parsed.list_id,
                        "Skipping non-list email"
                    );
                    continue;
                }

                debug!(uid = uid, message_id = %parsed.message_id, "Reconstructing email thread");
                email::reconstruct_thread(&mut parsed, &db).await;
                debug!(
                    uid = uid,
                    message_id = %parsed.message_id,
                    thread_root = ?parsed.thread_root,
                    "Thread reconstruction complete"
                );

                let t_post = std::time::Instant::now();
                if let Err(e) = matrix_post::post_email(&bot, &parsed, &db, &limits).await {
                    warn!(
                        uid = uid,
                        message_id = %parsed.message_id,
                        error = %e,
                        error_chain = ?e,
                        "Failed to post email to Matrix — adding to retry queue"
                    );
                    match serde_json::to_string(&parsed) {
                        Ok(payload) => {
                            if let Err(db_err) = db.push_retry(&payload).await {
                                error!(
                                    uid = uid,
                                    message_id = %parsed.message_id,
                                    error = %db_err,
                                    "Failed to push to retry queue — email may be lost"
                                );
                            } else {
                                debug!(uid = uid, message_id = %parsed.message_id, "Email added to retry queue");
                            }
                        }
                        Err(e) => {
                            error!(
                                uid = uid,
                                message_id = %parsed.message_id,
                                error = %e,
                                "Failed to serialize email for retry — email lost"
                            );
                        }
                    }
                } else {
                    debug!(
                        uid = uid,
                        message_id = %parsed.message_id,
                        elapsed_ms = t_post.elapsed().as_millis(),
                        "Email posted to Matrix"
                    );
                }
            }
        }
    }
    warn!("Matrix send loop: channel closed — IMAP sync task may have exited");
}

async fn retry_loop(bot: Bot, db: Db, limits: config::LimitsConfig) {
    info!("Email retry loop: started (poll interval: 300s)");
    loop {
        sleep(Duration::from_secs(300)).await;
        debug!("Email retry loop: polling queue");

        match db.pop_retry(10).await {
            Err(e) => {
                error!(error = %e, "Email retry loop: failed to poll retry queue");
                continue;
            }
            Ok(items) => {
                if items.is_empty() {
                    debug!("Email retry loop: queue empty");
                    continue;
                }
                info!(count = items.len(), "Email retry loop: processing items");
                for (id, payload, attempts) in items {
                    let parsed: ParsedEmail = match serde_json::from_str(&payload) {
                        Ok(p) => p,
                        Err(e) => {
                            error!(
                                retry_id = id,
                                error = %e,
                                "Email retry loop: corrupt payload — discarding"
                            );
                            db.ack_retry(id).await.ok();
                            continue;
                        }
                    };

                    if attempts >= 10 {
                        warn!(
                            retry_id = id,
                            message_id = %parsed.message_id,
                            attempts = attempts,
                            "Email retry loop: max attempts reached — dropping"
                        );
                        db.ack_retry(id).await.ok();
                        continue;
                    }

                    info!(
                        retry_id = id,
                        message_id = %parsed.message_id,
                        attempt = attempts + 1,
                        max_attempts = 10,
                        "Email retry loop: retrying post"
                    );
                    let t = std::time::Instant::now();
                    match matrix_post::post_email(&bot, &parsed, &db, &limits).await {
                        Ok(()) => {
                            info!(
                                retry_id = id,
                                message_id = %parsed.message_id,
                                attempt = attempts + 1,
                                elapsed_ms = t.elapsed().as_millis(),
                                "Email retry loop: retry succeeded"
                            );
                            db.ack_retry(id).await.ok();
                        }
                        Err(e) => {
                            let backoff_secs = (60u64 * (1u64 << attempts.min(10))).min(3600);
                            let next_retry_at =
                                chrono::Utc::now().timestamp() + backoff_secs as i64;
                            warn!(
                                retry_id = id,
                                message_id = %parsed.message_id,
                                attempt = attempts + 1,
                                max_attempts = 10,
                                backoff_secs = backoff_secs,
                                error = %e,
                                error_chain = ?e,
                                "Email retry loop: retry failed — backoff {}s", backoff_secs
                            );
                            db.fail_retry(id, next_retry_at).await.ok();
                        }
                    }
                }
            }
        }
    }
}

async fn smtp_retry_loop(db: Db, smtp_config: config::SmtpConfig, smtp_password: String) {
    let tls_mode = if smtp_config.require_smtps {
        "SMTPS"
    } else {
        "STARTTLS"
    };
    info!(
        smtp_host = %smtp_config.host,
        smtp_port = smtp_config.port,
        tls_mode = tls_mode,
        "SMTP retry loop: started"
    );

    loop {
        debug!("SMTP retry loop: polling queue");
        let items = match db.get_failed_smtp_routes(10).await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "SMTP retry loop: failed to fetch queue");
                sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        if !items.is_empty() {
            info!(count = items.len(), "SMTP retry loop: processing items");
        } else {
            debug!("SMTP retry loop: queue empty");
        }

        for item in items {
            let reply: smtp_send::SmtpReply = match serde_json::from_str(&item.smtp_payload) {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        matrix_event_id = %item.matrix_event_id,
                        error = %e,
                        "SMTP retry loop: corrupt payload — parking with max retry_at to stop blocking queue"
                    );
                    db.mark_route_failed(&item.matrix_event_id, i64::MAX)
                        .await
                        .ok();
                    continue;
                }
            };

            info!(
                matrix_event_id = %item.matrix_event_id,
                our_message_id = %reply.our_message_id,
                attempt = item.attempts + 1,
                max_attempts = 10,
                smtp_host = %smtp_config.host,
                tls_mode = tls_mode,
                "SMTP retry loop: attempting delivery"
            );
            let t = std::time::Instant::now();
            match smtp_send::send_reply(&smtp_config, &smtp_password, &reply).await {
                Ok(()) => {
                    info!(
                        matrix_event_id = %item.matrix_event_id,
                        our_message_id = %reply.our_message_id,
                        attempt = item.attempts + 1,
                        elapsed_ms = t.elapsed().as_millis(),
                        "SMTP retry loop: relay accepted message"
                    );
                    db.mark_route_smtp_accepted(&item.matrix_event_id)
                        .await
                        .ok();
                }
                Err(e) => {
                    let backoff_secs = (60u64 * (1u64 << (item.attempts as u32).min(10))).min(3600);
                    let next_retry_at = chrono::Utc::now().timestamp() + backoff_secs as i64;
                    warn!(
                        matrix_event_id = %item.matrix_event_id,
                        our_message_id = %reply.our_message_id,
                        attempt = item.attempts + 1,
                        max_attempts = 10,
                        backoff_secs = backoff_secs,
                        elapsed_ms = t.elapsed().as_millis(),
                        error = %e,
                        error_chain = ?e,
                        "SMTP retry loop: delivery failed — backoff {}s", backoff_secs
                    );
                    db.mark_route_failed(&item.matrix_event_id, next_retry_at)
                        .await
                        .ok();
                }
            }
        }

        // Sleep until the next scheduled retry, up to a 5-minute cap.
        let sleep_dur = match db.get_next_smtp_retry_at().await {
            Ok(None) => {
                debug!("SMTP retry loop: nothing queued — sleeping 300s");
                Duration::from_secs(300)
            }
            Ok(Some(ts)) => {
                let now = chrono::Utc::now().timestamp();
                let secs = (ts - now).clamp(1, 300) as u64;
                debug!(
                    next_retry_at = ts,
                    sleep_secs = secs,
                    "SMTP retry loop: sleeping until next retry"
                );
                Duration::from_secs(secs)
            }
            Err(e) => {
                error!(error = %e, "SMTP retry loop: failed to query next retry time — sleeping 300s");
                Duration::from_secs(300)
            }
        };
        sleep(sleep_dur).await;
    }
}

async fn delivery_confirmation_loop(client: Client, db: Db, timeout_secs: u64) {
    const NOTICE_LEASE_SECS: i64 = 300;
    loop {
        match db
            .claim_unconfirmed_delivery(timeout_secs as i64, NOTICE_LEASE_SECS)
            .await
        {
            Ok(Some(notice)) => {
                info!(
                    matrix_event_id = %notice.matrix_event_id,
                    room_id = %notice.room_id,
                    timeout_secs = timeout_secs,
                    "Mailing-list delivery remains unconfirmed — notice claimed"
                );

                match db.delivery_notice_is_active(&notice.matrix_event_id).await {
                    Ok(false) => {
                        info!(
                            matrix_event_id = %notice.matrix_event_id,
                            "Delivery was confirmed after notice scheduling — suppressing notice"
                        );
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            matrix_event_id = %notice.matrix_event_id,
                            error = %e,
                            "Could not re-check delivery state — releasing notice claim"
                        );
                        db.release_delivery_notice(&notice.matrix_event_id)
                            .await
                            .ok();
                        sleep(Duration::from_secs(60)).await;
                        continue;
                    }
                    Ok(true) => {}
                }

                let room_id = match notice.room_id.parse::<OwnedRoomId>() {
                    Ok(room_id) => room_id,
                    Err(e) => {
                        error!(
                            matrix_event_id = %notice.matrix_event_id,
                            room_id = %notice.room_id,
                            error = %e,
                            "Invalid room ID for delivery notice — parking notice"
                        );
                        db.mark_delivery_notice_sent(&notice.matrix_event_id)
                            .await
                            .ok();
                        continue;
                    }
                };
                let Some(room) = client.get_room(&room_id) else {
                    warn!(
                        matrix_event_id = %notice.matrix_event_id,
                        room_id = %room_id,
                        "Room unavailable for delivery notice — releasing claim"
                    );
                    db.release_delivery_notice(&notice.matrix_event_id)
                        .await
                        .ok();
                    sleep(Duration::from_secs(60)).await;
                    continue;
                };

                let minutes = timeout_secs.div_ceil(60);
                let content = RoomMessageEventContent::text_plain(format!(
                    "Email delivery unconfirmed for Matrix event {}. The SMTP relay accepted the message, but no mailing-list copy was received within {} minutes. No automatic resend was attempted.",
                    notice.matrix_event_id, minutes
                ));
                let transaction_id = OwnedTransactionId::from(format!(
                    "email-delivery-unconfirmed:{}",
                    notice.matrix_event_id
                ));
                match room.send(content).with_transaction_id(transaction_id).await {
                    Ok(_) => {
                        info!(
                            matrix_event_id = %notice.matrix_event_id,
                            room_id = %room_id,
                            "Final delivery-unconfirmed notice posted; no resend scheduled"
                        );
                        if let Err(e) = db.mark_delivery_notice_sent(&notice.matrix_event_id).await
                        {
                            warn!(
                                matrix_event_id = %notice.matrix_event_id,
                                error = %e,
                                "Failed to persist delivery notice completion"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            matrix_event_id = %notice.matrix_event_id,
                            room_id = %room_id,
                            error = %e,
                            "Failed to post delivery-unconfirmed notice — releasing claim"
                        );
                        db.release_delivery_notice(&notice.matrix_event_id)
                            .await
                            .ok();
                    }
                }
            }
            Ok(None) => sleep(Duration::from_secs(60)).await,
            Err(e) => {
                error!(error = %e, "Delivery confirmation monitor failed to claim work");
                sleep(Duration::from_secs(60)).await;
            }
        }
    }
}

async fn cleanup_loop(db: Db) {
    info!("Cleanup loop: started (interval: 24h)");
    let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
    loop {
        interval.tick().await;
        debug!("Cleanup loop: removing thread mappings older than 90 days");
        let cutoff_secs = 90 * 24 * 60 * 60;
        match db.cleanup_old_threads(cutoff_secs).await {
            Ok(n) if n > 0 => info!(deleted = n, "Cleanup loop: removed old thread mappings"),
            Ok(_) => debug!("Cleanup loop: no old thread mappings to remove"),
            Err(e) => error!(error = %e, "Cleanup loop: thread cleanup failed"),
        }
    }
}
