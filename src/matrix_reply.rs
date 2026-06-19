use std::collections::HashSet;

use matrix_sdk::{
    ruma::{
        events::room::message::{MessageType, OriginalSyncRoomMessageEvent, Relation},
        OwnedUserId,
    },
    Room, RoomState,
};
use tracing::{debug, info, warn};

use crate::config::{RoomAllowList, SmtpConfig};
use crate::db::Db;
use crate::smtp_send::{send_reply, SmtpReply};

use serde_json;

#[derive(Clone)]
pub struct ReplyState {
    pub bot_user_id: OwnedUserId,
    /// Empty = all room members may reply.
    pub allowed_repliers: HashSet<OwnedUserId>,
    pub smtp_config: SmtpConfig,
    pub smtp_password: String,
    pub db: Db,
    pub allowed_rooms: RoomAllowList,
}

pub fn register_reply_handler(client: &matrix_sdk::Client, state: ReplyState) {
    debug!("Registering Matrix reply→email event handler");
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room| {
            let state = state.clone();
            async move {
                handle_possible_reply(state, room, ev).await;
            }
        }
    });
    info!("Matrix reply→email event handler registered");
}

async fn handle_possible_reply(state: ReplyState, room: Room, ev: OriginalSyncRoomMessageEvent) {
    let event_id = ev.event_id.to_string();
    let sender = ev.sender.to_string();

    debug!(
        event_id = %event_id,
        sender = %sender,
        room_id = %room.room_id(),
        "Matrix reply handler: event received"
    );

    if ev.sender == state.bot_user_id {
        debug!(event_id = %event_id, "Skip: own message");
        return;
    }

    if room.state() != RoomState::Joined {
        debug!(
            event_id = %event_id,
            room_id = %room.room_id(),
            room_state = ?room.state(),
            "Skip: room not in Joined state"
        );
        return;
    }

    if !state.allowed_rooms.allows(room.room_id()) {
        warn!(
            event_id = %event_id,
            room_id = %room.room_id(),
            sender = sender,
            "Skip: room not in allowed_rooms"
        );
        return;
    }

    let MessageType::Text(ref text) = ev.content.msgtype else {
        debug!(event_id = %event_id, sender = %sender, "Skip: not a text message");
        return;
    };
    let raw_body = text.body.clone();

    if !state.allowed_repliers.is_empty() && !state.allowed_repliers.contains(&ev.sender) {
        warn!(
            event_id = %event_id,
            sender = %sender,
            "Skip: sender not in allowed_repliers — unauthorized bridge attempt"
        );
        return;
    }

    // Idempotency guard: skip if this Matrix event was already bridged.
    match state.db.get_route_by_matrix_event(&event_id).await {
        Ok(Some(existing)) => {
            info!(
                event_id = %event_id,
                smtp_status = ?existing.origin,
                "Skip: event already bridged (dedup)"
            );
            return;
        }
        Err(e) => {
            warn!(
                event_id = %event_id,
                error = %e,
                "DB error on dedup check — skipping to avoid double-send"
            );
            return;
        }
        Ok(None) => {
            debug!(event_id = %event_id, "Dedup check passed: not yet bridged");
        }
    }

    let relates_to = ev.content.relates_to.clone();
    match relates_to {
        Some(Relation::Thread(thread)) => {
            handle_thread_reply(state, room, ev, raw_body, thread).await;
        }
        Some(_) => {
            debug!(
                event_id = %event_id,
                sender = %sender,
                "Skip: related message is not a thread reply"
            );
        }
        None if state.smtp_config.allow_new_threads_from_matrix => {
            handle_new_thread_message(state, room, ev, raw_body).await;
        }
        None => {
            debug!(
                event_id = %event_id,
                sender = %sender,
                "Skip: top-level Matrix→Email posting disabled"
            );
        }
    }
}

async fn handle_thread_reply(
    state: ReplyState,
    room: Room,
    ev: OriginalSyncRoomMessageEvent,
    raw_body: String,
    thread: matrix_sdk::ruma::events::relation::Thread,
) {
    let event_id = ev.event_id.as_str();
    let sender = ev.sender.as_str();

    // The thread root must be known to the bridge. It may be email-originated
    // or a Matrix-originated top-level post that created a mailing-list thread.
    let thread_root_event_id = thread.event_id.to_string();
    debug!(
        event_id = event_id,
        thread_root_event_id = %thread_root_event_id,
        "Looking up thread root in DB"
    );
    let root_route = match state
        .db
        .get_route_by_matrix_event(&thread_root_event_id)
        .await
    {
        Ok(Some(r)) if r.origin == "email" || r.origin == "matrix" => {
            info!(
                event_id = event_id,
                thread_root_event_id = %thread_root_event_id,
                email_message_id = %r.email_message_id,
                subject = ?r.subject,
                origin = %r.origin,
                "Thread root found — proceeding"
            );
            r
        }
        Ok(Some(r)) => {
            debug!(
                event_id = event_id,
                thread_root_event_id = %thread_root_event_id,
                origin = %r.origin,
                "Skip: thread root has unsupported origin"
            );
            return;
        }
        Ok(None) => {
            debug!(
                event_id = event_id,
                thread_root_event_id = %thread_root_event_id,
                "Skip: thread root not found in DB (unknown or pre-bridge thread)"
            );
            return;
        }
        Err(e) => {
            warn!(
                event_id = event_id,
                thread_root_event_id = %thread_root_event_id,
                error = %e,
                "DB lookup failed for thread root"
            );
            return;
        }
    };

    let (in_reply_to_email_id, specific_route) =
        resolve_in_reply_to(&thread, &root_route, &state.db).await;
    debug!(
        event_id = event_id,
        in_reply_to_email_id = %in_reply_to_email_id,
        has_specific_in_reply_to = specific_route.is_some(),
        "Resolved In-Reply-To email Message-Id"
    );

    let references = build_references(&root_route, specific_route.as_ref(), &in_reply_to_email_id);
    debug!(
        event_id = event_id,
        references_count = references.len(),
        references = ?references,
        "Built email References chain"
    );

    let subject = root_route
        .subject
        .clone()
        .unwrap_or_else(|| "(no subject)".to_owned());

    let display_name = get_display_name(&room, &ev.sender).await;
    debug!(
        event_id = event_id,
        sender = sender,
        display_name = %display_name,
        "Resolved sender display name"
    );

    let body = sanitize_reply_body(&raw_body);
    let body_len = body.len();
    if body.is_empty() {
        info!(
            event_id = event_id,
            sender = sender,
            "Skip: body is empty after stripping quoted lines"
        );
        return;
    }
    debug!(
        event_id = event_id,
        body_len = body_len,
        "Reply body sanitized"
    );

    let smtp_reply = SmtpReply::new(
        display_name.clone(),
        ev.sender.to_string(),
        in_reply_to_email_id.clone(),
        references,
        subject.clone(),
        body,
    );

    send_matrix_email(
        &state,
        event_id,
        sender,
        &display_name,
        &smtp_reply,
        root_route.thread_root_email_message_id.as_deref(),
        Some(&subject),
        body_len,
        "Bridging Matrix reply → email",
    )
    .await;
}

async fn handle_new_thread_message(
    state: ReplyState,
    room: Room,
    ev: OriginalSyncRoomMessageEvent,
    raw_body: String,
) {
    let event_id = ev.event_id.as_str();
    let sender = ev.sender.as_str();

    let display_name = get_display_name(&room, &ev.sender).await;
    let body = sanitize_reply_body(&raw_body);
    let body_len = body.len();
    if body.is_empty() {
        info!(
            event_id = event_id,
            sender = sender,
            "Skip: body is empty after stripping quoted lines"
        );
        return;
    }

    let subject = derive_new_thread_subject(&display_name, &body);
    let smtp_reply = SmtpReply::new_thread(
        display_name.clone(),
        ev.sender.to_string(),
        subject.clone(),
        body,
    );

    if let Err(e) = state
        .db
        .store_thread(
            &smtp_reply.our_message_id,
            event_id,
            &smtp_reply.our_message_id,
            "matrix",
        )
        .await
    {
        warn!(
            event_id = event_id,
            our_message_id = %smtp_reply.our_message_id,
            error = %e,
            "Failed to store Matrix-originated thread mapping — aborting"
        );
        return;
    }

    send_matrix_email(
        &state,
        event_id,
        sender,
        &display_name,
        &smtp_reply,
        Some(&smtp_reply.our_message_id),
        Some(&subject),
        body_len,
        "Bridging top-level Matrix message → new email thread",
    )
    .await;
}

async fn send_matrix_email(
    state: &ReplyState,
    event_id: &str,
    sender: &str,
    display_name: &str,
    smtp_reply: &SmtpReply,
    thread_root_email_message_id: Option<&str>,
    subject: Option<&str>,
    body_len: usize,
    action: &'static str,
) {
    let smtp_payload = match serde_json::to_string(smtp_reply) {
        Ok(p) => p,
        Err(e) => {
            warn!(event_id = event_id, error = %e, "Failed to serialize SMTP payload");
            return;
        }
    };

    // Step 1: insert as PENDING before any SMTP attempt (crash-safe).
    debug!(
        event_id = event_id,
        our_message_id = %smtp_reply.our_message_id,
        "Inserting route as PENDING"
    );
    if let Err(e) = state
        .db
        .store_matrix_reply_pending(
            event_id,
            &smtp_reply.our_message_id,
            thread_root_email_message_id,
            subject,
            &smtp_payload,
        )
        .await
    {
        warn!(event_id = event_id, error = %e, "Failed to insert PENDING route — aborting");
        return;
    }
    debug!(event_id = event_id, "Route state: PENDING");

    info!(
        event_id = event_id,
        sender = sender,
        display_name = %display_name,
        our_message_id = %smtp_reply.our_message_id,
        in_reply_to = %smtp_reply.in_reply_to,
        subject = ?subject,
        body_len = body_len,
        action
    );

    // Step 2: attempt SMTP delivery.
    let t = std::time::Instant::now();
    match send_reply(&state.smtp_config, &state.smtp_password, &smtp_reply).await {
        Ok(()) => {
            info!(
                event_id = event_id,
                our_message_id = %smtp_reply.our_message_id,
                elapsed_ms = t.elapsed().as_millis(),
                "SMTP delivery succeeded — marking route SENT"
            );
            if let Err(e) = state.db.mark_route_sent(event_id).await {
                warn!(event_id = event_id, error = %e, "Failed to mark route SENT (will retry as PENDING)");
            } else {
                debug!(event_id = event_id, "Route state: SENT");
            }
        }
        Err(e) => {
            let next_retry_at = chrono::Utc::now().timestamp() + 60;
            warn!(
                event_id = event_id,
                our_message_id = %smtp_reply.our_message_id,
                elapsed_ms = t.elapsed().as_millis(),
                error = %e,
                error_chain = ?e,
                next_retry_in_secs = 60,
                "SMTP delivery failed — marking route FAILED, retry in 60s"
            );
            if let Err(db_err) = state.db.mark_route_failed(event_id, next_retry_at).await {
                warn!(event_id = event_id, error = %db_err, "Failed to mark route FAILED");
            } else {
                debug!(
                    event_id = event_id,
                    "Route state: FAILED (scheduled for retry)"
                );
            }
        }
    }
}

fn derive_new_thread_subject(display_name: &str, body: &str) -> String {
    let first_line = body
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("Matrix message");
    let mut subject: String = first_line.chars().take(72).collect();
    if first_line.chars().count() > 72 {
        subject.push_str("...");
    }
    if subject.is_empty() {
        subject = "Matrix message".to_owned();
    }
    format!("{} via Matrix: {}", display_name, subject)
}

/// Resolve which email Message-Id to use as In-Reply-To.
///
/// If the thread relation has an `m.in_reply_to` pointing to a specific event
/// within the thread, look that event up in message_routes and use its email
/// Message-Id. Falls back to the thread root's email Message-Id.
async fn resolve_in_reply_to(
    thread: &matrix_sdk::ruma::events::relation::Thread,
    root_route: &crate::db::RouteRecord,
    db: &Db,
) -> (String, Option<crate::db::RouteRecord>) {
    if let Some(ref irt) = thread.in_reply_to {
        let irt_event_id = irt.event_id.to_string();
        // Don't double-look-up if m.in_reply_to points at the thread root itself.
        if irt_event_id != thread.event_id.to_string() {
            match db.get_route_by_matrix_event(&irt_event_id).await {
                Ok(Some(r)) if r.origin == "email" || r.origin == "matrix" => {
                    let email_id = r.email_message_id.clone();
                    return (email_id, Some(r));
                }
                Ok(_) => {}
                Err(e) => warn!("DB lookup failed for m.in_reply_to {}: {e}", irt_event_id),
            }
        }
    }
    (root_route.email_message_id.clone(), None)
}

/// Build a deduplicated References chain from oldest ancestor to direct parent.
fn build_references(
    root_route: &crate::db::RouteRecord,
    specific_route: Option<&crate::db::RouteRecord>,
    in_reply_to_email_id: &str,
) -> Vec<String> {
    let mut refs: Vec<String> = Vec::new();

    // Include the email thread root (may differ from the Matrix thread root).
    if let Some(ref root_email_root) = root_route.thread_root_email_message_id {
        if !refs.contains(root_email_root) {
            refs.push(root_email_root.clone());
        }
    }

    // Include the Matrix thread root's own email Message-Id.
    if !refs.contains(&root_route.email_message_id) {
        refs.push(root_route.email_message_id.clone());
    }

    // If replying to a specific mid-thread message, include its thread root too.
    if let Some(sr) = specific_route {
        if let Some(ref sr_root) = sr.thread_root_email_message_id {
            if !refs.contains(sr_root) {
                refs.push(sr_root.clone());
            }
        }
    }

    // Remove the direct In-Reply-To from refs so the final list goes up to (not including) it.
    refs.retain(|r| r != in_reply_to_email_id);

    refs
}

async fn get_display_name(room: &Room, user_id: &OwnedUserId) -> String {
    if let Ok(Some(member)) = room.get_member(user_id).await {
        if let Some(name) = member.display_name() {
            return name.to_owned();
        }
    }
    user_id.localpart().to_owned()
}

/// Strip quoted lines and Matrix reply-fallback boilerplate, returning clean plaintext.
fn sanitize_reply_body(raw: &str) -> String {
    let lines: Vec<&str> = raw
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.starts_with("> ") && t != ">"
        })
        .collect();

    let start = lines.iter().position(|l| !l.trim().is_empty()).unwrap_or(0);
    let end = lines
        .iter()
        .rposition(|l| !l.trim().is_empty())
        .map(|i| i + 1)
        .unwrap_or(0);

    if start >= end {
        return String::new();
    }

    lines[start..end].join("\n")
}

#[cfg(test)]
mod tests {
    use super::sanitize_reply_body;

    #[test]
    fn strips_quoted_lines() {
        let input = "> Original line\n> Another quoted\nMy actual reply";
        assert_eq!(sanitize_reply_body(input), "My actual reply");
    }

    #[test]
    fn strips_leading_and_trailing_blank_lines() {
        let input = "\n\nHello world\n\n";
        assert_eq!(sanitize_reply_body(input), "Hello world");
    }

    #[test]
    fn empty_after_stripping() {
        let input = "> everything is quoted\n> nothing left";
        assert!(sanitize_reply_body(input).is_empty());
    }

    #[test]
    fn multiline_body_preserved() {
        let input = "> quote\nLine one\nLine two\nLine three";
        assert_eq!(sanitize_reply_body(input), "Line one\nLine two\nLine three");
    }
}
