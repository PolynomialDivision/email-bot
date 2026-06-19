use anyhow::{Context, Result};
use matrix_sdk::ruma::events::relation::Thread;
use matrix_sdk::ruma::events::room::message::{MessageType, Relation, RoomMessageEventContent};
use matrix_sdk::ruma::events::room::ImageInfo;
use matrix_sdk::ruma::{OwnedEventId, UInt};
use matrix_sdk::{Client, RoomState};
use tracing::{debug, info, warn};

use crate::config::{LimitsConfig, RoomAllowList};
use crate::db::Db;
use crate::email::ParsedEmail;
use crate::format::format_email;

pub async fn post_email(
    client: &Client,
    email: &ParsedEmail,
    db: &Db,
    limits: &LimitsConfig,
    allowed_rooms: &RoomAllowList,
) -> Result<()> {
    debug!(
        uid = email.uid,
        message_id = %email.message_id,
        subject = %email.subject,
        from = %email.from_email,
        thread_root = ?email.thread_root,
        attachment_count = email.attachments.len(),
        "matrix_post: preparing email for Matrix"
    );

    let (plain, html) = format_email(email, limits);
    debug!(
        message_id = %email.message_id,
        plain_len = plain.len(),
        html_len = html.len(),
        "matrix_post: message formatted"
    );

    // Determine threading
    let thread_info = if let Some(ref root_msg_id) = email.thread_root {
        debug!(
            message_id = %email.message_id,
            root_email_message_id = %root_msg_id,
            "matrix_post: looking up Matrix thread root in DB"
        );
        match db.get_thread_root(root_msg_id).await? {
            Some((root_event_id_str, thread_root_id)) => {
                info!(
                    message_id = %email.message_id,
                    root_email_message_id = %root_msg_id,
                    root_event_id = %root_event_id_str,
                    thread_root_id = %thread_root_id,
                    "matrix_post: thread root found — posting as thread reply"
                );
                Some((root_event_id_str, thread_root_id))
            }
            None => {
                warn!(
                    message_id = %email.message_id,
                    root_email_message_id = %root_msg_id,
                    "matrix_post: thread root not in DB — posting as new thread root"
                );
                None
            }
        }
    } else {
        debug!(message_id = %email.message_id, "matrix_post: no thread root — new thread");
        None
    };

    let mut content = RoomMessageEventContent::text_html(plain, html);

    if let Some((ref root_event_id_str, _)) = thread_info {
        match root_event_id_str.parse::<OwnedEventId>() {
            Ok(root_event_id) => {
                debug!(
                    message_id = %email.message_id,
                    root_event_id = %root_event_id,
                    "matrix_post: attaching m.thread relation"
                );
                let thread = Thread::reply(root_event_id.clone(), root_event_id);
                content.relates_to = Some(Relation::Thread(thread));
            }
            Err(e) => {
                warn!(
                    message_id = %email.message_id,
                    root_event_id = %root_event_id_str,
                    error = %e,
                    "matrix_post: invalid root event ID — posting without thread relation"
                );
            }
        }
    }

    let rooms: Vec<_> = client
        .joined_rooms()
        .into_iter()
        .filter(|r| r.state() == RoomState::Joined)
        .filter(|r| allowed_rooms.allows(r.room_id()))
        .collect();

    if rooms.is_empty() {
        warn!(
            message_id = %email.message_id,
            "matrix_post: no joined rooms — email cannot be posted (check bot invite)"
        );
        return Ok(());
    }

    debug!(
        message_id = %email.message_id,
        room_count = rooms.len(),
        "matrix_post: sending to rooms"
    );

    let mut last_event_id: Option<String> = None;

    for room in &rooms {
        let t_send = std::time::Instant::now();
        match room.send(content.clone()).await {
            Ok(resp) => {
                let event_id = resp.response.event_id.to_string();
                info!(
                    message_id = %email.message_id,
                    room_id = %room.room_id(),
                    event_id = %event_id,
                    elapsed_ms = t_send.elapsed().as_millis(),
                    "matrix_post: email posted"
                );
                last_event_id = Some(event_id);
            }
            Err(e) => {
                warn!(
                    message_id = %email.message_id,
                    room_id = %room.room_id(),
                    elapsed_ms = t_send.elapsed().as_millis(),
                    error = %e,
                    error_chain = ?e,
                    "matrix_post: failed to send to room"
                );
                return Err(e.into());
            }
        }
    }

    if let Some(event_id) = last_event_id {
        let thread_root_id = thread_info
            .map(|(_, root_id)| root_id)
            .unwrap_or_else(|| email.message_id.clone());

        debug!(
            message_id = %email.message_id,
            event_id = %event_id,
            thread_root_id = %thread_root_id,
            mailbox = %email.mailbox,
            "matrix_post: storing thread mapping and route record"
        );

        db.store_thread(
            &email.message_id,
            &event_id,
            &thread_root_id,
            &email.mailbox,
        )
        .await
        .context("store_thread after post")?;

        db.store_route(
            &event_id,
            &email.message_id,
            Some(&thread_root_id),
            Some(&email.subject),
            "email",
        )
        .await
        .context("store_route after post")?;

        debug!(
            message_id = %email.message_id,
            event_id = %event_id,
            "matrix_post: thread mapping and route stored"
        );
    }

    if !email.attachments.is_empty() {
        info!(
            message_id = %email.message_id,
            count = email.attachments.len(),
            "matrix_post: posting attachments"
        );
        post_attachments(client, email, limits, allowed_rooms).await;
    }

    Ok(())
}

async fn post_attachments(
    client: &Client,
    email: &ParsedEmail,
    limits: &LimitsConfig,
    allowed_rooms: &RoomAllowList,
) {
    let max_bytes = limits.effective_max_attachment_bytes();
    let rooms: Vec<_> = client
        .joined_rooms()
        .into_iter()
        .filter(|r| r.state() == RoomState::Joined)
        .filter(|r| allowed_rooms.allows(r.room_id()))
        .collect();

    for (idx, attachment) in email.attachments.iter().enumerate() {
        debug!(
            message_id = %email.message_id,
            attachment_index = idx,
            filename = %attachment.filename,
            content_type = %attachment.content_type,
            bytes = attachment.data.len(),
            max_bytes = max_bytes,
            "matrix_post: processing attachment"
        );

        if attachment.data.len() > max_bytes {
            warn!(
                message_id = %email.message_id,
                filename = %attachment.filename,
                bytes = attachment.data.len(),
                max_bytes = max_bytes,
                "matrix_post: skipping attachment — exceeds size limit"
            );
            continue;
        }

        let mime: mime::Mime = attachment
            .content_type
            .parse()
            .unwrap_or(mime::APPLICATION_OCTET_STREAM);

        debug!(
            message_id = %email.message_id,
            filename = %attachment.filename,
            mime = %mime,
            bytes = attachment.data.len(),
            "matrix_post: uploading attachment to Matrix media store"
        );
        let t_upload = std::time::Instant::now();
        match client
            .media()
            .upload(&mime, attachment.data.clone(), None)
            .await
        {
            Ok(response) => {
                let mxc_uri = response.content_uri;
                info!(
                    message_id = %email.message_id,
                    filename = %attachment.filename,
                    mxc_uri = %mxc_uri,
                    elapsed_ms = t_upload.elapsed().as_millis(),
                    "matrix_post: attachment uploaded"
                );
                let content = if mime.type_() == mime::IMAGE {
                    let mut info = ImageInfo::new();
                    info.mimetype = Some(attachment.content_type.clone());
                    info.size = Some(UInt::new_wrapping(attachment.data.len() as u64));
                    RoomMessageEventContent::new(MessageType::Image(
                        matrix_sdk::ruma::events::room::message::ImageMessageEventContent::plain(
                            attachment.filename.clone(),
                            mxc_uri,
                        )
                        .info(Box::new(info)),
                    ))
                } else {
                    RoomMessageEventContent::new(MessageType::File(
                        matrix_sdk::ruma::events::room::message::FileMessageEventContent::plain(
                            attachment.filename.clone(),
                            mxc_uri,
                        ),
                    ))
                };

                for room in &rooms {
                    if let Err(e) = room.send(content.clone()).await {
                        warn!(
                            message_id = %email.message_id,
                            filename = %attachment.filename,
                            room_id = %room.room_id(),
                            error = %e,
                            "matrix_post: failed to post attachment to room"
                        );
                    } else {
                        debug!(
                            message_id = %email.message_id,
                            filename = %attachment.filename,
                            room_id = %room.room_id(),
                            "matrix_post: attachment posted"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    message_id = %email.message_id,
                    filename = %attachment.filename,
                    bytes = attachment.data.len(),
                    elapsed_ms = t_upload.elapsed().as_millis(),
                    error = %e,
                    "matrix_post: failed to upload attachment to Matrix media store"
                );
            }
        }
    }
}
