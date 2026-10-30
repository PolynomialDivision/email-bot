use anyhow::{Context, Result};
use mxbot_common::matrix_sdk::{
    ruma::{
        events::{
            relation::Thread,
            room::{
                message::{
                    FileMessageEventContent, ImageMessageEventContent, MessageType, Relation,
                    RoomMessageEventContent,
                },
                ImageInfo, MediaSource,
            },
        },
        OwnedEventId, OwnedTransactionId, UInt,
    },
    RoomState,
};
use mxbot_common::{media, send::send_with_retry_txn, Bot};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::config::LimitsConfig;
use crate::db::Db;
use crate::email::{Attachment, ParsedEmail};
use crate::format::{fit_to_event, format_email, MAX_EVENT_CONTENT_BYTES};

pub async fn post_email(
    bot: &Bot,
    email: &ParsedEmail,
    db: &Db,
    limits: &LimitsConfig,
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
    let (plain, html) = fit_to_event(plain, html, MAX_EVENT_CONTENT_BYTES);
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
                    "matrix_post: thread root found, posting as thread reply"
                );
                Some((root_event_id_str, thread_root_id))
            }
            None => {
                warn!(
                    message_id = %email.message_id,
                    root_email_message_id = %root_msg_id,
                    "matrix_post: thread root not in DB, posting as new thread root"
                );
                None
            }
        }
    } else {
        debug!(message_id = %email.message_id, "matrix_post: no thread root, new thread");
        None
    };

    let txn_base = post_transaction_base(&email.message_id, &plain);
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
                    "matrix_post: invalid root event ID, posting without thread relation"
                );
            }
        }
    }

    let rooms: Vec<_> = bot
        .broadcast_rooms()
        .into_iter()
        .filter(|r| r.state() == RoomState::Joined)
        .collect();

    if rooms.is_empty() {
        // An error, not Ok: the email goes to the retry queue instead of being dropped.
        anyhow::bail!("no joined room to post the email to (check the bot's invite)");
    }

    debug!(
        message_id = %email.message_id,
        room_count = rooms.len(),
        "matrix_post: sending to rooms"
    );

    let mut last_event_id: Option<String> = None;

    for room in &rooms {
        let t_send = std::time::Instant::now();
        let txn_id = transaction_id(&txn_base, room.room_id().as_str(), "post");
        match send_with_retry_txn(room, content.clone(), &txn_id).await {
            Ok(event_id) => {
                let event_id = event_id.to_string();
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
        post_attachments(bot, email, limits, &txn_base).await;
    }

    Ok(())
}

/// The base of this email's transaction IDs: the same email (Message-ID and text) gets the
/// same IDs, so a post retried from the queue after a partial failure (one room done, the
/// next failed) returns the events already posted instead of posting them twice. The text is
/// part of it so two different emails with one generated ID (`uid-N`) never collide.
fn post_transaction_base(message_id: &str, plain: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(message_id.as_bytes());
    hash.update([0]);
    hash.update(plain.as_bytes());
    hash.finalize()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// One transaction ID per email, room and part (`post`, `attachment-N`).
fn transaction_id(base: &str, room_id: &str, part: &str) -> OwnedTransactionId {
    format!("email-{base}-{room_id}-{part}").into()
}

async fn post_attachments(bot: &Bot, email: &ParsedEmail, limits: &LimitsConfig, txn_base: &str) {
    let max_bytes = limits.effective_max_attachment_bytes();
    let rooms: Vec<_> = bot
        .broadcast_rooms()
        .into_iter()
        .filter(|r| r.state() == RoomState::Joined)
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
                "matrix_post: skipping attachment, exceeds size limit"
            );
            continue;
        }

        let mime: mime::Mime = attachment
            .content_type
            .parse()
            .unwrap_or(mime::APPLICATION_OCTET_STREAM);

        // Uploaded encrypted for an encrypted room, plain for the others; once per kind.
        let file = media::Attachment::new(
            attachment.filename.clone(),
            mime.clone(),
            attachment.data.clone(),
        );
        for room in &rooms {
            debug!(
                message_id = %email.message_id,
                filename = %attachment.filename,
                mime = %mime,
                bytes = attachment.data.len(),
                room_id = %room.room_id(),
                "matrix_post: uploading attachment to Matrix media store"
            );
            let t_upload = std::time::Instant::now();
            let source = match file.upload(room).await {
                Ok(source) => source,
                Err(e) => {
                    warn!(
                        message_id = %email.message_id,
                        filename = %attachment.filename,
                        bytes = attachment.data.len(),
                        room_id = %room.room_id(),
                        elapsed_ms = t_upload.elapsed().as_millis(),
                        error = %e,
                        "matrix_post: failed to upload attachment to Matrix media store"
                    );
                    continue;
                }
            };
            info!(
                message_id = %email.message_id,
                filename = %attachment.filename,
                mxc_uri = %media_uri(&source),
                encrypted = matches!(source, MediaSource::Encrypted(_)),
                elapsed_ms = t_upload.elapsed().as_millis(),
                "matrix_post: attachment uploaded"
            );
            let txn_id = transaction_id(
                txn_base,
                room.room_id().as_str(),
                &format!("attachment-{idx}"),
            );
            // The source again right before every attempt: should encryption have been
            // switched on since the upload, the file goes up again encrypted.
            let content = |source| attachment_content(attachment, &mime, source);
            if let Err(e) = media::send_content_txn(room, &file, content, &txn_id).await {
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
}

/// The event for an uploaded attachment: an image as `m.image` with its MIME type and size,
/// anything else as a bare `m.file`; the file name as body.
fn attachment_content(
    attachment: &Attachment,
    mime: &mime::Mime,
    source: MediaSource,
) -> RoomMessageEventContent {
    if mime.type_() == mime::IMAGE {
        let mut info = ImageInfo::new();
        info.mimetype = Some(attachment.content_type.clone());
        info.size = Some(UInt::new_wrapping(attachment.data.len() as u64));
        RoomMessageEventContent::new(MessageType::Image(
            ImageMessageEventContent::new(attachment.filename.clone(), source).info(Box::new(info)),
        ))
    } else {
        RoomMessageEventContent::new(MessageType::File(FileMessageEventContent::new(
            attachment.filename.clone(),
            source,
        )))
    }
}

/// The mxc URI of an upload, plain or encrypted, for the log.
fn media_uri(source: &MediaSource) -> &str {
    match source {
        MediaSource::Plain(uri) => uri.as_str(),
        MediaSource::Encrypted(file) => file.url.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_ids_are_stable_per_email_room_and_part() {
        let base = post_transaction_base("abc@example.org", "📧 Hello");
        assert_eq!(base, post_transaction_base("abc@example.org", "📧 Hello"));
        assert_eq!(base.len(), 32);
        // a generated Message-ID shared by two different emails
        assert_ne!(
            post_transaction_base("uid-5", "📧 One"),
            post_transaction_base("uid-5", "📧 Two")
        );
        let room = "!room:example.org";
        assert_eq!(
            transaction_id(&base, room, "post"),
            transaction_id(&base, room, "post")
        );
        assert_ne!(
            transaction_id(&base, room, "post"),
            transaction_id(&base, "!other:example.org", "post")
        );
        assert_ne!(
            transaction_id(&base, room, "post"),
            transaction_id(&base, room, "attachment-0")
        );
    }

    fn content_json(attachment: &Attachment, source: MediaSource) -> serde_json::Value {
        let mime = attachment
            .content_type
            .parse()
            .unwrap_or(mime::APPLICATION_OCTET_STREAM);
        serde_json::to_value(attachment_content(attachment, &mime, source)).unwrap()
    }

    fn attachment(filename: &str, content_type: &str) -> Attachment {
        Attachment {
            filename: filename.to_owned(),
            content_type: content_type.to_owned(),
            data: vec![7; 1234],
        }
    }

    fn plain() -> MediaSource {
        MediaSource::Plain("mxc://example.org/abc".into())
    }

    #[test]
    fn attachments_in_a_plain_room_look_as_before() {
        assert_eq!(
            content_json(&attachment("photo.jpg", "image/jpeg"), plain()),
            serde_json::json!({
                "msgtype": "m.image",
                "body": "photo.jpg",
                "url": "mxc://example.org/abc",
                "info": { "mimetype": "image/jpeg", "size": 1234 }
            })
        );
        // anything else, audio and video too, a bare m.file
        for (name, content_type) in [
            ("minutes.pdf", "application/pdf"),
            ("voice.mp3", "audio/mpeg"),
            ("clip.mp4", "video/mp4"),
            ("odd.bin", "not a mime type"),
        ] {
            assert_eq!(
                content_json(&attachment(name, content_type), plain()),
                serde_json::json!({
                    "msgtype": "m.file",
                    "body": name,
                    "url": "mxc://example.org/abc"
                })
            );
        }
    }

    #[test]
    fn attachments_in_an_encrypted_room_carry_the_key_not_a_url() {
        let file = serde_json::from_value(serde_json::json!({
            "url": "mxc://example.org/enc",
            "key": {
                "kty": "oct", "key_ops": ["encrypt", "decrypt"], "alg": "A256CTR",
                "k": "qcHVMSgYg-71CauWBezXI5qkaRb0LuIy-Wx5kIaHMIA", "ext": true
            },
            "iv": "X85+XgHN+HEAAAAAAAAAAA",
            "hashes": { "sha256": "5qG4fFnbbVdlAB1Q72JDKwCagV6Dbkx9uds4rSak37c" },
            "v": "v2"
        }))
        .unwrap();
        let source = MediaSource::Encrypted(Box::new(file));
        assert_eq!(media_uri(&source), "mxc://example.org/enc");
        let content = content_json(&attachment("photo.png", "image/png"), source);
        assert!(content.get("url").is_none(), "{content}");
        assert_eq!(content["file"]["url"], "mxc://example.org/enc");
        assert_eq!(content["msgtype"], "m.image");
        assert_eq!(content["info"]["mimetype"], "image/png");
    }
}
