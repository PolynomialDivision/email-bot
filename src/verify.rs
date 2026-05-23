use matrix_sdk::{
    Client, Room, RoomState,
    ruma::{
        OwnedServerName, OwnedUserId, RoomOrAliasId,
        events::{
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            room::member::StrippedRoomMemberEvent,
            room::message::{MessageType, OriginalSyncRoomMessageEvent},
        },
    },
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use crate::config::{RoomAllowList, UserAllowList};

#[derive(Clone)]
pub struct BotState {
    pub bot_user_id: OwnedUserId,
    pub allowed_inviters: UserAllowList,
    pub allowed_rooms: RoomAllowList,
    pub admin_users: HashSet<OwnedUserId>,
    pub reset_allowed: Arc<Mutex<HashSet<OwnedUserId>>>,
}

pub fn register_handlers(client: &Client, state: BotState) {
    // Auto-join invited rooms (only from allowed_inviters)
    client.add_event_handler({
        let state = state.clone();
        move |ev: StrippedRoomMemberEvent, room: Room, client: Client| {
            let state = state.clone();
            async move {
                if ev.state_key != state.bot_user_id {
                    return;
                }
                if !state.allowed_inviters.allows(&ev.sender) {
                    warn!(
                        room_id = %room.room_id(),
                        sender = %ev.sender,
                        "Rejecting invite: inviter not in allowed_inviters"
                    );
                    room.leave().await.ok();
                    return;
                }
                if !state.allowed_rooms.allows(room.room_id()) {
                    warn!(
                        room_id = %room.room_id(),
                        sender = %ev.sender,
                        "Rejecting invite: room not in allowed_rooms"
                    );
                    room.leave().await.ok();
                    return;
                }

                let room_id = room.room_id().to_owned();
                info!(room_id = %room_id, sender = %ev.sender, "Accepted invite — scheduling join");

                // Matrix federation requires explicit via-server hints so the homeserver knows
                // which server to contact. Without them matrix.org returns 404 "No known servers".
                let mut via: Vec<OwnedServerName> = Vec::new();
                // The inviter's homeserver always knows the room.
                via.push(ev.sender.server_name().to_owned());
                // Include the room's own server if different (covers non-federated rooms).
                if let Some(s) = room_id.server_name() {
                    let s = s.to_owned();
                    if !via.contains(&s) {
                        via.push(s);
                    }
                }
                debug!(room_id = %room_id, via = ?via, "Via servers for join");

                tokio::spawn(async move {
                    let room_or_alias = match RoomOrAliasId::parse(room_id.as_str()) {
                        Ok(id) => id,
                        Err(e) => {
                            error!(room_id = %room_id, error = %e, "Invalid room ID — cannot join");
                            return;
                        }
                    };

                    let mut delay = 2u64;
                    const MAX_ATTEMPTS: u32 = 8;
                    for attempt in 1..=MAX_ATTEMPTS {
                        match client.join_room_by_id_or_alias(&room_or_alias, &via).await {
                            Ok(_) => {
                                info!(room_id = %room_id, "Joined room");
                                return;
                            }
                            Err(ref e) if mxbot_common::verify::is_join_terminal(e) => {
                                warn!(
                                    room_id = %room_id,
                                    error = %e,
                                    "Join failed with non-retryable error — giving up"
                                );
                                return;
                            }
                            Err(e) if attempt == MAX_ATTEMPTS => {
                                warn!(
                                    room_id = %room_id,
                                    error = %e,
                                    attempts = MAX_ATTEMPTS,
                                    "Join failed — max attempts reached, giving up"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    room_id = %room_id,
                                    error = %e,
                                    attempt,
                                    retry_in_secs = delay,
                                    "Join failed — retrying in {delay}s"
                                );
                                sleep(Duration::from_secs(delay)).await;
                                delay = (delay * 2).min(300);
                            }
                        }
                    }
                });
            }
        }
    });

    // To-device verification requests
    client.add_event_handler({
        let state = state.clone();
        move |ev: ToDeviceKeyVerificationRequestEvent, client: Client| {
            let state = state.clone();
            async move {
                let Some(request) = client
                    .encryption()
                    .get_verification_request(&ev.sender, &ev.content.transaction_id)
                    .await
                else {
                    warn!("to-device verification request object not found");
                    return;
                };
                tokio::spawn(mxbot_common::verify::handle_verification_request(
                    client,
                    Arc::clone(&state.reset_allowed),
                    request,
                ));
            }
        }
    });

    // In-room messages: verification requests and !reset-trust command
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, client: Client| {
            let state = state.clone();
            async move {
                if let MessageType::VerificationRequest(_) = &ev.content.msgtype {
                    let Some(request) = client
                        .encryption()
                        .get_verification_request(&ev.sender, &ev.event_id)
                        .await
                    else {
                        warn!("in-room verification request object not found");
                        return;
                    };
                    tokio::spawn(mxbot_common::verify::handle_verification_request(
                        client,
                        Arc::clone(&state.reset_allowed),
                        request,
                    ));
                    return;
                }

                if ev.sender == state.bot_user_id || room.state() != RoomState::Joined {
                    return;
                }

                let MessageType::Text(ref text) = ev.content.msgtype else {
                    return;
                };
                let raw = text.body.trim();

                if let Some(target) = raw.strip_prefix("!reset-trust ") {
                    if state.admin_users.contains(&ev.sender) {
                        match target.trim().parse::<OwnedUserId>() {
                            Ok(target_user) => {
                                state.reset_allowed.lock().await.insert(target_user.clone());
                                info!(
                                    "Trust reset allowed for {} (by {})",
                                    target_user, ev.sender
                                );
                            }
                            Err(_) => {
                                warn!("!reset-trust: invalid user ID '{}'", target.trim())
                            }
                        }
                    } else {
                        warn!("!reset-trust from non-admin {} — ignored", ev.sender);
                    }
                }
            }
        }
    });
}

