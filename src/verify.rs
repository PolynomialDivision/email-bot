use futures_util::StreamExt;
use matrix_sdk::{
    Client, Room, RoomState,
    encryption::verification::{
        SasState, Verification, VerificationRequest, VerificationRequestState,
    },
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

#[derive(Clone)]
pub struct BotState {
    pub bot_user_id: OwnedUserId,
    pub allowed_inviters: HashSet<OwnedUserId>,
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
                if !state.allowed_inviters.is_empty()
                    && !state.allowed_inviters.contains(&ev.sender)
                {
                    warn!(
                        room_id = %room.room_id(),
                        sender = %ev.sender,
                        "Rejecting invite (sender not in allowed_inviters)"
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
                            Err(ref e) if is_join_terminal(e) => {
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
                tokio::spawn(handle_verification_request(client, state, request));
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
                    tokio::spawn(handle_verification_request(client, state, request));
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

pub async fn handle_verification_request(
    client: Client,
    state: BotState,
    request: VerificationRequest,
) {
    let user_id = request.other_user_id();

    let already_verified = client
        .encryption()
        .get_user_devices(user_id)
        .await
        .map(|devices| devices.devices().any(|d| d.is_verified()))
        .unwrap_or(false);

    if already_verified {
        let allowed = state.reset_allowed.lock().await.remove(user_id);
        if !allowed {
            warn!(
                "Rejecting verification from {} — already has a verified device",
                user_id
            );
            request.cancel().await.ok();
            return;
        }
        info!(
            "Allowing re-verification for {} (trust was reset by admin)",
            user_id
        );
    }

    info!("Accepting verification from {user_id}");
    if let Err(e) = request.accept().await {
        error!("Failed to accept verification request: {e}");
        return;
    }

    let mut stream = request.changes();
    while let Some(state) = stream.next().await {
        match state {
            VerificationRequestState::Transitioned { verification } => {
                if let Verification::SasV1(sas) = verification {
                    tokio::spawn(handle_sas(sas));
                    break;
                }
            }
            VerificationRequestState::Done | VerificationRequestState::Cancelled(_) => break,
            _ => {}
        }
    }
}

async fn handle_sas(sas: matrix_sdk::encryption::verification::SasVerification) {
    info!(
        "SAS with {} {}",
        sas.other_device().user_id(),
        sas.other_device().device_id()
    );

    if let Err(e) = sas.accept().await {
        error!("Failed to accept SAS: {e}");
        return;
    }

    let mut stream = sas.changes();
    while let Some(state) = stream.next().await {
        match state {
            SasState::KeysExchanged { .. } => {
                info!("Auto-confirming emojis");
                if let Err(e) = sas.confirm().await {
                    error!("SAS confirm failed: {e}");
                    break;
                }
            }
            SasState::Done { .. } => {
                info!(
                    "Verification done: {} {}",
                    sas.other_device().user_id(),
                    sas.other_device().device_id()
                );
                break;
            }
            SasState::Cancelled(info) => {
                warn!("Verification cancelled: {}", info.reason());
                break;
            }
            _ => {}
        }
    }
}

pub async fn bootstrap_cross_signing(client: &Client, user_id: &OwnedUserId) {
    // If recovery already restored all three cross-signing keys, skip the upload.
    // On matrix.org the upload requires UIA (m.oauth) which a headless bot cannot
    // complete — calling bootstrap when the keys are already present just generates
    // a noisy 401 on every startup with no benefit.
    if let Some(status) = client.encryption().cross_signing_status().await {
        if status.has_master && status.has_self_signing && status.has_user_signing {
            info!(
                user_id = %user_id,
                "Cross-signing already complete (keys present) — skipping bootstrap"
            );
            return;
        }
        info!(
            user_id = %user_id,
            has_master = status.has_master,
            has_self_signing = status.has_self_signing,
            has_user_signing = status.has_user_signing,
            "Cross-signing incomplete — attempting bootstrap"
        );
    }
    match client.encryption().bootstrap_cross_signing(None).await {
        Ok(()) => info!(user_id = %user_id, "Cross-signing bootstrapped successfully"),
        Err(e) => warn!(
            user_id = %user_id,
            error = %e,
            "Cross-signing bootstrap failed (non-fatal — bot can still send/receive messages)"
        ),
    }
}

/// Returns true for join errors that will not resolve with a retry.
fn is_join_terminal(e: &matrix_sdk::Error) -> bool {
    let s = e.to_string();
    // 404 "No known servers" — room unreachable via federation even with via hints
    // M_FORBIDDEN — bot is banned from the room
    // M_UNKNOWN_TOKEN — access token is invalid
    s.contains("No known servers")
        || s.contains("M_FORBIDDEN")
        || s.contains("M_UNKNOWN_TOKEN")
        || s.contains("M_GUEST_ACCESS_FORBIDDEN")
}
