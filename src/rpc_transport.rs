use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use ma_core::config::SecretBundle;
use ma_core::{
    decode_content, new_ma_endpoint, Did, DidDocumentResolver, Inbox, MaEndpoint, Message,
    SigningKey, CONTENT_TYPE_TERM, INBOX_PROTOCOL_ID, IPFS_PROTOCOL_ID, MESSAGE_TYPE_RPC,
    MESSAGE_TYPE_RPC_REPLY, RPC_PROTOCOL_ID,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::bridge::{spawn_room_bridge, BridgeMessage};
use crate::channel::{IrcBinding, PresenceRecord, RoomSpec};
use crate::root::{
    DigOutcome, EnterResult, RoomSnapshot, RootActor, TraverseReply, CONCOURSE_ROOM,
};

#[derive(Clone)]
pub struct RpcRuntime {
    pub endpoint: Arc<dyn MaEndpoint>,
    pub rpc_inbox: Inbox<Message>,
    pub inbox_inbox: Inbox<Message>,
    pub ipfs_inbox: Inbox<Message>,
    pub resolver: Arc<dyn DidDocumentResolver>,
    pub signing_key: SigningKey,
    pub runtime_did: String,
    pub irc_bridges: Arc<tokio::sync::Mutex<HashMap<String, mpsc::Sender<BridgeMessage>>>>,
}

impl RpcRuntime {
    pub fn services(&self) -> Vec<String> {
        let endpoint_id = self.endpoint.id();
        vec![
            format!("/iroh/{endpoint_id}{RPC_PROTOCOL_ID}"),
            format!("/iroh/{endpoint_id}{INBOX_PROTOCOL_ID}"),
            format!("/iroh/{endpoint_id}{IPFS_PROTOCOL_ID}"),
        ]
    }
}

pub async fn init_rpc_runtime(
    runtime_did: &str,
    bundle: &SecretBundle,
    resolver: Arc<dyn DidDocumentResolver>,
) -> Result<RpcRuntime> {
    Did::validate(runtime_did).context("invalid runtime did")?;
    let signing_key = bundle.signing_key().context("building signing key")?;
    let encryption_key = bundle.encryption_key().context("building encryption key")?;

    let mut endpoint = new_ma_endpoint(
        bundle.iroh_secret_key,
        encryption_key,
        resolver.clone(),
        false,
    )
    .await
    .context("starting iroh endpoint")?;
    let rpc_inbox = endpoint.service(RPC_PROTOCOL_ID);
    let inbox_inbox = endpoint.service(INBOX_PROTOCOL_ID);
    let ipfs_inbox = endpoint.service(IPFS_PROTOCOL_ID);
    let endpoint: Arc<dyn MaEndpoint> = Arc::from(endpoint);

    Ok(RpcRuntime {
        endpoint,
        rpc_inbox,
        inbox_inbox,
        ipfs_inbox,
        resolver,
        signing_key,
        runtime_did: runtime_did.to_string(),
        irc_bridges: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    })
}

pub async fn run_rpc_loop(
    runtime: RpcRuntime,
    root: Arc<tokio::sync::Mutex<RootActor>>,
) -> Result<()> {
    info!(did = %runtime.runtime_did, "rpc loop started");
    loop {
        let now = now_secs();
        // Keep non-RPC queues drained so services stay healthy even before full handlers exist.
        runtime.inbox_inbox.drain(now);
        runtime.ipfs_inbox.drain(now);
        for message in runtime.rpc_inbox.drain(now) {
            if message.message_type != MESSAGE_TYPE_RPC {
                debug!(
                    message_type = %message.message_type,
                    from = %message.from,
                    id = %message.id,
                    "ignoring non-RPC message in rpc inbox"
                );
                continue;
            }
            debug!(from = %message.from, to = %message.to, id = %message.id, "dispatching RPC message");
            if let Err(error) = handle_rpc_message(&runtime, &root, &message).await {
                warn!(error = %error, from = %message.from, id = %message.id, "rpc handler error");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn handle_rpc_message(
    runtime: &RpcRuntime,
    root: &Arc<tokio::sync::Mutex<RootActor>>,
    message: &Message,
) -> Result<()> {
    let to = Did::try_from(message.to.as_str()).context("invalid RPC target did")?;

    let (_, payload) = decode_content(&message.content).context("decoding RPC multicodec")?;
    let term: CborValue =
        ciborium::de::from_reader(&mut &payload[..]).context("decoding RPC term")?;
    let (verb, args) = parse_term(&term)?;

    // Always answer ping. If addressed to a fragment, reply from that fragment;
    // if addressed to bare runtime DID, reply from bare runtime DID.
    if verb == ":ping" {
        debug!(from = %message.from, target = %message.to, "handling ping");
        let mut pong = Vec::new();
        ciborium::ser::into_writer(&CborValue::Text(":pong".to_string()), &mut pong)
            .context("encoding :pong")?;
        let from_actor = match to.fragment.as_deref() {
            Some(fragment) if !fragment.trim().is_empty() => {
                format!("{}#{fragment}", runtime.runtime_did)
            }
            _ => runtime.runtime_did.clone(),
        };
        send_reply(runtime, &from_actor, &message.from, &message.id, &pong).await?;
        return Ok(());
    }

    let Some(fragment) = to.fragment.as_deref() else {
        warn!(from = %message.from, verb = %verb, "ignoring unfragmented non-ping RPC");
        return Ok(());
    };

    match (fragment, verb.as_str()) {
        ("concourse", ":enter") => {
            info!(from = %message.from, fragment, verb = %verb, "handling room enter via #concourse");
            let nick = args.first().cloned().unwrap_or_default();
            let (parent, name, description) = {
                let mut guard = root.lock().await;
                match guard.enter_entity(&message.from, CONCOURSE_ROOM, "{}")? {
                    EnterResult::Joined | EnterResult::ConcourseGuide(_) => {}
                }
                let room = guard
                    .entity_mut(CONCOURSE_ROOM)
                    .ok_or_else(|| anyhow!("concourse room missing"))?;
                (
                    format!("{}#concourse", runtime.runtime_did),
                    room.label.clone(),
                    room.description().to_string(),
                )
            };

            let reply_payload = encode_enter_ok(&parent, &nick, &name, &description)?;
            let from_actor = format!("{}#concourse", runtime.runtime_did);
            send_reply(
                runtime,
                &from_actor,
                &message.from,
                &message.id,
                &reply_payload,
            )
            .await?;
        }
        ("root", ":enter") => {
            info!(from = %message.from, fragment, verb = %verb, "handling room enter via #root");
            let target_room = args
                .first()
                .filter(|room| !room.trim().is_empty())
                .map(String::as_str)
                .unwrap_or("#concourse");
            let nick = args.get(1).cloned().unwrap_or_default();
            let (parent, name, description) = {
                let mut guard = root.lock().await;
                match guard.enter_entity(&message.from, target_room, "{}")? {
                    EnterResult::Joined | EnterResult::ConcourseGuide(_) => {}
                }
                let room = guard
                    .entity_mut(target_room)
                    .ok_or_else(|| anyhow!("target room missing"))?;
                let parent = if target_room.starts_with("did:ma:") {
                    target_room.to_string()
                } else {
                    format!("{}{}", runtime.runtime_did, room.fragment)
                };
                (parent, room.label.clone(), room.description().to_string())
            };

            let reply_payload =
                match enter_room_channel(runtime, root, target_room, &message.from, &nick).await {
                    Ok(()) => encode_enter_ok(&parent, &nick, &name, &description)?,
                    Err(error) => encode_command_result(Err(error))?,
                };
            let from_actor = format!("{}#root", runtime.runtime_did);
            send_reply(
                runtime,
                &from_actor,
                &message.from,
                &message.id,
                &reply_payload,
            )
            .await?;
        }
        ("concourse", ":dig") => {
            info!(from = %message.from, fragment, verb = %verb, "digging into room");
            let tokens = args.iter().map(String::as_str).collect::<Vec<_>>();
            let result = match RoomSpec::from_tokens(&tokens) {
                Ok(spec) => root.lock().await.dig_or_enter(&message.from, spec),
                Err(error) => Err(error),
            };
            match result {
                Ok(outcome) => {
                    let reply_payload =
                        encode_dig_ok(runtime, root, &outcome, &message.from).await?;
                    let from_actor = format!("{}#concourse", runtime.runtime_did);
                    send_reply(
                        runtime,
                        &from_actor,
                        &message.from,
                        &message.id,
                        &reply_payload,
                    )
                    .await?;
                }
                Err(error) => {
                    let reply_payload = encode_command_result(Err(error))?;
                    let from_actor = format!("{}#concourse", runtime.runtime_did);
                    send_reply(
                        runtime,
                        &from_actor,
                        &message.from,
                        &message.id,
                        &reply_payload,
                    )
                    .await?;
                }
            }
        }
        ("concourse", ":look") => {
            info!(from = %message.from, verb = %verb, "handling #concourse look");
            let payload = {
                let guard = root.lock().await;
                match guard.room_snapshot(CONCOURSE_ROOM) {
                    Some(snapshot) => encode_room_ctx(&runtime.runtime_did, &guard, &snapshot),
                    None => encode_command_result(Err(anyhow!("concourse room missing"))),
                }
            }?;
            let from_actor = format!("{}#concourse", runtime.runtime_did);
            send_reply(runtime, &from_actor, &message.from, &message.id, &payload).await?;
        }
        ("concourse", _) => {
            info!(from = %message.from, fragment, verb = %verb, "handling #concourse command");
            let command = command_input(&verb, &args);
            // Resolve a room fragment before deletion so its IRC bridge can
            // be stopped afterwards.
            let doomed = if matches!(verb.as_str(), ":delete" | ":del") {
                match args.first() {
                    Some(name) => root.lock().await.fragment_for(name),
                    None => None,
                }
            } else {
                None
            };
            let result = root.lock().await.concourse_command(&message.from, &command);
            if result.is_ok() {
                if let Some(fragment) = doomed {
                    stop_room_bridge(runtime, &fragment).await;
                }
            }
            let reply_payload = encode_command_result(result)?;
            let from_actor = format!("{}#concourse", runtime.runtime_did);
            send_reply(
                runtime,
                &from_actor,
                &message.from,
                &message.id,
                &reply_payload,
            )
            .await?;
        }
        ("root", _) => {
            let reply_payload = encode_command_result(Err(anyhow!(
                "unsupported #root command '{verb}' (try: :enter)"
            )))?;
            let from_actor = format!("{}#root", runtime.runtime_did);
            send_reply(
                runtime,
                &from_actor,
                &message.from,
                &message.id,
                &reply_payload,
            )
            .await?;
        }
        _ => {
            info!(from = %message.from, fragment, verb = %verb, "handling room/exit command");

            // Addressable exit children (#exit-…) only answer :traverse with
            // a traverse-ctx; every address in the reply is a full DID-URL.
            let exit_fragment = format!("#{fragment}");
            let exit_reply =
                {
                    let guard = root.lock().await;
                    if guard.find_exit(&exit_fragment).is_none() {
                        None
                    } else {
                        let payload = if verb == ":traverse" {
                            let did = traversal_did(&term);
                            match guard.exit_traverse(
                                &exit_fragment,
                                &message.from,
                                did.as_deref(),
                                &runtime.runtime_did,
                            ) {
                                Ok(reply) => encode_traverse_ok(&reply),
                                Err(error) => encode_command_result(Err(error)),
                            }
                        } else {
                            encode_command_result(Err(anyhow!(
                                "unknown exit command '{verb}' (try: :traverse)"
                            )))
                        };
                        Some(payload.map(|bytes| {
                            (bytes, format!("{}{}", runtime.runtime_did, exit_fragment))
                        }))
                    }
                };
            if let Some(exit_reply) = exit_reply {
                let (payload, from_actor) = exit_reply?;
                send_reply(runtime, &from_actor, &message.from, &message.id, &payload).await?;
                return Ok(());
            }

            let room_fragment = format!("#{fragment}");

            // Standard room entry handshake: commit presence and reply with
            // the DID ctx, echoing the dialed (canonical, full) room DID-URL.
            if verb == ":enter" {
                let nick = args.first().cloned().unwrap_or_default();
                let entered = {
                    let mut guard = root.lock().await;
                    guard.enter_entity(&message.from, &room_fragment, &nick)
                };
                let payload = match entered {
                    Ok(_) => match enter_room_channel(
                        runtime,
                        root,
                        &room_fragment,
                        &message.from,
                        &nick,
                    )
                    .await
                    {
                        Ok(()) => encode_did_enter_ok(&message.from, &message.to, &nick),
                        Err(error) => encode_command_result(Err(error)),
                    },
                    Err(error) => encode_command_result(Err(error)),
                }?;
                send_reply(runtime, &message.to, &message.from, &message.id, &payload).await?;
                return Ok(());
            }

            // Room presentation ctx: children (exits + presence) so the
            // avatar resolver can find exits for `go`.
            if verb == ":look" {
                let payload = {
                    let guard = root.lock().await;
                    match guard.room_snapshot(&room_fragment) {
                        Some(snapshot) => encode_room_ctx(&runtime.runtime_did, &guard, &snapshot),
                        None => encode_command_result(Err(anyhow!("entity not found"))),
                    }
                }?;
                send_reply(runtime, &message.to, &message.from, &message.id, &payload).await?;
                return Ok(());
            }

            // Dig a new room from inside this room (owner-gated).
            if verb == ":dig" {
                let tokens = args.iter().map(String::as_str).collect::<Vec<_>>();
                let result = match RoomSpec::from_tokens(&tokens) {
                    Ok(spec) => {
                        let mut guard = root.lock().await;
                        guard.dig_from_room(&message.from, &room_fragment, spec)
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(outcome) => {
                        let payload = encode_dig_ok(runtime, root, &outcome, &message.from).await?;
                        send_reply(runtime, &message.to, &message.from, &message.id, &payload)
                            .await?;
                    }
                    Err(error) => {
                        let payload = encode_command_result(Err(error))?;
                        send_reply(runtime, &message.to, &message.from, &message.id, &payload)
                            .await?;
                    }
                }
                return Ok(());
            }

            let command = command_input(&verb, &args);
            let mut result =
                root.lock()
                    .await
                    .room_command(&message.from, &room_fragment, &command);

            if result.is_ok() {
                match verb.as_str() {
                    ":irc-connect" => {
                        // Activation is done in the room state; now connect every
                        // occupant already present.
                        if let Some((fragment, binding, presence)) =
                            root.lock().await.room_irc_joinable(&room_fragment)
                        {
                            let sender = bridge_sender_for(runtime, root, &fragment, binding).await;
                            for occupant in presence {
                                let _ = sender
                                    .send(BridgeMessage::JoinActor {
                                        did: occupant.did,
                                        ctx: occupant.ctx,
                                        reply: None,
                                    })
                                    .await;
                            }
                        }
                    }
                    ":say" | ":emote" => {
                        let emote = verb == ":emote";
                        let text = args.join(" ");
                        let connected = root
                            .lock()
                            .await
                            .room_irc_joinable(&room_fragment)
                            .is_some();
                        if connected {
                            let joined = root
                                .lock()
                                .await
                                .room_irc_has_mirror(&room_fragment, &message.from);
                            if !joined {
                                result = Err(anyhow!(
                                    "not connected to the IRC channel; use :irc-connect"
                                ));
                            } else {
                                let sender = runtime
                                    .irc_bridges
                                    .lock()
                                    .await
                                    .get(&room_fragment)
                                    .cloned();
                                match sender {
                                    Some(sender) => {
                                        let channel = root
                                            .lock()
                                            .await
                                            .room_irc_channel(&room_fragment)
                                            .unwrap_or_default();
                                        let _ = sender
                                            .send(BridgeMessage::Say {
                                                did: message.from.clone(),
                                                text,
                                                emote,
                                            })
                                            .await;
                                        result = Ok(format!("sent to {channel}"));
                                    }
                                    None => {
                                        result = Err(anyhow!(
                                        "IRC channel is not active in this room; use :irc-connect"
                                    ))
                                    }
                                }
                            }
                        } else {
                            result = match broadcast_room_speech(
                                runtime,
                                root,
                                &room_fragment,
                                &message.from,
                                &text,
                                emote,
                            )
                            .await
                            {
                                Ok(()) => Ok("ok".to_string()),
                                Err(error) => Err(error),
                            };
                        }
                    }
                    ":irc-disconnect" => {
                        // room_command already deactivated the channel; now stop
                        // the bridge so every session quits the channel.
                        stop_room_bridge(runtime, &room_fragment).await;
                        let mut guard = root.lock().await;
                        if let Some(room) = guard.entity_mut(&room_fragment) {
                            room.deactivate_irc();
                        }
                    }
                    ":leave" => {
                        let sender = runtime
                            .irc_bridges
                            .lock()
                            .await
                            .get(&room_fragment)
                            .cloned();
                        if let Some(sender) = sender {
                            let _ = sender
                                .send(BridgeMessage::LeaveActor {
                                    did: message.from.clone(),
                                })
                                .await;
                        }
                    }
                    _ => {}
                }
            }

            let reply_payload = encode_command_result(result)?;
            let from_actor = format!("{}#{fragment}", runtime.runtime_did);
            send_reply(
                runtime,
                &from_actor,
                &message.from,
                &message.id,
                &reply_payload,
            )
            .await?;
        }
    }

    Ok(())
}

fn parse_term(term: &CborValue) -> Result<(String, Vec<String>)> {
    match term {
        CborValue::Text(atom) => Ok((atom.clone(), Vec::new())),
        CborValue::Array(items) => {
            let Some(CborValue::Text(atom)) = items.first() else {
                return Err(anyhow!("invalid RPC tuple head"));
            };
            let args = items
                .iter()
                .skip(1)
                .filter_map(|item| match item {
                    CborValue::Text(text) => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Ok((atom.clone(), args))
        }
        _ => Err(anyhow!("invalid RPC term")),
    }
}

/// The `:traverse` payload is `[{did, parent}, …]`; `parse_term` drops
/// maps, so the traveller DID is read from the raw term.
fn traversal_did(term: &CborValue) -> Option<String> {
    let CborValue::Array(items) = term else {
        return None;
    };
    let CborValue::Map(entries) = items.get(1)? else {
        return None;
    };
    entries.iter().find_map(|(key, value)| {
        let CborValue::Text(k) = key else {
            return None;
        };
        if k != "did" {
            return None;
        }
        let CborValue::Text(did) = value else {
            return None;
        };
        Some(did.clone())
    })
}

fn command_input(verb: &str, args: &[String]) -> String {
    std::iter::once(verb)
        .chain(args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

fn encode_command_result(result: Result<String>) -> Result<Vec<u8>> {
    let (status, text) = match result {
        Ok(text) => (":ok", text),
        Err(error) => (":error", error.to_string()),
    };
    let tuple = CborValue::Array(vec![
        CborValue::Text(status.to_string()),
        CborValue::Text(text),
    ]);
    let mut payload = Vec::new();
    ciborium::ser::into_writer(&tuple, &mut payload).context("encoding command reply")?;
    Ok(payload)
}

pub(crate) fn ctext(value: &str) -> CborValue {
    CborValue::Text(value.to_string())
}

pub(crate) fn cint(value: i64) -> CborValue {
    CborValue::Integer(ciborium::value::Integer::from(value))
}

fn ok_tuple(payload: CborValue) -> Result<Vec<u8>> {
    let tuple = CborValue::Array(vec![CborValue::Text(":ok".to_string()), payload]);
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&tuple, &mut bytes).context("encoding ok reply")?;
    Ok(bytes)
}

pub(crate) fn ctx_map(fields: Vec<(&str, CborValue)>) -> CborValue {
    CborValue::Map(fields.into_iter().map(|(k, v)| (ctext(k), v)).collect())
}

fn encode_traverse_ok(reply: &TraverseReply) -> Result<Vec<u8>> {
    ok_tuple(ctx_map(vec![
        ("did", ctext(&reply.did)),
        ("parent", ctext(&reply.parent)),
        ("text", ctext(&reply.text)),
        ("exit", ctext(&reply.exit)),
        ("direction", ctext(&reply.direction)),
    ]))
}

fn encode_did_enter_ok(did: &str, parent: &str, nick: &str) -> Result<Vec<u8>> {
    let name = if nick.trim().is_empty() { did } else { nick };
    ok_tuple(ctx_map(vec![
        ("actor", ctext(did)),
        ("did", ctext(did)),
        ("parent", ctext(parent)),
        ("kind", ctext("h00man")),
        ("protocol", ctext("/ma/agent/0.0.1")),
        ("name", ctext(name)),
        ("nick", ctext(name)),
        ("description", ctext("A direct DID presence.")),
        ("rev", cint(1)),
    ]))
}

fn encode_room_ctx(
    runtime_did: &str,
    root: &RootActor,
    snapshot: &RoomSnapshot,
) -> Result<Vec<u8>> {
    let room_actor = room_actor_url(runtime_did, &snapshot.fragment);

    let mut children: Vec<(CborValue, CborValue)> = Vec::new();
    for presence in &snapshot.presence {
        children.push((
            ctext(&presence.did),
            child_ctx_for_presence(&room_actor, presence, None),
        ));
    }
    for exit in &snapshot.exits {
        let exit_actor = format!("{runtime_did}{}", exit.fragment);
        let mut fields = vec![
            ("actor", ctext(&exit_actor)),
            ("kind", ctext("exit")),
            ("protocol", ctext("/ma/exit/0.0.1")),
            ("parent", ctext(&room_actor)),
            ("name", ctext(&exit.name)),
            ("nick", ctext(&exit.name)),
            (
                "description",
                ctext(&format!("An exit leading {}.", exit.name)),
            ),
            ("direction", ctext(&exit.name)),
        ];
        if let Some(target) = root.room_url(runtime_did, &exit.target) {
            fields.push(("target-room", ctext(&target)));
        }
        children.push((ctext(&exit_actor), ctx_map(fields)));
    }
    // Untrusted IRC occupants, rendered as room children so clients list them
    // among "Occupants:".
    for nick in &snapshot.irc_occupants {
        let actor = irc_fake_actor(runtime_did, nick);
        children.push((
            ctext(&actor),
            child_ctx_for_irc_fake(runtime_did, &snapshot.fragment, nick),
        ));
    }

    let mut fields = vec![
        ("protocol", ctext("/ma/room/0.0.1")),
        ("kind", ctext("room")),
        ("actor", ctext(&room_actor)),
        ("parent", ctext(&format!("{runtime_did}#root"))),
        ("rev", cint(0)),
        ("name", ctext(&snapshot.name)),
        ("nick", ctext(&snapshot.name)),
        ("description", ctext(&snapshot.description)),
    ];
    if let Some(owner) = &snapshot.owner {
        fields.push(("owner", ctext(owner)));
    }
    fields.push(("children", CborValue::Map(children)));
    ok_tuple(ctx_map(fields))
}

pub(crate) fn room_actor_url(runtime_did: &str, fragment: &str) -> String {
    format!("{runtime_did}{fragment}")
}

pub(crate) fn irc_fake_actor(runtime_did: &str, nick: &str) -> String {
    let safe: String = nick
        .to_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let safe = &safe[..safe.len().min(48)];
    format!("{runtime_did}#irc-{safe}")
}

pub(crate) fn child_ctx_for_presence(
    room_actor: &str,
    presence: &PresenceRecord,
    nick_override: Option<&str>,
) -> CborValue {
    let nick = nick_override.unwrap_or_else(|| presence.display_nick());
    ctx_map(vec![
        ("actor", ctext(&presence.did)),
        ("did", ctext(&presence.did)),
        ("parent", ctext(room_actor)),
        ("kind", ctext("h00man")),
        ("protocol", ctext("/ma/agent/0.0.1")),
        ("name", ctext(nick)),
        ("nick", ctext(nick)),
        ("description", ctext("A direct DID presence.")),
        ("rev", cint(1)),
    ])
}

pub(crate) fn child_ctx_for_irc_fake(
    runtime_did: &str,
    room_fragment: &str,
    nick: &str,
) -> CborValue {
    let actor = irc_fake_actor(runtime_did, nick);
    let room_actor = room_actor_url(runtime_did, room_fragment);
    ctx_map(vec![
        ("actor", ctext(&actor)),
        ("did", ctext(&actor)),
        ("parent", ctext(&room_actor)),
        ("kind", ctext("h00man")),
        ("protocol", ctext("/ma/agent/0.0.1")),
        ("name", ctext(nick)),
        ("nick", ctext(nick)),
        ("description", ctext("An IRC user in the channel.")),
        ("rev", cint(1)),
    ])
}

pub(crate) fn event_term(verb: &str, ctx: CborValue, text: Option<&str>) -> CborValue {
    let mut items = vec![ctext(verb), ctx];
    if let Some(text) = text {
        items.push(ctext(text));
    }
    CborValue::Array(items)
}

fn encode_enter_ok(parent: &str, nick: &str, name: &str, description: &str) -> Result<Vec<u8>> {
    let map = CborValue::Map(vec![
        (
            CborValue::Text("parent".to_string()),
            CborValue::Text(parent.to_string()),
        ),
        (
            CborValue::Text("nick".to_string()),
            CborValue::Text(nick.to_string()),
        ),
        (
            CborValue::Text("name".to_string()),
            CborValue::Text(name.to_string()),
        ),
        (
            CborValue::Text("description".to_string()),
            CborValue::Text(description.to_string()),
        ),
        (
            CborValue::Text("rev".to_string()),
            CborValue::Integer(ciborium::value::Integer::from(1i64)),
        ),
    ]);
    let tuple = CborValue::Array(vec![CborValue::Text(":ok".to_string()), map]);
    let mut payload = Vec::new();
    ciborium::ser::into_writer(&tuple, &mut payload).context("encoding :enter reply")?;
    Ok(payload)
}

/// Encode the enter-ok reply for a `dig` outcome. Local rooms also join any
/// active IRC channel (a join failure becomes a `[:error, reason]` reply); a
/// foreign target is simply the DID-URL the client should enter next.
async fn encode_dig_ok(
    runtime: &RpcRuntime,
    root: &Arc<tokio::sync::Mutex<RootActor>>,
    outcome: &DigOutcome,
    did: &str,
) -> Result<Vec<u8>> {
    match outcome {
        DigOutcome::Local {
            fragment,
            name,
            description,
        } => match enter_room_channel(runtime, root, fragment, did, "").await {
            Ok(()) => {
                let parent = format!("{}{}", runtime.runtime_did, fragment);
                encode_enter_ok(&parent, "", name, description)
            }
            Err(error) => encode_command_result(Err(error)),
        },
        DigOutcome::Foreign { target, label } => encode_enter_ok(target, "", label, ""),
    }
}

async fn send_reply(
    runtime: &RpcRuntime,
    from_actor: &str,
    to_actor: &str,
    reply_to_id: &str,
    payload: &[u8],
) -> Result<()> {
    let reply = Message::new_reply(
        from_actor,
        to_actor,
        MESSAGE_TYPE_RPC_REPLY,
        CONTENT_TYPE_TERM,
        payload,
        reply_to_id,
        &runtime.signing_key,
    )
    .context("building RPC reply")?;

    debug!(
        from = from_actor,
        to = to_actor,
        reply_to = reply_to_id,
        message_type = MESSAGE_TYPE_RPC_REPLY,
        "sending RPC reply"
    );
    let mut outbox = runtime
        .endpoint
        .outbox(runtime.resolver.as_ref(), to_actor, RPC_PROTOCOL_ID)
        .await
        .context("opening RPC reply outbox")?;
    outbox.send(&reply).await.context("sending RPC reply")
}

/// Get (creating if needed) the bridge command channel for an IRC-bound room.
async fn bridge_sender_for(
    runtime: &RpcRuntime,
    root: &Arc<tokio::sync::Mutex<RootActor>>,
    room_fragment: &str,
    binding: IrcBinding,
) -> mpsc::Sender<BridgeMessage> {
    let mut registry = runtime.irc_bridges.lock().await;
    if let Some(existing) = registry.get(room_fragment) {
        return existing.clone();
    }
    let sender = spawn_room_bridge(
        runtime.clone(),
        root.clone(),
        room_fragment.to_string(),
        binding,
    );
    registry.insert(room_fragment.to_string(), sender.clone());
    sender
}

async fn stop_room_bridge(runtime: &RpcRuntime, room_fragment: &str) {
    let sender = runtime.irc_bridges.lock().await.remove(room_fragment);
    if let Some(sender) = sender {
        let _ = sender.send(BridgeMessage::Stop).await;
    }
}

/// When the room is IRC-bound and active, make sure its bridge exists and
/// ask it to connect `did` to the channel under the occupant's own nick. The
/// join is awaited so the entry result reflects whether the nick was accepted.
async fn maybe_join_room_channel(
    runtime: &RpcRuntime,
    root: &Arc<tokio::sync::Mutex<RootActor>>,
    room_ref: &str,
    did: &str,
    ctx: &str,
) -> Result<()> {
    let Some((fragment, binding, _)) = root.lock().await.room_irc_joinable(room_ref) else {
        return Ok(());
    };
    let sender = bridge_sender_for(runtime, root, &fragment, binding).await;
    let (reply, reply_rx) = oneshot::channel();
    sender
        .send(BridgeMessage::JoinActor {
            did: did.to_string(),
            ctx: ctx.to_string(),
            reply: Some(reply),
        })
        .await
        .map_err(|_| anyhow!("IRC bridge is not available"))?;
    reply_rx
        .await
        .map_err(|_| anyhow!("IRC bridge did not respond"))?
        .map_err(anyhow::Error::msg)
}

/// Entry into a channel room is itself a join. On join failure, roll back the
/// just-committed presence and tell the caller their nick could not be accepted.
async fn enter_room_channel(
    runtime: &RpcRuntime,
    root: &Arc<tokio::sync::Mutex<RootActor>>,
    room_ref: &str,
    did: &str,
    nick: &str,
) -> Result<()> {
    if let Err(error) = maybe_join_room_channel(runtime, root, room_ref, did, nick).await {
        let (fragment, label) = {
            let mut guard = root.lock().await;
            let fragment = guard
                .fragment_for(room_ref)
                .unwrap_or_else(|| room_ref.to_string());
            let label = if let Some(room) = guard.entity_mut(&fragment) {
                // A re-entry whose nick change was rejected keeps the previous,
                // still-valid nick; a first entry that failed has no prior
                // presence to restore and is simply removed.
                match room.irc_mirror_nick(did).map(str::to_string) {
                    Some(previous) => {
                        let _ = room.enter(did, &previous);
                    }
                    None => room.leave(did),
                }
                room.label.clone()
            } else {
                fragment.clone()
            };
            (fragment, label)
        };
        send_nick_rejected_say(runtime, &fragment, &label, did).await;
        return Err(error);
    }
    Ok(())
}

/// An unsolicited `:say` from the room telling the caller their nick was not
/// accepted, so the rejection is visible even when the `:enter` reply is
/// consumed as a plain error.
async fn send_nick_rejected_say(runtime: &RpcRuntime, fragment: &str, label: &str, did: &str) {
    let room_actor = room_actor_url(&runtime.runtime_did, fragment);
    let ctx = ctx_map(vec![
        ("actor", ctext(&room_actor)),
        ("parent", ctext(&format!("{}#root", runtime.runtime_did))),
        ("kind", ctext("room")),
        ("protocol", ctext("/ma/room/0.0.1")),
        ("name", ctext(label)),
        ("nick", ctext(label)),
    ]);
    let term = event_term(":say", ctx, Some("your nick cannot be accepted"));
    if let Err(error) = send_unsolicited_term(runtime, &room_actor, did, &term).await {
        warn!(error = %error, to = %did, "failed to deliver nick-rejected say");
    }
}

pub(crate) async fn send_unsolicited_term(
    runtime: &RpcRuntime,
    from_actor: &str,
    to_actor: &str,
    term: &CborValue,
) -> Result<()> {
    let mut payload = Vec::new();
    ciborium::ser::into_writer(term, &mut payload).context("encoding unsolicited term")?;
    let message = Message::new(
        from_actor,
        to_actor,
        MESSAGE_TYPE_RPC,
        CONTENT_TYPE_TERM,
        &payload,
        &runtime.signing_key,
    )
    .context("building unsolicited RPC message")?;
    let mut outbox = runtime
        .endpoint
        .outbox(runtime.resolver.as_ref(), to_actor, RPC_PROTOCOL_ID)
        .await
        .context("opening event outbox")?;
    outbox
        .send(&message)
        .await
        .context("sending unsolicited term")
}

pub(crate) async fn broadcast_terms(
    runtime: &RpcRuntime,
    from_actor: &str,
    presences: &[PresenceRecord],
    terms: &[CborValue],
) {
    for presence in presences {
        for term in terms {
            if let Err(error) =
                send_unsolicited_term(runtime, from_actor, &presence.did, term).await
            {
                warn!(error = %error, to = %presence.did, "room event delivery failed");
            }
        }
    }
}

/// Broadcast a room-local `:say`/`:emote` to every occupant. This is the
/// ordinary ma-room behaviour for a room that is not IRC-connected.
async fn broadcast_room_speech(
    runtime: &RpcRuntime,
    root: &Arc<tokio::sync::Mutex<RootActor>>,
    room_fragment: &str,
    did: &str,
    text: &str,
    emote: bool,
) -> Result<()> {
    let (from_actor, terms, presence) = {
        let mut guard = root.lock().await;
        let room = guard
            .entity_mut(room_fragment)
            .ok_or_else(|| anyhow!("entity not found"))?;
        let record = room
            .presence
            .get(did)
            .cloned()
            .ok_or_else(|| anyhow!("sender is not in the room"))?;
        let from_actor = room_actor_url(&runtime.runtime_did, room_fragment);
        let ctx = child_ctx_for_presence(&from_actor, &record, None);
        let verb = if emote { ":emote" } else { ":say" };
        (
            from_actor,
            vec![event_term(verb, ctx, Some(text))],
            room.presence.values().cloned().collect::<Vec<_>>(),
        )
    };
    broadcast_terms(runtime, &from_actor, &presence, &terms).await;
    Ok(())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
