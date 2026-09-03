use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::channel::{desired_irc_nick, IrcBinding, IrcClient, IrcEvent, PresenceRecord};
use crate::root::RootActor;
use crate::rpc_transport::{
    broadcast_terms, child_ctx_for_irc_fake, child_ctx_for_presence, ctext, ctx_map, event_term,
    room_actor_url, send_unsolicited_term, RpcRuntime,
};

/// Seconds between authoritative `NAMES` resyncs of the room's untrusted
/// occupant list.
const CHANNEL_RESYNC_SECS: u64 = 60;

/// Commands and session events the room bridge consumes. One bridge task
/// exists per IRC-bound room; `JoinActor`/`LeaveActor`/`Say` come from the
/// RPC layer, `SessionEvent` from the room's per-occupant IRC sessions.
#[derive(Debug)]
pub enum BridgeMessage {
    JoinActor {
        did: String,
        ctx: String,
        reply: Option<oneshot::Sender<Result<(), String>>>,
    },
    LeaveActor {
        did: String,
    },
    Say {
        did: String,
        text: String,
        emote: bool,
    },
    Stop,
    SessionEvent {
        did: String,
        event: IrcEvent,
    },
}

struct RoomBridge {
    runtime: RpcRuntime,
    root: Arc<Mutex<RootActor>>,
    room_fragment: String,
    binding: IrcBinding,
    sessions: HashMap<String, IrcClient>,
    input: mpsc::Sender<BridgeMessage>,
}

/// Spawn the room's channel bridge and return its command channel.
pub fn spawn_room_bridge(
    runtime: RpcRuntime,
    root: Arc<Mutex<RootActor>>,
    room_fragment: String,
    binding: IrcBinding,
) -> mpsc::Sender<BridgeMessage> {
    let (input, rx) = mpsc::channel::<BridgeMessage>(128);
    let bridge = RoomBridge {
        runtime,
        root,
        room_fragment: room_fragment.clone(),
        binding,
        sessions: HashMap::new(),
        input: input.clone(),
    };
    tokio::spawn(async move {
        if let Err(error) = bridge.run(rx).await {
            warn!(room = %room_fragment, error = %error, "room IRC bridge terminated with error");
        }
    });
    input
}

impl RoomBridge {
    async fn run(mut self, mut rx: mpsc::Receiver<BridgeMessage>) -> Result<()> {
        let mut resync = tokio::time::interval(Duration::from_secs(CHANNEL_RESYNC_SECS));
        resync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            room = %self.room_fragment,
            channel = %self.binding.channel,
            server = %self.binding.server,
            "room IRC bridge started"
        );
        loop {
            tokio::select! {
                _ = resync.tick() => {
                    self.resync_occupants().await;
                }
                message = rx.recv() => {
                    let Some(message) = message else { break };
                    match message {
                        BridgeMessage::Stop => break,
                        BridgeMessage::JoinActor { did, ctx, reply } => {
                            let result = self.join_actor(&did, &ctx).await;
                            match reply {
                                Some(reply) => {
                                    let _ = reply.send(result.map_err(|error| error.to_string()));
                                }
                                None => {
                                    // Fire-and-forget joins (the `:irc-connect`
                                    // batch) must not silently strand an occupant
                                    // whose nick the channel rejected.
                                    if let Err(error) = &result {
                                        self.kick_occupant(&did, error).await;
                                    }
                                }
                            }
                        }
                        BridgeMessage::LeaveActor { did } => self.leave_actor(&did).await,
                        BridgeMessage::Say { did, text, emote } => self.say(&did, &text, emote).await,
                        BridgeMessage::SessionEvent { did, event } => self.handle_event(&did, event).await,
                    }
                }
            }
            if !self.room_exists().await {
                warn!(room = %self.room_fragment, "room no longer exists; stopping IRC bridge");
                break;
            }
        }
        self.shutdown().await;
        info!(room = %self.room_fragment, "room IRC bridge stopped");
        Ok(())
    }

    async fn room_exists(&self) -> bool {
        self.root.lock().await.has_room(&self.room_fragment)
    }

    async fn join_actor(&mut self, did: &str, ctx: &str) -> Result<()> {
        if let Some(client) = self.sessions.get(did).cloned() {
            return self.update_nick_if_changed(did, ctx, &client).await;
        }
        let simple_ctx =
            (!ctx.trim().is_empty() && !ctx.trim_start().starts_with('{')).then_some(ctx.trim());
        let desired = desired_irc_nick(simple_ctx, did);

        let (event_tx, mut event_rx) = mpsc::channel::<IrcEvent>(64);
        let input = self.input.clone();
        let did_for_events = did.to_string();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if input
                    .send(BridgeMessage::SessionEvent {
                        did: did_for_events.clone(),
                        event,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        match IrcClient::connect(&self.binding, &desired, event_tx).await {
            Ok(client) => {
                info!(room = %self.room_fragment, did, nick = %desired, "occupant joined the IRC channel");
                self.sessions.insert(did.to_string(), client);
                Ok(())
            }
            Err(error) => {
                warn!(room = %self.room_fragment, did, nick = %desired, error = %error, "occupant could not join the IRC channel");
                Err(error)
            }
        }
    }

    /// A re-entry with a new nick reaches IRC without reconnecting. The
    /// session stays keyed by `did`; only its presented nick is re-announced.
    /// The server's `NICK` reply flows back as `Renamed` and updates the
    /// room's mirror map, so nothing is changed here.
    async fn update_nick_if_changed(&self, did: &str, ctx: &str, client: &IrcClient) -> Result<()> {
        let simple_ctx =
            (!ctx.trim().is_empty() && !ctx.trim_start().starts_with('{')).then_some(ctx.trim());
        // Re-entry without a nick word carries no rename intent.
        let Some(simple_ctx) = simple_ctx else {
            return Ok(());
        };
        let desired = desired_irc_nick(Some(simple_ctx), did);

        let current = {
            let mut guard = self.root.lock().await;
            guard
                .entity_mut(&self.room_fragment)
                .and_then(|room| room.irc_mirror_nick(did).map(str::to_string))
                .unwrap_or_default()
        };

        if current.eq_ignore_ascii_case(&desired) {
            return Ok(());
        }

        client.change_nick(&desired).await?;
        info!(
            room = %self.room_fragment,
            did,
            old = %current,
            new = %desired,
            "occupant changed IRC nick"
        );
        Ok(())
    }

    /// Remove an occupant whose nick could not be registered on the channel
    /// and tell them to re-enter with a different nick. Used only for the
    /// fire-and-forget joins the `:irc-connect` batch sends.
    async fn kick_occupant(&self, did: &str, error: &anyhow::Error) {
        let room_actor = room_actor_url(&self.runtime.runtime_did, &self.room_fragment);
        let label = {
            let mut guard = self.root.lock().await;
            let Some(room) = guard.entity_mut(&self.room_fragment) else {
                return;
            };
            room.leave(did);
            room.unregister_irc_mirror(did);
            room.label.clone()
        };
        let ctx = ctx_map(vec![
            ("actor", ctext(&room_actor)),
            (
                "parent",
                ctext(&format!("{}#root", self.runtime.runtime_did)),
            ),
            ("kind", ctext("room")),
            ("protocol", ctext("/ma/room/0.0.1")),
            ("name", ctext(&label)),
            ("nick", ctext(&label)),
        ]);
        let text = format!(
            "your IRC nick could not be registered on the channel ({error}); re-enter with a different nick"
        );
        let term = event_term(":say", ctx, Some(&text));
        if let Err(error) = send_unsolicited_term(&self.runtime, &room_actor, did, &term).await {
            warn!(error = %error, to = %did, "failed to deliver nick-rejection say");
        }
        warn!(room = %self.room_fragment, did, "kicked occupant from the room (IRC nick rejected)");
    }

    async fn leave_actor(&mut self, did: &str) {
        if let Some(client) = self.sessions.remove(did) {
            let _ = client.quit().await;
        }
        let mut guard = self.root.lock().await;
        if let Some(room) = guard.entity_mut(&self.room_fragment) {
            room.unregister_irc_mirror(did);
        }
        info!(room = %self.room_fragment, did, "occupant left the IRC channel");
    }

    /// Send the caller's text to the channel under their own nick, then echo
    /// it into the room (IRC does not echo a sender's own PRIVMSG back).
    async fn say(&mut self, did: &str, text: &str, emote: bool) {
        let channel = self.binding.channel.clone();
        let Some(client) = self.sessions.get(did).cloned() else {
            warn!(room = %self.room_fragment, did, "say from an occupant without an IRC session");
            return;
        };
        let send = if emote {
            client.action(&channel, text).await
        } else {
            client.privmsg(&channel, text).await
        };
        if let Err(error) = send {
            warn!(room = %self.room_fragment, did, error = %error, "failed to send IRC message");
            return;
        }

        let verb = if emote { ":emote" } else { ":say" };
        let (term, presence) = {
            let mut guard = self.root.lock().await;
            let Some(room) = guard.entity_mut(&self.room_fragment) else {
                return;
            };
            let Some(nick) = room.irc_mirror_nick(did).map(str::to_string) else {
                return;
            };
            let Some(record) = room.presence.get(did).cloned() else {
                return;
            };
            let room_actor = room_actor_url(&self.runtime.runtime_did, &self.room_fragment);
            let ctx = child_ctx_for_presence(&room_actor, &record, Some(&nick));
            (
                vec![event_term(verb, ctx, Some(text))],
                room.presence
                    .values()
                    .cloned()
                    .collect::<Vec<PresenceRecord>>(),
            )
        };
        self.broadcast(term, presence).await;
    }

    async fn handle_event(&mut self, did: &str, event: IrcEvent) {
        match event {
            IrcEvent::Ready { nick } => {
                let mut guard = self.root.lock().await;
                if let Some(room) = guard.entity_mut(&self.room_fragment) {
                    room.register_irc_mirror(did, nick);
                }
            }
            IrcEvent::Message { nick, text } => self.channel_message(&nick, &text, false).await,
            IrcEvent::Action { nick, text } => self.channel_message(&nick, &text, true).await,
            IrcEvent::Joined { nick } => {
                let (terms, presence) = {
                    let mut guard = self.root.lock().await;
                    let Some(room) = guard.entity_mut(&self.room_fragment) else {
                        return;
                    };
                    if !room.observe_irc_join(&nick) {
                        return;
                    }
                    let ctx = child_ctx_for_irc_fake(
                        &self.runtime.runtime_did,
                        &self.room_fragment,
                        &nick,
                    );
                    (
                        vec![event_term(":arrive", ctx, None)],
                        room.presence
                            .values()
                            .cloned()
                            .collect::<Vec<PresenceRecord>>(),
                    )
                };
                self.broadcast(terms, presence).await;
            }
            IrcEvent::Parted { nick } | IrcEvent::Quit { nick } => {
                let (terms, presence) = {
                    let mut guard = self.root.lock().await;
                    let Some(room) = guard.entity_mut(&self.room_fragment) else {
                        return;
                    };
                    if !room.observe_irc_part(&nick) {
                        return;
                    }
                    let ctx = child_ctx_for_irc_fake(
                        &self.runtime.runtime_did,
                        &self.room_fragment,
                        &nick,
                    );
                    (
                        vec![event_term(":leave", ctx, None)],
                        room.presence
                            .values()
                            .cloned()
                            .collect::<Vec<PresenceRecord>>(),
                    )
                };
                self.broadcast(terms, presence).await;
            }
            IrcEvent::Renamed { old, new } => {
                let (terms, presence) = {
                    let mut guard = self.root.lock().await;
                    let Some(room) = guard.entity_mut(&self.room_fragment) else {
                        return;
                    };
                    let (fake_left, fake_arrived) = room.observe_irc_rename(&old, &new);
                    let mut terms = Vec::new();
                    if fake_left {
                        terms.push(event_term(
                            ":leave",
                            child_ctx_for_irc_fake(
                                &self.runtime.runtime_did,
                                &self.room_fragment,
                                &old,
                            ),
                            None,
                        ));
                    }
                    if fake_arrived {
                        terms.push(event_term(
                            ":arrive",
                            child_ctx_for_irc_fake(
                                &self.runtime.runtime_did,
                                &self.room_fragment,
                                &new,
                            ),
                            None,
                        ));
                    }
                    if terms.is_empty() {
                        return;
                    }
                    (
                        terms,
                        room.presence
                            .values()
                            .cloned()
                            .collect::<Vec<PresenceRecord>>(),
                    )
                };
                self.broadcast(terms, presence).await;
            }
            IrcEvent::Names { nicks } => {
                let mut guard = self.root.lock().await;
                if let Some(room) = guard.entity_mut(&self.room_fragment) {
                    room.sync_irc_occupants(&nicks);
                }
            }
            IrcEvent::Disconnected => {
                self.sessions.remove(did);
                let mut guard = self.root.lock().await;
                if let Some(room) = guard.entity_mut(&self.room_fragment) {
                    room.unregister_irc_mirror(did);
                }
                warn!(room = %self.room_fragment, did, "occupant IRC session disconnected");
            }
        }
    }

    /// Repeat channel speech into the room. Speech from one of our own
    /// mirrors is the echo of a message this bridge already broadcast when it
    /// was sent, so it is ignored here.
    async fn channel_message(&self, nick: &str, text: &str, emote: bool) {
        let (terms, presence) = {
            let mut guard = self.root.lock().await;
            let Some(room) = guard.entity_mut(&self.room_fragment) else {
                return;
            };
            if room.nick_is_irc_mirror(nick) {
                return;
            }
            // A speaker is a channel occupant by definition; make sure the
            // fake is known so occupants and speech stay consistent.
            room.observe_irc_join(nick);
            let ctx = child_ctx_for_irc_fake(&self.runtime.runtime_did, &self.room_fragment, nick);
            let verb = if emote { ":emote" } else { ":say" };
            (
                vec![event_term(verb, ctx, Some(text))],
                room.presence
                    .values()
                    .cloned()
                    .collect::<Vec<PresenceRecord>>(),
            )
        };
        self.broadcast(terms, presence).await;
    }

    async fn resync_occupants(&mut self) {
        // With no live channel session there is nothing to observe: drop the
        // stale untrusted list rather than presenting ghosts.
        let Some(client) = self.sessions.values().next().cloned() else {
            let mut guard = self.root.lock().await;
            if let Some(room) = guard.entity_mut(&self.room_fragment) {
                room.sync_irc_occupants(&[]);
            }
            return;
        };
        if let Err(error) = client.names(&self.binding.channel).await {
            warn!(room = %self.room_fragment, error = %error, "channel resync failed");
        }
    }

    async fn broadcast(&self, terms: Vec<ciborium::Value>, presence: Vec<PresenceRecord>) {
        if terms.is_empty() || presence.is_empty() {
            return;
        }
        let from_actor = room_actor_url(&self.runtime.runtime_did, &self.room_fragment);
        broadcast_terms(&self.runtime, &from_actor, &presence, &terms).await;
        debug!(room = %self.room_fragment, recipients = presence.len(), "broadcast room event");
    }

    async fn shutdown(&mut self) {
        for (did, client) in self.sessions.drain() {
            let _ = client.quit().await;
            let mut guard = self.root.lock().await;
            if let Some(room) = guard.entity_mut(&self.room_fragment) {
                room.unregister_irc_mirror(&did);
            }
        }
    }
}
