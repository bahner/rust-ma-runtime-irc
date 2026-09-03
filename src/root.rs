use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};

use crate::channel::{
    validate_irc_server_url, ChannelEntryPolicy, IrcBinding, PresenceRecord, RoomActor, RoomExit,
    RoomManifest, RoomSpec,
};

pub const CONCOURSE_ROOM: &str = "#concourse";

pub enum EnterResult {
    Joined,
    ConcourseGuide(String),
}

/// The result of a `dig` command: either a room created or entered in this
/// runtime, or a link to an existing room in another runtime.
#[derive(Debug, Clone)]
pub enum DigOutcome {
    Local {
        fragment: String,
        name: String,
        description: String,
    },
    Foreign {
        target: String,
        label: String,
    },
}

/// Minimal `:traverse` answer — the exit only says where it leads.
#[derive(Debug, Clone)]
pub struct TraverseReply {
    pub did: String,
    pub parent: String,
    pub text: String,
    pub exit: String,
    pub direction: String,
}

/// Plain room data for wire-level ctx assembly (the transport knows the
/// runtime DID and builds full actor addresses).
#[derive(Debug, Clone)]
pub struct RoomSnapshot {
    /// Internal fragment, including '#', e.g. "#room-a1b2c3d4e5".
    pub fragment: String,
    pub label: String,
    pub name: String,
    pub description: String,
    pub owner: Option<String>,
    pub locked: Option<String>,
    pub presence: Vec<PresenceRecord>,
    pub exits: Vec<RoomExit>,
    /// Untrusted IRC channel occupants (the room-held fake-actor list).
    pub irc_occupants: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoomGraphNode {
    pub fragment: String,
    pub label: String,
    pub description: String,
    pub exits: Vec<RoomExit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub irc: Option<IrcBinding>,
    pub presence_count: usize,
    pub is_concourse: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoomGraphSnapshot {
    pub rooms: Vec<RoomGraphNode>,
}

/// The serialisable root manifest: a single DAG-CBOR node holding every room's
/// durable state (identity, props, exits). Transient presence, IRC sessions,
/// root ownership, and claim state are not persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootManifest {
    pub rooms: Vec<RoomManifest>,
}

#[derive(Debug)]
pub struct RootActor {
    owners: HashSet<String>,
    claimed_by: Option<String>,
    entities: HashMap<String, RoomActor>,
    labels: HashMap<String, String>,
}

impl RootActor {
    pub fn new(owners: impl IntoIterator<Item = String>) -> Self {
        let mut entities = HashMap::new();
        entities.insert(
            CONCOURSE_ROOM.to_string(),
            RoomActor::new(CONCOURSE_ROOM, CONCOURSE_ROOM, ChannelEntryPolicy::open()),
        );
        let mut labels = HashMap::new();
        labels.insert(CONCOURSE_ROOM.to_string(), CONCOURSE_ROOM.to_string());

        Self {
            owners: owners.into_iter().collect(),
            claimed_by: None,
            entities,
            labels,
        }
    }

    /// Build the serialisable manifest from the current in-memory root,
    /// keeping only rooms and their durable props/exits.
    pub fn to_manifest(&self) -> RootManifest {
        let mut rooms = self
            .entities
            .values()
            .map(|room| RoomManifest {
                fragment: room.fragment.clone(),
                label: room.label.clone(),
                props: room.props.clone(),
                exits: room.exits.clone(),
                irc: room.irc.clone(),
            })
            .collect::<Vec<_>>();
        // Deterministic ordering so an unchanged root keeps a stable CID.
        rooms.sort_by(|left, right| left.fragment.cmp(&right.fragment));
        RootManifest { rooms }
    }

    /// Rebuild the root from a persisted manifest. Transient state starts
    /// empty and the configured owners are supplied by the caller.
    pub fn from_manifest(manifest: RootManifest, owners: impl IntoIterator<Item = String>) -> Self {
        let mut entities = HashMap::new();
        let mut labels = HashMap::new();
        for room in manifest.rooms {
            labels.insert(room.label.clone(), room.fragment.clone());
            entities.insert(room.fragment.clone(), RoomActor::from_manifest(room));
        }
        // `#concourse` always exists, even for a first-run or partial manifest.
        if !entities.contains_key(CONCOURSE_ROOM) {
            entities.insert(
                CONCOURSE_ROOM.to_string(),
                RoomActor::new(CONCOURSE_ROOM, CONCOURSE_ROOM, ChannelEntryPolicy::open()),
            );
            labels.insert(CONCOURSE_ROOM.to_string(), CONCOURSE_ROOM.to_string());
        }
        Self {
            owners: owners.into_iter().collect(),
            claimed_by: None,
            entities,
            labels,
        }
    }

    pub fn claim(&mut self, caller: &str) -> Result<()> {
        if let Some(current) = &self.claimed_by {
            if current == caller {
                return Ok(());
            }
            return Err(anyhow!("#root already claimed by another DID"));
        }
        self.claimed_by = Some(caller.to_string());
        Ok(())
    }

    pub fn is_owner(&self, did: &str) -> bool {
        self.owners.contains(did)
    }

    pub fn owners(&self) -> impl Iterator<Item = &str> {
        self.owners.iter().map(String::as_str)
    }

    pub fn dig_entity(&mut self, caller: &str, spec: RoomSpec) -> Result<()> {
        if !self.is_owner(caller) {
            return Err(anyhow!("only owners may dig entities"));
        }
        let label = spec.label.clone();
        let fragment = self.create_room(caller, &spec)?;
        // Link concourse → new room as an addressable exit child, so
        // `go <label>` works from the lobby. The target is stored as the
        // room fragment and resolved to the room's full DID-URL on the wire.
        if let Some(concourse) = self.entities.get_mut(CONCOURSE_ROOM) {
            if !concourse.exits.iter().any(|exit| exit.name == label) {
                concourse.add_exit(&label, &fragment)?;
            }
        }
        Ok(())
    }

    /// Create a room owned by `owner` and register its label. Returns the new
    /// room's fragment. The caller is responsible for linking it to a parent.
    fn create_room(&mut self, owner: &str, spec: &RoomSpec) -> Result<String> {
        let label = spec.label.clone();
        if label.trim_start_matches('#') == CONCOURSE_ROOM.trim_start_matches('#') {
            return Err(anyhow!("#concourse is a reserved room"));
        }
        if self.labels.contains_key(label.as_str()) {
            return Err(anyhow!("entity label already exists"));
        }

        let fragment = self.generate_fragment();
        self.labels.insert(label.clone(), fragment.clone());
        self.entities.insert(
            fragment.clone(),
            RoomActor::from_spec(
                fragment.clone(),
                spec.clone(),
                Some(owner.to_string()),
                ChannelEntryPolicy::open(),
            ),
        );
        Ok(fragment)
    }

    /// Dig a new room from inside `source` and enter it. Only the source
    /// room's owner may dig from it; the new room is owned by that caller.
    pub fn dig_from_room(
        &mut self,
        caller: &str,
        source: &str,
        spec: RoomSpec,
    ) -> Result<DigOutcome> {
        let source_fragment = self.resolve_entity_fragment(source)?;
        {
            let room = self
                .entities
                .get(&source_fragment)
                .ok_or_else(|| anyhow!("entity not found"))?;
            if room.owner() != Some(caller) {
                return Err(anyhow!("only the room owner may dig from this room"));
            }
        }

        if let Some(target) = spec.target.clone() {
            self.link_external(&source_fragment, &spec.label, &target)?;
            return Ok(DigOutcome::Foreign {
                target,
                label: spec.label,
            });
        }

        let label = spec.label.clone();
        let fragment = self.create_room(caller, &spec)?;
        // Link source → new room as an addressable exit child.
        if let Some(source_room) = self.entities.get_mut(&source_fragment) {
            if !source_room.exits.iter().any(|exit| exit.name == label) {
                source_room.add_exit(&label, &fragment)?;
            }
        }

        self.enter_entity(caller, &fragment, "{}")?;
        let room = self
            .entity_mut(&fragment)
            .ok_or_else(|| anyhow!("room missing after dig"))?;
        Ok(DigOutcome::Local {
            fragment: room.fragment.clone(),
            name: room.name().to_string(),
            description: room.description().to_string(),
        })
    }

    pub fn dig_or_enter(&mut self, caller: &str, spec: RoomSpec) -> Result<DigOutcome> {
        let label = spec.label.clone();
        if let Some(target) = spec.target.clone() {
            if !self.is_owner(caller) {
                return Err(anyhow!("only owners may dig entities"));
            }
            self.link_external(CONCOURSE_ROOM, &label, &target)?;
            return Ok(DigOutcome::Foreign { target, label });
        }

        if self.resolve_entity_fragment(&label).is_err() {
            self.dig_entity(caller, spec)?;
        }
        self.enter_entity(caller, &label, "{}")?;
        let room = self
            .entity_mut(&label)
            .ok_or_else(|| anyhow!("room missing after dig"))?;
        Ok(DigOutcome::Local {
            fragment: room.fragment.clone(),
            name: room.name().to_string(),
            description: room.description().to_string(),
        })
    }

    /// Link `source` to an existing room in another runtime. Creates an exit
    /// pointing at the full DID-URL but does not commit local presence; the
    /// caller is redirected to the foreign room instead.
    fn link_external(&mut self, source_fragment: &str, label: &str, target: &str) -> Result<()> {
        let source_room = self
            .entities
            .get_mut(source_fragment)
            .ok_or_else(|| anyhow!("entity not found"))?;
        source_room.add_exit(label, target)
    }

    fn generate_fragment(&self) -> String {
        loop {
            // Kind-prefixed fragment per the lambda-ma reference: dug rooms
            // are born as `room-<random>`, never a bare nanoid.
            let fragment = format!("#room-{}", nanoid!(10));
            if !self.entities.contains_key(fragment.as_str()) {
                return fragment;
            }
        }
    }

    pub fn delete_entity(&mut self, caller: &str, name_or_label: &str) -> Result<()> {
        if !self.is_owner(caller) {
            return Err(anyhow!("only owners may delete entities"));
        }
        let fragment = self.resolve_entity_fragment(name_or_label)?;
        if fragment == CONCOURSE_ROOM {
            return Err(anyhow!("#concourse cannot be deleted"));
        }
        let removed = self
            .entities
            .remove(fragment.as_str())
            .ok_or_else(|| anyhow!("entity not found"))?;
        let removed_fragment = fragment.clone();
        let removed_label = removed.label.clone();
        self.labels.remove(&removed_label);
        // Drop exits that pointed at the deleted room (concourse links and
        // any local fragment/label targets) so they never resolve to a dead
        // address on the wire.
        for room in self.entities.values_mut() {
            room.exits.retain(|exit| {
                exit.target != removed_fragment
                    && exit.target != removed_label
                    && !exit.target.ends_with(removed_fragment.as_str())
            });
        }
        Ok(())
    }

    pub fn enter_entity(
        &mut self,
        did: &str,
        entity_or_label: &str,
        ctx: &str,
    ) -> Result<EnterResult> {
        let fragment = self.resolve_entity_fragment(entity_or_label)?;
        let room = self
            .entities
            .get_mut(fragment.as_str())
            .ok_or_else(|| anyhow!("entity not found"))?;
        room.enter(did, ctx)?;

        if fragment == CONCOURSE_ROOM {
            return Ok(EnterResult::ConcourseGuide(self.concourse_instructions()));
        }

        Ok(EnterResult::Joined)
    }

    pub fn concourse_instructions(&self) -> String {
        let mut exits = self
            .entities
            .values()
            .filter(|entity| entity.fragment.as_str() != CONCOURSE_ROOM)
            .map(|entity| entity.label.clone())
            .collect::<Vec<_>>();
        exits.sort_unstable();

        let exits_text = if exits.is_empty() {
            "(no rooms yet)".to_string()
        } else {
            exits.join(", ")
        };

        format!(
            "Welcome to #concourse\nUse dig <name> to enter an existing room or create and enter a new one.\nLink an existing room in another runtime: dig <name> to <did:ma:...#fragment>\nSet room description: describe <room> <text_with_underscores>\nAdd exit: exit-add <room> <name> <did:ma:...#fragment>\nRemove exit: exit-del <room> <name>\nDelete room: delete <name>\nShow room ctx: ctx <room>\nList rooms with: exits\nInside a room: :prop <key> [value], :owner <did>, :unlock [secret], dig <name>, and IRC config with :irc-server, :irc-nick, :irc-channel, then :irc-connect (owners only).\nKnown rooms: {exits_text}"
        )
    }

    pub fn concourse_command(&mut self, caller: &str, input: &str) -> Result<String> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(self.concourse_instructions());
        }

        let mut parts = trimmed.split_whitespace();
        let raw_cmd = parts.next().unwrap_or_default();
        let cmd = raw_cmd.trim_start_matches(':').to_ascii_lowercase();

        match cmd.as_str() {
            "help" | "look" => Ok(self.concourse_instructions()),
            "exits" | "list" => {
                let mut exits = self
                    .entities
                    .values()
                    .filter(|entity| entity.fragment.as_str() != CONCOURSE_ROOM)
                    .map(|entity| entity.label.clone())
                    .collect::<Vec<_>>();
                exits.sort_unstable();
                if exits.is_empty() {
                    Ok("(no rooms yet)".to_string())
                } else {
                    Ok(exits.join("\n"))
                }
            }
            "dig" => {
                let args = parts.collect::<Vec<_>>();
                let spec = RoomSpec::from_tokens(&args)?;
                match self.dig_or_enter(caller, spec)? {
                    DigOutcome::Local { name, .. } => Ok(format!("entered {name}")),
                    DigOutcome::Foreign { target, label } => {
                        Ok(format!("linked {label} -> {target}"))
                    }
                }
            }
            "describe" => {
                let Some(room) = parts.next() else {
                    return Err(anyhow!("usage: describe <room> <text_with_underscores>"));
                };
                let description = parts.collect::<Vec<_>>().join(" ");
                if description.is_empty() {
                    return Err(anyhow!("usage: describe <room> <text_with_underscores>"));
                }
                let room_actor = self
                    .entity_mut(room)
                    .ok_or_else(|| anyhow!("entity not found"))?;
                room_actor.set_description(description.replace('_', " "));
                Ok(format!("updated description for {room}"))
            }
            "exit-add" => {
                let Some(room) = parts.next() else {
                    return Err(anyhow!("usage: exit-add <room> <name> <target>"));
                };
                let Some(exit_name) = parts.next() else {
                    return Err(anyhow!("usage: exit-add <room> <name> <target>"));
                };
                let Some(target) = parts.next() else {
                    return Err(anyhow!("usage: exit-add <room> <name> <target>"));
                };
                let room_actor = self
                    .entity_mut(room)
                    .ok_or_else(|| anyhow!("entity not found"))?;
                room_actor.add_exit(exit_name, target)?;
                Ok(format!("added exit {exit_name} -> {target}"))
            }
            "exit-del" => {
                let Some(room) = parts.next() else {
                    return Err(anyhow!("usage: exit-del <room> <name>"));
                };
                let Some(exit_name) = parts.next() else {
                    return Err(anyhow!("usage: exit-del <room> <name>"));
                };
                let room_actor = self
                    .entity_mut(room)
                    .ok_or_else(|| anyhow!("entity not found"))?;
                room_actor.remove_exit(exit_name)?;
                Ok(format!("removed exit {exit_name}"))
            }
            "delete" | "del" => {
                let Some(entity) = parts.next() else {
                    return Err(anyhow!("usage: delete <entity>"));
                };
                self.delete_entity(caller, entity)?;
                Ok(format!("deleted {entity}"))
            }
            "ctx" => {
                let Some(room) = parts.next() else {
                    return Err(anyhow!("usage: ctx <room>"));
                };
                let room_actor = self
                    .entity_mut(room)
                    .ok_or_else(|| anyhow!("entity not found"))?;
                let ctx = room_actor.ctx_view();
                let yaml = serde_yaml::to_string(&ctx)
                    .map_err(|error| anyhow!("serialising room ctx: {error}"))?;
                Ok(yaml)
            }
            "enter" => {
                let Some(entity) = parts.next() else {
                    return Err(anyhow!("usage: enter <entity>"));
                };
                match self.enter_entity(caller, entity, "{}")? {
                    EnterResult::Joined => Ok(format!("entered {entity}")),
                    EnterResult::ConcourseGuide(guide) => Ok(guide),
                }
            }
            other => Err(anyhow!(
                "unknown #concourse command '{other}' (try: help, exits, dig, describe, exit-add, exit-del, delete, ctx, enter)"
            )),
        }
    }

    pub fn room_command(&mut self, caller: &str, fragment: &str, input: &str) -> Result<String> {
        let mut parts = input.split_whitespace();
        let command = parts
            .next()
            .unwrap_or("look")
            .trim_start_matches(':')
            .to_ascii_lowercase();

        let is_runtime_owner = self.is_owner(caller);
        if matches!(command.as_str(), "irc-connect" | "irc-disconnect" | "owner")
            && !is_runtime_owner
        {
            return Err(anyhow!("only owners may do this"));
        }

        let room = self
            .entity_mut(fragment)
            .ok_or_else(|| anyhow!("entity not found"))?;

        match command.as_str() {
            "help" | "look" => {
                let exits = if room.exits.is_empty() {
                    "(no exits)".to_string()
                } else {
                    room.exits
                        .iter()
                        .map(|exit| format!("{} -> {}", exit.name, exit.target))
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                Ok(format!(
                    "{}\n{}\nExits:\n{exits}",
                    room.name(),
                    room.description()
                ))
            }
            "exits" => {
                if room.exits.is_empty() {
                    Ok("(no exits)".to_string())
                } else {
                    Ok(room
                        .exits
                        .iter()
                        .map(|exit| format!("{} -> {}", exit.name, exit.target))
                        .collect::<Vec<_>>()
                        .join("\n"))
                }
            }
            "say" => room.say(caller).map(|_| String::new()),
            "emote" => room.emote(caller).map(|_| String::new()),
            "prop" => {
                let Some(key) = parts.next() else {
                    return Err(anyhow!("usage: :prop <key> [value]"));
                };
                let value = parts.collect::<Vec<_>>().join(" ");
                if value.is_empty() {
                    Ok(room.get_prop(key).unwrap_or("(unset)").to_string())
                } else {
                    if key == "owner" {
                        return Err(anyhow!("owner is a protected prop; use :owner <did>"));
                    }
                    if !(is_runtime_owner || room.owner() == Some(caller)) {
                        return Err(anyhow!("only the room owner may set props"));
                    }
                    room.set_prop(key, &value);
                    Ok(format!("{key} set"))
                }
            }
            "owner" => {
                let Some(did) = parts.next() else {
                    return Err(anyhow!("usage: :owner <did>"));
                };
                if parts.next().is_some() {
                    return Err(anyhow!("usage: :owner <did>"));
                }
                room.set_owner(Some(did));
                Ok(format!("owner set to {did}"))
            }
            "unlock" => {
                let secret = parts.collect::<Vec<_>>().join(" ");
                if !room.is_locked() {
                    return Ok("room is not locked".to_string());
                }
                if is_runtime_owner || room.owner() == Some(caller) {
                    room.unlock();
                    return Ok("unlocked".to_string());
                }
                if secret.is_empty() {
                    return Err(anyhow!("usage: :unlock <secret>"));
                }
                if secret == room.locked_secret() {
                    room.unlock();
                    Ok("unlocked".to_string())
                } else {
                    Err(anyhow!("wrong unlock secret"))
                }
            }
            "irc-server" => {
                let value = parts.collect::<Vec<_>>().join(" ");
                if value.is_empty() {
                    return Ok(room
                        .irc
                        .as_ref()
                        .map(|irc| irc.server.as_str())
                        .filter(|value| !value.is_empty())
                        .unwrap_or("(IRC server not set)")
                        .to_string());
                }
                validate_irc_server_url(&value)?;
                room.irc_config_mut().server = value.clone();
                Ok(format!("IRC server set to {value}"))
            }
            "irc-nick" => {
                let value = parts.collect::<Vec<_>>().join(" ");
                if value.is_empty() {
                    return Ok(room
                        .irc
                        .as_ref()
                        .map(|irc| irc.nick.as_str())
                        .filter(|value| !value.is_empty())
                        .unwrap_or("(IRC nick not set)")
                        .to_string());
                }
                room.irc_config_mut().nick = value.clone();
                Ok(format!("IRC nick set to {value}"))
            }
            "irc-channel" => {
                let value = parts.collect::<Vec<_>>().join(" ");
                if value.is_empty() {
                    return Ok(room
                        .irc
                        .as_ref()
                        .map(|irc| irc.channel.as_str())
                        .filter(|value| !value.is_empty())
                        .unwrap_or("(IRC channel not set)")
                        .to_string());
                }
                if !value.starts_with('#') || value.len() == 1 {
                    return Err(anyhow!("usage: :irc-channel #<name>"));
                }
                room.irc_config_mut().channel = value.clone();
                Ok(format!("IRC channel set to {value}"))
            }
            "irc-connect" => {
                let binding = room
                    .irc
                    .as_ref()
                    .ok_or_else(|| anyhow!("set the IRC server and channel before :irc-connect"))?
                    .clone();
                binding.validate_for_join()?;
                room.irc_active = true;
                Ok(format!(
                    "connected {} on {}",
                    binding.channel, binding.server
                ))
            }
            "irc-disconnect" => {
                room.deactivate_irc();
                Ok(format!("disconnected {}", room.name()))
            }
            "leave" => {
                room.leave(caller);
                Ok(format!("left {}", room.name()))
            }
            other => Err(anyhow!(
                "unknown room command '{other}' (try: look, exits, say, emote, prop, owner, unlock, irc-server, irc-nick, irc-channel, irc-connect, irc-disconnect, leave)"
            )),
        }
    }

    pub fn entity_mut(&mut self, name: &str) -> Option<&mut RoomActor> {
        self.resolve_entity_fragment(name)
            .ok()
            .and_then(|fragment| self.entities.get_mut(fragment.as_str()))
    }

    /// The room's activated binding plus every occupant, when the room is
    /// currently bound to its channel.
    pub fn room_irc_joinable(
        &self,
        name: &str,
    ) -> Option<(String, IrcBinding, Vec<PresenceRecord>)> {
        let fragment = self.resolve_entity_fragment(name).ok()?;
        let room = self.entities.get(&fragment)?;
        let binding = room.joinable_binding()?.clone();
        let presence = room.presence.values().cloned().collect();
        Some((fragment, binding, presence))
    }

    /// Whether `did` has a live channel session in the room.
    pub fn room_irc_has_mirror(&self, name: &str, did: &str) -> bool {
        let Ok(fragment) = self.resolve_entity_fragment(name) else {
            return false;
        };
        self.entities
            .get(&fragment)
            .is_some_and(|room| room.irc_active && room.irc_has_mirror(did))
    }

    /// The room's configured channel name, if any.
    pub fn room_irc_channel(&self, name: &str) -> Option<String> {
        let fragment = self.resolve_entity_fragment(name).ok()?;
        self.entities
            .get(&fragment)
            .and_then(|room| room.irc.as_ref())
            .map(|binding| binding.channel.clone())
    }

    /// Public fragment lookup for bridge cleanup on room deletion.
    pub fn fragment_for(&self, name: &str) -> Option<String> {
        self.resolve_entity_fragment(name).ok()
    }

    pub fn has_room(&self, fragment: &str) -> bool {
        self.entities.contains_key(fragment)
    }

    /// Resolve a destination to a full DID-URL. Foreign DID-URLs pass
    /// through; local room fragments/labels are resolved against this
    /// runtime. Everything emitted on the wire is a full DID-URL.
    pub fn room_url(&self, runtime_did: &str, target: &str) -> Option<String> {
        if target.starts_with("did:") {
            Some(target.to_string())
        } else {
            self.resolve_entity_fragment(target)
                .ok()
                .map(|fragment| format!("{runtime_did}{fragment}"))
        }
    }

    /// Find the room that owns an exit fragment.
    pub fn find_exit(&self, fragment: &str) -> Option<(&str, &RoomExit)> {
        self.entities.values().find_map(|room| {
            room.exits
                .iter()
                .find(|exit| exit.fragment == fragment)
                .map(|exit| (room.fragment.as_str(), exit))
        })
    }

    /// Minimal `:traverse`. An unresolvable or empty target keeps the
    /// traveller in the source room; every address in the reply is a full
    /// DID-URL.
    pub fn exit_traverse(
        &self,
        exit_fragment: &str,
        caller: &str,
        did: Option<&str>,
        runtime_did: &str,
    ) -> Result<TraverseReply> {
        let (owner_fragment, exit) = self
            .find_exit(exit_fragment)
            .ok_or_else(|| anyhow!("exit not found"))?;
        let did = did.unwrap_or(caller).to_string();
        let direction = exit.name.clone();
        let exit_actor = format!("{runtime_did}{}", exit.fragment);
        let (parent, text) = match self.room_url(runtime_did, &exit.target) {
            Some(url) => (url, format!("You go {direction}.")),
            None => (
                format!("{runtime_did}{owner_fragment}"),
                "This exit leads nowhere.".to_string(),
            ),
        };
        Ok(TraverseReply {
            did,
            parent,
            text,
            exit: exit_actor,
            direction,
        })
    }

    /// Snapshot for wire-level ctx assembly.
    pub fn room_snapshot(&self, name_or_label: &str) -> Option<RoomSnapshot> {
        let fragment = self.resolve_entity_fragment(name_or_label).ok()?;
        let room = self.entities.get(&fragment)?;
        Some(RoomSnapshot {
            fragment: room.fragment.clone(),
            label: room.label.clone(),
            name: room.name().to_string(),
            description: room.description().to_string(),
            owner: room.owner().map(str::to_string),
            locked: room.get_prop("locked").map(str::to_string),
            presence: room.presence.values().cloned().collect(),
            exits: room.exits.clone(),
            irc_occupants: room.irc_occupant_nicks(),
        })
    }

    pub fn entities(&self) -> impl Iterator<Item = &str> {
        self.entities.keys().map(String::as_str)
    }

    pub fn room_graph_snapshot(&self) -> RoomGraphSnapshot {
        let mut rooms = self
            .entities
            .values()
            .map(|room| RoomGraphNode {
                fragment: room.fragment.clone(),
                label: room.label.clone(),
                description: room.description().to_string(),
                exits: room.exits.clone(),
                irc: room.irc.clone(),
                presence_count: room.presence.len(),
                is_concourse: room.fragment == CONCOURSE_ROOM,
            })
            .collect::<Vec<_>>();
        rooms.sort_by(|left, right| left.label.cmp(&right.label));
        RoomGraphSnapshot { rooms }
    }

    fn resolve_entity_fragment(&self, name_or_label: &str) -> Result<String> {
        if self.entities.contains_key(name_or_label) {
            return Ok(name_or_label.to_string());
        }
        if let Some(fragment) = self.labels.get(name_or_label) {
            return Ok(fragment.clone());
        }
        // Labels are stored plain ("lounge"); accept the legacy "#lounge"
        // spelling for internal command arguments.
        if let Some(stripped) = name_or_label.strip_prefix('#') {
            if let Some(fragment) = self.labels.get(stripped) {
                return Ok(fragment.clone());
            }
        }
        Err(anyhow!("entity not found"))
    }
}
