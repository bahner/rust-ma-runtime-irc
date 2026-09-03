pub mod acl;
pub mod bridge;
pub mod channel;
pub mod config;
pub mod did_publish;
pub mod kubo;
pub mod root;
pub mod root_publish;
pub mod rpc_transport;
pub mod status;

#[cfg(test)]
mod tests {
    use crate::acl::{is_allowed, load_transport_acl_from_yaml};
    use crate::channel::{ChannelEntryPolicy, RoomSpec};
    use crate::config::RuntimeConfig;
    use crate::root::{DigOutcome, RootActor, RootManifest, CONCOURSE_ROOM};

    #[test]
    fn root_claim_is_first_writer() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        assert!(root.claim("did:ma:owner").is_ok());
        assert!(root.claim("did:ma:owner").is_ok());
        assert!(root.claim("did:ma:other").is_err());
    }

    #[test]
    fn only_owners_can_create_rooms() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        root.claim("did:ma:owner").expect("owner claim");

        let spec_ok = RoomSpec::from_dig("rust desc:first_room").expect("valid dig");
        let spec_denied = RoomSpec::from_dig("denied").expect("valid dig");

        assert!(root.dig_entity("did:ma:owner", spec_ok).is_ok());
        assert!(root.dig_entity("did:ma:user", spec_denied).is_err());
    }

    #[test]
    fn deny_wins_in_entry_policy() {
        let mut policy = ChannelEntryPolicy::closed();
        policy.allow_did("did:ma:alice");
        policy.deny_did("did:ma:alice");

        assert!(!policy.can_enter("did:ma:alice"));
    }

    #[test]
    fn closed_policy_requires_allowlist() {
        let mut policy = ChannelEntryPolicy::closed();
        assert!(!policy.can_enter("did:ma:alice"));
        policy.allow_did("did:ma:alice");
        assert!(policy.can_enter("did:ma:alice"));
    }

    #[test]
    fn transport_acl_uses_ma_core_semantics() {
        let yaml = r#"
acl:
  "*": [rpc]
  "did:ma:blocked": null
"#;
        let acl = load_transport_acl_from_yaml(yaml).expect("acl parse");
        assert!(is_allowed(&acl, "did:ma:anyone", "rpc"));
        assert!(!is_allowed(&acl, "did:ma:blocked", "rpc"));
    }

    #[test]
    fn runtime_config_defaults_use_irc_slug_and_status_port() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.slug, "irc");
        assert_eq!(cfg.status_bind, "127.0.0.1:5667");
        assert_eq!(cfg.kubo_rpc_url, "http://127.0.0.1:5001");
    }

    #[test]
    fn runtime_config_reads_irc_settings_from_ma_core_extras() {
        let core = ma_core::config::Config::from_yaml_str(
            r#"
slug: irc
status_bind: 127.0.0.1:6000
owners: [did:ma:owner]
acl_file: transport.acl.yaml
"#,
        )
        .expect("ma-core config");

        let cfg = RuntimeConfig::from_core(&core).expect("IRC config");
        assert_eq!(cfg.status_bind, "127.0.0.1:6000");
        assert_eq!(cfg.owners, ["did:ma:owner"]);
        assert_eq!(
            cfg.acl_file.as_deref(),
            Some(std::path::Path::new("transport.acl.yaml"))
        );
    }

    #[test]
    fn root_has_default_concourse_room() {
        let root = RootActor::new(["did:ma:owner".to_string()]);
        assert!(root.entities().any(|name| name == CONCOURSE_ROOM));
    }

    #[test]
    fn root_manifest_persists_rooms_exits_and_props_only() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        root.claim("did:ma:owner").expect("owner claim");
        root.dig_entity(
            "did:ma:owner",
            RoomSpec::from_dig("hub description:main_hub").expect("dig parse"),
        )
        .expect("dig entity");
        root.enter_entity("did:ma:user", "hub", "{}")
            .expect("enter room");
        {
            let hub = root.entity_mut("hub").expect("dug room");
            let irc = hub.irc_config_mut();
            irc.server = "irc://127.0.0.1:6667".to_string();
            irc.channel = "#bar".to_string();
            irc.nick = "foo".to_string();
        }

        let manifest = root.to_manifest();
        let json = serde_json::to_string(&manifest).expect("serialise manifest");
        let decoded: RootManifest = serde_json::from_str(&json).expect("deserialise manifest");
        let mut restored = RootActor::from_manifest(decoded, ["did:ma:owner".to_string()]);

        assert!(restored.has_room(CONCOURSE_ROOM));

        let hub_fragment = {
            let hub = restored
                .entity_mut("hub")
                .expect("dug room restored by label");
            assert_eq!(hub.description(), "main hub");
            assert!(hub.presence.is_empty(), "presence must stay in memory only");
            let irc = hub.irc.as_ref().expect("irc binding restored");
            assert_eq!(irc.server, "irc://127.0.0.1:6667");
            assert_eq!(irc.channel, "#bar");
            assert_eq!(irc.nick, "foo");
            hub.fragment.clone()
        };

        let concourse = restored
            .entity_mut(CONCOURSE_ROOM)
            .expect("concourse restored");
        assert!(
            concourse
                .exits
                .iter()
                .any(|exit| exit.target == hub_fragment),
            "concourse -> hub exit restored"
        );
    }

    #[test]
    fn dig_parser_extracts_kind_and_options() {
        let spec = RoomSpec::from_dig("bahner description:my_secret_room").expect("dig parse");
        assert_eq!(spec.label, "bahner");
        assert_eq!(spec.description, "my secret room");
    }

    #[test]
    fn dig_parser_rejects_irc_binding_fields() {
        let room_with_server = RoomSpec::from_dig("x server:localhost nick:foo join:#chan");
        assert!(room_with_server.is_err());
    }

    #[test]
    fn dig_parser_extracts_external_target() {
        let spec = RoomSpec::from_dig("portal to did:ma:other#room-x").expect("dig parse");
        assert_eq!(spec.label, "portal");
        assert_eq!(spec.description, "");
        assert_eq!(spec.target.as_deref(), Some("did:ma:other#room-x"));
    }

    #[test]
    fn dig_parser_rejects_malformed_external_target() {
        assert!(RoomSpec::from_dig("portal to").is_err());
        assert!(RoomSpec::from_dig("portal to not-a-did").is_err());
        assert!(RoomSpec::from_dig("portal to did:ma:other#room-x description:no").is_err());
    }

    #[test]
    fn dig_parser_rejects_double_fragment_target() {
        // A full room DID-URL already ends in its own fragment; appending
        // another `#fragment` makes a malformed address that must fail at
        // parse time rather than being stored as an undeliverable exit.
        assert!(RoomSpec::from_dig("concourse to did:ma:other#room-a#concourse").is_err());
        assert!(RoomSpec::from_dig("concourse to did:ma:other#room-a#").is_err());
        assert!(RoomSpec::from_dig("concourse to did:ma:other").is_err());
    }

    #[test]
    fn concourse_command_digs_and_lists_exits() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        let dug = root
            .concourse_command(
                "did:ma:owner",
                "dig bahner description:room_for_hidden_work",
            )
            .expect("dig from concourse");
        assert!(dug.contains("entered bahner"));

        let exits = root
            .concourse_command("did:ma:owner", "exits")
            .expect("list exits");
        assert!(exits.contains("bahner"));
    }

    #[test]
    fn dig_enters_an_existing_room_without_recreating_it() {
        let owner = "did:ma:owner";
        let visitor = "did:ma:visitor";
        let mut root = RootActor::new([owner.to_string()]);
        root.dig_or_enter(
            owner,
            RoomSpec::from_dig("lounge description:first_room").expect("room spec"),
        )
        .expect("owner creates room");

        let outcome = root
            .dig_or_enter(
                visitor,
                RoomSpec::from_dig("lounge").expect("existing room spec"),
            )
            .expect("visitor enters existing room");
        let DigOutcome::Local {
            fragment,
            description,
            ..
        } = outcome
        else {
            panic!("expected a local room outcome");
        };
        assert_eq!(description, "first room");
        assert_eq!(root.entities().filter(|room| *room == fragment).count(), 1);
    }

    #[test]
    fn dig_to_links_an_external_room() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);

        let outcome = root
            .dig_or_enter(
                owner,
                RoomSpec::from_dig("portal to did:ma:other#room-x").expect("dig spec"),
            )
            .expect("owner digs to external room");
        let DigOutcome::Foreign { target, label } = outcome else {
            panic!("expected a foreign room outcome");
        };
        assert_eq!(label, "portal");
        assert_eq!(target, "did:ma:other#room-x");

        // The concourse exit points at the foreign DID-URL and no local room
        // was created for the target.
        let concourse = root
            .room_snapshot(CONCOURSE_ROOM)
            .expect("concourse snapshot");
        let exit = concourse
            .exits
            .iter()
            .find(|exit| exit.name == "portal")
            .expect("concourse exit for portal");
        assert_eq!(exit.target, "did:ma:other#room-x");
        assert!(root.room_snapshot("portal").is_none());
    }

    #[test]
    fn dig_to_overwrites_existing_exit_target() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);

        let first = root
            .dig_or_enter(
                owner,
                RoomSpec::from_dig("portal to did:ma:other#room-x").expect("dig spec"),
            )
            .expect("first dig to external room");
        let DigOutcome::Foreign { target, label } = first else {
            panic!("expected a foreign room outcome");
        };
        assert_eq!(label, "portal");
        assert_eq!(target, "did:ma:other#room-x");

        // Re-digging the same label to a new target is idempotent: it re-points
        // the existing exit instead of erroring or adding a duplicate.
        let second = root
            .dig_or_enter(
                owner,
                RoomSpec::from_dig("portal to did:ma:other#room-y").expect("dig spec"),
            )
            .expect("second dig to external room");
        let DigOutcome::Foreign { target, .. } = second else {
            panic!("expected a foreign room outcome");
        };
        assert_eq!(target, "did:ma:other#room-y");

        let concourse = root
            .room_snapshot(CONCOURSE_ROOM)
            .expect("concourse snapshot");
        let portal_exits = concourse
            .exits
            .iter()
            .filter(|exit| exit.name == "portal")
            .collect::<Vec<_>>();
        assert_eq!(portal_exits.len(), 1, "expected exactly one portal exit");
        assert_eq!(portal_exits[0].target, "did:ma:other#room-y");
    }

    #[test]
    fn dig_to_requires_owner_from_concourse() {
        let owner = "did:ma:owner";
        let visitor = "did:ma:visitor";
        let mut root = RootActor::new([owner.to_string()]);

        let result = root.dig_or_enter(
            visitor,
            RoomSpec::from_dig("portal to did:ma:other#room-x").expect("dig spec"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn entered_room_supports_essential_commands() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig lounge description:a_quiet_room")
            .expect("dig lounge");
        root.enter_entity(owner, "lounge", "{}")
            .expect("enter lounge");

        let look = root
            .room_command(owner, "lounge", ":look")
            .expect("look around");
        assert!(look.contains("lounge\na quiet room"));
        assert!(look.contains("(no exits)"));
        assert!(root.room_command(owner, "lounge", ":say hello").is_ok());
        assert!(root.room_command(owner, "lounge", ":emote waves").is_ok());
        assert_eq!(
            root.room_command(owner, "lounge", ":leave").expect("leave"),
            "left lounge"
        );
        assert!(root.room_command(owner, "lounge", ":say hello").is_err());
    }

    #[test]
    fn room_builds_irc_configuration_incrementally() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig lounge")
            .expect("dig lounge");

        assert!(root.room_command(owner, "lounge", ":irc-connect").is_err());
        assert!(root
            .room_command(owner, "lounge", ":server 127.0.0.1:6667")
            .is_err());
        root.room_command(owner, "lounge", ":irc-server irc://127.0.0.1:6667")
            .expect("set server");
        root.room_command(owner, "lounge", ":irc-nick ma-test")
            .expect("set nick");
        root.room_command(owner, "lounge", ":irc-channel #ma-test")
            .expect("set channel");

        assert_eq!(
            root.room_command(owner, "lounge", ":irc-connect")
                .expect("connect config"),
            "connected #ma-test on irc://127.0.0.1:6667"
        );
    }

    #[test]
    fn only_owners_can_toggle_irc_connection() {
        let owner = "did:ma:owner";
        let visitor = "did:ma:visitor";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig lounge")
            .expect("dig lounge");

        root.room_command(owner, "lounge", ":irc-server irc://127.0.0.1:6667")
            .expect("set server");
        root.room_command(owner, "lounge", ":irc-channel #chan")
            .expect("set channel");
        assert!(root.room_command(owner, "lounge", ":irc-connect").is_ok());

        assert!(root
            .room_command(visitor, "lounge", ":irc-disconnect")
            .is_err());
        assert!(root
            .room_command(visitor, "lounge", ":irc-connect")
            .is_err());
    }

    #[test]
    fn room_props_and_owner_commands() {
        let owner = "did:ma:owner";
        let visitor = "did:ma:visitor";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig alpha description:first_room")
            .expect("dig alpha");

        assert_eq!(
            root.room_snapshot("alpha").expect("alpha").owner.as_deref(),
            Some(owner)
        );
        assert_eq!(
            root.room_command(owner, "alpha", ":prop description")
                .unwrap(),
            "first room"
        );

        // The owner can set name/description/locked and read them back.
        assert!(root.room_command(owner, "alpha", ":prop name Beta").is_ok());
        assert!(root
            .room_command(owner, "alpha", ":prop description changed_room")
            .is_ok());
        assert!(root
            .room_command(owner, "alpha", ":prop locked s3cret")
            .is_ok());
        assert_eq!(
            root.room_command(owner, "alpha", ":prop name").unwrap(),
            "Beta"
        );
        assert_eq!(
            root.room_command(owner, "alpha", ":prop locked").unwrap(),
            "s3cret"
        );

        // `owner` is protected: it is set via :owner, never :prop.
        assert!(root
            .room_command(owner, "alpha", ":prop owner did:ma:bob")
            .is_err());
        assert!(root
            .room_command(owner, "alpha", ":owner did:ma:bob")
            .is_ok());
        assert_eq!(
            root.room_snapshot("alpha").expect("alpha").owner.as_deref(),
            Some("did:ma:bob")
        );

        // A non-owner cannot set props or reassign ownership.
        assert!(root
            .room_command(visitor, "alpha", ":prop name Nope")
            .is_err());
        assert!(root
            .room_command(visitor, "alpha", ":owner did:ma:visitor")
            .is_err());
    }

    #[test]
    fn only_room_owner_can_dig_from_room() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig alpha")
            .expect("dig alpha");

        // Assign the room to alice; now only alice can dig from it.
        root.room_command(owner, "alpha", ":owner did:ma:alice")
            .expect("assign owner");

        let beta = RoomSpec::from_dig("beta description:second_room").expect("beta spec");
        assert!(root
            .dig_from_room("did:ma:alice", "alpha", beta.clone())
            .is_ok());
        assert!(root.dig_from_room("did:ma:bob", "alpha", beta).is_err());
    }

    #[test]
    fn locked_room_requires_unlock_secret() {
        let owner = "did:ma:owner";
        let visitor = "did:ma:visitor";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig alpha")
            .expect("dig alpha");

        assert!(root
            .room_command(owner, "alpha", ":prop locked s3cret")
            .is_ok());
        assert!(root.enter_entity(visitor, "alpha", "{}").is_err());
        assert!(root
            .room_command(visitor, "alpha", ":unlock wrong")
            .is_err());
        assert!(root
            .room_command(visitor, "alpha", ":unlock s3cret")
            .is_ok());
        assert!(root.enter_entity(visitor, "alpha", "{}").is_ok());

        // The room owner may unlock without presenting the secret.
        assert!(root
            .room_command(owner, "alpha", ":prop locked again")
            .is_ok());
        assert!(root.room_command(owner, "alpha", ":unlock").is_ok());
    }

    #[test]
    fn entities_use_kind_prefixed_fragments() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        root.concourse_command("did:ma:owner", "dig hidden description:secret")
            .expect("dig hidden");

        let fragments = root.entities().collect::<Vec<_>>();
        let hidden_fragment = fragments
            .iter()
            .find(|fragment| **fragment != "#concourse")
            .expect("hidden fragment exists");
        assert!(
            hidden_fragment.starts_with("#room-"),
            "room fragment should be kind-prefixed: {hidden_fragment}"
        );
        assert_ne!(*hidden_fragment, "#hidden");
    }

    #[test]
    fn room_ctx_contains_exits_and_optional_irc_binding() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        root.concourse_command("did:ma:owner", "dig alpha description:alpha_room")
            .expect("dig alpha");
        root.concourse_command("did:ma:owner", "dig beta description:beta_room")
            .expect("dig beta");

        root.concourse_command("did:ma:owner", "exit-add alpha north did:ma:runtime#room2")
            .expect("exit add");

        root.room_command("did:ma:owner", "alpha", ":irc-server irc://127.0.0.1")
            .expect("set IRC server");
        root.room_command("did:ma:owner", "alpha", ":irc-nick foo")
            .expect("set IRC nick");
        root.room_command("did:ma:owner", "alpha", ":irc-channel #chan")
            .expect("set IRC channel");

        let ctx = root
            .concourse_command("did:ma:owner", "ctx alpha")
            .expect("ctx output");
        assert!(ctx.contains("name: alpha"));
        assert!(ctx.contains("description: alpha room"));
        assert!(ctx.contains("- name: north"));
        assert!(ctx.contains("target: did:ma:runtime#room2"));
        assert!(ctx.contains("server: irc://127.0.0.1"));
    }

    #[test]
    fn room_graph_snapshot_contains_concourse_and_dug_rooms() {
        let mut root = RootActor::new(["did:ma:owner".to_string()]);
        root.concourse_command("did:ma:owner", "dig alpha description:alpha_room")
            .expect("dig alpha");
        root.concourse_command("did:ma:owner", "exit-add alpha north did:ma:runtime#north")
            .expect("add exit");

        let snapshot = root.room_graph_snapshot();
        assert!(snapshot.rooms.iter().any(|room| room.is_concourse));

        let alpha = snapshot
            .rooms
            .iter()
            .find(|room| room.label == "alpha")
            .expect("alpha room");
        assert_eq!(alpha.description, "alpha room");
        assert_eq!(alpha.exits.len(), 1);
        assert_eq!(alpha.exits[0].name, "north");
        assert_eq!(alpha.exits[0].target, "did:ma:runtime#north");
    }

    #[test]
    fn digging_a_room_links_concourse_exit_and_traverses_full_url() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig lounge description:lounge_room")
            .expect("dig lounge");

        let concourse = root
            .room_snapshot(CONCOURSE_ROOM)
            .expect("concourse snapshot");
        let exit = concourse
            .exits
            .iter()
            .find(|exit| exit.name == "lounge")
            .expect("concourse exit for lounge");
        assert!(
            exit.fragment.starts_with("#exit-"),
            "exit fragment: {}",
            exit.fragment
        );

        let room = root.room_snapshot("lounge").expect("lounge snapshot");
        let reply = root
            .exit_traverse(&exit.fragment, owner, None, "did:ma:runtime")
            .expect("traverse");
        assert!(reply.parent.starts_with("did:ma:runtime#room-"));
        assert_eq!(reply.parent, format!("did:ma:runtime{}", room.fragment));
        assert_eq!(reply.direction, "lounge");
    }

    #[test]
    fn exit_target_resolves_to_full_room_url() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig alpha description:alpha_room")
            .expect("dig alpha");

        let room = root.room_snapshot("alpha").expect("alpha snapshot");
        // Foreign full DID-URLs pass through untouched.
        assert_eq!(
            root.room_url("did:ma:rt", "did:ma:other#room-x"),
            Some("did:ma:other#room-x".to_string())
        );
        // Local room fragments resolve to the room's full DID-URL.
        let expected = format!("did:ma:rt{}", room.fragment);
        assert_eq!(root.room_url("did:ma:rt", &room.fragment), Some(expected));
        // Unknown targets are unresolvable (blocked on the wire).
        assert_eq!(root.room_url("did:ma:rt", "#missing"), None);
    }

    #[test]
    fn room_snapshot_exposes_exits_and_presence() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig alpha description:alpha_room")
            .expect("dig alpha");
        root.concourse_command(owner, "exit-add alpha north did:ma:other#room2")
            .expect("exit add");

        let snapshot = root.room_snapshot("alpha").expect("alpha snapshot");
        assert_eq!(snapshot.label, "alpha");
        assert_eq!(snapshot.exits.len(), 1);
        assert!(snapshot.exits[0].fragment.starts_with("#exit-"));
        assert!(snapshot.presence.iter().any(|p| p.did == owner));
    }

    #[test]
    fn deleting_a_room_cleans_concourse_exit() {
        let owner = "did:ma:owner";
        let mut root = RootActor::new([owner.to_string()]);
        root.concourse_command(owner, "dig scrap description:to_delete")
            .expect("dig scrap");

        let concourse = root
            .room_snapshot(CONCOURSE_ROOM)
            .expect("concourse snapshot");
        assert!(concourse.exits.iter().any(|exit| exit.name == "scrap"));

        root.concourse_command(owner, "delete scrap")
            .expect("delete scrap");

        let concourse = root
            .room_snapshot(CONCOURSE_ROOM)
            .expect("concourse snapshot");
        assert!(!concourse.exits.iter().any(|exit| exit.name == "scrap"));
        assert!(root.room_snapshot("scrap").is_none());
    }
}
