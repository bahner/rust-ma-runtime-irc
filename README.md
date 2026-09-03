# ma-irc

`ma-irc` is a separate runtime prototype focused on IRC-backed room actors.

## Quick connect

1. Start runtime:

```sh
cargo run -- --config ./config.yaml
```

2. Verify runtime is up:

```sh
curl -s http://127.0.0.1:5667/status.json
```

3. Use concourse command endpoint:

```sh
curl -s -X POST http://127.0.0.1:5667/concourse/cmd \
  -H 'content-type: application/json' \
  -d '{"from":"did:ma:owner","command":"dig hub description:main_hub"}'
```

4. Fetch full room graph in one call:

```sh
curl -s http://127.0.0.1:5667/concourse/state
```

The HTTP endpoint includes permissive CORS headers (`OPTIONS`, `GET`, `POST`) so browser clients can call it directly in prototype setups.

## Scope in this prototype

- Uses `ma-core` ACL semantics for transport-level allow/deny checks.
- Provides native `#root` behaviour primitives in process.
- Supports `#root:claim` semantics (`claim` in code) with first-writer ownership.
- Creates a default `#concourse` room on startup.
- Supports owner-gated room creation (`dig` from `#concourse` by a runtime
  owner, and from a room by that room's owner).
- Supports linking to an existing room in another runtime with
  `dig <name> to <did:ma:...#fragment>`.
- Supports per-room entry policy:
  - `allow: true|false`
  - `allow_dids: []`
  - `deny_dids: []`
- Rooms store props (`name`, `description`, `owner`, `locked`, …) plus `exits`,
  presence, and an optional IRC binding.

This is a bootstrap codebase, not yet a full IRC transport implementation.

## Runtime-first workflow

The runtime starts first. Afterwards, room/entity operations are expected from
inside the space via `#concourse` commands in ma-zion, not via CLI flags.

## Configuration

Generate the ma-core config and encrypted secret bundle before first use:

```sh
cargo run -- --gen-headless-config
```

On Linux this writes `~/.config/ma/irc.yaml` and
`~/.config/ma/irc.bin`, both with mode `0600`. The YAML path can be overridden
with `--config`, but key material remains in the encrypted bundle selected by
ma-core. `config.example.yaml` documents the IRC-specific settings that may be
added to the generated config.

Default slug is `irc`.
Default status bind is `127.0.0.1:5667`.

Runtime DID is never a placeholder. The runtime loads the ma-core
`SecretBundle` and derives a real `did:ma:<ipns>` from its IPNS key. The bundle
also supplies distinct iroh, signing, and encryption keys.

At startup, `ma-irc` publishes its own DID document via ma-core and Kubo/IPNS.
It also persists its root (the rooms, their exits and props) as a single
DAG-CBOR manifest: published periodically and on graceful shutdown, with the
new CID pinned, the previous CID unpinned, and `root_cid` written back to
`config.yaml`. On the next startup the root is restored from that CID.
Transient state (presence, active IRC sessions, channel occupants) is kept in
memory only.

Relevant config keys:

- `kubo_rpc_url` (default `http://127.0.0.1:5001`)
- `status_bind` (default `127.0.0.1:5667`)
- `owners`
- `acl_file`
- `root_publish_interval_secs` (default `300`) — how often the root manifest
  is republished while the runtime is running

## Status endpoint

By default, `ma-irc` runs a local status endpoint and exposes `GET /status.json`.

```sh
curl -s http://127.0.0.1:5667/status.json
```

Concourse room helpers are exposed for runtime-internal command dispatch:

```sh
curl -s http://127.0.0.1:5667/concourse/help
curl -s -X POST http://127.0.0.1:5667/concourse/cmd \
  -H 'content-type: application/json' \
  -d '{"from":"did:ma:owner","command":"dig bahner description:my_room"}'
curl -s -X POST http://127.0.0.1:5667/concourse/cmd \
  -H 'content-type: application/json' \
  -d '{"from":"did:ma:owner","command":"exits"}'
curl -s http://127.0.0.1:5667/concourse/state
```

One-shot CLI runs can skip hosting with:

```sh
cargo run -- --config ./config.yaml --no-status-server
```

## Run

```sh
cargo run -- --config ./config.yaml
```

Generate config headlessly (ma-runtime style):

```sh
cargo run -- --gen-headless-config
```

After startup, connect from ma-zion and enter `#concourse`.
Use in-world commands there, for example:

- `dig bahner description:my_room`
- `dig portal to did:ma:other#room-x`
- `exit-add bahner north did:ma:runtime#otherroom`
- `ctx bahner`
- `exits`

`dig` enters an existing room with that label, or creates and enters it when it
does not exist. `dig <name> to <did:ma:...#fragment>` instead links the current
room (or `#concourse`) to an existing room in another runtime: it creates an
exit pointing at the full DID-URL and returns that room as the target to enter.
A room can be bound to one IRC channel; the channel is then the
room, not a single relay persona:

- `:irc-server irc://127.0.0.1:6667` and `:irc-channel #bar` configure the room's
  channel binding (once). The server is a URL: `irc://` for plaintext TCP
  (default port 6667) and `ircs://` for TLS (default port 6697). TLS verifies
  the server certificate against the Mozilla root store, so a self-signed
  `ircs://` server will be rejected.
- `:irc-nick foo` sets a room default nick, used when an occupant has not
  already presented their own nick.
- `:irc-connect` is the runtime-owner-controlled on/off switch: it activates
  the binding and joins every occupant already in the room. Entry into a
  connected room is itself the join: if the IRC server will not accept the
  occupant's nick, the entry is denied and the occupant is told so with a room
  `:say`.
- `:irc-disconnect` turns the switch off and disconnects every session.

While connected, `:say`/`:emote` go to the IRC channel under the caller's own
nick, and channel speech from both ma actors and IRC users is broadcast back
to the room's ma actors. While disconnected the room is an ordinary ma room:
`:say`/`:emote` are broadcast normally to the room's ma actors.

Channel speech (`PRIVMSG` / `CTCP ACTION`) is repeated into the room as
`:say` / `:emote` events. Every nick on the channel appears as a room occupant:
ma occupants are trusted presences, while IRC-only users are rendered as
untrusted *fake* actors (`did:ma:<rt>#irc-<nick>`). That fake list is not a set
of ma entities — it is room state, kept current by channel JOIN/PART/QUIT/NICK
events and reconciled periodically with `NAMES`.

The repository's `irc.zscheme` provides matching local Scheme words. After
loading it into Zion's session environment, Scheme code can use:

```scheme
(irc-config "irc://127.0.0.1:6667" "foo" "#bar")
(irc-connect)
(irc-say "hello")
(irc-disconnect)
```

`irc-say` forwards to the room's ordinary `say` verb, which routes to the IRC
channel while connected and broadcasts normally otherwise.

The library targets `.my.ctx.room`; it does not send IRC configuration to
`#concourse` and it is not loaded automatically by Zion.

### Room props and ownership

Rooms carry props (`name`, `description`, `owner`, `locked`, …) read and written
inside the room with:

- `:prop <key>` — read a prop's value.
- `:prop <key> <value>` — set a prop (the room owner or a runtime owner).
- `:owner <did>` — set the room's own owner (runtime owners only).
- `:prop locked <secret>` — lock the room (owner only). While locked, entry is
  denied; `:unlock <secret>` unlocks it again, and an owner may unlock without
  presenting the secret.

The room's `owner` defaults to the runtime owner who dug it. Only a room's
owner can `dig` further rooms from inside it, so ownership is the way a room
admin hands out the ability to create more rooms. The first rooms must still be
dug from `#concourse` by a runtime owner.

Notes:

- Dug rooms are born with kind-prefixed fragments per the lambda-ma
  reference (`did:ma:<rt>#room-…`; exits `#exit-…`), and every ctx address
  on the wire is a full DID-URL. Rooms are labelled by the plain name given
  to `dig` — no IRC-channel `#` prefix.
- Users discover and navigate via labels/exits from `#concourse`.

Check transport ACL decision:

```sh
cargo run -- --config ./config.example.yaml --acl-check-did did:ma:anyone --acl-check-cap rpc
```

## Design notes

- Keep runtime-specific IRC logic in this repository.
- Keep shared identity/message/ACL logic in `ma-core`.
- Keep `rust-ma-runtime` unchanged.
