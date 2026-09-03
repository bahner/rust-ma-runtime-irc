use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use nanoid::nanoid;
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsConnector;
use url::{Host, Url};

const IRC_DEFAULT_PORT: u16 = 6667;
const IRCS_DEFAULT_PORT: u16 = 6697;
const REGISTRATION_TIMEOUT_SECS: u64 = 10;
const MAX_NICK_ATTEMPTS: usize = 6;
const MAX_IRC_NICK_LEN: usize = 30;

#[derive(Debug, Clone)]
pub struct RoomSpec {
    pub label: String,
    pub description: String,
    /// An existing room to link to instead of creating a new one. When set,
    /// this is a full DID-URL in another runtime (`did:ma:...#fragment`).
    pub target: Option<String>,
}

impl RoomSpec {
    pub fn from_dig(input: &str) -> Result<Self> {
        let tokens = input.split_whitespace().collect::<Vec<_>>();
        Self::from_tokens(&tokens)
    }

    pub fn from_tokens(tokens: &[&str]) -> Result<Self> {
        let Some(name) = tokens.first().copied() else {
            return Err(anyhow!("dig requires an entity name"));
        };

        let mut description = String::new();
        let mut target: Option<String> = None;
        let mut rest = tokens.iter().skip(1).peekable();
        while let Some(token) = rest.next() {
            if *token == "to" {
                let Some(did) = rest.next() else {
                    return Err(anyhow!("dig to requires a DID-URL after 'to'"));
                };
                if !is_valid_dig_target(did) {
                    return Err(anyhow!(
                        "dig to requires a full DID-URL (did:ma:...#fragment)"
                    ));
                }
                target = Some((*did).to_string());
                continue;
            }
            let Some((k, v)) = token.split_once(':') else {
                return Err(anyhow!(
                    "invalid dig token '{token}', expected key:value or 'to <did>'"
                ));
            };
            if k.is_empty() || v.is_empty() {
                return Err(anyhow!(
                    "invalid dig token '{token}', key and value are required"
                ));
            }
            match k {
                "description" | "desc" => description = v.replace('_', " "),
                "server" | "nick" | "join" | "connect" | "disconnect" | "net" | "pass" => {
                    return Err(anyhow!(
                        "dig does not configure IRC; use :irc-server, :irc-channel, and :irc-connect inside the room"
                    ))
                }
                other => {
                    return Err(anyhow!(
                        "unsupported dig key '{other}' (supported: description/desc)"
                    ))
                }
            }
        }

        if target.is_some() && !description.is_empty() {
            return Err(anyhow!("description only applies when digging a new room"));
        }

        Ok(Self {
            label: normalise_entity_name(name),
            description,
            target,
        })
    }
}

fn normalise_entity_name(raw: &str) -> String {
    // Rooms are rooms, not IRC channels: the label is the plain name the
    // digger typed. '#' belongs only to DID-URL fragments (#room-…, #exit-…).
    raw.trim().to_string()
}

/// Validate a `dig … to` target's shape: a full DID-URL with exactly one
/// non-empty fragment (`did:ma:…#fragment`). A second `#` (for example
/// appending `#concourse` to a full room DID-URL that already carries its own
/// fragment) is rejected here so a malformed target is never stored as an exit
/// that the wire layer can only fail to deliver to later.
fn is_valid_dig_target(did: &str) -> bool {
    if !did.starts_with("did:") {
        return false;
    }
    let parts: Vec<&str> = did.split('#').collect();
    match parts.as_slice() {
        [identifier, fragment] => !identifier.is_empty() && !fragment.is_empty(),
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct ChannelEntryPolicy {
    pub allow: bool,
    pub allow_dids: HashSet<String>,
    pub deny_dids: HashSet<String>,
}

impl ChannelEntryPolicy {
    pub fn open() -> Self {
        Self {
            allow: true,
            allow_dids: HashSet::new(),
            deny_dids: HashSet::new(),
        }
    }

    pub fn closed() -> Self {
        Self {
            allow: false,
            allow_dids: HashSet::new(),
            deny_dids: HashSet::new(),
        }
    }

    pub fn can_enter(&self, did: &str) -> bool {
        if self.deny_dids.contains(did) {
            return false;
        }
        if self.allow {
            return true;
        }
        self.allow_dids.contains(did)
    }

    pub fn set_allow(&mut self, allow: bool) {
        self.allow = allow;
    }

    pub fn allow_did(&mut self, did: &str) {
        self.allow_dids.insert(did.to_string());
    }

    pub fn deny_did(&mut self, did: &str) {
        self.deny_dids.insert(did.to_string());
    }
}

#[derive(Debug, Clone)]
pub struct PresenceRecord {
    pub did: String,
    pub ctx: String,
}

impl PresenceRecord {
    /// The human-readable nick this presence presents in the room: its ctx
    /// when the ctx is a plain nick word, otherwise the bare DID.
    pub fn display_nick(&self) -> &str {
        let ctx = self.ctx.trim();
        if !ctx.is_empty() && !ctx.starts_with('{') {
            ctx
        } else {
            self.did.as_str()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomExit {
    /// Direction/name word clients resolve on (`go north`).
    pub name: String,
    /// Destination: a full DID-URL or a local room fragment/label.
    /// The wire layer always resolves this to a full DID-URL.
    pub target: String,
    /// Addressable child fragment (`#exit-…`), dialable as `:traverse`.
    pub fragment: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IrcBinding {
    pub server: String,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pass: Option<String>,
}

impl IrcBinding {
    pub fn validate_for_join(&self) -> Result<()> {
        server_endpoint(&self.server)?;
        if !self.channel.starts_with('#') || self.channel.len() == 1 {
            return Err(anyhow!("set the IRC channel with :irc-channel #<name>"));
        }
        Ok(())
    }
}

/// An IRC-legal nick from a human word: keeps letters/digits and the
/// RFC-legal special characters, truncates, and never starts with a digit
/// or '-'.
pub fn sanitise_irc_nick(raw: &str) -> String {
    let mut nick: String = raw
        .chars()
        .filter(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '-' | '_' | '[' | ']' | '\\' | '`' | '^' | '{' | '|' | '}'
                )
        })
        .collect();
    if nick.is_empty() {
        nick.push('_');
    }
    if nick.starts_with(|ch: char| ch.is_ascii_digit() || ch == '-') {
        nick.insert(0, '_');
    }
    if nick.len() > MAX_IRC_NICK_LEN {
        nick.truncate(MAX_IRC_NICK_LEN);
    }
    nick
}

/// Deterministic nick from a bare DID, so an occupant that presented no nick
/// still joins the channel "as itself" (`ma-<did suffix>`).
fn did_fallback_nick(did: &str) -> String {
    let suffix = did.strip_prefix("did:ma:").unwrap_or(did);
    let mut nick = String::with_capacity(MAX_IRC_NICK_LEN);
    nick.push_str("ma-");
    for ch in suffix.chars() {
        if nick.len() >= MAX_IRC_NICK_LEN {
            break;
        }
        nick.push(ch);
    }
    nick
}

/// The nick an occupant wants on the room's channel. The nick the actor
/// already presents wins; otherwise a DID-derived nick. All choices are
/// IRC-sanitised.
pub fn desired_irc_nick(presence_ctx: Option<&str>, did: &str) -> String {
    if let Some(ctx) = presence_ctx {
        let ctx = ctx.trim();
        if !ctx.is_empty() && !ctx.starts_with('{') {
            let nick = sanitise_irc_nick(ctx);
            if !nick.is_empty() {
                return nick;
            }
        }
    }
    sanitise_irc_nick(&did_fallback_nick(did))
}

fn nick_candidate(base: &str, attempt: usize) -> String {
    if attempt == 0 {
        return base.to_string();
    }
    let underscores = attempt.min(MAX_NICK_ATTEMPTS);
    let keep = MAX_IRC_NICK_LEN.saturating_sub(underscores);
    let mut nick = base.chars().take(keep).collect::<String>();
    nick.extend(std::iter::repeat_n('_', underscores));
    nick
}

/// Channel events observed by one connected IRC session and forwarded to the
/// room bridge. Nicks are the display casing the server used.
#[derive(Debug, Clone, PartialEq)]
pub enum IrcEvent {
    /// Registration completed; `nick` is the nick the server accepted.
    Ready {
        nick: String,
    },
    Message {
        nick: String,
        text: String,
    },
    Action {
        nick: String,
        text: String,
    },
    Joined {
        nick: String,
    },
    Parted {
        nick: String,
    },
    Quit {
        nick: String,
    },
    Renamed {
        old: String,
        new: String,
    },
    /// A complete `353`/`366` channel listing.
    Names {
        nicks: Vec<String>,
    },
    /// The connection ended (EOF/error), not a deliberate QUIT.
    Disconnected,
}

/// Outbound command to a connection's writer task. `Line` writes a raw IRC
/// command; `Nick` requests a nick change and reports the server's answer.
enum IrcCommand {
    Line(String),
    Nick {
        nick: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// A nick change awaiting the server's confirmation.
struct PendingNick {
    new: String,
    reply: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Clone)]
pub struct IrcClient {
    commands: mpsc::Sender<IrcCommand>,
}

impl IrcClient {
    /// Register on the server with `nick` (retrying suffixed candidates on
    /// nick-in-use), join `binding.channel`, then stream every observed
    /// channel event into `events`. The accepted nick is reported as the
    /// first event (`Ready`).
    pub async fn connect(
        binding: &IrcBinding,
        nick: &str,
        events: mpsc::Sender<IrcEvent>,
    ) -> Result<Self> {
        binding.validate_for_join()?;
        let desired = nick.trim();
        if desired.is_empty() {
            return Err(anyhow!("an IRC nick is required to join the channel"));
        }
        let desired = desired.to_string();

        let endpoint = server_endpoint(&binding.server)?;
        let tcp = TcpStream::connect(&endpoint.connect)
            .await
            .map_err(|error| anyhow!("connecting to IRC server {}: {error}", endpoint.connect))?;

        let (reader, writer): (
            Box<dyn AsyncRead + Unpin + Send>,
            Box<dyn AsyncWrite + Unpin + Send>,
        ) = if endpoint.tls {
            let server_name = match endpoint.sni.parse::<std::net::IpAddr>() {
                Ok(ip) => ServerName::from(ip),
                Err(_) => ServerName::try_from(endpoint.sni.clone())
                    .map_err(|_| anyhow!("invalid TLS server name '{}'", endpoint.sni))?,
            };
            let tls = tls_connector()?
                .connect(server_name, tcp)
                .await
                .map_err(|error| anyhow!("TLS handshake with {} failed: {error}", endpoint.sni))?;
            let (reader, writer) = tokio::io::split(tls);
            (Box::new(reader), Box::new(writer))
        } else {
            let (reader, writer) = tokio::io::split(tcp);
            (Box::new(reader), Box::new(writer))
        };
        let mut writer = writer;
        let mut lines = BufReader::new(reader).lines();

        if let Some(password) = binding.pass.as_deref() {
            write_line(&mut writer, &format!("PASS {password}")).await?;
        }

        let mut nick = nick_candidate(&desired, 0);
        write_line(&mut writer, &format!("NICK {nick}")).await?;
        write_line(&mut writer, &format!("USER {} 0 * :{}", desired, desired)).await?;
        let mut attempts = 0usize;
        let nick = loop {
            let line = tokio::time::timeout(
                std::time::Duration::from_secs(REGISTRATION_TIMEOUT_SECS),
                lines.next_line(),
            )
            .await
            .map_err(|_| anyhow!("IRC registration timed out"))?
            .map_err(|error| anyhow!("reading IRC registration: {error}"))?
            .ok_or_else(|| anyhow!("IRC server closed during registration"))?;

            if let Some(token) = line.strip_prefix("PING ") {
                write_line(&mut writer, &format!("PONG {token}")).await?;
                continue;
            }
            match line.split_whitespace().nth(1).unwrap_or_default() {
                "001" => break line.split_whitespace().nth(2).unwrap_or(&nick).to_string(),
                "433" | "436" | "437" if attempts < MAX_NICK_ATTEMPTS => {
                    attempts += 1;
                    nick = nick_candidate(&desired, attempts);
                    write_line(&mut writer, &format!("NICK {nick}")).await?;
                }
                "433" | "436" | "437" => return Err(anyhow!("IRC nick is already in use")),
                _ => {}
            }
        };

        // The room bridge needs the accepted nick before any further channel
        // event so it can tell our own mirror nicks from untrusted users.
        let _ = events.send(IrcEvent::Ready { nick: nick.clone() }).await;
        write_line(&mut writer, &format!("JOIN {}", binding.channel)).await?;
        write_line(&mut writer, &format!("NAMES {}", binding.channel)).await?;

        let (commands, mut command_rx) = mpsc::channel::<IrcCommand>(32);
        let channel = binding.channel.clone();
        let initial_nick = nick.clone();
        tokio::spawn(async move {
            let mut current_nick = initial_nick;
            let mut pending_names: HashMap<String, Vec<String>> = HashMap::new();
            let mut pending_nick: Option<PendingNick> = None;
            loop {
                tokio::select! {
                    line = lines.next_line() => {
                        let line = match line {
                            Ok(Some(line)) => line,
                            Ok(None) | Err(_) => {
                                let _ = events.send(IrcEvent::Disconnected).await;
                                break;
                            }
                        };
                        if let Some(token) = line.strip_prefix("PING ") {
                            if write_line(&mut writer, &format!("PONG {token}")).await.is_err() {
                                let _ = events.send(IrcEvent::Disconnected).await;
                                break;
                            }
                            continue;
                        }
                        let Some(parsed) = parse_irc_line(&line) else {
                            continue;
                        };

                        if let Some(pending) = pending_nick.take() {
                            let renamed_new = parsed
                                .trailing
                                .as_deref()
                                .or_else(|| parsed.params.first().map(String::as_str));
                            let success = parsed.command == "NICK"
                                && parsed
                                    .nick
                                    .as_deref()
                                    .is_some_and(|old| old.eq_ignore_ascii_case(&current_nick))
                                && renamed_new
                                    .is_some_and(|new| new.eq_ignore_ascii_case(&pending.new));
                            if success {
                                current_nick = pending.new.clone();
                                let _ = pending.reply.send(Ok(()));
                                // Fall through so the NICK line is also reported
                                // to the bridge as a Renamed event.
                            } else if matches!(parsed.command.as_str(), "433" | "436" | "437") {
                                let _ = pending.reply.send(Err(format!(
                                    "IRC nick '{}' is already in use",
                                    pending.new
                                )));
                                continue;
                            } else {
                                pending_nick = Some(pending);
                            }
                        }

                        if !handle_irc_line(&channel, parsed, &mut pending_names, &events).await {
                            break;
                        }
                    }
                    command = command_rx.recv() => {
                        match command {
                            Some(IrcCommand::Line(line)) => {
                                if write_line(&mut writer, &line).await.is_err() {
                                    let _ = events.send(IrcEvent::Disconnected).await;
                                    break;
                                }
                            }
                            Some(IrcCommand::Nick { nick, reply }) => {
                                if write_line(&mut writer, &format!("NICK {nick}")).await.is_err() {
                                    let _ = reply.send(Err("IRC connection closed".to_string()));
                                    let _ = events.send(IrcEvent::Disconnected).await;
                                    break;
                                }
                                pending_nick = Some(PendingNick { new: nick, reply });
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Ok(Self { commands })
    }

    pub async fn privmsg(&self, channel: &str, text: &str) -> Result<()> {
        if text.contains(['\r', '\n']) {
            return Err(anyhow!("IRC message must be one line"));
        }
        self.commands
            .send(IrcCommand::Line(format!("PRIVMSG {channel} :{text}")))
            .await
            .map_err(|_| anyhow!("IRC connection closed"))
    }

    /// Send a CTCP ACTION (`/me`) to the channel.
    pub async fn action(&self, channel: &str, text: &str) -> Result<()> {
        self.privmsg(channel, &format!("\u{1}ACTION {text}\u{1}"))
            .await
    }

    pub async fn names(&self, channel: &str) -> Result<()> {
        self.commands
            .send(IrcCommand::Line(format!("NAMES {channel}")))
            .await
            .map_err(|_| anyhow!("IRC connection closed"))
    }

    pub async fn quit(&self) -> Result<()> {
        self.commands
            .send(IrcCommand::Line("QUIT :leaving".to_string()))
            .await
            .map_err(|_| anyhow!("IRC connection closed"))
    }

    /// Change this connection's nick and wait for the server's answer. The
    /// server confirms with a `NICK` line (reported to the bridge as
    /// `Renamed`) or rejects with a `433`/`436`/`437` error.
    pub async fn change_nick(&self, nick: &str) -> Result<()> {
        if nick.is_empty() || nick.chars().any(char::is_whitespace) {
            return Err(anyhow!("invalid IRC nick"));
        }
        let (reply, reply_rx) = oneshot::channel();
        self.commands
            .send(IrcCommand::Nick {
                nick: nick.to_string(),
                reply,
            })
            .await
            .map_err(|_| anyhow!("IRC connection closed"))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("IRC connection closed"))?
            .map_err(anyhow::Error::msg)
    }
}

struct ParsedIrcLine {
    nick: Option<String>,
    command: String,
    params: Vec<String>,
    trailing: Option<String>,
}

/// Split one raw IRC line into prefix nick, command, params and trailing
/// text. Numbers and commands are compared case-insensitively on the wire.
fn parse_irc_line(line: &str) -> Option<ParsedIrcLine> {
    let (prefix, body) = match line.strip_prefix(':') {
        Some(rest) => {
            let (prefix, body) = rest.split_once(' ')?;
            (Some(prefix), body)
        }
        None => (None, line),
    };
    let mut head = body.splitn(2, ' ');
    let command = head.next()?.to_ascii_uppercase();
    let mut params = Vec::new();
    let mut trailing = None;
    if let Some(args) = head.next() {
        if let Some(rest) = args.strip_prefix(':') {
            trailing = Some(rest.to_string());
        } else {
            let parts = args.split(' ').collect::<Vec<_>>();
            let mut index = 0;
            while index < parts.len() {
                let token = parts[index];
                index += 1;
                if token.is_empty() {
                    continue;
                }
                if let Some(rest) = token.strip_prefix(':') {
                    let mut text = rest.to_string();
                    if index < parts.len() {
                        text.push(' ');
                        text.push_str(&parts[index..].join(" "));
                    }
                    trailing = Some(text);
                    break;
                }
                params.push(token.to_string());
            }
        }
    }
    let nick = prefix
        .and_then(|prefix| prefix.split('!').next())
        .map(str::to_string);
    Some(ParsedIrcLine {
        nick,
        command,
        params,
        trailing,
    })
}

/// Dispatch one parsed line. Returns `false` when the event channel is gone
/// and the reader should stop.
async fn handle_irc_line(
    channel: &str,
    parsed: ParsedIrcLine,
    pending_names: &mut HashMap<String, Vec<String>>,
    events: &mpsc::Sender<IrcEvent>,
) -> bool {
    let command = parsed.command.clone();
    match command.as_str() {
        "PRIVMSG" => {
            let Some(target) = parsed.params.first() else {
                return true;
            };
            if !target.eq_ignore_ascii_case(channel) {
                return true;
            }
            let (Some(nick), Some(text)) = (parsed.nick, parsed.trailing) else {
                return true;
            };
            let event = match text.strip_prefix("\u{1}ACTION") {
                Some(action) => match action.strip_suffix('\u{1}') {
                    Some(body) => IrcEvent::Action {
                        nick,
                        text: body.trim().to_string(),
                    },
                    None => IrcEvent::Message {
                        nick,
                        text: text.clone(),
                    },
                },
                None => IrcEvent::Message { nick, text },
            };
            events.send(event).await.is_ok()
        }
        "JOIN" => {
            let targets_channel = parsed
                .params
                .iter()
                .any(|param| param.eq_ignore_ascii_case(channel))
                || parsed
                    .trailing
                    .as_deref()
                    .is_some_and(|trailing| trailing.eq_ignore_ascii_case(channel));
            if !targets_channel {
                return true;
            }
            let Some(nick) = parsed.nick else {
                return true;
            };
            events.send(IrcEvent::Joined { nick }).await.is_ok()
        }
        "PART" => {
            if !parsed
                .params
                .first()
                .is_some_and(|param| param.eq_ignore_ascii_case(channel))
            {
                return true;
            }
            let Some(nick) = parsed.nick else {
                return true;
            };
            events.send(IrcEvent::Parted { nick }).await.is_ok()
        }
        "QUIT" => {
            let Some(nick) = parsed.nick else {
                return true;
            };
            events.send(IrcEvent::Quit { nick }).await.is_ok()
        }
        "NICK" => {
            let new = parsed.trailing.or_else(|| parsed.params.first().cloned());
            let (Some(old), Some(new)) = (parsed.nick, new) else {
                return true;
            };
            events.send(IrcEvent::Renamed { old, new }).await.is_ok()
        }
        "353" => {
            let names = parsed.trailing.unwrap_or_default();
            let channel_key = parsed
                .params
                .get(2)
                .cloned()
                .unwrap_or_else(|| channel.to_string());
            let entry = pending_names.entry(channel_key).or_default();
            for word in names.split_whitespace() {
                let nick = word.trim_start_matches(['@', '+', '%', '~', '&']);
                if !nick.is_empty() {
                    entry.push(nick.to_string());
                }
            }
            true
        }
        "366" => {
            let channel_key = parsed
                .params
                .get(1)
                .cloned()
                .unwrap_or_else(|| channel.to_string());
            let mut seen = HashSet::new();
            let nicks = pending_names.remove(&channel_key).unwrap_or_default();
            let nicks = nicks
                .into_iter()
                .filter(|nick| seen.insert(nick.to_lowercase()))
                .collect();
            events.send(IrcEvent::Names { nicks }).await.is_ok()
        }
        _ => true,
    }
}

async fn write_line<W>(writer: &mut W, line: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(format!("{line}\r\n").as_bytes())
        .await
        .map_err(|error| anyhow!("writing IRC command: {error}"))
}

struct ServerEndpoint {
    connect: String,
    sni: String,
    tls: bool,
}

/// Parse an IRC server URL into its connection target. `irc://` is plaintext
/// TCP (default port 6667); `ircs://` is TLS (default port 6697).
fn server_endpoint(server: &str) -> Result<ServerEndpoint> {
    if server.trim().is_empty() {
        return Err(anyhow!(
            "set the IRC server with :irc-server <irc://host[:port]> or <ircs://host[:port]>"
        ));
    }
    if !(server.starts_with("irc://") || server.starts_with("ircs://")) {
        return Err(anyhow!(
            "IRC server must be a full URL: irc://host[:port] or ircs://host[:port] (got '{server}')"
        ));
    }
    let url = Url::parse(server)
        .map_err(|error| anyhow!("invalid IRC server URL '{server}': {error}"))?;

    let tls = url.scheme() == "ircs";

    let host = url
        .host()
        .ok_or_else(|| anyhow!("IRC server URL '{server}' is missing a host"))?;
    let port = url.port().unwrap_or(if tls {
        IRCS_DEFAULT_PORT
    } else {
        IRC_DEFAULT_PORT
    });

    let (sni, connect) = match host {
        Host::Domain(domain) => (domain.to_string(), format!("{domain}:{port}")),
        Host::Ipv4(ip) => (ip.to_string(), format!("{ip}:{port}")),
        Host::Ipv6(ip) => (ip.to_string(), format!("[{ip}]:{port}")),
    };

    Ok(ServerEndpoint { connect, sni, tls })
}

/// Validate that `server` is a supported IRC URL. Used by `:irc-server` for
/// early feedback before any connection attempt.
pub fn validate_irc_server_url(server: &str) -> Result<()> {
    server_endpoint(server).map(|_| ())
}

fn tls_connector() -> Result<TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

#[derive(Debug, Serialize)]
pub struct RoomCtxView<'a> {
    pub name: String,
    pub description: String,
    pub exits: &'a [RoomExit],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub irc: Option<&'a IrcBinding>,
}

/// Durable per-room state persisted in the root manifest: the room's
/// identity, props, exits, and IRC binding. Transient state (presence, active
/// IRC sessions, channel occupants) is deliberately excluded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomManifest {
    pub fragment: String,
    pub label: String,
    pub props: HashMap<String, String>,
    pub exits: Vec<RoomExit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub irc: Option<IrcBinding>,
}

#[derive(Debug)]
pub struct RoomActor {
    pub fragment: String,
    /// Addressable label used for resolution (`#concourse`, plain room names).
    pub label: String,
    /// Authoritative prop store: `name`, `description`, `owner`, `locked`, …
    pub props: HashMap<String, String>,
    pub exits: Vec<RoomExit>,
    pub irc: Option<IrcBinding>,
    /// Whether the room's channel is activated: occupants joining the room
    /// join the channel as themselves.
    pub irc_active: bool,
    /// Trusted occupants (`did` -> channel nick) currently connected to the
    /// room's channel. These are the ma actors already present in the room.
    pub irc_mirrors: HashMap<String, String>,
    /// Untrusted IRC users on the channel, keyed by lowercased nick:
    /// lowercased nick -> display nick. A plain room-held list, kept current
    /// by channel JOIN/PART/QUIT/NICK events and periodic NAMES resyncs.
    pub irc_occupants: HashMap<String, String>,
    pub policy: ChannelEntryPolicy,
    pub presence: HashMap<String, PresenceRecord>,
}

impl RoomActor {
    pub fn new(
        fragment: impl Into<String>,
        label: impl Into<String>,
        policy: ChannelEntryPolicy,
    ) -> Self {
        let label = label.into();
        let mut props = HashMap::new();
        props.insert("name".to_string(), label.clone());
        props.insert("description".to_string(), String::new());
        props.insert("locked".to_string(), String::new());
        Self {
            fragment: fragment.into(),
            label,
            props,
            exits: Vec::new(),
            irc: None,
            irc_active: false,
            irc_mirrors: HashMap::new(),
            irc_occupants: HashMap::new(),
            policy,
            presence: HashMap::new(),
        }
    }

    pub fn from_spec(
        fragment: String,
        spec: RoomSpec,
        owner: Option<String>,
        policy: ChannelEntryPolicy,
    ) -> Self {
        let mut props = HashMap::new();
        props.insert("name".to_string(), spec.label.clone());
        props.insert("description".to_string(), spec.description.clone());
        props.insert("locked".to_string(), String::new());
        if let Some(owner) = owner {
            props.insert("owner".to_string(), owner);
        }
        Self {
            fragment,
            label: spec.label,
            props,
            exits: Vec::new(),
            irc: None,
            irc_active: false,
            irc_mirrors: HashMap::new(),
            irc_occupants: HashMap::new(),
            policy,
            presence: HashMap::new(),
        }
    }

    pub fn from_manifest(manifest: RoomManifest) -> Self {
        Self {
            fragment: manifest.fragment,
            label: manifest.label,
            props: manifest.props,
            exits: manifest.exits,
            irc: manifest.irc,
            irc_active: false,
            irc_mirrors: HashMap::new(),
            irc_occupants: HashMap::new(),
            policy: ChannelEntryPolicy::open(),
            presence: HashMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        self.props
            .get("name")
            .map(String::as_str)
            .unwrap_or(&self.label)
    }

    pub fn description(&self) -> &str {
        self.props
            .get("description")
            .map(String::as_str)
            .unwrap_or("")
    }

    pub fn owner(&self) -> Option<&str> {
        self.props.get("owner").map(String::as_str)
    }

    pub fn set_owner(&mut self, owner: Option<&str>) {
        match owner.filter(|owner| !owner.is_empty()) {
            Some(owner) => {
                self.props.insert("owner".to_string(), owner.to_string());
            }
            None => {
                self.props.remove("owner");
            }
        }
    }

    pub fn get_prop(&self, key: &str) -> Option<&str> {
        self.props.get(key).map(String::as_str)
    }

    pub fn set_prop(&mut self, key: &str, value: &str) {
        if value.is_empty() {
            self.props.remove(key);
        } else {
            self.props.insert(key.to_string(), value.to_string());
        }
    }

    /// The stored lock secret; an empty value means the room is unlocked.
    pub fn locked_secret(&self) -> &str {
        self.get_prop("locked").unwrap_or("")
    }

    pub fn is_locked(&self) -> bool {
        !self.locked_secret().is_empty()
    }

    pub fn unlock(&mut self) {
        self.set_prop("locked", "");
    }

    pub fn set_description(&mut self, description: String) {
        self.set_prop("description", &description);
    }

    /// Ensure an exit named `name` leads to `target`. Idempotent: if an exit
    /// with that name already exists it is re-pointed at the new target,
    /// keeping its addressable `fragment` stable for clients that already
    /// hold it; otherwise a fresh exit is created.
    pub fn add_exit(&mut self, name: &str, target: &str) -> Result<()> {
        if let Some(exit) = self.exits.iter_mut().find(|entry| entry.name == name) {
            exit.target = target.to_string();
        } else {
            self.exits.push(RoomExit {
                fragment: format!("#exit-{}", nanoid!(10)),
                name: name.to_string(),
                target: target.to_string(),
            });
        }
        Ok(())
    }

    pub fn remove_exit(&mut self, name: &str) -> Result<()> {
        let before = self.exits.len();
        self.exits.retain(|entry| entry.name != name);
        if self.exits.len() == before {
            return Err(anyhow!("exit not found"));
        }
        Ok(())
    }

    pub fn irc_config_mut(&mut self) -> &mut IrcBinding {
        self.irc.get_or_insert_with(IrcBinding::default)
    }

    pub fn ctx_view(&self) -> RoomCtxView<'_> {
        RoomCtxView {
            name: self.name().to_string(),
            description: self.description().to_string(),
            exits: &self.exits,
            irc: self.irc.as_ref(),
        }
    }

    pub fn enter(&mut self, did: &str, ctx: &str) -> Result<()> {
        if self.is_locked() {
            return Err(anyhow!("room is locked"));
        }
        if !self.policy.can_enter(did) {
            return Err(anyhow!("entry denied by channel policy"));
        }
        self.presence.insert(
            did.to_string(),
            PresenceRecord {
                did: did.to_string(),
                ctx: ctx.to_string(),
            },
        );
        Ok(())
    }

    pub fn leave(&mut self, did: &str) {
        self.presence.remove(did);
    }

    fn require_present(&self, did: &str) -> Result<()> {
        if !self.presence.contains_key(did) {
            return Err(anyhow!("sender is not in the room"));
        }
        Ok(())
    }

    pub fn say(&self, did: &str) -> Result<()> {
        self.require_present(did)
    }

    pub fn emote(&self, did: &str) -> Result<()> {
        self.require_present(did)
    }

    /// Drop all channel state; the room keeps its binding configuration and
    /// can be re-joined later.
    pub fn deactivate_irc(&mut self) {
        self.irc_active = false;
        self.irc_mirrors.clear();
        self.irc_occupants.clear();
    }

    /// The room's binding, only while the channel is activated and joinable.
    pub fn joinable_binding(&self) -> Option<&IrcBinding> {
        if !self.irc_active {
            return None;
        }
        self.irc
            .as_ref()
            .filter(|binding| binding.validate_for_join().is_ok())
    }

    pub fn register_irc_mirror(&mut self, did: &str, nick: String) {
        self.irc_mirrors.insert(did.to_string(), nick);
    }

    pub fn unregister_irc_mirror(&mut self, did: &str) {
        self.irc_mirrors.remove(did);
    }

    pub fn irc_mirror_nick(&self, did: &str) -> Option<&str> {
        self.irc_mirrors.get(did).map(String::as_str)
    }

    pub fn irc_has_mirror(&self, did: &str) -> bool {
        self.irc_mirrors.contains_key(did)
    }

    fn mirror_did_for_nick(&self, nick: &str) -> Option<String> {
        self.irc_mirrors
            .iter()
            .find(|(_, mirror)| mirror.eq_ignore_ascii_case(nick))
            .map(|(did, _)| did.clone())
    }

    pub fn nick_is_irc_mirror(&self, nick: &str) -> bool {
        self.irc_mirrors
            .values()
            .any(|mirror| mirror.eq_ignore_ascii_case(nick))
    }

    /// Track an observed channel JOIN. Returns `true` when a new *fake*
    /// occupant was added (the bridge broadcasts an arrival). Mirror joins
    /// are already present as trusted occupants and never become fakes.
    pub fn observe_irc_join(&mut self, nick: &str) -> bool {
        if self.nick_is_irc_mirror(nick) {
            return false;
        }
        self.irc_occupants
            .insert(nick.to_lowercase(), nick.to_string())
            .is_none()
    }

    /// Track an observed PART/QUIT. Returns `true` when a *fake* occupant was
    /// removed (the bridge broadcasts a departure). A mirror nick that left
    /// is dropped silently — the trusted presence outlives the session.
    pub fn observe_irc_part(&mut self, nick: &str) -> bool {
        if let Some(did) = self.mirror_did_for_nick(nick) {
            self.irc_mirrors.remove(&did);
            return false;
        }
        self.irc_occupants.remove(&nick.to_lowercase()).is_some()
    }

    /// Track an observed NICK change. Returns `(fake_left, fake_arrived)` so
    /// the bridge can broadcast both transitions for a renamed fake.
    pub fn observe_irc_rename(&mut self, old: &str, new: &str) -> (bool, bool) {
        if let Some(did) = self.mirror_did_for_nick(old) {
            if let Some(_nick) = self.irc_mirrors.remove(&did) {
                self.irc_mirrors.insert(did, new.to_string());
            }
            return (false, false);
        }
        let fake_left = self.irc_occupants.remove(&old.to_lowercase()).is_some();
        let fake_arrived = if fake_left && !self.nick_is_irc_mirror(new) {
            self.irc_occupants
                .insert(new.to_lowercase(), new.to_string())
                .is_none()
        } else {
            false
        };
        (fake_left, fake_arrived)
    }

    /// Reconcile the fake list against an authoritative channel listing
    /// (`353`/`366`). Mirror nicks are preserved. Fakes are added/removed
    /// silently — this is the periodic resync, not a join/leave event.
    pub fn sync_irc_occupants(&mut self, listed: &[String]) {
        let mut target = HashMap::new();
        for nick in listed {
            if self.nick_is_irc_mirror(nick) {
                continue;
            }
            target.insert(nick.to_lowercase(), nick.clone());
        }
        self.irc_occupants.retain(|key, _| target.contains_key(key));
        for (key, nick) in target {
            self.irc_occupants.entry(key).or_insert(nick);
        }
    }

    pub fn irc_occupant_nicks(&self) -> Vec<String> {
        let mut nicks = self.irc_occupants.values().cloned().collect::<Vec<_>>();
        nicks.sort_unstable();
        nicks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_endpoint_requires_a_full_irc_url() {
        assert!(server_endpoint("localhost:6667").is_err());
        assert!(server_endpoint("127.0.0.1:6667").is_err());
        assert!(server_endpoint("localhost").is_err());
        assert!(server_endpoint("http://localhost:6667").is_err());
    }

    #[test]
    fn server_endpoint_parses_irc_and_ircs_urls() {
        let plain = server_endpoint("irc://localhost").unwrap();
        assert!(!plain.tls);
        assert_eq!(plain.connect, "localhost:6667");
        assert_eq!(plain.sni, "localhost");

        let tls = server_endpoint("ircs://localhost:6697").unwrap();
        assert!(tls.tls);
        assert_eq!(tls.connect, "localhost:6697");
        assert_eq!(tls.sni, "localhost");
    }

    #[test]
    fn sanitise_nick_keeps_legal_chars_and_prefixes_digits() {
        assert_eq!(sanitise_irc_nick("fjodor"), "fjodor");
        assert_eq!(sanitise_irc_nick("123abc"), "_123abc");
        assert_eq!(sanitise_irc_nick("bob smith"), "bobsmith");
        assert_eq!(sanitise_irc_nick("#chan"), "chan");
        assert_eq!(sanitise_irc_nick(""), "_");
        assert_eq!(sanitise_irc_nick("a^b{c}"), "a^b{c}");
    }

    #[test]
    fn desired_nick_prefers_presence_then_did() {
        assert_eq!(desired_irc_nick(Some("fjodor"), "did:ma:abc"), "fjodor");
        let did_nick = desired_irc_nick(None, "did:ma:abc123");
        assert!(did_nick.starts_with("ma-abc123"), "got {did_nick}");
        // A JSON-ish presence ctx counts as no nick.
        assert_eq!(
            desired_irc_nick(Some("{\"kind\":\"h00man\"}"), "did:ma:abc123"),
            did_nick
        );
    }

    #[test]
    fn presence_display_nick_falls_back_to_did_for_json_ctx() {
        let plain = PresenceRecord {
            did: "did:ma:abc".to_string(),
            ctx: "fjodor".to_string(),
        };
        assert_eq!(plain.display_nick(), "fjodor");
        let json = PresenceRecord {
            did: "did:ma:abc".to_string(),
            ctx: "{}".to_string(),
        };
        assert_eq!(json.display_nick(), "did:ma:abc");
    }

    #[test]
    fn irc_line_parser_splits_common_server_forms() {
        let privmsg = parse_irc_line(":bob!user@host PRIVMSG #chan :hello there").unwrap();
        assert_eq!(privmsg.nick.as_deref(), Some("bob"));
        assert_eq!(privmsg.command, "PRIVMSG");
        assert_eq!(privmsg.params, ["#chan"]);
        assert_eq!(privmsg.trailing.as_deref(), Some("hello there"));

        let names = parse_irc_line(":srv 353 ma-test = #chan :@op +reg plain").unwrap();
        assert_eq!(names.command, "353");
        assert_eq!(names.params, ["ma-test", "=", "#chan"]);
        assert_eq!(names.trailing.as_deref(), Some("@op +reg plain"));

        let nick = parse_irc_line(":bob!u@h NICK :newbob").unwrap();
        assert_eq!(nick.nick.as_deref(), Some("bob"));
        assert_eq!(nick.command, "NICK");
        assert_eq!(nick.trailing.as_deref(), Some("newbob"));

        let no_prefix = parse_irc_line("PING :token").unwrap();
        assert_eq!(no_prefix.nick, None);
        assert_eq!(no_prefix.command, "PING");
    }

    #[test]
    fn room_tracks_fakes_and_mirrors_separately() {
        let mut room = RoomActor::new("#room-x", "lounge", ChannelEntryPolicy::open());
        room.enter("did:ma:alice", "alice").expect("enter alice");

        // Alice's session registers: she is a mirror, never a fake.
        room.register_irc_mirror("did:ma:alice", "alice".to_string());
        assert!(!room.observe_irc_join("alice"));
        assert!(room.irc_occupant_nicks().is_empty());

        // A channel user joins: fake list grows once, duplicates are ignored.
        assert!(room.observe_irc_join("bob"));
        assert!(!room.observe_irc_join("bob"));
        assert_eq!(room.irc_occupant_nicks(), ["bob"]);

        // A listing that contains mirrors and fakes keeps fakes only.
        room.sync_irc_occupants(&["alice".to_string(), "bob".to_string(), "carol".to_string()]);
        assert_eq!(room.irc_occupant_nicks(), ["bob", "carol"]);

        // A listing that drops a fake removes it silently.
        room.sync_irc_occupants(&["alice".to_string(), "carol".to_string()]);
        assert_eq!(room.irc_occupant_nicks(), ["carol"]);

        // PART of a fake is reported; PART of a mirror is silent and unregisters.
        assert!(room.observe_irc_part("carol"));
        assert!(!room.observe_irc_part("alice"));
        assert!(!room.irc_has_mirror("did:ma:alice"));
        assert!(room.irc_occupant_nicks().is_empty());
    }

    #[test]
    fn renamed_fake_moves_key_and_reports_both_events() {
        let mut room = RoomActor::new("#room-x", "lounge", ChannelEntryPolicy::open());
        room.observe_irc_join("Bob");
        assert_eq!(room.irc_occupant_nicks(), ["Bob"]);
        let (left, arrived) = room.observe_irc_rename("Bob", "Robert");
        assert!(left);
        assert!(arrived);
        assert_eq!(room.irc_occupant_nicks(), ["Robert"]);

        // A mirror rename updates the mirror nick without fake events.
        room.enter("did:ma:alice", "alice").expect("enter alice");
        room.register_irc_mirror("did:ma:alice", "alice".to_string());
        let (left, arrived) = room.observe_irc_rename("alice", "alice_");
        assert!(!left);
        assert!(!arrived);
        assert_eq!(room.irc_mirror_nick("did:ma:alice"), Some("alice_"));
    }

    #[tokio::test]
    async fn irc_client_registers_with_retry_and_forwards_events() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind IRC test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept IRC client");
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();

            // Reject the first nick, accept the suffixed retry.
            assert_eq!(lines.next_line().await.unwrap().unwrap(), "NICK ma-test");
            assert_eq!(
                lines.next_line().await.unwrap().unwrap(),
                "USER ma-test 0 * :ma-test"
            );
            writer
                .write_all(b":srv 433 * ma-test :nickname in use\r\n")
                .await
                .expect("send 433");
            assert_eq!(lines.next_line().await.unwrap().unwrap(), "NICK ma-test_");
            writer
                .write_all(b":srv 001 ma-test_ :welcome\r\n")
                .await
                .expect("send welcome");
            assert_eq!(lines.next_line().await.unwrap().unwrap(), "JOIN #ma-test");
            // The client asks for a fresh listing right after joining.
            assert_eq!(lines.next_line().await.unwrap().unwrap(), "NAMES #ma-test");
            writer
                .write_all(b":srv 353 ma-test_ = #ma-test :@op +bob ma-test_\r\n")
                .await
                .expect("send 353");
            writer
                .write_all(b":srv 366 ma-test_ #ma-test :end of names\r\n")
                .await
                .expect("send 366");
            writer
                .write_all(b":bob!u@h PRIVMSG #ma-test :hello from bob\r\n")
                .await
                .expect("send privmsg");
            writer
                .write_all(b":bob!u@h PRIVMSG #ma-test :\x01ACTION waves\x01\r\n")
                .await
                .expect("send action");
            writer
                .write_all(b":carol!u@h JOIN #ma-test\r\n")
                .await
                .expect("send join");
            writer
                .write_all(b":carol!u@h PART #ma-test :bye\r\n")
                .await
                .expect("send part");
            writer
                .write_all(b":bob!u@h QUIT :gone\r\n")
                .await
                .expect("send quit");
            writer
                .write_all(b":bob!u@h NICK :robert\r\n")
                .await
                .expect("send nick");
            // The client sends its own channel message, then quits.
            assert_eq!(
                lines.next_line().await.unwrap().unwrap(),
                "PRIVMSG #ma-test :hello"
            );
            assert_eq!(lines.next_line().await.unwrap().unwrap(), "QUIT :leaving");
        });

        let binding = IrcBinding {
            server: format!("irc://{address}"),
            channel: "#ma-test".to_string(),
            net: None,
            pass: None,
        };
        let (events, mut event_rx) = mpsc::channel::<IrcEvent>(16);
        let client = IrcClient::connect(&binding, "ma-test", events)
            .await
            .expect("connect IRC client");

        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Ready {
                nick: "ma-test_".to_string()
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Names {
                nicks: vec!["op".to_string(), "bob".to_string(), "ma-test_".to_string()]
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Message {
                nick: "bob".to_string(),
                text: "hello from bob".to_string()
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Action {
                nick: "bob".to_string(),
                text: "waves".to_string()
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Joined {
                nick: "carol".to_string()
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Parted {
                nick: "carol".to_string()
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Quit {
                nick: "bob".to_string()
            })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IrcEvent::Renamed {
                old: "bob".to_string(),
                new: "robert".to_string()
            })
        );

        client
            .privmsg(&binding.channel, "hello")
            .await
            .expect("send PRIVMSG");
        client.quit().await.expect("send QUIT");
        drop(client);
        server.await.expect("IRC test server");
    }
}
