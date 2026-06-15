//! yoink — a zero-setup shared clipboard for your LAN.
//!
//! The binary wires everything together and runs the app event loop, which
//! is the single owner of all mutation:
//!
//! 1. Clipboard `Copied(text)` → only in the `auto-share`/`mirror` modes
//!    (the default `manual` mode never shares what you copy) → skip if `text`
//!    matches one of the last few history entries (dedupe / echo guard; a
//!    window of one is not enough when two instances share one OS clipboard)
//!    → `add_entry` on the *devices* doc. The OS clipboard is hard-wired to
//!    the devices scope; it never feeds a room.
//! 2. Per-scope doc updates (one forwarder task per open scope feeds a
//!    merged channel) → notify the UI; and in `mirror` mode, if the scope is
//!    `devices`, `origin` is remote, the clipboard is reachable and the
//!    latest entry is from another device, wasn't applied before (track last
//!    applied entry id) and is fresh (created within ~30s of local now, so a
//!    `SYNC_STEP_2` backlog replay never clobbers the clipboard with stale
//!    entries) → `clipboard.set_text`. Rooms never touch the OS clipboard.
//! 3. Discovery events → update the peer registry (lost peers are kept
//!    offline when still connected or explicitly listed, removed otherwise),
//!    forward to `sync.peer_discovered` / `peer_lost`; notify the UI.
//! 4. Sync events (connect/disconnect, per scope) → notify the UI; a
//!    devices-scope connect also seeds the peer registry from the HELLO.
//! 5. [`AppCommand`]s from the server:
//!    - `SetDeviceTrusted` → block/unblock (or pair/unpair under
//!      `--require-pairing`) via `sync.set_trusted` + persist config + notify
//!    - `AddEntry` → dedupe against the scope's latest entry only, add to
//!      that scope's doc; devices adds also set the local clipboard (even
//!      when the entry was a duplicate) — room adds never do
//!    - `CopyEntry` → look up entry by id in the scope's doc →
//!      `clipboard.set_text` (deliberate, so allowed from rooms too)
//!    - `JoinRoom` → create-or-find the room's doc, start syncing, advertise
//!      the room unless `--untrusted`, persist it in the config + notify
//!    - `LeaveRoom` → stop syncing, update room advertisements, drop the doc
//!      + notify
//! 6. Ctrl-C → persist the config, `discovery.shutdown()`, exit.
//!
//! Clipboard history is never written to disk: every run starts empty and a
//! restart clears it. Only the config is persisted, on the blocking pool
//! (reaped on the next flush tick) so a slow disk cannot stall event
//! consumption.
//!
//! The server listens on `0.0.0.0` plus a best-effort `IPV6_V6ONLY` `[::]`
//! socket on the same port, because mDNS advertises IPv6 addresses too.
//!
//! Trust model: by default every device on the LAN is trusted and syncs the
//! personal clipboard automatically; blocking one (persisted) stops it.
//! `--require-pairing` flips this to a strict allowlist where devices must
//! pair first. Rooms are always open-join regardless. `--mode` selects how
//! the OS clipboard is involved (manual / auto-share / mirror), and
//! `--untrusted` is a one-flag hardened preset for networks you don't control
//! (forces `--require-pairing` + manual mode and stops advertising joined
//! room names over mDNS).
//!
//! Config (`config.toml` in the config dir): `device_id` (created on first
//! run), optional `name` override, `allowed` (paired devices, used under
//! `--require-pairing`), `blocked` (blocked devices), `rooms` (joined rooms,
//! rejoined — empty — on startup since history is not persisted).
//!
//! [`AppCommand`]: yoink_core::AppCommand

mod app;
mod banner;
mod config;

use anyhow::Context;
use clap::Parser;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use yoink_clipboard::ClipboardHandle;
use yoink_core::{DeviceInfo, DocSet, ShareMode};
use yoink_discovery::Discovery;
use yoink_server::{ServerCtx, Settings};
use yoink_sync::{SyncManager, TrustSettings};

use crate::app::{App, AppChannels};
use crate::config::Config;

/// A zero-setup shared clipboard for your LAN.
#[derive(Debug, Parser)]
#[command(name = "yoink", version, about)]
struct Args {
    /// Port for the web UI and peer sync (0 picks a free port).
    #[arg(long, default_value_t = 7679)]
    port: u16,

    /// Device name shown to peers (defaults to the hostname).
    #[arg(long)]
    name: Option<String>,

    /// Override the config/state directory (useful for running several
    /// instances on one machine).
    #[arg(long)]
    config_dir: Option<std::path::PathBuf>,

    /// Don't open the web UI in your browser on startup (it opens by default).
    /// Handy for headless machines or running several instances at once.
    #[arg(long = "no-open")]
    no_open: bool,

    /// How clipboard text moves between devices: `manual` (default — share
    /// only what you paste in and click Share), `auto-share` (auto-share
    /// whatever you copy; received items wait to be copied), or `mirror`
    /// (full two-way clipboard mirror).
    #[arg(long, value_parser = parse_mode, default_value_t = ShareMode::Manual)]
    mode: ShareMode,

    /// Require devices to explicitly pair before syncing the personal
    /// clipboard. Off by default: every device on your network is trusted
    /// until you block it.
    #[arg(long)]
    require_pairing: bool,

    /// Harden yoink for a network you don't control (a large shared or public
    /// LAN). A one-flag conservative preset: forces strict pairing (as
    /// `--require-pairing`) and manual share mode (`--mode manual`, overriding
    /// those flags), and stops advertising your joined room names over mDNS so
    /// strangers can't enumerate them.
    #[arg(long)]
    untrusted: bool,
}

/// clap value parser for `--mode`: maps the kebab-case spelling to a
/// [`ShareMode`] with a friendly error listing the choices.
fn parse_mode(s: &str) -> Result<ShareMode, String> {
    s.parse()
        .map_err(|_| format!("unknown mode '{s}' (expected: manual, auto-share, mirror)"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EffectiveSettings {
    require_pairing: bool,
    mode: ShareMode,
    advertise_rooms: bool,
}

/// Resolve the effective trust model, share mode and discovery posture from
/// the CLI flags.
///
/// `--untrusted` is the hardened preset for networks you don't control: it
/// forces strict pairing and manual share mode regardless of the matching
/// individual flags (warning if an explicit `--mode` is overridden), and keeps
/// joined room names out of mDNS. Future conservative defaults belong here
/// too, so the one flag locks them all.
fn resolve_hardening(args: &Args) -> EffectiveSettings {
    let require_pairing = args.require_pairing || args.untrusted;
    let mode = if args.untrusted {
        if args.mode != ShareMode::Manual {
            tracing::warn!(
                requested = %args.mode,
                "--untrusted forces manual share mode; ignoring --mode"
            );
        }
        ShareMode::Manual
    } else {
        args.mode
    };
    EffectiveSettings {
        require_pairing,
        mode,
        advertise_rooms: !args.untrusted,
    }
}

fn rooms_to_advertise(rooms: &[String], effective: EffectiveSettings) -> &[String] {
    if effective.advertise_rooms {
        rooms
    } else {
        &[]
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start tokio runtime")?;
    runtime.block_on(run(args))
}

fn init_tracing() {
    // Default to WARN so normal operation leaves stdout clean (the startup
    // banner is the only expected output); `RUST_LOG` opts into more.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn run(args: Args) -> anyhow::Result<()> {
    let effective = resolve_hardening(&args);

    let config_dir = resolve_config_dir(args.config_dir)?;
    let mut config = Config::load_or_init(&config_dir)?;
    // We only ever persist sanitized room names, but the config can be
    // hand-edited; canonicalize once so unsanitized names (which could never
    // round-trip the sync wire encoding) don't get advertised or joined.
    let sanitized_rooms = config::sanitize_rooms(&config.rooms);
    let config_dirty = sanitized_rooms != config.rooms;
    config.rooms = sanitized_rooms;

    let name = args
        .name
        .or_else(|| config.name.clone())
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned());
    let device = DeviceInfo {
        id: config.device_id.clone(),
        name,
    };

    let docs = Arc::new(DocSet::new());
    // Create every scope's doc and wire its update forwarder *before* sync or
    // the server exist, so no update can slip past the loop (broadcast
    // receivers only see messages sent after subscribing) and sync joins
    // these docs instead of creating empty ones. History is not persisted, so
    // there is nothing to restore — every scope starts empty.
    let (doc_events_tx, doc_rx) = mpsc::channel(app::DOC_EVENT_QUEUE);
    let forwarders = app::wire_scopes(&docs, &config.rooms, &doc_events_tx);

    let listener = bind_listener(args.port).await?;
    let port = listener
        .local_addr()
        .context("failed to read the bound listener address")?
        .port();
    // Best-effort: a machine without IPv6 (or with the port taken on v6 by
    // someone else) still works, peers just have to dial our v4 addresses.
    let v6_listener = match bind_v6_listener(port) {
        Ok(listener) => Some(listener),
        Err(err) => {
            tracing::warn!(error = %err, port, "failed to bind [::] listener; serving IPv4 only");
            None
        }
    };

    let (clipboard, clipboard_rx) = ClipboardHandle::spawn(Duration::from_millis(400));
    let seeded_rooms: std::collections::HashSet<String> = config.rooms.iter().cloned().collect();
    let trust = TrustSettings {
        require_pairing: effective.require_pairing,
        allowed: config.allowed.iter().cloned().collect(),
        blocked: config.blocked.iter().cloned().collect(),
    };
    let (sync, sync_rx) = SyncManager::new(docs.clone(), device.clone(), trust, &seeded_rooms);
    // Under `--untrusted` we never advertise our joined room names over mDNS,
    // so a stranger on the network cannot enumerate them.
    let advertised_rooms = rooms_to_advertise(&config.rooms, effective);
    let (discovery, discovery_rx) =
        Discovery::start(&device.id, &device.name, port, advertised_rooms)
            .context("failed to start mDNS peer discovery")?;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let settings = Arc::new(parking_lot::RwLock::new(Settings {
        mode: effective.mode,
        clipboard_available: clipboard.available(),
    }));
    let joined_rooms = Arc::new(parking_lot::RwLock::new(
        config.rooms.iter().cloned().collect::<BTreeSet<String>>(),
    ));
    let (commands_tx, command_rx) = mpsc::channel(64);
    let (notify_tx, _) = broadcast::channel(64);

    let ctx = ServerCtx {
        device: device.clone(),
        docs: docs.clone(),
        sync: sync.clone(),
        peers: peers.clone(),
        settings: settings.clone(),
        joined_rooms: joined_rooms.clone(),
        commands: commands_tx,
        notify: notify_tx.clone(),
    };
    spawn_web_servers(ctx, listener, v6_listener);

    let url = format!("http://localhost:{port}");
    banner::print(&url, &device.name, &device.id);
    if !args.no_open
        && let Err(err) = open::that(&url)
    {
        tracing::warn!(error = %err, "failed to open the web UI in a browser");
    }

    let app = App {
        docs,
        device,
        clipboard,
        auto_capture: effective.mode.captures_clipboard(),
        auto_apply: effective.mode.auto_applies(),
        advertise_rooms: effective.advertise_rooms,
        sync,
        discovery,
        peers,
        settings,
        joined_rooms,
        notify: notify_tx,
        config,
        config_dir,
        last_applied_entry_id: None,
        config_dirty,
        config_write: app::BackgroundWrite::idle(),
        doc_events_tx,
        forwarders,
    };
    app.run(AppChannels {
        clipboard: clipboard_rx,
        doc: doc_rx,
        discovery: discovery_rx,
        sync: sync_rx,
        command: command_rx,
    })
    .await
}

/// Spawn the web server on the primary `0.0.0.0` listener plus, when present,
/// the companion `[::]` one. Each runs as its own detached task; an unexpected
/// `serve` exit is logged but never brings the process down, since the app
/// loop (clipboard, sync) is the part that must keep running.
fn spawn_web_servers(
    ctx: ServerCtx,
    listener: tokio::net::TcpListener,
    v6_listener: Option<tokio::net::TcpListener>,
) {
    if let Some(v6_listener) = v6_listener {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(err) = yoink_server::serve(v6_listener, ctx).await {
                tracing::error!(error = %err, "IPv6 web server exited");
            }
        });
    }
    tokio::spawn(async move {
        if let Err(err) = yoink_server::serve(listener, ctx).await {
            tracing::error!(error = %err, "web server exited");
        }
    });
}

fn resolve_config_dir(flag: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(dir) = flag {
        return Ok(dir);
    }
    let dirs = directories::ProjectDirs::from("", "", "yoink")
        .context("could not determine a config directory for this platform; pass --config-dir")?;
    Ok(dirs.config_dir().to_path_buf())
}

async fn bind_listener(port: u16) -> anyhow::Result<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => Ok(listener),
        Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => anyhow::bail!(
            "port {port} is already in use (is another yoink instance running?); \
             pick a different one with --port, or use --port 0 for a random free port"
        ),
        Err(err) => Err(err).with_context(|| format!("failed to bind 0.0.0.0:{port}")),
    }
}

/// Companion IPv6 listener on the same port as the primary `0.0.0.0` one.
/// mDNS advertises our IPv6 addresses too, so without this a peer dialing
/// one of those would fail (the dialer prefers IPv4, making this a
/// resilience gap rather than a hard break). `IPV6_V6ONLY` is set so the
/// socket coexists with the separate v4 socket instead of failing the bind
/// on dual-stack platforms. tokio 1.52's `TcpSocket` has no `only_v6`
/// setter, hence socket2.
fn bind_v6_listener(port: u16) -> std::io::Result<tokio::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_only_v6(true)?;
    // Mirror tokio's `TcpListener::bind`, which sets SO_REUSEADDR on Unix so
    // restarts are not blocked by lingering TIME_WAIT sockets.
    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;
    let addr = std::net::SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, port));
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    // tokio's from_std requires the socket to already be non-blocking.
    socket.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(socket.into())
}

#[cfg(test)]
mod tests {
    use super::{Args, EffectiveSettings, bind_v6_listener, resolve_hardening};
    use clap::Parser as _;
    use yoink_core::ShareMode;

    #[test]
    fn untrusted_forces_hardened_effective_settings() {
        let args = Args::parse_from(["yoink", "--untrusted", "--mode", "mirror"]);

        assert_eq!(
            resolve_hardening(&args),
            EffectiveSettings {
                require_pairing: true,
                mode: ShareMode::Manual,
                advertise_rooms: false,
            }
        );
    }

    #[test]
    fn require_pairing_alone_keeps_room_advertisements() {
        let args = Args::parse_from(["yoink", "--require-pairing", "--mode", "auto-share"]);

        assert_eq!(
            resolve_hardening(&args),
            EffectiveSettings {
                require_pairing: true,
                mode: ShareMode::AutoShare,
                advertise_rooms: true,
            }
        );
    }

    #[tokio::test]
    async fn v6_listener_coexists_with_v4_on_the_same_port() {
        let v4 = tokio::net::TcpListener::bind(("0.0.0.0", 0))
            .await
            .expect("bind v4");
        let port = v4.local_addr().expect("v4 addr").port();

        // Environments without IPv6 are exactly what the production path
        // degrades on; nothing to verify there.
        let Ok(v6) = bind_v6_listener(port) else {
            eprintln!("skipping: IPv6 unavailable in this environment");
            return;
        };

        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect((std::net::Ipv6Addr::LOCALHOST, port)),
            v6.accept(),
        );
        client.expect("connect over v6 loopback");
        accepted.expect("v6 listener accepts");
    }
}
