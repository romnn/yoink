//! yoink — a zero-setup shared clipboard for your LAN.
//!
//! The binary wires everything together and runs the app event loop, which
//! is the single owner of all mutation:
//!
//! 1. Clipboard `Copied(text)` → skip if `text` matches one of the last few
//!    history entries (dedupe / echo guard; a window of one is not enough
//!    when two instances share one OS clipboard) → `add_entry` on the
//!    *devices* doc. The OS clipboard is hard-wired to the devices scope;
//!    it never feeds a room.
//! 2. Per-scope doc updates (one forwarder task per open scope feeds a
//!    merged channel) → mark that scope's snapshot dirty; if the scope is
//!    `devices`, `origin` is remote, auto-apply is on and the latest entry
//!    is from another device, wasn't applied before (track last applied
//!    entry id) and is fresh (created within ~30s of local now, so a
//!    `SYNC_STEP_2` backlog replay never clobbers the clipboard with stale
//!    entries) → `clipboard.set_text`; room updates never touch the OS
//!    clipboard; notify the UI.
//! 3. Discovery events → update the peer registry (lost peers are kept
//!    offline when allowed, removed when not), forward to
//!    `sync.peer_discovered` / `peer_lost`; notify the UI.
//! 4. Sync events (connect/disconnect, per scope) → notify the UI; a
//!    devices-scope connect also seeds the peer registry from the HELLO.
//! 5. [`AppCommand`]s from the server:
//!    - `SetAllowed` → `sync.set_allowed` + persist config + notify
//!    - `SetAutoApply` → settings + persist config + notify
//!    - `AddEntry` → dedupe against the scope's latest entry only, add to
//!      that scope's doc; devices adds also set the local clipboard (even
//!      when the entry was a duplicate) — room adds never do
//!    - `CopyEntry` → look up entry by id in the scope's doc →
//!      `clipboard.set_text` (deliberate, so allowed from rooms too)
//!    - `JoinRoom` → restore-or-create `rooms/{name}.bin`, start syncing
//!      and advertising the room, persist it in the config + notify
//!    - `LeaveRoom` → stop syncing/advertising, flush a final snapshot
//!      (the file is kept: rejoining restores history), drop the doc + notify
//! 6. Ctrl-C → save every dirty snapshot, `discovery.shutdown()`, exit.
//!
//! Snapshot and config writes run on the blocking pool (reaped on the next
//! flush tick) so a slow disk cannot stall event consumption; each scope's
//! snapshot file has its own single writer.
//!
//! The server listens on `0.0.0.0` plus a best-effort `IPV6_V6ONLY` `[::]`
//! socket on the same port, because mDNS advertises IPv6 addresses too.
//!
//! Config (`config.toml` in the config dir): `device_id` (created on first
//! run), optional `name` override, `auto_apply` (default true), `allowed`
//! (persisted allowlist), `rooms` (joined rooms, rejoined on startup). The
//! devices doc snapshot lives next to it as `state.bin`; each joined room
//! snapshots to `rooms/{name}.bin`.
//!
//! [`AppCommand`]: yoink_core::AppCommand

mod app;
mod config;

use anyhow::Context;
use clap::Parser;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use yoink_clipboard::ClipboardHandle;
use yoink_core::{DeviceInfo, DocSet};
use yoink_discovery::Discovery;
use yoink_server::{ServerCtx, Settings};
use yoink_sync::SyncManager;

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

    /// Open the web UI in the default browser after startup.
    #[arg(long)]
    open: bool,
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
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let mut config = Config::load_or_init(&config_dir)?;
    // We only ever persist sanitized room names, but the config can be
    // hand-edited; canonicalize once so unsanitized names (which could never
    // round-trip the sync wire encoding) don't get advertised or joined.
    config.rooms = config::sanitize_rooms(&config.rooms);

    let name = args
        .name
        .or_else(|| config.name.clone())
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned());
    let device = DeviceInfo {
        id: config.device_id.clone(),
        name,
    };

    let docs = Arc::new(DocSet::new());
    // Restore every persisted doc and wire its update forwarder *before*
    // sync or the server exist, so no update can slip past the loop
    // (broadcast receivers only see messages sent after subscribing) and
    // sync joins the restored room docs instead of creating empty ones.
    let (doc_events_tx, doc_rx) = mpsc::channel(app::DOC_EVENT_QUEUE);
    let (forwarders, snapshots) =
        app::restore_scopes(&docs, &config_dir, &config.rooms, &doc_events_tx);

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
    let (sync, sync_rx) = SyncManager::new(
        docs.clone(),
        device.clone(),
        config.allowed.iter().cloned().collect(),
        &seeded_rooms,
    );
    let (discovery, discovery_rx) = Discovery::start(&device.id, &device.name, port, &config.rooms)
        .context("failed to start mDNS peer discovery")?;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let settings = Arc::new(parking_lot::RwLock::new(Settings {
        auto_apply: config.auto_apply,
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
    println!("yoink — shared LAN clipboard");
    println!("  device: {} ({})", device.name, device.id);
    println!("  web UI: {url}");
    if args.open
        && let Err(err) = open::that(&url)
    {
        tracing::warn!(error = %err, "failed to open the web UI in a browser");
    }

    let app = App {
        docs,
        device,
        clipboard,
        sync,
        discovery,
        peers,
        settings,
        joined_rooms,
        notify: notify_tx,
        config,
        config_dir,
        last_applied_entry_id: None,
        snapshots,
        config_dirty: false,
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
    use super::bind_v6_listener;

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
