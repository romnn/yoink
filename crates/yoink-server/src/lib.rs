//! Local HTTP server: web UI, JSON API, UI live-update WebSocket, and the
//! `/sync` endpoint peers connect to.
//!
//! # Routes
//!
//! | route          | description                                          |
//! |----------------|------------------------------------------------------|
//! | `GET /`        | embedded single-page web UI (devices view)           |
//! | `GET /r/{name}` | the same page in room view; an unsanitized name 307s to its sanitized URL, an unsanitizable one to `/` |
//! | `GET /{word}`  | unreserved single-segment shorthand; 307 to `/r/{sanitized word}` so typing `localhost:7679/standup` just works |
//! | `GET /assets/idiomorph.js` | embedded [idiomorph] DOM-morphing library  |
//! | `GET /api/state?scope=` | full UI state for one scope as JSON (shape below) |
//! | `POST /api/command` | body = [`AppCommand`] JSON; enqueues, replies 202 |
//! | `GET /ws/ui?scope=` | WebSocket; pushes the scoped state JSON on connect and on every app-loop notify |
//! | `GET /sync`    | WebSocket; peer sync, handed to [`SyncManager::handle_inbound`] |
//!
//! `scope` is the string form of [`Scope`](yoink_core::Scope) — `"devices"`
//! or `"room:{name}"` — defaulting to `devices` when absent; an unparseable
//! value answers 400. The single-segment words `api`, `ws`, `sync`, `r`,
//! `assets` and `favicon.ico` are reserved and never treated as room
//! shorthands.
//!
//! [idiomorph]: https://github.com/bigskysoftware/idiomorph
//!
//! # Security
//!
//! The listeners bind `0.0.0.0` (plus a best-effort `[::]` one) so peers can
//! reach `/sync`, therefore every *other* route — room views and redirects
//! included — must reject non-loopback clients (check
//! `ConnectInfo<SocketAddr>`; both `127.0.0.0/8` and `::1`). Only `/sync` is
//! reachable from the LAN, and it never reveals document data before the
//! sync handshake validates the peer. Both WebSocket routes cap message and
//! frame size at 8 MiB so a stranger cannot make us buffer unbounded frames
//! before that validation.
//!
//! # State JSON shape
//!
//! ```json
//! {
//!   "device": {"id": "...", "name": "..."},
//!   "scope": "devices",
//!   "settings": {"auto_apply": true, "clipboard_available": true},
//!   "peers": [{"id": "...", "name": "...", "online": true,
//!              "allowed": false, "connected": false}],
//!   "rooms": {"joined": ["attic"],
//!             "network": [{"name": "attic", "devices": 2}]},
//!   "members": [{"id": "...", "name": "...", "connected": true}],
//!   "entries": [{"id": "...", "device_id": "...", "device_name": "...",
//!                "text": "...", "created_at_ms": 0}]
//! }
//! ```
//!
//! `peers` is always the devices view: the union of currently discovered
//! peers and allowed-but-offline device ids (so the user can revoke an
//! offline peer), with `connected` meaning a live `devices`-scope sync
//! connection regardless of the requested scope. `rooms.network` is the
//! union of rooms advertised by online peers and our own joined rooms,
//! `devices` counting the online peers advertising each (0 is possible for
//! a room only we hold). `members` is populated for room scopes only:
//! online peers advertising the room, `connected` meaning a live sync
//! connection in that room. `entries` is newest-first, capped at 100 for
//! the UI, and empty when the scope has no document (room not joined).

mod loopback;
mod routes;
mod state_json;

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use yoink_core::{AppCommand, DeviceInfo, DocSet};
use yoink_discovery::PeerInfo;
use yoink_sync::SyncManager;

/// UI-visible slice of the app's runtime configuration. A snapshot the app
/// loop keeps in sync; the server only reads it to fill `settings` in the
/// state JSON.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Whether incoming personal-clipboard entries are written to the OS
    /// clipboard automatically (the user's `SetAutoApply` choice).
    pub auto_apply: bool,
    /// Whether an OS clipboard backend was found at startup; when `false` the
    /// UI hides clipboard-dependent affordances rather than failing silently.
    pub clipboard_available: bool,
}

/// A peer as the UI should see it (registry maintained by the app loop).
#[derive(Debug, Clone)]
pub struct PeerView {
    /// Last-known discovery announcement for the peer (identity, name, rooms).
    pub info: PeerInfo,
    /// Whether the peer is currently advertising on the network. Kept distinct
    /// from removal so an allowed-but-offline peer can still be revoked.
    pub online: bool,
}

/// Shared state handed to the server. The server only ever *reads* shared
/// state and enqueues [`AppCommand`]s; all mutation happens in the app loop.
#[derive(Clone)]
pub struct ServerCtx {
    /// This instance's own identity, rendered as `device` in the state JSON.
    pub device: DeviceInfo,
    /// All scoped documents; read to build the `entries` history per scope.
    pub docs: Arc<DocSet>,
    /// Peer sync engine: serves `/sync` inbound connections and reports
    /// per-scope live connectivity and the personal-clipboard allowlist.
    pub sync: Arc<SyncManager>,
    /// Discovered/allowed peer registry keyed by device id, maintained by the
    /// app loop; the union the devices view renders.
    pub peers: Arc<parking_lot::RwLock<HashMap<String, PeerView>>>,
    /// UI-visible settings snapshot, kept current by the app loop.
    pub settings: Arc<parking_lot::RwLock<Settings>>,
    /// Names of the rooms this instance currently has open; a mirror
    /// maintained by the app loop (the source of truth lives in the sync
    /// manager and the doc set, which the server must not mutate).
    pub joined_rooms: Arc<parking_lot::RwLock<BTreeSet<String>>>,
    /// Channel the handlers enqueue [`AppCommand`]s on; the app loop is the
    /// sole consumer and the only place state actually mutates.
    pub commands: mpsc::Sender<AppCommand>,
    /// App loop sends `()` whenever any UI-visible state changed.
    pub notify: broadcast::Sender<()>,
}

/// Serve until the listener fails or the task is cancelled.
///
/// # Errors
///
/// Propagates the error from [`axum::serve`] if accepting connections fails
/// (e.g. the listening socket is closed out from under it).
pub async fn serve(listener: tokio::net::TcpListener, ctx: ServerCtx) -> anyhow::Result<()> {
    let app = routes::router(ctx);
    // Connect info is what lets the loopback guard tell local browsers apart
    // from LAN peers, so it must be wired in here and not be optional.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
