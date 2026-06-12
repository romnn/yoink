//! Peer-to-peer document sync over `WebSockets`.
//!
//! # Wire protocol (version 2)
//!
//! Every frame is a binary WebSocket message whose first byte is a tag:
//!
//! | tag    | name        | payload                                            |
//! |--------|-------------|----------------------------------------------------|
//! | `0x01` | HELLO       | JSON `{"device_id","device_name","proto","scope"}` |
//! | `0x02` | `SYNC_STEP_1` | yrs state vector (lib0 v1)                       |
//! | `0x03` | `SYNC_STEP_2` | yrs update (lib0 v1), reply to `SYNC_STEP_1`     |
//! | `0x04` | `UPDATE`      | yrs incremental update (lib0 v1)                 |
//!
//! `scope` is the string form of [`Scope`] (`"devices"` or `"room:{name}"`)
//! and selects the document the connection syncs. Each connection carries
//! exactly one scope, so a peer pair sharing the personal clipboard and two
//! rooms holds three independent connections. A HELLO whose scope fails to
//! parse is rejected. The scope field is what bumped the protocol to
//! version 2: v1 peers, which do not send it, are refused by the version
//! check — intentionally, because a v1 peer would treat any connection as
//! the personal clipboard.
//!
//! `SYNC_STEP_2` is normally the reply to `SYNC_STEP_1`, but may also arrive
//! unsolicited carrying the sender's full state: a side that lost outbound
//! updates (its internal relay queue lagged) pushes a full-state `SYNC_STEP_2`
//! plus a fresh `SYNC_STEP_1` to every peer connected in the lagged scope to
//! repair the divergence. Applying yrs updates is idempotent, so receivers
//! need no special handling.
//!
//! Handshake: the dialing side sends HELLO immediately after connecting,
//! carrying the scope it wants to sync; the accepting side answers with its
//! own HELLO echoing that scope. A side that receives a HELLO with a
//! mismatched protocol version, a scope other than the one it dialed, or a
//! peer it does not accept in that scope closes the socket — **no document
//! data may be sent before the peer's HELLO has been validated**.
//! Acceptance is scope-specific: a `devices` HELLO is accepted only from a
//! device id on the local allowlist (so personal sync happens iff *both*
//! devices allow each other), while a `room:{name}` HELLO is accepted iff
//! this instance currently has that room joined. The device allowlist
//! deliberately does not govern rooms — open join is the design. A valid
//! HELLO for a `(device id, scope)` pair that already has a connection
//! *takes over*: the existing connection is assumed to be a zombie (the
//! peer restarted or gave up on it) and is torn down in favor of the new
//! socket; connections to the same device in other scopes are untouched.
//! After validation both sides send `SYNC_STEP_1` and answer incoming
//! `SYNC_STEP_1` with `SYNC_STEP_2` (the diff against the received state
//! vector). `SYNC_STEP_2` and `UPDATE` payloads are applied to the scope's
//! document with `origin = peer device id`.
//!
//! A peer is reported as connected ([`SyncEvent::PeerConnected`], carrying
//! the connection's scope) only once its first post-HELLO frame arrives: a
//! side that refuses us sends its HELLO *before* validating ours and then
//! closes without ever sending document frames, so its HELLO alone proves
//! nothing.
//!
//! Live propagation: the manager subscribes to each synced scope's
//! [`ClipDoc`] updates and forwards each one to every peer connected in that
//! scope except `update.origin`. yrs suppresses events for no-op
//! transactions, so this cannot echo-storm in a mesh.
//!
//! # Liveness
//!
//! Each side sends a WebSocket ping every [`KEEPALIVE_PING_INTERVAL`] and
//! hangs up after [`KEEPALIVE_IDLE_TIMEOUT`] without *any* inbound traffic
//! (data, ping or pong all count). This bounds how long a silently dead TCP
//! connection can shadow a peer and block its reconnect.
//!
//! # Dial rule
//!
//! Both sides of a peer pair may discover each other, so to avoid duplicate
//! connections only the side with the lexicographically *smaller* device id
//! dials; the other side waits for the inbound connection. Dialing is per
//! `(peer, scope)`: a peer is dialed in the `devices` scope when it is on
//! the local allowlist, and in a room scope when the room is in our joined
//! set *and* the peer advertises that room over mDNS. Failed attempts —
//! TCP/WebSocket errors, handshake failures, and connections closed before
//! the peer's first post-HELLO frame (i.e. the peer refused us) — retry with
//! exponential backoff (1s doubling to 30s) while the peer remains
//! discovered, eligible in that scope and disconnected; after an established
//! connection ends the dialer retries after 1s with the backoff reset.
//!
//! De-allowing a device hangs up and stops dialing only its `devices`-scope
//! connection; room connections to the same device survive, because the
//! allowlist does not govern rooms. Leaving a room cancels the room's dial
//! loops and hangs up all its connections.

mod connection;
mod dialer;
mod frames;
mod socket;

use parking_lot::Mutex;
use socket::PeerSocket;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use yoink_core::{ClipDoc, DeviceInfo, DocSet, DocUpdate, Scope, sanitize_room_name};
use yoink_discovery::PeerInfo;

const EVENT_QUEUE: usize = 64;

/// How often a connection sends a WebSocket ping when otherwise idle.
pub const KEEPALIVE_PING_INTERVAL: Duration = Duration::from_secs(15);

/// How long a connection may go without any inbound traffic before it is
/// considered dead and hung up. Must comfortably exceed
/// [`KEEPALIVE_PING_INTERVAL`] so a healthy peer's pongs always arrive in
/// time.
pub const KEEPALIVE_IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Connection lifecycle notifications the manager pushes to the app loop, one
/// per `(device id, scope)`. The app uses them to drive its connected-peers
/// view; the carried [`Scope`] distinguishes the personal clipboard from each
/// joined room so a peer present in several scopes shows up once per scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncEvent {
    /// A peer's connection in `scope` became live and proved it accepted us.
    ///
    /// Emitted once the peer's first post-HELLO frame proves it accepted us.
    /// May be emitted for an already-connected peer without an intervening
    /// [`SyncEvent::PeerDisconnected`]: a new connection for the same
    /// `(device id, scope)` takes over the old one (see the crate docs on
    /// takeover).
    PeerConnected {
        /// Stable id of the peer device, as advertised in its HELLO.
        device_id: String,
        /// Human-readable label from the peer's HELLO, so the app can name a
        /// peer that connected before (or without) being resolved via mDNS.
        device_name: String,
        /// Which shared space this connection syncs; lets the app attribute
        /// the peer to the personal clipboard or a specific joined room.
        scope: Scope,
    },
    /// A previously-announced connection in `scope` ended (peer hung up, was
    /// de-allowed, the room was left, or it was force-closed as wedged).
    PeerDisconnected {
        /// Stable id of the peer device whose connection ended.
        device_id: String,
        /// Which shared space the now-ended connection had been syncing.
        scope: Scope,
    },
}

/// Owns all peer connections (inbound and dialed), the allowlist and the
/// joined-room set.
///
/// Discovery, allowlist and room-membership changes are pushed in by the app
/// loop via [`SyncManager::peer_discovered`] / [`SyncManager::peer_lost`] /
/// [`SyncManager::set_allowed`] / [`SyncManager::join_room`] /
/// [`SyncManager::leave_room`]; the manager reacts by dialing, hanging up,
/// or accepting connections.
pub struct SyncManager {
    docs: Arc<DocSet>,
    device: DeviceInfo,
    state: Mutex<State>,
    events: mpsc::Sender<SyncEvent>,
    /// Lets `&self` methods hand a manager reference to spawned tasks
    /// without forcing `self: &Arc<Self>` on them.
    self_ref: Weak<Self>,
    /// Distinguishes successive connections/dialers for the same peer so a
    /// task tearing down only ever removes its *own* registry entry.
    generation: AtomicU64,
}

/// Connections and dial loops are keyed per peer *and* scope: one device may
/// hold a devices connection and several room connections at once, each
/// syncing its own document.
pub(crate) type ConnKey = (String, Scope);

#[derive(Default)]
struct State {
    allowed: HashSet<String>,
    /// Names of the rooms this instance currently has open. Governs which
    /// room HELLOs are accepted and which room scopes are dialed.
    joined: HashSet<String>,
    peers: HashMap<String, PeerInfo>,
    connections: HashMap<ConnKey, ConnectionHandle>,
    dialers: HashMap<ConnKey, DialerHandle>,
    /// One fan-out task per scope being synced; dropping a sender (or
    /// sending on it) stops the task. The `Devices` entry lives as long as
    /// the manager, room entries come and go with join/leave.
    fan_outs: HashMap<Scope, watch::Sender<()>>,
    keepalive: Keepalive,
}

/// Keepalive timing, kept in state (rather than as bare consts at the use
/// site) so tests can shrink the intervals to something exercisable.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Keepalive {
    pub(crate) ping_interval: Duration,
    pub(crate) idle_timeout: Duration,
}

impl Default for Keepalive {
    fn default() -> Self {
        Self {
            ping_interval: KEEPALIVE_PING_INTERVAL,
            idle_timeout: KEEPALIVE_IDLE_TIMEOUT,
        }
    }
}

/// A live, handshake-complete connection.
struct ConnectionHandle {
    generation: u64,
    outbound: mpsc::Sender<Vec<u8>>,
    /// Sending `true` (or dropping the handle, which closes the channel)
    /// makes the connection task tear the socket down immediately — even
    /// mid-send, so a peer wedged in TCP backpressure cannot keep the socket
    /// open.
    hangup: watch::Sender<bool>,
    /// Whether [`SyncEvent::PeerConnected`] has been emitted for this entry,
    /// so whoever removes it knows whether a `PeerDisconnected` is owed.
    /// Only read or written under the state lock.
    announced: bool,
}

struct DialerHandle {
    generation: u64,
    /// Sending (or dropping the handle, which closes the channel) stops the
    /// dial loop before its next attempt. A live connection the dial task is
    /// currently pumping is deliberately unaffected — see `peer_lost`.
    cancel: watch::Sender<()>,
}

impl SyncManager {
    /// `allowed` and `joined_rooms` seed the allowlist and room membership
    /// from persisted config; every seeded room is joined exactly as if
    /// [`SyncManager::join_room`] had been called (its doc is created in
    /// `docs` when missing and its fan-out task starts).
    ///
    /// Must be called from within a tokio runtime: it spawns the tasks that
    /// fan document updates out to connected peers.
    pub fn new(
        docs: Arc<DocSet>,
        device: DeviceInfo,
        allowed: HashSet<String>,
        joined_rooms: &HashSet<String>,
    ) -> (Arc<Self>, mpsc::Receiver<SyncEvent>) {
        let (events_tx, events_rx) = mpsc::channel(EVENT_QUEUE);
        let manager = Arc::new_cyclic(|weak| Self {
            docs,
            device,
            state: Mutex::new(State {
                allowed,
                ..State::default()
            }),
            events: events_tx,
            self_ref: weak.clone(),
            generation: AtomicU64::new(0),
        });
        {
            let devices_doc = manager.docs.devices();
            let mut state = manager.state.lock();
            manager.spawn_fan_out(&mut state, Scope::Devices, devices_doc);
        }
        for room in joined_rooms {
            manager.join_room(room);
        }
        (manager, events_rx)
    }

    /// Override keepalive timing for connections established afterwards.
    /// Testing aid: the production defaults make the ping/idle paths take
    /// tens of seconds to observe.
    #[doc(hidden)]
    pub fn set_keepalive(&self, ping_interval: Duration, idle_timeout: Duration) {
        self.state.lock().keepalive = Keepalive {
            ping_interval,
            idle_timeout,
        };
    }

    /// Allow or disallow syncing with a device in the `devices` scope.
    /// Disallowing disconnects any live devices-scope connection; allowing
    /// dials if the peer is currently discovered (and the dial rule says we
    /// are the dialer). Room connections to the same device are unaffected
    /// either way — the allowlist does not govern rooms.
    pub fn set_allowed(&self, device_id: &str, allowed: bool) {
        if allowed {
            self.state.lock().allowed.insert(device_id.to_string());
            self.maybe_spawn_dialer(device_id, &Scope::Devices);
            return;
        }
        let key: ConnKey = (device_id.to_string(), Scope::Devices);
        let (connection, dialer) = {
            let mut state = self.state.lock();
            state.allowed.remove(device_id);
            (state.connections.remove(&key), state.dialers.remove(&key))
        };
        if let Some(dialer) = dialer {
            let _ = dialer.cancel.send(());
        }
        if let Some(connection) = connection {
            self.drop_connection(&key, &connection);
        }
    }

    /// Whether `device_id` is on the `devices`-scope allowlist (i.e. we are
    /// willing to sync the personal clipboard with it).
    #[must_use]
    pub fn is_allowed(&self, device_id: &str) -> bool {
        self.state.lock().allowed.contains(device_id)
    }

    /// Snapshot of the current `devices`-scope allowlist, for persisting it or
    /// rendering it in the UI.
    #[must_use]
    pub fn allowed(&self) -> HashSet<String> {
        self.state.lock().allowed.clone()
    }

    /// Device ids with a live, handshake-complete connection in `scope`.
    pub fn connected(&self, scope: &Scope) -> HashSet<String> {
        self.state
            .lock()
            .connections
            .keys()
            .filter(|(_, connection_scope)| connection_scope == scope)
            .map(|(device_id, _)| device_id.clone())
            .collect()
    }

    /// Open a room: create its doc when missing (joining doubles as
    /// creation), start fanning its updates out, and dial every discovered
    /// peer that advertises the room. Idempotent — joining an already-joined
    /// room is a no-op. `name` must already be sanitized
    /// ([`sanitize_room_name`]); anything else is ignored with a warning,
    /// since an unsanitizable name can never round-trip the wire encoding.
    ///
    /// The room doc itself is *not* removed by [`SyncManager::leave_room`];
    /// the app owns doc lifecycle (it snapshots rooms to disk before
    /// dropping them from the [`DocSet`]).
    pub fn join_room(&self, name: &str) {
        if sanitize_room_name(name).as_deref() != Some(name) {
            tracing::warn!(room = name, "ignoring join for unsanitized room name");
            return;
        }
        let scope = Scope::room(name);
        let dial_targets: Vec<String> = {
            let mut state = self.state.lock();
            if !state.joined.insert(name.to_string()) {
                return;
            }
            let doc = self.docs.get_or_create(&scope);
            self.spawn_fan_out(&mut state, scope.clone(), doc);
            state
                .peers
                .values()
                .filter(|peer| peer.rooms.iter().any(|room| room == name))
                .map(|peer| peer.device_id.clone())
                .collect()
        };
        for device_id in dial_targets {
            self.maybe_spawn_dialer(&device_id, &scope);
        }
    }

    /// Close a room: stop its fan-out, cancel its dial loops and hang up all
    /// its connections (each announced one emits a
    /// [`SyncEvent::PeerDisconnected`] carrying the room scope). Subsequent
    /// inbound HELLOs for the room are refused until it is joined again.
    pub fn leave_room(&self, name: &str) {
        let scope = Scope::room(name);
        let (fan_out, dialers, connections) = {
            let mut state = self.state.lock();
            if !state.joined.remove(name) {
                return;
            }
            let fan_out = state.fan_outs.remove(&scope);
            let dialer_keys: Vec<ConnKey> = state
                .dialers
                .keys()
                .filter(|(_, dialer_scope)| *dialer_scope == scope)
                .cloned()
                .collect();
            let dialers: Vec<DialerHandle> = dialer_keys
                .iter()
                .filter_map(|key| state.dialers.remove(key))
                .collect();
            let connection_keys: Vec<ConnKey> = state
                .connections
                .keys()
                .filter(|(_, connection_scope)| *connection_scope == scope)
                .cloned()
                .collect();
            let connections: Vec<(ConnKey, ConnectionHandle)> = connection_keys
                .into_iter()
                .filter_map(|key| state.connections.remove(&key).map(|conn| (key, conn)))
                .collect();
            (fan_out, dialers, connections)
        };
        // Dropping the watch sender closes the channel, which the fan-out
        // task treats as cancellation.
        drop(fan_out);
        for dialer in dialers {
            let _ = dialer.cancel.send(());
        }
        for (key, connection) in connections {
            self.drop_connection(&key, &connection);
        }
    }

    /// Discovery reported a peer (new or re-resolved with fresh addresses
    /// and room advertisements).
    pub fn peer_discovered(self: &Arc<Self>, peer: PeerInfo) {
        if peer.device_id == self.device.id {
            // Discovery filters our own announcement, but stay defensive: a
            // self-dial would tie up a connection slot until the handshake
            // rejects it.
            return;
        }
        let device_id = peer.device_id.clone();
        let scopes: Vec<Scope> = {
            let mut state = self.state.lock();
            let mut scopes = vec![Scope::Devices];
            scopes.extend(
                peer.rooms
                    .iter()
                    .filter(|room| state.joined.contains(*room))
                    .map(Scope::room),
            );
            state.peers.insert(device_id.clone(), peer);
            scopes
        };
        for scope in &scopes {
            self.maybe_spawn_dialer(&device_id, scope);
        }
    }

    /// Discovery reported a peer gone; stops any dial loops for it in every
    /// scope.
    pub fn peer_lost(&self, device_id: &str) {
        let dialers: Vec<DialerHandle> = {
            let mut state = self.state.lock();
            state.peers.remove(device_id);
            let keys: Vec<ConnKey> = state
                .dialers
                .keys()
                .filter(|(dialer_id, _)| dialer_id == device_id)
                .cloned()
                .collect();
            keys.iter()
                .filter_map(|key| state.dialers.remove(key))
                .collect()
        };
        for dialer in dialers {
            let _ = dialer.cancel.send(());
        }
        // Established connections are deliberately left alone: mDNS
        // announcements flap while a working socket keeps syncing fine.
    }

    /// Serve an inbound peer connection (the axum `/sync` route hands the
    /// upgraded socket here). The connection's scope is whatever the peer's
    /// HELLO asks for. Runs until the connection closes.
    pub async fn handle_inbound(self: &Arc<Self>, socket: axum::extract::ws::WebSocket) {
        self.run_connection(PeerSocket::Inbound(Box::new(socket)), None, None)
            .await;
    }

    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Relaxed)
    }

    fn emit(&self, event: SyncEvent) {
        // try_send so a stalled consumer cannot wedge connection tasks; the
        // queue is deep enough that loss means the app loop is gone anyway.
        if let Err(err) = self.events.try_send(event) {
            tracing::warn!(%err, "dropping sync event");
        }
    }

    /// Start the task that forwards `scope`'s doc updates to its peers and
    /// register its cancel handle. Caller holds the state lock.
    fn spawn_fan_out(&self, state: &mut State, scope: Scope, doc: Arc<ClipDoc>) {
        let updates = doc.subscribe();
        let (cancel_tx, cancel_rx) = watch::channel(());
        tokio::spawn(fan_out(
            self.self_ref.clone(),
            scope.clone(),
            doc,
            updates,
            cancel_rx,
        ));
        state.fan_outs.insert(scope, cancel_tx);
    }

    /// Tear down a connection handle that has already been removed from the
    /// registry, emitting the disconnect iff the peer had been announced.
    fn drop_connection(&self, key: &ConnKey, handle: &ConnectionHandle) {
        let _ = handle.hangup.send(true);
        if handle.announced {
            self.emit(SyncEvent::PeerDisconnected {
                device_id: key.0.clone(),
                scope: key.1.clone(),
            });
        }
    }

    /// Remove and immediately tear down a connection, but only if
    /// `generation` still identifies it — a takeover may have replaced the
    /// entry since the caller looked at it.
    fn force_close(&self, key: &ConnKey, generation: u64) {
        let removed = {
            let mut state = self.state.lock();
            match state.connections.get(key) {
                Some(handle) if handle.generation == generation => state.connections.remove(key),
                _ => None,
            }
        };
        if let Some(handle) = removed {
            self.drop_connection(key, &handle);
        }
    }

    /// Queue `frame` to every peer connected in `scope` except `skip_origin`,
    /// without ever waiting: a peer whose outbound queue is full has not
    /// drained hundreds of frames and is presumed wedged, so it is hung up on
    /// the spot — reconnect plus the initial sync exchange will heal it. One
    /// slow peer must never delay the others.
    fn send_to_peers(&self, scope: &Scope, frame: &[u8], skip_origin: Option<&str>) {
        let targets: Vec<(ConnKey, u64, mpsc::Sender<Vec<u8>>)> = {
            let state = self.state.lock();
            state
                .connections
                .iter()
                .filter(|((device_id, connection_scope), _)| {
                    connection_scope == scope && skip_origin != Some(device_id.as_str())
                })
                .map(|(key, conn)| (key.clone(), conn.generation, conn.outbound.clone()))
                .collect()
        };
        for (key, generation, outbound) in targets {
            match outbound.try_send(frame.to_vec()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(peer = %key.0, "peer hung up before update was queued");
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(peer = %key.0, scope = %key.1, "outbound queue full; hanging up on wedged peer");
                    self.force_close(&key, generation);
                }
            }
        }
    }
}

/// Forwards every committed change of `scope`'s doc as an UPDATE frame to
/// all peers connected in that scope except the one a change came from.
/// `cancel` firing (or its sender being dropped) ends the task — that is how
/// `leave_room` stops a room's fan-out.
async fn fan_out(
    manager: Weak<SyncManager>,
    scope: Scope,
    doc: Arc<ClipDoc>,
    mut updates: broadcast::Receiver<DocUpdate>,
    mut cancel: watch::Receiver<()>,
) {
    loop {
        let received = tokio::select! {
            _ = cancel.changed() => return,
            received = updates.recv() => received,
        };
        match received {
            Ok(update) => {
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                let frame = frames::Frame::Update(update.update).encode();
                manager.send_to_peers(&scope, &frame, update.origin.as_deref());
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                tracing::warn!(skipped, %scope, "doc update stream lagged; pushing full resync");
                // The skipped updates never reached the peers, and yrs parks
                // any later updates that depend on them, so without repair
                // the peers diverge until their next reconnect. Push the full
                // state as an unsolicited SYNC_STEP_2 — applying it is
                // idempotent, so peers that missed nothing no-op — and a
                // fresh SYNC_STEP_1 so anything the peers have for us flows
                // back too.
                let step2 = frames::Frame::SyncStep2(doc.snapshot()).encode();
                manager.send_to_peers(&scope, &step2, None);
                let step1 = frames::Frame::SyncStep1(doc.state_vector()).encode();
                manager.send_to_peers(&scope, &step1, None);
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}
