//! Per-connection protocol driver shared by inbound and dialed sockets:
//! handshake, registration, then frame pumping until the socket closes, goes
//! silent past the keepalive deadline, or the manager hangs up.

use crate::frames::{Frame, Hello};
use crate::socket::{Incoming, PeerSocket};
use crate::{ConnKey, ConnectionHandle, Keepalive, SyncEvent, SyncManager};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use yoink_core::{ClipDoc, PROTOCOL_VERSION, Scope};

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const OUTBOUND_QUEUE: usize = 256;
/// Closing a wedged TCP connection can itself block (the close frame is just
/// another write), so teardown gives up on politeness after this long and
/// lets the socket drop.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

/// Signals that the connection must be torn down (socket failure, protocol
/// violation, or a manager hangup); details are logged where the failure is
/// detected.
struct Hangup;

/// How a connection ended, from the dialer's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionOutcome {
    /// The peer proved it accepted us (sent a post-HELLO frame) and
    /// `PeerConnected` was emitted; redialing may start fresh.
    Established,
    /// Ended before the peer sent any post-HELLO frame: connect/handshake
    /// failure, local rejection, or the peer refusing us and closing. The
    /// dialer must treat this as a failed attempt and keep backing off.
    Failed,
}

/// Identity and resources of a validated, registered connection, bundled for
/// the frame loop.
struct Registered<'a> {
    peer: &'a Hello,
    doc: &'a ClipDoc,
    generation: u64,
    keepalive: Keepalive,
}

impl SyncManager {
    /// `dialed_scope` is the scope a dial loop set out to sync (None for
    /// inbound sockets, whose scope is learned from the peer's HELLO).
    /// `owning_dialer` is the generation of the dial loop driving this
    /// connection (None for inbound sockets), so registration knows not to
    /// cancel its own dialer as "redundant".
    pub(crate) async fn run_connection(
        self: &Arc<Self>,
        mut socket: PeerSocket,
        dialed_scope: Option<Scope>,
        owning_dialer: Option<u64>,
    ) -> ConnectionOutcome {
        // The dialing side leads with its HELLO so the accepting side learns
        // the requested scope; the accepting side answers by echoing the
        // scope back. Either way our HELLO goes out before the peer's is
        // validated: HELLO carries no document data, and a refused dialer
        // relies on seeing HELLO-then-close (rather than silence) from us.
        let peer = match &dialed_scope {
            Some(scope) => {
                let hello = self.hello_frame(scope.clone());
                if socket.send(hello).await.is_err() {
                    return ConnectionOutcome::Failed;
                }
                match await_hello(&mut socket).await {
                    Some(peer) => peer,
                    None => {
                        close_socket(socket).await;
                        return ConnectionOutcome::Failed;
                    }
                }
            }
            None => {
                let Some(peer) = await_hello(&mut socket).await else {
                    close_socket(socket).await;
                    return ConnectionOutcome::Failed;
                };
                let hello = self.hello_frame(peer.scope.clone());
                if socket.send(hello).await.is_err() {
                    close_socket(socket).await;
                    return ConnectionOutcome::Failed;
                }
                peer
            }
        };

        if peer.proto != PROTOCOL_VERSION {
            // v1 peers land here: their HELLO carries no scope (decoded as
            // `Devices`) and is refused before any document data flows.
            tracing::warn!(
                peer = %peer.device_id,
                peer_proto = peer.proto,
                our_proto = PROTOCOL_VERSION,
                "protocol version mismatch; refusing to sync"
            );
            close_socket(socket).await;
            return ConnectionOutcome::Failed;
        }
        if peer.device_id == self.device.id {
            tracing::debug!("connected to ourselves; closing");
            close_socket(socket).await;
            return ConnectionOutcome::Failed;
        }
        if let Some(scope) = &dialed_scope
            && *scope != peer.scope
        {
            tracing::warn!(
                peer = %peer.device_id,
                dialed = %scope,
                answered = %peer.scope,
                "peer answered with a different scope; refusing"
            );
            close_socket(socket).await;
            return ConnectionOutcome::Failed;
        }
        // From here on `peer.scope` is the connection's scope: it is what an
        // inbound peer asked for, and a dialed socket just verified the echo.

        // The doc is fetched outside the state lock; whether the scope is
        // still accepted is checked inside it, so a concurrent join/leave
        // resolves to either "refused" or "registered and swept by
        // leave_room" — never a live connection for an unjoined room.
        let doc = self.docs.get(&peer.scope);
        let key: ConnKey = (peer.device_id.clone(), peer.scope.clone());
        let (outbound_tx, outbound_rx) = mpsc::channel::<Vec<u8>>(OUTBOUND_QUEUE);
        let (hangup_tx, hangup_rx) = watch::channel(false);
        let generation = self.next_generation();
        // Validation + registration must be atomic, and the guard must be
        // fully out of scope before the close().await paths below.
        let (verdict, displaced, keepalive) = {
            let mut state = self.state.lock();
            let keepalive = state.keepalive;
            let accepted = match &peer.scope {
                // Rooms are open spaces: membership, not the device
                // allowlist, is what gates them.
                Scope::Devices => state.allowed.contains(&peer.device_id),
                Scope::Room(name) => state.joined.contains(name),
            };
            if !accepted {
                let reason = match &peer.scope {
                    Scope::Devices => "refusing un-allowed device",
                    Scope::Room(_) => "refusing a room we have not joined",
                };
                (Err(reason), None, keepalive)
            } else if let Some(doc) = doc {
                // Takeover: a fresh HELLO for an already-connected
                // (device, scope) pair means the existing connection is
                // probably a zombie (the peer restarted or timed out on its
                // side) — prefer the new socket. The old entry's `announced`
                // state carries over so the app's view (peer is connected)
                // stays consistent even if this connection dies before
                // announcing itself.
                let displaced = state.connections.remove(&key);
                let announced = displaced.as_ref().is_some_and(|old| old.announced);
                state.connections.insert(
                    key.clone(),
                    ConnectionHandle {
                        generation,
                        outbound: outbound_tx,
                        hangup: hangup_tx,
                        announced,
                    },
                );
                // An inbound connection can win a race against our own dialer
                // for the same peer and scope; that dialer is now redundant.
                // A dialed connection must leave its own dial loop alone — it
                // is the one driving us and handles its own retry/backoff.
                if state
                    .dialers
                    .get(&key)
                    .is_some_and(|dialer| Some(dialer.generation) != owning_dialer)
                {
                    state.dialers.remove(&key);
                }
                (Ok(doc), displaced, keepalive)
            } else {
                // A joined room is guaranteed a doc by join_room, so a miss
                // means the app tore the doc down mid-leave. Refuse rather
                // than sync into nothing.
                tracing::warn!(peer = %peer.device_id, scope = %peer.scope, "no document for scope; refusing");
                (Err("no document for scope"), None, keepalive)
            }
        };
        if let Some(old) = displaced {
            tracing::info!(peer = %peer.device_id, scope = %peer.scope, "new connection takes over; dropping the old one");
            // The old task's generation no longer matches the registry entry,
            // so its teardown neither removes our entry nor emits a spurious
            // PeerDisconnected.
            let _ = old.hangup.send(true);
        }
        let doc = match verdict {
            Ok(doc) => doc,
            Err(reason) => {
                tracing::info!(peer = %peer.device_id, scope = %peer.scope, reason, "closing socket after handshake");
                close_socket(socket).await;
                return ConnectionOutcome::Failed;
            }
        };

        // PeerConnected is deliberately NOT emitted yet: a peer that refuses
        // us sends its HELLO before validating ours, so only its first
        // post-HELLO frame proves it accepted us (see `pump`). Registration
        // still happens first because the register-then-SYNC_STEP_1 ordering
        // is what guarantees no update committed after our state vector
        // snapshot can be missed (it lands in the outbound queue instead).
        let mut announced = false;
        let step1 = Frame::SyncStep1(doc.state_vector()).encode();
        if socket.send(step1).await.is_ok() {
            let registered = Registered {
                peer: &peer,
                doc: &doc,
                generation,
                keepalive,
            };
            announced = self
                .pump(&registered, &mut socket, outbound_rx, hangup_rx)
                .await;
        }

        close_socket(socket).await;
        self.finish_connection(&key, generation);
        if announced {
            ConnectionOutcome::Established
        } else {
            ConnectionOutcome::Failed
        }
    }

    fn hello_frame(&self, scope: Scope) -> Vec<u8> {
        Frame::Hello(Hello {
            device_id: self.device.id.clone(),
            device_name: self.device.name.clone(),
            proto: PROTOCOL_VERSION,
            scope,
        })
        .encode()
    }

    /// Frame loop. Returns whether the peer was announced (PeerConnected
    /// emitted), which happens on its first valid post-HELLO frame.
    async fn pump(
        &self,
        conn: &Registered<'_>,
        socket: &mut PeerSocket,
        mut outbound_rx: mpsc::Receiver<Vec<u8>>,
        mut hangup: watch::Receiver<bool>,
    ) -> bool {
        let mut announced = false;
        let mut last_inbound = Instant::now();
        let mut ping_timer = tokio::time::interval_at(
            Instant::now() + conn.keepalive.ping_interval,
            conn.keepalive.ping_interval,
        );
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = hangup_signalled(&mut hangup) => break,
                _ = tokio::time::sleep_until(last_inbound + conn.keepalive.idle_timeout) => {
                    tracing::info!(peer = %conn.peer.device_id, scope = %conn.peer.scope, "connection silent past keepalive deadline; hanging up");
                    break;
                }
                _ = ping_timer.tick() => {
                    if send_or_hangup(socket, &hangup, None).await.is_err() {
                        break;
                    }
                }
                queued = outbound_rx.recv() => match queued {
                    Some(frame) => {
                        if send_or_hangup(socket, &hangup, Some(frame)).await.is_err() {
                            break;
                        }
                    }
                    // The manager dropped our handle: hang up requested.
                    None => break,
                },
                incoming = socket.recv() => match incoming {
                    Some(Incoming::Frame(payload)) => {
                        last_inbound = Instant::now();
                        match handle_frame(conn.doc, &conn.peer.device_id, &payload) {
                            Ok(reply) => {
                                if !announced {
                                    if !self.try_announce(conn.peer, conn.generation) {
                                        // Someone removed our registry entry
                                        // between registration and now.
                                        break;
                                    }
                                    announced = true;
                                }
                                if let Some(reply) = reply
                                    && send_or_hangup(socket, &hangup, Some(reply)).await.is_err()
                                {
                                    break;
                                }
                            }
                            Err(Hangup) => break,
                        }
                    }
                    // Pings/pongs prove liveness but carry no protocol data.
                    Some(Incoming::Activity) => last_inbound = Instant::now(),
                    None => break,
                },
            }
        }
        announced
    }

    /// Emit PeerConnected for this connection iff it is still the registered
    /// one; returns false when the entry is gone or replaced (a de-allow,
    /// room leave or takeover won the race), in which case the caller must
    /// hang up. Emitting under the state lock keeps the event ordered against
    /// removal sites, which read `announced` to decide whether a
    /// PeerDisconnected is owed (`emit` never blocks, so holding the lock is
    /// safe).
    fn try_announce(&self, peer: &Hello, generation: u64) -> bool {
        let key: ConnKey = (peer.device_id.clone(), peer.scope.clone());
        let mut state = self.state.lock();
        match state.connections.get_mut(&key) {
            Some(handle) if handle.generation == generation => {
                handle.announced = true;
                tracing::info!(peer = %peer.device_id, name = %peer.device_name, scope = %peer.scope, "peer connected");
                self.emit(SyncEvent::PeerConnected {
                    device_id: peer.device_id.clone(),
                    device_name: peer.device_name.clone(),
                    scope: peer.scope.clone(),
                });
                true
            }
            _ => false,
        }
    }

    fn finish_connection(self: &Arc<Self>, key: &ConnKey, generation: u64) {
        let removed = {
            let mut state = self.state.lock();
            match state.connections.get(key) {
                Some(handle) if handle.generation == generation => state.connections.remove(key),
                // Someone else (set_allowed, leave_room, a wedged-peer
                // force-close, or a takeover) already removed this connection
                // and dealt with the disconnect event.
                _ => None,
            }
        };
        if let Some(handle) = removed {
            if handle.announced {
                tracing::info!(peer = %key.0, scope = %key.1, "peer disconnected");
                self.emit(SyncEvent::PeerDisconnected {
                    device_id: key.0.clone(),
                    scope: key.1.clone(),
                });
            } else {
                // Never announced: a refused/failed handshake, not a real
                // session — logging it as a disconnect reads like flapping.
                tracing::debug!(peer = %key.0, scope = %key.1, "connection closed before announcement");
            }
        }
        // Reconnect if the peer is still discovered and eligible in this
        // scope (and the dial rule elects us). A no-op while our own dial
        // loop is registered — it drives its own retries.
        self.maybe_spawn_dialer_after(&key.0, &key.1, crate::dialer::REDIAL_DELAY);
    }
}

/// Returns the frame to send back, if the incoming frame demands a reply.
/// `doc` is the document of the connection's scope.
fn handle_frame(doc: &ClipDoc, peer_id: &str, payload: &[u8]) -> Result<Option<Vec<u8>>, Hangup> {
    match Frame::decode(payload) {
        Ok(Frame::Hello(_)) => {
            tracing::debug!(peer = %peer_id, "ignoring repeated HELLO");
            Ok(None)
        }
        Ok(Frame::SyncStep1(state_vector)) => match doc.diff(&state_vector) {
            Ok(diff) => Ok(Some(Frame::SyncStep2(diff).encode())),
            Err(err) => {
                tracing::warn!(peer = %peer_id, %err, "bad state vector; dropping connection");
                Err(Hangup)
            }
        },
        Ok(Frame::SyncStep2(update)) | Ok(Frame::Update(update)) => doc
            .apply_update(&update, Some(peer_id))
            .map(|()| None)
            .map_err(|err| {
                tracing::warn!(peer = %peer_id, %err, "bad update; dropping connection");
                Hangup
            }),
        Err(err) => {
            tracing::warn!(peer = %peer_id, %err, "undecodable frame; dropping connection");
            Err(Hangup)
        }
    }
}

/// Send `frame` (or a keepalive ping when `None`), but give up as soon as the
/// manager signals hangup: a send into a dead-but-open TCP connection can
/// block indefinitely once the kernel buffers fill, and the whole point of
/// the hangup signal is to escape exactly that.
async fn send_or_hangup(
    socket: &mut PeerSocket,
    hangup: &watch::Receiver<bool>,
    frame: Option<Vec<u8>>,
) -> Result<(), Hangup> {
    let mut hangup = hangup.clone();
    tokio::select! {
        biased;
        _ = hangup_signalled(&mut hangup) => Err(Hangup),
        result = async {
            match frame {
                Some(frame) => socket.send(frame).await,
                None => socket.send_ping().await,
            }
        } => result.map_err(|_| Hangup),
    }
}

/// Completes when the manager signals hangup — explicitly (`send(true)`) or
/// by dropping the connection handle, which closes the channel.
async fn hangup_signalled(hangup: &mut watch::Receiver<bool>) {
    // wait_for checks the current value first and errors when the channel
    // closes, so a signal sent before we got here is never missed.
    let _ = hangup.wait_for(|&fired| fired).await;
}

async fn close_socket(socket: PeerSocket) {
    if tokio::time::timeout(CLOSE_TIMEOUT, socket.close())
        .await
        .is_err()
    {
        tracing::debug!("socket close timed out; dropping it hard");
    }
}

/// The peer's HELLO, or `None` when it sent something else, sent nothing
/// usable, or ran out the handshake clock.
async fn await_hello(socket: &mut PeerSocket) -> Option<Hello> {
    match tokio::time::timeout(HELLO_TIMEOUT, recv_hello(socket)).await {
        Ok(hello) => hello,
        Err(_) => {
            tracing::debug!("peer did not send HELLO in time");
            None
        }
    }
}

async fn recv_hello(socket: &mut PeerSocket) -> Option<Hello> {
    loop {
        match socket.recv().await? {
            Incoming::Frame(payload) => {
                return match Frame::decode(&payload) {
                    Ok(Frame::Hello(hello)) => Some(hello),
                    Ok(other) => {
                        tracing::warn!(frame = other.name(), "expected HELLO as first frame");
                        None
                    }
                    Err(err) => {
                        tracing::warn!(%err, "undecodable frame during handshake");
                        None
                    }
                };
            }
            Incoming::Activity => continue,
        }
    }
}
