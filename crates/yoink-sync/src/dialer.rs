//! Outbound connection establishment with reconnect backoff, one dial loop
//! per `(peer, scope)`.

use crate::connection::ConnectionOutcome;
use crate::socket::PeerSocket;
use crate::{ConnKey, DialerHandle, State, SyncManager};
use std::net::{IpAddr, SocketAddr};
use std::sync::Weak;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use yoink_core::Scope;
use yoink_discovery::PeerInfo;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Cap on inbound message/frame size for dialed sockets, mirroring the limit
/// the axum side puts on inbound connections: nothing the peer sends is
/// trusted before HELLO validation, so one giant frame must not be able to
/// balloon memory. 8 MiB comfortably fits a full clipboard document.
const MAX_WS_PAYLOAD: usize = 8 * 1024 * 1024;

/// Delay before redialing a peer whose *established* connection just ended,
/// so a flapping peer is not hammered. Failed attempts (including handshakes
/// the peer refuses) instead back off exponentially from [`INITIAL_BACKOFF`]
/// to [`MAX_BACKOFF`].
pub(crate) const REDIAL_DELAY: Duration = INITIAL_BACKOFF;

/// Whether `scope` should be dialed for `peer` at all: the personal scope
/// trusts the LAN by default (blocked devices excluded) or uses the allowlist
/// under `--require-pairing`; for a room, our own membership plus the peer
/// advertising the room over mDNS (device trust deliberately plays no part in
/// rooms).
fn scope_eligible(state: &State, peer: &PeerInfo, scope: &Scope) -> bool {
    match scope {
        Scope::Devices => state.devices_trusted(&peer.device_id),
        Scope::Room(name) => {
            state.joined.contains(name) && peer.rooms.iter().any(|room| room == name)
        }
    }
}

impl SyncManager {
    pub(crate) fn maybe_spawn_dialer(&self, peer_id: &str, scope: &Scope) {
        self.maybe_spawn_dialer_after(peer_id, scope, Duration::ZERO);
    }

    /// Start a dial loop for `(peer_id, scope)` if the dial rule elects us
    /// and the peer is discovered, eligible in that scope, and neither
    /// connected nor already being dialed in it. The loop makes its first
    /// attempt after `delay`.
    pub(crate) fn maybe_spawn_dialer_after(&self, peer_id: &str, scope: &Scope, delay: Duration) {
        // Dial rule: only the side with the smaller device id dials, the
        // other side waits for the inbound connection.
        if self.device.id.as_str() >= peer_id {
            return;
        }
        let mut state = self.state.lock();
        let eligible = state
            .peers
            .get(peer_id)
            .is_some_and(|peer| scope_eligible(&state, peer, scope));
        let key: ConnKey = (peer_id.to_string(), scope.clone());
        if !eligible || state.connections.contains_key(&key) || state.dialers.contains_key(&key) {
            return;
        }
        let generation = self.next_generation();
        let (cancel_tx, cancel_rx) = watch::channel(());
        tokio::spawn(dial_loop(
            self.self_ref.clone(),
            peer_id.to_string(),
            scope.clone(),
            generation,
            delay,
            cancel_rx,
        ));
        state.dialers.insert(
            key,
            DialerHandle {
                generation,
                cancel: cancel_tx,
            },
        );
    }

    /// Fresh `PeerInfo` (addresses and advertised rooms can change between
    /// attempts) iff dialing `(peer_id, scope)` should continue.
    fn dial_target(&self, peer_id: &str, scope: &Scope) -> Option<PeerInfo> {
        let state = self.state.lock();
        if state
            .connections
            .contains_key(&(peer_id.to_string(), scope.clone()))
        {
            return None;
        }
        state
            .peers
            .get(peer_id)
            .filter(|peer| scope_eligible(&state, peer, scope))
            .cloned()
    }

    fn remove_dialer_entry(&self, peer_id: &str, scope: &Scope, generation: u64) {
        let key: ConnKey = (peer_id.to_string(), scope.clone());
        let mut state = self.state.lock();
        if state
            .dialers
            .get(&key)
            .is_some_and(|dialer| dialer.generation == generation)
        {
            state.dialers.remove(&key);
        }
    }
}

/// Completes when the manager wants this dial loop gone: an explicit cancel
/// send or the `DialerHandle` being dropped both wake the receiver. Only
/// consulted between attempts — never while `run_connection` is pumping a
/// live socket, which is how an established connection survives mDNS flaps
/// (`peer_lost`) of its dial loop.
async fn cancelled(cancel: &mut watch::Receiver<()>) {
    let _ = cancel.changed().await;
}

async fn dial_loop(
    manager: Weak<SyncManager>,
    peer_id: String,
    scope: Scope,
    generation: u64,
    first_delay: Duration,
    mut cancel: watch::Receiver<()>,
) {
    let mut delay = first_delay;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        if !delay.is_zero() {
            tokio::select! {
                () = cancelled(&mut cancel) => return,
                () = tokio::time::sleep(delay) => {}
            }
        }
        let Some(manager) = manager.upgrade() else {
            return;
        };
        let Some(peer) = manager.dial_target(&peer_id, &scope) else {
            manager.remove_dialer_entry(&peer_id, &scope, generation);
            return;
        };
        let stream = tokio::select! {
            // Cancellation means our registry entry is already gone; nothing
            // to clean up.
            () = cancelled(&mut cancel) => return,
            stream = try_connect(&peer) => stream,
        };
        let outcome = match stream {
            Some(stream) => Some(
                manager
                    .run_connection(
                        PeerSocket::Outbound(Box::new(stream)),
                        Some(scope.clone()),
                        Some(generation),
                    )
                    .await,
            ),
            None => None,
        };
        drop(manager);
        match outcome {
            Some(ConnectionOutcome::Established) => {
                // The peer had accepted us, so this was a real connection
                // ending — not a refusal. Redial promptly with the backoff
                // reset; dial_target stops us if redialing no longer applies.
                backoff = INITIAL_BACKOFF;
                delay = REDIAL_DELAY;
            }
            // TCP/WebSocket failure, or a connection that ended before the
            // peer proved it accepted us (e.g. it does not allow us, or left
            // the room): back off so a refusing peer is not hammered at a
            // fixed rate.
            Some(ConnectionOutcome::Failed) | None => {
                delay = backoff;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn try_connect(peer: &PeerInfo) -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_PAYLOAD))
        .max_frame_size(Some(MAX_WS_PAYLOAD));
    for addr in ordered_addrs(&peer.addrs) {
        let url = format!("ws://{}/sync", SocketAddr::new(addr, peer.port));
        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(url.as_str(), Some(config), false),
        )
        .await
        {
            Ok(Ok((stream, _response))) => {
                tracing::debug!(peer = %peer.device_id, %url, "dialed peer");
                return Some(stream);
            }
            Ok(Err(err)) => tracing::debug!(peer = %peer.device_id, %url, %err, "dial failed"),
            Err(_) => tracing::debug!(peer = %peer.device_id, %url, "dial timed out"),
        }
    }
    None
}

/// IPv4 first: link-local IPv6 addresses need a scope id mDNS resolution does
/// not carry, so they are the least likely to connect.
fn ordered_addrs(addrs: &[IpAddr]) -> Vec<IpAddr> {
    let mut sorted = addrs.to_vec();
    sorted.sort_by_key(|addr| match addr {
        IpAddr::V4(_) => 0,
        IpAddr::V6(_) => 1,
    });
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn ipv4_addresses_are_tried_first() {
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let v4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let v4_2 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2));
        assert_eq!(ordered_addrs(&[v6, v4, v4_2]), vec![v4, v4_2, v6]);
    }
}
