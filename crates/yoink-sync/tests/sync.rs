//! End-to-end tests: two full stacks (`DocSet` + `SyncManager` + a minimal axum
//! server exposing `/sync`) on loopback ephemeral ports, discovery simulated
//! by calling `peer_discovered` directly. A hand-rolled websocket client
//! speaks the wire protocol directly where a misbehaving peer is needed.

use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use yoink_core::{ClipDoc, DeviceInfo, DocSet, PROTOCOL_VERSION, Scope};
use yoink_discovery::PeerInfo;
use yoink_sync::{SyncEvent, SyncManager, TrustSettings};

const TIMEOUT: Duration = Duration::from_secs(10);

const TAG_HELLO: u8 = 0x01;
const TAG_SYNC_STEP_1: u8 = 0x02;
const TAG_SYNC_STEP_2: u8 = 0x03;
const TAG_UPDATE: u8 = 0x04;

struct Stack {
    docs: Arc<DocSet>,
    /// The devices (personal) doc, which most tests touch.
    doc: Arc<ClipDoc>,
    /// Room names this stack advertises over (simulated) mDNS.
    rooms: Vec<String>,
    device: DeviceInfo,
    manager: Arc<SyncManager>,
    events: mpsc::Receiver<SyncEvent>,
    addr: SocketAddr,
}

/// Spin up a full loopback stack. Fallible (the ephemeral-port bind can fail
/// if the host is exhausted) so the calling test panics in test context — see
/// the crate-wide convention of keeping `unwrap`/`expect` out of test helpers.
async fn stack(id: &str, allowed: &[&str]) -> Result<Stack, std::io::Error> {
    stack_with_rooms(id, allowed, &[]).await
}

/// Allowlist-based stack: runs in the strict pairing model so `allowed` gates
/// connections exactly as the mutual-allow tests expect.
async fn stack_with_rooms(
    id: &str,
    allowed: &[&str],
    rooms: &[&str],
) -> Result<Stack, std::io::Error> {
    let trust = TrustSettings {
        require_pairing: true,
        allowed: allowed.iter().map(ToString::to_string).collect(),
        blocked: HashSet::new(),
    };
    stack_with_trust(id, trust, rooms).await
}

/// A stack in the default trust model: every discovered device is trusted
/// unless explicitly blocked, so peers connect without any pairing.
async fn trusting_stack(id: &str) -> Result<Stack, std::io::Error> {
    stack_with_trust(id, TrustSettings::default(), &[]).await
}

async fn stack_with_trust(
    id: &str,
    trust: TrustSettings,
    rooms: &[&str],
) -> Result<Stack, std::io::Error> {
    let docs = Arc::new(DocSet::new());
    let device = DeviceInfo {
        id: id.to_string(),
        name: format!("device-{id}"),
    };
    let joined: HashSet<String> = rooms.iter().map(ToString::to_string).collect();
    let (manager, events) = SyncManager::new(docs.clone(), device.clone(), trust, &joined);

    let app = axum::Router::new()
        .route("/sync", axum::routing::any(sync_route))
        .with_state(manager.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok(Stack {
        doc: docs.devices(),
        docs,
        rooms: rooms.iter().map(ToString::to_string).collect(),
        device,
        manager,
        events,
        addr,
    })
}

async fn sync_route(
    axum::extract::State(manager): axum::extract::State<Arc<SyncManager>>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| async move { manager.handle_inbound(socket).await })
}

fn peer_info(stack: &Stack) -> PeerInfo {
    PeerInfo {
        device_id: stack.device.id.clone(),
        name: stack.device.name.clone(),
        addrs: vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        port: stack.addr.port(),
        rooms: stack.rooms.clone(),
    }
}

fn connected_event(device_id: &str, device_name: &str, scope: Scope) -> SyncEvent {
    SyncEvent::PeerConnected {
        device_id: device_id.into(),
        device_name: device_name.into(),
        scope,
    }
}

fn disconnected_event(device_id: &str, scope: Scope) -> SyncEvent {
    SyncEvent::PeerDisconnected {
        device_id: device_id.into(),
        scope,
    }
}

/// Poll `cond` until it holds, panicking after [`TIMEOUT`].
async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Receive events until `expected` shows up. The inner block reports `false`
/// if the channel closes first, so this fails in test context (an `assert!`,
/// not a bare `panic!`) on both timeout and closure.
async fn expect_event(events: &mut mpsc::Receiver<SyncEvent>, expected: SyncEvent) {
    let mut seen = Vec::new();
    let arrived = tokio::time::timeout(TIMEOUT, async {
        loop {
            match events.recv().await {
                Some(event) if event == expected => return true,
                Some(event) => seen.push(event),
                None => return false,
            }
        }
    })
    .await;
    assert!(
        matches!(arrived, Ok(true)),
        "did not observe {expected:?} (timeout or channel closed); saw {seen:?}"
    );
}

/// Receive events until all of `expected` have shown up, in any order —
/// needed when connections in several scopes race each other. Reports `false`
/// on channel closure so the assertion (not a bare `panic!`) fires in test
/// context.
async fn expect_events(events: &mut mpsc::Receiver<SyncEvent>, mut expected: Vec<SyncEvent>) {
    let arrived = tokio::time::timeout(TIMEOUT, async {
        while !expected.is_empty() {
            match events.recv().await {
                Some(event) => {
                    if let Some(found) = expected.iter().position(|e| *e == event) {
                        expected.remove(found);
                    }
                }
                None => return false,
            }
        }
        true
    })
    .await;
    assert!(
        matches!(arrived, Ok(true)),
        "did not observe all events (timeout or channel closed); still waiting for {expected:?}"
    );
}

fn connect_mutually(a: &Stack, b: &Stack) {
    a.manager.peer_discovered(peer_info(b));
    b.manager.peer_discovered(peer_info(a));
}

type RawClient =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect to a stack's `/sync` endpoint as a bare websocket client. `None`
/// if the dial fails, so the calling test fails in test context.
async fn raw_connect(addr: SocketAddr) -> Option<RawClient> {
    let (stream, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
        .await
        .ok()?;
    Some(stream)
}

fn tagged(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![tag];
    frame.extend_from_slice(payload);
    frame
}

fn hello_frame(device_id: &str, device_name: &str, scope: &str) -> Vec<u8> {
    let json = serde_json::json!({
        "device_id": device_id,
        "device_name": device_name,
        "proto": PROTOCOL_VERSION,
        "scope": scope,
    });
    // Serializing a `serde_json::Value` cannot fail; the default mirrors the
    // production encoder's infallible fallback.
    tagged(TAG_HELLO, &serde_json::to_vec(&json).unwrap_or_default())
}

async fn raw_send(client: &mut RawClient, frame: Vec<u8>) {
    // A send failure means the connection is gone; the test surfaces it in
    // test context via the next `raw_recv` returning `None`, so swallowing the
    // error here keeps the helper from panicking outside a test function.
    let _ = client.send(WsMessage::Binary(frame.into())).await;
}

/// Next binary frame, skipping control traffic; `None` once the server closed
/// or nothing arrives within [`TIMEOUT`] (the calling test then fails in test
/// context rather than this helper panicking).
async fn raw_recv(client: &mut RawClient) -> Option<Vec<u8>> {
    loop {
        let message = tokio::time::timeout(TIMEOUT, client.next()).await.ok()??;
        match message {
            Ok(WsMessage::Binary(payload)) => return Some(payload.to_vec()),
            Ok(WsMessage::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

/// Drive a raw client through the handshake in `scope`: send our HELLO, read
/// the server's HELLO and `SYNC_STEP_1`, and return the server's state vector.
/// `None` if the server deviates from the handshake, so the calling test fails
/// in test context rather than this helper panicking.
async fn raw_handshake(
    client: &mut RawClient,
    device_id: &str,
    device_name: &str,
    scope: &str,
) -> Option<Vec<u8>> {
    raw_send(client, hello_frame(device_id, device_name, scope)).await;
    let hello = raw_recv(client).await?;
    let (&hello_tag, _) = hello.split_first()?;
    if hello_tag != TAG_HELLO {
        return None;
    }
    let step1 = raw_recv(client).await?;
    let (&step1_tag, state_vector) = step1.split_first()?;
    (step1_tag == TAG_SYNC_STEP_1).then(|| state_vector.to_vec())
}

#[test]
fn keepalive_constants_are_sane() {
    // The idle timeout must give a healthy peer multiple ping/pong rounds
    // before we declare it dead.
    assert!(yoink_sync::KEEPALIVE_PING_INTERVAL * 2 <= yoink_sync::KEEPALIVE_IDLE_TIMEOUT);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_allow_connects_once_and_syncs_both_ways() {
    let mut a = stack("aaa", &["bbb"]).await.expect("test stack");
    let mut b = stack("bbb", &["aaa"]).await.expect("test stack");
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;
    expect_event(
        &mut b.events,
        connected_event("aaa", "device-aaa", Scope::Devices),
    )
    .await;

    // The dial rule (only "aaa" dials) must leave exactly one connection on
    // each side even though both discovered each other.
    wait_for("exactly one connection per side", || {
        a.manager.connected(&Scope::Devices) == HashSet::from(["bbb".to_string()])
            && b.manager.connected(&Scope::Devices) == HashSet::from(["aaa".to_string()])
    })
    .await;

    a.doc.add_entry(&a.device, "from a".into());
    wait_for("entry from a to reach b", || {
        b.doc.entries().iter().any(|e| e.text == "from a")
    })
    .await;

    b.doc.add_entry(&b.device, "from b".into());
    wait_for("entry from b to reach a", || {
        a.doc.entries().iter().any(|e| e.text == "from b")
    })
    .await;

    assert_eq!(a.manager.connected(&Scope::Devices).len(), 1);
    assert_eq!(b.manager.connected(&Scope::Devices).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn one_sided_allow_is_refused() {
    let mut a = stack("aaa", &["bbb"]).await.expect("test stack");
    let mut b = stack("bbb", &[]).await.expect("test stack");
    connect_mutually(&a, &b);

    a.doc.add_entry(&a.device, "secret".into());

    // Give A's dialer ample time for several (refused) attempts.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert!(b.manager.connected(&Scope::Devices).is_empty());
    assert!(
        b.doc.entries().is_empty(),
        "B refused A, so no document data may have crossed"
    );
    while let Ok(event) = b.events.try_recv() {
        assert!(
            !matches!(event, SyncEvent::PeerConnected { .. }),
            "B must never report an unpaired peer as connected: {event:?}"
        );
    }
    // B sends its HELLO before validating ours and only then closes, so A's
    // handshake "succeeds" — but B never sends a post-HELLO frame, so A must
    // not announce the peer (no connected/disconnected flapping).
    if let Ok(event) = a.events.try_recv() {
        panic!("A must not emit any event for a peer that refused it: {event:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn default_trust_model_connects_without_pairing() {
    // Neither side explicitly lists the other; under the default trust model
    // they must still connect and sync the personal clipboard automatically.
    let mut a = trusting_stack("aaa").await.expect("test stack");
    let b = trusting_stack("bbb").await.expect("test stack");
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;

    b.doc.add_entry(&b.device, "no pairing needed".into());
    wait_for("entry reaches a without any pairing", || {
        a.doc
            .entries()
            .iter()
            .any(|e| e.text == "no pairing needed")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn default_trust_model_block_refuses_then_unblock_reconnects() {
    let mut a = trusting_stack("aaa").await.expect("test stack");
    let trust = TrustSettings {
        blocked: ["aaa".to_string()].into(),
        ..TrustSettings::default()
    };
    let mut b = stack_with_trust("bbb", trust, &[])
        .await
        .expect("test stack");
    connect_mutually(&a, &b);

    a.doc.add_entry(&a.device, "blocked secret".into());
    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert!(!b.manager.is_trusted("aaa"));
    assert_eq!(b.manager.blocked(), HashSet::from(["aaa".to_string()]));
    assert!(b.manager.connected(&Scope::Devices).is_empty());
    assert!(
        b.doc.entries().is_empty(),
        "blocked peer must not sync document data"
    );
    while let Ok(event) = b.events.try_recv() {
        assert!(
            !matches!(event, SyncEvent::PeerConnected { .. }),
            "B must never report a blocked peer as connected: {event:?}"
        );
    }

    b.manager.set_trusted("aaa", true);
    assert!(b.manager.is_trusted("aaa"));
    assert!(b.manager.blocked().is_empty());
    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;

    a.doc.add_entry(&a.device, "after unblock".into());
    wait_for("entry reaches b after unblock", || {
        b.doc.entries().iter().any(|e| e.text == "after unblock")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unpair_hangs_up_and_both_sides_observe_disconnect() {
    let mut a = stack("aaa", &["bbb"]).await.expect("test stack");
    let mut b = stack("bbb", &["aaa"]).await.expect("test stack");
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;
    expect_event(
        &mut b.events,
        connected_event("aaa", "device-aaa", Scope::Devices),
    )
    .await;

    // B unpairs A; A only finds out through the socket closing.
    b.manager.set_trusted("aaa", false);

    expect_event(&mut b.events, disconnected_event("aaa", Scope::Devices)).await;
    expect_event(&mut a.events, disconnected_event("bbb", Scope::Devices)).await;
    wait_for("both sides drop the connection", || {
        a.manager.connected(&Scope::Devices).is_empty()
            && b.manager.connected(&Scope::Devices).is_empty()
    })
    .await;
    assert!(!b.manager.is_trusted("aaa"));
    assert!(b.manager.allowed().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn dialer_reconnects_after_peer_returns() {
    let mut a = stack("aaa", &["bbb"]).await.expect("test stack");
    let b = stack("bbb", &["aaa"]).await.expect("test stack");
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;

    b.manager.set_trusted("aaa", false);
    expect_event(&mut a.events, disconnected_event("bbb", Scope::Devices)).await;

    // Once B pairs A again, A's backoff dialer must re-establish the
    // connection without any new discovery event.
    b.manager.set_trusted("aaa", true);
    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;

    b.doc.add_entry(&b.device, "after reconnect".into());
    wait_for("entry to reach a after reconnect", || {
        a.doc.entries().iter().any(|e| e.text == "after reconnect")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_hello_takes_over_and_sync_still_works() {
    let mut a = stack("aaa", &["zzz"]).await.expect("test stack");

    // First connection for "zzz" completes handshake + initial sync.
    let mut client1 = raw_connect(a.addr).await.expect("connect to test stack");
    let _server_sv = raw_handshake(&mut client1, "zzz", "raw-1", "devices")
        .await
        .expect("handshake");
    let fresh = ClipDoc::new();
    raw_send(&mut client1, tagged(TAG_SYNC_STEP_1, &fresh.state_vector())).await;
    expect_event(
        &mut a.events,
        connected_event("zzz", "raw-1", Scope::Devices),
    )
    .await;
    let reply = raw_recv(&mut client1).await.expect("SYNC_STEP_2 reply");
    assert_eq!(reply[0], TAG_SYNC_STEP_2);

    // Second connection for the same device id must take over: the old
    // socket is hung up and the new one syncs.
    let mut client2 = raw_connect(a.addr).await.expect("connect to test stack");
    let server_sv = raw_handshake(&mut client2, "zzz", "raw-2", "devices")
        .await
        .expect("handshake");

    assert!(
        raw_recv(&mut client1).await.is_none(),
        "the displaced connection must be closed by the server"
    );
    assert_eq!(
        a.manager.connected(&Scope::Devices),
        HashSet::from(["zzz".to_string()]),
        "takeover must never leave the peer unregistered"
    );

    // The takeover may re-announce the peer without an intervening
    // disconnect; what must never happen is a PeerDisconnected for "zzz"
    // while the new connection is live.
    let remote = ClipDoc::new();
    let remote_device = DeviceInfo {
        id: "zzz".into(),
        name: "raw-2".into(),
    };
    remote.add_entry(&remote_device, "via takeover".into());
    let diff = remote.diff(&server_sv).unwrap();
    raw_send(&mut client2, tagged(TAG_SYNC_STEP_2, &diff)).await;

    wait_for("takeover entry to reach the server doc", || {
        a.doc.entries().iter().any(|e| e.text == "via takeover")
    })
    .await;
    expect_event(
        &mut a.events,
        connected_event("zzz", "raw-2", Scope::Devices),
    )
    .await;
    while let Ok(event) = a.events.try_recv() {
        assert!(
            !matches!(event, SyncEvent::PeerDisconnected { .. }),
            "takeover must not flap the peer to disconnected: {event:?}"
        );
    }

    // Updates keep flowing outward through the surviving connection.
    a.doc.add_entry(&a.device, "to client".into());
    let update = raw_recv(&mut client2).await.expect("UPDATE frame");
    assert_eq!(update[0], TAG_UPDATE);
    remote.apply_update(&update[1..], Some("aaa")).unwrap();
    assert!(remote.entries().iter().any(|e| e.text == "to client"));
}

#[tokio::test(flavor = "multi_thread")]
async fn keepalive_pings_keep_an_idle_connection_alive() {
    let mut a = stack("aaa", &["bbb"]).await.expect("test stack");
    let mut b = stack("bbb", &["aaa"]).await.expect("test stack");
    // Aggressive intervals so several ping rounds fit into a short test; the
    // connection only survives the idle timeout if pings/pongs actually flow
    // and count as activity.
    a.manager
        .set_keepalive(Duration::from_millis(200), Duration::from_secs(1));
    b.manager
        .set_keepalive(Duration::from_millis(200), Duration::from_secs(1));
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", Scope::Devices),
    )
    .await;
    expect_event(
        &mut b.events,
        connected_event("aaa", "device-aaa", Scope::Devices),
    )
    .await;

    // No document traffic at all for 3x the idle timeout.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        a.manager.connected(&Scope::Devices),
        HashSet::from(["bbb".to_string()])
    );
    assert_eq!(
        b.manager.connected(&Scope::Devices),
        HashSet::from(["aaa".to_string()])
    );
    if let Ok(event) = a.events.try_recv() {
        panic!("idle but healthy connection must not flap: {event:?}");
    }
    if let Ok(event) = b.events.try_recv() {
        panic!("idle but healthy connection must not flap: {event:?}");
    }

    a.doc.add_entry(&a.device, "still alive".into());
    wait_for("entry to reach b after idle period", || {
        b.doc.entries().iter().any(|e| e.text == "still alive")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn silent_peer_is_hung_up_after_idle_timeout() {
    let mut a = stack("aaa", &["zzz"]).await.expect("test stack");
    a.manager
        .set_keepalive(Duration::from_millis(100), Duration::from_millis(500));

    let mut client = raw_connect(a.addr).await.expect("connect to test stack");
    let _server_sv = raw_handshake(&mut client, "zzz", "raw-silent", "devices")
        .await
        .expect("handshake");
    let fresh = ClipDoc::new();
    raw_send(&mut client, tagged(TAG_SYNC_STEP_1, &fresh.state_vector())).await;
    expect_event(
        &mut a.events,
        connected_event("zzz", "raw-silent", Scope::Devices),
    )
    .await;

    // Stop reading and writing entirely while keeping the TCP connection
    // open: the client's websocket layer only answers pings when it is
    // polled, so the server sees a black hole and must hang up on its own.
    expect_event(&mut a.events, disconnected_event("zzz", Scope::Devices)).await;
    wait_for("server to drop the zombie connection", || {
        a.manager.connected(&Scope::Devices).is_empty()
    })
    .await;
    // Only now may the client socket be dropped; otherwise the server would
    // see a clean close instead of silence.
    drop(client);
}

#[tokio::test(flavor = "multi_thread")]
async fn room_syncs_between_strangers_without_touching_devices() {
    // Neither stack allows the other: the room must connect anyway, because
    // the allowlist does not govern rooms.
    let mut a = stack_with_rooms("aaa", &[], &["attic"])
        .await
        .expect("test stack");
    let mut b = stack_with_rooms("bbb", &[], &["attic"])
        .await
        .expect("test stack");
    let room = Scope::room("attic");
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", room.clone()),
    )
    .await;
    expect_event(
        &mut b.events,
        connected_event("aaa", "device-aaa", room.clone()),
    )
    .await;
    assert_eq!(
        a.manager.connected(&room),
        HashSet::from(["bbb".to_string()])
    );

    let a_room = a.docs.get(&room).expect("join created the room doc");
    let b_room = b.docs.get(&room).expect("join created the room doc");
    a_room.add_entry(&a.device, "from a".into());
    wait_for("room entry from a to reach b", || {
        b_room.entries().iter().any(|e| e.text == "from a")
    })
    .await;
    b_room.add_entry(&b.device, "from b".into());
    wait_for("room entry from b to reach a", || {
        a_room.entries().iter().any(|e| e.text == "from b")
    })
    .await;

    // The personal scope stays untouched: no connection, no entries.
    assert!(a.manager.connected(&Scope::Devices).is_empty());
    assert!(b.manager.connected(&Scope::Devices).is_empty());
    assert!(a.doc.entries().is_empty());
    assert!(b.doc.entries().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn unjoined_room_hello_is_refused_even_for_allowed_device() {
    // "zzz" is on the devices allowlist, which must NOT grant room access.
    let mut a = stack("aaa", &["zzz"]).await.expect("test stack");

    let mut client = raw_connect(a.addr).await.expect("connect to test stack");
    raw_send(&mut client, hello_frame("zzz", "raw-room", "room:attic")).await;
    // The server sends its HELLO before validating ours, then closes without
    // any document frame.
    let hello = raw_recv(&mut client).await.expect("server HELLO");
    assert_eq!(hello[0], TAG_HELLO);
    assert!(
        raw_recv(&mut client).await.is_none(),
        "an un-joined room HELLO must be closed without document frames"
    );

    assert_eq!(
        a.docs.scopes(),
        vec![Scope::Devices],
        "a refused room HELLO must not create a room doc"
    );
    assert!(a.manager.connected(&Scope::room("attic")).is_empty());
    if let Ok(event) = a.events.try_recv() {
        panic!("no events may be emitted for a refused room connection: {event:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn leave_room_hangs_up_and_stops_room_sync() {
    let mut a = stack_with_rooms("aaa", &[], &["attic"])
        .await
        .expect("test stack");
    let mut b = stack_with_rooms("bbb", &[], &["attic"])
        .await
        .expect("test stack");
    let room = Scope::room("attic");
    connect_mutually(&a, &b);

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", room.clone()),
    )
    .await;
    expect_event(
        &mut b.events,
        connected_event("aaa", "device-aaa", room.clone()),
    )
    .await;

    let a_room = a.docs.get(&room).unwrap();
    let b_room = b.docs.get(&room).unwrap();

    b.manager.leave_room("attic");
    expect_event(&mut b.events, disconnected_event("aaa", room.clone())).await;
    expect_event(&mut a.events, disconnected_event("bbb", room.clone())).await;
    wait_for("both sides drop the room connection", || {
        a.manager.connected(&room).is_empty() && b.manager.connected(&room).is_empty()
    })
    .await;

    // A keeps the room open and adds an entry. A's dialer keeps retrying and
    // B's doc object still exists (the app removes it separately), but B has
    // left, so nothing may arrive.
    a_room.add_entry(&a.device, "after leave".into());
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        b_room.entries().is_empty(),
        "an entry crossed into a room that was left"
    );
    if let Ok(event) = b.events.try_recv() {
        panic!("no further events expected on the leaving side: {event:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn devices_and_room_connections_coexist_without_cross_bleed() {
    let mut a = stack_with_rooms("aaa", &["bbb"], &["attic"])
        .await
        .expect("test stack");
    let mut b = stack_with_rooms("bbb", &["aaa"], &["attic"])
        .await
        .expect("test stack");
    let room = Scope::room("attic");
    connect_mutually(&a, &b);

    // The devices and room connections race each other, so the two
    // PeerConnected events can arrive in either order.
    expect_events(
        &mut a.events,
        vec![
            connected_event("bbb", "device-bbb", Scope::Devices),
            connected_event("bbb", "device-bbb", room.clone()),
        ],
    )
    .await;
    expect_events(
        &mut b.events,
        vec![
            connected_event("aaa", "device-aaa", Scope::Devices),
            connected_event("aaa", "device-aaa", room.clone()),
        ],
    )
    .await;
    assert_eq!(
        a.manager.connected(&Scope::Devices),
        HashSet::from(["bbb".to_string()])
    );
    assert_eq!(
        a.manager.connected(&room),
        HashSet::from(["bbb".to_string()])
    );

    let a_room = a.docs.get(&room).unwrap();
    let b_room = b.docs.get(&room).unwrap();

    a.doc.add_entry(&a.device, "personal".into());
    wait_for("devices entry to reach b's devices doc", || {
        b.doc.entries().iter().any(|e| e.text == "personal")
    })
    .await;

    a_room.add_entry(&a.device, "shared".into());
    wait_for("room entry to reach b's room doc", || {
        b_room.entries().iter().any(|e| e.text == "shared")
    })
    .await;

    // Each update must land only in its own scope's doc.
    assert!(
        b_room.entries().iter().all(|e| e.text != "personal"),
        "a devices entry bled into the room doc"
    );
    assert!(
        b.doc.entries().iter().all(|e| e.text != "shared"),
        "a room entry bled into the devices doc"
    );
    assert!(a_room.entries().iter().all(|e| e.text != "personal"));
    assert!(a.doc.entries().iter().all(|e| e.text != "shared"));
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_is_keyed_per_device_and_scope() {
    let mut a = stack_with_rooms("aaa", &["zzz"], &["attic"])
        .await
        .expect("test stack");
    let room = Scope::room("attic");
    let a_room = a.docs.get(&room).unwrap();

    // Devices-scope connection for "zzz".
    let mut devices_client = raw_connect(a.addr).await.expect("connect to test stack");
    raw_handshake(&mut devices_client, "zzz", "raw-devices", "devices")
        .await
        .expect("handshake");
    raw_send(
        &mut devices_client,
        tagged(TAG_SYNC_STEP_1, &ClipDoc::new().state_vector()),
    )
    .await;
    expect_event(
        &mut a.events,
        connected_event("zzz", "raw-devices", Scope::Devices),
    )
    .await;
    let reply = raw_recv(&mut devices_client).await.expect("SYNC_STEP_2");
    assert_eq!(reply[0], TAG_SYNC_STEP_2);

    // A room-scope connection for the same device id must coexist with the
    // devices one, not displace it.
    let mut room_client1 = raw_connect(a.addr).await.expect("connect to test stack");
    raw_handshake(&mut room_client1, "zzz", "raw-room-1", "room:attic")
        .await
        .expect("handshake");
    raw_send(
        &mut room_client1,
        tagged(TAG_SYNC_STEP_1, &ClipDoc::new().state_vector()),
    )
    .await;
    expect_event(
        &mut a.events,
        connected_event("zzz", "raw-room-1", room.clone()),
    )
    .await;
    let reply = raw_recv(&mut room_client1).await.expect("SYNC_STEP_2");
    assert_eq!(reply[0], TAG_SYNC_STEP_2);
    assert_eq!(
        a.manager.connected(&Scope::Devices),
        HashSet::from(["zzz".to_string()])
    );
    assert_eq!(
        a.manager.connected(&room),
        HashSet::from(["zzz".to_string()])
    );

    // A second room connection for the same device takes over the room
    // connection only.
    let mut room_client2 = raw_connect(a.addr).await.expect("connect to test stack");
    raw_handshake(&mut room_client2, "zzz", "raw-room-2", "room:attic")
        .await
        .expect("handshake");
    assert!(
        raw_recv(&mut room_client1).await.is_none(),
        "the displaced room connection must be closed by the server"
    );

    // The devices connection survived the room takeover: updates still flow.
    a.doc.add_entry(&a.device, "devices entry".into());
    let update = raw_recv(&mut devices_client)
        .await
        .expect("UPDATE on the devices connection");
    assert_eq!(update[0], TAG_UPDATE);
    let devices_replica = ClipDoc::new();
    devices_replica
        .apply_update(&update[1..], Some("aaa"))
        .unwrap();
    assert!(
        devices_replica
            .entries()
            .iter()
            .any(|e| e.text == "devices entry")
    );

    // ...and room updates land on the surviving room connection.
    a_room.add_entry(&a.device, "room entry".into());
    let update = raw_recv(&mut room_client2)
        .await
        .expect("UPDATE on the room connection");
    assert_eq!(update[0], TAG_UPDATE);
    let room_replica = ClipDoc::new();
    room_replica
        .apply_update(&update[1..], Some("aaa"))
        .unwrap();
    assert!(
        room_replica
            .entries()
            .iter()
            .any(|e| e.text == "room entry")
    );

    assert_eq!(
        a.manager.connected(&Scope::Devices),
        HashSet::from(["zzz".to_string()])
    );
    assert_eq!(
        a.manager.connected(&room),
        HashSet::from(["zzz".to_string()])
    );
    while let Ok(event) = a.events.try_recv() {
        assert!(
            !matches!(event, SyncEvent::PeerDisconnected { .. }),
            "a per-scope takeover must not flap any scope to disconnected: {event:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn joining_after_discovery_dials_known_peers() {
    let mut a = stack("aaa", &[]).await.expect("test stack");
    let mut b = stack("bbb", &[]).await.expect("test stack");
    let room = Scope::room("attic");

    // Discovery happens before either side joins; both already advertise the
    // room (as their mDNS TXT records would).
    let mut info_a = peer_info(&a);
    info_a.rooms = vec!["attic".to_string()];
    let mut info_b = peer_info(&b);
    info_b.rooms = vec!["attic".to_string()];
    a.manager.peer_discovered(info_b);
    b.manager.peer_discovered(info_a);

    // Without a join on our side nothing may connect, advertised or not.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(a.manager.connected(&room).is_empty());
    assert!(b.manager.connected(&room).is_empty());

    b.manager.join_room("attic");
    a.manager.join_room("attic");
    // Idempotent: a second join must not spawn a second dialer/connection.
    a.manager.join_room("attic");

    expect_event(
        &mut a.events,
        connected_event("bbb", "device-bbb", room.clone()),
    )
    .await;
    expect_event(
        &mut b.events,
        connected_event("aaa", "device-aaa", room.clone()),
    )
    .await;
    wait_for("exactly one room connection per side", || {
        a.manager.connected(&room) == HashSet::from(["bbb".to_string()])
            && b.manager.connected(&room) == HashSet::from(["aaa".to_string()])
    })
    .await;
}
