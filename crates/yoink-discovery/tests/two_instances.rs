//! Real-network integration test: two yoink instances on one machine must
//! discover each other via multicast loopback.
//!
//! Requires a network interface with multicast enabled, so it is ignored by
//! default; run with: `cargo test -p yoink-discovery -- --ignored`

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;
use yoink_discovery::{Discovery, DiscoveryEvent, PeerInfo};

/// Drains discovery events until `device_id` is found. Returns `None` if the
/// channel closes first (the daemon stopped), letting the calling test fail
/// with a panic in test context rather than panicking inside this helper.
async fn wait_for_found(
    rx: &mut mpsc::Receiver<DiscoveryEvent>,
    device_id: &str,
) -> Option<PeerInfo> {
    while let Some(event) = rx.recv().await {
        if let DiscoveryEvent::Found(peer) = event
            && peer.device_id == device_id
        {
            return Some(peer);
        }
    }
    None
}

/// Drains discovery events until `expected` is reported lost. Returns `false`
/// if the channel closes first, so the calling test can panic in test context.
async fn wait_for_lost(rx: &mut mpsc::Receiver<DiscoveryEvent>, expected: &str) -> bool {
    while let Some(event) = rx.recv().await {
        if let DiscoveryEvent::Lost { device_id } = event
            && device_id == expected
        {
            return true;
        }
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires multicast networking; run with --ignored"]
async fn two_instances_discover_each_other() {
    let (disco_a, mut rx_a) =
        Discovery::start("itest-aaaa-1111", "yoink-itest-a", 49301, &[]).expect("start instance a");
    let (disco_b, mut rx_b) =
        Discovery::start("itest-bbbb-2222", "yoink-itest-b", 49302, &[]).expect("start instance b");

    let deadline = Duration::from_secs(10);
    let found_b = timeout(deadline, wait_for_found(&mut rx_a, "itest-bbbb-2222"))
        .await
        .expect("instance a did not discover instance b in time")
        .expect("instance a's discovery channel closed before finding b");
    let found_a = timeout(deadline, wait_for_found(&mut rx_b, "itest-aaaa-1111"))
        .await
        .expect("instance b did not discover instance a in time")
        .expect("instance b's discovery channel closed before finding a");

    assert_eq!(found_b.name, "yoink-itest-b");
    assert_eq!(found_b.port, 49302);
    assert!(
        !found_b.addrs.is_empty(),
        "peer b resolved with no addresses"
    );

    assert_eq!(found_a.name, "yoink-itest-a");
    assert_eq!(found_a.port, 49301);
    assert!(
        !found_a.addrs.is_empty(),
        "peer a resolved with no addresses"
    );

    // `shutdown` waits for the unregister confirmation, so the goodbye
    // packets must already be on the wire when it returns — observable as a
    // prompt Lost event on the other instance rather than a TTL expiry.
    disco_a.shutdown();
    let observed_lost = timeout(deadline, wait_for_lost(&mut rx_b, "itest-aaaa-1111"))
        .await
        .expect("instance b did not observe instance a's goodbye in time");
    assert!(
        observed_lost,
        "instance b's discovery channel closed before observing a's goodbye"
    );
    disco_b.shutdown();
}
