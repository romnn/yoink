//! Real-network integration test: two yoink instances on one machine must
//! discover each other via multicast loopback.
//!
//! Requires a network interface with multicast enabled, so it is ignored by
//! default; run with: `cargo test -p yoink-discovery -- --ignored`

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;
use yoink_discovery::{Discovery, DiscoveryEvent, PeerInfo};

async fn wait_for_found(rx: &mut mpsc::Receiver<DiscoveryEvent>, device_id: &str) -> PeerInfo {
    loop {
        match rx.recv().await {
            Some(DiscoveryEvent::Found(peer)) if peer.device_id == device_id => return peer,
            Some(_) => continue,
            None => panic!("discovery channel closed before finding {device_id}"),
        }
    }
}

async fn wait_for_lost(rx: &mut mpsc::Receiver<DiscoveryEvent>, expected: &str) {
    loop {
        match rx.recv().await {
            Some(DiscoveryEvent::Lost { device_id }) if device_id == expected => return,
            Some(_) => continue,
            None => panic!("discovery channel closed before losing {expected}"),
        }
    }
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
        .expect("instance a did not discover instance b in time");
    let found_a = timeout(deadline, wait_for_found(&mut rx_b, "itest-aaaa-1111"))
        .await
        .expect("instance b did not discover instance a in time");

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
    timeout(deadline, wait_for_lost(&mut rx_b, "itest-aaaa-1111"))
        .await
        .expect("instance b did not observe instance a's goodbye in time");
    disco_b.shutdown();
}
