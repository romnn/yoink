//! Fixture server for eyeballing the UI: the real router and a [`ServerCtx`]
//! seeded with fake peers, rooms and history, no discovery or clipboard
//! needed. Run with `cargo run -p yoink-server --example ui_fixture` and open
//! <http://127.0.0.1:7691/> (or `/r/attic` for the room view).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use yoink_core::{DeviceInfo, DocSet, Scope};
use yoink_discovery::PeerInfo;
use yoink_server::{PeerView, ServerCtx, Settings, serve};
use yoink_sync::SyncManager;

fn peer(id: &str, name: &str, online: bool, rooms: &[&str]) -> (String, PeerView) {
    (
        id.to_string(),
        PeerView {
            info: PeerInfo {
                device_id: id.into(),
                name: name.into(),
                addrs: vec![],
                port: 7679,
                rooms: rooms.iter().map(ToString::to_string).collect(),
            },
            online,
        },
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let device = DeviceInfo {
        id: "dev-self".into(),
        name: "framework-13".into(),
    };
    let docs = Arc::new(DocSet::new());

    let other = DeviceInfo {
        id: "dev-mac".into(),
        name: "mac-studio".into(),
    };
    let devices_doc = docs.devices();
    devices_doc.add_entry(&other, "https://crates.io/crates/yrs".into());
    devices_doc.add_entry(
        &device,
        "ssh -L 5432:localhost:5432 roman@10.0.0.17  # tunnel for the staging db".into(),
    );
    devices_doc.add_entry(
        &other,
        "The five boxing wizards jump quickly, then paste a rather long paragraph to see how \
         the four-line clamp behaves. Lorem ipsum dolor sit amet, consectetur adipiscing elit, \
         sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim \
         veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo \
         consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum."
            .into(),
    );
    devices_doc.add_entry(&device, "8f4c-2210-aa91".into());

    let attic = docs.get_or_create(&Scope::room("attic"));
    attic.add_entry(&other, "standup notes -> https://pad.local/standup".into());
    attic.add_entry(
        &device,
        "docker compose -f deploy/compose.yaml up -d".into(),
    );

    let allowed: HashSet<String> = ["dev-mac".to_string(), "dev-ghost".to_string()].into();
    let joined: BTreeSet<String> = ["attic".to_string()].into();
    let joined_rooms: HashSet<String> = joined.iter().cloned().collect();
    let (sync, _events) = SyncManager::new(docs.clone(), device.clone(), allowed, &joined_rooms);

    let peers: HashMap<String, PeerView> = [
        peer("dev-mac", "mac-studio", true, &["attic", "standup"]),
        peer("dev-pi", "kitchen-pi", true, &["attic"]),
        peer("dev-old", "old-thinkpad", false, &[]),
    ]
    .into();

    let (commands, mut commands_rx) = mpsc::channel(64);
    let (notify, _) = broadcast::channel(16);
    // Drain commands so the UI's POSTs succeed; the fixture has no app loop.
    tokio::spawn(async move { while commands_rx.recv().await.is_some() {} });

    let ctx = ServerCtx {
        device,
        docs,
        sync,
        peers: Arc::new(parking_lot::RwLock::new(peers)),
        settings: Arc::new(parking_lot::RwLock::new(Settings {
            auto_apply: true,
            clipboard_available: true,
        })),
        joined_rooms: Arc::new(parking_lot::RwLock::new(joined)),
        commands,
        notify,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:7691").await?;
    eprintln!("ui fixture on http://127.0.0.1:7691/ and /r/attic");
    serve(listener, ctx).await
}
