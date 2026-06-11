//! Builds the UI state JSON documented in the crate root.

use crate::{PeerView, ServerCtx, Settings};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use yoink_core::{ClipEntry, DeviceInfo, Scope};

/// UI cap; the document itself keeps up to `yoink_core::MAX_HISTORY` entries.
const MAX_UI_ENTRIES: usize = 100;

#[derive(Serialize)]
struct StateJson<'a> {
    device: &'a DeviceInfo,
    scope: String,
    settings: SettingsJson,
    peers: Vec<PeerJson>,
    rooms: RoomsJson,
    members: Vec<MemberJson>,
    entries: Vec<ClipEntry>,
}

/// Mirror of [`Settings`] so the wire shape stays fixed even if the public
/// struct grows fields the UI should not see.
#[derive(Serialize)]
struct SettingsJson {
    auto_apply: bool,
    clipboard_available: bool,
}

#[derive(Serialize)]
struct PeerJson {
    id: String,
    name: String,
    online: bool,
    allowed: bool,
    connected: bool,
}

#[derive(Serialize)]
struct RoomsJson {
    joined: Vec<String>,
    network: Vec<NetworkRoomJson>,
}

#[derive(Serialize)]
struct NetworkRoomJson {
    name: String,
    devices: usize,
}

#[derive(Serialize)]
struct MemberJson {
    id: String,
    name: String,
    connected: bool,
}

pub(crate) fn build_state(ctx: &ServerCtx, scope: &Scope) -> serde_json::Value {
    // Snapshot shared state up front; the guards must never live past this
    // function so no sync lock can be held across an await in callers.
    let registry = ctx.peers.read().clone();
    let settings = ctx.settings.read().clone();
    let joined = ctx.joined_rooms.read().clone();
    // A scope without a doc (room not joined, or just left) still renders:
    // the UI shows an empty history until the join command lands.
    let entries = ctx
        .docs
        .get(scope)
        .map(|doc| doc.entries())
        .unwrap_or_default();
    let scope_connected = ctx.sync.connected(scope);
    // `peers[].connected` always reports devices-scope connectivity, even
    // when a room is being viewed; room connectivity goes in `members`.
    let devices_connected = if scope.is_devices() {
        scope_connected.clone()
    } else {
        ctx.sync.connected(&Scope::Devices)
    };
    StateSnapshot {
        device: &ctx.device,
        settings: &settings,
        scope,
        registry: &registry,
        allowed: &ctx.sync.allowed(),
        devices_connected: &devices_connected,
        scope_connected: &scope_connected,
        joined: &joined,
        entries,
    }
    .build()
}

/// Everything [`build_state`] snapshots out of the shared state, separated
/// from the JSON assembly so tests can exercise the assembly without a live
/// [`SyncManager`](yoink_sync::SyncManager).
struct StateSnapshot<'a> {
    device: &'a DeviceInfo,
    settings: &'a Settings,
    scope: &'a Scope,
    registry: &'a HashMap<String, PeerView>,
    allowed: &'a HashSet<String>,
    /// Device ids with a live `devices`-scope connection.
    devices_connected: &'a HashSet<String>,
    /// Device ids with a live connection in `scope`.
    scope_connected: &'a HashSet<String>,
    joined: &'a BTreeSet<String>,
    /// Oldest-first as produced by `ClipDoc::entries`; the UI gets it
    /// newest-first and capped.
    entries: Vec<ClipEntry>,
}

impl StateSnapshot<'_> {
    fn build(self) -> serde_json::Value {
        let mut peers: Vec<PeerJson> = self
            .registry
            .values()
            .map(|view| PeerJson {
                id: view.info.device_id.clone(),
                name: view.info.name.clone(),
                online: view.online,
                allowed: self.allowed.contains(&view.info.device_id),
                connected: self.devices_connected.contains(&view.info.device_id),
            })
            .collect();
        // Allowed-but-never-seen devices must still show up so the user can
        // revoke them; without a last-seen name the id is all we have.
        for id in self.allowed {
            if !self.registry.contains_key(id) {
                peers.push(PeerJson {
                    id: id.clone(),
                    name: id.clone(),
                    online: false,
                    allowed: true,
                    connected: self.devices_connected.contains(id),
                });
            }
        }
        // HashMap iteration order is random; sort so the device list does not
        // jump around between re-renders.
        peers.sort_by(|a, b| (a.name.to_lowercase(), &a.id).cmp(&(b.name.to_lowercase(), &b.id)));

        // Our own joined rooms are always listed (advertiser count 0 is fine)
        // so a room only we hold still shows up as joinable context. BTreeMap
        // gives the sorted-by-name order for free.
        let mut network: BTreeMap<&str, usize> =
            self.joined.iter().map(|name| (name.as_str(), 0)).collect();
        for view in self.registry.values().filter(|view| view.online) {
            for room in &view.info.rooms {
                *network.entry(room.as_str()).or_insert(0) += 1;
            }
        }
        let network: Vec<NetworkRoomJson> = network
            .into_iter()
            .map(|(name, devices)| NetworkRoomJson {
                name: name.to_string(),
                devices,
            })
            .collect();

        let mut members: Vec<MemberJson> = match self.scope.room_name() {
            None => Vec::new(),
            Some(room) => self
                .registry
                .values()
                .filter(|view| view.online && view.info.rooms.iter().any(|r| r == room))
                .map(|view| MemberJson {
                    id: view.info.device_id.clone(),
                    name: view.info.name.clone(),
                    connected: self.scope_connected.contains(&view.info.device_id),
                })
                .collect(),
        };
        members.sort_by(|a, b| (a.name.to_lowercase(), &a.id).cmp(&(b.name.to_lowercase(), &b.id)));

        let entries: Vec<ClipEntry> = self
            .entries
            .into_iter()
            .rev()
            .take(MAX_UI_ENTRIES)
            .collect();

        let state = StateJson {
            device: self.device,
            scope: self.scope.to_string(),
            settings: SettingsJson {
                auto_apply: self.settings.auto_apply,
                clipboard_available: self.settings.clipboard_available,
            },
            peers,
            rooms: RoomsJson {
                joined: self.joined.iter().cloned().collect(),
                network,
            },
            members,
            entries,
        };
        serde_json::to_value(&state).unwrap_or_else(|err| {
            // Plain structs of strings/bools/ints cannot fail to serialize,
            // but degrade to an empty object rather than panicking in a
            // handler.
            tracing::error!(%err, "failed to serialize UI state");
            serde_json::Value::Object(serde_json::Map::new())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yoink_discovery::PeerInfo;

    fn device() -> DeviceInfo {
        DeviceInfo {
            id: "dev-self".into(),
            name: "my-laptop".into(),
        }
    }

    fn settings() -> Settings {
        Settings {
            auto_apply: true,
            clipboard_available: false,
        }
    }

    fn peer_view(id: &str, name: &str, online: bool, rooms: &[&str]) -> PeerView {
        PeerView {
            info: PeerInfo {
                device_id: id.into(),
                name: name.into(),
                addrs: vec![],
                port: 4242,
                rooms: rooms.iter().map(|r| r.to_string()).collect(),
            },
            online,
        }
    }

    fn entry(id: &str, text: &str, created_at_ms: u64) -> ClipEntry {
        ClipEntry {
            id: id.into(),
            device_id: "dev-self".into(),
            device_name: "my-laptop".into(),
            text: text.into(),
            created_at_ms,
        }
    }

    /// Builds with empty defaults except for what the test overrides.
    struct Fixture {
        scope: Scope,
        registry: HashMap<String, PeerView>,
        allowed: HashSet<String>,
        devices_connected: HashSet<String>,
        scope_connected: HashSet<String>,
        joined: BTreeSet<String>,
        entries: Vec<ClipEntry>,
    }

    impl Default for Fixture {
        fn default() -> Self {
            Self {
                scope: Scope::Devices,
                registry: HashMap::new(),
                allowed: HashSet::new(),
                devices_connected: HashSet::new(),
                scope_connected: HashSet::new(),
                joined: BTreeSet::new(),
                entries: Vec::new(),
            }
        }
    }

    impl Fixture {
        fn build(self) -> serde_json::Value {
            let dev = device();
            let settings = settings();
            StateSnapshot {
                device: &dev,
                settings: &settings,
                scope: &self.scope,
                registry: &self.registry,
                allowed: &self.allowed,
                devices_connected: &self.devices_connected,
                scope_connected: &self.scope_connected,
                joined: &self.joined,
                entries: self.entries,
            }
            .build()
        }
    }

    #[test]
    fn state_shape_and_peer_union() {
        let mut registry = HashMap::new();
        registry.insert(
            "p-online".to_string(),
            peer_view("p-online", "Alpha", true, &[]),
        );
        registry.insert(
            "p-offline".to_string(),
            peer_view("p-offline", "Bravo", false, &[]),
        );
        let state = Fixture {
            registry,
            allowed: ["p-online".to_string(), "p-ghost".to_string()].into(),
            devices_connected: ["p-online".to_string()].into(),
            scope_connected: ["p-online".to_string()].into(),
            entries: vec![entry("e1", "older", 1000), entry("e2", "newer", 2000)],
            ..Fixture::default()
        }
        .build();

        assert_eq!(state["device"]["id"], "dev-self");
        assert_eq!(state["device"]["name"], "my-laptop");
        assert_eq!(state["scope"], "devices");
        assert_eq!(state["settings"]["auto_apply"], true);
        assert_eq!(state["settings"]["clipboard_available"], false);
        assert_eq!(state["rooms"]["joined"].as_array().unwrap().len(), 0);
        assert_eq!(state["rooms"]["network"].as_array().unwrap().len(), 0);
        assert_eq!(state["members"].as_array().unwrap().len(), 0);

        let peers = state["peers"].as_array().unwrap();
        assert_eq!(peers.len(), 3);
        let by_id = |id: &str| {
            peers
                .iter()
                .find(|p| p["id"] == id)
                .unwrap_or_else(|| panic!("peer {id} missing"))
        };

        let online = by_id("p-online");
        assert_eq!(online["name"], "Alpha");
        assert_eq!(online["online"], true);
        assert_eq!(online["allowed"], true);
        assert_eq!(online["connected"], true);

        let offline = by_id("p-offline");
        assert_eq!(offline["name"], "Bravo");
        assert_eq!(offline["online"], false);
        assert_eq!(offline["allowed"], false);
        assert_eq!(offline["connected"], false);

        // Allowed but never discovered: name falls back to the id.
        let ghost = by_id("p-ghost");
        assert_eq!(ghost["name"], "p-ghost");
        assert_eq!(ghost["online"], false);
        assert_eq!(ghost["allowed"], true);
        assert_eq!(ghost["connected"], false);

        let entries = state["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["id"], "e2");
        assert_eq!(entries[0]["text"], "newer");
        assert_eq!(entries[0]["device_name"], "my-laptop");
        assert_eq!(entries[0]["created_at_ms"], 2000);
        assert_eq!(entries[1]["id"], "e1");
    }

    #[test]
    fn entries_are_newest_first_and_capped_at_100() {
        let entries: Vec<ClipEntry> = (0..150)
            .map(|i| entry(&format!("e{i}"), &format!("text {i}"), i))
            .collect();
        let state = Fixture {
            entries,
            ..Fixture::default()
        }
        .build();
        let out = state["entries"].as_array().unwrap();
        assert_eq!(out.len(), 100);
        assert_eq!(out[0]["id"], "e149");
        assert_eq!(out[99]["id"], "e50");
        assert_eq!(state["peers"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn peers_are_sorted_by_name_then_id() {
        let mut registry = HashMap::new();
        registry.insert("z".to_string(), peer_view("z", "apple", true, &[]));
        registry.insert("a".to_string(), peer_view("a", "Zebra", true, &[]));
        registry.insert("m".to_string(), peer_view("m", "apple", true, &[]));
        let state = Fixture {
            registry,
            ..Fixture::default()
        }
        .build();
        let names: Vec<(String, String)> = state["peers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    p["name"].as_str().unwrap().to_string(),
                    p["id"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            names,
            vec![
                ("apple".to_string(), "m".to_string()),
                ("apple".to_string(), "z".to_string()),
                ("Zebra".to_string(), "a".to_string()),
            ]
        );
    }

    #[test]
    fn network_rooms_aggregate_online_advertisers_plus_joined() {
        let mut registry = HashMap::new();
        registry.insert(
            "a".to_string(),
            peer_view("a", "Alpha", true, &["attic", "standup"]),
        );
        registry.insert("b".to_string(), peer_view("b", "Bravo", true, &["attic"]));
        // Offline peers must not count toward (or even surface) a room.
        registry.insert(
            "c".to_string(),
            peer_view("c", "Charlie", false, &["attic", "ghost-room"]),
        );
        let state = Fixture {
            registry,
            joined: ["solo".to_string(), "attic".to_string()].into(),
            ..Fixture::default()
        }
        .build();

        assert_eq!(
            state["rooms"]["joined"],
            serde_json::json!(["attic", "solo"])
        );
        assert_eq!(
            state["rooms"]["network"],
            serde_json::json!([
                {"name": "attic", "devices": 2},
                {"name": "solo", "devices": 0},
                {"name": "standup", "devices": 1},
            ])
        );
    }

    #[test]
    fn members_are_online_advertisers_with_room_scope_connectivity() {
        let mut registry = HashMap::new();
        registry.insert(
            "a".to_string(),
            peer_view("a", "Alpha", true, &["attic", "standup"]),
        );
        registry.insert("b".to_string(), peer_view("b", "Bravo", true, &["attic"]));
        registry.insert(
            "c".to_string(),
            peer_view("c", "Charlie", false, &["attic"]),
        );
        registry.insert("d".to_string(), peer_view("d", "Delta", true, &[]));
        let state = Fixture {
            scope: Scope::room("attic"),
            registry,
            // Devices-scope connectivity differs from room connectivity on
            // purpose: `peers[].connected` and `members[].connected` must not
            // leak into each other.
            devices_connected: ["b".to_string()].into(),
            scope_connected: ["a".to_string()].into(),
            joined: ["attic".to_string()].into(),
            ..Fixture::default()
        }
        .build();

        assert_eq!(state["scope"], "room:attic");
        assert_eq!(
            state["members"],
            serde_json::json!([
                {"id": "a", "name": "Alpha", "connected": true},
                {"id": "b", "name": "Bravo", "connected": false},
            ])
        );
        let peers = state["peers"].as_array().unwrap();
        let alpha = peers.iter().find(|p| p["id"] == "a").unwrap();
        let bravo = peers.iter().find(|p| p["id"] == "b").unwrap();
        assert_eq!(alpha["connected"], false);
        assert_eq!(bravo["connected"], true);
    }
}
