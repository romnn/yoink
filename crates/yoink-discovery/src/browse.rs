//! Pure translation of mDNS browse events into [`DiscoveryEvent`]s.
//!
//! Kept free of any daemon/network state so the dedup, self-filtering and
//! fullname bookkeeping can be unit-tested without touching mDNS.

use std::collections::HashMap;
use std::net::IpAddr;

use crate::{DiscoveryEvent, PeerInfo};

/// A resolved mDNS announcement reduced to the fields yoink cares about.
///
/// Decoupled from `mdns_sd::ResolvedService` (which is `#[non_exhaustive]`
/// and cannot be constructed in tests).
#[derive(Debug, Clone)]
pub(crate) struct Announcement {
    pub fullname: String,
    /// TXT `id` property; `None` means the announcement is not a yoink peer.
    pub txt_id: Option<String>,
    /// TXT `name` property; falls back to the device id when missing.
    pub txt_name: Option<String>,
    /// TXT `rooms` property: comma-joined room names (absent on v1 peers).
    pub txt_rooms: Option<String>,
    pub addrs: Vec<IpAddr>,
    pub port: u16,
}

/// Tracks what we have told the consumer about each peer so removals can be
/// translated to `Lost` and identical consecutive `Found`s are suppressed.
pub(crate) struct BrowseState {
    self_id: String,
    /// mDNS fullname -> device id, needed because `ServiceRemoved` only
    /// carries the fullname.
    by_fullname: HashMap<String, String>,
    /// device id -> last emitted peer info, for deduplication.
    last_emitted: HashMap<String, PeerInfo>,
}

impl BrowseState {
    pub(crate) fn new(self_id: impl Into<String>) -> Self {
        Self {
            self_id: self_id.into(),
            by_fullname: HashMap::new(),
            last_emitted: HashMap::new(),
        }
    }

    /// Handles a resolved announcement. Returns `Found` unless it is our own
    /// announcement, not a yoink peer, or identical to what was last emitted.
    pub(crate) fn on_resolved(&mut self, mut ann: Announcement) -> Option<DiscoveryEvent> {
        let Some(device_id) = ann.txt_id else {
            tracing::debug!(fullname = %ann.fullname, "ignoring announcement without TXT id");
            return None;
        };
        if device_id == self.self_id {
            return None;
        }

        // Normalize addresses so a re-resolve with the same set (in a
        // different order, e.g. from a HashSet) compares equal.
        ann.addrs.sort_unstable();
        ann.addrs.dedup();

        // Empty segments (e.g. an empty TXT value) are dropped; names are
        // normalized like addresses so re-resolves compare equal.
        let mut rooms: Vec<String> = ann
            .txt_rooms
            .as_deref()
            .unwrap_or_default()
            .split(',')
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect();
        rooms.sort_unstable();
        rooms.dedup();

        let peer = PeerInfo {
            name: ann.txt_name.unwrap_or_else(|| device_id.clone()),
            device_id: device_id.clone(),
            addrs: ann.addrs,
            port: ann.port,
            rooms,
        };

        self.by_fullname.insert(ann.fullname, device_id.clone());
        if self.last_emitted.get(&device_id) == Some(&peer) {
            return None;
        }
        self.last_emitted.insert(device_id, peer.clone());
        Some(DiscoveryEvent::Found(peer))
    }

    /// Handles a removed announcement. Returns `Lost` only when no other
    /// fullname still announces the same device (a peer can briefly exist
    /// under two fullnames after an instance rename).
    pub(crate) fn on_removed(&mut self, fullname: &str) -> Option<DiscoveryEvent> {
        let device_id = self.by_fullname.remove(fullname)?;
        if self.by_fullname.values().any(|id| *id == device_id) {
            return None;
        }
        self.last_emitted.remove(&device_id);
        Some(DiscoveryEvent::Lost { device_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    const SELF_ID: &str = "self-device-id";

    fn announcement(fullname: &str, id: &str, addrs: &[IpAddr], port: u16) -> Announcement {
        Announcement {
            fullname: fullname.to_string(),
            txt_id: Some(id.to_string()),
            txt_name: Some(format!("{id}-name")),
            txt_rooms: None,
            addrs: addrs.to_vec(),
            port,
        }
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, last))
    }

    #[test]
    fn resolved_peer_emits_found() {
        let mut state = BrowseState::new(SELF_ID);
        let event = state.on_resolved(announcement(
            "peer._yoink._tcp.local.",
            "peer-1",
            &[ip(2)],
            9001,
        ));
        match event {
            Some(DiscoveryEvent::Found(peer)) => {
                assert_eq!(peer.device_id, "peer-1");
                assert_eq!(peer.name, "peer-1-name");
                assert_eq!(peer.addrs, vec![ip(2)]);
                assert_eq!(peer.port, 9001);
            }
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn own_announcement_is_filtered() {
        let mut state = BrowseState::new(SELF_ID);
        let ann = announcement("me._yoink._tcp.local.", SELF_ID, &[ip(2)], 9001);
        assert!(state.on_resolved(ann).is_none());
        // Our own fullname was never tracked, so its removal is silent too.
        assert!(state.on_removed("me._yoink._tcp.local.").is_none());
    }

    #[test]
    fn announcement_without_id_is_ignored() {
        let mut state = BrowseState::new(SELF_ID);
        let ann = Announcement {
            txt_id: None,
            ..announcement("x._yoink._tcp.local.", "unused", &[ip(2)], 9001)
        };
        assert!(state.on_resolved(ann).is_none());
    }

    #[test]
    fn missing_name_falls_back_to_device_id() {
        let mut state = BrowseState::new(SELF_ID);
        let ann = Announcement {
            txt_name: None,
            ..announcement("p._yoink._tcp.local.", "peer-1", &[ip(2)], 9001)
        };
        match state.on_resolved(ann) {
            Some(DiscoveryEvent::Found(peer)) => assert_eq!(peer.name, "peer-1"),
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn identical_re_resolve_is_suppressed() {
        let mut state = BrowseState::new(SELF_ID);
        let first = announcement("p._yoink._tcp.local.", "peer-1", &[ip(2), ip(3)], 9001);
        assert!(state.on_resolved(first).is_some());
        // Same data with addresses in a different order must not re-emit.
        let again = announcement("p._yoink._tcp.local.", "peer-1", &[ip(3), ip(2)], 9001);
        assert!(state.on_resolved(again).is_none());
    }

    #[test]
    fn changed_addresses_emit_found_again() {
        let mut state = BrowseState::new(SELF_ID);
        assert!(
            state
                .on_resolved(announcement(
                    "p._yoink._tcp.local.",
                    "peer-1",
                    &[ip(2)],
                    9001
                ))
                .is_some()
        );
        let event = state.on_resolved(announcement(
            "p._yoink._tcp.local.",
            "peer-1",
            &[ip(7)],
            9001,
        ));
        match event {
            Some(DiscoveryEvent::Found(peer)) => assert_eq!(peer.addrs, vec![ip(7)]),
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[test]
    fn removal_translates_to_lost_via_fullname() {
        let mut state = BrowseState::new(SELF_ID);
        let ann = announcement("p._yoink._tcp.local.", "peer-1", &[ip(2)], 9001);
        assert!(state.on_resolved(ann).is_some());
        match state.on_removed("p._yoink._tcp.local.") {
            Some(DiscoveryEvent::Lost { device_id }) => assert_eq!(device_id, "peer-1"),
            other => panic!("expected Lost, got {other:?}"),
        }
        // Unknown / already removed fullname stays silent.
        assert!(state.on_removed("p._yoink._tcp.local.").is_none());
    }

    #[test]
    fn lost_then_found_again_re_emits() {
        let mut state = BrowseState::new(SELF_ID);
        let ann = announcement("p._yoink._tcp.local.", "peer-1", &[ip(2)], 9001);
        assert!(state.on_resolved(ann.clone()).is_some());
        assert!(state.on_removed("p._yoink._tcp.local.").is_some());
        // After a Lost, the identical announcement is news again.
        assert!(state.on_resolved(ann).is_some());
    }

    #[test]
    fn removal_is_silent_while_another_fullname_announces_same_device() {
        let mut state = BrowseState::new(SELF_ID);
        assert!(
            state
                .on_resolved(announcement(
                    "old._yoink._tcp.local.",
                    "peer-1",
                    &[ip(2)],
                    9001
                ))
                .is_some()
        );
        // Renamed instance: same device id under a new fullname.
        let renamed = state.on_resolved(announcement(
            "new._yoink._tcp.local.",
            "peer-1",
            &[ip(2)],
            9001,
        ));
        assert!(
            renamed.is_none(),
            "identical data under new fullname must not re-emit"
        );

        assert!(state.on_removed("old._yoink._tcp.local.").is_none());
        match state.on_removed("new._yoink._tcp.local.") {
            Some(DiscoveryEvent::Lost { device_id }) => assert_eq!(device_id, "peer-1"),
            other => panic!("expected Lost, got {other:?}"),
        }
    }
}
