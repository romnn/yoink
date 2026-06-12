use crate::ClipDoc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// Longest accepted room name. Room names also ride in mDNS TXT records,
/// which are size-budgeted, so keep names short.
pub const MAX_ROOM_NAME_LEN: usize = 48;

/// Which shared space an entry, connection or command belongs to.
///
/// `Devices` is the personal clipboard: allowlist-gated, fed by the OS
/// clipboard, auto-applied. A `Room` is an open named space anyone on the
/// LAN can join; nothing enters a room except by deliberate user action.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    /// The personal clipboard, shared only between this user's allowlisted
    /// devices.
    Devices,
    /// A named LAN-wide room anyone can join; carries the room name.
    Room(String),
}

impl Scope {
    /// Construct a [`Scope::Room`] from any string-like name. The caller is
    /// responsible for sanitizing untrusted names first (see
    /// [`sanitize_room_name`]).
    #[must_use]
    pub fn room(name: impl Into<String>) -> Self {
        Scope::Room(name.into())
    }

    /// Whether this is the personal-clipboard scope.
    #[must_use]
    pub fn is_devices(&self) -> bool {
        matches!(self, Scope::Devices)
    }

    /// The room name, or `None` for [`Scope::Devices`].
    #[must_use]
    pub fn room_name(&self) -> Option<&str> {
        match self {
            Scope::Devices => None,
            Scope::Room(name) => Some(name),
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scope::Devices => f.write_str("devices"),
            Scope::Room(name) => write!(f, "room:{name}"),
        }
    }
}

/// A string failed to parse as a [`Scope`]; carries the offending input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid scope: {0:?}")]
pub struct InvalidScope(pub String);

impl FromStr for Scope {
    type Err = InvalidScope;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "devices" {
            return Ok(Scope::Devices);
        }
        if let Some(name) = s.strip_prefix("room:") {
            // Round-trip strictness: only already-sanitized names parse, so a
            // peer cannot smuggle weird strings through the wire encoding.
            if sanitize_room_name(name).as_deref() == Some(name) {
                return Ok(Scope::Room(name.to_string()));
            }
        }
        Err(InvalidScope(s.to_string()))
    }
}

impl Serialize for Scope {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Reduce arbitrary user input ("My Room!", "/myroom") to a canonical room
/// name: lowercase ASCII alphanumerics and single hyphens, trimmed, at most
/// [`MAX_ROOM_NAME_LEN`] chars. Returns `None` when nothing usable remains.
#[must_use]
pub fn sanitize_room_name(input: &str) -> Option<String> {
    let mut name = String::new();
    let mut last_hyphen = true;
    for c in input.trim().chars() {
        if c.is_ascii_alphanumeric() {
            name.push(c.to_ascii_lowercase());
            last_hyphen = false;
        } else if !last_hyphen {
            name.push('-');
            last_hyphen = true;
        }
        if name.len() >= MAX_ROOM_NAME_LEN {
            break;
        }
    }
    let name = name.trim_matches('-').to_string();
    if name.is_empty() { None } else { Some(name) }
}

/// All documents this instance holds, one per scope. The `Devices` doc
/// always exists; room docs are created when a room is joined and removed
/// when it is left (their snapshots may outlive them on disk).
pub struct DocSet {
    /// The personal-clipboard doc, kept in its own field rather than the
    /// `rooms` map so it is unconditionally present and `remove` can never
    /// drop it. This removes the only would-be panic path from [`devices`].
    ///
    /// [`devices`]: DocSet::devices
    devices: Arc<ClipDoc>,
    rooms: Mutex<HashMap<Scope, Arc<ClipDoc>>>,
}

impl DocSet {
    /// Create a fresh doc set with an empty personal clipboard and no rooms.
    #[must_use]
    pub fn new() -> Self {
        Self {
            devices: Arc::new(ClipDoc::new()),
            rooms: Mutex::new(HashMap::new()),
        }
    }

    /// The personal clipboard doc (always present).
    #[must_use]
    pub fn devices(&self) -> Arc<ClipDoc> {
        Arc::clone(&self.devices)
    }

    /// The doc for `scope`, or `None` if no room doc by that name exists. The
    /// `Devices` scope always resolves.
    #[must_use]
    pub fn get(&self, scope: &Scope) -> Option<Arc<ClipDoc>> {
        if scope.is_devices() {
            return Some(self.devices());
        }
        self.rooms.lock().get(scope).cloned()
    }

    /// The doc for `scope`, creating an empty room doc if absent. For the
    /// `Devices` scope this is equivalent to [`DocSet::devices`].
    #[must_use]
    pub fn get_or_create(&self, scope: &Scope) -> Arc<ClipDoc> {
        if scope.is_devices() {
            return self.devices();
        }
        self.rooms
            .lock()
            .entry(scope.clone())
            .or_insert_with(|| Arc::new(ClipDoc::new()))
            .clone()
    }

    /// Drop a room doc (leaving a room). The `Devices` doc is never removed.
    pub fn remove(&self, scope: &Scope) {
        if !scope.is_devices() {
            self.rooms.lock().remove(scope);
        }
    }

    /// Every scope currently held, sorted, always including `Devices`.
    #[must_use]
    pub fn scopes(&self) -> Vec<Scope> {
        let mut scopes: Vec<Scope> = self.rooms.lock().keys().cloned().collect();
        scopes.push(Scope::Devices);
        scopes.sort();
        scopes
    }
}

impl Default for DocSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_string_roundtrip() {
        for scope in [Scope::Devices, Scope::room("my-room-2")] {
            assert_eq!(scope.to_string().parse::<Scope>().unwrap(), scope);
        }
    }

    #[test]
    fn scope_rejects_unsanitized_room_names() {
        assert!("room:My Room".parse::<Scope>().is_err());
        assert!("room:".parse::<Scope>().is_err());
        assert!("room:-x-".parse::<Scope>().is_err());
        assert!("attic".parse::<Scope>().is_err());
    }

    #[test]
    fn sanitize_room_names() {
        assert_eq!(sanitize_room_name("My Room!"), Some("my-room".into()));
        assert_eq!(sanitize_room_name("/standup"), Some("standup".into()));
        assert_eq!(sanitize_room_name("a--b"), Some("a-b".into()));
        assert_eq!(sanitize_room_name("!!!"), None);
        assert_eq!(sanitize_room_name(""), None);
        let long = "x".repeat(100);
        assert_eq!(sanitize_room_name(&long).unwrap().len(), MAX_ROOM_NAME_LEN);
    }

    #[test]
    fn docset_lifecycle() {
        let docs = DocSet::new();
        assert_eq!(docs.scopes(), vec![Scope::Devices]);

        let room = Scope::room("attic");
        let doc = docs.get_or_create(&room);
        assert!(Arc::ptr_eq(&doc, &docs.get_or_create(&room)));
        assert_eq!(docs.scopes().len(), 2);

        docs.remove(&room);
        assert!(docs.get(&room).is_none());
        docs.remove(&Scope::Devices);
        assert!(docs.get(&Scope::Devices).is_some());
    }
}
