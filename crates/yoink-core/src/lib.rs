//! Core data model for yoink: the CRDT-backed shared clipboard document.
//!
//! Everything network- and platform-specific lives in sibling crates; this
//! crate only knows about clipboard entries and the yrs document that holds
//! them.

mod doc;
mod entry;
mod scope;

pub use doc::{ClipDoc, DocError, DocUpdate};
pub use entry::{ClipEntry, now_ms};
pub use scope::{DocSet, InvalidScope, MAX_ROOM_NAME_LEN, Scope, sanitize_room_name};

use serde::{Deserialize, Serialize};

/// Version of the peer-to-peer sync protocol. Peers with a different version
/// refuse to sync rather than risk misinterpreting frames.
///
/// v2: HELLO carries a `scope` (devices vs. room) — a v1 peer would silently
/// treat a room connection as the personal clipboard, so v1/v2 must refuse
/// each other.
pub const PROTOCOL_VERSION: u32 = 2;

/// Maximum number of clipboard entries kept in the shared history. Older
/// entries are pruned from the front of the CRDT array.
pub const MAX_HISTORY: u32 = 200;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// Stable unique id (UUID) persisted in the device's config.
    pub id: String,
    /// Human-readable name shown to peers (defaults to the hostname).
    pub name: String,
}

/// Mutations the web UI can request. They are executed by the app event loop,
/// which is the single owner of doc writes, clipboard writes and config
/// persistence — HTTP handlers only ever enqueue these.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum AppCommand {
    SetAllowed {
        device_id: String,
        allowed: bool,
    },
    SetAutoApply {
        enabled: bool,
    },
    AddEntry {
        text: String,
        #[serde(default = "Scope::default_devices")]
        scope: Scope,
    },
    CopyEntry {
        id: String,
        #[serde(default = "Scope::default_devices")]
        scope: Scope,
    },
    /// Joining is idempotent and doubles as creation — visiting a room URL
    /// is what brings the room into existence.
    JoinRoom {
        name: String,
    },
    LeaveRoom {
        name: String,
    },
}

impl Scope {
    fn default_devices() -> Self {
        Scope::Devices
    }
}
