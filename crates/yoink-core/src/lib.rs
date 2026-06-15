//! Core data model for yoink: the CRDT-backed shared clipboard document.
//!
//! Everything network- and platform-specific lives in sibling crates; this
//! crate only knows about clipboard entries and the yrs document that holds
//! them.

mod doc;
mod entry;
mod mode;
mod scope;

pub use doc::{ClipDoc, DocError, DocUpdate};
pub use entry::{ClipEntry, now_ms};
pub use mode::ShareMode;
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

/// Identity a peer announces over the wire: who is sending updates and how to
/// present them in another device's history and device-management UI.
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
    /// Change whether a device is trusted for the personal-clipboard
    /// (`Devices`) scope. Under the default trust-the-LAN model `trusted =
    /// false` blocks the device and `true` unblocks it; under
    /// `--require-pairing` the same flag pairs (`true`) or unpairs (`false`)
    /// it. The event loop routes it to the right list based on the active
    /// model.
    SetDeviceTrusted {
        /// Peer whose trust state is being changed.
        device_id: String,
        /// `true` to trust (unblock / pair), `false` to distrust (block /
        /// unpair).
        trusted: bool,
    },
    /// Append text the user typed in the UI as a new entry in `scope`.
    AddEntry {
        /// Clipboard text to store.
        text: String,
        /// Scope the entry belongs to; defaults to the personal clipboard.
        #[serde(default = "Scope::default_devices")]
        scope: Scope,
    },
    /// Copy an existing entry back onto the OS clipboard.
    CopyEntry {
        /// Id of the entry to copy, within `scope`.
        id: String,
        /// Scope the entry lives in; defaults to the personal clipboard.
        #[serde(default = "Scope::default_devices")]
        scope: Scope,
    },
    /// Join (and thereby ensure the existence of) a room.
    ///
    /// Joining is idempotent and doubles as creation — visiting a room URL
    /// is what brings the room into existence.
    JoinRoom {
        /// Room name to join; sanitized by the event loop before use.
        name: String,
    },
    /// Leave a room, dropping its in-memory doc.
    LeaveRoom {
        /// Room name to leave.
        name: String,
    },
}

impl Scope {
    fn default_devices() -> Self {
        Scope::Devices
    }
}
