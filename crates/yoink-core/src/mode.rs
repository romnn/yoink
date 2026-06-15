//! The share mode: how aggressively yoink moves clipboard text between
//! devices. Chosen once at startup with the `--mode` flag.

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

/// How aggressively yoink moves clipboard text between devices. The wire and
/// CLI spelling is kebab-case (`manual`, `auto-share`, `mirror`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Display, EnumString,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum ShareMode {
    /// Share only what you paste into yoink and hit Share. Nothing you copy is
    /// captured, and received entries are never written to the OS clipboard
    /// automatically — you click Copy. The default.
    #[default]
    Manual,
    /// Automatically share whatever you copy to the OS clipboard; received
    /// entries wait in the shared list and are never auto-applied.
    AutoShare,
    /// Two-way clipboard mirror: copies are shared automatically and received
    /// entries are written to the OS clipboard automatically.
    Mirror,
}

impl ShareMode {
    /// Whether this mode captures OS-clipboard copies into the shared
    /// document (`auto-share` and `mirror`).
    #[must_use]
    pub fn captures_clipboard(self) -> bool {
        matches!(self, ShareMode::AutoShare | ShareMode::Mirror)
    }

    /// Whether this mode writes received personal-clipboard entries to the OS
    /// clipboard automatically (`mirror` only).
    #[must_use]
    pub fn auto_applies(self) -> bool {
        matches!(self, ShareMode::Mirror)
    }
}
