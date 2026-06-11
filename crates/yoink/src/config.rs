//! Persisted per-device configuration (`config.toml` in the config dir).

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use yoink_core::sanitize_room_name;

pub(crate) const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Config {
    /// Stable device identity, created once on first run. Losing it would
    /// make every peer treat this machine as a brand-new device.
    pub device_id: String,
    /// Optional display-name override; when absent the hostname is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "default_auto_apply")]
    pub auto_apply: bool,
    /// Device ids we allow syncing with, persisted across restarts.
    #[serde(default)]
    pub allowed: Vec<String>,
    /// Sanitized names of the rooms this device has joined, kept sorted.
    /// Rejoined on startup; each room's doc is restored from
    /// `rooms/{name}.bin`. The serde default keeps configs from before the
    /// rooms feature loading cleanly.
    #[serde(default)]
    pub rooms: Vec<String>,
}

fn default_auto_apply() -> bool {
    true
}

impl Config {
    /// Load the config from `dir`, creating the directory and a fresh config
    /// (with a new random device id) on first run.
    ///
    /// A config file that exists but fails to parse is a hard error rather
    /// than a silent reset: wiping it would discard the user's device
    /// identity and allowlist.
    pub fn load_or_init(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("failed to create config directory {}", dir.display()))?;
        let path = dir.join(CONFIG_FILE);
        match std::fs::read_to_string(&path) {
            Ok(raw) => toml::from_str(&raw).with_context(|| {
                format!(
                    "config file {} is corrupt; fix it or remove it \
                     (removing resets the device id and allowlist)",
                    path.display()
                )
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let config = Self {
                    device_id: uuid::Uuid::new_v4().to_string(),
                    name: None,
                    auto_apply: true,
                    allowed: Vec::new(),
                    rooms: Vec::new(),
                };
                config.save(dir)?;
                Ok(config)
            }
            Err(err) => {
                Err(err).with_context(|| format!("failed to read config file {}", path.display()))
            }
        }
    }

    pub fn save(&self, dir: &Path) -> anyhow::Result<()> {
        let raw = toml::to_string_pretty(self).context("failed to serialize config")?;
        write_atomic(&dir.join(CONFIG_FILE), raw.as_bytes())
    }
}

/// Canonicalize a persisted room list: sanitize every name, drop the
/// unusable ones with a warning (we never persist such names, so they can
/// only have been hand-edited in), then sort and dedupe so the config file
/// and the mDNS announcement stay deterministic.
pub(crate) fn sanitize_rooms(rooms: &[String]) -> Vec<String> {
    let mut sanitized: Vec<String> = rooms
        .iter()
        .filter_map(|raw| {
            let name = sanitize_room_name(raw);
            if name.is_none() {
                tracing::warn!(room = %raw, "dropping unusable room name from config");
            }
            name
        })
        .collect();
    sanitized.sort();
    sanitized.dedup();
    sanitized
}

/// Write via a sibling `.tmp` file and rename, so a crash mid-write can never
/// leave a truncated file behind.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = tmp_path(path);
    std::fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(".tmp");
    PathBuf::from(os)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("yoink-config-test-{}", uuid::Uuid::new_v4())))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn first_run_creates_and_persists_device_id() {
        let dir = TempDir::new();
        let created = Config::load_or_init(dir.path()).unwrap();
        assert!(!created.device_id.is_empty());
        assert!(created.name.is_none());
        assert!(created.auto_apply);
        assert!(created.allowed.is_empty());
        assert!(created.rooms.is_empty());

        let reloaded = Config::load_or_init(dir.path()).unwrap();
        assert_eq!(created, reloaded);
    }

    #[test]
    fn round_trip_preserves_all_fields() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.path()).unwrap();
        let config = Config {
            device_id: "id-1".into(),
            name: Some("desk".into()),
            auto_apply: false,
            allowed: vec!["peer-a".into(), "peer-b".into()],
            rooms: vec!["attic".into(), "standup".into()],
        };
        config.save(dir.path()).unwrap();

        let reloaded = Config::load_or_init(dir.path()).unwrap();
        assert_eq!(config, reloaded);
        assert!(!dir.path().join(format!("{CONFIG_FILE}.tmp")).exists());
    }

    #[test]
    fn corrupt_config_is_a_hard_error_and_left_untouched() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.path()).unwrap();
        let path = dir.path().join(CONFIG_FILE);
        std::fs::write(&path, "not = [valid").unwrap();

        let err = Config::load_or_init(dir.path()).unwrap_err();
        assert!(err.to_string().contains("corrupt"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "not = [valid");
    }

    #[test]
    fn missing_optional_fields_use_defaults() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.path()).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), "device_id = \"id-2\"\n").unwrap();

        let config = Config::load_or_init(dir.path()).unwrap();
        assert_eq!(config.device_id, "id-2");
        assert!(config.name.is_none());
        assert!(config.auto_apply);
        assert!(config.allowed.is_empty());
        assert!(
            config.rooms.is_empty(),
            "a config from before the rooms feature loads with no rooms"
        );
    }

    #[test]
    fn sanitize_rooms_drops_dupes_and_junk() {
        let rooms: Vec<String> = ["standup", "My Room", "standup", "!!!", "attic"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            sanitize_rooms(&rooms),
            vec![
                "attic".to_string(),
                "my-room".to_string(),
                "standup".to_string()
            ]
        );
        assert!(sanitize_rooms(&[]).is_empty());
    }
}
