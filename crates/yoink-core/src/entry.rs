use crate::DeviceInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use yrs::Any;

/// A single clipboard item in the shared history. Entries are immutable once
/// created, which is why they are stored as plain `Any` values in the CRDT
/// array instead of nested shared types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClipEntry {
    pub id: String,
    pub device_id: String,
    pub device_name: String,
    pub text: String,
    pub created_at_ms: u64,
}

impl ClipEntry {
    pub fn new(device: &DeviceInfo, text: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: device.id.clone(),
            device_name: device.name.clone(),
            text,
            created_at_ms: now_ms(),
        }
    }

    pub(crate) fn to_any(&self) -> Any {
        let mut map: HashMap<String, Any> = HashMap::new();
        map.insert("id".into(), Any::String(self.id.as_str().into()));
        map.insert(
            "device_id".into(),
            Any::String(self.device_id.as_str().into()),
        );
        map.insert(
            "device_name".into(),
            Any::String(self.device_name.as_str().into()),
        );
        map.insert("text".into(), Any::String(self.text.as_str().into()));
        map.insert(
            "created_at_ms".into(),
            Any::BigInt(self.created_at_ms as i64),
        );
        Any::Map(Arc::new(map))
    }

    pub(crate) fn from_any(any: &Any) -> Option<Self> {
        let Any::Map(map) = any else { return None };
        let get_str = |key: &str| match map.get(key) {
            Some(Any::String(s)) => Some(s.to_string()),
            _ => None,
        };
        let created_at_ms = match map.get("created_at_ms") {
            Some(Any::BigInt(n)) => *n as u64,
            Some(Any::Number(n)) => *n as u64,
            _ => 0,
        };
        Some(Self {
            id: get_str("id")?,
            device_id: get_str("device_id")?,
            device_name: get_str("device_name").unwrap_or_default(),
            text: get_str("text")?,
            created_at_ms,
        })
    }
}

/// Milliseconds since the unix epoch. Used only for display ordering hints;
/// CRDT ordering is what actually governs the history.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
