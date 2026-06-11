//! Binary frame codec for the sync wire protocol (see the crate docs for the
//! normative tag table).

use serde::{Deserialize, Serialize};
use thiserror::Error;
use yoink_core::Scope;

const TAG_HELLO: u8 = 0x01;
const TAG_SYNC_STEP_1: u8 = 0x02;
const TAG_SYNC_STEP_2: u8 = 0x03;
const TAG_UPDATE: u8 = 0x04;

/// Handshake payload exchanged as the first frame on every connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Hello {
    pub device_id: String,
    pub device_name: String,
    pub proto: u32,
    /// Which shared space this connection syncs (protocol v2). Scope's
    /// strict string parsing rejects malformed values at decode time. The
    /// field defaults to `Devices` when absent so a v1 HELLO (which predates
    /// it) still decodes and gets refused by the version check with a clear
    /// log line, instead of surfacing as malformed JSON.
    #[serde(default = "scope_devices")]
    pub scope: Scope,
}

fn scope_devices() -> Scope {
    Scope::Devices
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Frame {
    Hello(Hello),
    SyncStep1(Vec<u8>),
    SyncStep2(Vec<u8>),
    Update(Vec<u8>),
}

#[derive(Debug, Error)]
pub(crate) enum FrameError {
    #[error("empty frame")]
    Empty,
    #[error("unknown frame tag {0:#04x}")]
    UnknownTag(u8),
    #[error("malformed HELLO payload: {0}")]
    BadHello(#[from] serde_json::Error),
}

impl Frame {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Frame::Hello(_) => "HELLO",
            Frame::SyncStep1(_) => "SYNC_STEP_1",
            Frame::SyncStep2(_) => "SYNC_STEP_2",
            Frame::Update(_) => "UPDATE",
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Frame::Hello(hello) => {
                // A struct of two strings and an integer cannot fail to
                // serialize, so the fallback is unreachable in practice.
                let json = serde_json::to_vec(hello).unwrap_or_default();
                tagged(TAG_HELLO, &json)
            }
            Frame::SyncStep1(payload) => tagged(TAG_SYNC_STEP_1, payload),
            Frame::SyncStep2(payload) => tagged(TAG_SYNC_STEP_2, payload),
            Frame::Update(payload) => tagged(TAG_UPDATE, payload),
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        let (&tag, payload) = bytes.split_first().ok_or(FrameError::Empty)?;
        match tag {
            TAG_HELLO => Ok(Frame::Hello(serde_json::from_slice(payload)?)),
            TAG_SYNC_STEP_1 => Ok(Frame::SyncStep1(payload.to_vec())),
            TAG_SYNC_STEP_2 => Ok(Frame::SyncStep2(payload.to_vec())),
            TAG_UPDATE => Ok(Frame::Update(payload.to_vec())),
            other => Err(FrameError::UnknownTag(other)),
        }
    }
}

fn tagged(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + payload.len());
    buf.push(tag);
    buf.extend_from_slice(payload);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_roundtrip() {
        for scope in [Scope::Devices, Scope::room("attic")] {
            let frame = Frame::Hello(Hello {
                device_id: "dev-1".into(),
                device_name: "laptop".into(),
                proto: 2,
                scope,
            });
            let bytes = frame.encode();
            assert_eq!(bytes[0], TAG_HELLO);
            assert_eq!(Frame::decode(&bytes).unwrap(), frame);
        }
    }

    #[test]
    fn hello_without_scope_defaults_to_devices() {
        // A v1 HELLO never carried a scope; it must decode (as Devices) so
        // the version check can refuse it explicitly.
        let mut bytes = vec![TAG_HELLO];
        bytes.extend_from_slice(br#"{"device_id":"x","device_name":"y","proto":1}"#);
        match Frame::decode(&bytes).unwrap() {
            Frame::Hello(hello) => {
                assert_eq!(hello.scope, Scope::Devices);
                assert_eq!(hello.proto, 1);
            }
            other => panic!("expected HELLO, got {}", other.name()),
        }
    }

    #[test]
    fn hello_with_invalid_scope_is_rejected() {
        for scope in ["room:Bad Name", "room:", "attic", ""] {
            let mut bytes = vec![TAG_HELLO];
            let json = format!(
                r#"{{"device_id":"x","device_name":"y","proto":2,"scope":{}}}"#,
                serde_json::to_string(scope).unwrap()
            );
            bytes.extend_from_slice(json.as_bytes());
            assert!(
                matches!(Frame::decode(&bytes), Err(FrameError::BadHello(_))),
                "scope {scope:?} must not decode"
            );
        }
    }

    #[test]
    fn payload_frames_roundtrip() {
        for frame in [
            Frame::SyncStep1(vec![1, 2, 3]),
            Frame::SyncStep2(vec![]),
            Frame::Update(vec![0xff; 64]),
        ] {
            assert_eq!(Frame::decode(&frame.encode()).unwrap(), frame);
        }
    }

    #[test]
    fn empty_frame_is_rejected() {
        assert!(matches!(Frame::decode(&[]), Err(FrameError::Empty)));
    }

    #[test]
    fn unknown_tag_is_rejected() {
        assert!(matches!(
            Frame::decode(&[0x7f, 1, 2]),
            Err(FrameError::UnknownTag(0x7f))
        ));
    }

    #[test]
    fn malformed_hello_json_is_rejected() {
        let mut bytes = vec![TAG_HELLO];
        bytes.extend_from_slice(b"not json");
        assert!(matches!(
            Frame::decode(&bytes),
            Err(FrameError::BadHello(_))
        ));

        // Valid JSON with missing fields must also fail.
        let mut bytes = vec![TAG_HELLO];
        bytes.extend_from_slice(br#"{"device_id":"x"}"#);
        assert!(matches!(
            Frame::decode(&bytes),
            Err(FrameError::BadHello(_))
        ));
    }
}
