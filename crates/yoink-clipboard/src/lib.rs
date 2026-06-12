//! Local OS clipboard integration: watch for copies, write remote entries.
//!
//! A dedicated OS thread owns the `arboard::Clipboard` (it is not usable
//! across threads on every platform) and polls for text changes. Writes are
//! funneled to the same thread through a channel.

mod state;
mod worker;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Bounded buffer between the clipboard thread and the async consumer. When
/// it fills up (consumer stalled), the worker drops events instead of
/// blocking the clipboard thread.
const EVENT_CHANNEL_CAPACITY: usize = 32;

/// Something the worker observed happening to the local OS clipboard, emitted
/// on the receiver returned by [`ClipboardHandle::spawn`]. Text the handle
/// wrote itself via [`ClipboardHandle::set_text`] is suppressed and never
/// surfaces here, so every event reflects a genuine local user action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClipboardEvent {
    /// The user copied new text into the OS clipboard.
    Copied(String),
}

/// Handle to the clipboard worker thread.
///
/// Implementation notes:
/// - Poll `get_text()` every `poll_interval`; emit `Copied` only when the
///   text differs from the last observed value.
/// - Suppression: `set_text` records what it wrote; the watcher must NOT
///   emit a `Copied` event for text the handle itself just wrote, otherwise
///   applying a remote entry would loop it straight back into the doc.
/// - Headless degradation: if `arboard::Clipboard::new()` fails (no display
///   server), log a warning once and run disabled — `available()` returns
///   false, the receiver never yields, `set_text` is a no-op. The rest of
///   the app keeps working (entries still sync, just no OS clipboard I/O).
/// - Availability is live, not frozen at startup: the flag flips to false
///   the moment the worker thread exits (or a write discovers it gone), so
///   consumers polling `available()` stay truthful for the UI.
#[derive(Clone)]
pub struct ClipboardHandle {
    /// Shared with the worker thread, which flips it false on exit so a
    /// clipboard that dies mid-run is reported as unavailable from then on.
    available: Arc<AtomicBool>,
    cmd_tx: std::sync::mpsc::Sender<worker::Command>,
    /// Keeps the event channel open when the worker thread could not even be
    /// spawned, so the receiver never yields in degraded mode. `None` in
    /// normal operation: there the worker holds the only sender and the
    /// channel closes naturally once every handle is dropped.
    _degraded_keepalive: Option<mpsc::Sender<ClipboardEvent>>,
}

impl ClipboardHandle {
    /// Spawn the worker thread and return the handle plus the stream of
    /// locally copied text.
    pub fn spawn(poll_interval: Duration) -> (Self, mpsc::Receiver<ClipboardEvent>) {
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (init_tx, init_rx) = std::sync::mpsc::channel();
        let available = Arc::new(AtomicBool::new(false));

        let worker_event_tx = event_tx.clone();
        let worker_available = available.clone();
        let spawned = std::thread::Builder::new()
            .name("yoink-clipboard".to_owned())
            .spawn(move || {
                worker::run(
                    &cmd_rx,
                    &worker_event_tx,
                    &init_tx,
                    poll_interval,
                    &worker_available,
                );
            });

        let initially_available = match spawned {
            // Blocks only for the duration of `Clipboard::new()` on the
            // worker thread; a recv error means the thread died before
            // reporting, which we treat as unavailable.
            Ok(_) => init_rx.recv().unwrap_or(false),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to spawn clipboard worker thread; clipboard integration disabled"
                );
                false
            }
        };

        let handle = Self {
            available,
            cmd_tx,
            _degraded_keepalive: (!initially_available).then_some(event_tx),
        };
        (handle, event_rx)
    }

    /// Whether an OS clipboard is actually reachable *right now*. Starts out
    /// reflecting the worker's init result and flips to false for good if
    /// the worker thread ever exits.
    #[must_use]
    pub fn available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    /// Write text into the OS clipboard without triggering a `Copied` event.
    pub fn set_text(&self, text: String) {
        if !self.available() {
            return;
        }
        if self.cmd_tx.send(worker::Command::SetText(text)).is_err() {
            // Worker thread gone (it normally outlives every handle, so this
            // means it died); make sure availability reflects that even if
            // the thread could not run its own exit path.
            self.available.store(false, Ordering::Relaxed);
            tracing::warn!("clipboard worker thread is gone; dropping set_text");
        }
    }
}
