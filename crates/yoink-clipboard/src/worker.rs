use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;

use crate::ClipboardEvent;
use crate::state::WatchState;

pub(crate) enum Command {
    SetText(String),
}

/// Flips the shared availability flag to false when dropped, so *every* way
/// out of the worker thread (normal exit, early return, panic unwind) leaves
/// `ClipboardHandle::available()` truthful.
struct AvailabilityGuard(Arc<AtomicBool>);

impl Drop for AvailabilityGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Body of the dedicated clipboard thread. The `arboard::Clipboard` is
/// created and used exclusively here because it is not usable across threads
/// on every platform.
///
/// `init_tx` reports whether the OS clipboard could be opened; `spawn` blocks
/// on it so `available()` is answerable immediately. `available` is the live
/// flag the handle reads; it stays true only while this thread runs with a
/// working clipboard.
pub(crate) fn run(
    cmd_rx: Receiver<Command>,
    event_tx: tokio::sync::mpsc::Sender<ClipboardEvent>,
    init_tx: Sender<bool>,
    poll_interval: Duration,
    available: Arc<AtomicBool>,
) {
    let _guard = AvailabilityGuard(available.clone());
    let mut clipboard = match arboard::Clipboard::new() {
        Ok(clipboard) => {
            available.store(true, Ordering::Relaxed);
            let _ = init_tx.send(true);
            clipboard
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "OS clipboard unavailable (headless?); clipboard integration disabled"
            );
            let _ = init_tx.send(false);
            // Stay alive holding `event_tx` so the receiver never yields, and
            // drain ignored writes until every handle is dropped.
            while cmd_rx.recv().is_ok() {}
            return;
        }
    };

    // A zero interval would turn `recv_timeout` into a busy loop.
    let poll_interval = poll_interval.max(Duration::from_millis(1));

    let mut state = WatchState::new();
    // Content already in the clipboard at startup was not copied while we
    // were watching; record it silently so it is not re-shared on launch.
    if let Ok(text) = clipboard.get_text() {
        state.seed(text);
    }

    // `recv_timeout` doubles as the poll timer: either a command arrives or
    // the timeout elapses and we poll the clipboard. One mechanism, no busy
    // loop and no separate timer.
    loop {
        match cmd_rx.recv_timeout(poll_interval) {
            Ok(Command::SetText(text)) => match clipboard.set_text(text.as_str()) {
                // Only suppress on success: a failed write leaves the
                // clipboard unchanged, so there is no echo to swallow.
                Ok(()) => state.record_set(text),
                Err(err) => {
                    tracing::warn!(error = %err, "failed to write text to OS clipboard");
                }
            },
            Err(RecvTimeoutError::Timeout) => poll(&mut clipboard, &mut state, &event_tx),
            // All handles dropped; nothing can reach us anymore.
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn poll(
    clipboard: &mut arboard::Clipboard,
    state: &mut WatchState,
    event_tx: &tokio::sync::mpsc::Sender<ClipboardEvent>,
) {
    let text = match clipboard.get_text() {
        Ok(text) => text,
        // On Linux (and others) a clipboard holding non-text content, or
        // nothing at all, surfaces as an error here. That is "no text right
        // now", not a failure worth logging on every tick.
        Err(err) => {
            tracing::trace!(error = %err, "clipboard has no text");
            return;
        }
    };
    let Some(event) = state.observe(text) else {
        return;
    };
    match event_tx.try_send(event) {
        Ok(()) => {}
        // Dropping is better than blocking the clipboard thread forever on a
        // stalled consumer; the entry is lost but the watcher stays live.
        Err(TrySendError::Full(_)) => {
            tracing::warn!("clipboard event channel full; dropping copied text");
        }
        // Receiver gone (app shutting down); keep serving set_text quietly.
        Err(TrySendError::Closed(_)) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Holds regardless of whether the host has a clipboard: with one the
    /// loop sees the closed command channel and exits; without one the
    /// degraded drain loop sees it and returns. Either way the guard must
    /// leave the flag false.
    #[test]
    fn worker_exit_flips_available_to_false() {
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Command>();
        drop(cmd_tx);
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        let (init_tx, _init_rx) = std::sync::mpsc::channel();
        let available = Arc::new(AtomicBool::new(true));

        run(
            cmd_rx,
            event_tx,
            init_tx,
            Duration::from_millis(1),
            available.clone(),
        );

        assert!(!available.load(Ordering::SeqCst));
    }

    #[test]
    fn availability_guard_clears_flag_even_on_unwind() {
        let available = Arc::new(AtomicBool::new(true));
        let flag = available.clone();
        let result = std::panic::catch_unwind(move || {
            let _guard = AvailabilityGuard(flag);
            panic!("worker died");
        });
        assert!(result.is_err());
        assert!(!available.load(Ordering::SeqCst));
    }
}
