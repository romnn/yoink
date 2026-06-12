use crate::ClipboardEvent;

/// Pure emit/suppress decision logic for the clipboard watcher.
///
/// Kept free of any `arboard` or threading concerns so the rules (echo
/// suppression after `set_text`, deduplication of repeated polls, dropping
/// empty text) can be unit-tested without a display server.
#[derive(Debug, Default)]
pub(crate) struct WatchState {
    /// Text the handle last wrote via `set_text` that has not yet been seen
    /// coming back from a poll. Present means: swallow the next observation
    /// of exactly this text instead of echoing it back as a user copy.
    last_set: Option<String>,
    /// Text last seen in the OS clipboard, used to detect changes between
    /// polls.
    last_observed: Option<String>,
}

impl WatchState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record the clipboard contents found at startup without emitting an
    /// event: whatever was in the clipboard before the watcher started was
    /// not copied while we were running, so it must not be re-shared.
    pub(crate) fn seed(&mut self, text: String) {
        self.last_observed = Some(text);
    }

    /// Record text the handle just wrote into the OS clipboard so the next
    /// poll seeing it is treated as our own echo, not as a user copy.
    pub(crate) fn record_set(&mut self, text: String) {
        self.last_set = Some(text);
    }

    /// Feed one successfully polled clipboard text; returns the event to
    /// emit, if any.
    pub(crate) fn observe(&mut self, text: String) -> Option<ClipboardEvent> {
        if self.last_set.as_deref() == Some(text.as_str()) {
            // Our own write coming back from the poll loop: advance the
            // observation baseline silently. The suppression is consumed so
            // that a genuine user copy of the same text later (after other
            // clipboard activity) still emits.
            self.last_set = None;
            self.last_observed = Some(text);
            return None;
        }
        // Seeing anything else means our last write was overwritten before we
        // ever observed it (or never landed); the suppression record is stale
        // and keeping it would wrongly swallow a future user copy.
        self.last_set = None;

        if self.last_observed.as_deref() == Some(text.as_str()) {
            return None;
        }
        let emit = !text.trim().is_empty();
        self.last_observed = Some(text.clone());
        emit.then_some(ClipboardEvent::Copied(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copied(text: &str) -> ClipboardEvent {
        ClipboardEvent::Copied(text.to_owned())
    }

    #[test]
    fn first_observation_emits() {
        let mut state = WatchState::new();
        assert_eq!(state.observe("hello".into()), Some(copied("hello")));
    }

    #[test]
    fn repeated_observation_is_silent() {
        let mut state = WatchState::new();
        assert_eq!(state.observe("hello".into()), Some(copied("hello")));
        assert_eq!(state.observe("hello".into()), None);
        assert_eq!(state.observe("hello".into()), None);
    }

    #[test]
    fn changed_text_emits_again() {
        let mut state = WatchState::new();
        assert_eq!(state.observe("one".into()), Some(copied("one")));
        assert_eq!(state.observe("two".into()), Some(copied("two")));
        assert_eq!(state.observe("one".into()), Some(copied("one")));
    }

    #[test]
    fn seeded_text_does_not_emit() {
        let mut state = WatchState::new();
        state.seed("preexisting".into());
        assert_eq!(state.observe("preexisting".into()), None);
        assert_eq!(state.observe("fresh".into()), Some(copied("fresh")));
    }

    #[test]
    fn set_text_suppresses_its_own_echo() {
        let mut state = WatchState::new();
        state.record_set("remote entry".into());
        assert_eq!(state.observe("remote entry".into()), None);
        // The echo also updates last-observed, so re-polling stays silent.
        assert_eq!(state.observe("remote entry".into()), None);
    }

    #[test]
    fn set_then_user_copies_same_text_later_emits() {
        let mut state = WatchState::new();
        state.record_set("foo".into());
        assert_eq!(state.observe("foo".into()), None);
        assert_eq!(state.observe("bar".into()), Some(copied("bar")));
        // The earlier set_text("foo") must not suppress this genuine copy.
        assert_eq!(state.observe("foo".into()), Some(copied("foo")));
    }

    #[test]
    fn stale_set_record_is_cleared_when_overwritten_before_observed() {
        let mut state = WatchState::new();
        state.record_set("ours".into());
        // The user copied something before we ever saw our own write land.
        assert_eq!(state.observe("theirs".into()), Some(copied("theirs")));
        // Our write is gone from the clipboard; a later user copy of the same
        // text is genuine and must emit.
        assert_eq!(state.observe("ours".into()), Some(copied("ours")));
    }

    #[test]
    fn empty_text_never_emits() {
        let mut state = WatchState::new();
        assert_eq!(state.observe(String::new()), None);
        assert_eq!(state.observe("   \n\t".into()), None);
    }

    #[test]
    fn whitespace_observation_updates_baseline_without_emitting() {
        let mut state = WatchState::new();
        assert_eq!(state.observe("real".into()), Some(copied("real")));
        assert_eq!(state.observe("  ".into()), None);
        // The clipboard genuinely changed back to "real", so it emits again.
        assert_eq!(state.observe("real".into()), Some(copied("real")));
    }

    #[test]
    fn set_of_empty_text_is_still_swallowed_silently() {
        let mut state = WatchState::new();
        state.record_set(String::new());
        assert_eq!(state.observe(String::new()), None);
    }
}
