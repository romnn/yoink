use crate::{ClipEntry, DeviceInfo, MAX_HISTORY};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::broadcast;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Array, ArrayRef, Doc, Out, ReadTxn, StateVector, Subscription, Transact, Update};

/// An encoded yrs update together with where it came from.
#[derive(Debug, Clone)]
pub struct DocUpdate {
    /// yrs update encoded with lib0 v1 encoding.
    pub update: Vec<u8>,
    /// Device id of the remote peer this update was received from, or `None`
    /// when the change originated locally. Sync forwards an update to every
    /// connected peer except its origin, which (together with yrs suppressing
    /// events for no-op transactions) prevents echo loops in a mesh.
    pub origin: Option<String>,
}

/// Why a remote update or state vector could not be ingested.
#[derive(Debug, Error)]
pub enum DocError {
    /// The byte slice was not a valid lib0 v1 update or state vector.
    #[error("malformed update or state vector: {0}")]
    Decode(#[from] yrs::encoding::read::Error),
    /// The update decoded but yrs rejected it during integration.
    #[error("failed to apply update: {0}")]
    Apply(#[from] yrs::error::UpdateError),
}

/// Thread-safe wrapper around the shared clipboard yrs document.
///
/// All access goes through an internal mutex because yrs transactions demand
/// exclusive access to the document store (concurrent `transact_mut` calls
/// panic rather than block).
pub struct ClipDoc {
    state: Mutex<DocState>,
    updates: broadcast::Sender<DocUpdate>,
}

struct DocState {
    doc: Doc,
    entries: ArrayRef,
    _observer: Subscription,
}

impl ClipDoc {
    /// Create an empty document with its update observer already wired to the
    /// broadcast channel returned by [`ClipDoc::subscribe`].
    ///
    /// # Panics
    ///
    /// Panics if the update observer cannot be registered. This is
    /// unreachable: registration needs a read transaction, and nothing else
    /// can hold one on a doc that has not yet escaped this constructor.
    #[must_use]
    pub fn new() -> Self {
        let doc = Doc::new();
        let entries = doc.get_or_insert_array("entries");
        let (updates, _) = broadcast::channel(256);
        let tx = updates.clone();
        // Cannot fail here: registering the observer acquires a read
        // transaction and nothing else can hold one on a freshly created doc.
        #[allow(
            clippy::expect_used,
            reason = "observer registration only fails when a read txn is already held; impossible on a doc still inside its constructor (see the # Panics section)"
        )]
        let observer = doc
            .observe_update_v1(move |txn, event| {
                let origin = txn
                    .origin()
                    .and_then(|o| std::str::from_utf8(o.as_ref()).ok())
                    .map(str::to_string);
                let _ = tx.send(DocUpdate {
                    update: event.update.clone(),
                    origin,
                });
            })
            .expect("freshly created doc has no competing transaction");
        Self {
            state: Mutex::new(DocState {
                doc,
                entries,
                _observer: observer,
            }),
            updates,
        }
    }

    /// Restore a document from a snapshot produced by [`ClipDoc::snapshot`].
    ///
    /// # Errors
    ///
    /// Returns [`DocError::Decode`] if `snapshot` is not a valid lib0 v1
    /// update, or [`DocError::Apply`] if yrs rejects it during integration.
    pub fn load(snapshot: &[u8]) -> Result<Self, DocError> {
        let this = Self::new();
        this.apply_update(snapshot, None)?;
        Ok(this)
    }

    /// Append a locally produced clipboard entry and prune history beyond
    /// [`MAX_HISTORY`].
    pub fn add_entry(&self, device: &DeviceInfo, text: String) -> ClipEntry {
        let entry = ClipEntry::new(device, text);
        let state = self.state.lock();
        let mut txn = state.doc.transact_mut();
        state.entries.push_back(&mut txn, entry.to_any());
        let len = state.entries.len(&txn);
        if len > MAX_HISTORY {
            state.entries.remove_range(&mut txn, 0, len - MAX_HISTORY);
        }
        entry
    }

    /// All entries, oldest first.
    pub fn entries(&self) -> Vec<ClipEntry> {
        let state = self.state.lock();
        let txn = state.doc.transact();
        state
            .entries
            .iter(&txn)
            .filter_map(|out| match out {
                Out::Any(any) => ClipEntry::from_any(&any),
                _ => None,
            })
            .collect()
    }

    /// The most recently appended entry, or `None` when history is empty.
    pub fn latest(&self) -> Option<ClipEntry> {
        self.entries().pop()
    }

    /// This document's state vector, lib0 v1 encoded, for a peer to diff
    /// against in sync step 1.
    pub fn state_vector(&self) -> Vec<u8> {
        let state = self.state.lock();
        let txn = state.doc.transact();
        txn.state_vector().encode_v1()
    }

    /// Encode every change the remote peer (identified by its state vector)
    /// has not seen yet, *including* pending updates parked for missing
    /// dependencies (matching yrs' own `DefaultProtocol::handle_sync_step1`).
    /// This matters with partial allowlists: in an A<->B<->C line topology B
    /// relays everything it holds, and if parked updates were excluded
    /// (`encode_diff_v1`) they would be invisible to C until B happened to
    /// integrate them.
    ///
    /// # Errors
    ///
    /// Returns [`DocError::Decode`] if `remote_state_vector` is not a valid
    /// lib0 v1 state vector.
    pub fn diff(&self, remote_state_vector: &[u8]) -> Result<Vec<u8>, DocError> {
        let sv = StateVector::decode_v1(remote_state_vector)?;
        let state = self.state.lock();
        let txn = state.doc.transact();
        Ok(txn.encode_state_as_update_v1(&sv))
    }

    /// Apply a remote update. `origin` should be the sending peer's device id
    /// so subscribers can avoid echoing the update back to it.
    ///
    /// # Errors
    ///
    /// Returns [`DocError::Decode`] if `update` is not a valid lib0 v1 update,
    /// or [`DocError::Apply`] if yrs rejects it during integration.
    pub fn apply_update(&self, update: &[u8], origin: Option<&str>) -> Result<(), DocError> {
        let parsed = Update::decode_v1(update)?;
        let state = self.state.lock();
        let mut txn = match origin {
            Some(origin) => state.doc.transact_mut_with(origin),
            None => state.doc.transact_mut(),
        };
        txn.apply_update(parsed)?;
        Ok(())
    }

    /// Full document state as a single update, suitable for persistence.
    pub fn snapshot(&self) -> Vec<u8> {
        let state = self.state.lock();
        let txn = state.doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    }

    /// Subscribe to all committed changes (local and remote). Slow receivers
    /// may observe `Lagged` errors and should resync from current state.
    pub fn subscribe(&self) -> broadcast::Receiver<DocUpdate> {
        self.updates.subscribe()
    }
}

impl Default for ClipDoc {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(id: &str) -> DeviceInfo {
        DeviceInfo {
            id: id.into(),
            name: format!("device-{id}"),
        }
    }

    /// Sync two docs both ways via state-vector diff exchange.
    fn sync(a: &ClipDoc, b: &ClipDoc) {
        let diff_for_b = a.diff(&b.state_vector()).unwrap();
        let diff_for_a = b.diff(&a.state_vector()).unwrap();
        b.apply_update(&diff_for_b, Some("a")).unwrap();
        a.apply_update(&diff_for_a, Some("b")).unwrap();
    }

    #[test]
    fn add_and_read_entries() {
        let doc = ClipDoc::new();
        let dev = device("d1");
        doc.add_entry(&dev, "hello".into());
        doc.add_entry(&dev, "world".into());
        let entries = doc.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].text, "hello");
        assert_eq!(entries[1].text, "world");
        assert_eq!(doc.latest().unwrap().text, "world");
        assert_eq!(entries[0].device_name, "device-d1");
    }

    #[test]
    fn concurrent_adds_converge() {
        let a = ClipDoc::new();
        let b = ClipDoc::new();
        a.add_entry(&device("a"), "from a".into());
        b.add_entry(&device("b"), "from b".into());
        sync(&a, &b);
        let ea = a.entries();
        let eb = b.entries();
        assert_eq!(ea, eb);
        assert_eq!(ea.len(), 2);
    }

    #[test]
    fn history_is_pruned() {
        let doc = ClipDoc::new();
        let dev = device("d1");
        for i in 0..(MAX_HISTORY + 50) {
            doc.add_entry(&dev, format!("entry {i}"));
        }
        let entries = doc.entries();
        assert_eq!(u32::try_from(entries.len()).unwrap(), MAX_HISTORY);
        assert_eq!(
            entries.last().unwrap().text,
            format!("entry {}", MAX_HISTORY + 49)
        );
    }

    #[test]
    fn snapshot_roundtrip() {
        let doc = ClipDoc::new();
        doc.add_entry(&device("d1"), "persisted".into());
        let restored = ClipDoc::load(&doc.snapshot()).unwrap();
        assert_eq!(restored.entries(), doc.entries());
    }

    #[test]
    fn subscriber_sees_origin() {
        let doc = ClipDoc::new();
        let mut rx = doc.subscribe();

        doc.add_entry(&device("d1"), "local".into());
        let local = rx.try_recv().unwrap();
        assert_eq!(local.origin, None);

        let other = ClipDoc::new();
        other.add_entry(&device("d2"), "remote".into());
        let diff = other.diff(&doc.state_vector()).unwrap();
        doc.apply_update(&diff, Some("peer-1")).unwrap();
        let remote = rx.try_recv().unwrap();
        assert_eq!(remote.origin.as_deref(), Some("peer-1"));
    }

    #[test]
    fn diff_includes_pending_updates() {
        let a = ClipDoc::new();
        let mut updates = a.subscribe();
        a.add_entry(&device("a"), "first".into());
        let u1 = updates.try_recv().unwrap().update;
        a.add_entry(&device("a"), "second".into());
        let u2 = updates.try_recv().unwrap().update;

        // B receives only the second update; it is parked as pending because
        // its dependency (the first update) is missing.
        let b = ClipDoc::new();
        b.apply_update(&u2, Some("a")).unwrap();
        assert_eq!(b.entries().len(), 0, "u2 alone must stay parked at b");

        // C syncs from B alone (line topology a<->b<->c where a and c never
        // talk). B's diff must carry the parked update too, so that once C
        // obtains the first update it converges without ever reaching A.
        let c = ClipDoc::new();
        let diff = b.diff(&c.state_vector()).unwrap();
        c.apply_update(&diff, Some("b")).unwrap();
        c.apply_update(&u1, Some("a")).unwrap();
        let texts: Vec<_> = c.entries().into_iter().map(|e| e.text).collect();
        assert_eq!(texts, vec!["first".to_string(), "second".to_string()]);
    }

    #[test]
    fn noop_update_fires_no_event() {
        let a = ClipDoc::new();
        let b = ClipDoc::new();
        a.add_entry(&device("a"), "once".into());
        let diff = a.diff(&b.state_vector()).unwrap();
        b.apply_update(&diff, Some("a")).unwrap();

        let mut rx = b.subscribe();
        // Applying the exact same update again must not emit an event,
        // otherwise meshes of 3+ peers would re-broadcast forever.
        b.apply_update(&diff, Some("a")).unwrap();
        assert!(rx.try_recv().is_err());
    }
}
