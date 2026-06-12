//! The app event loop: single owner of doc writes, clipboard writes and
//! config persistence. Everything else (server, sync, discovery) feeds
//! events or commands into it through channels.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{broadcast, mpsc};
use yoink_clipboard::{ClipboardEvent, ClipboardHandle};
use yoink_core::{AppCommand, ClipDoc, DeviceInfo, DocSet, DocUpdate, Scope, sanitize_room_name};
use yoink_discovery::{Discovery, DiscoveryEvent, PeerInfo};
use yoink_server::PeerView;
use yoink_sync::{SyncEvent, SyncManager};

use crate::config::{self, Config};

/// How many of the newest history entries a freshly observed clipboard copy
/// is deduplicated against. Checking only the latest entry is not enough:
/// two yoink instances on one machine share the OS clipboard, so poll/sync
/// races can interleave a peer's echoed entry between a copy and its dedupe
/// check, producing adjacent duplicates. A small window covers the realistic
/// interleavings without suppressing genuine re-copies of older text.
pub(crate) const COPY_DEDUPE_WINDOW: usize = 3;

/// Capacity of the merged doc-event channel all per-scope forwarders feed.
/// Matches the underlying per-doc broadcast capacity, so the forwarders (and
/// not this channel) are where sustained backpressure surfaces as a lag.
pub(crate) const DOC_EVENT_QUEUE: usize = 256;

/// Whether a newly produced text should be skipped because it already
/// appears among the most recent history entries (`recent`). This is both a
/// dedupe and an echo guard: without it, applying a remote entry to the
/// clipboard and then observing that same clipboard content would re-add it
/// forever.
pub(crate) fn is_duplicate(text: &str, recent: &[String]) -> bool {
    recent.iter().any(|recent_text| recent_text == text)
}

/// Auto-apply freshness window. A `SYNC_STEP_2` backlog apply is
/// indistinguishable from a live UPDATE at this layer, so without a
/// freshness gate a peer (re)connecting would clobber the local clipboard
/// with its newest *backlog* entry, however old. Tradeoff: devices whose
/// clocks disagree by more than this window lose auto-apply, but they never
/// lose history (entries still sync; manual copy still works) — far better
/// than silently overwriting the user's clipboard with stale data.
pub(crate) const AUTO_APPLY_MAX_AGE: Duration = Duration::from_secs(30);

/// Inputs for deciding whether the latest entry of a remotely-originated doc
/// update should be written to the OS clipboard. Only ever evaluated for the
/// `devices` scope: rooms never touch the OS clipboard passively, no matter
/// what arrives in them (DESIGN.md).
pub(crate) struct AutoApplyCheck<'a> {
    pub auto_apply: bool,
    pub clipboard_available: bool,
    pub self_device_id: &'a str,
    pub entry_device_id: &'a str,
    pub entry_id: &'a str,
    pub last_applied_entry_id: Option<&'a str>,
    pub entry_created_at_ms: u64,
    pub now_ms: u64,
}

impl AutoApplyCheck<'_> {
    pub(crate) fn should_apply(&self) -> bool {
        // abs_diff because clock skew can also stamp a genuinely fresh entry
        // "in the future"; both directions are treated the same.
        let max_age_ms = u64::try_from(AUTO_APPLY_MAX_AGE.as_millis()).unwrap_or(u64::MAX);
        let fresh = self.now_ms.abs_diff(self.entry_created_at_ms) <= max_age_ms;
        self.auto_apply
            && self.clipboard_available
            && fresh
            && self.entry_device_id != self.self_device_id
            && self.last_applied_entry_id != Some(self.entry_id)
    }
}

/// What `AppCommand::AddEntry` does besides appending to the scope's doc.
/// Devices adds mirror the text into the local clipboard so a paste right
/// after "add" does what users expect; room adds never do — sharing into a
/// room is not a copy, and rooms must not touch the OS clipboard implicitly.
pub(crate) struct AddEntryPlan {
    pub add_to_doc: bool,
    pub mirror_to_clipboard: bool,
}

pub(crate) fn plan_add_entry(scope: &Scope, duplicate_of_latest: bool) -> AddEntryPlan {
    AddEntryPlan {
        add_to_doc: !duplicate_of_latest,
        mirror_to_clipboard: scope.is_devices(),
    }
}

/// Registry policy when a peer's mDNS announcement disappears: allowed peers
/// stay (flipped offline) so the UI can still show their name and offer
/// revoke, while never-allowed strangers are removed — otherwise they would
/// linger as dead rows forever.
pub(crate) fn registry_on_lost(
    peers: &mut HashMap<String, PeerView>,
    device_id: &str,
    allowed: bool,
) {
    if allowed {
        if let Some(view) = peers.get_mut(device_id) {
            view.online = false;
        }
    } else {
        peers.remove(device_id);
    }
}

/// At most one in-flight background file write. The app loop never blocks on
/// it: a write is started on the blocking pool and its completion is reaped
/// on a later flush tick (or at shutdown), so a slow disk cannot stall event
/// consumption while failures still re-mark the corresponding dirty flag.
pub(crate) struct BackgroundWrite {
    handle: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl BackgroundWrite {
    pub(crate) fn idle() -> Self {
        Self { handle: None }
    }

    pub(crate) fn in_flight(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Start `write` on the blocking pool. Callers must reap first: two
    /// concurrent writers would race over the same tmp file.
    pub(crate) fn start(&mut self, write: impl FnOnce() -> anyhow::Result<()> + Send + 'static) {
        debug_assert!(self.handle.is_none(), "background write already pending");
        self.handle = Some(tokio::task::spawn_blocking(write));
    }

    /// Await and clear the previous write; `Ok(())` when none was pending.
    /// Only blocks meaningfully when a write is still in flight (shutdown
    /// does that on purpose; the flush tick checks `in_flight` first).
    pub(crate) async fn reap(&mut self) -> anyhow::Result<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        match handle.await {
            Ok(result) => result,
            Err(err) => Err(anyhow::anyhow!("background write task panicked: {err}")),
        }
    }
}

/// Per-scope snapshot persistence: each open doc gets its own file, dirty
/// flag and single-writer [`BackgroundWrite`], so the existing reap/retry
/// discipline applies per file.
pub(crate) struct SnapshotState {
    pub path: PathBuf,
    pub dirty: bool,
    pub write: BackgroundWrite,
}

impl SnapshotState {
    pub(crate) fn clean(path: PathBuf) -> Self {
        Self {
            path,
            dirty: false,
            write: BackgroundWrite::idle(),
        }
    }
}

/// Where `scope`'s snapshot lives: the devices doc keeps its historical
/// `state.bin` location, each room snapshots to `rooms/{name}.bin`. Room
/// names are sanitized (lowercase ASCII alphanumerics and hyphens), so they
/// are safe as file names.
pub(crate) fn snapshot_path(config_dir: &Path, scope: &Scope) -> PathBuf {
    match scope.room_name() {
        None => config_dir.join("state.bin"),
        Some(name) => config_dir.join("rooms").join(format!("{name}.bin")),
    }
}

/// Atomic snapshot write plus lazy parent-directory creation: `rooms/` only
/// comes into existence once the first room snapshot is written.
pub(crate) fn write_snapshot(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create snapshot directory {}", parent.display()))?;
    }
    config::write_atomic(path, bytes)
}

/// Restore `scope`'s doc from its snapshot file and register it in `docs`.
///
/// The snapshot is applied INTO the registered doc (a CRDT merge) rather
/// than inserted as a fresh doc: the devices scope always pre-exists in a
/// `DocSet`, so an insert-unless-present would silently discard the loaded
/// history and keep the empty pre-created doc. Merging also makes a
/// double-join race harmless — both snapshots simply converge.
pub(crate) fn restore_doc(docs: &DocSet, scope: &Scope, path: &Path) -> Arc<ClipDoc> {
    let doc = docs.get_or_create(scope);
    match std::fs::read(path) {
        Ok(snapshot) => {
            // Clipboard history is expendable, so unlike the config a
            // corrupt snapshot only warns and starts fresh.
            if let Err(err) = doc.apply_update(&snapshot, None) {
                tracing::warn!(
                    error = %err,
                    path = %path.display(),
                    "snapshot file is corrupt; starting with an empty history",
                );
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(
                error = %err,
                path = %path.display(),
                "failed to read snapshot file; starting with an empty history",
            );
        }
    }
    doc
}

/// What a per-scope forwarder feeds into the loop's merged doc channel.
pub(crate) enum DocEvent {
    Update {
        scope: Scope,
        update: DocUpdate,
    },
    /// The forwarder's broadcast receiver lagged: updates were lost, so the
    /// scope's snapshot must be re-marked dirty and the UI refreshed even
    /// though no update payload is available.
    Lagged {
        scope: Scope,
    },
}

/// Spawn the task forwarding `doc`'s update stream into the merged channel,
/// tagged with its scope. The subscription is taken *before* the task is
/// spawned, so wiring forwarders up before sync (or the server) exists
/// guarantees no update slips past the loop — broadcast receivers only see
/// messages sent after subscribing.
pub(crate) fn spawn_doc_forwarder(
    scope: Scope,
    doc: &ClipDoc,
    events: mpsc::Sender<DocEvent>,
) -> tokio::task::JoinHandle<()> {
    let updates = doc.subscribe();
    tokio::spawn(forward_doc_updates(scope, updates, events))
}

async fn forward_doc_updates(
    scope: Scope,
    mut updates: broadcast::Receiver<DocUpdate>,
    events: mpsc::Sender<DocEvent>,
) {
    loop {
        let event = match updates.recv().await {
            Ok(update) => DocEvent::Update {
                scope: scope.clone(),
                update,
            },
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    %scope,
                    "doc update receiver lagged; forcing snapshot and UI refresh",
                );
                DocEvent::Lagged {
                    scope: scope.clone(),
                }
            }
            // The doc outlives its forwarder for every scope we sync, so a
            // closed stream means the doc was dropped (room left) and the
            // forwarder is done.
            Err(broadcast::error::RecvError::Closed) => return,
        };
        // The app loop owning the receiver is the only thing that stops on
        // its own; without it there is nobody left to forward to.
        if events.send(event).await.is_err() {
            return;
        }
    }
}

/// Restore every persisted scope's doc and wire its update forwarder, used by
/// `main` to seed the loop's [`App`] before sync or the server exist. The
/// `devices` scope is always restored first, followed by every joined room;
/// doing this up front guarantees no update slips past the loop and that sync
/// joins the restored room docs instead of creating empty ones.
///
/// Returns the per-scope forwarder handles and clean (not-yet-dirty) snapshot
/// states, both keyed by scope, ready to be moved into the [`App`].
pub(crate) fn restore_scopes(
    docs: &DocSet,
    config_dir: &Path,
    rooms: &[String],
    doc_events_tx: &mpsc::Sender<DocEvent>,
) -> (
    HashMap<Scope, tokio::task::JoinHandle<()>>,
    HashMap<Scope, SnapshotState>,
) {
    let mut forwarders = HashMap::new();
    let mut snapshots = HashMap::new();
    let scopes = std::iter::once(Scope::Devices).chain(rooms.iter().map(Scope::room));
    for scope in scopes {
        let path = snapshot_path(config_dir, &scope);
        let doc = restore_doc(docs, &scope, &path);
        forwarders.insert(
            scope.clone(),
            spawn_doc_forwarder(scope.clone(), &doc, doc_events_tx.clone()),
        );
        snapshots.insert(scope, SnapshotState::clean(path));
    }
    (forwarders, snapshots)
}

/// All receivers the loop selects over, handed in by `main` after wiring.
pub(crate) struct AppChannels {
    /// OS clipboard `Copied` events.
    pub clipboard: mpsc::Receiver<ClipboardEvent>,
    /// Merged per-scope doc updates (and lag signals) from the forwarders.
    pub doc: mpsc::Receiver<DocEvent>,
    /// mDNS peer found/lost events.
    pub discovery: mpsc::Receiver<DiscoveryEvent>,
    /// Per-scope peer connect/disconnect events from sync.
    pub sync: mpsc::Receiver<SyncEvent>,
    /// Commands issued by the web server (the UI's only mutation path).
    pub command: mpsc::Receiver<AppCommand>,
}

pub(crate) struct App {
    pub docs: Arc<DocSet>,
    pub device: DeviceInfo,
    pub clipboard: ClipboardHandle,
    pub sync: Arc<SyncManager>,
    pub discovery: Discovery,
    pub peers: Arc<parking_lot::RwLock<HashMap<String, PeerView>>>,
    pub settings: Arc<parking_lot::RwLock<yoink_server::Settings>>,
    /// Mirror of the joined room names for the server (UI reads it); the
    /// loop is the only writer, keeping it in lockstep with `config.rooms`.
    pub joined_rooms: Arc<parking_lot::RwLock<BTreeSet<String>>>,
    pub notify: broadcast::Sender<()>,
    pub config: Config,
    pub config_dir: PathBuf,
    /// Id of the last remote entry written to the OS clipboard, so a peer
    /// resending the same state never re-applies it.
    pub last_applied_entry_id: Option<String>,
    /// One snapshot file per open scope; entries come and go with
    /// join/leave, the `Devices` entry lives as long as the loop.
    pub snapshots: HashMap<Scope, SnapshotState>,
    pub config_dirty: bool,
    pub config_write: BackgroundWrite,
    /// Sender side of the merged doc channel, kept so `JoinRoom` can wire up
    /// the new room's forwarder.
    pub doc_events_tx: mpsc::Sender<DocEvent>,
    /// Per-scope forwarder tasks, aborted on `LeaveRoom` so a left room
    /// stops producing doc events.
    pub forwarders: HashMap<Scope, tokio::task::JoinHandle<()>>,
}

impl App {
    pub async fn run(mut self, mut ch: AppChannels) -> anyhow::Result<()> {
        let mut flush = tokio::time::interval(Duration::from_secs(1));
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        loop {
            tokio::select! {
                Some(event) = ch.clipboard.recv() => self.on_clipboard(event),
                // The loop holds a sender (`doc_events_tx`), so this channel
                // can never close from under the select.
                Some(event) = ch.doc.recv() => self.on_doc_event(event),
                Some(event) = ch.discovery.recv() => self.on_discovery(event),
                Some(event) = ch.sync.recv() => self.on_sync(&event),
                Some(command) = ch.command.recv() => self.on_command(command).await,
                _ = flush.tick() => self.flush_dirty().await,
                result = &mut ctrl_c => {
                    if let Err(err) = result {
                        tracing::error!(error = %err, "failed to listen for ctrl-c; shutting down");
                    }
                    break;
                }
            }
        }

        tracing::info!("shutting down");
        // Settle in-flight background writes first so the final writes below
        // can never race them over the same tmp files. Blocking the loop is
        // fine here: there is nothing left to consume.
        for (scope, snapshot) in &mut self.snapshots {
            if let Err(err) = snapshot.write.reap().await {
                snapshot.dirty = true;
                tracing::warn!(error = %err, %scope, "in-flight snapshot write failed during shutdown");
            }
            if !snapshot.dirty {
                continue;
            }
            let Some(doc) = self.docs.get(scope) else {
                continue;
            };
            if let Err(err) = write_snapshot(&snapshot.path, &doc.snapshot()) {
                tracing::warn!(
                    error = %err,
                    path = %snapshot.path.display(),
                    "failed to write doc snapshot on shutdown",
                );
            }
        }
        if let Err(err) = self.config_write.reap().await {
            self.config_dirty = true;
            tracing::error!(error = %err, "in-flight config write failed during shutdown");
        }
        self.discovery.shutdown();
        if self.config_dirty
            && let Err(err) = self.config.save(&self.config_dir)
        {
            tracing::error!(error = %err, "failed to persist config on shutdown");
        }
        Ok(())
    }

    fn on_clipboard(&mut self, event: ClipboardEvent) {
        let ClipboardEvent::Copied(text) = event;
        // Clipboard capture is hard-wired to the devices scope: the OS
        // clipboard never feeds a room (DESIGN.md).
        let devices = self.docs.devices();
        if is_duplicate(&text, &recent_texts(&devices, COPY_DEDUPE_WINDOW)) {
            return;
        }
        // The resulting doc update comes back through our own subscription,
        // which marks the snapshot dirty and notifies the UI.
        devices.add_entry(&self.device, text);
    }

    fn on_doc_event(&mut self, event: DocEvent) {
        match event {
            DocEvent::Update { scope, update } => self.on_doc_update(&scope, &update),
            DocEvent::Lagged { scope } => {
                self.mark_snapshot_dirty(&scope);
                self.notify_ui();
            }
        }
    }

    fn on_doc_update(&mut self, scope: &Scope, update: &DocUpdate) {
        self.mark_snapshot_dirty(scope);
        // Auto-apply is exclusive to the devices scope: a room update never
        // touches the OS clipboard passively, no matter who sent it —
        // copying out of a room is always a deliberate `CopyEntry`.
        if scope.is_devices()
            && update.origin.is_some()
            && let Some(entry) = self.docs.devices().latest()
        {
            let auto_apply = self.settings.read().auto_apply;
            let check = AutoApplyCheck {
                auto_apply,
                clipboard_available: self.clipboard.available(),
                self_device_id: &self.device.id,
                entry_device_id: &entry.device_id,
                entry_id: &entry.id,
                last_applied_entry_id: self.last_applied_entry_id.as_deref(),
                entry_created_at_ms: entry.created_at_ms,
                now_ms: yoink_core::now_ms(),
            };
            if check.should_apply() {
                self.clipboard.set_text(entry.text);
                self.last_applied_entry_id = Some(entry.id);
            }
        }
        self.notify_ui();
    }

    fn mark_snapshot_dirty(&mut self, scope: &Scope) {
        if let Some(snapshot) = self.snapshots.get_mut(scope) {
            snapshot.dirty = true;
        }
        // A missing entry means the scope was left while one of its updates
        // was still queued; the final snapshot was already flushed by
        // `leave_room`, so there is nothing left to persist.
    }

    fn on_discovery(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::Found(peer) => {
                tracing::info!(device_id = %peer.device_id, name = %peer.name, "peer discovered");
                self.peers.write().insert(
                    peer.device_id.clone(),
                    PeerView {
                        info: peer.clone(),
                        online: true,
                    },
                );
                self.sync.peer_discovered(peer);
            }
            DiscoveryEvent::Lost { device_id } => {
                tracing::info!(%device_id, "peer lost");
                registry_on_lost(
                    &mut self.peers.write(),
                    &device_id,
                    self.sync.is_allowed(&device_id),
                );
                self.sync.peer_lost(&device_id);
            }
        }
        self.notify_ui();
    }

    fn on_sync(&mut self, event: &SyncEvent) {
        match event {
            SyncEvent::PeerConnected {
                device_id,
                device_name,
                scope,
            } => {
                tracing::info!(%device_id, %device_name, %scope, "peer connected");
                // A peer can connect before (or without) showing up via mDNS
                // — e.g. it dialed us right after we restarted. Seed the
                // registry from the HELLO so the UI never shows a bare id.
                // Only the devices scope seeds: the peer registry is the
                // pairing UI, and a room peer may be a total stranger there.
                if scope.is_devices() {
                    let mut peers = self.peers.write();
                    peers
                        .entry(device_id.clone())
                        .and_modify(|view| {
                            view.info.name = device_name.clone();
                            view.online = true;
                        })
                        .or_insert_with(|| PeerView {
                            info: PeerInfo {
                                device_id: device_id.clone(),
                                name: device_name.clone(),
                                addrs: Vec::new(),
                                port: 0,
                                rooms: Vec::new(),
                            },
                            online: true,
                        });
                }
            }
            SyncEvent::PeerDisconnected { device_id, scope } => {
                tracing::info!(%device_id, %scope, "peer disconnected");
            }
        }
        self.notify_ui();
    }

    async fn on_command(&mut self, command: AppCommand) {
        match command {
            AppCommand::SetAllowed { device_id, allowed } => {
                self.sync.set_allowed(&device_id, allowed);
                if allowed {
                    if !self.config.allowed.contains(&device_id) {
                        self.config.allowed.push(device_id);
                    }
                } else {
                    self.config.allowed.retain(|id| id != &device_id);
                }
                self.persist_config();
                self.notify_ui();
            }
            AppCommand::SetAutoApply { enabled } => {
                self.settings.write().auto_apply = enabled;
                self.config.auto_apply = enabled;
                self.persist_config();
                self.notify_ui();
            }
            AppCommand::AddEntry { text, scope } => {
                let Some(doc) = self.docs.get(&scope) else {
                    // Can only be a room scope: the devices doc always
                    // exists. Most likely the room was left while the add
                    // was in flight.
                    tracing::warn!(%scope, "add_entry for a scope with no open doc; dropping");
                    return;
                };
                // Deliberate UI adds only dedupe against the latest entry
                // (window 1): re-sharing an older text should still create a
                // new entry, unlike passive clipboard polls.
                let plan = plan_add_entry(&scope, is_duplicate(&text, &recent_texts(&doc, 1)));
                if plan.add_to_doc {
                    doc.add_entry(&self.device, text.clone());
                }
                // Mirror the shared entry into the sharing device's own
                // clipboard so a paste right after "add" does what users
                // expect — even when the duplicate entry itself was skipped.
                // Room adds never mirror: sharing into a room is not a copy.
                if plan.mirror_to_clipboard {
                    self.clipboard.set_text(text);
                }
            }
            AppCommand::CopyEntry { id, scope } => {
                let Some(doc) = self.docs.get(&scope) else {
                    tracing::warn!(%scope, "copy_entry for a scope with no open doc; dropping");
                    return;
                };
                // Copying *out* of a room is a deliberate action and therefore
                // allowed; only passive capture/auto-apply is devices-only.
                if let Some(entry) = doc.entries().into_iter().find(|entry| entry.id == id) {
                    self.clipboard.set_text(entry.text);
                } else {
                    tracing::warn!(%id, %scope, "copy requested for unknown entry");
                }
            }
            AppCommand::JoinRoom { name } => self.join_room(&name),
            AppCommand::LeaveRoom { name } => self.leave_room(&name).await,
        }
    }

    fn join_room(&mut self, name: &str) {
        // Joining doubles as creation, so the name may be raw user input
        // ("My Room", a typed URL path); canonicalize it here.
        let Some(name) = sanitize_room_name(name) else {
            tracing::warn!(room = name, "ignoring join for unusable room name");
            return;
        };
        // Idempotent: visiting an already-joined room's URL is a no-op.
        if self.config.rooms.binary_search(&name).is_ok() {
            return;
        }
        let scope = Scope::room(&name);
        let path = snapshot_path(&self.config_dir, &scope);
        // A previous membership may have left a snapshot behind; rejoining
        // restores that history. Register the doc and its forwarder before
        // `sync.join_room`, so sync finds the restored doc (instead of
        // creating an empty one) and no early peer update slips past the
        // loop.
        let doc = restore_doc(&self.docs, &scope, &path);
        self.forwarders.insert(
            scope.clone(),
            spawn_doc_forwarder(scope.clone(), &doc, self.doc_events_tx.clone()),
        );
        let mut snapshot = SnapshotState::clean(path);
        // Materialize the room's file on the next flush tick even before any
        // entry exists, so join/leave/rejoin behaves the same with or
        // without traffic.
        snapshot.dirty = true;
        self.snapshots.insert(scope, snapshot);
        self.sync.join_room(&name);
        self.joined_rooms.write().insert(name.clone());
        if let Err(pos) = self.config.rooms.binary_search(&name) {
            self.config.rooms.insert(pos, name);
        }
        self.persist_config();
        self.discovery.set_rooms(&self.config.rooms);
        self.notify_ui();
    }

    async fn leave_room(&mut self, name: &str) {
        // Sanitize for symmetry with join: the UI may echo back whatever
        // form it had.
        let Some(name) = sanitize_room_name(name) else {
            tracing::warn!(room = name, "ignoring leave for unusable room name");
            return;
        };
        if self.config.rooms.binary_search(&name).is_err() {
            tracing::warn!(room = %name, "ignoring leave for a room that is not joined");
            return;
        }
        let scope = Scope::room(&name);
        // Order matters: stop sync (hangs up the room's connections) and the
        // forwarder first, so the doc is quiescent when the final snapshot
        // is taken.
        self.sync.leave_room(&name);
        if let Some(forwarder) = self.forwarders.remove(&scope) {
            forwarder.abort();
        }
        if let Some(mut snapshot) = self.snapshots.remove(&scope) {
            // Settle any in-flight write before the final one so they cannot
            // race over the same tmp file; then write synchronously. The
            // file is deliberately kept on disk — rejoining restores the
            // room's history.
            if let Err(err) = snapshot.write.reap().await {
                tracing::warn!(error = %err, room = %name, "in-flight room snapshot write failed on leave");
            }
            if let Some(doc) = self.docs.get(&scope) {
                let bytes = doc.snapshot();
                let path = snapshot.path;
                let written =
                    tokio::task::spawn_blocking(move || write_snapshot(&path, &bytes)).await;
                match written {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        tracing::warn!(error = %err, room = %name, "failed to write final room snapshot on leave");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, room = %name, "final room snapshot write panicked");
                    }
                }
            }
        }
        self.docs.remove(&scope);
        self.joined_rooms.write().remove(&name);
        self.config.rooms.retain(|room| room != &name);
        self.persist_config();
        self.discovery.set_rooms(&self.config.rooms);
        self.notify_ui();
    }

    fn persist_config(&mut self) {
        // Only mark dirty; the flush tick performs the write off the loop
        // (spawn_blocking) so a slow disk cannot stall event consumption.
        // The in-memory change survives failures and is retried on shutdown.
        self.config_dirty = true;
    }

    /// Persist whatever is dirty. Each write runs on the blocking pool and
    /// is reaped on a later tick; nothing here blocks the event loop.
    async fn flush_dirty(&mut self) {
        self.flush_snapshots().await;
        self.flush_config().await;
    }

    async fn flush_snapshots(&mut self) {
        for (scope, snapshot) in &mut self.snapshots {
            if snapshot.write.in_flight() {
                // Still writing (slow disk); the dirty flag survives to the
                // next tick, so nothing is lost by waiting.
                continue;
            }
            if let Err(err) = snapshot.write.reap().await {
                snapshot.dirty = true;
                tracing::warn!(
                    error = %err,
                    path = %snapshot.path.display(),
                    "failed to write doc snapshot; will retry",
                );
            }
            if !snapshot.dirty {
                continue;
            }
            let Some(doc) = self.docs.get(scope) else {
                // The scope's doc is gone (unreachable in practice: leaving
                // a room removes its snapshot entry in the same handler);
                // nothing can be persisted for it anymore.
                snapshot.dirty = false;
                continue;
            };
            let bytes = doc.snapshot();
            let path = snapshot.path.clone();
            snapshot.dirty = false;
            snapshot.write.start(move || write_snapshot(&path, &bytes));
        }
    }

    async fn flush_config(&mut self) {
        if self.config_write.in_flight() {
            return;
        }
        if let Err(err) = self.config_write.reap().await {
            self.config_dirty = true;
            tracing::error!(error = %err, "failed to persist config; will retry");
        }
        if !self.config_dirty {
            return;
        }
        let config = self.config.clone();
        let dir = self.config_dir.clone();
        self.config_dirty = false;
        self.config_write.start(move || config.save(&dir));
    }

    fn notify_ui(&self) {
        // The clipboard can die at runtime (worker thread exit); refresh the
        // cached settings flag on every notify so the UI chip stays truthful.
        self.settings.write().clipboard_available = self.clipboard.available();
        // No receiver just means no UI is connected right now.
        let _ = self.notify.send(());
    }
}

/// Texts of `doc`'s `n` most recent history entries, newest first.
fn recent_texts(doc: &ClipDoc, n: usize) -> Vec<String> {
    doc.entries()
        .into_iter()
        .rev()
        .take(n)
        .map(|entry| entry.text)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All guards pass: remote fresh entry, clipboard available, not yet
    /// applied. Individual tests flip one knob at a time from here.
    fn base() -> AutoApplyCheck<'static> {
        AutoApplyCheck {
            auto_apply: true,
            clipboard_available: true,
            self_device_id: "self",
            entry_device_id: "peer",
            entry_id: "e1",
            last_applied_entry_id: None,
            entry_created_at_ms: 1_000_000,
            now_ms: 1_000_000,
        }
    }

    fn texts(texts: &[&str]) -> Vec<String> {
        texts.iter().map(ToString::to_string).collect()
    }

    fn device(id: &str) -> DeviceInfo {
        DeviceInfo {
            id: id.into(),
            name: format!("device-{id}"),
        }
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("yoink-app-test-{}", uuid::Uuid::new_v4())))
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
    fn duplicate_within_recent_window_is_skipped() {
        let recent = texts(&["newest", "middle", "oldest"]);
        assert!(is_duplicate("newest", &recent));
        assert!(is_duplicate("middle", &recent));
        assert!(is_duplicate("oldest", &recent));
        assert!(!is_duplicate("fresh", &recent));
        assert!(!is_duplicate("anything", &[]));
    }

    #[test]
    fn duplicate_against_latest_only_ignores_older_entries() {
        // The AddEntry path dedupes with window 1; an older text must not
        // count as a duplicate there.
        let latest_only = texts(&["newest"]);
        assert!(is_duplicate("newest", &latest_only));
        assert!(!is_duplicate("middle", &latest_only));
    }

    #[test]
    fn auto_apply_requires_setting_clipboard_and_remote_entry() {
        assert!(base().should_apply());
        assert!(
            !AutoApplyCheck {
                auto_apply: false,
                ..base()
            }
            .should_apply()
        );
        assert!(
            !AutoApplyCheck {
                clipboard_available: false,
                ..base()
            }
            .should_apply()
        );
        assert!(
            !AutoApplyCheck {
                entry_device_id: "self",
                ..base()
            }
            .should_apply()
        );
    }

    #[test]
    fn auto_apply_never_reapplies_the_same_entry() {
        assert!(
            !AutoApplyCheck {
                last_applied_entry_id: Some("e1"),
                ..base()
            }
            .should_apply()
        );
        assert!(
            AutoApplyCheck {
                entry_id: "e2",
                last_applied_entry_id: Some("e1"),
                ..base()
            }
            .should_apply()
        );
    }

    #[test]
    fn auto_apply_freshness_gate() {
        let max = u64::try_from(AUTO_APPLY_MAX_AGE.as_millis()).unwrap_or(u64::MAX);
        let now = 1_000_000_000;
        let at = |entry_created_at_ms: u64| AutoApplyCheck {
            entry_created_at_ms,
            now_ms: now,
            ..base()
        };
        // A live remote copy applies.
        assert!(at(now - 1_000).should_apply());
        // Boundary: exactly the window edge still applies.
        assert!(at(now - max).should_apply());
        // One past the edge: a backlog entry replayed via SYNC_STEP_2 on
        // (re)connect must not clobber the local clipboard.
        assert!(!at(now - max - 1).should_apply());
        // Clock skew works in both directions.
        assert!(at(now + max).should_apply());
        assert!(!at(now + max + 1).should_apply());
    }

    #[test]
    fn room_add_is_never_mirrored_to_the_clipboard() {
        // Sharing into a room is not a copy: neither a fresh nor a duplicate
        // room add may touch the OS clipboard.
        let room = Scope::room("attic");
        let fresh = plan_add_entry(&room, false);
        assert!(fresh.add_to_doc);
        assert!(!fresh.mirror_to_clipboard);
        let duplicate = plan_add_entry(&room, true);
        assert!(!duplicate.add_to_doc);
        assert!(!duplicate.mirror_to_clipboard);
    }

    #[test]
    fn devices_add_mirrors_even_when_duplicate() {
        let duplicate = plan_add_entry(&Scope::Devices, true);
        assert!(!duplicate.add_to_doc, "duplicate is not re-added");
        assert!(
            duplicate.mirror_to_clipboard,
            "paste right after add must still work"
        );
        let fresh = plan_add_entry(&Scope::Devices, false);
        assert!(fresh.add_to_doc);
        assert!(fresh.mirror_to_clipboard);
    }

    #[test]
    fn snapshot_paths_per_scope() {
        let dir = Path::new("/cfg");
        assert_eq!(
            snapshot_path(dir, &Scope::Devices),
            PathBuf::from("/cfg/state.bin")
        );
        assert_eq!(
            snapshot_path(dir, &Scope::room("standup")),
            PathBuf::from("/cfg/rooms/standup.bin")
        );
    }

    #[test]
    fn join_leave_rejoin_restores_room_history() {
        let dir = TempDir::new();
        let docs = DocSet::new();
        let scope = Scope::room("attic");
        let path = snapshot_path(dir.path(), &scope);

        // Join: no snapshot on disk yet, so the room starts empty.
        let doc = restore_doc(&docs, &scope, &path);
        assert!(doc.entries().is_empty());
        doc.add_entry(&device("d1"), "shared into room".into());

        // Leave: the final flush creates `rooms/` lazily and keeps the file;
        // the doc itself is dropped from the set.
        write_snapshot(&path, &doc.snapshot()).unwrap();
        drop(doc);
        docs.remove(&scope);
        assert!(path.exists(), "snapshot file survives leaving the room");
        assert!(docs.get(&scope).is_none());

        // Rejoin: history is restored from the kept snapshot.
        let doc = restore_doc(&docs, &scope, &path);
        let entries = doc.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].text, "shared into room");
    }

    #[test]
    fn devices_snapshot_restores_into_the_preexisting_doc() {
        // Regression: Scope::Devices always pre-exists in a DocSet, so an
        // insert-unless-present restore silently discarded the loaded
        // history and history vanished on every restart.
        let dir = TempDir::new();
        let path = snapshot_path(dir.path(), &Scope::Devices);

        let snapshot = {
            let doc = ClipDoc::new();
            doc.add_entry(&device("d1"), "persisted across restart".into());
            doc.snapshot()
        };
        write_snapshot(&path, &snapshot).unwrap();

        let docs = DocSet::new();
        let doc = restore_doc(&docs, &Scope::Devices, &path);
        assert_eq!(doc.entries().len(), 1);
        assert_eq!(doc.entries()[0].text, "persisted across restart");
        assert!(Arc::ptr_eq(&doc, &docs.devices()));
    }

    #[test]
    fn corrupt_room_snapshot_starts_fresh() {
        let dir = TempDir::new();
        let path = snapshot_path(dir.path(), &Scope::room("attic"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not a yrs update").unwrap();

        let docs = DocSet::new();
        let doc = restore_doc(&docs, &Scope::room("attic"), &path);
        assert!(doc.entries().is_empty());
        // The corrupt file is left in place for inspection; only a
        // successful flush replaces it.
        assert!(path.exists());
    }

    #[tokio::test]
    async fn forwarder_tags_updates_with_their_scope() {
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let scope = Scope::room("attic");
        let doc = ClipDoc::new();
        let forwarder = spawn_doc_forwarder(scope.clone(), &doc, events_tx);

        doc.add_entry(&device("d1"), "hello".into());
        match events_rx.recv().await.expect("forwarder is alive") {
            DocEvent::Update {
                scope: event_scope,
                update,
            } => {
                assert_eq!(event_scope, scope);
                assert!(update.origin.is_none(), "local add carries no origin");
            }
            DocEvent::Lagged { .. } => panic!("no lag expected"),
        }

        // Dropping the doc closes its broadcast stream, which ends the
        // forwarder — the same way leaving a room would after the abort.
        drop(doc);
        forwarder.await.expect("forwarder exits cleanly");
    }

    #[tokio::test]
    async fn background_write_reports_failures_on_reap() {
        let mut write = BackgroundWrite::idle();
        assert!(write.reap().await.is_ok(), "idle reap is Ok");

        write.start(|| anyhow::bail!("disk full"));
        let err = write.reap().await.expect_err("failure surfaces on reap");
        assert!(err.to_string().contains("disk full"));

        assert!(write.reap().await.is_ok(), "reap clears the failure");
    }

    #[tokio::test]
    async fn background_write_tracks_in_flight() {
        let mut write = BackgroundWrite::idle();
        assert!(!write.in_flight());

        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        write.start(move || {
            release_rx
                .recv()
                .map_err(|err| anyhow::anyhow!("test release channel closed: {err}"))
        });
        assert!(write.in_flight());

        release_tx.send(()).expect("worker still waiting");
        assert!(write.reap().await.is_ok());
        assert!(!write.in_flight());
    }

    fn view(id: &str, online: bool) -> PeerView {
        PeerView {
            info: PeerInfo {
                device_id: id.into(),
                name: format!("name-{id}"),
                addrs: Vec::new(),
                port: 0,
                rooms: Vec::new(),
            },
            online,
        }
    }

    #[test]
    fn lost_allowed_peer_goes_offline_lost_stranger_is_removed() {
        let mut peers = HashMap::new();
        peers.insert("friend".to_string(), view("friend", true));
        peers.insert("stranger".to_string(), view("stranger", true));

        registry_on_lost(&mut peers, "friend", true);
        registry_on_lost(&mut peers, "stranger", false);

        assert!(!peers["friend"].online, "allowed peer flips offline");
        assert!(
            !peers.contains_key("stranger"),
            "never-allowed stranger is dropped from the registry"
        );

        // Losing an unknown id is a no-op either way.
        registry_on_lost(&mut peers, "ghost", true);
        registry_on_lost(&mut peers, "ghost", false);
        assert_eq!(peers.len(), 1);
    }
}
