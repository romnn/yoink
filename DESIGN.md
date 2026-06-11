# yoink design notes

## Two sharing scopes

yoink has (or is architected toward) two kinds of shared space, both backed by
the same CRDT machinery:

| | **Devices** (personal) | **Rooms** (`/myroom`) |
|---|---|---|
| identity | your own machines | a name on this network |
| consent | mutual allowlist, persisted | open join — visiting the URL *is* creating/joining |
| OS clipboard | captured + auto-applied | never auto-captured; sharing into a room is always deliberate |
| lifetime | permanent | lives while someone on the network holds it (snapshotted locally by members) |
| count | exactly one | any number, active concurrently |

The asymmetry is intentional. Your clipboard is sensitive and automatic, so it
moves only between machines you explicitly paired. A room is a lightweight,
named meeting point — zero ceremony to open one (`http://localhost:7679/r/standup`),
powerful because several can be active at once, and never annoying because
nothing enters a room unless someone deliberately puts it there.

## How rooms map onto the architecture

The architecture was shaped so rooms are an extension, not a rework:

- **Docs**: `ClipDoc` is already self-contained. Rooms = a registry
  `scope -> Arc<ClipDoc>` where scope is `devices` or `room:{name}`. Personal
  state stays in `state.bin`; each joined room snapshots to `rooms/{name}.bin`.
- **Sync**: the HELLO frame gains a `scope` field (protocol v2). One WebSocket
  connection per `(peer, scope)`; everything else — handshake, SYNC_STEP_1/2,
  origin-tagged fan-out, dial rule, backoff — is reused unchanged. The
  allowlist check applies only to the `devices` scope; a `room:{name}` HELLO
  is accepted iff we currently have that room open.
- **Discovery**: instances advertise joined room names in their mDNS TXT
  record (capped, sanitized). The UI aggregates peers' advertisements into
  "rooms on this network" so joining is one click, not just typed URLs.
- **Server/UI**: `/` stays the personal clipboard. `/r/{name}` serves a room
  view (same entry-feed UI, share box scoped to the room); unreserved bare
  paths like `/myroom` redirect to `/r/myroom` so typing a room URL just
  works. Room routes stay loopback-guarded like everything except `/sync`.
- **App loop**: commands carry an optional scope; clipboard capture/auto-apply
  remain hard-wired to the `devices` scope by design.

## History presentation

The CRDT array keeps a capped history (200 entries) regardless of UI — that
is what makes sync robust across disconnects. The question is presentation:
a flat "History" log read as the centerpiece of the devices view, which felt
heavier than the product is. Decision: the devices view leads with a hero
"Current clipboard" card (the answer to "what would paste right now?") and
collapses everything older into a quiet "Earlier" disclosure. Rooms keep a
feed layout — a room is its feed; dropping several things into it during a
session is the use case.

## Threat model (v1)

The LAN is trusted. Device identity is self-asserted over mDNS, so a hostile
network can impersonate devices or join rooms. Mitigations on the roadmap:
TOFU device keys for pairing, optional room codes that derive an HMAC for the
HELLO and an encryption key for frames. The sync handshake already enforces
"no document data before validation", which is where those checks will slot in.
