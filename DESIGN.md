# yoink design notes

## Two sharing scopes

yoink has (or is architected toward) two kinds of shared space, both backed by
the same CRDT machinery:

| | **Devices** (personal) | **Rooms** (`/myroom`) |
|---|---|---|
| identity | your own machines | a name on this network |
| consent | LAN-trusted by default; optional strict pairing | open join — visiting the URL *is* creating/joining |
| OS clipboard | manual by default; `auto-share` captures, `mirror` captures + applies | never auto-captured; sharing into a room is always deliberate |
| lifetime | in-memory history, config persists device lists | in-memory history, joined room names persist |
| count | exactly one | any number, active concurrently |

The asymmetry is intentional. The personal scope is for devices on a network
you trust: it connects automatically by default, and you explicitly block
devices you do not want. On networks you do not control, `--require-pairing`
or `--untrusted` switches back to an explicit allowlist. A room is a
lightweight, named meeting point — zero ceremony to open one
(`http://localhost:7679/r/standup`), powerful because several can be active
at once, and never annoying because nothing enters a room unless someone
deliberately puts it there.

## How rooms map onto the architecture

The architecture was shaped so rooms are an extension, not a rework:

- **Docs**: `ClipDoc` is already self-contained. Rooms = a registry
  `scope -> Arc<ClipDoc>` where scope is `devices` or `room:{name}`. Docs are
  in-memory only; restart means clean history for every scope.
- **Sync**: the HELLO frame gains a `scope` field (protocol v2). One WebSocket
  connection per `(peer, scope)`; everything else — handshake, SYNC_STEP_1/2,
  origin-tagged fan-out, dial rule, backoff — is reused unchanged. The
  trust model applies only to the `devices` scope; a `room:{name}` HELLO is
  accepted iff we currently have that room open.
- **Discovery**: instances advertise joined room names in their mDNS TXT
  record (capped, sanitized). The UI aggregates peers' advertisements into
  "rooms on this network" so joining is one click, not just typed URLs.
  `--untrusted` suppresses our own room-name advertisements while still
  browsing peers' announcements.
- **Server/UI**: `/` stays the personal clipboard. `/r/{name}` serves a room
  view (same entry-feed UI, share box scoped to the room); unreserved bare
  paths like `/myroom` redirect to `/r/myroom` so typing a room URL just
  works. Room routes stay loopback-guarded like everything except `/sync`.
- **App loop**: commands carry an optional scope; clipboard capture and
  auto-apply remain hard-wired to the `devices` scope and are gated by
  `ShareMode`.

## History presentation

The CRDT array keeps a capped in-memory history (200 entries) while the
process runs — that is what makes sync robust across disconnects during a
session. The UI presents a flat **Shared** list for devices and rooms. There
is no "current clipboard" card anymore: in manual and auto-share modes,
entries are things you may copy deliberately; only `mirror` writes received
entries to the OS clipboard automatically.

## Threat model (v1)

The LAN is trusted by default. Device identity is self-asserted over mDNS, so
a hostile network can impersonate devices or join rooms. `--untrusted` opts
into the safer preset available today: strict pairing, manual share mode, and
no mDNS advertisement of our joined room names.
Mitigations on the roadmap: TOFU device keys for pairing, optional room codes
that derive an HMAC for the HELLO and an encryption key for frames. The sync
handshake already enforces "no document data before validation", which is
where those checks will slot in.
