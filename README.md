# yoink

A zero-setup shared clipboard for your local network.

Run `yoink` on two machines in the same WLAN/LAN (say, a MacBook and a Linux
box), open the local web UI on each, allow the other device — and from then on
anything you copy on one machine is available on the other. No server, no
account, no pairing dance: devices find each other via mDNS and the clipboard
history converges through a CRDT.

## Quick start

```sh
cargo install --path crates/yoink   # or: cargo run -p yoink

yoink                # serves the UI on http://localhost:7679
yoink --open         # same, and opens the browser for you
```

Start it on a second device on the same network. Each device's UI lists the
other under **Devices** — flip the **Share** toggle on *both* sides (sharing
is mutual opt-in) and the clipboards start syncing. Copy text anywhere on one
machine; it lands in the other machine's clipboard automatically (the
**auto-apply** toggle controls that). The UI always shows the current shared
clipboard front and center, with earlier entries tucked away one click below.

## Rooms

Besides your paired devices, any URL is a **room**: open
`http://localhost:7679/r/standup` (or just type `/standup` — it redirects)
and the room exists; anyone on the same network running yoink can open the
same path on their own instance and is in. No pairing, no invites — rooms
show up under "Rooms on this network" on every instance for one-click join,
and several rooms can be active at once.

Rooms are deliberately different from device pairing: your OS clipboard is
**never** captured into a room or auto-applied from one. Everything in a room
was put there on purpose, so an open-join space stays safe. Each room keeps
its own synced feed (and its own local snapshot, so rejoining restores what
you saw); the "Copy" button on any room entry still puts it straight on your
local clipboard.

### Trying it on a single machine

Two instances can run side by side — handy for kicking the tires:

```sh
yoink --port 7679 --config-dir /tmp/yoink-a --name alpha
yoink --port 7680 --config-dir /tmp/yoink-b --name beta
```

Open both UIs, allow each device on the other's page, and text shared on one
appears on the other in real time.

## How it works

- **Discovery** — every instance registers `_yoink._tcp.local.` over
  mDNS/DNS-SD and browses for others. Found peers show up in the UI within a
  few seconds.
- **Consent** — each device keeps a persisted allowlist. A sync connection is
  only established when *both* devices have allowed each other; an un-allowed
  peer is cut off during the handshake, before any clipboard data is sent.
- **Sync** — the clipboard history lives in a [yrs](https://docs.rs/yrs)
  (Y-CRDT) document. Peers exchange state vectors and diffs over a WebSocket
  and stream incremental updates afterwards, so concurrent copies on several
  machines converge without conflicts — even across temporary disconnects.
- **Local UI & API** — each instance serves a small web UI from its own port.
  Everything except the peer sync endpoint is restricted to loopback, so
  other machines on the LAN can sync with you but cannot read your history or
  flip your settings.

## Workspace layout

| crate                    | what it does                                        |
|--------------------------|-----------------------------------------------------|
| `crates/yoink-core`      | CRDT clipboard document, entries, shared types      |
| `crates/yoink-discovery` | mDNS register + browse (`mdns-sd`)                  |
| `crates/yoink-clipboard` | OS clipboard watch/write (`arboard`)                |
| `crates/yoink-sync`      | peer WebSocket sync protocol, allowlist, dialing    |
| `crates/yoink-server`    | axum HTTP server, web UI, `/sync` endpoint          |
| `crates/yoink`           | the `yoink` binary: CLI, config, app event loop     |

## Notes & limitations

- Text only for now (images are a natural next step — the CRDT layer doesn't
  care).
- Peers trust the LAN: device identity is self-asserted over mDNS, so only
  use it on networks you trust. Authenticated pairing (TOFU keys) is on the
  roadmap.
- Clipboard watching polls every 400 ms, so a copy can take up to half a
  second to appear remotely.
- State lives in your platform config dir (`~/.config/yoink` on Linux):
  `config.toml` (device id, name, allowlist, settings) and `state.bin`
  (clipboard history snapshot).
