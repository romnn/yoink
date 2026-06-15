# yoink

A zero-setup shared clipboard for your local network.

Run `yoink` on two machines in the same WLAN/LAN (say, a MacBook and a Linux
box), open the local web UI on each, and share text between them. No server,
no account, no pairing dance by default: devices find each other via mDNS,
trust the LAN, and the in-memory shared history converges through a CRDT.

## Quick start

```sh
cargo install --path crates/yoink   # or: cargo run -p yoink

yoink                # serves the UI on http://localhost:7679
yoink --no-open      # same, but does not open the browser for you
```

Start it on a second device on the same network. Each device's UI lists the
other under **Devices**. By default, yoink trusts devices on your LAN and uses
manual sharing: paste text into the box and click **Share**. Use `--mode
auto-share` to automatically share copied text, or `--mode mirror` for a full
two-way clipboard mirror. Use **Block** for a device you do not want to sync
with, or start with `--require-pairing` to switch to an explicit pair/unpair
allowlist.

## Rooms

Besides the personal devices view, any URL is a **room**: open
`http://localhost:7679/r/standup` (or just type `/standup` — it redirects)
and the room exists; anyone on the same network running yoink can open the
same path on their own instance and is in. No pairing, no invites — rooms
show up under "Rooms on this network" on every instance for one-click join,
and several rooms can be active at once.

Rooms are deliberately different from device sharing: your OS clipboard is
**never** captured into a room or auto-applied from one. Everything in a room
was put there on purpose, so an open-join space stays safe. Each room keeps
its own in-memory feed while yoink is running; restarting starts clean. The
**Copy** button on any room entry still puts it straight on your local
clipboard.

### Trying it on a single machine

Two instances can run side by side — handy for kicking the tires:

```sh
yoink --port 7679 --config-dir /tmp/yoink-a --name alpha
yoink --port 7680 --config-dir /tmp/yoink-b --name beta
```

Open both UIs and text shared on one appears on the other in real time. Add
`--require-pairing` to both commands if you want to exercise the pair/unpair
flow locally.

## How it works

- **Discovery** — every instance registers `_yoink._tcp.local.` over
  mDNS/DNS-SD and browses for others. Found peers show up in the UI within a
  few seconds.
- **Trust model** — by default, every discovered device on the LAN is trusted
  unless you explicitly block it. `--require-pairing` flips the personal
  clipboard to a persisted allowlist where both devices must pair each other.
  `--untrusted` is a conservative preset for networks you do not control:
  strict pairing plus manual share mode.
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
| `crates/yoink-sync`      | peer WebSocket sync protocol, trust model, dialing  |
| `crates/yoink-server`    | axum HTTP server, web UI, `/sync` endpoint          |
| `crates/yoink`           | the `yoink` binary: CLI, config, app event loop     |

## Notes & limitations

- Text only for now (images are a natural next step — the CRDT layer doesn't
  care).
- Peers trust the LAN by default: device identity is self-asserted over mDNS,
  so only use the default mode on networks you trust. Authenticated device
  identities (TOFU keys) are on the roadmap.
- Clipboard watching polls every 400 ms in `auto-share` and `mirror` modes,
  so a copy can take up to half a second to appear remotely.
- State lives in your platform config dir (`~/.config/yoink` on Linux):
  `config.toml` (device id, name, paired devices, blocked devices, and joined
  rooms). Clipboard history is not persisted.
