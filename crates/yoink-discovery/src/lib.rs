//! Zero-configuration peer discovery over mDNS (DNS-SD).
//!
//! Each yoink instance registers itself as a `_yoink._tcp.local.` service and
//! simultaneously browses for other instances on the local network. The
//! service TXT record carries the device id, display name and protocol
//! version; the SRV record carries the sync port.

mod browse;

use std::net::IpAddr;
use thiserror::Error;
use tokio::sync::mpsc;

use browse::{Announcement, BrowseState};
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo};

/// DNS-SD service type yoink instances register and browse.
pub const SERVICE_TYPE: &str = "_yoink._tcp.local.";

/// Mirrors `yoink_core::PROTOCOL_VERSION`. Duplicated because yoink-discovery
/// deliberately has no dependency on yoink-core; the authoritative version
/// check happens during sync handshake, this TXT value is informational.
const PROTOCOL_VERSION: u32 = 2;

/// Budget for the TXT `rooms` value. TXT records are size-constrained
/// (individual strings cap at 255 bytes), so advertise only as many room
/// names as fit and log the rest; membership still works for un-advertised
/// rooms when the peer learns of them another way (e.g. the user types the
/// room URL).
const ROOMS_TXT_BUDGET: usize = 200;

/// How many translated events may queue up before the bridge task awaits the
/// consumer. Discovery events are rare, so a small buffer suffices.
const EVENT_CHANNEL_CAPACITY: usize = 64;

/// A peer yoink instance discovered on the local network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub device_id: String,
    pub name: String,
    /// All addresses the peer's mDNS announcement resolved to.
    pub addrs: Vec<IpAddr>,
    pub port: u16,
    /// Room names the peer advertises (sorted, deduplicated). Drives the
    /// "rooms on this network" UI and room-scope dialing.
    pub rooms: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// A peer appeared or its announcement changed (re-resolved).
    Found(PeerInfo),
    /// A peer's announcement disappeared from the network.
    Lost { device_id: String },
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("mdns error: {0}")]
    Mdns(#[from] mdns_sd::Error),
}

/// Handle to the running mDNS daemon. Dropping it without calling
/// [`Discovery::shutdown`] leaves the OS to expire the announcement on its
/// own, so call shutdown on graceful exit to unregister immediately.
pub struct Discovery {
    daemon: ServiceDaemon,
    /// Fullname of our own registration, needed to unregister on shutdown.
    fullname: String,
    /// Everything needed to rebuild the ServiceInfo when the advertised
    /// rooms change (mdns-sd updates an announcement by re-registering the
    /// same fullname).
    registration: Registration,
    /// Bridge task forwarding daemon events into the tokio channel. It exits
    /// on its own once the daemon shuts down (the flume sender disconnects)
    /// or the consumer drops the receiver.
    _bridge: tokio::task::JoinHandle<()>,
}

struct Registration {
    device_id: String,
    device_name: String,
    instance_name: String,
    host_name: String,
    port: u16,
}

impl Registration {
    fn service_info(&self, rooms: &[String]) -> Result<ServiceInfo, mdns_sd::Error> {
        let proto = PROTOCOL_VERSION.to_string();
        let rooms = encode_rooms(rooms);
        let txt_props = [
            ("id", self.device_id.as_str()),
            ("name", self.device_name.as_str()),
            ("proto", proto.as_str()),
            ("rooms", rooms.as_str()),
        ];
        // Empty ip string + addr_auto: the daemon fills in (and tracks) the
        // addresses of all usable interfaces itself.
        Ok(ServiceInfo::new(
            SERVICE_TYPE,
            &self.instance_name,
            &self.host_name,
            "",
            self.port,
            &txt_props[..],
        )?
        .enable_addr_auto())
    }
}

impl Discovery {
    /// Register ourselves and start browsing for peers.
    ///
    /// Must be called from within a tokio runtime (it spawns the task that
    /// bridges daemon events into the returned channel).
    ///
    /// Implementation notes:
    /// - Instance name must be unique per device: use `"{device_name}-{first
    ///    8 chars of device_id}"` to avoid collisions between devices with
    ///   the same hostname.
    /// - TXT properties: `id` = device id, `name` = display name, `proto` =
    ///   protocol version.
    /// - Use `ServiceInfo::enable_addr_auto()` so addresses track interface
    ///   changes.
    /// - Filter out our own announcement (TXT `id` == `device_id`).
    /// - Map mDNS fullname -> device id so `ServiceRemoved` can be translated
    ///   into `DiscoveryEvent::Lost`.
    /// - Bridge the mdns-sd flume receiver into the tokio channel with a
    ///   spawned task using `recv_async`.
    pub fn start(
        device_id: &str,
        device_name: &str,
        port: u16,
        rooms: &[String],
    ) -> Result<(Self, mpsc::Receiver<DiscoveryEvent>), DiscoveryError> {
        let daemon = ServiceDaemon::new()?;

        let short_id: String = device_id.chars().take(8).collect();
        let instance_name = format!("{device_name}-{short_id}");
        // mdns-sd requires a hostname ending in ".local."; derive a unique,
        // DNS-safe one from the instance name so two instances on one machine
        // never share (or conflict over) host records.
        let host_name = format!("{}.local.", dns_safe_label(&instance_name));
        let registration = Registration {
            device_id: device_id.to_string(),
            device_name: device_name.to_string(),
            instance_name,
            host_name,
            port,
        };
        let service = registration.service_info(rooms)?;
        let fullname = service.get_fullname().to_string();
        daemon.register(service)?;

        let browse_rx = daemon.browse(SERVICE_TYPE)?;
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let bridge = tokio::spawn(bridge_events(browse_rx, tx, device_id.to_string()));

        tracing::info!(%fullname, port, "registered mDNS service and browsing for peers");
        Ok((
            Self {
                daemon,
                fullname,
                registration,
                _bridge: bridge,
            },
            rx,
        ))
    }

    /// Re-announce with an updated room list (joining/leaving a room).
    pub fn set_rooms(&self, rooms: &[String]) {
        match self.registration.service_info(rooms) {
            // Re-registering the same fullname updates the live announcement.
            Ok(service) => {
                if let Err(error) = self.daemon.register(service) {
                    tracing::warn!(%error, "failed to re-announce updated room list");
                }
            }
            Err(error) => tracing::warn!(%error, "failed to build updated announcement"),
        }
    }

    /// Unregister our announcement and stop the daemon. Blocks briefly (up
    /// to ~500ms) for the unregister confirmation.
    pub fn shutdown(&self) {
        // Wait for the daemon to confirm the unregister before requesting its
        // exit: the confirmation is sent only after the goodbye packets went
        // out, and without it the process could exit with the unregister
        // still queued — leaving peers to wait out the TTL.
        match self.daemon.unregister(&self.fullname) {
            Ok(status) => match status.recv_timeout(std::time::Duration::from_millis(500)) {
                Ok(status) => tracing::debug!(?status, "mDNS service unregistered"),
                Err(error) => tracing::warn!(
                    %error,
                    fullname = %self.fullname,
                    "no mDNS unregister confirmation; goodbye packets may not have been sent"
                ),
            },
            Err(error) => {
                tracing::warn!(%error, fullname = %self.fullname, "failed to unregister mDNS service");
            }
        }
        if let Err(error) = self.daemon.shutdown() {
            tracing::debug!(%error, "mDNS daemon shutdown request failed (already stopped?)");
        }
    }
}

/// Forwards daemon browse events into the consumer channel, translating them
/// through [`BrowseState`]. Ends when the daemon stops (flume disconnect) or
/// the consumer drops the receiver.
async fn bridge_events(
    events: mdns_sd::Receiver<ServiceEvent>,
    tx: mpsc::Sender<DiscoveryEvent>,
    self_device_id: String,
) {
    let mut state = BrowseState::new(self_device_id);
    while let Ok(event) = events.recv_async().await {
        let translated = match event {
            ServiceEvent::ServiceResolved(resolved) => state.on_resolved(announcement(&resolved)),
            ServiceEvent::ServiceRemoved(_ty, fullname) => state.on_removed(&fullname),
            other => {
                tracing::trace!(?other, "ignoring mDNS browse event");
                None
            }
        };
        if let Some(event) = translated {
            tracing::debug!(?event, "discovery event");
            if tx.send(event).await.is_err() {
                break;
            }
        }
    }
    tracing::debug!("mDNS event bridge task exiting");
}

/// Comma-join room names (they are sanitized, so commas cannot occur in
/// them), newest-name-last, dropping names past the TXT budget.
fn encode_rooms(rooms: &[String]) -> String {
    let mut names: Vec<&str> = rooms.iter().map(String::as_str).collect();
    names.sort_unstable();
    names.dedup();
    let mut encoded = String::new();
    for name in names {
        let extra = name.len() + usize::from(!encoded.is_empty());
        if encoded.len() + extra > ROOMS_TXT_BUDGET {
            tracing::warn!(
                dropped = name,
                "room list exceeds mDNS TXT budget; not advertising remaining rooms"
            );
            break;
        }
        if !encoded.is_empty() {
            encoded.push(',');
        }
        encoded.push_str(name);
    }
    encoded
}

fn announcement(resolved: &ResolvedService) -> Announcement {
    Announcement {
        fullname: resolved.fullname.clone(),
        txt_rooms: resolved
            .txt_properties
            .get_property_val_str("rooms")
            .map(str::to_string),
        txt_id: resolved
            .txt_properties
            .get_property_val_str("id")
            .map(str::to_string),
        txt_name: resolved
            .txt_properties
            .get_property_val_str("name")
            .map(str::to_string),
        addrs: resolved
            .addresses
            .iter()
            .map(mdns_sd::ScopedIp::to_ip_addr)
            .collect(),
        port: resolved.port,
    }
}

/// Reduces an arbitrary display string to a hostname label: lowercase
/// alphanumerics and hyphens, never empty.
fn dns_safe_label(name: &str) -> String {
    let label: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let label = label.trim_matches('-');
    if label.is_empty() {
        "yoink".to_string()
    } else {
        label.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::dns_safe_label;

    #[test]
    fn dns_safe_label_sanitizes() {
        assert_eq!(
            dns_safe_label("Roman's MacBook-a1b2c3d4"),
            "roman-s-macbook-a1b2c3d4"
        );
        assert_eq!(dns_safe_label("---"), "yoink");
        assert_eq!(dns_safe_label(""), "yoink");
        assert_eq!(dns_safe_label(".plain."), "plain");
    }
}
