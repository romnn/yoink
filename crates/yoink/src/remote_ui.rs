//! `--remote-ui`: serving the web UI to other devices on the network.

use anyhow::Context as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use yoink_server::{RemoteAccess, RemoteAuth, UiAccess, UiToken};

/// How other devices on the network authenticate to the web UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum RemoteUi {
    /// Require the secret link (or QR code) yoink shows at startup.
    Token,
    /// Let anyone on the network in, with no secret.
    Open,
}

/// Builds the server's access policy for `mode`, generating a fresh token for
/// [`RemoteUi::Token`].
///
/// `port` is the UI's bound port, which the remote links point at.
pub(crate) fn access(mode: Option<RemoteUi>, port: u16) -> anyhow::Result<UiAccess> {
    let Some(mode) = mode else {
        return Ok(UiAccess::LoopbackOnly);
    };
    let auth = match mode {
        RemoteUi::Token => RemoteAuth::Token(
            UiToken::generate().context("failed to generate the remote UI token")?,
        ),
        RemoteUi::Open => RemoteAuth::Open,
    };
    let addresses = lan_ipv4_addrs()
        .into_iter()
        .map(|ip| SocketAddr::from((ip, port)))
        .collect();
    Ok(UiAccess::Remote(RemoteAccess { auth, addresses }))
}

/// This machine's IPv4 addresses that other devices are likely to reach, best first.
///
/// IPv6 is left out: link-local addresses need a zone id that browsers reject in URLs, and
/// a LAN that has IPv6 has IPv4 too in practice.
fn lan_ipv4_addrs() -> Vec<Ipv4Addr> {
    let interfaces = match if_addrs::get_if_addrs() {
        Ok(interfaces) => interfaces,
        Err(err) => {
            tracing::warn!(error = %err, "failed to list network interfaces for the remote UI links");
            return Vec::new();
        }
    };
    let mut addrs: Vec<Ipv4Addr> = interfaces
        .iter()
        .filter(|interface| interface.is_oper_up() && !interface.is_loopback())
        .filter_map(|interface| match interface.ip() {
            IpAddr::V4(ip) if !ip.is_link_local() => Some(ip),
            _ => None,
        })
        .collect();
    let primary = default_route_ipv4();
    addrs.sort_by_key(|ip| (Some(*ip) != primary, lan_rank(*ip), *ip));
    addrs.dedup();
    addrs
}

/// The address the OS sends traffic for other networks from, which on a home or office
/// network is the machine's address on that network rather than a VM or container bridge.
///
/// Connecting a UDP socket only selects a route; nothing is sent.
/// The target is in `192.0.2.0/24` (TEST-NET-1, RFC 5737), which is never local, so the
/// default route answers.
/// `None` when there is no default route, such as on an offline network.
fn default_route_ipv4() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}

/// Orders addresses after the default-route one, so the address a phone on the same
/// Wi-Fi most likely reaches comes first; the terminal QR code shows only that one.
///
/// Home routers hand out `192.168/16` and offices `10/8`, while `172.16/12` is mostly
/// container bridges, and anything else (VPN ranges, public addresses) is rarely what a
/// nearby phone can reach.
fn lan_rank(ip: Ipv4Addr) -> u8 {
    match ip.octets() {
        [192, 168, ..] => 0,
        [10, ..] => 1,
        [172, second, ..] if (16..32).contains(&second) => 2,
        _ => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::lan_rank;
    use std::net::Ipv4Addr;

    #[test]
    fn home_networks_rank_before_container_bridges_and_vpns() {
        let mut addrs = [
            Ipv4Addr::new(100, 101, 102, 103),
            Ipv4Addr::new(172, 17, 0, 1),
            Ipv4Addr::new(10, 0, 0, 7),
            Ipv4Addr::new(192, 168, 1, 5),
        ];
        addrs.sort_by_key(|ip| (lan_rank(*ip), *ip));
        assert_eq!(
            addrs,
            [
                Ipv4Addr::new(192, 168, 1, 5),
                Ipv4Addr::new(10, 0, 0, 7),
                Ipv4Addr::new(172, 17, 0, 1),
                Ipv4Addr::new(100, 101, 102, 103),
            ]
        );
    }
}
