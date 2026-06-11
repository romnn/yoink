//! Loopback-only guard for every route except `/sync`.

use axum::extract::{ConnectInfo, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::net::{IpAddr, SocketAddr};

/// Whether `addr` is a local client. Covers `127.0.0.0/8`, `::1` and the
/// IPv4-mapped form `::ffff:127.x.x.x` — which of the three the OS reports
/// depends on how the browser resolved `localhost` and whether the listening
/// socket is dual-stack.
pub(crate) fn is_loopback(addr: &SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

/// Middleware rejecting non-loopback clients with 403. A missing
/// `ConnectInfo` extension also rejects: failing closed means a wiring
/// mistake can never expose the UI or API to the LAN.
pub(crate) async fn require_loopback(req: Request, next: Next) -> Response {
    let addr = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0);
    match addr {
        Some(addr) if is_loopback(&addr) => next.run(req).await,
        Some(addr) => {
            tracing::debug!(%addr, path = %req.uri().path(), "rejected non-loopback request");
            StatusCode::FORBIDDEN.into_response()
        }
        None => {
            tracing::warn!(
                path = %req.uri().path(),
                "request without connect info; rejecting (server not started via serve()?)"
            );
            StatusCode::FORBIDDEN.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn loopback_addresses_are_accepted() {
        assert!(is_loopback(&addr("127.0.0.1:1234")));
        assert!(is_loopback(&addr("127.8.4.3:80")));
        assert!(is_loopback(&addr("127.255.255.254:65535")));
        assert!(is_loopback(&addr("[::1]:9090")));
        assert!(is_loopback(&addr("[::ffff:127.0.0.1]:9090")));
        assert!(is_loopback(&addr("[::ffff:127.42.0.7]:1")));
    }

    #[test]
    fn non_loopback_addresses_are_rejected() {
        assert!(!is_loopback(&addr("192.168.1.5:1234")));
        assert!(!is_loopback(&addr("10.0.0.1:80")));
        assert!(!is_loopback(&addr("128.0.0.1:80")));
        assert!(!is_loopback(&addr("[fe80::1]:9090")));
        assert!(!is_loopback(&addr("[::2]:9090")));
        assert!(!is_loopback(&addr("[::ffff:192.168.1.5]:9090")));
        assert!(!is_loopback(&addr("[2001:db8::1]:443")));
    }
}
