//! Defenses against other websites driving the web UI through the user's browser.
//!
//! The access guard trusts a client by its address, and a browser shares its address with
//! every site it has open.
//! Two attacks follow, and this module refuses the requests each one needs.
//!
//! - **Cross-site requests.** A page on another site can POST to the API or open the UI
//!   WebSocket on `localhost`.
//!   WebSocket connections are exempt from the same-origin policy, so that page could even
//!   read the history.
//!   Browsers name the requesting site in `Origin`, so a request whose `Origin` differs
//!   from its `Host` is refused.
//! - **DNS rebinding.** A page whose own domain later resolves to `127.0.0.1` or a LAN
//!   address is same-origin with itself, so it passes the `Origin` check and can read
//!   responses.
//!   Its requests still carry that public domain in `Host`, so only hosts a public DNS
//!   name cannot be are accepted: IP literals, `localhost` names, single-label names and
//!   mDNS `.local` names.

use axum::extract::Request;
use axum::http::uri::Authority;
use axum::http::{HeaderValue, header};
use std::net::{Ipv4Addr, Ipv6Addr};

const MISSING_HOST_MESSAGE: &str = "request has no Host header";

const UNTRUSTED_HOST_MESSAGE: &str =
    "open yoink by IP address, localhost or a .local name, not through another domain";

const CROSS_ORIGIN_MESSAGE: &str = "requests from other websites are not allowed";

/// Returns why `request` must be refused as possibly made on behalf of another website,
/// or `None` when it is safe to serve.
pub(crate) fn rejection(request: &Request) -> Option<&'static str> {
    let Some(host) = request_host(request) else {
        return Some(MISSING_HOST_MESSAGE);
    };
    if !is_trusted_host(host) {
        return Some(UNTRUSTED_HOST_MESSAGE);
    }
    // Same-origin page loads and fetches may omit `Origin`; cross-site POSTs and every
    // WebSocket upgrade carry it.
    match request.headers().get(header::ORIGIN) {
        Some(origin) if !origin_matches_host(origin, host) => Some(CROSS_ORIGIN_MESSAGE),
        _ => None,
    }
}

/// The authority the client addressed, with any port: the `Host` header, or the request
/// target's authority for HTTP/2 and absolute-form requests that carry no `Host`.
fn request_host(request: &Request) -> Option<&str> {
    match request.headers().get(header::HOST) {
        Some(host) => host.to_str().ok(),
        None => request.uri().authority().map(Authority::as_str),
    }
}

/// Whether `authority` (`host` or `host:port`) names this machine in a way only the
/// user's own network can answer, never a public DNS name.
fn is_trusted_host(authority: &str) -> bool {
    let host = strip_port(authority);
    if let Some(literal) = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        return literal.parse::<Ipv6Addr>().is_ok();
    }
    if host.parse::<Ipv4Addr>().is_ok() {
        return true;
    }
    let name = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    let is_dns_name = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.');
    // `localhost` and its subdomains resolve inside the browser, `.local` over mDNS, and a
    // single label through the local search domain: no public DNS answers any of them.
    let top_label = name.rsplit('.').next().unwrap_or_default();
    is_dns_name && (!name.contains('.') || top_label == "localhost" || top_label == "local")
}

fn strip_port(authority: &str) -> &str {
    match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => authority,
    }
}

/// Whether `origin` (`scheme://host[:port]`) names the same authority the request was sent
/// to.
///
/// Both omit the scheme's default port, so they compare as plain strings.
/// The opaque origin `null` (sandboxed frames, `file:` pages) never matches.
fn origin_matches_host(origin: &HeaderValue, host: &str) -> bool {
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        .is_some_and(|authority| authority.eq_ignore_ascii_case(host))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    fn request(host: Option<&str>, origin: Option<&str>) -> Request {
        let mut builder = Request::builder().uri("/api/state");
        if let Some(host) = host {
            builder = builder.header(header::HOST, host);
        }
        if let Some(origin) = origin {
            builder = builder.header(header::ORIGIN, origin);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[test]
    fn local_and_lan_hosts_are_trusted() {
        for host in [
            "localhost",
            "localhost:7679",
            "LocalHost.:7679",
            "app.localhost:7679",
            "127.0.0.1:7679",
            "192.168.1.5:7679",
            "[::1]:7679",
            "[fe80::1]",
            "laptop:7679",
            "laptop.local:7679",
        ] {
            assert!(is_trusted_host(host), "{host}");
        }
    }

    /// A public name in `Host` is the signature of DNS rebinding.
    #[test]
    fn public_names_and_garbage_are_untrusted() {
        for host in [
            "evil.example:7679",
            "evil.example",
            "localhost.evil.example",
            "local.evil.example",
            "[not-an-ip]:7679",
            "",
            ":7679",
            "a b",
        ] {
            assert!(!is_trusted_host(host), "{host:?}");
        }
    }

    #[test]
    fn same_origin_and_origin_free_requests_pass() {
        assert_eq!(rejection(&request(Some("localhost:7679"), None)), None);
        assert_eq!(
            rejection(&request(
                Some("localhost:7679"),
                Some("http://localhost:7679")
            )),
            None
        );
        assert_eq!(
            rejection(&request(
                Some("192.168.1.5:7679"),
                Some("http://192.168.1.5:7679")
            )),
            None
        );
    }

    #[test]
    fn other_origins_are_refused() {
        for origin in [
            "https://evil.example",
            "http://localhost:7680",
            "http://127.0.0.1:7679",
            "null",
        ] {
            assert_eq!(
                rejection(&request(Some("localhost:7679"), Some(origin))),
                Some(CROSS_ORIGIN_MESSAGE),
                "{origin}"
            );
        }
    }

    /// A rebinding page is same-origin with itself, so only its `Host` gives it away.
    #[test]
    fn rebinding_requests_are_refused_even_when_same_origin() {
        assert_eq!(
            rejection(&request(
                Some("evil.example:7679"),
                Some("http://evil.example:7679")
            )),
            Some(UNTRUSTED_HOST_MESSAGE)
        );
    }

    #[test]
    fn requests_without_any_host_are_refused() {
        assert_eq!(rejection(&request(None, None)), Some(MISSING_HOST_MESSAGE));
    }
}
