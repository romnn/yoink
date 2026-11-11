//! Access control for the web UI and API, which is every route except `/sync`.
//!
//! A request a browser may have made on behalf of another website is refused first; the
//! `same_origin` module explains how that is told apart.
//! After that, clients on this machine always pass.
//! Other devices pass only as [`UiAccess`] allows: never, with the secret token, or always.
//!
//! A remote browser proves the token once by opening a link carrying `?token=`.
//! The guard trades it for a cookie and redirects to the same URL without the token, so
//! the secret leaves the address bar.
//! The browser then sends the cookie on every page load, API call and WebSocket upgrade.

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use crate::same_origin;

/// Query parameter a remote link carries the token in.
const TOKEN_QUERY_KEY: &str = "token";

/// Cookie a remote browser holds the token in after opening a link.
const TOKEN_COOKIE: &str = "yoink_ui_token";

/// The token changes on every restart, which already bounds the cookie's useful life, so a
/// long lifetime only spares phones from rescanning while one run lasts.
const TOKEN_COOKIE_MAX_AGE_SECS: u32 = 365 * 24 * 60 * 60;

const LOOPBACK_ONLY_MESSAGE: &str =
    "this yoink's web UI only answers its own machine; start it with --remote-ui to allow others";

const TOKEN_REQUIRED_MESSAGE: &str =
    "open the link or scan the QR code yoink shows at startup to use its web UI from this device";

/// Who may use the web UI and API.
///
/// `/sync` is never subject to it: peers always reach that route from the network, and the
/// sync handshake validates them instead.
#[derive(Debug, Clone)]
pub enum UiAccess {
    /// Only clients on this machine.
    LoopbackOnly,
    /// Clients on this machine plus other devices on the network, as the [`RemoteAccess`]
    /// allows.
    Remote(RemoteAccess),
}

/// How other devices on the network reach and authenticate to the web UI.
#[derive(Debug, Clone)]
pub struct RemoteAccess {
    /// What a remote client must present.
    pub auth: RemoteAuth,
    /// Addresses, with the UI's port, that other devices can reach this machine on.
    ///
    /// They only build [`RemoteAccess::links`]; the guard accepts a client on any address.
    pub addresses: Vec<SocketAddr>,
}

/// What a remote client must present to use the web UI.
#[derive(Debug, Clone)]
pub enum RemoteAuth {
    /// The secret token, from a link or from the cookie that link sets.
    Token(UiToken),
    /// Nothing: every device on the network has full access.
    Open,
}

impl RemoteAccess {
    /// Returns one link per address that opens the web UI on another device.
    ///
    /// Under [`RemoteAuth::Token`] each link carries the token, so it is as secret as the
    /// token itself.
    #[must_use]
    pub fn links(&self) -> Vec<String> {
        self.addresses
            .iter()
            .map(|address| match &self.auth {
                RemoteAuth::Token(token) => {
                    format!("http://{address}/?{TOKEN_QUERY_KEY}={}", token.as_str())
                }
                RemoteAuth::Open => format!("http://{address}/"),
            })
            .collect()
    }
}

/// A random secret that lets a remote browser use the web UI.
///
/// Its [`Debug`] output is redacted so the secret cannot leak through logs.
#[derive(Clone, PartialEq, Eq)]
pub struct UiToken(String);

impl UiToken {
    /// Generates a fresh 128-bit token from the operating system's secure random source.
    ///
    /// The token is 32 lowercase hex digits, safe to embed in a URL or cookie unescaped.
    ///
    /// # Errors
    ///
    /// Returns the [`getrandom::Error`] when the operating system's random source fails.
    pub fn generate() -> Result<Self, getrandom::Error> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes)?;
        Ok(Self(format!("{:032x}", u128::from_ne_bytes(bytes))))
    }

    /// Returns the token as it appears in links and cookies.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Compares without stopping at the first mismatch, so response timing does not reveal
    /// how much of a guess was right.
    fn matches(&self, candidate: &str) -> bool {
        let expected = self.0.as_bytes();
        let candidate = candidate.as_bytes();
        expected.len() == candidate.len()
            && expected
                .iter()
                .zip(candidate)
                .fold(0u8, |difference, (left, right)| difference | (left ^ right))
                == 0
    }
}

impl fmt::Debug for UiToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UiToken(<redacted>)")
    }
}

/// Whether `addr` is a local client.
///
/// Covers `127.0.0.0/8`, `::1` and the IPv4-mapped form `::ffff:127.x.x.x`.
/// Which of the three the OS reports depends on how the browser resolved `localhost` and
/// whether the listening socket is dual-stack.
pub(crate) fn is_loopback(addr: &SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

fn client_addr(request: &Request) -> Option<SocketAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr)
}

/// Middleware admitting a request to the web UI or API as `access` allows.
///
/// A missing `ConnectInfo` extension rejects in every mode: failing closed means a wiring
/// mistake can never expose the UI or API to the network.
pub(crate) async fn guard_ui(
    State(access): State<Arc<UiAccess>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(client) = client_addr(&request) else {
        return missing_connect_info(&request);
    };
    if let Some(reason) = same_origin::rejection(&request) {
        tracing::debug!(%client, path = %request.uri().path(), reason, "rejected possibly cross-site request");
        return forbidden(reason);
    }
    if is_loopback(&client) {
        return next.run(request).await;
    }
    let remote = match access.as_ref() {
        UiAccess::LoopbackOnly => {
            tracing::debug!(%client, path = %request.uri().path(), "rejected non-loopback request");
            return forbidden(LOOPBACK_ONLY_MESSAGE);
        }
        UiAccess::Remote(remote) => remote,
    };
    let token = match &remote.auth {
        RemoteAuth::Open => return next.run(request).await,
        RemoteAuth::Token(token) => token,
    };

    // Link with the token: trade it for the cookie
    if request.method() == Method::GET
        && let Some(location) = strip_token_query(request.uri(), token)
    {
        return redirect_with_cookie(&location, token);
    }

    // Returning browser with the cookie
    if cookie_carries(request.headers(), token) {
        return next.run(request).await;
    }

    tracing::debug!(%client, path = %request.uri().path(), "rejected remote request without a valid token");
    forbidden(TOKEN_REQUIRED_MESSAGE)
}

/// Middleware admitting only clients on this machine, whatever the [`UiAccess`].
///
/// Guards what must never leave the machine, such as the remote links, which carry the
/// token.
pub(crate) async fn require_loopback(request: Request, next: Next) -> Response {
    match client_addr(&request) {
        Some(client) if is_loopback(&client) => next.run(request).await,
        Some(client) => {
            tracing::debug!(%client, path = %request.uri().path(), "rejected non-loopback request");
            StatusCode::FORBIDDEN.into_response()
        }
        None => missing_connect_info(&request),
    }
}

fn missing_connect_info(request: &Request) -> Response {
    tracing::warn!(
        path = %request.uri().path(),
        "request without connect info; rejecting (server not started via serve()?)"
    );
    StatusCode::FORBIDDEN.into_response()
}

fn forbidden(message: &'static str) -> Response {
    (StatusCode::FORBIDDEN, message).into_response()
}

/// Returns the URL to redirect to when `uri`'s query carries the valid token: the same path
/// and query with every `token` parameter removed.
///
/// Returns `None` when no `token` parameter matches, so a wrong token gets no redirect.
fn strip_token_query(uri: &Uri, token: &UiToken) -> Option<String> {
    let query = uri.query()?;
    let mut valid = false;
    let mut kept = Vec::new();
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some((TOKEN_QUERY_KEY, value)) => valid |= token.matches(value),
            _ => kept.push(pair),
        }
    }
    if !valid {
        return None;
    }
    // A path starting with `//` would make the location protocol-relative, sending the
    // browser to another host.
    let path = format!("/{}", uri.path().trim_start_matches('/'));
    if kept.is_empty() {
        Some(path)
    } else {
        Some(format!("{path}?{}", kept.join("&")))
    }
}

/// Redirects to `location` and sets the token cookie.
///
/// `SameSite=Lax` rather than `Strict`: a link opened from another site (a chat app, a QR
/// scanner's web view) is a cross-site navigation, and browsers withhold `Strict` cookies
/// on the redirect that follows it, so the first load would fail.
/// `Lax` still withholds the cookie from cross-site POSTs, fetches and WebSocket upgrades,
/// which keeps other sites from driving the API.
fn redirect_with_cookie(location: &str, token: &UiToken) -> Response {
    let cookie = format!(
        "{TOKEN_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={TOKEN_COOKIE_MAX_AGE_SECS}",
        token.as_str()
    );
    match (
        HeaderValue::try_from(location),
        HeaderValue::try_from(cookie),
    ) {
        (Ok(location), Ok(cookie)) => (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, location), (header::SET_COOKIE, cookie)],
        )
            .into_response(),
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

fn cookie_carries(headers: &HeaderMap, token: &UiToken) -> bool {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .any(|(name, value)| name == TOKEN_COOKIE && token.matches(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn token(value: &str) -> UiToken {
        UiToken(value.into())
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

    #[test]
    fn generated_tokens_are_fresh_hex() {
        let first = UiToken::generate().unwrap();
        let second = UiToken::generate().unwrap();
        assert_eq!(first.as_str().len(), 32);
        assert!(first.as_str().chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn token_debug_output_is_redacted() {
        let secret = token("0123456789abcdef0123456789abcdef");
        assert!(!format!("{secret:?}").contains(secret.as_str()));
    }

    #[test]
    fn token_matches_only_the_exact_value() {
        let secret = token("abcd");
        assert!(secret.matches("abcd"));
        // Prefixes, extensions, case changes and the empty string are all wrong guesses.
        for guess in ["abc", "abcde", "ABCD", "abce", ""] {
            assert!(!secret.matches(guess), "{guess}");
        }
    }

    #[test]
    fn links_carry_the_token_only_in_token_mode() {
        let addresses = vec![addr("192.168.1.5:7679"), addr("10.0.0.2:7679")];
        let token_links = RemoteAccess {
            auth: RemoteAuth::Token(token("abcd")),
            addresses: addresses.clone(),
        }
        .links();
        assert_eq!(
            token_links,
            [
                "http://192.168.1.5:7679/?token=abcd",
                "http://10.0.0.2:7679/?token=abcd",
            ]
        );
        let open_links = RemoteAccess {
            auth: RemoteAuth::Open,
            addresses,
        }
        .links();
        assert_eq!(
            open_links,
            ["http://192.168.1.5:7679/", "http://10.0.0.2:7679/"]
        );
    }

    #[test]
    fn strip_token_query_keeps_other_parameters() {
        let secret = token("abcd");
        let cases = [
            ("/?token=abcd", Some("/")),
            ("/r/attic?token=abcd", Some("/r/attic")),
            (
                "/api/state?scope=devices&token=abcd",
                Some("/api/state?scope=devices"),
            ),
            ("/?a=1&token=abcd&b=2", Some("/?a=1&b=2")),
            // A leading `//` must not become a protocol-relative, off-site location.
            ("//evil.example/?token=abcd", Some("/evil.example/")),
            // Wrong, missing or valueless tokens get no redirect.
            ("/?token=nope", None),
            ("/?scope=devices", None),
            ("/?token", None),
            ("/", None),
        ];
        for (uri, expected) in cases {
            let uri: Uri = uri.parse().unwrap();
            assert_eq!(
                strip_token_query(&uri, &secret).as_deref(),
                expected,
                "{uri}"
            );
        }
    }

    #[test]
    fn cookie_is_found_among_others_and_across_headers() {
        let secret = token("abcd");
        let mut headers = HeaderMap::new();
        headers.append(header::COOKIE, HeaderValue::from_static("theme=dark"));
        assert!(!cookie_carries(&headers, &secret));
        headers.append(
            header::COOKIE,
            HeaderValue::from_static("a=1; yoink_ui_token=abcd; b=2"),
        );
        assert!(cookie_carries(&headers, &secret));
    }

    #[test]
    fn cookie_with_a_wrong_value_or_name_is_rejected() {
        let secret = token("abcd");
        for cookie in ["yoink_ui_token=abce", "other_token=abcd", "yoink_ui_token="] {
            let mut headers = HeaderMap::new();
            headers.insert(header::COOKIE, HeaderValue::from_static(cookie));
            assert!(!cookie_carries(&headers, &secret), "{cookie}");
        }
    }
}
