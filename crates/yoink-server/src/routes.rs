//! Router assembly and the (deliberately thin) HTTP/WebSocket handlers.

use crate::ServerCtx;
use crate::loopback::require_loopback;
use crate::state_json::build_state;
use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::middleware;
use axum::response::{Html, IntoResponse, Json, Redirect, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use yoink_core::{AppCommand, Scope, sanitize_room_name};

const INDEX_HTML: &str = include_str!("../assets/index.html");

/// Vendored copy of idiomorph (bigskysoftware/idiomorph, 0BSD license),
/// served at `/assets/idiomorph.js` so the UI stays free of external
/// resources.
const IDIOMORPH_JS: &str = include_str!("../assets/idiomorph.min.js");

/// Single-segment paths that must never be treated as a room shorthand, so
/// `/standup` can redirect to `/r/standup` without ever shadowing a real
/// route (or a future one — `favicon.ico` is requested by browsers on spec).
const RESERVED_PATHS: &[&str] = &["api", "ws", "sync", "r", "assets", "favicon.ico"];

/// Cap for incoming WebSocket messages and frames on both upgrade routes.
/// `/sync` is reachable from the LAN before any handshake validation, so
/// without a cap a stranger could make us buffer arbitrarily large frames;
/// 8 MiB comfortably fits a full-history `SYNC_STEP_2` while bounding pre-auth
/// memory use.
const MAX_WS_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn router(ctx: ServerCtx) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/r/{name}", get(room_page))
        .route("/assets/idiomorph.js", get(idiomorph_js))
        .route("/api/state", get(api_state))
        .route("/api/command", post(api_command))
        .route("/ws/ui", get(ws_ui))
        // Static segments win over captures in axum's router, so this only
        // sees paths no explicit route claims.
        .route("/{word}", get(bare_room_redirect))
        .layer(middleware::from_fn(require_loopback))
        // Added after the guard layer on purpose: `/sync` must stay reachable
        // from the LAN; the sync handshake itself enforces the allowlist.
        .route("/sync", get(sync))
        .with_state(ctx)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// `/r/{name}`: the same embedded page (its JS reads `location.pathname`),
/// but only under the canonical name — anything else redirects so every room
/// has exactly one URL and the client-side scope parse stays trivial.
async fn room_page(Path(name): Path<String>) -> Response {
    match sanitize_room_name(&name) {
        Some(clean) if clean == name => Html(INDEX_HTML).into_response(),
        Some(clean) => Redirect::temporary(&format!("/r/{clean}")).into_response(),
        None => Redirect::temporary("/").into_response(),
    }
}

/// `/{word}` typed into the address bar is almost certainly meant as a room
/// name; send it to the canonical room URL.
async fn bare_room_redirect(Path(word): Path<String>) -> Response {
    if RESERVED_PATHS.contains(&word.as_str()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match sanitize_room_name(&word) {
        Some(clean) => Redirect::temporary(&format!("/r/{clean}")).into_response(),
        None => Redirect::temporary("/").into_response(),
    }
}

async fn idiomorph_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            // Embedded in the binary, so the content can only change with a
            // rebuild; a long immutable cache is safe and keeps reloads snappy.
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        IDIOMORPH_JS,
    )
}

#[derive(Deserialize)]
struct ScopeQuery {
    scope: Option<String>,
}

/// Missing means the devices view; anything present must parse as a strict
/// [`Scope`] string or the request is a 400 — silently falling back to the
/// devices scope could leak personal history to a page that asked for a room.
fn parse_scope(query: &ScopeQuery) -> Result<Scope, (StatusCode, String)> {
    match &query.scope {
        None => Ok(Scope::Devices),
        Some(raw) => raw
            .parse()
            .map_err(|err| (StatusCode::BAD_REQUEST, format!("{err}"))),
    }
}

async fn api_state(State(ctx): State<ServerCtx>, Query(query): Query<ScopeQuery>) -> Response {
    match parse_scope(&query) {
        Ok(scope) => Json(build_state(&ctx, &scope)).into_response(),
        Err(rejection) => rejection.into_response(),
    }
}

/// Parses the body by hand instead of using the `Json` extractor so that any
/// malformed input (wrong content type, unknown `cmd`, syntax error) yields a
/// uniform 400 instead of a mix of 400/415/422.
async fn api_command(State(ctx): State<ServerCtx>, body: Bytes) -> Response {
    let cmd: AppCommand = match serde_json::from_slice(&body) {
        Ok(cmd) => cmd,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, format!("invalid command: {err}")).into_response();
        }
    };
    if ctx.commands.send(cmd).await.is_err() {
        tracing::error!("app command channel closed; is the app loop gone?");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    StatusCode::ACCEPTED.into_response()
}

async fn ws_ui(
    State(ctx): State<ServerCtx>,
    Query(query): Query<ScopeQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let scope = match parse_scope(&query) {
        Ok(scope) => scope,
        Err(rejection) => return rejection.into_response(),
    };
    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .max_frame_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| ui_socket(socket, ctx, scope))
}

async fn ui_socket(mut socket: WebSocket, ctx: ServerCtx, scope: Scope) {
    // Subscribe before the first snapshot so a change racing the initial
    // send still triggers a (redundant but harmless) refresh.
    let mut notify = ctx.notify.subscribe();
    if send_state(&mut socket, &ctx, &scope).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            changed = notify.recv() => match changed {
                // Lag is harmless: state is rebuilt from scratch for every
                // push, so one fresh send catches the receiver up.
                Ok(()) | Err(RecvError::Lagged(_)) => {
                    if send_state(&mut socket, &ctx, &scope).await.is_err() {
                        break;
                    }
                }
                // Notify sender gone means the app loop is shutting down.
                Err(RecvError::Closed) => break,
            },
            // Drain incoming frames so client closes are noticed promptly;
            // the UI never sends anything meaningful over this socket.
            msg = socket.recv() => match msg {
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
        }
    }
}

async fn send_state(
    socket: &mut WebSocket,
    ctx: &ServerCtx,
    scope: &Scope,
) -> Result<(), axum::Error> {
    let state = build_state(ctx, scope).to_string();
    socket.send(Message::text(state)).await
}

async fn sync(State(ctx): State<ServerCtx>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .max_frame_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move { ctx.sync.handle_inbound(socket).await })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PeerView, Settings};
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::Request;
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};
    use tower::ServiceExt;
    use yoink_core::{DeviceInfo, DocSet};
    use yoink_discovery::PeerInfo;
    use yoink_sync::SyncManager;

    /// A full production router around a self-contained [`ServerCtx`].
    /// Must be called from a tokio runtime ([`SyncManager::new`] spawns).
    fn test_app() -> (Router, ServerCtx) {
        let docs = Arc::new(DocSet::new());
        let device = DeviceInfo {
            id: "dev-self".into(),
            name: "my-laptop".into(),
        };
        let (sync, _events) = SyncManager::new(
            docs.clone(),
            device.clone(),
            HashSet::new(),
            &HashSet::new(),
        );
        let (commands, _commands_rx) = mpsc::channel(8);
        let (notify, _) = broadcast::channel(8);
        let ctx = ServerCtx {
            device,
            docs,
            sync,
            peers: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            settings: Arc::new(parking_lot::RwLock::new(Settings {
                auto_apply: true,
                clipboard_available: true,
            })),
            joined_rooms: Arc::new(parking_lot::RwLock::new(BTreeSet::new())),
            commands,
            notify,
        };
        (router(ctx.clone()), ctx)
    }

    fn request(path: &str, peer: Option<&str>) -> Request<Body> {
        let mut req = Request::builder()
            .uri(path)
            .body(Body::empty())
            .expect("test request");
        if let Some(peer) = peer {
            let addr: SocketAddr = peer.parse().expect("test addr");
            req.extensions_mut().insert(ConnectInfo(addr));
        }
        req
    }

    fn local_request(path: &str) -> Request<Body> {
        request(path, Some("127.0.0.1:5000"))
    }

    async fn json_body(res: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
            .await
            .expect("read body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    fn location(res: &Response) -> &str {
        res.headers()
            .get(header::LOCATION)
            .expect("location header")
            .to_str()
            .expect("ascii location")
    }

    #[tokio::test]
    async fn loopback_clients_reach_guarded_routes() {
        for path in [
            "/",
            "/api/state",
            "/r/attic",
            "/assets/idiomorph.js",
            "/standup",
        ] {
            for peer in ["127.0.0.1:5000", "[::1]:5000", "[::ffff:127.0.0.1]:5000"] {
                let (app, _ctx) = test_app();
                let res = app.oneshot(request(path, Some(peer))).await.unwrap();
                assert!(
                    res.status() == StatusCode::OK || res.status().is_redirection(),
                    "{path} from {peer}: {}",
                    res.status()
                );
            }
        }
    }

    #[tokio::test]
    async fn lan_clients_get_403_on_guarded_routes() {
        // Room pages and redirects included: a redirect still confirms which
        // rooms exist, so the guard must cover them too.
        for path in [
            "/",
            "/api/state",
            "/r/attic",
            "/assets/idiomorph.js",
            "/standup",
        ] {
            for peer in ["192.168.1.20:5000", "[fe80::1]:5000"] {
                let (app, _ctx) = test_app();
                let res = app.oneshot(request(path, Some(peer))).await.unwrap();
                assert_eq!(res.status(), StatusCode::FORBIDDEN, "{path} from {peer}");
            }
        }
    }

    #[tokio::test]
    async fn missing_connect_info_fails_closed() {
        let (app, _ctx) = test_app();
        let res = app.oneshot(request("/", None)).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn sync_route_bypasses_the_guard() {
        let (app, _ctx) = test_app();
        let res = app
            .oneshot(request("/sync", Some("192.168.1.20:5000")))
            .await
            .unwrap();
        // Not a WebSocket handshake, so the upgrade is rejected — but with a
        // handler error, not the guard's 403.
        assert_ne!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn room_pages_serve_the_ui_under_canonical_names() {
        for path in ["/", "/r/attic", "/r/my-room-2"] {
            let (app, _ctx) = test_app();
            let res = app.oneshot(local_request(path)).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK, "{path}");
            let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
                .await
                .unwrap();
            assert!(
                std::str::from_utf8(&bytes).unwrap().contains("yoink"),
                "{path} should serve the app page"
            );
        }
    }

    #[tokio::test]
    async fn unsanitized_room_urls_redirect_to_canonical() {
        let cases = [
            ("/r/My%20Room", "/r/my-room"),
            ("/r/ATTIC", "/r/attic"),
            ("/r/a--b", "/r/a-b"),
            ("/r/%21%21%21", "/"), // "!!!" sanitizes to nothing
        ];
        for (path, target) in cases {
            let (app, _ctx) = test_app();
            let res = app.oneshot(local_request(path)).await.unwrap();
            assert_eq!(res.status(), StatusCode::TEMPORARY_REDIRECT, "{path}");
            assert_eq!(location(&res), target, "{path}");
        }
    }

    #[tokio::test]
    async fn bare_words_redirect_to_room_urls() {
        let cases = [
            ("/standup", "/r/standup"),
            ("/Standup", "/r/standup"),
            ("/My%20Room", "/r/my-room"),
            ("/%21%21%21", "/"),
        ];
        for (path, target) in cases {
            let (app, _ctx) = test_app();
            let res = app.oneshot(local_request(path)).await.unwrap();
            assert_eq!(res.status(), StatusCode::TEMPORARY_REDIRECT, "{path}");
            assert_eq!(location(&res), target, "{path}");
        }
    }

    #[tokio::test]
    async fn reserved_paths_are_never_room_redirects() {
        for path in ["/api", "/ws", "/r", "/assets", "/favicon.ico"] {
            let (app, _ctx) = test_app();
            let res = app.oneshot(local_request(path)).await.unwrap();
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "{path}");
        }
        // `/sync` is a real route, so it never reaches the redirect handler.
        let (app, _ctx) = test_app();
        let res = app.oneshot(local_request("/sync")).await.unwrap();
        assert!(!res.status().is_redirection());
    }

    #[tokio::test]
    async fn api_state_defaults_to_the_devices_scope() {
        let (app, ctx) = test_app();
        ctx.docs.devices().add_entry(&ctx.device, "hello".into());
        let res = app.oneshot(local_request("/api/state")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let state = json_body(res).await;
        assert_eq!(state["scope"], "devices");
        assert_eq!(state["entries"][0]["text"], "hello");
    }

    #[tokio::test]
    async fn api_state_serves_room_scopes() {
        let (app, ctx) = test_app();
        let room = Scope::room("attic");
        ctx.docs
            .get_or_create(&room)
            .add_entry(&ctx.device, "room note".into());
        ctx.joined_rooms.write().insert("attic".into());
        let res = app
            .oneshot(local_request("/api/state?scope=room:attic"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let state = json_body(res).await;
        assert_eq!(state["scope"], "room:attic");
        assert_eq!(state["rooms"]["joined"], serde_json::json!(["attic"]));
        assert_eq!(state["entries"][0]["text"], "room note");
    }

    #[tokio::test]
    async fn api_state_for_an_unjoined_room_has_no_entries() {
        let (app, _ctx) = test_app();
        let res = app
            .oneshot(local_request("/api/state?scope=room:nowhere"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let state = json_body(res).await;
        assert_eq!(state["scope"], "room:nowhere");
        assert_eq!(state["entries"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn api_state_rejects_garbage_scopes() {
        for query in [
            "?scope=garbage",
            "?scope=room:Bad%20Name",
            "?scope=room:",
            "?scope=",
        ] {
            let (app, _ctx) = test_app();
            let res = app
                .oneshot(local_request(&format!("/api/state{query}")))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{query}");
        }
    }

    #[tokio::test]
    async fn ws_ui_rejects_garbage_scopes_before_upgrading() {
        let (app, _ctx) = test_app();
        let res = app
            .oneshot(local_request("/ws/ui?scope=nonsense"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn network_rooms_and_members_flow_through_the_route() {
        let (app, ctx) = test_app();
        ctx.peers.write().insert(
            "p1".into(),
            PeerView {
                info: PeerInfo {
                    device_id: "p1".into(),
                    name: "Alpha".into(),
                    addrs: vec![],
                    port: 4242,
                    rooms: vec!["attic".into()],
                },
                online: true,
            },
        );
        ctx.joined_rooms.write().insert("attic".into());
        let res = app
            .oneshot(local_request("/api/state?scope=room:attic"))
            .await
            .unwrap();
        let state = json_body(res).await;
        assert_eq!(
            state["rooms"]["network"],
            serde_json::json!([{"name": "attic", "devices": 1}])
        );
        assert_eq!(
            state["members"],
            serde_json::json!([{"id": "p1", "name": "Alpha", "connected": false}])
        );
    }

    #[tokio::test]
    async fn idiomorph_asset_is_served_with_long_cache() {
        let (app, _ctx) = test_app();
        let res = app
            .oneshot(local_request("/assets/idiomorph.js"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "application/javascript; charset=utf-8"
        );
        assert_eq!(
            res.headers()[header::CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
        let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert!(std::str::from_utf8(&bytes).unwrap().contains("Idiomorph"));
    }

    #[test]
    fn index_html_is_embedded_and_self_contained() {
        assert!(INDEX_HTML.contains("<!doctype html>"));
        // The UI must work without any external resource so the app has zero
        // network dependencies beyond its own routes. The only allowed
        // "http://" is the SVG namespace inside inline SVG/favicon markup,
        // the only allowed <link> is the data-URI icon, and the only allowed
        // <script src> is the idiomorph asset the server itself serves.
        let without_svg_ns = INDEX_HTML.replace("http://www.w3.org/2000/svg", "");
        assert!(!without_svg_ns.contains("http://"));
        assert!(!INDEX_HTML.contains("https://"));
        let without_own_asset =
            INDEX_HTML.replace("<script src=\"/assets/idiomorph.js\"></script>", "");
        assert!(!without_own_asset.contains("<script src"));
        for link in INDEX_HTML.split("<link ").skip(1) {
            assert!(
                link.starts_with("rel=\"icon\" href=\"data:image/svg+xml,"),
                "non-inline <link> found: {}",
                &link[..link.len().min(80)]
            );
        }
    }

    #[test]
    fn index_html_has_an_inline_favicon() {
        // Without a favicon every browser logs a 404 for /favicon.ico. The
        // color is a fixed hex because favicons do not inherit currentColor.
        assert!(INDEX_HTML.contains("<link rel=\"icon\" href=\"data:image/svg+xml,"));
        assert!(INDEX_HTML.contains("%237c8cff"));
    }
}
