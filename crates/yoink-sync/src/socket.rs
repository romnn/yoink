//! Unifies inbound (axum) and outbound (tokio-tungstenite) WebSockets behind
//! one binary-frame interface so the connection logic is role-agnostic.

use axum::extract::ws;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// The connection is unusable; the caller should drop it.
#[derive(Debug)]
pub(crate) struct SocketClosed;

/// What [`PeerSocket::recv`] produced.
pub(crate) enum Incoming {
    /// A binary protocol frame.
    Frame(Vec<u8>),
    /// Non-protocol traffic (ping, pong, stray text). Carries no payload, but
    /// proves the peer endpoint is alive — the keepalive logic counts it as
    /// inbound activity.
    Activity,
}

pub(crate) enum PeerSocket {
    Inbound(Box<ws::WebSocket>),
    Outbound(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
}

impl PeerSocket {
    pub(crate) async fn send(&mut self, frame: Vec<u8>) -> Result<(), SocketClosed> {
        let result = match self {
            PeerSocket::Inbound(socket) => socket
                .send(ws::Message::Binary(frame.into()))
                .await
                .map_err(|err| err.to_string()),
            PeerSocket::Outbound(stream) => stream
                .send(WsMessage::Binary(frame.into()))
                .await
                .map_err(|err| err.to_string()),
        };
        result.map_err(|err| {
            tracing::debug!(%err, "websocket send failed");
            SocketClosed
        })
    }

    /// Keepalive probe. The peer's websocket layer answers with a pong, which
    /// [`PeerSocket::recv`] reports as [`Incoming::Activity`].
    pub(crate) async fn send_ping(&mut self) -> Result<(), SocketClosed> {
        let result = match self {
            PeerSocket::Inbound(socket) => socket
                .send(ws::Message::Ping(Vec::new().into()))
                .await
                .map_err(|err| err.to_string()),
            PeerSocket::Outbound(stream) => stream
                .send(WsMessage::Ping(Vec::new().into()))
                .await
                .map_err(|err| err.to_string()),
        };
        result.map_err(|err| {
            tracing::debug!(%err, "websocket ping failed");
            SocketClosed
        })
    }

    /// Next inbound websocket traffic. Pings are answered by the websocket
    /// layers themselves; they (and pongs and stray text) are surfaced as
    /// [`Incoming::Activity`] so the caller can track liveness. `None` means
    /// the connection ended (close frame, stream end, or read error).
    pub(crate) async fn recv(&mut self) -> Option<Incoming> {
        match self {
            PeerSocket::Inbound(socket) => match socket.recv().await? {
                Ok(ws::Message::Binary(payload)) => Some(Incoming::Frame(payload.to_vec())),
                Ok(ws::Message::Close(_)) => None,
                Ok(_) => Some(Incoming::Activity),
                Err(err) => {
                    tracing::debug!(%err, "websocket read failed");
                    None
                }
            },
            PeerSocket::Outbound(stream) => match stream.next().await? {
                Ok(WsMessage::Binary(payload)) => Some(Incoming::Frame(payload.to_vec())),
                Ok(WsMessage::Close(_)) => None,
                Ok(_) => Some(Incoming::Activity),
                Err(err) => {
                    tracing::debug!(%err, "websocket read failed");
                    None
                }
            },
        }
    }

    /// Best-effort close notification; failures are irrelevant at this point.
    pub(crate) async fn close(mut self) {
        match &mut self {
            PeerSocket::Inbound(socket) => {
                let _ = socket.send(ws::Message::Close(None)).await;
            }
            PeerSocket::Outbound(stream) => {
                let _ = stream.close(None).await;
            }
        }
    }
}
