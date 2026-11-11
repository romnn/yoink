//! `GET /api/remote`: the links and QR codes the local UI shows for opening yoink on
//! another device.

use crate::access::{RemoteAuth, UiAccess};
use axum::Json;
use qrcode::QrCode;
use qrcode::render::svg;
use serde::Serialize;
use std::sync::Arc;

/// Response shape of `GET /api/remote`.
#[derive(Debug, Serialize)]
pub(crate) struct RemoteJson {
    /// `"off"`, `"token"` or `"open"`.
    auth: &'static str,
    /// Empty when `auth` is `"off"`, or when no network address was found.
    links: Vec<RemoteLinkJson>,
}

#[derive(Debug, Serialize)]
struct RemoteLinkJson {
    url: String,
    /// Standalone SVG document; `None` if the URL does not fit in a QR code, which no URL
    /// yoink builds comes close to.
    qr_svg: Option<String>,
}

pub(crate) async fn remote_links(access: Arc<UiAccess>) -> Json<RemoteJson> {
    let UiAccess::Remote(remote) = access.as_ref() else {
        return Json(RemoteJson {
            auth: "off",
            links: Vec::new(),
        });
    };
    let auth = match remote.auth {
        RemoteAuth::Token(_) => "token",
        RemoteAuth::Open => "open",
    };
    let links = remote
        .links()
        .into_iter()
        .map(|url| RemoteLinkJson {
            qr_svg: qr_svg(&url),
            url,
        })
        .collect();
    Json(RemoteJson { auth, links })
}

fn qr_svg(data: &str) -> Option<String> {
    let code = QrCode::new(data).ok()?;
    Some(
        code.render::<svg::Color<'_>>()
            .min_dimensions(240, 240)
            .build(),
    )
}
