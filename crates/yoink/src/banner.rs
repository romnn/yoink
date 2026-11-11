//! The startup banner: a small, colorful pointer to the local web UI, printed
//! to stdout when the daemon comes up, plus the links and QR code for other
//! devices under `--remote-ui`. Color is emitted only to a real
//! terminal and never when `NO_COLOR` is set, so redirected or piped output
//! stays plain; `CLICOLOR_FORCE` overrides the terminal check.

use qrcode::QrCode;
use qrcode::render::unicode::Dense1x2;
use std::io::IsTerminal;
use yoink_core::DeviceInfo;
use yoink_server::{RemoteAccess, RemoteAuth, UiAccess};

/// Print the startup banner: the web UI URL, highlighted so it stands out and
/// stays clickable, alongside this device's name and short id.
///
/// When `access` admits other devices, the remote links follow, plus a QR code for the
/// best one unless `show_qr` is off.
pub(crate) fn print(local_url: &str, device: &DeviceInfo, access: &UiAccess, show_qr: bool) {
    let style = Style::detect();
    // A short id reads better than a full uuid and is enough to tell devices
    // apart at a glance; fall back to the whole thing if it is unexpectedly
    // short (never for a uuid, but device ids can be hand-edited).
    let short_id = device.id.get(..8).unwrap_or(&device.id);

    println!();
    println!(
        "  {}  {}",
        style.paint("1;38;5;205", "yoink"),
        style.paint("2", "shared clipboard for your LAN"),
    );
    println!();
    println!(
        "  {}  {}",
        style.paint("1;38;5;42", "➜"),
        style.paint("1;4;38;5;44", local_url),
    );
    println!(
        "     {}  {} {}",
        style.paint("2", "device"),
        device.name,
        style.paint("2", &format!("· {short_id}")),
    );
    if let UiAccess::Remote(remote) = access {
        print_remote(&style, remote, show_qr);
    }
    println!();
}

fn print_remote(style: &Style, remote: &RemoteAccess, show_qr: bool) {
    // Best first, so the QR code shows the link a phone most likely reaches.
    let links = remote.links();
    let Some((first, others)) = links.split_first() else {
        println!(
            "     {}  {}",
            style.paint("2", "remote"),
            style.paint(
                WARN,
                "no network address found; other devices can't reach this machine"
            ),
        );
        return;
    };

    // Links
    println!(
        "     {}  {}",
        style.paint("2", "remote"),
        style.paint("4;38;5;44", first),
    );
    for link in others {
        println!("             {}", style.paint("4;38;5;44", link));
    }
    let note = match remote.auth {
        RemoteAuth::Token(_) => style.paint(
            "2",
            "secret link: anyone who has it can use this yoink · changes on every restart",
        ),
        RemoteAuth::Open => style.paint(
            WARN,
            "open to everyone on your network: anyone can read and share from this yoink",
        ),
    };
    println!("             {note}");

    // QR code for the best link
    if show_qr && let Some(qr) = style.qr(first) {
        println!();
        for line in qr.lines() {
            println!("     {line}");
        }
        println!(
            "     {}",
            style.paint("2", "scan with a phone on the same network"),
        );
    }
}

/// Bold amber, for the lines that warn.
const WARN: &str = "1;38;5;214";

/// Whether ANSI styling should be emitted, decided once from the environment.
struct Style {
    color: bool,
}

impl Style {
    fn detect() -> Self {
        // NO_COLOR (https://no-color.org/) wins when set. Otherwise color a
        // real terminal, or any stream when CLICOLOR_FORCE explicitly asks.
        let forced = std::env::var("CLICOLOR_FORCE").is_ok_and(|v| v != "0");
        let color =
            std::env::var_os("NO_COLOR").is_none() && (forced || std::io::stdout().is_terminal());
        Self { color }
    }

    /// Wrap `text` in the SGR parameters `params` (e.g. `"1;4;38;5;44"`), or
    /// return it untouched when color is disabled.
    fn paint(&self, params: &str, text: &str) -> String {
        if self.color {
            format!("\x1b[{params}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    /// Renders `data` as a QR code of half-height block characters, two modules per
    /// character cell.
    ///
    /// Scanners need dark modules on a light background.
    /// With color, every line is painted black on white, which reads the same on any
    /// terminal theme.
    /// Without color, the glyphs take the terminal's own colors, so the code is drawn
    /// inverted, which comes out right on the far more common dark themes.
    fn qr(&self, data: &str) -> Option<String> {
        let code = QrCode::new(data).ok()?;
        let (dark, light) = if self.color {
            (Dense1x2::Dark, Dense1x2::Light)
        } else {
            (Dense1x2::Light, Dense1x2::Dark)
        };
        let qr = code
            .render::<Dense1x2>()
            .dark_color(dark)
            .light_color(light)
            .build();
        if !self.color {
            return Some(qr);
        }
        Some(
            qr.lines()
                .map(|line| self.paint("38;5;16;48;5;231", line))
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }
}
