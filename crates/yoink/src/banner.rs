//! The startup banner: a small, colorful pointer to the local web UI, printed
//! to stdout when the daemon comes up. Color is emitted only to a real
//! terminal and never when `NO_COLOR` is set, so redirected or piped output
//! stays plain; `CLICOLOR_FORCE` overrides the terminal check.

use std::io::IsTerminal;

/// Print the startup banner: the web UI URL, highlighted so it stands out and
/// stays clickable, alongside this device's name and short id.
pub(crate) fn print(url: &str, device_name: &str, device_id: &str) {
    let style = Style::detect();
    // A short id reads better than a full uuid and is enough to tell devices
    // apart at a glance; fall back to the whole thing if it is unexpectedly
    // short (never for a uuid, but device ids can be hand-edited).
    let short_id = device_id.get(..8).unwrap_or(device_id);

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
        style.paint("1;4;38;5;44", url),
    );
    println!(
        "     {}  {} {}",
        style.paint("2", "device"),
        device_name,
        style.paint("2", &format!("· {short_id}")),
    );
    println!();
}

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
}
