#!/usr/bin/env bash
# Installs Kaseta for the current user.
#
# Everything goes under $HOME: no root, no system directories. Recording is a
# per-user activity that needs the user's own audio session, so installing it
# system-wide would be both unnecessary and wrong.
set -euo pipefail

BIN_DIR="$HOME/.local/bin"
APP_DIR="$HOME/.local/share/applications"
ICON_DIR="$HOME/.local/share/icons/hicolor/scalable/apps"
UNIT_DIR="$HOME/.config/systemd/user"
CONF_DIR="$HOME/.config/kaseta"
DATA_DIR="$HOME/.local/share/kaseta"

here() { cd "$(dirname "${BASH_SOURCE[0]}")" && pwd; }
ROOT="$(here)"

say() { printf '\033[1m%s\033[0m\n' "$*"; }
warn() { printf '\033[33m%s\033[0m\n' "$*"; }

say "Building…"
cargo build --release -p kasetad

mkdir -p "$BIN_DIR" "$APP_DIR" "$ICON_DIR" "$UNIT_DIR" "$CONF_DIR" "$DATA_DIR"

install -m755 "$ROOT/target/release/kasetad" "$BIN_DIR/kasetad"
install -m755 "$ROOT/packaging/kaseta-open" "$BIN_DIR/kaseta-open"
install -m755 "$ROOT/packaging/kaseta-tray" "$BIN_DIR/kaseta-tray"
# The desktop session's PATH may not include ~/.local/bin, so the entry points
# at the launcher absolutely rather than by name.
sed "s|__BIN_DIR__|$BIN_DIR|" "$ROOT/packaging/kaseta.desktop" > "$APP_DIR/kaseta.desktop"
chmod 644 "$APP_DIR/kaseta.desktop"

# A second entry so recording can be bound to a key in the desktop's own
# shortcut settings. Binding a global hotkey needs the compositor's cooperation,
# and every desktop exposes that differently; a launchable action is the one
# mechanism all of them share.
sed "s|__BIN_DIR__|$BIN_DIR|" "$ROOT/packaging/kaseta-record.desktop" > "$APP_DIR/kaseta-record.desktop"
chmod 644 "$APP_DIR/kaseta-record.desktop"
install -m644 "$ROOT/packaging/kaseta.svg" "$ICON_DIR/kaseta.svg"
install -m644 "$ROOT/packaging/kaseta-symbolic.svg" "$ICON_DIR/kaseta-symbolic.svg"
install -m644 "$ROOT/packaging/kaseta.service" "$UNIT_DIR/kaseta.service"

# Created empty rather than overwritten: it holds the summariser key, and
# reinstalling must not discard it.
if [ ! -f "$CONF_DIR/env" ]; then
    cat > "$CONF_DIR/env" <<'ENV'
# Summaries are produced through OpenRouter. Without a key, recordings are still
# captured and transcribed — only the summary is skipped.
# KASETA_OPENROUTER_KEY=sk-or-...
# KASETA_OPENROUTER_MODEL=anthropic/claude-3.5-haiku
ENV
    chmod 600 "$CONF_DIR/env"
fi

# The transcription worker lives in its own virtual environment: it needs
# Python, and mixing it into the system site-packages would be antisocial.
if [ ! -x "$ROOT/worker/.venv/bin/python" ]; then
    say "Setting up the transcription worker…"
    python3 -m venv "$ROOT/worker/.venv"
    "$ROOT/worker/.venv/bin/pip" install -q -e "$ROOT/worker"
fi
# Replaced rather than only appended when missing: moving or recloning the repo
# would otherwise leave this pointing at a virtual environment that no longer
# exists, and transcription would fail much later with a confusing error.
sed -i '/^KASETA_WORKER_PYTHON=/d' "$CONF_DIR/env"
echo "KASETA_WORKER_PYTHON=$ROOT/worker/.venv/bin/python" >> "$CONF_DIR/env"

update-desktop-database "$APP_DIR" 2>/dev/null || true
gtk-update-icon-cache -f -t "$HOME/.local/share/icons/hicolor" 2>/dev/null || true

# Activation needs a reachable user manager. Under sudo, over SSH without a user
# bus, or on a non-systemd session there is none, and running these under `set
# -e` would abort with a raw bus error after everything was already installed.
if systemctl --user show-environment >/dev/null 2>&1; then
    systemctl --user daemon-reload
    systemctl --user enable kaseta.service
    # Restart rather than `enable --now`: that starts a stopped service but
    # leaves a running one on the old binary, so reinstalling would appear to
    # do nothing. The interface is compiled into the executable, which makes a
    # stale process particularly confusing — the repository is up to date and
    # the window is not.
    systemctl --user restart kaseta.service
    ACTIVATED=1
else
    ACTIVATED=0
fi

say ""
say "Kaseta is installed."
printf '  %-22s %s\n' "Application:" "in your launcher, as “Kaseta”"
printf '  %-22s %s\n' "Recordings:" "$DATA_DIR"
printf '  %-22s %s\n' "Summaries:" "add a key in Settings"
printf '  %-22s %s\n' "Hotkey:" "bind 'kaseta-tray toggle' in your desktop settings"
say ""

if ! echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
    warn "Note: $BIN_DIR is not on your PATH."
    warn "The launcher works regardless; only the kasetad command needs it."
fi

if [ "$ACTIVATED" -eq 0 ]; then
    warn "No systemd user session was reachable, so the recorder was not started."
    warn "Run this from your desktop session, or start it yourself:"
    warn "  systemctl --user enable --now kaseta.service"
elif systemctl --user is-active --quiet kaseta.service; then
    say "The recorder is running."
else
    warn "The service did not start; see: journalctl --user -u kaseta -n 30"
fi
