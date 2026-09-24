#!/usr/bin/env bash
# Install the agent on this Mac as a per-user LaunchAgent: starts at login,
# restarted by launchd if it exits. The agent only dials out to the hub.
#
#   scripts/install-macos-agent.sh --hub wss://hub.example.com --token-file PATH [--name NAME]
#   scripts/install-macos-agent.sh --uninstall
#
# The token is copied (0600) next to the binary; the original is not needed
# afterwards.
set -euo pipefail

LABEL=io.github.nhatvu148.fleetmon-agent
DIR="$HOME/Library/Application Support/fleetmon"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG="$HOME/Library/Logs/fleetmon-agent.log"
DOMAIN="gui/$(id -u)"

hub="" token="" name="" uninstall=0
while [ $# -gt 0 ]; do
  case "$1" in
    --hub) hub=$2; shift 2 ;;
    --token-file) token=$2; shift 2 ;;
    --name) name=$2; shift 2 ;;
    --uninstall) uninstall=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ "$uninstall" = 1 ]; then
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
  rm -f "$PLIST"
  rm -rf "$DIR"
  echo "removed $LABEL"
  exit 0
fi

# Both values are written into the plist; accept only what they need to be.
[[ "$hub" =~ ^wss?://[A-Za-z0-9.-]+(:([0-9]{1,5}))?/?$ ]] || { echo "invalid --hub: $hub" >&2; exit 2; }
port=${BASH_REMATCH[2]:-}
[ -z "$port" ] || { [ "$port" -ge 1 ] && [ "$port" -le 65535 ]; } || { echo "invalid --hub port: $port" >&2; exit 2; }
[ -z "$name" ] || [[ "$name" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || { echo "invalid --name: $name" >&2; exit 2; }
[ -s "$token" ] || { echo "no token at --token-file ${token:-<unset>}" >&2; exit 2; }

cd "$(dirname "$0")/.."
cargo build --release --locked -q -p fleetmon-agent

# Everything that can fail runs before the running agent is touched: a typo or a
# failed build must leave the current install working.
mkdir -p "$DIR" "$(dirname "$PLIST")" "$(dirname "$LOG")"

name_args=""
[ -n "$name" ] && name_args="    <string>--name</string>
    <string>$name</string>"

new_plist=$(mktemp)
trap 'rm -f "$new_plist"' EXIT
cat > "$new_plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$DIR/fleetmon-agent</string>
    <string>--hub</string>
    <string>$hub</string>
    <string>--token-file</string>
    <string>$DIR/token</string>
$name_args
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>NO_COLOR</key>
    <string>1</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <!-- The agent reconnects to the hub by itself; this only covers it exiting. -->
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>ProcessType</key>
  <string>Background</string>
  <key>StandardOutPath</key>
  <string>$LOG</string>
  <key>StandardErrorPath</key>
  <string>$LOG</string>
</dict>
</plist>
PLIST
plutil -lint -s "$new_plist"

# Only now swap: stop the old agent (its binary cannot be replaced while it
# runs under launchd's KeepAlive), install, start. The current install is kept
# aside first, and restored and restarted if any step of the swap fails.
bak=$(mktemp -d)
trap 'rm -f "$new_plist"; rm -rf "$bak"' EXIT
for f in "$DIR/fleetmon-agent" "$DIR/token" "$PLIST"; do
  if [ -e "$f" ]; then cp -p "$f" "$bak/"; fi
done
rollback() {
  # Best effort, step by step: under `set -e` one failed restore would otherwise
  # abort before the previous agent is started again.
  set +e
  echo "install failed; restoring the previous agent" >&2
  if [ -e "$bak/fleetmon-agent" ]; then cp -p "$bak/fleetmon-agent" "$DIR/"; fi
  if [ -e "$bak/token" ]; then cp -p "$bak/token" "$DIR/"; fi
  if [ -e "$bak/$(basename "$PLIST")" ]; then
    cp -p "$bak/$(basename "$PLIST")" "$PLIST"
    launchctl bootstrap "$DOMAIN" "$PLIST" 2>/dev/null || true
  fi
  exit 1
}
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
install -m 755 target/release/fleetmon-agent "$DIR/fleetmon-agent" || rollback
install -m 600 "$token" "$DIR/token" || rollback
install -m 644 "$new_plist" "$PLIST" || rollback
launchctl bootstrap "$DOMAIN" "$PLIST" || rollback
sleep 3
state=$(launchctl print "$DOMAIN/$LABEL" 2>/dev/null | awk -F'= ' '/^\tstate/ {print $2; exit}')
echo "$LABEL: ${state:-unknown}"
tail -n 2 "$LOG" 2>/dev/null || true
[ "$state" = running ] || exit 1
