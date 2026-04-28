#!/usr/bin/env bash
set -euo pipefail

LABEL="com.herwanto.hira.rbs-helper"
PROJECT_DIR="/Users/mherwanto/Desktop/herwanto OS"
HELPER="$PROJECT_DIR/scripts/rbs_mac_helper.py"
ENV_FILE="$HOME/.hira-rbs-helper.env"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$PROJECT_DIR/logs"

if [[ ! -f "$HELPER" ]]; then
  echo "RBS helper not found: $HELPER" >&2
  exit 1
fi

if [[ -z "${GOOGLE_SERVICE_ACCOUNT_JSON:-}" || -z "${GOOGLE_SHEET_ID:-}" ]]; then
  echo "GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_SHEET_ID must be exported in this Terminal first." >&2
  exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR"

cat > "$ENV_FILE" <<EOF
export GOOGLE_SERVICE_ACCOUNT_JSON='${GOOGLE_SERVICE_ACCOUNT_JSON}'
export GOOGLE_SHEET_ID='${GOOGLE_SHEET_ID}'
export RBS_CHROME_PROFILE_DIR="\$HOME/.hira-rbs-chrome"
unset RBS_CHROME_USER_DATA_DIR
unset RBS_CHROME_PROFILE_NAME
EOF
chmod 600 "$ENV_FILE"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>source "$ENV_FILE"; cd "$PROJECT_DIR"; exec /usr/bin/python3 "$HELPER" --once</string>
  </array>

  <key>StartInterval</key>
  <integer>60</integer>

  <key>StandardOutPath</key>
  <string>$LOG_DIR/rbs-helper.log</string>

  <key>StandardErrorPath</key>
  <string>$LOG_DIR/rbs-helper.err.log</string>

  <key>WorkingDirectory</key>
  <string>$PROJECT_DIR</string>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl enable "gui/$(id -u)/$LABEL"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "Installed $LABEL"
echo "It checks once per minute and only opens Chrome when an RBS job exists."
echo "Logs:"
echo "  $LOG_DIR/rbs-helper.log"
echo "  $LOG_DIR/rbs-helper.err.log"
