#!/usr/bin/env bash
set -euo pipefail

LABEL="com.herwanto.hira.rbs-helper"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
rm -f "$PLIST"

echo "Removed $LABEL"
echo "Local credentials file kept at $HOME/.hira-rbs-helper.env"
