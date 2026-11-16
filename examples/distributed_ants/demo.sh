#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
CONFIG_FILE="${1:-swarm.config.toml}"
if (( $# > 0 )); then
    shift
fi
exec python3 "$SCRIPT_DIR/../swarm_control/swarm_demo.py" ants "$CONFIG_FILE" \
    --nx "${NX_BIN:-$SCRIPT_DIR/../../target/release/nx}" "$@"