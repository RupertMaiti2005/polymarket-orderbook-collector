#!/usr/bin/env bash
# Runs cargo inside WSL with the target dir on the Linux filesystem (building
# on /mnt/c is very slow). Usage from Windows: wsl.exe -- bash scripts/wsl-cargo.sh build
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/voluntas-target}"
source "$HOME/.cargo/env" 2>/dev/null || true
exec cargo "$@"
