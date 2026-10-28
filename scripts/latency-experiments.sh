#!/usr/bin/env bash
# Runs the extension 4 latency experiments. Results are recorded in LATENCY.md.
set -uo pipefail
P="${CARGO_TARGET_DIR:-$HOME/voluntas-target}/release/latency_probe"
SECS="${SECS:-90}"
echo "=== A: 8 connections, default DNS ==="
"$P" --connections 8 --tokens 300 --secs "$SECS"
echo "=== B: 8 connections pinned round-robin across resolved IPs ==="
"$P" --connections 8 --tokens 300 --secs "$SECS" --pin-ips
echo "=== C: 8 connections, fixed source ports 41000+ ==="
"$P" --connections 8 --tokens 300 --secs "$SECS" --source-port-base 41000
echo "=== D: 8 connections, TCP_NODELAY off ==="
"$P" --connections 8 --tokens 300 --secs "$SECS" --no-nodelay
echo "=== E: 16 connections (connection spraying) ==="
"$P" --connections 16 --tokens 300 --secs "$SECS"
