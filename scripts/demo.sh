#!/usr/bin/env bash
# End-to-end demo (Linux/macOS): start the server, run every task type through
# the CLI client, print stats, then stop the server with SIGINT (graceful).
# On Windows use scripts/demo.ps1 instead.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN="${BUILD_DIR:-build}/bin"
PORT="${PORT:-7878}"
WS="$(mktemp -d)"
cp examples/workspace/* "$WS"/
"$BIN/concurrentcore-server" --port "$PORT" --workspace "$WS" --log-level info &
SERVER=$!
trap 'kill -INT $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do "$BIN/concurrentcore-client" --port "$PORT" ping >/dev/null 2>&1 && break; sleep 0.1; done
C=("$BIN/concurrentcore-client" --port "$PORT")
"${C[@]}" ping
"${C[@]}" submit --type text-analysis --file sample.txt --wait
"${C[@]}" submit --type sha256 --file sample.txt --wait
"${C[@]}" submit --type compress --input sample.txt --format gzip --level 9 --wait
"${C[@]}" submit --type decompress --input sample.txt.gz --output roundtrip.txt --wait
"${C[@]}" submit --type compute --limit 2000000 --wait
"${C[@]}" stats
cmp "$WS/sample.txt" "$WS/roundtrip.txt" && echo "round trip OK"
kill -INT "$SERVER"; wait "$SERVER"; trap - EXIT
echo "server exited with status $?"
rm -rf "$WS"
