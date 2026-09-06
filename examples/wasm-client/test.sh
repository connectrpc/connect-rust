#!/usr/bin/env bash
set -euo pipefail

SERVER_PID=""
ADDR="${ADDR:-127.0.0.1:8080}"

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "Building eliza-server..."
cargo build ${CI:+--locked} -p eliza-example --bin eliza-server

echo "Starting eliza-server on $ADDR..."
cargo run ${CI:+--locked} -p eliza-example --bin eliza-server -- --addr "$ADDR" &
SERVER_PID=$!

for i in $(seq 1 30); do
    if curl -s -X POST "http://$ADDR/connectrpc.eliza.v1.ElizaService/Say" \
        -H "Content-Type: application/json" \
        -d '{"sentence":"healthcheck"}' > /dev/null 2>&1; then
        echo "Server is ready."
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Server process died unexpectedly."
        exit 1
    fi
    sleep 0.1
done

echo "Running wasm integration test in headless Firefox..."
wasm-pack test --headless --firefox examples/wasm-client
echo "Wasm integration test passed."
