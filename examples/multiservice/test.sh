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

echo "Building multiservice example..."
cargo build ${CI:+--locked} -p multiservice-example

echo "Starting multiservice-server on $ADDR..."
ADDR="$ADDR" cargo run ${CI:+--locked} -p multiservice-example --bin multiservice-server &
SERVER_PID=$!

# Wait for server to be ready
for i in $(seq 1 30); do
    if curl -s "http://$ADDR/health" > /dev/null 2>&1; then
        echo "Server is ready."
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Server process died unexpectedly."
        exit 1
    fi
    sleep 0.1
done

if ! curl -s "http://$ADDR/health" > /dev/null 2>&1; then
    echo "Server failed to start within 3 seconds."
    exit 1
fi

echo "Running multiservice-client..."
cargo run ${CI:+--locked} -p multiservice-example --bin multiservice-client
STATUS=$?

if [ $STATUS -eq 0 ]; then
    echo "All multiservice tests passed."
else
    echo "Client failed with exit code $STATUS."
    exit $STATUS
fi
