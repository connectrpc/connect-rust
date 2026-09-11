#!/usr/bin/env bash
#
# CPU + allocator profiling harness for the connectrpc-rs, tonic (prost) and
# tonic-protobuf (upb) bench servers.
#
# Usage: profile_server.sh <target> [duration_secs] [concurrency] [n_conns] [records]
#   target: connectrpc | tonic | tonic-protobuf            (fortunes, needs docker)
#           echo-{connectrpc,tonic,tonic-protobuf}
#           log-{connectrpc,tonic,tonic-protobuf} | log-connectrpc-noutf8
#
# Produces:
#   /tmp/connectrpc-profile/<target>/flamegraph.svg    — CPU flamegraph
#   /tmp/connectrpc-profile/<target>/perf-summary.txt  — top functions
#   /tmp/connectrpc-profile/<target>/heap-summary.txt  — allocation sites
#
set -euo pipefail

TARGET="${1:-connectrpc}"
DURATION="${2:-300}"
CONCURRENCY="${3:-64}"
# Number of client h2 connections (echo/log targets). >1 reduces h2
# mutex contention so server-side framework/proto overhead is visible.
N_CONNS="${4:-8}"
# Records per batch (log targets only). Controls decode workload density.
RECORDS="${5:-50}"

OUTDIR="/tmp/connectrpc-profile/${TARGET}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Validate target ──────────────────────────────────────────────────

# NEEDS_VALKEY marks targets whose server takes a valkey address as argv[1].
NEEDS_VALKEY=0
# The tonic-protobuf servers live in the out-of-workspace benches/rpc-grpc-rust
# crate, which has its own manifest and target dir.
GRPC_RUST_DIR="$ROOT_DIR/benches/rpc-grpc-rust"
SERVER_PKG=""
SERVER_MANIFEST=""
SERVER_TARGET_DIR="$ROOT_DIR/target"
case "$TARGET" in
  connectrpc)
    SERVER_PKG="rpc-bench"
    SERVER_BIN_NAME="fortune_server"
    LOAD_BIN_NAME="fortune_load"
    LOAD_ARGS="$DURATION $CONCURRENCY grpc"
    NEEDS_VALKEY=1
    ;;
  tonic)
    SERVER_PKG="rpc-bench-tonic"
    SERVER_BIN_NAME="fortune-server-tonic"
    LOAD_BIN_NAME="fortune_load"
    LOAD_ARGS="$DURATION $CONCURRENCY grpc"
    NEEDS_VALKEY=1
    ;;
  tonic-protobuf)
    SERVER_MANIFEST="$GRPC_RUST_DIR/Cargo.toml"
    SERVER_TARGET_DIR="$GRPC_RUST_DIR/target"
    SERVER_BIN_NAME="fortune-server-tonic-protobuf"
    LOAD_BIN_NAME="fortune_load"
    LOAD_ARGS="$DURATION $CONCURRENCY grpc"
    NEEDS_VALKEY=1
    ;;
  echo-connectrpc)
    SERVER_PKG="rpc-bench"
    SERVER_BIN_NAME="echo_server"
    LOAD_BIN_NAME="echo_load"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS"
    ;;
  echo-tonic)
    SERVER_PKG="rpc-bench-tonic"
    SERVER_BIN_NAME="echo-server-tonic"
    LOAD_BIN_NAME="echo_load"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS"
    ;;
  echo-tonic-protobuf)
    SERVER_MANIFEST="$GRPC_RUST_DIR/Cargo.toml"
    SERVER_TARGET_DIR="$GRPC_RUST_DIR/target"
    SERVER_BIN_NAME="echo-server-tonic-protobuf"
    LOAD_BIN_NAME="echo_load"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS"
    ;;
  log-connectrpc)
    SERVER_PKG="rpc-bench"
    SERVER_BIN_NAME="log_server"
    LOAD_BIN_NAME="log_load"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS $RECORDS"
    ;;
  log-tonic)
    SERVER_PKG="rpc-bench-tonic"
    SERVER_BIN_NAME="log-server-tonic"
    LOAD_BIN_NAME="log_load"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS $RECORDS"
    ;;
  log-tonic-protobuf)
    SERVER_MANIFEST="$GRPC_RUST_DIR/Cargo.toml"
    SERVER_TARGET_DIR="$GRPC_RUST_DIR/target"
    SERVER_BIN_NAME="log-server-tonic-protobuf"
    LOAD_BIN_NAME="log_load"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS $RECORDS"
    ;;
  log-connectrpc-noutf8)
    SERVER_PKG="rpc-bench"
    SERVER_BIN_NAME="log_server_noutf8"
    LOAD_BIN_NAME="log_load_noutf8"
    LOAD_ARGS="$DURATION $CONCURRENCY $N_CONNS $RECORDS"
    ;;
  *)
    echo "Unknown target: $TARGET" >&2
    echo "Expected: connectrpc | tonic | tonic-protobuf | echo-{connectrpc,tonic,tonic-protobuf} | log-{connectrpc,tonic,tonic-protobuf} | log-connectrpc-noutf8" >&2
    exit 1
    ;;
esac

SERVER_BIN="$SERVER_TARGET_DIR/release/$SERVER_BIN_NAME"
LOAD_BIN="$ROOT_DIR/target/release/$LOAD_BIN_NAME"

# ── Check prerequisites ─────────────────────────────────────────────

for cmd in perf inferno-collapse-perf inferno-flamegraph jeprof; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Required tool not found: $cmd" >&2
    if [[ "$cmd" == inferno-* ]]; then
      echo "Install with: cargo install inferno" >&2
    fi
    exit 1
  fi
done

JEMALLOC_LIB="${JEMALLOC_LIB:-}"
if [[ -z "$JEMALLOC_LIB" ]]; then
  for candidate in /lib/x86_64-linux-gnu/libjemalloc.so.2 /usr/lib/libjemalloc.so.2 /usr/lib64/libjemalloc.so.2; do
    if [[ -f "$candidate" ]]; then
      JEMALLOC_LIB="$candidate"
      break
    fi
  done
fi
if [[ -z "$JEMALLOC_LIB" || ! -f "$JEMALLOC_LIB" ]]; then
  echo "jemalloc not found. Set JEMALLOC_LIB=/path/to/libjemalloc.so" >&2
  exit 1
fi

# ── Prepare output directory ────────────────────────────────────────

rm -rf "$OUTDIR"
mkdir -p "$OUTDIR"

echo "=== Profiling $TARGET ==="
echo "  Duration:    ${DURATION}s"
echo "  Concurrency: $CONCURRENCY"
echo "  Output:      $OUTDIR"
echo ""

# ── Build ────────────────────────────────────────────────────────────

echo "Building $TARGET server with debug info..."
if [[ -n "$SERVER_MANIFEST" ]]; then
  # Prebuilt protoc + plugin in GRPC_RUST_PROTOC_DIR skips the cmake toolchain
  # build (see benches/rpc-grpc-rust/README.md).
  GRPC_RUST_FEATURES=()
  if [[ -n "${GRPC_RUST_PROTOC_DIR:-}" ]]; then
    GRPC_RUST_FEATURES=(--no-default-features)
  fi
  CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release --manifest-path "$SERVER_MANIFEST" --target-dir "$SERVER_TARGET_DIR" --bin "$SERVER_BIN_NAME" "${GRPC_RUST_FEATURES[@]}"
else
  CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release -p "$SERVER_PKG" --bin "$SERVER_BIN_NAME"
fi

echo "Building load generator..."
CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release -p rpc-bench --bin "$LOAD_BIN_NAME"

echo ""

# ── Start valkey (fortune targets only) ───────────────────────────────

VALKEY_NAME=""
VALKEY_ADDR=""
SERVER_ARGS=()
if [[ "$NEEDS_VALKEY" == "1" ]]; then
  echo "Starting valkey container..."
  VALKEY_NAME="valkey-profile-$$"
  docker run -d --rm -p 127.0.0.1::6379 --name "$VALKEY_NAME" valkey/valkey:8-alpine >/dev/null
  VALKEY_ADDR=$(docker port "$VALKEY_NAME" 6379)
  # Wait for readiness then seed the fortunes hash.
  VALKEY_PORT=${VALKEY_ADDR##*:}
  for _ in $(seq 1 50); do
    redis-cli -p "$VALKEY_PORT" ping &>/dev/null && break
    sleep 0.1
  done
  redis-cli -p "$VALKEY_PORT" >/dev/null <<'EOF'
HSET fortunes 1 "fortune: No such file or directory"
HSET fortunes 2 "A computer scientist is someone who fixes things that aren't broken."
HSET fortunes 3 "After enough decimal places, nobody gives a damn."
HSET fortunes 4 "A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1"
HSET fortunes 5 "A computer program does what you tell it to do, not what you want it to do."
HSET fortunes 6 "Emacs is a nice operating system, but I prefer UNIX. - Tom Christaensen"
HSET fortunes 7 "Any program that runs right is obsolete."
HSET fortunes 8 "A list is only as strong as its weakest link. - Donald Knuth"
HSET fortunes 9 "Feature: A bug with seniority."
HSET fortunes 10 "Computers make very fast, very accurate mistakes."
HSET fortunes 11 "<script>alert(This should not be displayed);</script>"
HSET fortunes 12 "framework-benchmark"
EOF
  echo "  valkey ready at $VALKEY_ADDR"
  SERVER_ARGS=("$VALKEY_ADDR")
  echo ""
fi

# ── Start server with jemalloc profiling ─────────────────────────────

STDOUT_FILE=$(mktemp)
cleanup() {
  rm -f "$STDOUT_FILE"
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill -INT "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "${PERF_PID:-}" ]]; then
    kill -INT "$PERF_PID" 2>/dev/null || true
    wait "$PERF_PID" 2>/dev/null || true
  fi
  if [[ -n "$VALKEY_NAME" ]]; then
    docker rm -f "$VALKEY_NAME" &>/dev/null || true
  fi
}
trap cleanup EXIT

echo "Starting $TARGET server with jemalloc heap profiling..."
LD_PRELOAD="$JEMALLOC_LIB" \
MALLOC_CONF="prof:true,prof_final:true,lg_prof_sample:16,lg_prof_interval:30,prof_prefix:$OUTDIR/heap" \
  "$SERVER_BIN" "${SERVER_ARGS[@]}" > "$STDOUT_FILE" 2>"$OUTDIR/server-stderr.log" &
SERVER_PID=$!

# Wait for server to print its address
for i in $(seq 1 50); do
  if [[ -s "$STDOUT_FILE" ]]; then
    break
  fi
  sleep 0.1
done

if [[ ! -s "$STDOUT_FILE" ]]; then
  echo "Server failed to start (no address printed)" >&2
  kill "$SERVER_PID" 2>/dev/null || true
  exit 1
fi

SERVER_ADDR=$(head -1 "$STDOUT_FILE")
echo "  Server listening on $SERVER_ADDR (PID $SERVER_PID)"

# ── Attach perf ──────────────────────────────────────────────────────

echo "Attaching perf record..."
perf record -g --call-graph dwarf -F 99 -p "$SERVER_PID" -o "$OUTDIR/perf.data" &
PERF_PID=$!
sleep 1

# ── Run load ─────────────────────────────────────────────────────────

echo "Running load: $LOAD_ARGS..."
echo ""
"$LOAD_BIN" "$SERVER_ADDR" $LOAD_ARGS
echo ""

# ── Stop perf ────────────────────────────────────────────────────────

echo "Stopping perf..."
kill -INT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true

# ── Stop server (clean exit triggers jemalloc final dump) ────────────

echo "Stopping server..."
kill -INT "$SERVER_PID" 2>/dev/null || true
# Give the process time to exit cleanly (jemalloc prof_final needs atexit)
sleep 2
# If still running, force kill
if kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "  Server did not exit cleanly, forcing kill..."
  kill -KILL "$SERVER_PID" 2>/dev/null || true
fi
wait "$SERVER_PID" 2>/dev/null || true

# ── Process CPU profile ─────────────────────────────────────────────

echo "Generating flamegraph..."
perf script -i "$OUTDIR/perf.data" | inferno-collapse-perf | inferno-flamegraph > "$OUTDIR/flamegraph.svg"

echo "Generating perf summary..."
perf report -i "$OUTDIR/perf.data" --stdio --no-children --percent-limit 0.5 > "$OUTDIR/perf-summary.txt" 2>/dev/null

# ── Process heap profile ─────────────────────────────────────────────

HEAP_FILE=""
for f in "$OUTDIR"/heap.*.heap; do
  if [[ -f "$f" ]]; then
    HEAP_FILE="$f"
  fi
done
if [[ -n "$HEAP_FILE" ]]; then
  echo "Processing heap profile: $(basename "$HEAP_FILE")"
  cp "$HEAP_FILE" "$OUTDIR/heap-final.heap"
  jeprof --text "$SERVER_BIN" "$OUTDIR/heap-final.heap" > "$OUTDIR/heap-summary.txt" 2>/dev/null || \
    echo "(jeprof failed — heap profile may still be usable with jeprof interactively)" > "$OUTDIR/heap-summary.txt"
else
  echo "No heap profile found (jemalloc may not have dumped)"
  echo "(no heap profile captured)" > "$OUTDIR/heap-summary.txt"
fi

# ── Summary ──────────────────────────────────────────────────────────

echo ""
echo "=== Done: $TARGET ==="
echo "  Flamegraph:    $OUTDIR/flamegraph.svg"
echo "  CPU summary:   $OUTDIR/perf-summary.txt"
echo "  Heap summary:  $OUTDIR/heap-summary.txt"
echo ""

# Print top 10 CPU functions
echo "Top CPU functions:"
head -30 "$OUTDIR/perf-summary.txt" | grep -E '^\s+[0-9]' | head -10 || true
echo ""
