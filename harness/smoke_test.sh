#!/usr/bin/env bash
set -euo pipefail

# ---- config ----
DRIFTSTORE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GTSTORE_ROOT="${GTSTORE_ROOT:-$DRIFTSTORE_ROOT/../DistributedSystems/gtstore}"

# we use 60000s because GTStore uses 50000s for servers
DRIFTSTORE_NODE_ADDR="127.0.0.1:60051"
DRIFTSTORE_CLIENT_SELF="smoke-test-client"

GTSTORE_N=1
GTSTORE_K=1
GTSTORE_STORAGE_PORT=50053 # arbitrary port start for storage node in GTStore

PIDS=()
# cleanup processes
cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# ---- GTStore ----
echo "=== GTStore ==="
# start GTStore manager
"$GTSTORE_ROOT/bin/manager" -n "$GTSTORE_N" -k "$GTSTORE_K" &
PIDS+=($!)
sleep 1   # let the manager come up before storage tries to register with it

# start GTStore storage node
"$GTSTORE_ROOT/bin/storage" --port "$GTSTORE_STORAGE_PORT" &
PIDS+=($!)
sleep 1   # let storage finish registering before test_app hits the manager

# put <smoketest, hello> into GTStore
if "$GTSTORE_ROOT/bin/test_app" --put smoketest --val hello \
   && "$GTSTORE_ROOT/bin/test_app" --get smoketest; then
    echo "PASS: GTStore"
    gtstore_result=0
else
    echo "FAIL: GTStore"
    gtstore_result=1
fi

# ---- Driftstore ----
echo "=== Driftstore ==="
# start driftstore node on provided addr
"$DRIFTSTORE_ROOT/bin/node" --listen="$DRIFTSTORE_NODE_ADDR" &
PIDS+=($!)
sleep 2

# when client is successful (with provided identifier): comm with target node
if "$DRIFTSTORE_ROOT/bin/client" --target="$DRIFTSTORE_NODE_ADDR" --self="$DRIFTSTORE_CLIENT_SELF"; then
    echo "PASS: Driftstore"
    driftstore_result=0
else
    echo "FAIL: Driftstore"
    driftstore_result=1
fi

# ---- summary ----
echo "=== Summary ==="
echo "GTStore:    $([ $gtstore_result -eq 0 ] && echo PASS || echo FAIL)"
echo "Driftstore: $([ $driftstore_result -eq 0 ] && echo PASS || echo FAIL)"

exit $(( gtstore_result || driftstore_result ))