#!/usr/bin/env bash
set -uo pipefail
cd ~/driftstore   # change to your repo root
pkill -9 -f "bin/node" 2>/dev/null || true
sleep 0.3

A="127.0.0.1:60051"
B="127.0.0.1:60052"
C="127.0.0.1:60053"

extract_ring() {
    # pulls "tokens=[a:x,b:y,...]" out of the client's status line and
    # writes one "token:node_id" pair per line, for a legible diff.
    sed -n 's/.*tokens=\[\(.*\)\]/\1/p' | tr ',' '\n'
}

echo "=== launching A, B ==="
./bin/node --listen="$A" --gossip-interval=300 > /tmp/node_a.log 2>&1 &
PID_A=$!
./bin/node --listen="$B" --seed="$A" --gossip-interval=300 > /tmp/node_b.log 2>&1 &
PID_B=$!

cleanup() {
    kill -9 "$PID_A" "$PID_B" "${PID_C:-}" 2>/dev/null || true
}
trap cleanup EXIT

sleep 2
echo "--- A status before join ---"
./bin/client --target="$A" --status | tee /tmp/status_before.txt
./bin/client --target="$A" --status | extract_ring > /tmp/ring_before.txt
echo "ring entries before join: $(wc -l < /tmp/ring_before.txt)"

echo ""
echo "=== joining C (seed=A) ==="
./bin/node --listen="$C" --seed="$A" --gossip-interval=300 > /tmp/node_c.log 2>&1 &
PID_C=$!

sleep 2
echo "--- A status after join ---"
./bin/client --target="$A" --status | tee /tmp/status_after.txt
./bin/client --target="$A" --status | extract_ring > /tmp/ring_after.txt
echo "ring entries after join: $(wc -l < /tmp/ring_after.txt)"

echo ""
echo "=== diff (ring_before -> ring_after) ==="
diff /tmp/ring_before.txt /tmp/ring_after.txt
DIFF_STATUS=$?

echo ""
if [[ "$DIFF_STATUS" -eq 0 ]]; then
    echo "RESULT: no diff at all (unexpected — join should have added tokens)"
elif diff /tmp/ring_before.txt /tmp/ring_after.txt | grep -q '^[<]'; then
    echo "RESULT: FAIL — some pre-existing token line was removed/changed, not a pure addition"
else
    echo "RESULT: PASS — pure additions only, no existing token line changed"
fi