#!/usr/bin/env bash
#
# Verifies DriftClient's per-call coordinator rotation together with
# Phase 3's W<N tolerance for an unreachable replica.
#
# Scenario: 3-node cluster (N=3, W=2, R=2), kill node C IMMEDIATELY
# (no wait for the probe-interval to mark it locally unreachable — this
# is the sharper test, since C's fan-out/coordinator RPCs must actually
# fail live, not be pre-filtered out of the preference list). Then issue
# 3 put() calls through ONE DriftClient, seeds=[A,B,C] in that order.
# Round-robin means:
#   call 0 -> coordinator A (alive)   -- ordinary path
#   call 1 -> coordinator B (alive)   -- ordinary path
#   call 2 -> coordinator C (DEAD)    -- transport failure, DriftClient
#             walks forward and wraps to A (start=2, size=3: next index
#             is (2+1)%3=0=A before it'd reach B) -- so A ends up
#             coordinating BOTH call 0 and call 2, B only call 1. This
#             is the one call that exercises DriftClient's failover, not
#             just the coordinator-side W-of-N fan-out logic already
#             covered by test_replica_boundary.sh.
# All three calls must report success=true: W=2 is achievable from the
# two surviving replicas (A, B) regardless of which of them coordinates.
#
# Log correlation: Put's own PUT_SUCCEEDED/PUT_FAILED lines carry
# "key=<key>" (confirmed in the currently-uploaded node.cpp).
# ReplicateWrite/ReplicateRead are deliberately NOT logged (accepted
# tradeoff against log clutter) so this harness does NOT check replica
# logs for evidence of the write landing -- Put's own acks count already
# proves quorum was reached; this section only proves WHICH node
# actually coordinated which call, since that's the thing rotation is
# supposed to produce and Put's log line is the only place it's
# observable. Given the rotation math above:
#   node A's log must show exactly 2 "PUT_SUCCEEDED key=rotkey" lines
#   node B's log must show exactly 1
#
# Run from the repo root (expects ./bin/node, ./bin/client, and
# ./bin/driftclient to exist -- see Makefile).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"
DRIVER_BIN="./bin/driftclient"

A="127.0.0.1:60061"
B="127.0.0.1:60062"
C="127.0.0.1:60063"
LOGDIR="./test_logs_coordinator_rotation"

PASS=0
FAIL=0
pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

PID_A=""
PID_B=""
PID_C=""
cleanup() {
    kill -9 "$PID_A" "$PID_B" "$PID_C" 2>/dev/null || true
}
trap cleanup EXIT

pkill -9 -f "$NODE_BIN" 2>/dev/null || true   # Q11 — stale stopped processes
sleep 0.5

rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

echo "=== launching A, B, C (N=3 W=2 R=2) ==="
"$NODE_BIN" --listen="$A" --N=3 --W=2 --R=2 --gossip-interval=300 --probe-interval=3000 \
    > "$LOGDIR/node_a.log" 2>&1 &
PID_A=$!
"$NODE_BIN" --listen="$B" --seed="$A" --N=3 --W=2 --R=2 --gossip-interval=300 --probe-interval=3000 \
    > "$LOGDIR/node_b.log" 2>&1 &
PID_B=$!
"$NODE_BIN" --listen="$C" --seed="$A" --N=3 --W=2 --R=2 --gossip-interval=300 --probe-interval=3000 \
    > "$LOGDIR/node_c.log" 2>&1 &
PID_C=$!

sleep 2

echo "=== confirming 3-way gossip convergence before killing anything ==="
STATUS_A=$("$CLIENT_BIN" --target="$A" --status 2>&1)
echo "$STATUS_A" | grep -q "table_size=3" \
    && pass "A's table converged to 3 entries" \
    || fail "A's table did not converge: $STATUS_A"

echo ""
echo "=== killing C immediately — no wait for the probe interval ==="
kill -9 "$PID_C"
PID_C=""

echo ""
echo "=== issuing 3 put() calls through one DriftClient (seeds=A,B,C) ==="
DRIVER_OUT=$("$DRIVER_BIN" --seeds="$A,$B,$C" --key="rotkey" --value="rotval" --calls=3 2>&1)
echo "$DRIVER_OUT"

echo "$DRIVER_OUT" | grep -q "connect result=succeeded" \
    && pass "DriftClient connected (A answered Ping before C died)" \
    || fail "DriftClient failed to connect: $DRIVER_OUT"

for i in 0 1 2; do
    echo "$DRIVER_OUT" | grep -q "call=$i success=true" \
        && pass "call=$i succeeded" \
        || fail "call=$i did not report success=true: $DRIVER_OUT"
done

echo ""
echo "=== correlating: which node's Put log shows key=rotkey, and how many times ==="
A_COUNT=$(grep -c "event=PUT_SUCCEEDED key=rotkey" "$LOGDIR/node_a.log")
B_COUNT=$(grep -c "event=PUT_SUCCEEDED key=rotkey" "$LOGDIR/node_b.log")
echo "node A PUT_SUCCEEDED(rotkey) count=$A_COUNT   node B PUT_SUCCEEDED(rotkey) count=$B_COUNT"

[[ "$A_COUNT" -eq 2 ]] \
    && pass "A coordinated exactly 2 calls (call 0 directly, call 2 via failover from dead C)" \
    || fail "expected A_COUNT=2, got $A_COUNT"

[[ "$B_COUNT" -eq 1 ]] \
    && pass "B coordinated exactly 1 call (call 1)" \
    || fail "expected B_COUNT=1, got $B_COUNT"

[[ $((A_COUNT + B_COUNT)) -eq 3 ]] \
    && pass "total coordinated calls across survivors == 3 (all calls accounted for)" \
    || fail "A_COUNT + B_COUNT != 3 -- a call is unaccounted for"

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1