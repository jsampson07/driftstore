#!/usr/bin/env bash
#
# Verifies phase4/observability-logging's instrumentation directly:
#   1. CONFLICT_RESOLVED fires when Get resolves a genuine conflict, and
#      carries concurrent_versions > 1.
#   2. EQUAL_CLOCK_DETECTED fires ONLY on a true duplicate delivery
#      (compareVectorClocks == EQUAL in storeReplicatedWrite) -- NOT on
#      ordinary replica agreement during a Get (computeFrontier's own
#      EQUAL-dedup step is a different, expected case and must stay silent
#      here; conflating the two sites was the mistake this test guards
#      against).
#   3. clock= is present on both PUT_SUCCEEDED and GET_SUCCEEDED lines.
#
# Deliberately does NOT use two real coordinators racing each other.
# Traced through what that would do: storeReplicatedWrite's CONCURRENT
# branch has no arbitration, so if coordinator A's Put (full fan-out
# included) finishes before coordinator B's Put starts, B's later fan-out
# just overwrites every replica uniformly -- no divergence survives to be
# observed, CONFLICT_RESOLVED never fires. Sequential two-coordinator puts
# cannot produce a conflict under the current design; only genuinely
# overlapping RPCs can, and that's a real race (non-deterministic on
# localhost). This script instead injects two pre-built CONCURRENT clocks
# directly via --replicate-write (same technique test_replica_boundary.sh
# uses) so the conflict is 100% reproducible.
#
# Known limitation this surfaces: --replicate-write's --clock= only sets
# the counters map -- last_updated/writer_id aren't settable via the CLI.
# So this script can assert a conflict was detected and resolved to ONE
# of the two injected values, but not WHICH one -- that tiebreak is
# test_vector_clocks.cpp's job, already covered there directly. If branch
# 8's two-coordinator scenario needs a deterministic winner, it'll need
# real overlapping --put calls (for authentic timestamps) launched with
# `&`, accepting the raciness, or client.cpp will need --last-updated=/
# --writer-id= flags added to --replicate-write.
#
# Run from the repo root (expects ./bin/node and ./bin/client to exist).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"

A="127.0.0.1:60071"
B="127.0.0.1:60072"
C="127.0.0.1:60073"
LOGDIR="./test_logs_conflict_observability"

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

pkill -9 -f "$NODE_BIN" 2>/dev/null || true   # Q11 -- stale stopped processes
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

echo "=== confirming 3-way gossip convergence (N=3 with 3 nodes -> every key's preference list is all three) ==="
STATUS_C=$("$CLIENT_BIN" --target="$C" --status 2>&1)
echo "$STATUS_C" | grep -q "table_size=3" \
    && pass "C's table converged to 3 entries" \
    || fail "C's table did not converge: $STATUS_C"

echo ""
echo "=== part 1: inject two genuinely CONCURRENT versions of the same key ==="
echo "    write1 -> A: counters={X:2,Y:1}   write2 -> B: counters={X:1,Y:2}"
echo "    (X favors write1, Y favors write2 -- neither dominates, by construction)"

W1_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="conflictkey=valA" --clock="X:2,Y:1" 2>&1)
echo "$W1_OUT" | grep -q "success=true" && echo "$W1_OUT" | grep -q "outcome=STORED" \
    && pass "write1 stored on A (fresh key, unconditional store)" \
    || fail "write1 did not report success=true outcome=STORED: $W1_OUT"

W2_OUT=$("$CLIENT_BIN" --target="$B" --replicate-write="conflictkey=valB" --clock="X:1,Y:2" 2>&1)
echo "$W2_OUT" | grep -q "success=true" && echo "$W2_OUT" | grep -q "outcome=STORED" \
    && pass "write2 stored on B (fresh key, unconditional store)" \
    || fail "write2 did not report success=true outcome=STORED: $W2_OUT"

echo ""
echo "=== issuing Get(conflictkey) against C (holds neither copy itself) ==="
GET_OUT=$("$CLIENT_BIN" --target="$C" --get="conflictkey" 2>&1)
echo "$GET_OUT"

echo "$GET_OUT" | grep -q "found=true" \
    && pass "Get(conflictkey) returned found=true" \
    || fail "Get(conflictkey) did not return found=true: $GET_OUT"

if echo "$GET_OUT" | grep -q "value=valA" || echo "$GET_OUT" | grep -q "value=valB"; then
    pass "Get(conflictkey) resolved to one of the two genuinely concurrent values"
else
    fail "Get(conflictkey) returned neither valA nor valB: $GET_OUT"
fi

echo ""
echo "=== correlating: C's own log must show the resolution, with clock= present ==="
grep -q "event=CONFLICT_RESOLVED key=conflictkey concurrent_versions=2" "$LOGDIR/node_c.log" \
    && pass "C logged CONFLICT_RESOLVED key=conflictkey concurrent_versions=2" \
    || fail "C's log missing CONFLICT_RESOLVED for conflictkey (check $LOGDIR/node_c.log)"

grep "event=GET_SUCCEEDED key=conflictkey" "$LOGDIR/node_c.log" | grep -q "clock=" \
    && pass "GET_SUCCEEDED(conflictkey) line carries clock=" \
    || fail "GET_SUCCEEDED(conflictkey) line missing clock= -- item 1's scope gap may still be open"

echo ""
echo "=== part 2: EQUAL_CLOCK_DETECTED canary -- true duplicate delivery to A ==="

E1_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="equalkey=first" --clock="Z:1" 2>&1)
echo "$E1_OUT" | grep -q "outcome=STORED" \
    && pass "equalkey's first delivery stored (outcome=STORED)" \
    || fail "equalkey's first delivery did not report outcome=STORED: $E1_OUT"

E2_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="equalkey=first" --clock="Z:1" 2>&1)
echo "$E2_OUT" | grep -q "outcome=ALREADY_CURRENT" \
    && pass "equalkey's exact-duplicate redelivery reports outcome=ALREADY_CURRENT" \
    || fail "equalkey's redelivery did not report outcome=ALREADY_CURRENT: $E2_OUT"

EQUAL_ON_A=$(grep -c "event=EQUAL_CLOCK_DETECTED key=equalkey" "$LOGDIR/node_a.log")
[[ "$EQUAL_ON_A" -eq 1 ]] \
    && pass "A logged EQUAL_CLOCK_DETECTED for equalkey exactly once" \
    || fail "expected exactly 1 EQUAL_CLOCK_DETECTED(equalkey) on A, got $EQUAL_ON_A"

echo ""
echo "=== part 3: sanity check clock= on an ordinary Put/Get (item 1's actual scope) ==="

PUT_OUT=$("$CLIENT_BIN" --target="$A" --put="normalkey=normalval" 2>&1)
echo "$PUT_OUT" | grep -q "success=true" \
    && pass "ordinary Put(normalkey) succeeded" \
    || fail "ordinary Put(normalkey) did not succeed: $PUT_OUT"

grep "event=PUT_SUCCEEDED key=normalkey" "$LOGDIR/node_a.log" | grep -q "clock=" \
    && pass "PUT_SUCCEEDED(normalkey) line carries clock=" \
    || fail "PUT_SUCCEEDED(normalkey) line missing clock="

GET2_OUT=$("$CLIENT_BIN" --target="$A" --get="normalkey" 2>&1)
echo "$GET2_OUT" | grep -q "found=true" && echo "$GET2_OUT" | grep -q "value=normalval" \
    && pass "ordinary Get(normalkey) round-tripped correctly" \
    || fail "ordinary Get(normalkey) did not round-trip: $GET2_OUT"

grep "event=GET_SUCCEEDED key=normalkey" "$LOGDIR/node_a.log" | grep -q "clock=" \
    && pass "GET_SUCCEEDED(normalkey) line carries clock=" \
    || fail "GET_SUCCEEDED(normalkey) line missing clock="

echo ""
echo "=== canary silence check: EQUAL_CLOCK_DETECTED must fire EXACTLY once total, across all three logs, for this whole run ==="
echo "    (the one legitimate fire is part 2's true duplicate -- part 1's genuine"
echo "    concurrency and part 3's ordinary put/get must NOT trigger it. If this"
echo "    count is anything other than 1, the canary was wired to the wrong EQUAL"
echo "    site -- likely computeFrontier's dedup step instead of storeReplicatedWrite.)"

TOTAL_EQUAL=$(cat "$LOGDIR/node_a.log" "$LOGDIR/node_b.log" "$LOGDIR/node_c.log" | grep -c "event=EQUAL_CLOCK_DETECTED")
[[ "$TOTAL_EQUAL" -eq 1 ]] \
    && pass "EQUAL_CLOCK_DETECTED fired exactly once across the whole run" \
    || fail "expected exactly 1 EQUAL_CLOCK_DETECTED total across all logs, got $TOTAL_EQUAL"

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1