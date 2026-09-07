#!/usr/bin/env bash
#
# phase4/conflict-scenario-harness (branch 8) -- the REAL two-coordinator
# concurrent-write scenario, as opposed to branch 7's
# test_conflict_observability.sh, which verified the logging in isolation
# via direct --replicate-write injection and never touched a coordinator.
#
# THE MECHANISM, precisely:
#   1. Two Put()s to the SAME brand-new key, through two DIFFERENT
#      coordinators, must genuinely OVERLAP in real time -- not just be
#      issued close together. If coordinator A's entire Put (fan-out
#      included) finishes before coordinator B's Put even starts,
#      storeReplicatedWrite's no-arbitration policy on CONCURRENT clocks
#      means B's later, fully-sequential fan-out just overwrites every
#      replica uniformly. No disagreement survives -- sequential
#      two-coordinator puts cannot produce a conflict under this design.
#   2. Given genuine overlap, each coordinator independently computes its
#      own clock via buildNewClock against ITS OWN local state at that
#      instant. As long as neither coordinator's copy was already
#      overwritten by the other's ReplicateWrite before it commits, the
#      two resulting clocks are causally CONCURRENT (neither dominates)
#      -- this falls out of (1), it isn't a separate thing to arrange.
#   3. Every replica applies "last physical arrival wins" for CONCURRENT
#      clocks (no arbitration), and arrival order can differ per-replica
#      under a real race -- so different replicas can end up holding
#      DIFFERENT final values even after BOTH Puts have fully returned.
#      Genuine, silent AP divergence.
#   4. A subsequent Get() fans out ReplicateRead, collects the disagreeing
#      versions, computeFrontier keeps both, resolveLWW inside
#      resolveGetResult picks a winner -- CONFLICT_RESOLVED fires off a
#      REAL race here, not an injected one.
#
# FLAKINESS -- accepted deliberately, not an oversight: whether step 1's
# overlap produces step 3's divergence is a genuine race, non-deterministic
# on localhost. This script does not try to force it. Instead: retry a
# FRESH key up to MAX_ATTEMPTS times (fresh key each time, so no
# cross-attempt contamination of vector-clock state), and on EVERY
# attempt -- whether divergence happened or not -- check a hard invariant
# that must hold regardless of the race's outcome:
#
#     replicas actually disagree (ground truth, via direct
#     --replicate-read on all three, bypassing Get's own resolution)
#       <==>
#     CONFLICT_RESOLVED was logged on the Get-coordinating node
#
# This turns "the race is flaky" into "the race's OCCURRENCE is flaky,
# but the SYSTEM'S REACTION to it, whenever it occurs, is not." If genuine
# divergence is never observed across all attempts, that's reported as a
# failure at the end -- a run that never actually triggered the scenario
# hasn't tested anything, even if every individual check trivially passed.
#
# NOT verified here, and can't be with current instrumentation: WHICH of
# the two values SHOULD have won under LWW. renderClock() (node_kv.cpp)
# and client.cpp's clockToString() only render the counters map --
# last_updated and writer_id aren't exposed on any log line or CLI output
# anywhere in the codebase. So this confirms a conflict was correctly
# detected and resolved to ONE of the two legitimate values, not that it
# resolved to the CORRECT one. Closing that gap needs last_updated/
# writer_id surfaced somewhere observable -- flagged, not a prerequisite.
#
# Run from the repo root (expects ./bin/node and ./bin/client to exist).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"

A="127.0.0.1:60081"
B="127.0.0.1:60082"
C="127.0.0.1:60083"
LOGDIR="./test_logs_conflict_scenario"
MAX_ATTEMPTS=20

PASS=0
FAIL=0
pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

# field "<line>" "<name>" -> the value after "<name>=", up to the next space
field() { echo "$1" | sed -n "s/.*$2=\([^ ]*\).*/\1/p"; }

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
echo "=== racing up to $MAX_ATTEMPTS attempts: two concurrent Put()s per attempt, fresh key each time ==="

DIVERGED_COUNT=0
for i in $(seq 1 "$MAX_ATTEMPTS"); do
    KEY="racekey_$i"
    OUT_A="$LOGDIR/put_a_$i.out"
    OUT_B="$LOGDIR/put_b_$i.out"

    # Fire both puts as close to simultaneously as bash allows -- no sleep
    # between them, that IS the test.
    "$CLIENT_BIN" --target="$A" --put="$KEY=valA" > "$OUT_A" 2>&1 &
    PUT_A_PID=$!
    "$CLIENT_BIN" --target="$B" --put="$KEY=valB" > "$OUT_B" 2>&1 &
    PUT_B_PID=$!
    wait "$PUT_A_PID" "$PUT_B_PID"

    if ! grep -q "success=true" "$OUT_A" || ! grep -q "success=true" "$OUT_B"; then
        fail "attempt $i: a concurrent put failed outright (not just non-divergence) -- A: $(cat "$OUT_A") | B: $(cat "$OUT_B")"
        continue
    fi

    # Ground truth: read all three replicas directly, bypassing Get's own
    # resolution entirely -- this is what "diverged" actually means.
    READ_A=$("$CLIENT_BIN" --target="$A" --replicate-read="$KEY" 2>&1)
    READ_B=$("$CLIENT_BIN" --target="$B" --replicate-read="$KEY" 2>&1)
    READ_C=$("$CLIENT_BIN" --target="$C" --replicate-read="$KEY" 2>&1)

    if echo "$READ_A" | grep -q "found=false" || echo "$READ_B" | grep -q "found=false" || echo "$READ_C" | grep -q "found=false"; then
        fail "attempt $i: a replica reports found=false for $KEY after both puts returned -- a delivery anomaly, not ordinary non-divergence. A: $READ_A | B: $READ_B | C: $READ_C"
        continue
    fi

    VAL_A=$(field "$READ_A" value)
    VAL_B=$(field "$READ_B" value)
    VAL_C=$(field "$READ_C" value)

    DIVERGED=false
    if [[ "$VAL_A" != "$VAL_B" || "$VAL_B" != "$VAL_C" ]]; then
        DIVERGED=true
        DIVERGED_COUNT=$((DIVERGED_COUNT + 1))
    fi

    GET_OUT=$("$CLIENT_BIN" --target="$C" --get="$KEY" 2>&1)
    CONFLICT_LOGGED=false
    grep -q "event=CONFLICT_RESOLVED key=$KEY" "$LOGDIR/node_c.log" && CONFLICT_LOGGED=true

    if [[ "$DIVERGED" == true && "$CONFLICT_LOGGED" == true ]]; then
        pass "attempt $i: replicas diverged (A=$VAL_A B=$VAL_B C=$VAL_C) -- CONFLICT_RESOLVED correctly logged"
        if echo "$GET_OUT" | grep -q "value=valA" || echo "$GET_OUT" | grep -q "value=valB"; then
            pass "attempt $i: Get resolved to one of the two legitimate values"
        else
            fail "attempt $i: Get resolved to neither valA nor valB: $GET_OUT"
        fi
    elif [[ "$DIVERGED" == false && "$CONFLICT_LOGGED" == false ]]; then
        echo "attempt $i: no divergence this round (A=B=C=$VAL_A) -- not a failure, the race just didn't land this time"
    elif [[ "$DIVERGED" == true && "$CONFLICT_LOGGED" == false ]]; then
        fail "attempt $i: replicas ACTUALLY diverged (A=$VAL_A B=$VAL_B C=$VAL_C) but CONFLICT_RESOLVED was NOT logged -- Get silently picked a value without detecting the conflict"
    else
        fail "attempt $i: CONFLICT_RESOLVED was logged but replicas actually agree (A=B=C=$VAL_A) -- false-positive conflict detection"
    fi
done

echo ""
echo "=== divergence rate ==="
echo "genuine replica divergence observed in $DIVERGED_COUNT / $MAX_ATTEMPTS attempts"
if [[ "$DIVERGED_COUNT" -eq 0 ]]; then
    fail "never observed genuine divergence across $MAX_ATTEMPTS attempts -- this run did not actually exercise the two-coordinator conflict scenario, regardless of how many individual checks passed. Consider raising MAX_ATTEMPTS, or the localhost race window may be too narrow to hit this way at all (a test-only hook to force interleaving deterministically would be the next step if so)."
fi

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs and per-attempt put outputs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1