#!/usr/bin/env bash
#
# Verifies Phase 3 step 2: the ReplicateWrite/ReplicateRead RPC boundary,
# in isolation, with no coordinator/fan-out involved.
#
# Covers:
#   1. A direct write, then a direct read of the same key, round-trips.
#   2. A read for a key that was never written returns found=false.
#   3. ReplicateWrite's replica-side defensive dominance check
#      (phase4/replicate-write-defensive-check), exercised directly via
#      the --clock flag on --replicate-write: a stale (dominated) write
#      is refused and does not overwrite; a newer (dominating) write
#      stores; a genuinely concurrent write stores anyway with no local
#      arbitration; an exact-clock replay (EQUAL) is refused. Each
#      refusal is checked by reading back afterward and confirming the
#      stored value/clock is unchanged — not just that outcome said the
#      right thing.
#   4. Once a node knows it's REMOVED (self-targeted RemoveNode),
#      ReplicateWrite/ReplicateRead both refuse — and the post-removal
#      read is checked against "foo", a key we KNOW exists (written in
#      step 1), not a missing key, so a false-negative there can only
#      mean the REMOVED guard fired, not that the data was never there.
#
# NOT covered, by design: whether ReplicateWrite's REMOVED branch
# actually skipped localPut() internally, vs. writing then discarding.
# That's unobservable externally once REMOVED, since ReplicateRead is
# blocked too — this proves the guard's external effect, not its
# internal mechanism. Also not covered here: LWW timestamp tiebreaking
# for concurrent writes reaching different replicas via different
# coordinators — this file stays below the coordinator layer by design;
# that's phase4/conflict-scenario-harness's job, exercised through Put.
#
# Run from the repo root (expects ./bin/node and ./bin/client to exist).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"

A="127.0.0.1:60051"
LOGDIR="./test_logs_replica_boundary"

PASS=0
FAIL=0

pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

PID_A=""
cleanup() {
    [[ -n "$PID_A" ]] && kill -9 "$PID_A" 2>/dev/null || true
}
trap cleanup EXIT

pkill -9 -f "$NODE_BIN" 2>/dev/null || true   # Q11
sleep 0.5

rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

echo "=== launching single node A (no seed needed for this test) ==="
"$NODE_BIN" --listen="$A" > "$LOGDIR/node_a.log" 2>&1 &
PID_A=$!
sleep 1

if ! kill -0 "$PID_A" 2>/dev/null; then
    fail "node A failed to start — check $LOGDIR/node_a.log"
    echo "=== summary: $PASS passed, $FAIL failed ==="
    exit 1
fi

echo ""
echo "=== test 1: direct ReplicateWrite then ReplicateRead ==="

WRITE_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="foo=bar" 2>&1)
echo "$WRITE_OUT" | grep -q "success=true" \
    && pass "ReplicateWrite(foo=bar) succeeded" \
    || fail "ReplicateWrite(foo=bar) did not report success=true: $WRITE_OUT"

READ_OUT=$("$CLIENT_BIN" --target="$A" --replicate-read="foo" 2>&1)
if echo "$READ_OUT" | grep -q "found=true" && echo "$READ_OUT" | grep -q "value=bar"; then
    pass "ReplicateRead(foo) returned found=true value=bar"
else
    fail "ReplicateRead(foo) did not round-trip correctly: $READ_OUT"
fi

echo ""
echo "=== test 2: ReplicateRead on a key that was never written ==="

MISSING_OUT=$("$CLIENT_BIN" --target="$A" --replicate-read="never_written_key" 2>&1)
echo "$MISSING_OUT" | grep -q "found=false" \
    && pass "ReplicateRead(never_written_key) returned found=false" \
    || fail "ReplicateRead(never_written_key) unexpectedly found something: $MISSING_OUT"

echo ""
echo "=== test 3: ReplicateWrite defensive dominance check ==="
# One key ("dk"), threaded sequentially through five writes -- mirrors how
# a replica actually receives ReplicateWrite calls over time, rather than
# five independent fixtures. Every clock below was handwritten and checked
# by hand against compareVectorClocks' rules before being encoded here;
# this bypasses coordinatePut/buildNewClock entirely, so nothing here
# depends on the coordinator computing anything correctly.
#
# Must run BEFORE test 4 (self-removal) -- once A is REMOVED it refuses
# every ReplicateWrite/ReplicateRead, which would make every check below
# vacuously "pass" for the wrong reason.

# (a) Baseline: brand-new key always stores, regardless of clock.
BASELINE_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="dk=v1" --clock="n1:1" 2>&1)
echo "$BASELINE_OUT" | grep -q "outcome=STORED" \
    && pass "dk=v1 (new key, clock n1:1) stored" \
    || fail "dk=v1 baseline write did not report STORED: $BASELINE_OUT"

READ_A=$("$CLIENT_BIN" --target="$A" --replicate-read="dk" 2>&1)
if echo "$READ_A" | grep -q "value=v1" && echo "$READ_A" | grep -q "clock=n1:1"; then
    pass "dk baseline round-trips as value=v1 clock=n1:1"
else
    fail "dk baseline did not round-trip: $READ_A"
fi

# (b) Stale write: an empty clock is dominated by n1:1 on every axis
#     (missing counters count as 0). Existing must win -- refuse, and the
#     stored value/clock must be UNCHANGED, not just the outcome label.
STALE_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="dk=v2-stale" 2>&1)
echo "$STALE_OUT" | grep -q "outcome=ALREADY_CURRENT" \
    && pass "dk=v2-stale (empty clock, dominated by n1:1) refused" \
    || fail "stale write was not refused: $STALE_OUT"

READ_B=$("$CLIENT_BIN" --target="$A" --replicate-read="dk" 2>&1)
if echo "$READ_B" | grep -q "value=v1" && echo "$READ_B" | grep -q "clock=n1:1"; then
    pass "dk unchanged after refused stale write (still v1 / n1:1)"
else
    fail "dk was overwritten despite an ALREADY_CURRENT outcome: $READ_B"
fi

# (c) Newer write: n1:2 strictly dominates n1:1 (same axis, higher count).
#     Must store.
NEWER_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="dk=v3" --clock="n1:2" 2>&1)
echo "$NEWER_OUT" | grep -q "outcome=STORED" \
    && pass "dk=v3 (clock n1:2, dominates n1:1) stored" \
    || fail "newer write was not stored: $NEWER_OUT"

READ_C=$("$CLIENT_BIN" --target="$A" --replicate-read="dk" 2>&1)
if echo "$READ_C" | grep -q "value=v3" && echo "$READ_C" | grep -q "clock=n1:2"; then
    pass "dk now shows v3 / n1:2"
else
    fail "dk did not update to v3 / n1:2: $READ_C"
fi

# (d) Genuine conflict: n1:1,n2:1 against existing n1:2. Existing leads on
#     n1; incoming leads on n2, an axis existing doesn't have at all --
#     neither dominates. Per decision C1: no local arbitration, store
#     anyway. Worth its own case distinct from (c): (c) proves
#     DOMINATED->STORED, this proves CONCURRENT->STORED -- a different
#     branch condition that "it just stores either way" makes easy to
#     under-test.
CONCURRENT_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="dk=v4" --clock="n1:1,n2:1" 2>&1)
echo "$CONCURRENT_OUT" | grep -q "outcome=STORED" \
    && pass "dk=v4 (clock n1:1,n2:1, concurrent with n1:2) stored, no arbitration" \
    || fail "concurrent write was not stored: $CONCURRENT_OUT"

READ_D=$("$CLIENT_BIN" --target="$A" --replicate-read="dk" 2>&1)
if echo "$READ_D" | grep -q "value=v4" && echo "$READ_D" | grep -q "clock=n1:1,n2:1"; then
    pass "dk now shows v4 / n1:1,n2:1 (concurrent write applied as-is)"
else
    fail "dk did not update to the concurrent write: $READ_D"
fi

# (e) Exact-clock replay with a DIFFERENT value than what's currently
#     stored. Deliberately not replaying the same value: if v4 were
#     resent, "unchanged" would be true whether or not the guard actually
#     fired. A different value makes this a real test that EQUAL refuses
#     to overwrite, not a coincidence of idempotency.
REPLAY_OUT=$("$CLIENT_BIN" --target="$A" --replicate-write="dk=v5-should-not-land" --clock="n1:1,n2:1" 2>&1)
echo "$REPLAY_OUT" | grep -q "outcome=ALREADY_CURRENT" \
    && pass "dk=v5 (identical clock to v4, EQUAL case) refused" \
    || fail "exact-clock replay was not refused: $REPLAY_OUT"

READ_E=$("$CLIENT_BIN" --target="$A" --replicate-read="dk" 2>&1)
if echo "$READ_E" | grep -q "value=v4" && echo "$READ_E" | grep -q "clock=n1:1,n2:1"; then
    pass "dk still shows v4 (EQUAL-clock replay did not overwrite)"
else
    fail "dk was overwritten by an EQUAL-clock replay: $READ_E"
fi

echo ""
echo "=== test 4: A removes itself, then Replicate{Write,Read} must refuse ==="

REMOVE_OUT=$("$CLIENT_BIN" --target="$A" --remove="$A" 2>&1)
echo "$REMOVE_OUT" | grep -q "accepted=true" \
    && pass "RemoveNode(self) accepted" \
    || fail "RemoveNode(self) not accepted: $REMOVE_OUT"

STATUS_OUT=$("$CLIENT_BIN" --target="$A" --status 2>&1)
echo "$STATUS_OUT" | grep -q "${A}:REMOVED" \
    && pass "A's own table entry shows REMOVED (no gossip delay — self-targeted)" \
    || fail "A's table does not show itself REMOVED: $STATUS_OUT"

# "foo" is KNOWN to exist (test 1 wrote it). found=false here can only
# be the REMOVED guard, not missing data.
POST_REMOVE_READ=$("$CLIENT_BIN" --target="$A" --replicate-read="foo" 2>&1)
echo "$POST_REMOVE_READ" | grep -q "found=false" \
    && pass "ReplicateRead(foo) refused post-removal despite foo existing (guard fired)" \
    || fail "ReplicateRead(foo) did NOT refuse post-removal: $POST_REMOVE_READ"

POST_REMOVE_WRITE=$("$CLIENT_BIN" --target="$A" --replicate-write="foo2=baz" 2>&1)
echo "$POST_REMOVE_WRITE" | grep -q "success=false" \
    && pass "ReplicateWrite(foo2=baz) refused post-removal" \
    || fail "ReplicateWrite(foo2=baz) did NOT refuse post-removal: $POST_REMOVE_WRITE"

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1