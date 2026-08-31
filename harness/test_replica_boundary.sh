#!/usr/bin/env bash
#
# Verifies Phase 3 step 2: the ReplicateWrite/ReplicateRead RPC boundary,
# in isolation, with no coordinator/fan-out involved.
#
# Covers:
#   1. A direct write, then a direct read of the same key, round-trips.
#   2. A read for a key that was never written returns found=false.
#   3. Once a node knows it's REMOVED (self-targeted RemoveNode),
#      ReplicateWrite/ReplicateRead both refuse — and the post-removal
#      read is checked against "foo", a key we KNOW exists (written in
#      step 1), not a missing key, so a false-negative there can only
#      mean the REMOVED guard fired, not that the data was never there.
#
# NOT covered, by design: whether ReplicateWrite's REMOVED branch
# actually skipped localPut() internally, vs. writing then discarding.
# That's unobservable externally once REMOVED, since ReplicateRead is
# blocked too — this proves the guard's external effect, not its
# internal mechanism.
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
echo "=== test 3: A removes itself, then Replicate{Write,Read} must refuse ==="

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