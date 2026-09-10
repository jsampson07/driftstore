#!/usr/bin/env bash
#
# feature/read-repair -- verifies Phase 6: a Get() that finds a replica
# behind the resolved winner pushes the winner back to it, without
# hinted handoff ever being involved.
#
# THE SETUP, precisely, and why it's deliberately NOT a 4-node
# hint-holder scenario:
#   With N=3 and exactly 3 total nodes, killing one replica (C) before a
#   Put leaves it durably missing the write, with no hint created for it
#   ANYWHERE -- but the actual mechanism is upstream of what you might
#   expect, and is worth being precise about:
#
#   ring.hpp's preferenceList increments natural_count for every distinct
#   candidate it walks past, reachable or not -- an unreachable candidate
#   just gets deferred into pending_hints instead of added to the
#   returned list. Filling that slot from pending_hints requires finding
#   a DIFFERENT, not-yet-seen, reachable candidate later in the walk. With
#   only 3 physical nodes total, once all 3 have been visited once,
#   there's nobody left to promote -- the ring walk returns a 2-element
#   list, not a 3-element list with a hint marker. So if C is already in
#   unreachable_peers_ (this script's 1s post-kill sleep gives gossip's
#   own markUnreachable-on-failure path -- node_membership.cpp -- a
#   couple of chances to notice C is down) by the time preferenceListForKey
#   runs, coordinatePut never dials C at all: it's simply not a pref_list
#   member, so coordinatePut's own findSubstitute/HINT_SEARCH_EXHAUSTED
#   fallback (which only fires when a LISTED member's live RPC fails
#   mid-fan-out) never gets exercised either.
#
#   The end state is identical either way -- C misses the write, W is
#   still met via the coordinator + one live replica, no hint exists for
#   C anywhere -- so the check below asserts that invariant directly
#   (no HINT_CREATED/HINT_STORED for these keys, full stop) rather than
#   asserting which specific code path produced it. That also makes the
#   check correct regardless of whether gossip's random target selection
#   happens to hit C within the sleep window on a given run.
#
#   Separately worth noting for anyone reading this later: this means
#   N == total cluster size is a case where an unreachable replica's slot
#   just silently shrinks the preference list rather than ever getting a
#   hint-holder substituted -- test_hinted_handoff.sh's 4-node setup
#   exists specifically so hinted handoff's own code never has to hit
#   this edge case. Worth its own look, not something this script fixes.
#
# Two sub-scenarios, both built from one setup, exercising both branches
# Get's repair logic added:
#   1. Get coordinated by A (a node that IS current) -- C is a remote
#      stale peer. Exercises repairReplicas()'s detached-thread path.
#   2. Get coordinated by C itself (the stale node). Exercises the
#      synchronous self-repair storeReplicatedWrite() call -- must
#      already be true by the time Get() returns, no sleep to tolerate.
#
# NOTE on --clock=: client.cpp's --replicate-write --clock= was
# considered as a way to inject staleness directly and skip the kill/
# restart cycle. It doesn't work: parseClockArg splits a "node_id:counter"
# pair on the FIRST colon it finds, and node_id IS this project's listen
# address (e.g. "127.0.0.1:60051", per Q7), which already contains a
# colon. Any --clock= argument naming a real node_id in this codebase is
# currently unparseable. Not fixed here -- flagged as a client.cpp bug,
# not worked around silently. This is also *why* this script uses a real
# kill/restart rather than direct injection, not just a style choice.
#
# Run from the repo root (expects ./bin/node and ./bin/client to exist).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"

A="127.0.0.1:60051"
B="127.0.0.1:60052"
C="127.0.0.1:60053"

GOSSIP_INTERVAL=300
PROBE_INTERVAL=1000
LOGDIR="./test_logs_read_repair"

PASS=0
FAIL=0
pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

# field "<line>" "<name>" -> the value after "<name>=", up to the next space
field() { echo "$1" | sed -n "s/.*$2=\([^ ]*\).*/\1/p"; }

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

pkill -9 -f "$NODE_BIN" 2>/dev/null || true   # Q11 -- stale processes from
                                               # earlier runs silently hold
                                               # a listen socket.
sleep 0.5

rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

status_dump() { "$CLIENT_BIN" --target="$1" --status 2>/dev/null; }

wait_for() {
    # Table-state polling (table_size=N, <node_id>:STATUS). See wait_for_log
    # for log-event checks -- those never show up in a status dump.
    local target="$1" needle="$2" desc="$3" timeout="${4:-20}"
    local elapsed=0
    while (( elapsed < timeout )); do
        if status_dump "$target" | grep -q -- "$needle"; then
            pass "$desc (took ~${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    fail "$desc (timed out after ${timeout}s)"
    return 1
}

wait_for_log() {
    local logfile="$1" needle="$2" desc="$3" timeout="${4:-20}"
    local elapsed=0
    while (( elapsed < timeout )); do
        if grep -q -- "$needle" "$logfile" 2>/dev/null; then
            pass "$desc (took ~${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    fail "$desc (timed out after ${timeout}s)"
    return 1
}

# NOTE: direct redirection (`> file 2>&1 &`), not `| tee file &` -- piping
# through tee would make $! capture tee's PID, not node's.

echo "=== launching A, B, C (N=3 W=2 R=2) ==="
"$NODE_BIN" --listen="$A" --N=3 --W=2 --R=2 \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_a.log" 2>&1 &
PIDS+=($!)
"$NODE_BIN" --listen="$B" --seed="$A" --N=3 --W=2 --R=2 \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_b.log" 2>&1 &
PIDS+=($!)
"$NODE_BIN" --listen="$C" --seed="$A" --N=3 --W=2 --R=2 \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_c.log" 2>&1 &
PIDS+=($!)

sleep 1
echo "=== waiting for 3-way convergence ==="
wait_for "$A" "table_size=3" "A sees all 3 members" || exit 1
wait_for "$B" "table_size=3" "B sees all 3 members" || exit 1
wait_for "$C" "table_size=3" "C sees all 3 members" || exit 1

# --- build the stale-C setup, once, for both sub-scenarios ---------------

echo ""
echo "=== killing C, writing two keys while C is down ==="
kill -9 "${PIDS[2]}" 2>/dev/null || true
sleep 1

KEY1="rrkey_remote"   # scenario 1: A coordinates the repairing Get
KEY2="rrkey_self"     # scenario 2: C coordinates its own repairing Get
VAL1="v1"
VAL2="v2"

PUT1=$("$CLIENT_BIN" --target="$A" --put="${KEY1}=${VAL1}" 2>&1)
echo "$PUT1" | grep -q "success=true" \
    && pass "Put($KEY1) succeeded with C down" \
    || fail "Put($KEY1) did not succeed: $PUT1"
echo "$PUT1" | grep -q "acks=2" \
    && pass "Put($KEY1) acks=2 (coordinator A + live replica B only)" \
    || fail "Put($KEY1) acks unexpected: $PUT1"

PUT2=$("$CLIENT_BIN" --target="$A" --put="${KEY2}=${VAL2}" 2>&1)
echo "$PUT2" | grep -q "success=true" \
    && pass "Put($KEY2) succeeded with C down" \
    || fail "Put($KEY2) did not succeed: $PUT2"

# Mechanism-agnostic on purpose -- see header comment. C may have been
# excluded upstream (never dialed, ring-walk had no substitute to
# promote) or downstream (dialed, failed, findSubstitute exhausted).
# Either way, no hint should exist for either key.
NOHINT=$(grep -E "HINT_CREATED|HINT_STORED" "$LOGDIR"/node_*.log 2>/dev/null | grep -E "key=($KEY1|$KEY2)" || true)
if [[ -z "$NOHINT" ]]; then
    pass "no hint was created anywhere for $KEY1 or $KEY2 -- read-repair is C's only path back"
else
    fail "a hint WAS created for one of these keys -- scenario invalid, not isolating read-repair: $NOHINT"
fi

if grep -q "event=GOSSIP_FAILED target=$C" "$LOGDIR/node_a.log" 2>/dev/null; then
    echo "INFO: C was excluded upstream (gossip marked it unreachable before the Put ran, per the header comment)"
else
    echo "INFO: C was likely excluded via coordinatePut's own findSubstitute fallback instead -- both are valid for this test"
fi

DIRECT_READ_A_1=$("$CLIENT_BIN" --target="$A" --replicate-read="$KEY1" 2>&1)
echo "$DIRECT_READ_A_1" | grep -q "found=true" && echo "$DIRECT_READ_A_1" | grep -q "value=$VAL1" \
    && pass "A directly holds $KEY1=$VAL1 right after the Put (sanity)" \
    || fail "A does not hold $KEY1=$VAL1 as expected: $DIRECT_READ_A_1"

echo ""
echo "=== restarting C ==="
"$NODE_BIN" --listen="$C" --seed="$A" --N=3 --W=2 --R=2 \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_c_restart.log" 2>&1 &
PIDS[2]=$!   # replace tracked PID so cleanup kills the *new* C process

# NOT waiting on NODE_REBOOTED here: mergeInto only logs that on a
# REMOVED->UP transition (ring.hpp). C was never RemoveNode'd in this
# script -- its gossiped status stayed UP the whole time it was dead, a
# crash isn't a gossip-level event -- so that condition can never be met
# and this would time out every single run. verify_remove_reboot.sh's
# Test 3 only sees NODE_REBOOTED because Test 2, earlier in that same
# script, explicitly removes the node first.
#
# What actually gates scenario 1 below is A's LOCAL reachability view
# (Phase 1's second, non-gossiped mechanism) -- preferenceListForKey
# excludes anyone still in unreachable_peers_, so if A hasn't yet
# rediscovered C, its Get would silently exclude C from the preference
# list entirely rather than fail loudly. reachabilityRound's own
# pingPeer logs PROBE_SUCCEEDED the instant a probe succeeds, right
# before markReachable runs -- that's the real signal to wait on.
wait_for_log "$LOGDIR/node_a.log" "event=PROBE_SUCCEEDED target=$C" "A's reachability probe confirms C is back" || exit 1
sleep 1   # PROBE_SUCCEEDED logs a hair before markReachable/deliverHints
          # actually complete in the caller -- negligible gap, small buffer anyway

# Ground truth before trusting anything else: C genuinely still lacks
# both writes. If this fails, nothing below this point proves anything.
C_PRECHECK1=$("$CLIENT_BIN" --target="$C" --replicate-read="$KEY1" 2>&1)
echo "$C_PRECHECK1" | grep -q "found=false" \
    && pass "C confirmed still missing $KEY1 pre-repair (ground truth)" \
    || fail "C already has $KEY1 before any Get happened -- scenario invalid: $C_PRECHECK1"

# --- scenario 1: A coordinates, C is a remote stale peer -----------------

echo ""
echo "=== scenario 1: Get($KEY1) via A -- exercises repairReplicas() detached-thread path ==="

GET1=$("$CLIENT_BIN" --target="$A" --get="$KEY1" 2>&1)
echo "$GET1" | grep -q "value=$VAL1" \
    && pass "Get($KEY1) via A returned the correct winning value ($VAL1)" \
    || fail "Get($KEY1) via A returned wrong/missing value: $GET1"

grep -q "event=READ_REPAIR_INIT key=$KEY1" "$LOGDIR/node_a.log" \
    && pass "READ_REPAIR_INIT logged on A for $KEY1" \
    || fail "READ_REPAIR_INIT not logged on A for $KEY1"

# Repair runs on a detached thread the client's response didn't wait on --
# give it a moment before checking for its effects.
sleep 2

grep -q "event=READ_REPAIR_SUCCEEDED key=$KEY1 peer=$C outcome=STORED" "$LOGDIR/node_a.log" \
    && pass "READ_REPAIR_SUCCEEDED logged on A for $KEY1 -> C, outcome=STORED" \
    || fail "READ_REPAIR_SUCCEEDED not logged as expected: $(grep READ_REPAIR "$LOGDIR/node_a.log")"

# The actual proof: read C directly, bypassing Get's own resolution.
C_AFTER_1=$("$CLIENT_BIN" --target="$C" --replicate-read="$KEY1" 2>&1)
echo "$C_AFTER_1" | grep -q "found=true" && echo "$C_AFTER_1" | grep -q "value=$VAL1" \
    && pass "direct replicate-read on C confirms $KEY1=$VAL1 after repair" \
    || fail "C does not actually hold the repaired value: $C_AFTER_1"

grep -q "HINT_DELIVERED.*key=$KEY1" "$LOGDIR"/node_*.log 2>/dev/null \
    && fail "HINT_DELIVERED fired for $KEY1 -- repair came from hinted handoff, not read-repair, invalidating this scenario" \
    || pass "no HINT_DELIVERED anywhere for $KEY1 -- repair is attributable to read-repair alone"

# --- scenario 2: C coordinates its own Get, self-repair path -------------

echo ""
echo "=== scenario 2: Get($KEY2) via C itself -- exercises synchronous self-repair path ==="

C_PRECHECK2=$("$CLIENT_BIN" --target="$C" --replicate-read="$KEY2" 2>&1)
echo "$C_PRECHECK2" | grep -q "found=false" \
    && pass "C confirmed still missing $KEY2 pre-repair (ground truth)" \
    || fail "C already has $KEY2 before its own Get -- scenario invalid: $C_PRECHECK2"

GET2=$("$CLIENT_BIN" --target="$C" --get="$KEY2" 2>&1)
echo "$GET2" | grep -q "value=$VAL2" \
    && pass "Get($KEY2) via C itself returned the correct winning value ($VAL2)" \
    || fail "Get($KEY2) via C returned wrong/missing value: $GET2"

# Self-repair is synchronous -- must already be true by the time Get
# returned. No sleep here on purpose: if this needed one, that would
# itself be evidence the self-repair branch isn't actually synchronous.
grep -q "event=READ_REPAIR_INIT key=$KEY2" "$LOGDIR/node_c_restart.log" \
    && pass "READ_REPAIR_INIT logged on C for $KEY2" \
    || fail "READ_REPAIR_INIT not logged on C for $KEY2"

C_AFTER_2=$("$CLIENT_BIN" --target="$C" --replicate-read="$KEY2" 2>&1)
echo "$C_AFTER_2" | grep -q "found=true" && echo "$C_AFTER_2" | grep -q "value=$VAL2" \
    && pass "C's own local store holds $KEY2=$VAL2 immediately after its own Get returned" \
    || fail "C's local store not repaired synchronously: $C_AFTER_2"

# --- final cross-replica ground truth for both keys -----------------------

echo ""
echo "=== final check: all three replicas agree on both keys ==="
for kv in "$KEY1:$VAL1" "$KEY2:$VAL2"; do
    k="${kv%%:*}"; v="${kv##*:}"
    RA=$(field "$("$CLIENT_BIN" --target="$A" --replicate-read="$k" 2>&1)" value)
    RB=$(field "$("$CLIENT_BIN" --target="$B" --replicate-read="$k" 2>&1)" value)
    RC=$(field "$("$CLIENT_BIN" --target="$C" --replicate-read="$k" 2>&1)" value)
    if [[ "$RA" == "$v" && "$RB" == "$v" && "$RC" == "$v" ]]; then
        pass "$k: A=B=C=$v -- fully converged"
    else
        fail "$k: A=$RA B=$RB C=$RC -- did not converge to $v"
    fi
done

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1