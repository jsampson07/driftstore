#!/usr/bin/env bash
#
# Verifies Phase 5: hinted handoff end-to-end.
#
# Covers:
#   1. A write succeeds and stays available while one of a key's natural
#      owners is down (a substitute holds a hint for it).
#   2. The hint-creating write path fires — HINT_CREATED (remote substitute)
#      or the local-coordinator self-hint variant of HINT_STORED.
#   3. Once the true owner comes back up, PROBE_SUCCEEDED -> HINT_DELIVERED
#      fires and the owner actually has the value (checked via a direct
#      ReplicateRead against it, not just a log line).
#
# NOT covered, by design: cleanly distinguishing the two hint-creation code
# paths (live RPC failure mid-write vs. the coordinator already knowing the
# owner is down before computing the preference list). coordinatePut's
# fan-out doesn't log a distinct event when a live ReplicateWrite to a
# preference-list target fails (it silently calls markUnreachable and moves
# to substitute search) — so which path fired for a given key isn't fully
# recoverable from logs alone. Test 3 below opportunistically tries a
# second affected key and reports what it finds, but that result is
# informational, not asserted pass/fail.
#
# 4 nodes are required, not 3 — hinted handoff needs a node OUTSIDE the
# natural N to hand a hint to. With N=3 and exactly 3 nodes there is no
# spare node to substitute onto, and every key trivially maps to all 3
# (the trick prior scripts use for determinism). With 4 nodes and N=3,
# which specific node ends up a natural owner for a given key depends on
# consistent-hash placement, which this script does not replicate — so it
# empirically searches for a key whose natural owners happen to include B,
# by trying candidate keys against a live cluster and checking whether the
# hint machinery engages for that key. This is the same "trial against the
# real ring" approach vnode_experiment.cpp already uses elsewhere in this
# project, not a new technique introduced just for this script.
#
# Run from the repo root (expects ./bin/node and ./bin/client to exist).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"

A="127.0.0.1:60051"
B="127.0.0.1:60052"
C="127.0.0.1:60053"
D="127.0.0.1:60054"
B_PORT="${B##*:}"

N=3
W=2
R=2
GOSSIP_INTERVAL=800
PROBE_INTERVAL=500
LOGDIR="./test_logs_hinted_handoff"

PASS=0
FAIL=0

pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# --- setup ---------------------------------------------------------------

pkill -9 -f "$NODE_BIN" 2>/dev/null || true   # Q11
sleep 0.5

rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

status_dump() {
    # stderr intentionally NOT discarded — if the RPC itself is failing
    # (vs. the table just not being converged yet), that error message is
    # exactly what tells the difference, and hiding it was a bug in this
    # script, not a feature.
    "$CLIENT_BIN" --target="$1" --status 2>&1
}

wait_for() {
    # $1 = target address, $2 = substring in GetStatus dump, $3 = desc,
    # $4 = timeout (default 20)
    local target="$1" needle="$2" desc="$3" timeout="${4:-20}"
    local elapsed=0
    local last_output=""
    while (( elapsed < timeout )); do
        last_output=$(status_dump "$target")
        if grep -q -- "$needle" <<< "$last_output"; then
            pass "$desc (took ~${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    fail "$desc (timed out after ${timeout}s)"
    echo "--- last status_dump($target) output, for diagnosis ---"
    echo "$last_output"
    echo "--- end ---"
    return 1
}

wait_for_log() {
    # $1 = log file (glob-expandable string), $2 = substring, $3 = desc,
    # $4 = timeout (default 20)
    local logfile="$1" needle="$2" desc="$3" timeout="${4:-20}"
    local elapsed=0
    while (( elapsed < timeout )); do
        if grep -h -q -- "$needle" $logfile 2>/dev/null; then
            pass "$desc (took ~${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    fail "$desc (timed out after ${timeout}s)"
    return 1
}

echo "=== launching 4-node cluster (N=$N W=$W R=$R) ==="
"$NODE_BIN" --listen="$A" --N="$N" --W="$W" --R="$R" \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_a.log" 2>&1 &
PIDS+=($!)

"$NODE_BIN" --listen="$B" --seed="$A" --N="$N" --W="$W" --R="$R" \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_b.log" 2>&1 &
PIDS+=($!)

"$NODE_BIN" --listen="$C" --seed="$A" --N="$N" --W="$W" --R="$R" \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_c.log" 2>&1 &
PIDS+=($!)

"$NODE_BIN" --listen="$D" --seed="$A" --N="$N" --W="$W" --R="$R" \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_d.log" 2>&1 &
PIDS+=($!)

sleep 1

echo "=== waiting for initial convergence (all 4 nodes see table_size=4) ==="
wait_for "$A" "table_size=4" "A sees all 4 members" || exit 1
wait_for "$B" "table_size=4" "B sees all 4 members" || exit 1
wait_for "$C" "table_size=4" "C sees all 4 members" || exit 1
wait_for "$D" "table_size=4" "D sees all 4 members" || exit 1

# --- test 1: baseline write works before anything is down -----------------

echo ""
echo "=== test 1: baseline put/get round-trip while cluster is healthy ==="

BASE_OUT=$("$CLIENT_BIN" --target="$A" --put="baseline=v0" 2>&1)
echo "$BASE_OUT" | grep -q "success=true" \
    && pass "baseline put succeeded" \
    || fail "baseline put did not succeed: $BASE_OUT"

# --- test 2: kill B, find a key whose natural owners include B ------------

echo ""
echo "=== test 2: kill B, find a key affected by it, confirm hint creation ==="

kill -9 "${PIDS[1]}" 2>/dev/null || true
sleep 1

if ss -ltnp 2>/dev/null | grep -q ":$B_PORT "; then
    fail "B's port $B_PORT still held after kill (Q11) — results below are unreliable"
else
    pass "B's port released cleanly after kill"
fi

FOUND_KEY=""
for i in $(seq 0 14); do
    candidate="hh_key_${i}"
    "$CLIENT_BIN" --target="$A" --put="${candidate}=val_${i}" > /dev/null 2>&1
    sleep 0.3
    if grep -h "key=${candidate}" "$LOGDIR"/node_*.log 2>/dev/null \
        | grep -Eq "hint_for=${B}|original_owner=${B}"; then
        FOUND_KEY="$candidate"
        pass "found key affected by B's outage: $candidate (hint mechanism engaged)"
        break
    fi
done

if [[ -z "$FOUND_KEY" ]]; then
    fail "no candidate key among 15 tried produced a hint for B — hinted handoff did not engage at all"
    echo "=== summary: $PASS passed, $FAIL failed ==="
    exit 1
fi

echo "--- hint-creation log lines for $FOUND_KEY ---"
grep -h "key=${FOUND_KEY}" "$LOGDIR"/node_*.log | grep -E "HINT_CREATED|HINT_STORED"

# --- test 3: write stays available during the outage -----------------------

echo ""
echo "=== test 3: $FOUND_KEY is still readable cluster-wide while B is down ==="

GET_OUT=$("$CLIENT_BIN" --target="$C" --get="$FOUND_KEY" 2>&1)
if echo "$GET_OUT" | grep -q "found=true"; then
    pass "get($FOUND_KEY) via C succeeded during B's outage: $GET_OUT"
else
    fail "get($FOUND_KEY) via C failed during B's outage: $GET_OUT"
fi

# Opportunistic second hit — informational only, see header comment on why
# this can't be asserted as definitely exercising the "upfront" ring.hpp
# substitution path specifically, as opposed to a second live-failure hit.
SECOND_KEY=""
for i in $(seq 15 29); do
    candidate="hh_key_${i}"
    "$CLIENT_BIN" --target="$A" --put="${candidate}=val_${i}" > /dev/null 2>&1
    sleep 0.3
    if grep -h "key=${candidate}" "$LOGDIR"/node_*.log 2>/dev/null \
        | grep -Eq "hint_for=${B}|original_owner=${B}"; then
        SECOND_KEY="$candidate"
        echo "INFO: second key also affected by B: $candidate — see header comment, path not distinguished"
        break
    fi
done

# --- test 4: bring B back, confirm delivery ---------------------------------

echo ""
echo "=== test 4: restart B, verify hint delivery ==="

echo "restarting B on the same address..."
"$NODE_BIN" --listen="$B" --seed="$A" --N="$N" --W="$W" --R="$R" \
    --gossip-interval="$GOSSIP_INTERVAL" --probe-interval="$PROBE_INTERVAL" \
    > "$LOGDIR/node_b_restart.log" 2>&1 &
PIDS[1]=$!   # replace tracked PID so cleanup kills the *new* B process

wait_for_log "$LOGDIR/node_*.log" "PROBE_SUCCEEDED.*target=${B}" \
    "a peer's probe to B succeeds again" 30

wait_for_log "$LOGDIR/node_*.log" "HINT_DELIVERED.*owner=${B}" \
    "at least one HINT_DELIVERED for B fires" 20

echo "--- HINT_DELIVERED lines for B ---"
grep -h "HINT_DELIVERED" "$LOGDIR"/node_*.log | grep "owner=${B}"

# The actual proof: read the value directly off B, not just trust the log.
READ_OUT=$("$CLIENT_BIN" --target="$B" --replicate-read="$FOUND_KEY" 2>&1)
EXPECTED_VAL="val_${FOUND_KEY##hh_key_}"
if echo "$READ_OUT" | grep -q "found=true" && echo "$READ_OUT" | grep -q "value=${EXPECTED_VAL}"; then
    pass "B directly holds $FOUND_KEY=$EXPECTED_VAL after delivery: $READ_OUT"
else
    fail "B does not hold the delivered value for $FOUND_KEY: $READ_OUT"
fi

if [[ -n "$SECOND_KEY" ]]; then
    EXPECTED_VAL2="val_${SECOND_KEY##hh_key_}"
    READ_OUT2=$("$CLIENT_BIN" --target="$B" --replicate-read="$SECOND_KEY" 2>&1)
    if echo "$READ_OUT2" | grep -q "found=true" && echo "$READ_OUT2" | grep -q "value=${EXPECTED_VAL2}"; then
        pass "B directly holds $SECOND_KEY=$EXPECTED_VAL2 after delivery: $READ_OUT2"
    else
        fail "B does not hold the delivered value for $SECOND_KEY: $READ_OUT2"
    fi
fi

wait_for "$A" "${B}:UP" "B fully re-converges to UP on A"
wait_for "$C" "${B}:UP" "B fully re-converges to UP on C"
wait_for "$D" "${B}:UP" "B fully re-converges to UP on D"

# --- summary -----------------------------------------------------------

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1