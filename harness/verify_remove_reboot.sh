#!/usr/bin/env bash
#
# Verifies (2) multi-node REMOVE_SUCCEEDED + silent convergence, and
# (3) the reboot half — NODE_REBOOTED firing on peers, never on the
# rebooted node itself.
#
# Run from the repo root (expects ./bin/node and ./bin/client to exist).

set -uo pipefail

NODE_BIN="./bin/node"
CLIENT_BIN="./bin/client"

A="127.0.0.1:60051"
B="127.0.0.1:60052"
C="127.0.0.1:60053"
C_PORT="${C##*:}"

GOSSIP_INTERVAL=1000
LOGDIR="./test_logs_remove_reboot"

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

# --- setup -------------------------------------------------------------

pkill -9 -f "$NODE_BIN" 2>/dev/null || true   # Q11: stale processes from
                                               # earlier runs silently hold
                                               # a listen socket and make a
                                               # later legitimate bind fail
                                               # in a way that looks like a
                                               # network problem, not what
                                               # it actually is.
sleep 0.5

rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

status_dump() {
    # $1 = target address
    "$CLIENT_BIN" --target="$1" --status 2>/dev/null
}

wait_for() {
    # $1 = target address, $2 = substring to look for in the status dump,
    # $3 = human-readable description, $4 = timeout in seconds (default 20)
    #
    # Polls GetStatus — use this for TABLE STATE checks only
    # (table_size=N, <node_id>:UP, <node_id>:REMOVED). It will never match
    # a log event name like NODE_REBOOTED — that's not part of the table
    # dump. Use wait_for_log for that.
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
    # $1 = log file path, $2 = substring, $3 = description,
    # $4 = timeout in seconds (default 20).
    # Use this for LOG EVENT checks (NODE_REBOOTED, REMOVE_SUCCEEDED, etc.)
    # — these live in the log file, never in GetStatus's table dump.
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

# NOTE: direct redirection (`> file 2>&1 &`), not `| tee file &`.
# Piping through tee means $! captures tee's PID, not node's — killing
# "$!" would leave the actual node process running. Direct redirection
# keeps $! pointing at the process we actually need to kill/track.

echo "=== launching 3-node cluster ==="
"$NODE_BIN" --listen="$A" --gossip-interval="$GOSSIP_INTERVAL" \
    > "$LOGDIR/node_a.log" 2>&1 &
PIDS+=($!)

"$NODE_BIN" --listen="$B" --seed="$A" --gossip-interval="$GOSSIP_INTERVAL" \
    > "$LOGDIR/node_b.log" 2>&1 &
PIDS+=($!)

"$NODE_BIN" --listen="$C" --seed="$A" --gossip-interval="$GOSSIP_INTERVAL" \
    > "$LOGDIR/node_c.log" 2>&1 &
PIDS+=($!)

sleep 1

echo "=== waiting for initial convergence (all 3 nodes see table_size=3) ==="
wait_for "$A" "table_size=3" "A sees all 3 members" || exit 1
wait_for "$B" "table_size=3" "B sees all 3 members" || exit 1
wait_for "$C" "table_size=3" "C sees all 3 members" || exit 1

# --- test 2: RemoveNode + silent convergence ----------------------------

echo ""
echo "=== test 2: remove C via A, verify convergence ==="

REMOVE_OUT=$("$CLIENT_BIN" --target="$A" --remove="$C" 2>&1)
if echo "$REMOVE_OUT" | grep -q "accepted=true"; then
    pass "RemoveNode RPC accepted"
else
    fail "RemoveNode RPC not accepted: $REMOVE_OUT"
fi

# REMOVE_SUCCEEDED should log exactly once, only on the node that
# processed the RPC (A) — this is the logging asymmetry: removal itself
# is never re-logged as it propagates, only the originating write is.
a_count=$(grep -c "REMOVE_SUCCEEDED" "$LOGDIR/node_a.log" || true)
b_count=$(grep -c "REMOVE_SUCCEEDED" "$LOGDIR/node_b.log" || true)
c_count=$(grep -c "REMOVE_SUCCEEDED" "$LOGDIR/node_c.log" || true)

[[ "$a_count" -eq 1 ]] && pass "REMOVE_SUCCEEDED logged exactly once on A" \
                        || fail "REMOVE_SUCCEEDED count on A = $a_count, expected 1"
[[ "$b_count" -eq 0 ]] && pass "REMOVE_SUCCEEDED not logged on B (expected — no propagation log)" \
                        || fail "REMOVE_SUCCEEDED count on B = $b_count, expected 0"
[[ "$c_count" -eq 0 ]] && pass "REMOVE_SUCCEEDED not logged on C (expected — no propagation log)" \
                        || fail "REMOVE_SUCCEEDED count on C = $c_count, expected 0"

# Propagation itself has no log trail — this is the accepted gap — so the
# only way to confirm it actually happened is polling GetStatus.
wait_for "$B" "${C}:REMOVED" "C's status converges to REMOVED on B"

# Informational, not a pass/fail: C won't know it's been removed until it
# next gossips and hears about it.
if status_dump "$C" | grep -q "${C}:UP"; then
    echo "INFO: C's own view of itself is still UP — expected, it hasn't gossiped yet"
fi

# --- test 3: reboot half — NODE_REBOOTED --------------------------------

echo ""
echo "=== test 3: kill + restart C, verify NODE_REBOOTED on peers only ==="

kill -9 "${PIDS[2]}" 2>/dev/null || true
sleep 1

if ss -ltnp 2>/dev/null | grep -q ":$C_PORT "; then
    fail "C's port $C_PORT still held after kill — check for a stale/stopped process (Q11)"
else
    pass "C's port released cleanly after kill"
fi

echo "restarting C on the same address..."
"$NODE_BIN" --listen="$C" --seed="$A" --gossip-interval="$GOSSIP_INTERVAL" \
    > "$LOGDIR/node_c_restart.log" 2>&1 &
PIDS[2]=$!   # replace tracked PID so cleanup kills the *new* C process

sleep 1

wait_for_log "$LOGDIR/node_a.log" "NODE_REBOOTED" "NODE_REBOOTED logged on A"
wait_for_log "$LOGDIR/node_b.log" "NODE_REBOOTED" "NODE_REBOOTED logged on B"

# Give things a few more seconds to fully settle before the negative check
# below — checking too early could pass by accident rather than by proof.
sleep 3

c_reboot_count=$(grep -c "NODE_REBOOTED" "$LOGDIR/node_c_restart.log" || true)
[[ "$c_reboot_count" -eq 0 ]] && pass "C never logs its own revival (count=0)" \
                               || fail "C logged NODE_REBOOTED about itself ($c_reboot_count times) — unexpected"

# Show the actual timestamps so the staggered-arrival claim is visible,
# not just asserted.
echo ""
echo "--- NODE_REBOOTED timestamps (expect A and B to differ) ---"
grep "NODE_REBOOTED" "$LOGDIR/node_a.log" || echo "(none on A)"
grep "NODE_REBOOTED" "$LOGDIR/node_b.log" || echo "(none on B)"

wait_for "$A" "${C}:UP" "C fully re-converges to UP on A"
wait_for "$B" "${C}:UP" "C fully re-converges to UP on B"

# --- summary -------------------------------------------------------------

echo ""
echo "=== summary: $PASS passed, $FAIL failed ==="
echo "logs kept in $LOGDIR for inspection"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1