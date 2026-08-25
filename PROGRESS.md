# Driftstore — Progress Log

> Paste this file into this Claude Project's context to preserve continuity
> across conversations. Update it at the end of each phase, or sooner if a
> significant decision gets made. Claude will propose updates as phases wrap;
> apply them here and in the repo.

## Phase status

| Phase | Description | Status |
|---|---|---|
| 0 | Scaffolding + comparison harness skeleton | ✅ Done |
| 1 | Gossip membership + local failure detection | 🔄 In progress |
| 2 | Consistent hashing ring + virtual nodes | ⬜ Not started |
| 3 | Any-node coordinator + basic quorum read/write | ⬜ Not started |
| 4 | Vector clocks + conflict detection | ⬜ Not started |
| 5 | Hinted handoff | ⬜ Not started |
| 6 | Read-repair | ⬜ Not started |
| 7 | Dashboard | ⬜ Not started |
| 8 | Fault-injection harness (minimum viable) | ⬜ Not started |
| 9 | Convergence testing + full comparison run + polish | ⬜ Not started |

---

## Phase 0 — Scaffolding + comparison harness (done)

**Repo:** `driftstore`, sibling to GTStore's repo at `../DistributedSystems/gtstore`.

**Build:** Plain Make + `pkg-config` (no CMake), adapted from GTStore's Makefile.
Single `node` binary + a `client` stub — no manager/storage split, since Driftstore
has no centralized role. `make all` builds `bin/node` and `bin/client`.

**Proto (`src/driftstore.proto`):** service `DriftStoreNode`, one RPC —
`Ping(PingRequest) returns (PingResponse)`. Both messages carry a single
string field (`sender_node_id` / `responder_node_id`).

**Node identity:** `node_id` is literally the node's own `--listen` address
(e.g. `127.0.0.1:60051`) — no separate ID scheme. Decided over letting the OS
assign an ephemeral port specifically because Phase 8's kill/restart testing
needs stable identity across restarts; ephemeral ports would make a restarted
node look like a brand-new one to the rest of the cluster.

**Structured logging (`src/logging.hpp`):**
Format: `[<ISO8601 UTC, ms precision>] node=<node_id> event=<EVENT_TYPE> <key=value ...>`
Written to `stderr` (unbuffered — survives a crash better than buffered `stdout`).
`EventType` is a fixed enum with an exhaustive `switch` (no `default` case) in
`toString()`, so `-Wall`/`-Wswitch` catches a forgotten case if a new event
type gets added later without updating the stringifier.
Current events: `NODE_INIT`, `PING_SENT`, `PING_RECEIVED`, `PING_SUCCEEDED`, `PING_FAILED`.

**Harness (`harness/smoke_test.sh`):** launches GTStore (`manager -n 1 -k 1`,
`storage --port 50053`, then `test_app --put`/`--get`) and Driftstore
(`node --listen=...`, then `client --target=... --self=...`), checks each
system's exit code, prints PASS/FAIL for both. Uses `trap cleanup EXIT` so
background processes always get killed, even on failure — avoids orphaned
processes squatting on ports across runs.
**Confirmed passing for both systems.**

### GTStore comparisons made so far
- **Build structure:** GTStore's Makefile builds two roles (`manager`, `storage`)
  because of the centralized manager. Driftstore builds one symmetric `node`
  binary — every process is the same kind of peer.
- **Client bootstrap:** GTStore's client `init()` establishes a channel to the
  manager before anything else can happen — a hard dependency and a SPOF baked
  into the first line of client code. Driftstore's client has no equivalent
  step; it just dials an address directly.
- **Membership at startup:** GTStore's manager is told the full cluster shape
  up front (`-n`, `-k` flags) and is the single source of truth for it for the
  life of the process. Driftstore has no registry to ask, so `node` has no
  equivalent flag — this is the specific gap Phase 1's gossip exists to fill.
  (Root cause is the *absent registry*, not the *routing* — a system could in
  principle keep centralized membership while still letting any node route
  requests; Driftstore drops both, but they're separable decisions.)

---

## Phase 1 — Gossip membership + local failure detection (in progress)

Two independent mechanisms, on two different timescales:
1. **Gossip membership table exchange** — periodic random-peer exchange +
   merge, eventually-consistent ring view across the cluster.
2. **Local reachability tracking** — private, per-node view of "is this peer
   currently responding," derived from recent request outcomes, never gossiped.
   Not started; still deferred (see `OPEN_QUESTIONS.md` Q4).

Conflict resolution (Q1) is locked: LWW on `last_updated`, higher `writer_id`
on an exact timestamp tie. Gossip is push-pull. Periodic empty-candidate-list
ticks log `GOSSIP_NO_PEERS` and skip the round — retry/backoff stays scoped to
`bootstrapFromSeed` only. Target selection is `UP`-only (Q6).

### Implemented in `node.cpp`

**Proto (`src/driftstore.proto`):** `NodeStatus` (`UP` / `REMOVED`),
`MembershipEntry` / `MembershipTable`, and `GossipExchange(GossipRequest) returns
(GossipResponse)` — request and response both carry a `MembershipTable` (push
and pull in the same round trip).

**Own entry + merge:** the constructor seeds `table_` with this node's own
`MembershipEntry` (`writer_id` and `address` both set to `node_id_`, status
`driftstore::UP`, `last_updated` = now). `mergeInto()` is LWW on
`last_updated`, `writer_id` tiebreak, and always takes an incoming entry the
local table doesn't have. `applyGossip()` is the locked wrapper: holds
`table_mutex_`, calls `mergeInto(table_, incoming)`, returns the merged table.
Always merge, never overwrite — `table_` can change under a round trip, so a
response is re-merged rather than assigned.

**`SendGossip` / `applyGossip` split:** `SendGossip` is outbound-only. It
dials the peer, logs `GOSSIP_SENT` then `GOSSIP_SUCCEEDED` / `GOSSIP_FAILED`,
and returns the `GossipResponse`. It does **not** merge that response into
`table_` — the caller must `applyGossip(response.table())` to complete the
pull half. Skipping that step silently degrades the round from push-pull to
push-only. The inbound `GossipExchange` handler logs `GOSSIP_RECEIVED`,
`applyGossip(request->table())`, logs `GOSSIP_MERGED`, and writes the merged
table onto the response.

**`bootstrapFromSeed`:** snapshot `table_` under `table_mutex_`, drop the
lock, `SendGossip` to the seed, then `applyGossip(response.table())`. Same
snapshot-then-unlock shape the periodic round uses — the RPC must not be held
under `table_mutex_`, or an inbound `GossipExchange` would stall for a
network round trip.

**`start()` / `gossipLoop()`:** `start(gossip_interval_ms)` spawns a detached
thread that loops `gossipRound()` + sleep. Detached + no stop signal is only
correct while this process's only exit path is SIGKILL (see `ARCHITECTURE.md`).
`main()` already had optional `--seed`; `bootstrapFromSeed` runs after
`BuildAndStart()` so the node is reachable as a server before it dials out.

**Logging:** `EventType` gained `GOSSIP_SENT`, `GOSSIP_RECEIVED`,
`GOSSIP_SUCCEEDED`, `GOSSIP_FAILED`, `GOSSIP_NO_PEERS`, `GOSSIP_MERGED`.

### Periodic gossip scheduler — implemented

`gossipRound()` and `selectGossipTarget()` are implemented, matching the
locked design in `ARCHITECTURE.md`: snapshot-then-unlock before any RPC,
`UP`-only + exclude-self peer selection, uniform random choice among eligible
peers, `GOSSIP_NO_PEERS` logged and the round skipped on an empty candidate
list (no retry/backoff at this level — that stays scoped to
`bootstrapFromSeed`), and `applyGossip(response.table())` called after
`SendGossip` to complete the pull half of push-pull.

`main()` updated: added a `--gossip-interval=<ms>` flag (default 1000, parsed
with `std::stoll`, no exception handling on malformed input yet — see
`OPEN_QUESTIONS.md` Q9). `service.start(...)` is called unconditionally after
`bootstrapFromSeed()`, not gated on whether `--seed` was provided — a
seedless node still needs to run its own scheduler so it's ready to gossip
once another node joins it later.

Public/private boundary confirmed: only `start()`, `Ping`, `GossipExchange`,
`SendGossip`, `applyGossip`, and `bootstrapFromSeed` are public;
`gossipLoop`, `gossipRound`, and `selectGossipTarget` are private.

### Bugs caught during implementation

- `NodeStatus::UP` referenced without the `driftstore::` namespace qualifier
  in an early draft of `selectGossipTarget` — wouldn't compile. Fixed to
  `driftstore::UP`, matching the existing precedent already used in the
  constructor.
- The same enumerator was later reintroduced as `driftstore::up` (wrong case)
  in two separate spots across successive edits before landing correctly as
  `driftstore::UP` — worth noting this recurred more than once, not just
  caught-and-done on the first pass.
- `selectGossipTarget` initially fell off the end of its body without a
  `return` on the non-empty-candidates path — undefined behavior for a
  function with a non-void return type, not just an incomplete stub.
- `main()` referenced an undeclared `gossip_interval_ms` where the actual
  local variable was named `gossip_interval_str` at the time — a copy-paste
  mismatch between the parameter name in `start()`'s signature and the local
  variable holding the parsed value.
- **Runtime crash, found via gdb, not compile-time:** `toString(EventType)`
  in `logging.hpp` had no `case` for `GOSSIP_NO_PEERS` — the enum value was
  added but the stringifier switch was never updated to match. Every node
  with zero `UP` peers on a gossip tick (which includes any node before it's
  joined by a peer, or a seed node before anything joins it) called
  `logEvent(GOSSIP_NO_PEERS, ...)` → `toString(GOSSIP_NO_PEERS)`, which fell
  off the end of a non-void function — compilers commonly emit a trap
  instruction for this exact "provably unreachable" case, which is what
  produced "Illegal instruction (core dumped)" at runtime. Diagnosed with
  `gdb --args ./bin/node --listen=...`, `run`, `bt` — the backtrace pointed
  directly at `toString` ← `logEvent` ← `gossipRound` ← `gossipLoop`. Fixed
  by adding the missing case. Open follow-up, not yet confirmed: whether
  `-Wswitch` (part of `-Wall`, and the mechanism `logging.hpp`'s own comment
  says exists specifically to catch a missing case like this at compile time)
  is actually enabled as a hard error in the Makefile's `CXXFLAGS` — see
  `OPEN_QUESTIONS.md`.

### Not yet verified

Temporary `std::cout` debug statements (`"Gossip"`, `"AFTER GOSSIP LOOP WAITING FOR REQUESTS"`)
were added to `node.cpp` during this troubleshooting session and need to be
confirmed removed before this is considered clean — they were never part of
the project's stderr-based structured logging format.

### Not yet built for Phase 1

The scheduler itself is now implemented but **not yet verified** against real
multi-node convergence — no run with 2+ live node processes watching
`GOSSIP_SENT` / `GOSSIP_RECEIVED` / `GOSSIP_SUCCEEDED` / `GOSSIP_MERGED`
correlate across processes has happened yet as of this update.

**Explicit next step:** that multi-node correlated-log run, before:

- the admin join/remove RPC
- the full `bootstrapFromSeed` retry/backoff/ordered-fallback upgrade
- the status/debug endpoint

All three still unstarted.

### Debugging notes

This session's crash was the project's first real use of gdb rather than
correlated logs, and it was the right call specifically because the failure
was single-process (one node, one crash, one stack) rather than the
multi-process divergence the correlated-logging approach is built for —
gdb's `bt` answered "where did control stop and what's the call chain" in
one command, which log correlation across independent processes isn't suited
to answer for a single crashed process.

---

## How to update this file
When a phase wraps: flip its status in the table, add a summary block (what
was built, key decisions + why, in the same shape as Phase 0's above), and log
any new GTStore comparisons that came up. When a question in
`OPEN_QUESTIONS.md` gets resolved, move it to that file's "Resolved" section
rather than deleting it — the reasoning is worth keeping.
