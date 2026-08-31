# Driftstore — Progress Log

> Paste this file into this Claude Project's context to preserve continuity
> across conversations. Update it at the end of each phase, or sooner if a
> significant decision gets made. Claude will propose updates as phases wrap;
> apply them here and in the repo.

## Phase status

| Phase | Description | Status |
|---|---|---|
| 0 | Scaffolding + comparison harness skeleton | ✅ Done |
| 1 | Gossip membership + local failure detection | ✅ Done |
| 2 | Consistent hashing ring + virtual nodes | ✅ Done |
| 3 | Any-node coordinator + basic quorum read/write | 🔄 In progress |
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
`MembershipEntry` / `MembershipTable`, `GossipExchange(GossipRequest) returns
(GossipResponse)` — request and response both carry a `MembershipTable` (push
and pull in the same round trip) — and `GetStatus(StatusRequest) returns
(StatusResponse)` (empty request; response carries `node_id` + a text table
dump — see "Status/debug endpoint" below).

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
network round trip. Now also logs `GOSSIP_MERGED` after completing its merge
(see "GOSSIP_MERGED coverage" below — this call site used to be silent).

**`start()` / `gossipLoop()`:** `start(gossip_interval_ms)` spawns a detached
thread that loops `gossipRound()` + sleep. Detached + no stop signal is only
correct while this process's only exit path is SIGKILL (see `ARCHITECTURE.md`).
`main()` already had optional `--seed`; `bootstrapFromSeed` runs after
`BuildAndStart()` so the node is reachable as a server before it dials out.

**`GetStatus` / `dumpTable` — status/debug endpoint:** `dumpTable()` is a
free function (anonymous namespace, alongside `nowMillis()`) that snapshots a
`MembershipTable`, sorts entries by `node_id`, and renders
`table_size=<n> entries=[node_id:status:writer_id:last_updated, ...]` as a
single string — sorted specifically so output is diffable by eye across
different nodes' status calls. `GetStatus` snapshots `table_` under
`table_mutex_`, calls `dumpTable`, and returns it in `StatusResponse`. Purely
a human debugging tool for now — deliberately not shaped as a machine-facing
endpoint (no plan yet to have the fault-injection or comparison harness call
it programmatically; revisit if that changes).

**`client.cpp` `--status` mode:** new flag, mutually exclusive with `--self`
(a status query has no `sender_node_id` to populate). Prints
`node=<id> <table_dump>` to stdout on success. Uses a 2s RPC deadline —
doesn't retroactively fix Q10's concern about `Ping`'s missing deadline, but
doesn't repeat that gap in new code.

**Logging:** `EventType` gained `GOSSIP_SENT`, `GOSSIP_RECEIVED`,
`GOSSIP_SUCCEEDED`, `GOSSIP_FAILED`, `GOSSIP_NO_PEERS`, `GOSSIP_MERGED`.
`GetStatus` intentionally has no dedicated `EventType` — it's a pull-based
debug query, not part of the gossip protocol itself, so logging it would add
noise to logs meant to trace real membership propagation.

**`GOSSIP_MERGED` coverage — now logged at all three merge-completion
points.** Previously only the inbound `GossipExchange` handler logged
`GOSSIP_MERGED` — `gossipRound()`'s pull-half merge and
`bootstrapFromSeed()`'s merge were silent, discovered while adding temporary
table-dump instrumentation for multi-node convergence testing (see
"Verification" below). Kept the log calls at both previously-silent sites
permanently, with a short `source=round peer=<addr>` /
`source=bootstrap seed=<addr>` payload — not the full table dump, which was
temporary and has been removed.

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
`OPEN_QUESTIONS.md` Q9). At the time this was written, `service.start(...)`
was called unconditionally after `bootstrapFromSeed()`, not gated on whether
`--seed` was provided — a seedless node still needs to run its own scheduler
so it's ready to gossip once another node joins it later. **This is no
longer fully accurate** — see "bootstrapFromSeed retry/backoff —
implemented" below, which changes this for the case where a seed list was
given and bootstrap ultimately fails.

Public/private boundary confirmed: `start()`, `Ping`, `GossipExchange`,
`GetStatus`, `SendGossip`, `applyGossip`, and `bootstrapFromSeed` are public;
`gossipLoop`, `gossipRound`, and `selectGossipTarget` are private. (Signatures
of `SendGossip` and `bootstrapFromSeed` changed since this was written — see
below — the public/private boundary itself didn't.)

### bootstrapFromSeed retry/backoff — implemented

`SendGossip`'s return type changed from a plain `GossipResponse` to
`std::optional<GossipResponse>` — this resolves Q8 (see
`OPEN_QUESTIONS.md` "Resolved"). The `grpc::Status` check already inside
`SendGossip` didn't move; only the outcome changed: `status.ok()` now
returns the response wrapped in `optional`, failure returns `std::nullopt`
instead of relying on gRPC leaving an out-param at its default (empty-map)
value on failure, which both callers previously depended on implicitly
without checking for it. Considered exposing `grpc::Status` alongside the
response (to distinguish *why* an attempt failed — `UNAVAILABLE` vs.
`DEADLINE_EXCEEDED`, etc.) and decided against it: no caller branches on
failure *type*, only failure-or-not, and the general problem those codes
would be standing in for — distinguishing "dead" from "slow/partitioned" —
isn't solvable from a single RPC's status regardless. That's the unreliable
failure-detector problem Dynamo/SWIM-style systems solve with aggregated
signal over time (Phase 3's reachability tracking), not a richer return type
on one call.

`bootstrapFromSeed(const std::vector<std::string>& seeds) -> bool` now
takes a seed list (previously a single address) and retries:

- Up to 3 passes over the full seed list, in order.
- Exponential backoff *between passes only* (250ms → 500ms → 1000ms,
  doubling on each pass failure) — no delay between individual seeds within
  the same pass. The 1000ms value is never actually slept on: the loop
  returns immediately after pass 3 fails rather than backing off before
  giving up with nothing left to try.
- The table snapshot used for each `SendGossip` call is taken fresh,
  immediately before that individual attempt — not once per pass, not once
  for the whole call. Reasoning: the server is already listening
  (`BuildAndStart()` runs before `bootstrapFromSeed` is called), so an
  inbound `GossipExchange` from a peer that still has a stale entry for
  this node (the reboot case — `node_id` is the listen address, per Q7, so
  a restarted node is indistinguishable by identity from its pre-crash
  self) can land and merge into `table_` *during* the retry loop.
  Per-attempt snapshotting means that merge gets used on the very next seed
  contacted instead of sitting unused until the next pass. This isn't a
  corruption risk — LWW (Q1) means the node's own fresh self-entry
  (timestamped at construction, i.e. at this boot) beats a stale incoming
  entry for the same node_id — it's free, real cluster knowledge that
  per-attempt snapshotting just picks up sooner. (Edge case noted, not
  treated as a new problem: this protection assumes local wall-clock
  ordering is trustworthy across the reboot — see the addendum on Q1 in
  `OPEN_QUESTIONS.md`.)
- On success (any seed responds): merges the response, logs `GOSSIP_MERGED`
  and `BOOTSTRAP_SUCCEEDED`, returns `true` immediately — does not keep
  trying remaining seeds in that pass.
- On exhausting all 3 passes: logs `BOOTSTRAP_FAILED` with a summary
  (`passes=3 seeds=<n>`) and returns `false`. `bootstrapFromSeed` owns this
  log call itself rather than returning failure detail to `main()` for it
  to log — it already has full context (which pass, how many seeds) at the
  point it gives up; `main()` only needs the one bit.

`main()` updated to match: `--seed` now accepts a comma-separated list
(split into `std::vector<std::string>` via a new `splitSeeds()` helper). If
`bootstrapFromSeed` returns `false`, `main()` exits (`return 1`)
**without** calling `service.start(...)` — a node that never joined the
cluster shouldn't run its own gossip scheduler as if it had. A seedless
node (no `--seed` given at all) is unaffected — bootstrap is skipped
entirely, same as before.

New `EventType`s: `BOOTSTRAP_INIT`, `BOOTSTRAP_SUCCEEDED`, `BOOTSTRAP_FAILED`.

**Verified:** dead-seed-list test run against two unreachable addresses —
`BOOTSTRAP_FAILED` logged with `passes=3 seeds=2` as expected; backoff gaps
between pass 1→2 and pass 2→3 matched the intended ~250ms/~500ms timing
read off log timestamps; `main()`'s failure-path `return 1` exits the
process cleanly with exit code `1` (measured correctly via
`${PIPESTATUS[0]}` after catching that a naive `echo $?` after a `| tee`
pipe reports `tee`'s exit code, not `node`'s — see this chat). No explicit
`server->Shutdown()` call turned out to be necessary for a clean exit on
this path — resolves Q14, see `OPEN_QUESTIONS.md`. Also verified: a seed
list with a dead address followed by a live one succeeds on the second
entry within pass 1, with zero backoff sleep triggered — confirms the list
is actually walked in order and success short-circuits before any sleep
logic runs. The 3-node convergence regression test was rerun and still
shows `GOSSIP_SENT → GOSSIP_RECEIVED → GOSSIP_MERGED` correlating across
independent nodes' logs, confirming the `SendGossip` signature change
didn't break the existing happy path.

**Not run:** the reboot-race scenario (kill a joined node, restart it on
the same address while a peer still holds a stale pre-crash entry for it,
confirm the inbound gossip lands mid-retry-loop) was not exercised — this
was the specific scenario the per-attempt-snapshot decision was designed
around, and "reasoned through it, never observed it" is a real gap worth
being honest about rather than letting the rest of this verified list imply
otherwise. Worth doing before trusting that behavior under real restart
conditions, not urgent to close Phase 1.

### GTStore comparisons made this session

- **Bootstrap-seed dependency vs. manager SPOF: same failure category,
  different blast radius.** GTStore's client `init()` dials the manager as
  a hard, permanent dependency — if the manager is down, the system is
  unreachable for *every* client, for the *life of the process*, because
  nothing substitutes for the manager as the source of cluster truth.
  Driftstore's seed-list dependency is the same *shape* of problem (an
  actor with zero cluster knowledge needs a first point of contact) but
  scoped far narrower: it matters only for the one joining node, only for
  the few seconds between process start and successful bootstrap. Once
  past that, the node never depends on a seed again — its own membership
  table and gossip loop take over completely. The 3-pass seed-*list* (vs.
  GTStore's single manager address) shrinks the blast radius further
  still: bootstrap only fails if every seed in the list is down
  simultaneously, not just one designated node.
- **Symmetric client/server role creates a race GTStore structurally can't
  have.** Every Driftstore node is simultaneously a gossip client (dialing
  seeds during bootstrap) and a gossip server (accepting inbound
  `GossipExchange`, already live before bootstrap even starts) — which is
  exactly what makes the reboot race described above possible: this
  node's own RPC surface is mutable by an inbound peer before it's
  finished establishing its own view of the cluster. GTStore has no
  equivalent scenario at all: only the manager ever holds or serves
  membership state, so no storage node's startup can race against an
  inbound write to shared state, because storage nodes never have shared
  membership state to race over. The tradeoff is explicit: GTStore never
  has to reason about this class of concurrent-mutation-during-startup
  bug, but it also never gets the "free knowledge from an inbound race"
  upside a symmetric design gets here.
- **Static seed list vs. no equivalent bootstrap concept at all.**
  GTStore's client has nothing resembling a seed list — the manager
  address is a single fixed dependency by design, so there's no "what if
  it's briefly unreachable" question, because failing over to something
  else was never the design goal. Whether Driftstore's seed set should
  ever be dynamic (nodes "promoted" to seed status, or resolved via
  external discovery) instead of a static admin-declared list was raised
  and deferred — see Q13.

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
  by adding the missing case.

  Follow-up checked and resolved: `-Wswitch` **is** enabled (it's part of
  `-Wall`, which the Makefile already sets), so a future missing `case`
  *will* produce a compiler warning. However, the Makefile has no `-Werror`,
  so that warning would not fail the build — a future missing case would
  still compile successfully and crash at runtime exactly like this one did.
  Decision: not being fixed right now (deprioritized, see
  `OPEN_QUESTIONS.md` if this gets revisited).

**Caught during the `SendGossip`/`bootstrapFromSeed` retry work:**

- First draft of `SendGossip`'s `std::optional<GossipResponse>` conversion
  returned `std::optional<response>` on the success path — treats a
  variable (`response`) as a template type argument, which doesn't
  compile. `return response;` is sufficient; `GossipResponse` converts
  implicitly into `std::optional<GossipResponse>` on return.
- After `SendGossip`'s return type changed, both call sites
  (`bootstrapFromSeed`, `gossipRound`) initially kept `response` declared
  as a plain `driftstore::GossipResponse` — a direct type mismatch against
  the new `std::optional<GossipResponse>` return. Once the declared type
  was corrected, `.table()` was still called directly on the `optional`
  wrapper (`response.table()`) instead of on the value it contains
  (`response->table()`) — `std::optional` has no `.table()` member, only
  the `GossipResponse` inside it does. **This one recurred**: flagged and
  fixed at both call sites in one pass, then reappeared in the same shape
  in the next pasted revision before landing correctly — same recurrence
  pattern already logged above for the `driftstore::UP`/`up` case
  mismatch.
- `main()`'s draft of the `BOOTSTRAP_INIT` log call referenced `node_id_`
  (trailing underscore — `NodeServiceImpl`'s private member) instead of
  the local variable `node_id` (no underscore) actually in scope inside
  `main()`. Same category as the earlier `gossip_interval_ms` /
  `gossip_interval_str` mismatch: a name valid in one scope reused in a
  different scope where it isn't declared, most likely from copy-pasting
  an existing `logEvent(...)` call as a template for a new one.
- An intermediate revision of `bootstrapFromSeed` dropped its `else`
  branch (the one logging `BOOTSTRAP_FAILED`) entirely — correctly, in
  the same revision, for `gossipRound`'s now-redundant `GOSSIP_FAILED`
  else branch, but the two aren't equivalent: `BOOTSTRAP_FAILED` carries
  information `GOSSIP_FAILED` alone doesn't ("this failure happened
  during bootstrap"). Caught before it shipped — as written it would have
  left the retry loop with no way to report failure at all.
- Final retry-loop draft referenced `seed_addr` inside the success-branch
  log call inside `for (std::string seed : seeds)` — undefined in that
  scope; the loop variable is `seed`. Caught before `main()` was written
  against it.
- An early draft of `main()`'s failure-exit branch called `eixt();` — a
  typo for `exit()`, which also wouldn't have compiled. Superseded by
  `return 1;` in the version actually implemented, matching `main()`'s
  existing `return 1` pattern for the `listen.empty()` usage-error case.

### Verification

**Debug `std::cout` statements — confirmed removed.** The temporary
`std::cout` debug lines (`"Gossip"`, `"AFTER GOSSIP LOOP WAITING FOR
REQUESTS"`) added during the `GOSSIP_NO_PEERS` crash investigation are
confirmed absent from `node.cpp`. No longer an open item.

**Multi-node convergence — verified.** Ran 3 live processes (one seed, two
joining via `--seed`), stderr captured via `tee` to per-node log files.
Confirmed `GOSSIP_SENT` → `GOSSIP_RECEIVED` → `GOSSIP_MERGED` correlate
across independent processes' logs — not just inferred from single-node
behavior.

Went one step further than log correlation alone: temporarily instrumented
`GOSSIP_MERGED` with a full table-content dump (table size + sorted
per-entry `node_id:status:writer_id:timestamp`) to confirm actual table
*convergence*, not just clean RPC exchanges — the two are different claims,
and correlated SENT/RECEIVED/MERGED logs alone can't distinguish "converged"
from "kept merging and staying different." All three nodes settled on
matching table contents. The temporary dump payload has since been removed;
only the (trimmed) `GOSSIP_MERGED` log calls at the two previously-silent
sites were kept permanently (see "GOSSIP_MERGED coverage" above). This same
`dumpTable()` logic is now what backs the permanent `GetStatus` RPC.

**Debugging note — stale/stopped processes masked real behavior as a
network failure.** The first attempt at this multi-node run appeared broken
(every gossip attempt timing out after ~20s, `GOSSIP_RECEIVED` never firing
on the seed). Root cause was **not** a code bug: `ss -ltnp` + `ps aux` found
`node` processes left in a *stopped* state (`Tl`/`tl`) from earlier
`gdb --args ./bin/node --listen=...` sessions that were never cleanly
killed. A stopped process still holds its listen socket at the kernel
level even though it can't `accept()` or complete a handshake, so a later,
legitimate `node` process targeting the same address silently lost the
bind — `AddListeningPort`'s success/failure out-param is currently
unchecked, so this failure was indistinguishable from healthy startup in
that node's own logs. Fixed with `kill -9` (not plain `kill` — a stopped
process doesn't act on `SIGTERM`) on both the stopped `node` processes and
their `gdb` parents. See `OPEN_QUESTIONS.md` Q11.

### Admin removal — `RemoveNode` RPC + reboot detection (done)

**No separate admin `Join` RPC.** Considered and rejected — `bootstrapFromSeed`,
the self-entry written at construction, and ordinary gossip propagation
already fully cover "how does a new node's presence become known
cluster-wide," with zero admin action required. A `Join` RPC would only
have added synchronous join-confirmation, which wasn't worth a new state
transition on its own.

**Q5 resolved: `remove` does not need seed-targeting.** The seed mechanism
solves "an actor with zero cluster knowledge needs a first point of
contact" — true for a joining node, not true for an admin issuing
`remove`, who already has to know the target's address to name it in the
request. `RemoveNode` can be sent to any node the admin already knows is
live. (Full resolution moved to `OPEN_QUESTIONS.md`.)

**`RemoveRequest.removed_node_id`, not `target_node_id`.** Renamed during
design — "target" already means "the address an RPC gets dialed to"
elsewhere in this codebase (`GOSSIP_SENT`'s `target=`, `client.cpp
--target=`). The node being removed is never dialed by the handler that
processes the request, so naming it `target_node_id` would collide two
different meanings under one word. `removed_node_id` stays unambiguous
across the `.proto`, the handler, and the log payload.

**Soft-remove semantics — deliberate choice, not a discovered gap.** A
removed node that keeps running, or restarts, is not permanently
excluded: its own freshly-timestamped self-entry will eventually
out-race the `REMOVED` entry under plain LWW, and the node becomes `UP`
again cluster-wide with zero special-casing required. "Sticky" removal
(surviving a restart) was considered and explicitly not adopted — it
would need something beyond LWW-on-wall-clock-timestamp (e.g. a
generation-number scheme, the way Cassandra's gossiper distinguishes a
legitimate restart from state it should refuse to re-accept). Soft-remove
matches the actual intent: a restarted node should still serve.

**`RemoveNode` handler (`node.cpp`):**
- Holds `table_mutex_` for the whole operation (find + branch + write) —
  no snapshot-then-unlock, unlike the gossip paths, because there's no
  RPC in the critical section to justify releasing the lock.
- `removed_node_id` not found in `table_` → `accepted=false`, no write.
- Found, already `REMOVED` → `accepted=true`, true no-op — no write,
  `last_updated` unchanged. Verified empirically: two consecutive
  `--remove` calls against the same node produce identical `last_updated`
  on the second call.
- Found, currently `UP` → mutated **in place**, not rebuilt: `status=REMOVED`,
  `writer_id=` this node's own `node_id_` (the admin isn't a table entry,
  can't be a writer), `last_updated=nowMillis()`, `tokens` explicitly
  cleared via `clear_tokens()` (deliberate: removal wipes vnode token
  assignments, relevant once Phase 2 lands). `address` is preserved for
  free as a side effect of mutating in place instead of rebuilding the
  entry — no explicit carry-forward code needed.
- Logs `REMOVE_SUCCEEDED` on both the write path and the no-op path.

**`mergeInto()` gained a third parameter, `caller_node_id`.** `mergeInto`
is a free function, not a class member, so it has no access to `node_id_`
for the log line's `node=` field. Making it a member function instead
(implicit access to `node_id_`) was considered and rejected, specifically
to keep the LWW comparison logic — the single most correctness-critical
function in this codebase — testable in complete isolation: constructible
with two bare tables, no `NodeServiceImpl`/gRPC/mutex required. Every call
site (`applyGossip`) already has `node_id_` in scope to pass through.

**`NODE_REBOOTED` detection lives inside `mergeInto()` itself, not at call
sites** — deliberate, to guarantee coverage across all three merge call
paths (`GossipExchange`'s inbound pull, `gossipRound`'s pull,
`bootstrapFromSeed`'s pull) without relying on each call site remembering
to check.

Placement bug caught before landing: an early draft checked
`local_entry.status() == REMOVED && incoming_entry.status() == UP`
*unconditionally*, before the LWW comparison decided whether the incoming
entry actually wins. That fires falsely during completely ordinary
post-removal gossip convergence — any stale, pre-removal `UP` entry still
circulating from a peer that hasn't heard about the removal yet would
trip the check on arrival, even though the LWW comparison correctly
rejects it and nothing in the table actually changes. Fixed by moving the
check inside each winning branch (`>`, and the `==`-with-tiebreak
branch), evaluated against the local entry's state *before* the overwrite
executes.

**Confirmed invariant:** `NODE_REBOOTED` can only ever be logged by a peer
observing another node's transition — never by a node about its own
identity. A node's own self-entry timestamp is set once, at construction,
and never refreshed while it runs; incoming gossip about its own identity
can never out-race that fresh timestamp under LWW, so a node's local view
of its own status never actually becomes `REMOVED` in the first place.
(Holds only as long as exactly one live process answers to a given
`node_id` at a time — see the incident below, and Q7.)

**Correction, caught during Phase 2 design:** the broader claim above — "a
node's local view of its own status never actually becomes `REMOVED` in the
first place" — is inaccurate, and was caught by tracing `mergeInto` directly,
then confirmed empirically (a live, un-killed node was administratively
removed via a *different* node; its own subsequent `GetStatus` call showed
itself `REMOVED`). There is no self-entry exemption anywhere in `mergeInto`'s
comparison logic — a live node's own table entry for itself is exactly as
overwritable by incoming gossip as any other entry, and does flip to
`REMOVED` while the process keeps running, because the removal's timestamp
necessarily outraces the node's construction-time self-entry (removal can
only happen after the node became known to begin with). The *narrower* claim
— that `NODE_REBOOTED` specifically can never be logged about self — still
holds, but for a more precise reason than originally stated: the self-entry
is written directly into `table_` at construction, bypassing `mergeInto`
entirely, so the transition-detection branch never has an opportunity to
fire against a node's own identity from its own process, regardless of what
its local status is or becomes. Not currently acted on anywhere: a
live-but-removed node keeps gossiping and keeps behaving normally in every
other respect — see `OPEN_QUESTIONS.md` Q15/Q16.

**`client.cpp --remove=<node_id>`:** new flag, same shape as `--status` —
mutually exclusive with `--self`, 2s RPC deadline. Noted, not fixed:
`accepted=false` (unknown node) and an actual RPC failure both currently
return exit code `1` — not distinguishable by exit code alone if this
gets used in scripts later.

### Verification

Built `harness/verify_remove_reboot.sh`: 3-node cluster, remove one via
RPC, confirm convergence, kill + restart the removed node, confirm reboot
detection. Closes the two items this section previously listed as
outstanding.

- **`REMOVE_SUCCEEDED` propagation asymmetry — confirmed.** Logs exactly
  once, only on the node that processed the RPC. Peers that later
  converge on `REMOVED` via gossip produce no log trail for that
  convergence — confirmed as the accepted gap it was designed to be, by
  asserting the log count is exactly zero on the other two nodes, not
  just "no failure occurred."
- **`NODE_REBOOTED` peer-only asymmetry — confirmed, with real payloads.**
  A live run showed `NODE_REBOOTED` firing on both peers (~3ms apart — a
  real but small stagger at this scale: 3 nodes, 1s gossip interval, all
  on localhost) and zero times on the rebooted node's own restart log.

**Script bug caught before trusting a run:** an early version of the
script polled `GetStatus`'s table dump to check for the *log event name*
`NODE_REBOOTED` — but that string only ever exists in a stderr log line,
never in table state, so the check was structurally guaranteed to time
out regardless of whether the underlying code was correct. Produced two
false `FAIL`s on a run where the actual logic was already right
(confirmed independently by grepping the log files directly). Fixed by
splitting into two helpers: `wait_for` (polls `GetStatus`, for
table-state assertions) and `wait_for_log` (greps the log file directly,
for event-log assertions).

**Real incident, not reproduced in the final script run:** during earlier
ad hoc manual testing (before this script existed), a second `node`
process was started on an address a first process already held. Because
`AddListeningPort`'s return value is unchecked (already flagged, Q11),
the second process's failed bind was silent — it kept running as an
outbound-only gossip client, pushed a fresh self-announcement for an
identity another live process already owned, and produced a confusing
`NODE_REBOOTED` line that looked like the *original* process logging a
revival about itself, which should be structurally impossible per the
invariant above. Root cause: two live processes briefly sharing one
`node_id` (Q7's identity-equals-address assumption, combined with Q11's
silent-bind-failure gap) — not a bug in `mergeInto` or `RemoveNode`. Did
not reproduce in the scripted run, which showed clean single-process
behavior throughout.

### GTStore comparisons made this session

- **Administrative membership changes are a capability GTStore
  structurally doesn't have.** GTStore's manager takes `-n`/`-k` once at
  startup and that's its entire configuration surface for the process's
  life — no removal operation exists because membership never changes at
  runtime. Every design question this session (soft vs. sticky remove,
  whether a removed-but-restarted node comes back, how to log a reversal)
  is a cost that comes specifically from choosing dynamic, decentralized
  membership over GTStore's static, centralized version — GTStore never
  has to answer any of them because it never has the capability they're
  about.
- **The `REMOVE_SUCCEEDED`/`NODE_REBOOTED` logging asymmetry has no
  GTStore analogue at all.** Because only the manager ever holds
  membership state, there is no concept of "an event that different
  nodes independently observe on their own schedule" over there — every
  membership fact is known exactly once, at the manager, the instant it
  happens. Driftstore's correlated-but-staggered log trail (one line on
  the node that processed a `remove`, N independently-timed lines as
  peers observe a later `NODE_REBOOTED`) is the direct cost of not having
  a single authoritative observer.

### Debugging notes

The `GOSSIP_NO_PEERS` crash (see "Bugs caught during implementation") was
the project's first real use of gdb rather than correlated logs, and it was
the right call specifically because the failure was single-process (one
node, one crash, one stack) rather than the multi-process divergence the
correlated-logging approach is built for — gdb's `bt` answered "where did
control stop and what's the call chain" in one command, which log
correlation across independent processes isn't suited to answer for a
single crashed process.

The stale-process issue during the convergence run (see "Verification"
above) was the opposite case: no crash, no single stack to inspect — the
failure only made sense once `ss`/`ps` output was cross-referenced against
three independent nodes' logs, confirming that correlated system-level
inspection (not gdb) is the right tool once the question is "why can't
these processes talk to each other" rather than "why did this one process
die."

---

## Phase 2 — Consistent hashing ring + virtual nodes (Done)

Full design worked out in conversation before any code was written, per this
project's own rule that correctness-critical logic gets designed and
reasoned through directly, not handed to Cursor from a standing start.
Nothing below is implemented yet — this is the spec implementation follows.

**Q4 resolved.** Phase 2 treats every `UP` member as reachable; the real
per-peer reachability filter is deferred to Phase 3, once local reachability
tracking actually produces a signal to consume. Preference-list computation
this phase is pure ring math — walk clockwise, collect N distinct physical
nodes, skip only `REMOVED`. Moved to `OPEN_QUESTIONS.md`'s Resolved section.

**Tokens — deterministic, not random+persisted.** `token_i = hash(node_id_ +
":" + i)` for `i` in `[0, V)`. Considered random tokens (Dynamo's original
approach, §4.2/§6.1 — chosen uniformly at random from the hash space, then
persisted to disk so a restart can reload the same mapping) and rejected in
favor of deterministic derivation: `node_id_` is already a stable, permanent
identity (Phase 0's address-as-identity decision, made specifically for
restart stability), so deriving tokens from it gets the same restart-stable
ring position for free — no disk, no new failure surface (missing file on
first boot vs. later boots, a crash mid-write leaving a truncated token
list, directory permissions), and no asymmetry with the rest of the system,
which persists nothing else at all. This also closes part of Q7's original
concern from Phase 1: a restarted node reclaiming its *exact* prior ring
position — needed so Phase 5's hinted-handoff obligations and Phase 6's
read-repair assumptions about stable ownership survive a restart — is now
structural, not a separate mechanism that has to be built and kept correct.
Deterministic derivation is not a lower-fidelity substitute for randomness
here — a well-mixed hash function applied to a unique, stable seed produces
output statistically indistinguishable from true randomness for placement
purposes; the only axis actually being chosen on is whether reproducing a
node's ring position requires external state.

**Hash function — FNV-1a, 64-bit.** Fixed published spec, no standard-library
dependency. Deliberately not `std::hash`: the standard makes no guarantee
`std::hash<std::string>` is stable across different stdlib implementations,
different versions of the same implementation, or even different runs of the
same binary (some implementations seed it per-process). That instability
would be a uniquely nasty bug class here specifically, because two nodes'
hash functions silently disagreeing on the same input would look, from the
outside, exactly like a real ring-logic bug — and this project's own planned
debugging approach for this phase ("feed one snapshot to two nodes' ring
logic and diff the output") assumes identical inputs produce identical
output whenever the code is correct. Same function used for both token
derivation and key hashing, so they land in one comparable space.

**Ring — `std::map<uint64_t, node_id>`, maintained incrementally, not
derived on demand.** Presence in the map is itself the `UP` signal — no
separate status field or lookup needed mid-walk. Chosen over deriving the
ring fresh from `table_` on every lookup mainly on cost grounds (a full
rebuild-and-sort is paid on every single `put`/`get` once Phase 3 exists,
and 10k+ times during this phase's own vnode-distribution experiment;
`std::map`'s ordered-associative structure gives O(log(N·V)) insert/erase/
lookup with no array-shift cost, so the standing structure is cheaper both
to build once and to query repeatedly). The sync-risk cost of a second piece
of state alongside `table_` is real but contained: `table_` currently has
exactly two mutation surfaces (`mergeInto`, reached only through
`applyGossip`'s three callers, and `RemoveNode`), both already funneled
through a small number of choke points from Phase 1's own design — so ring
updates have the same small number of places to stay correct in.

Ring is updated at **five** points, not the three originally assumed
mid-design:
1. **Constructor** — insert own tokens directly, same moment the self-entry
   is written into `table_`. Necessary for the same reason as the
   `NODE_REBOOTED` self-invariant above: a node's own entry is never "new"
   from its own perspective, so it never takes `mergeInto`'s insertion path
   for itself, no matter how much gossip happens afterward. Missing this
   would mean a node could compute a preference list that, by hash
   position, should include itself — and exclude itself, because its own
   ring never learned it owns those tokens, even though every other node's
   ring correctly would.
2. **`RemoveNode`** — erase directly, same construction-bypasses-`mergeInto`
   reasoning, on the one node that processes the admin RPC.
3–5. **Inside `mergeInto` itself:** insert on a genuinely new node arriving
   as `UP`; insert on `REMOVED→UP` (reboot, same branch that already
   detects `NODE_REBOOTED`); erase on `UP→REMOVED` — a peer learning about a
   removal *secondhand*, through ordinary gossip propagation, not the node
   that processed the original RPC. This third `mergeInto` condition was
   missed in an earlier pass of this design (only the direct `RemoveNode`
   erase was accounted for) — without it, every node except the one that
   processed the removal would keep a removed node's tokens live in its
   ring indefinitely, computing preference lists that silently keep
   including a node that's been gone for a while. The bug specifically
   would not show up testing the simple case (check the removing node's own
   preference lists); it only appears on a node that heard about the
   removal multi-hop.

**Preference-list walk:** `ring.upper_bound(hash(key))` to find the first
token clockwise, then walk forward deduping by physical node — an ordered
`std::vector<node_id>` for the actual result, an `std::unordered_set
<node_id>` alongside it purely as an O(1)-average "already collected"
check while walking, not part of the answer itself. Wraps via
`ring.begin()` on reaching `ring.end()`. Hard-stops after one full pass of
the ring regardless of whether N was reached, to guarantee termination on a
cluster with fewer than N distinct physical nodes — the naive
wraparound-forever version hangs indefinitely on that input rather than
just returning a wrong answer. Returns a short list (fewer than N distinct
nodes) in that case; what a caller does with a short list is Phase 3's
problem, not this function's — the function's only obligation is to
terminate and report accurately.

**Read/write preference-list asymmetry, worth having settled before Phase 3
exists:** the preference-list *computation* is identical for a read and a
write on the same key against the same ring state — neither one "chooses"
a different list. What differs is downstream capability: per §4.3, Dynamo's
preference list intentionally contains more than N nodes specifically so a
write has somewhere to go when it hits an unreachable top-N candidate (push
a copy onto the next live node further down the same list, as a
hinted-handoff stand-in). A read hitting that same gap has no equivalent
move — it can only query nodes that already happen to hold a copy of this
key (the live top-N members, plus any node currently sitting on a hint for
it), since querying "the next node in the ring" speculatively would just
return nothing. Same list; writes can expand where copies physically exist
when they hit a gap, reads are stuck querying wherever copies already
landed.

**Multi-coordinator divergence — a new failure class, not yet exercised by
any code, worth having internalized before Phase 3.** Because every node
computes its own preference list from its own local ring rather than
consulting one authoritative source, two coordinators handling the same key
near-simultaneously, with membership mid-change, can legitimately compute
two different N-node sets for that key — not a bug in either computation,
correct relative to what each currently knows. Same underlying cause,
different exposure, for a read arriving after a write once the ring has
drifted between the two: the read's computed list can include a node that
was never part of the original write's list, meaning R+W>N's usual
"guaranteed overlap" intuition is doing slightly less work than it sounds
like whenever a read and its corresponding write straddle a membership
change — hinted handoff and read-repair, not the quorum math itself, are
what actually close that gap. Nothing to fix in Phase 2; this is exactly
the shape of thing Phase 4 (vector clocks) and Phase 6 (read-repair) exist
to handle, and it's worth meeting this insight now rather than as a
surprise once those phases are in front of it.

**Left explicitly open, not blocking Phase 2 code — see `OPEN_QUESTIONS.md`
Q15/Q16:** whether a live node that's learned (per the Phase 1 correction
above) that its own status is `REMOVED` should stop selecting outbound
gossip targets, whether inbound `GossipExchange` should also decline to
merge once self-status is `REMOVED`, and whether a `REMOVED` node should
decline to coordinate once Phase 3 exists.

### Bugs caught during implementation

**`mergeInto`'s `UP→REMOVED` branch — wrong entry's tokens.** While writing
the ring-erase branch, it wasn't obvious by inspection whether to erase
`incoming_entry.tokens()` or `local_entry.tokens()` — both compile, both
look plausible. The correct answer is `local_entry.tokens()`: by the time a
`REMOVED` entry is gossiped, `RemoveNode` has already called
`clear_tokens()` on it at the source, so `incoming_entry.tokens()` is empty
and erasing from it is a silent no-op — the ring keeps a removed node's
tokens forever on any node that only ever learns about the removal
secondhand. The tokens that actually need erasing are the ones still
sitting in `ring_` from before this update, which live on `local_entry`,
read *before* the unconditional `(*local_entries)[node_id] = incoming_entry`
overwrite a few lines below — `local_entry` is a reference into the same
map slot that assignment mutates, not a copy, so ordering matters.

**Table-write regression, introduced while fixing the bug above.** An
intermediate draft moved `(*local_entries)[node_id] = incoming_entry` inside
the `REMOVED→UP` branch specifically, alongside its ring-insert logic. That
left the table write conditional on the transition type instead of on
`incoming_wins` alone — an ordinary `UP→UP` refresh (most real gossip
traffic) or a same-transition `UP→REMOVED` update stopped reaching `table_`
at all, silently breaking basic LWW convergence. Fixed by pulling the write
back out to run unconditionally whenever `incoming_wins`, with the two
transition-specific branches only handling the ring side effect.

**Vnode-distribution experiment — FNV-1a's weak diffusion on near-identical
inputs.** First run of `vnode_experiment.cpp` showed `stddev` identical to
two decimal places between `V=1` and `V=4`, and *worse* distribution at
`V=16`/`V=64` than at `V=1` — the opposite of the expected monotonic
improvement. Root cause: `token_i = fnv1a64(node_id + ":" + i)` for
sequential single-digit `i` produces token strings identical except for
their final byte, and two FNV-1a inputs differing only in their last byte —
processed as `hash ^= byte; hash *= prime` with no further mixing —
produce outputs differing by exactly `1 × prime` (`1,099,511,628,211`,
confirmed by direct computation). Against a 64-bit space of ~1.8×10^19,
that's a rounding error: a node's first ten vnodes (`i=0`–`9`) all land
within a span of ~10×prime, functionally one point on the ring regardless
of `V`. Crossing a digit-length boundary (`i=9→10`, `i=99→100`) adds a full
extra input byte and jumps by ~10^17–10^18 — which is *why* `V=16`/`64`
looked slightly better (some vnodes now have 2-digit suffixes) and `V=256`
better still (some have 3), rather than a clean curve.

First fix attempt was wrong, and empirically, not just in hindsight:
tried reordering to `fnv1a64(i + ":" + node_id)`, on the theory that
putting the varying byte first gives the fixed suffix more bytes to mix
through afterward. Made `V=1`/`V=4` *catastrophically* worse instead
(`stddev=40000`, one node getting literally 100% of keys) — moving the
short low-entropy field to the front just clusters across the 5 physical
nodes instead of across one node's vnodes, since `"node0"`–`"node4"` have
the exact same last-byte-only-differs problem `node_id` did originally.
Caught by re-running the same measurement before trusting the fix, not by
reasoning alone.

**Actual fix:** a standard 64-bit bit-mixing finalizer (`splitmix64`-style,
fixed published constants) applied *after* `fnv1a64`'s output, not a change
to the input string at all:
```cpp
uint64_t mix64(uint64_t x) {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}
```
Applied uniformly to both token derivation (`computeTokens`) and key
hashing (`preferenceListForKey`) — required by this phase's own "one
comparable space" principle above; fixing only one side would leave tokens
and keys positioned via different transforms. `fnv1a64` itself is
unchanged and still correct as a primitive; `mix64` is a wrapper, not a
replacement. Result, `V` swept against 5 physical nodes / 100k keys:

| V   | stddev (before) | stddev (after) |
|-----|-----------------|-----------------|
| 1   | 28806.93         | 18022.65 |
| 4   | 28806.93 (=V=1)  | 11054.46 |
| 16  | 16205.95         | 4712.36  |
| 64  | 16219.99         | 3496.91  |
| 256 | 13441.57         | 1304.57  |

Clean monotonic decrease after the fix, roughly tracking the `1/√V`
scaling expected for points scattered on a circle. Residual imbalance at
`V=256` (~1300 stddev against a 20000 mean, ~6.5%) doesn't go to zero and
isn't expected to — that's inherent to placing points randomly rather than
computing a provably even partition, not a sign the fix is incomplete.

### Verification status

`test_ring.cpp` now covers three of the five ring-mutation points:
new-node discovery (test 1), and `REMOVED→UP`/`UP→REMOVED` together via a
chained lifecycle test (`testReboot`) that reuses one entry across
new-node → remove → reboot, checking both `local` and `ring` after each
step. That removal step is real regression coverage for the two bugs
above — it constructs `incoming_entry` with empty tokens via
`clear_tokens()`, matching production behavior, and asserts
`ring.count(t0) == 0` afterward, which fails against the old
`incoming_entry.tokens()` version.

Two gaps remain, both explicitly accepted rather than overlooked:
- **`RemoveNode`'s direct erasure** has no test coverage. It isn't a free
  function like `mergeInto`/`preferenceList`, so covering it needs either a
  live gRPC round trip or a further refactor to extract its ring-erase
  logic the same way. Deferred as tracked debt.
- **Same-status `UP→UP` refresh** — the path with no dedicated branch in
  `mergeInto`, the one the table-write regression actually broke — has no
  regression test. Declined deliberately, not missed: the fix is
  understood and the regression it guards against is already fixed in the
  current code, but a future refactor that reintroduces it would go
  uncaught.

Both tracked as Q17 in `OPEN_QUESTIONS.md`.

### GTStore comparisons made this session
- **Preference-list computation from one authoritative center vs. computed
  independently by every node.** GTStore's `GetNodeForKey` is one function
  call against one `num_buckets` value in one process — every client gets
  the same answer, always, because only one place is capable of computing
  an answer. Driftstore has no such place: every node derives its own
  preference list from its own local, eventually-converging ring. The
  multi-coordinator divergence case above is a failure class GTStore cannot
  produce at all, structurally, not just in practice.
- **Random+persisted tokens (Dynamo's own original approach) vs.
  deterministic derivation.** Dynamo needs disk persistence for its
  node→token mapping because a Dynamo node has no cheap, stable identity to
  lean on. Driftstore already solved that problem differently, in Phase 0,
  by making `node_id_` the listen address specifically for restart
  stability — deterministic tokens extend that existing simplification
  rather than reaching for a second mechanism to solve the same underlying
  problem. GTStore never faces this question at all: `bucket_id = hash(key)
  % num_buckets` isn't a node identity, it's a position that only exists
  relative to `num_buckets` at that instant, recomputed by the manager
  whenever `N` changes — nothing about it needs to survive a restart,
  because nothing about it is owned by any single node to begin with.
- **Exact balance vs. bounded movement, made concrete by the
  vnode-distribution numbers above.** GTStore's `key % num_buckets` gives
  perfectly even load by construction — no stddev to measure, because it's
  dividing directly rather than scattering points and hoping. What it can't
  give: when `num_buckets` changes, nearly every key's assignment changes
  with it, since the modulus itself changed. Driftstore's ring keeps a
  residual, measured imbalance even at `V=256` (~6.5% of mean) in exchange
  for a join or leave only reassigning the ~1/N slice of keyspace between
  the changed node and its ring neighbor. Same trade as the multi-coordinator
  point above, from a different angle: GTStore pays its membership-change
  cost all at once, structurally, because there's only one bucket count to
  recompute from; Driftstore pays a small continuous cost (imbalance) to
  avoid paying a large discrete one (mass reassignment) whenever membership
  moves.

### Update — ring observability + live join-scenario verification (later session)

Closed the specific gap Q17 flagged as the reason two ring-mutation points
couldn't be tested: no live multi-node test had ever touched Phase 2 code,
and there was no RPC/CLI surface exposing `ring_` externally. Took option
(c) from Q17's list.

**`dumpRing()`** implemented in `node.cpp`, added to the existing
`GetStatus` response (`ring_dump` field) rather than a new RPC — table and
ring are two views of state already mutated under the same
`table_mutex_`, so one round trip getting both was preferred over a second
endpoint. No explicit sort needed, unlike `dumpTable`: `ring_` is a real
`std::map<uint64_t, std::string>`, already ordered by construction, unlike
protobuf's `map<string, MembershipEntry>`, which gives no iteration-order
guarantee. `client.cpp --status` was separately found to only print
`table_dump`, never the new `ring_dump` field — fixed alongside.

**Live join-scenario run:** 2-node cluster (A, B), snapshot `ring_dump` via
`GetStatus`, join a 3rd node C via `--seed`, snapshot again, diff.
Before: `table_size=2`, `ring_size=64`. After: `table_size=3`,
`ring_size=96`. Diff: 32 added lines, 0 removed, 0 changed — pure
additions only. Confirms Phase 2's stated production scenario ("a new node
joins... only virtual-node ranges adjacent to its tokens move") directly:
since `ring_[token] = node_id` is only ever set once per token or erased,
never rewritten, a zero-modified-lines diff is a direct proof that no
existing key's `preferenceList()` result changed — not an inference from
the ring math, an empirical confirmation of it.

**Does not fully close Q17.** The live-observability blocker is gone, but
neither of Q17's two original coverage gaps (`RemoveNode`'s direct
erasure, the `UP→UP` refresh path) got a test out of this run — the join
scenario exercises insertion, not removal. Both remain open, now with
working tooling available to close them if that's worth doing later. See
`OPEN_QUESTIONS.md` Q17 for the precise update.

**GTStore comparison:** GTStore's `GetNodeForKey` is `hash(key) %
num_buckets` — bumping `num_buckets` changes the modulus for every key at
once, so there's no such thing as a "pure addition" diff for a join over
there; it's a global reshuffle by construction. The diff above is the
empirical version of that already-documented tradeoff, not a new one.

---

## Phase 3 — Any-node coordinator + basic quorum read/write (design locked, implementation starting)

Full design worked out in conversation before any code was written, same
rule as Phase 2. Nothing below is implemented yet.

**Proto additions:** `Put`/`Get` (client-facing) and `ReplicateWrite`/
`ReplicateRead` (coordinator-to-replica, internal) as four distinct RPCs
rather than reusing `Put`/`Get` for both roles — chosen specifically so a
replica's log can tell "a client asked me directly" apart from "a
coordinator asked me on a client's behalf" without inspecting request
internals, which matters given this project's correlated-log debugging
approach. Value fields are `string`, not `bytes`. No timestamp/version
field anywhere yet — deliberately deferred to Phase 4, so as not to
quietly pre-commit to LWW before that phase's real decision between LWW
and sibling versions. `N`/`W`/`R` are node-level CLI flags (same pattern
as `--vnodes`), not per-request parameters.

**`N`/`W`/`R` validation — resolved, strict.** `main()` will enforce
`N, W, R >= 1`, `W <= N`, `R <= N`, and `W + R > N` at startup, refusing
to start (same `return 1` usage-error path as `listen.empty()`) rather
than accepting a configuration that can never satisfy quorum. Chosen
over leaving these unvalidated the way `--gossip-interval` currently is
(Q9) specifically because the failure modes aren't comparable: a bad
interval value degrades gracefully or crashes obviously and immediately,
while a bad `N`/`W`/`R` combination (`W=0`, or `W+R <= N`) would
silently produce wrong-looking quorum behavior later — a write reporting
success with no real durability guarantee, or a read that can never
detect a stale replica — which is much harder to notice than a startup
crash. **Decided, not yet implemented** — `main()` doesn't parse
`--N=`/`--W=`/`--R=` yet.

**Coordinator role is not tied to preference-list membership.** Any node
can coordinate any key's request, but it's only *in* that key's
preference list when its own tokens happen to fall among the top-N — not
by virtue of coordinating. When it is: it writes locally first (no
self-RPC — an RPC-to-self can fail in loopback-specific ways a direct
local write can't) and that local write counts toward `W`. `ReplicateWrite`
fans out to the rest of the list, fired concurrently — sequential fan-out
would make write latency the *sum* of replica response times instead of
the *max*. `>= W` acks (local write included) marks the write successful.
Read side is symmetric: `ReplicateRead` to `R` replicas, also concurrent.

**Fan-out completion semantics — resolved: concurrent launch, wait for
all, then count.** "Concurrent" answers *when the RPCs start*, not *when
the coordinator stops waiting for them* — these are separate axes, worth
having kept separate on purpose. Considered and rejected: returning to
the caller the instant the `W`th (or `R`th) ack arrives, leaving the
remaining launched RPCs running unattended. Rejected because it turns a
bounded, easy-to-reason-about `std::async` + `.get()`-in-a-loop into a
real thread-lifetime problem — a shared counter/condition-variable to
wake the waiter early, plus a decision about whether a straggler's
result, arriving into shared state after the request has already
returned, needs that state to outlive the request — in exchange for a
latency optimization nothing in this phase's scope actually asks for.
Concurrent launch alone already gets the "latency is the max, not the
sum" property the paragraph above cares about; waiting for every
launched RPC to finish before checking the count is what keeps the
fan-out-plus-quorum logic itself simple. The read side's "return the
first response received" doesn't need early termination either: every
launched `ReplicateRead` still runs to completion, each result gets
stamped with an arrival-order sequence number as it lands, and "first"
is read off that sequence after all of them are in — not raced for live.

**`N` is not a pass/fail threshold — only `W`/`R` are, and only the
coordinator ever checks them.** `preferenceListForKey` can legitimately
return fewer than `N` candidates, either because fewer than `N` distinct
physical nodes exist in the ring yet, or because `N`-or-more exist but
some are currently unreachable — deliberately not distinguished (see the
reachability section below); the coordinator doesn't need to know which
cause produced a short list, only how short. Neither cause is itself a
failure. The only question that matters, checked *before* any RPC goes
out: is `preference_list.size() >= W` (write) / `>= R` (read)? If not,
the request is already guaranteed to fail and the coordinator can refuse
without dialing anyone. If so, the coordinator fans out to every
candidate it has — which may itself be fewer than `N` — and only fails
if the actual ack/response count comes up short of `W`/`R` afterward.
This judgment lives entirely in the coordinator: `preferenceListForKey`
and `ring.hpp`'s `preferenceList()` stay completely ignorant of `W`/`R`,
and of pass/fail generally, consistent with `ring.hpp`'s existing
single-purpose, side-effect-free design (pure ring math, testable on
fabricated input alone). One consequence worth flagging for Phase 5:
`preferenceList()` still caps at exactly `N` candidates — it does not
walk further to stockpile standby nodes the way Dynamo's own preference
list does specifically for hinted-handoff purposes (paper §4.3). That's
deliberately out of scope until Phase 5 actually has hint bookkeeping to
attach to a standby candidate; adding it now would be building half a
mechanism with nothing to attach the other half to.

**Read conflict policy — deliberately the naive floor, not LWW.** When
`R` responses disagree, the coordinator returns the first response
received and logs the disagreement. Discussed and rejected as a design
option: returning the highest-`last_updated` response — that's not a
simpler fallback, it *is* one of Phase 4's two real candidate policies
(LWW) arrived at a phase early, and it assumes a timestamp field that
doesn't exist yet on stored values. The actual naive floor has zero new
state and zero comparison logic: whichever reply lands first, full stop.
It's supposed to be bad — that badness is the argument for why Phase 4
needs to exist at all.

**Reachability tracking — private, non-gossiped, per-peer state; not a
`NodeStatus` value.** Deliberately not added to `MembershipEntry`/
gossiped: doing so would let one node's local, possibly-wrong belief
about a peer propagate as if it were cluster-agreed fact, which is
exactly the SWIM-style mechanism Section 2 already declined in favor of
Dynamo's model. Lives as its own state in `NodeServiceImpl`
(`std::unordered_set<std::string>` of currently-*unreachable* peer IDs —
presence means unreachable, absence means reachable, which gets the
locked optimistic default for a peer known only through gossip, never
directly contacted — "unknown = reachable" — for free, with no
special-casing), under its own mutex, separate from `table_mutex_` and
the new KV store's lock.

Marked unreachable on any failed `Put`/`Get`/`ReplicateWrite`/
`ReplicateRead`/gossip outcome to that peer. Two paths back to reachable,
and only two: (1) a successful gossip round to that peer —
`selectGossipTarget` stays `UP`-only and must never start filtering on
reachability, since once a peer is excluded from `preferenceList()`,
gossip is the only remaining channel that still contacts it at all; (2)
a new dedicated background probe thread, using the existing (previously
unused) `Ping` RPC, on its own configurable interval, polling only the
current unreachable set — same loop-then-sleep shape as `gossipLoop`,
deliberately not condition-variable/notify-based (considered and
rejected: solves a CPU cost — an O(1) empty-set check every few seconds —
that doesn't meaningfully exist at this interval and scale, in exchange
for a new synchronization primitive and a real new bug class, a missed
`notify_one()` from any of five different marking call sites).

**Hysteresis (last-N-outcomes) considered and rejected in favor of a bare
bool, single-most-recent-outcome.** This matches both the project plan's
own Phase 1 description ("clears as soon as a request to that peer
succeeds again") and the Dynamo paper's own §4.8.3 description of local
failure detection — neither describes anything with memory across
multiple outcomes. Flapping is accepted, not engineered away: sloppy
quorum (`W`/`R` < `N`) is specifically what already tolerates a
wrong-for-one-cycle routing decision, so smoothing reachability at the
source spends real complexity (and, for any threshold-to-recover > 1, real
availability — a node's needless exclusion time) solving a problem the
architecture already has slack for. This has zero correctness weight
either way, since reachability never touches the gossiped membership
table that actually governs correctness.

**`preferenceList()`'s new parameter — a predicate, not raw reachability
state.** Per Q4's resolution ("the actual reachability filter as a
second, additive condition on the same function"), the ring walk gains a
second skip condition alongside its existing physical-node dedupe: a
`std::function<bool(const std::string&)>` reachability check, defaulted
to always-`true`. Every existing caller — including `test_ring.cpp`'s
synthetic-snapshot tests — keeps compiling and behaving identically
unchanged. Chosen over passing the raw `unordered_set` directly,
specifically to preserve `ring.hpp`'s existing property of being fully
standalone and testable against fabricated inputs, with no dependency on
`NodeServiceImpl`'s concrete types.

**KV storage — new state, not yet touching anything else.**
`std::unordered_map<std::string, std::string>`, bare value, own mutex —
separate from `table_mutex_` and reachability's mutex specifically so
`Put`/`Get`, gossip rounds, and reachability marking never serialize
against each other for no reason. Three independent locks total in the
system after this phase.

**`REMOVED` node behavior — see `OPEN_QUESTIONS.md` Q15/Q16, now
resolved.** Declines `Put`/`Get`/`ReplicateWrite`/`ReplicateRead`; keeps
initiating and answering gossip, and keeps answering `GetStatus`/`Ping`.

**Client — new stateful library, named `DriftClient`, not an extension
of `client.cpp`.** `connect(seed_nodes)` takes a list with fallback across it, mirroring
`bootstrapFromSeed`'s own shape, rather than a single fixed target. A
single-target client would relocate GTStore's manager-SPOF problem onto
"whichever one address happened to be passed on the command line" instead
of eliminating it — the whole point of "any node can coordinate" doesn't
reach the client side without this.

### GTStore comparisons made this session
- **Client bootstrap SPOF, revisited.** Already noted in Phase 0 that
  Driftstore's `client.cpp` has no manager-dial step GTStore's `init()`
  has. Building the real client library surfaced the sharper version:
  a client that only ever knows *one* node address has just moved the
  SPOF from "the manager" to "whichever node happened to be on the
  command line" — `connect(seed_nodes)`'s fallback list is what actually
  closes that gap, not the absence of a manager by itself.
- **Read/write asymmetry has no GTStore analogue.** GTStore's write-all/
  ack-all model means every replica has the same value by the time any
  read can happen — there's no such thing as a read getting back
  disagreeing versions in that model. Driftstore's read handler needs
  disagreement-handling logic a write handler doesn't, purely because
  sloppy quorum permits replicas to diverge in the first place.
- **Reachability's flapping-is-fine stance is a direct consequence of
  sloppy quorum existing at all.** GTStore has no quorum concept — every
  replica gets every write — so it has no equivalent slack to spend. A
  wrong-for-one-cycle routing decision here costs nothing structurally;
  the same kind of local misjudgment in a write-all system would be a
  correctness bug, not a tolerated inefficiency.

### Implementation session — replica RPC boundary, reachability state, probe thread

**KV storage + `ReplicateWrite`/`ReplicateRead` — implemented, verified.**
`std::unordered_map<std::string, std::string> kv_store_` with its own
mutex; `localPut`/`localGet` wrap it. `ReplicateWrite`/`ReplicateRead`
are thin: `isSelfRemoved()` guard first (Q16 now confirmed in code, not
just design), then a direct call into `localPut`/`localGet`. Verified
against a single live node with no coordinator involved, via a new
`harness/test_replica_boundary.sh` plus two new `client.cpp` modes
(`--replicate-write=<key>=<value>`, `--replicate-read=<key>`): a
write-then-read round-trips; a read on a never-written key returns
`found=false`; and, the stronger check, a node that has self-removed via
`--remove=<own address>` refuses a `ReplicateRead` for a key it's
*known* to already hold (written before removal) — proving the guard
actually fires, rather than merely proving the key is absent.
**Confirmed passing.** Not covered by this harness, by design: whether
`ReplicateWrite`'s `REMOVED` branch skips `localPut` internally vs.
writing-then-discarding is unobservable externally once `REMOVED`, since
`ReplicateRead` is blocked too — that's a code-inspection guarantee, not
a test-harness one.

**Reachability state — implemented.** `unreachable_peers_`
(`std::unordered_set<std::string>`, presence = unreachable) plus its own
mutex, as designed. API surface turned out to be three functions, not
four: `markUnreachable(peer_id)`, `markReachable(peer_id)`, and
`unreachableSnapshot()` (returns a copy of the current set). No
`isReachable(single_id)` — traced every intended caller (the probe loop,
`preferenceListForKey`), and neither ever asks about one peer in
isolation; both only ever want "the whole current set," so a per-peer
query was dropped as an API surface nothing would call.

Wired into `SendGossip`'s existing success/failure branches
(`markReachable`/`markUnreachable` on `peer_address`) — the only
outbound RPC that exists in the codebase at this point. The other four
triggers named in this section's original design (`Put`, `Get`,
`ReplicateWrite`, `ReplicateRead` failures) have no caller yet — that's
still steps 7/8, ahead — and are deliberately left unwired rather than
built untested. Noting this explicitly so it doesn't read as finished
when 4 of 5 intended trigger points don't exist yet.

**`preferenceListForKey`'s nested-locking problem — resolved.** Feeding
a *live* reachability check into the ring walk while `table_mutex_` is
already held would establish `table_mutex_` → `unreachable_peers_mutex_`
as a lock-acquisition order for the first time in this codebase — not
itself a deadlock, but one waiting to happen the moment any other code
path acquires the same two locks in the opposite order. Two alternatives
considered and rejected before landing on the fix: (1) a single merged
lock covering both `table_` and `unreachable_peers_` — rejected, since
it would serialize reachability churn (which will happen on every RPC
outcome once steps 7/8 exist) against ordinary gossip/membership work
for no structural reason; (2) generating the plain preference list first
and filtering reachability *afterward* — rejected, because
`preferenceList()`'s walk is specifically built to skip a failing
candidate and keep walking to find a replacement, so post-hoc filtering
can silently return fewer than `N` even when `N` reachable candidates
exist further down the ring, forcing the coordinator to re-derive the
walk's own skip-and-continue logic one layer up. Actual fix: apply the
existing snapshot-then-release-lock pattern `gossipRound`/
`bootstrapFromSeed` already use for network calls, to a second-mutex
acquisition instead — `unreachableSnapshot()` copies the set and
releases `unreachable_peers_mutex_` *before* `table_mutex_` is ever
taken, so the two locks are never held simultaneously and there's no
ordering rule to violate or document. The copied set is captured by move
into a `std::function<bool(const std::string&)>` closure passed to
`preferenceList()`. Same staleness tolerance every other snapshot in
this file already accepts (the set can be a few ms stale by the time the
walk runs) — not a new kind of imprecision.

**`preferenceList()`'s predicate — implemented in `ring.hpp` (author's
own edit, not yet reviewed in chat).** Wraps the existing `seen`-dedupe
check with the reachability predicate. Three rules settled explicitly
before writing it: an unreachable candidate is skipped outright — never
added to `seen`, never added to the result, even though it's a unique
physical node; a reachable-and-already-seen candidate is skipped as
before; a reachable-and-new candidate is added to both, as before. The
deliberate cost of the first rule: an unreachable physical node with
multiple vnodes gets re-rejected by the predicate once per vnode
encountered in a single walk, instead of being remembered and skipped
after the first rejection — accepted as bounded, wasted-but-not-incorrect
work, since `visited >= ring.size()` still guarantees termination
regardless of how many times any single candidate gets rejected. **Not
yet confirmed:** the corresponding `test_ring.cpp` case (fabricated
ring, a predicate excluding one node, confirming both the exclusion and
that the three existing unrelated tests still pass against the new
default-`true` parameter) was planned but not confirmed written or run
as of this session.

**Probe thread — implemented.** `reachabilityRound()`/
`reachabilityLoop()`, same loop-then-sleep shape as `gossipLoop`,
spawned as a second detached thread from `start()` (now
`start(gossip_interval_ms, peer_interval_ms)`). Own flag,
`--probe-interval=` — not shared with `--gossip-interval`, since the two
serve different purposes and have no structural reason to share a
clock. Probing within a round is concurrent (`std::async` per
unreachable peer, `.get()` on all before the round ends) — same
launch-all-wait-for-all shape as the fan-out decision above, chosen so
one genuinely-dead peer can't block probes to others in the same round
that may have already recovered. Each probe (`pingPeer`) uses a 500ms
RPC deadline specifically so a dead peer can only cost the round a
bounded amount of time — a starting value, not a tuned one. An empty
unreachable set produces no log line at all — unlike `GOSSIP_NO_PEERS`
(rare, notable), "nothing is currently unreachable" is the default
healthy state on most ticks of a working cluster, so logging it every
interval would be pure noise; actual probe attempts/outcomes still log
via three new `EventType`s.

**New `EventType`s: `PROBE_SENT`, `PROBE_SUCCEEDED`, `PROBE_FAILED`.**
Deliberately distinct from the existing `PING_SENT`/`PING_SUCCEEDED`/
`PING_FAILED`, which stay scoped to `client.cpp`'s human-initiated
diagnostic ping. Reusing the existing three would conflate "someone ran
a manual health check" with "the background probe thread's routine
sweep" under one log signature — exactly the ambiguity correlated-log
debugging can't afford. Same reasoning `GOSSIP_NO_PEERS`/`GOSSIP_FAILED`
already established as precedent for splitting event types along a
similar seam.

#### Bugs caught during implementation (this session)

- `std::unordered_set<const std::string&> unreachable_peers_` —
  reference types aren't valid container value types (no
  default-constructibility, no reassignment); fixed to plain
  `std::string`.
- `ReplicateWrite`'s `if (isSelfRemoved)` — missing call parentheses; a
  bare non-static member function name isn't a valid standalone
  expression. `ReplicateRead`'s equivalent check was written correctly
  in the same draft, so this was an inconsistency between the two
  handlers, not a repeated misunderstanding.
- `localPut(request->key, request->value)` / `localGet(request->key)` —
  missing `()` on protobuf accessor methods.
- `return grpc::Status:OK;` (single colon) in both `ReplicateWrite` and
  `ReplicateRead` — not `::`.
- `ReplicateRead`'s `REMOVED` branch called
  `response->set_success(false)` — `ReplicateReadResponse` has no
  `success` field (only `found`/`value`); a copy-paste artifact from
  `ReplicateWrite`'s branch just above it in the same draft.
- Both handlers initially declared with `StatusRequest`/`StatusResponse`
  parameter types (copy-paste from `GetStatus`) instead of their own
  generated `Replicate{Write,Read}{Request,Response}` types — caught by
  `override` refusing to compile once the proto's real RPC names were in
  use, exactly the failure mode `override` exists to catch.
- `localGet`: `kv_store.find()` (missing trailing underscore, called
  with no argument), the returned iterator treated as if it were the
  stored value itself, and a missing `return std::nullopt;` on the
  not-found path — same undefined-behavior-via-falling-off-a-non-void-
  function bug class as the `toString(EventType)`/`GOSSIP_NO_PEERS`
  crash logged under Phase 1.
- `markUnreachable`'s body called `unreachable_peers_.insert(node_id)` —
  `node_id` undeclared in that scope; the parameter is `peer_id`.
  `markReachable`, written correctly in the same pass, uses `peer_id`
  properly — another same-draft inconsistency, not a repeated
  misunderstanding of the concept.
- `preferenceListForKey`'s lambda captured a set under the name
  `unreachable_peers` (via
  `[unreachable_peers = std::move(unreachable_peers)]`) but the body
  referenced `unreachable` — undeclared identifier.
- `reachabilityLoop` had no `std::this_thread::sleep_for(...)` between
  rounds — a busy-spin bug the compiler can't catch, found by comparison
  against `gossipLoop`'s otherwise-identical shape.
- `main()` still calls `service.start(gossip_interval_ms)` with one
  argument after `start()`'s signature grew to two
  (`gossip_interval_ms, peer_interval_ms`) — **flagged, fix given, not
  yet confirmed applied.** Needs a new `--probe-interval=` flag (same
  parsing pattern as `--gossip-interval=`) and the updated call site.
  Compile-blocking, not a design question — first thing to check in a
  resumed session.

#### GTStore comparisons made this session

- **`N` vs. `W`/`R` as separate concepts has no GTStore analogue.**
  GTStore's write-all/ack-all model has no notion of "enough replicas
  exist to possibly succeed" as distinct from "all replicas exist" — `K`
  (its replica count) is simultaneously the target list size and the
  success threshold, because it's always both at once. Driftstore
  splitting `preference_list.size()` (bounded by `N`) from the actual
  pass/fail gate (`W`/`R`) is what makes a coordinator's fan-out
  meaningfully different from a `K`-of-`K` write: a shortfall against
  `N` isn't automatically a shortfall against the thing that actually
  decides success.
- **The probe thread is a mechanism GTStore never needed at all.**
  GTStore's manager is assumed reachable — there's no local, per-node
  belief about a peer's liveness to maintain, because no node other than
  the manager ever needs an opinion about who else is up. Driftstore's
  probe thread (and the reachability state it feeds) exists specifically
  because "any node can coordinate" means every node independently needs
  its own, possibly-wrong, self-correcting opinion about who's currently
  reachable — there's no single authority to just ask instead.

#### Not yet done — resume point for Phase 3

- `main()`: add `--probe-interval=` flag, update `service.start(...)`
  call to pass both intervals. Compile-blocking as of this session's
  last code shown — check this first.
- `ring.hpp`'s predicate change and the corresponding `test_ring.cpp`
  case: written per description, not yet reviewed in chat, not yet
  confirmed passing.
- `N`/`W`/`R` CLI flags + startup validation (`N,W,R >= 1`, `W,R <= N`,
  `W + R > N` — see the design-lock section above): decided, not
  implemented.
- `Put`/`Get` coordinator handlers (fan-out to `ReplicateWrite`/
  `ReplicateRead`, quorum counting against `W`/`R`,
  concurrent-launch-wait-for-all per the semantics locked above): not
  started. Flagged throughout as worth designing/writing personally
  rather than treating as boilerplate.
- `REMOVED` guard on `Put`/`Get` themselves (a separate call site from
  `ReplicateWrite`/`ReplicateRead`'s already-working guard): not
  started.
- `DriftClient` library (`connect(seed_nodes)` with fallback, `put`/
  `get`): not started.
- End-to-end verification harness (3-node cluster, kill one, write with
  `W < N`, confirm success, confirm the coordinator's log line and the
  killed replica's absence correlate): not started — natural next step
  once `Put`/`Get` exist.

---

## How to update this file
When a phase wraps: flip its status in the table, add a summary block (what
was built, key decisions + why, in the same shape as Phase 0's above), and log
any new GTStore comparisons that came up. When a question in
`OPEN_QUESTIONS.md` gets resolved, move it to that file's "Resolved" section
rather than deleting it — the reasoning is worth keeping.