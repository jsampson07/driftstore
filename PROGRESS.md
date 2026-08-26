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

### Open gap found during this session, not yet closed

`EventType::REMOVE_INIT` and `EventType::REMOVE_FAILED` are declared in
`logging.hpp` (enum entry + `toString()` case) but **not currently
emitted anywhere.** `RemoveNode`'s not-found path (`accepted=false`) does
not call `logEvent` at all — an admin attempting to remove an
unrecognized `node_id` currently produces zero log trail on the node that
rejected it. Not fixed as part of this session — see `OPEN_QUESTIONS.md`
Q15.

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

## How to update this file
When a phase wraps: flip its status in the table, add a summary block (what
was built, key decisions + why, in the same shape as Phase 0's above), and log
any new GTStore comparisons that came up. When a question in
`OPEN_QUESTIONS.md` gets resolved, move it to that file's "Resolved" section
rather than deleting it — the reasoning is worth keeping.