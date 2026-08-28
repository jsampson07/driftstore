# Driftstore — Open Questions

Running list of unresolved design questions. When one gets settled, move it
to "Resolved" with the decision and the reasoning — don't just delete it.

---

## Open

### Q2 — Is `client.cpp`'s `--self` an honest placeholder, or worth changing?
`client.cpp` isn't a real node — it's not bound to any address — so `--self`
(used to populate `sender_node_id` on outgoing Pings) is a fabricated
identity, not a real one. Options: keep it address-shaped (mirrors what a
real node will pass once Phase 3 makes every caller an actual node), or
switch to an honest fixed string like `"test-client"` to stop pretending it's
something it isn't.

*Status:* unresolved, not blocking — `client.cpp` is throwaway Phase 0
scaffolding either way. Now also used for `--status` queries, which have no
`--self` at all — doesn't change the resolution of this question, but worth
noting `client.cpp` now has two somewhat different identities depending on
mode (a fabricated node identity for Ping, no identity at all for status).

**Update, `RemoveNode` session:** now a third mode, `--remove=<node_id>`,
also with no `--self` identity — same shape as `--status`. Separately, this
session also surfaced a real naming-collision bug in this exact file's
vicinity: the proto field that became `RemoveRequest.removed_node_id` was
originally drafted as `target_node_id`, which collided with `--target`'s
already-established meaning ("address this client dials"), not the node
being acted on. Caught and renamed before landing — but it's a concrete
example of the identity-mess this question is about, not just an abstract
worry anymore. Doesn't resolve Q2, but raises the cost of leaving it
unresolved as more modes accumulate on one binary.

### Q3 — Does GTStore's `storage` / `test_app` need an explicit manager address?
`harness/smoke_test.sh` assumes `storage --port $PORT` and `test_app` locate
the manager via a hardcoded default (localhost + some fixed port), since no
manager-address flag was given for either. Unconfirmed against GTStore's
actual source.

*Status:* unresolved, low stakes — the harness passes under this assumption,
but worth confirming it isn't passing by accident (e.g. a stale process from
an earlier run happening to still be up).

### Q7 — Is `node_id == listen address` permanent, or will they ever need to diverge?
Currently `node_id_` *is* the `--listen` address string (locked in Phase 0, for stable
identity across restarts — see `PROGRESS.md`), and the own-entry constructor now sets
`address` to that same string (`self_entry.set_address(node_id_)`). Fine as long as a
node's identity and its network address are always the same thing. Worth flagging
because it stops being fine the moment those two ever need to diverge — e.g. a node that
restarts on a different address/port but should keep the same identity (existing token
placements, existing hinted-handoff obligations addressed to it). Not clear yet whether
that scenario is in scope for this project at all, or whether "identity == current
address, permanently" is an acceptable simplification for its intended scale.

*Status:* unresolved, not blocking — surfaced while fixing the `self_entry` constructor
bug (Phase 1 gossip session), not yet acted on anywhere.

**Update, `RemoveNode`/reboot-detection session:** this stopped being purely
hypothetical. During ad hoc manual testing of `NODE_REBOOTED`, a second
`node` process was started on an address a first process already held.
Because `AddListeningPort`'s failure is silent (Q11), the second process
kept running as an outbound-only gossip client while never actually being
reachable — two live processes briefly answering to one `node_id`
simultaneously, which is exactly the "identity and process lifetime
diverge" scenario this question asks about. Produced a real, confusing log
line (a process appearing to log `NODE_REBOOTED` about itself, which should
be structurally impossible — see `PROGRESS.md`). Did not reproduce once
testing moved to the scripted harness, which starts from a clean
`pkill`'d state each run. Worth treating as a concrete argument for
resolving this question before Phase 8's kill/restart fault injection
deliberately creates the exact conditions that produced it here by
accident.

### Q9 — No error handling on `--gossip-interval`'s parse in `main()`
`std::stoll(gossip_interval_ms)` throws on malformed input (e.g.
`--gossip-interval=abc`), and nothing catches it — unlike the
`listen.empty()` case, which prints a usage message and exits 1, a bad
`--gossip-interval` value crashes via an uncaught exception
(`std::terminate`). Every other flag in `main()` is a raw string with
nothing to parse, so this introduces a new failure mode that didn't exist
before this session's `main()` changes.

*Status:* unresolved, low stakes — leave as an unhandled crash-on-bad-input
for now (consistent with minimal validation elsewhere at this stage), or
wrap in try/catch with a usage message, matching the `listen.empty()` path.

### Q10 — Was the smoke-test hang actually root-caused, or just no longer observed?
While debugging the `toString`/`GOSSIP_NO_PEERS` crash, `smoke_test.sh`
separately appeared to hang after starting the Driftstore node (script
never reached its PASS/FAIL/summary output). The explanation given
("node process runs indefinitely because of the gossip loop") doesn't
actually account for this: the node is backgrounded with `&` in the
harness specifically so the script doesn't block on it — the only
foreground call in that section is `client`. The more likely mechanism is
that `client.cpp`'s `Ping` call has no deadline set on its
`grpc::ClientContext`, so it can block indefinitely if the channel never
reaches `READY` — but this hasn't actually been checked against
`client.cpp`'s source. Manual two-terminal testing replaced the script for
now, which may have just avoided the conditions that triggered the hang
rather than fixed them.

*Status:* unresolved — worth confirming whether `smoke_test.sh` currently
completes end-to-end before trusting it as a regression check again, and
worth checking whether `client.cpp` sets any RPC deadline at all. Notably,
`client.cpp`'s new `--status` path *does* now set a 2s deadline — `Ping`
still doesn't, so the asymmetry is now visible directly in the source, not
just suspected.

### Q13 — Should seed membership ever become dynamic (promotion/discovery) instead of a static admin-declared list?
Raised during `bootstrapFromSeed` retry/backoff design: with `--seed` now
accepting a list rather than a single address, worth asking whether that
list should ever be populated dynamically — nodes "promoted" to seed status
at runtime, or resolved via an external discovery mechanism (DNS SRV
records, cloud-provider instance tagging, etc.) — instead of being a fixed
set every node is handed identically at launch.

*Status:* unresolved, explicitly deferred by request ("I won't worry about
it for now, I will address it when it comes up"). Reasoning for choosing
static-for-now, on record in case this gets revisited: any dynamic scheme
still needs *some* fixed way for a brand-new node (zero cluster knowledge,
by definition) to learn the *current* seed set before it can join at all —
which either reduces to a static bootstrap list anyway (just one for
discovering seeds instead of joining directly), or introduces an external
discovery dependency that's authoritative about current seed identity,
which is structurally similar to the manager SPOF this project set out not
to have, even if it wouldn't be called a "manager." Cassandra's own
`seeds` config (static, admin-picked, no automatic rotation/election) was
used as rough precedent for the static-list decision.

*Context:* raised during Phase 1 `bootstrapFromSeed` retry/backoff design.

### Q15 — Should a live node that's learned its own status is `REMOVED` stop participating in gossip?
Surfaced during Phase 2 design while tracing what actually happens to a node
that's administratively removed but never killed: its own local table entry
for itself genuinely flips to `REMOVED` via ordinary gossip merge — see
`PROGRESS.md`'s Phase 1 correction, added this same session. Nothing
currently acts on that. Two separable sub-questions: (1) should
`gossipRound` check self-status before calling `selectGossipTarget`,
refusing to *initiate* further gossip once self-status is `REMOVED`; (2)
should the inbound `GossipExchange` handler *also* decline to respond or
merge once self-status is `REMOVED`, or is answering peers harmless since
nothing downstream depends on it.

*Status:* unresolved, not blocking Phase 2 — no data path currently depends
on the answer. Leaning yes on (1), gated inside `gossipRound` itself rather
than inside `selectGossipTarget` — folding it into target selection would
mean a `nullopt` return conflates "no `UP` peers exist" with "chose not to
look," both currently logged as `GOSSIP_NO_PEERS`; a genuinely no-peers
situation and a deliberate self-removed silence should probably be
distinguishable in the logs (e.g. a new `GOSSIP_SELF_REMOVED` event) rather
than collapsed into one that already means something else. Leaning no on
(2) — a node can't prevent peers from dialing it regardless of what it does
internally, and refusing to answer or merge doesn't buy anything it doesn't
already not-get from going silent as an initiator. Neither lean is locked.
Worth deciding before Phase 8's kill/restart fault injection, where a
remove-without-kill scenario producing a quietly-still-gossiping "removed"
node is exactly the kind of thing that'd be confusing to debug fresh if
it's still undecided by then.

*Context:* raised during Phase 2 design conversation, after confirming
empirically that a live-but-removed node's own view of itself does flip to
`REMOVED`.

### Q16 — Once Phase 3 exists, should a `REMOVED` node decline to coordinate or hold data?
Related to Q15 but a separate layer: even if a `REMOVED` node keeps
gossiping normally, should it also refuse to act as a coordinator or serve
as a replica for a key, once it's aware of its own removed status? No
coordinator exists yet, so nothing currently depends on the answer either
way.

*Status:* unresolved, explicitly deferred to Phase 3 design.
*Context:* raised alongside Q15, Phase 2 design conversation.

### Q17 — Phase 2's ring-mutation test coverage: two of five points still open, by choice
`test_ring.cpp` now covers three of the five ring-mutation points —
new-node discovery, and `REMOVED→UP`/`UP→REMOVED` via a chained
`testReboot` lifecycle test that checks both `local` and `ring` at each
step. The removal step is genuine regression coverage for the two
`UP→REMOVED` bugs found during implementation (see `PROGRESS.md`): its
`incoming_entry` is built with empty tokens via `clear_tokens()`, matching
production, and would fail against the old buggy version.

Two points remain open, both a deliberate decision, not an oversight:
- **`RemoveNode`'s direct erasure** — not a free function, so covering it
  needs a live gRPC round trip or a further extraction of its ring-erase
  logic. No live multi-node test has ever touched any Phase 2 code, and
  there's still no RPC/CLI surface exposing `ring_`/`preferenceList`
  externally.
- **Same-status `UP→UP` refresh** — the path with no dedicated `mergeInto`
  branch, the one the table-write regression actually broke. Declined
  knowingly: a future refactor that reintroduces that regression would go
  uncaught by anything currently in the suite.

Options unchanged from before: (a) close either or both later, (b) accept
as carried debt into Phase 3, (c) a minimal debug RPC exposing
`ring_`/`preferenceList`, which would help both this and the general
live-observability gap.

*Status:* unresolved, explicitly accepted as open debt — Phase 2 declared
done with this gap known and documented, not discovered later.
*Context:* surfaced closing out Phase 2; narrowed after `testReboot` was
added; final scope decided explicitly rather than by default.

---

## Resolved

### Q1 — Gossip membership conflict resolution (Phase 1)
When two nodes gossip and their membership tables disagree about the same
peer entry — e.g. one says "up," the other says "removed" — how does a node
decide which entry wins? What goes in the version field used to break the
tie: a per-node logical counter, a wall-clock timestamp, or something else?

**Resolution:** LWW on `last_updated` (wall-clock timestamp asserted at the
time of the change). On an exact timestamp tie, higher `writer_id` wins as
a deterministic secondary key. This doesn't recover true write order — it
guarantees every node computes the same winner regardless of merge order,
which is what actually makes the table converge (commutative, idempotent —
a last-writer-wins register, structurally a state-based CRDT).

*Context:* raised at the start of Phase 1's gossip design.
*Relevant reading:* Dynamo paper, §4.8.1 "Ring Membership."
*Full reasoning:* `driftstore-phase1-design-decisions.md`.

**Related edge case, not treated as a new problem:** surfaced during
`bootstrapFromSeed` retry/backoff design — a node rebooting on the same
address (per Q7, `node_id` *is* the listen address) can receive gossip from
a peer that still has a stale pre-crash entry for it while bootstrap is
still retrying. LWW means the node's own fresh self-entry, timestamped at
construction on this boot, beats that stale incoming entry — but only as
long as local wall-clock ordering is trustworthy across the reboot. An
unsynced clock immediately after boot (pre-NTP-sync) could in principle let
a stale incoming entry out-timestamp the fresh self-entry. Same trust
assumption Q1 already accepts project-wide for LWW generally — not a new
problem, just a concrete scenario where it could actually bite.

### Q6 — Gossip target selection: whole table, or `UP`-only entries?
Should the periodic gossip round pick a random peer from every entry in the
local table, or only from entries currently marked `UP`?

**Resolution:** `UP`-only. Gossip carries the sender's *entire* table, not
just its own status, so correcting a stale view of some node X doesn't
require dialing X directly — it happens through merging with whichever live
peer you do select. Selecting from the whole table would mean periodically
trying to contact nodes already believed `REMOVED` (which, per the removal
design, are expected to no longer be running) for no information gain.

*Context:* raised while designing the round scheduler's peer selection.
*Full reasoning:* this chat's Phase 1 implementation thread.

### Q11 — Stale/stopped processes from abandoned gdb sessions can hold a
listen socket indefinitely, masking real behavior as a network failure
Multiple `node --listen=127.0.0.1:60051` processes launched under `gdb`
across earlier debugging sessions, never cleanly killed, were left in a
*stopped* state (`Tl`/`tl` in `ps aux`) — the kernel still held their
`LISTEN` socket (visible in `ss -ltnp`) even though the process couldn't
`accept()` a connection or complete a gRPC/HTTP-2 handshake. A subsequent
legitimate `node` process targeting the same address silently lost the
bind (`AddListeningPort`'s success/failure out-param is unchecked), and
every peer's gossip attempt against that address then failed after a ~20s
connect timeout — indistinguishable, from the logs alone, from a genuine
network/reachability failure.

**Resolution:** `kill -9` (not plain `kill`/`SIGTERM` — a stopped process
doesn't act on `SIGTERM`) on both the stopped `node` processes and their
`gdb` parents. Adopted as standing practice: `pkill -9 -f bin/node` before
any multi-node test run, and prefer `gdb`'s own `kill`/`quit`-with-confirm
over backgrounding a debug session and moving on.

*Context:* surfaced during the first multi-node convergence verification
run for Phase 1.
*Relevant follow-up, not yet fixed:* `AddListeningPort`'s return value is
still unchecked in `node.cpp`, so a real bind failure still can't be told
apart from healthy startup in that node's own logs. Worth fixing before
Phase 8's kill/restart fault injection, where this exact ambiguity would
directly undermine a test result's validity.

**Update, `RemoveNode`/reboot-detection session:** this gap stopped being
purely theoretical — it was the direct root cause of a real confusing
debugging session (a silently-failed second bind produced a `NODE_REBOOTED`
log that looked like a node revived itself, see `PROGRESS.md` and Q7's
update above). Raising this from "worth fixing before Phase 8" to "has
already produced one real incident outside of Phase 8" — still not fixed,
but the priority case is no longer hypothetical.

### Q12 — Should the Makefile enforce `-Werror` so a missing `EventType`
switch case fails the build instead of crashing at runtime?
The `GOSSIP_NO_PEERS` crash (see `PROGRESS.md`, "Bugs caught during
implementation") happened because `toString(EventType)`'s switch was
missing a case — and it compiled successfully anyway. Checked the
Makefile: `-Wall` is set, and `-Wswitch` (which would flag exactly this) is
part of `-Wall`, so the warning *is* being emitted. But there's no
`-Werror`, so the warning doesn't fail the build — the exact bug that
already happened once could happen again on a future `EventType` addition
and still produce a "successful" compile.

**Resolution:** not being adopted right now — deprioritized by explicit
request. `-Werror` would also hard-fail the build on any warning from
generated protobuf/gRPC code, not just hand-written code, which is a real
cost worth being aware of if this gets revisited (the usual mitigation is
scoping it narrower, e.g. `-Werror=switch`, rather than blanket
`-Werror`).

*Context:* raised while closing out Phase 1's status-endpoint session.

### Q5 — Does admin `remove` need to target a designated seed, same as bootstrap?
The ordered-fallback-with-backoff seed mechanism (250ms timeout, exponential
backoff, 3-try cap) was designed to solve "an actor with zero prior cluster
knowledge needs a first point of contact" — which is necessarily true for a
brand-new node joining (it only has its `--seed` launch flags to go on).
It's not obviously true for an admin issuing a `remove` command: the admin
already stood up the cluster and may already know the address of any live
node, not just a seed.

**Resolution:** No — `remove` does not need seed-targeting. The seed
mechanism specifically solves "zero cluster knowledge, needs a first
contact," which is true by definition for a joining node and not true for
an admin issuing `remove`: naming a `removed_node_id` in the request
already requires knowing that node's address, so the admin trivially has
at least one live address to send the RPC to (the target node itself, or
any other node they already know about). `RemoveNode` can be sent to any
node the admin knows is live — no retry/backoff, no seed restriction.

Also resolved alongside this: whether a separate admin `Join` RPC was
needed at all. It isn't — `bootstrapFromSeed` plus the self-entry written
at construction plus ordinary gossip propagation already cover a new
node's presence becoming known cluster-wide with no admin action
required. Only `remove` needed new RPC surface.

*Context:* raised during Phase 1 proto/schema work.
*Resolved during:* the `RemoveNode`/reboot-detection design and
implementation session — see `PROGRESS.md`.

### Q8 — Should `SendGossip` surface RPC success/failure explicitly to its caller?
Currently `SendGossip` returned a plain `GossipResponse`, with the
`grpc::Status` checked (and logged as `GOSSIP_SUCCEEDED`/`GOSSIP_FAILED`)
only inside `SendGossip` itself, then discarded. Both callers applied the
response unconditionally, relying on gRPC leaving the out-param untouched
(default-constructed, empty `map`) on failure rather than checking for
success explicitly.

**Resolution:** Changed `SendGossip`'s return type to
`std::optional<driftstore::GossipResponse>` — `std::nullopt` on RPC
failure, the response wrapped in `optional` on success. Both callers
(`gossipRound`, `bootstrapFromSeed`) now check `if (response)` explicitly
before calling `applyGossip`, rather than relying on the previous implicit,
unverified-by-the-caller safety.

Considered and rejected: also exposing the `grpc::Status` itself (e.g. via
`std::pair<grpc::Status, GossipResponse>` or an out-param), specifically to
distinguish a genuinely dead peer from one that's merely slow or
partitioned. Rejected because (a) no caller branches on failure *type*,
only failure-or-not — the locked retry/backoff design (see `PROGRESS.md`)
retries uniformly regardless of why an attempt failed, and (b) the
underlying problem this would be solving — telling "dead" apart from
"slow" — isn't actually solvable from a single RPC's status code in an
asynchronous network; a lost message and an arbitrarily delayed one look
identical from the caller's side. That distinction, when it matters,
belongs to Phase 3's reachability tracking (built on aggregated signal —
consecutive-failure counts, latency history — not one call's status), not
to this return type.

Also considered: a named result struct (`SendGossipResult { bool ok;
GossipResponse response; }`) instead of bare `std::optional`, for
readability at call sites. Deferred — `std::optional<GossipResponse>` says
everything currently needed; converting to a struct later (e.g. if a
per-call field like round-trip latency is ever needed) is a small,
low-risk refactor when that need actually materializes, not before.

*Context:* raised while tracing the gossip round end-to-end from memory.
*Resolved during:* this chat's `bootstrapFromSeed` retry/backoff design and
implementation thread.

### Q14 — Does `main()`'s bootstrap-failure exit path need an explicit `server->Shutdown()` before returning?
`main()` exits (`return 1`) if `bootstrapFromSeed` exhausts all 3 retry
passes without success — the first place in this codebase a process needs
to exit while a `grpc::Server` object exists but was never explicitly
`Shutdown()`'d (every other path reaches `server->Wait()` and blocks
forever instead). Unconfirmed whether `grpc::Server`'s destructor handles
an un-`Shutdown()`'d server cleanly on process exit.

**Resolution:** tested empirically against an all-unreachable seed list.
The process exits promptly and cleanly with exit code `1` on this path — no
explicit `server->Shutdown()` call is necessary. (Note on the test itself,
for anyone rerunning this: measuring the exit code through a `| tee` pipe
gives `tee`'s exit code, not `node`'s, via the standard `$?`-after-a-pipeline
gotcha — use `${PIPESTATUS[0]}`, or redirect directly without a pipe, to
measure the actual process's exit code.)

*Context:* raised during `bootstrapFromSeed` retry/backoff design, this
chat.
*Resolved during:* same thread, via the dead-seed-list empirical test.

### Q4 — Phase 2's preference-list computation has nothing to filter on yet
`driftstore-phase1-design-decisions.md` defers local per-peer reachability
tracking from Phase 1 to Phase 3 (no data path exists yet to generate a
signal to watch). But the project plan's Phase 2 description says
preference-list computation skips nodes "the coordinator's own local view
currently considers unreachable — this is where Phase 1's reachability
tracking actually gets used." If reachability tracking now lands in Phase 3,
Phase 2 has no filter signal to consume when it's built. Options: Phase 2
builds preference lists that treat every `UP` member as reachable, and
Phase 3 wires in the actual filter once reachability tracking exists; or
some smaller piece of reachability tracking gets pulled forward. Raised
during Phase 1 proto/schema work.

**Resolution:** Option A — Phase 2 treats every `UP` member as reachable;
preference-list computation this phase is pure ring math (walk clockwise,
collect N distinct physical nodes, skip only `REMOVED`). Phase 3 wires in
the actual reachability filter as a second, additive condition on the same
function once local reachability tracking exists to consume. Chosen over
pulling a minimal reachability signal forward into Phase 2, specifically to
keep the two mechanisms — gossiped membership and local, non-gossiped
reachability — decoupled in code the same way they're already decoupled
conceptually elsewhere in this project's plan, rather than re-coupling them
under Phase 2's time-box for the sake of a demo scenario ("coordinator
routes around a hung peer") that isn't due until Phase 3 regardless.

*Context:* raised during Phase 1 proto/schema work.
*Resolved during:* Phase 2 design conversation.