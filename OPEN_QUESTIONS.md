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

**Update, later session:** option (c) — a minimal debug RPC exposing
`ring_` — is now implemented (`ring_dump` on `GetStatus`) and was used to
run a live 2→3 node join scenario, diffed before/after (see
`PROGRESS.md`'s Phase 2 addendum for the full result). This closes the
specific "no live multi-node test, no RPC surface" reasoning that made
both remaining gaps hard to close — but doesn't itself add coverage for
either one. `RemoveNode`'s direct erasure and the `UP→UP` refresh path
both remain untested; the tooling to write those tests now exists, closing
them is still a separate, undone step.

### Q18 — Should `GetStatus` expose the current `unreachable_peers_` set?
Reachability state is deliberately private and non-gossiped (see
`PROGRESS.md`'s Phase 3 design section) — but that also means there's
currently no way to observe a node's own reachability belief from
outside it at all, unlike `table_`/`ring_`, which `GetStatus` already
exposes via `table_dump`/`ring_dump`. Raised while designing the probe
thread's verification story: confirming "a peer landed in the
unreachable set" currently requires trusting log lines (`GOSSIP_FAILED`,
`PROBE_FAILED`) rather than querying live state directly.

*Status:* unresolved, not blocking — probe thread and reachability
marking work and are logged either way; this is purely a live
debugging/observability convenience, not a correctness question. Worth
weighing against the same log-vs-noise instinct already applied to the
empty-set case (see `PROGRESS.md`'s probe thread section): exposing it
is arguably the same category as `ring_dump` — additive, cheap — but
it's still new surface on `GetStatus`, not something to add reflexively
just because it's easy.

*Context:* raised during the probe thread design/implementation session.

### Q19 — Should N/W/R be enforced system-wide, or admin-declared per-node by convention only?
Surfaced while starting `Put`/`Get`'s design: `N`/`W`/`R` are node-level CLI
flags, identical in delivery mechanism to `--vnodes`. But `--vnodes`
transfers safely because the thing it configures — the ring — is
synchronized ground truth: whatever tokens a node actually claims get
gossiped, and every node's ring converges to agreement regardless of what
value any individual node was launched with. `N`/`W`/`R` have no equivalent
mechanism. Nothing gossips them, nothing reconciles them; each process just
holds whatever its own launch flags said. Two distinct problems follow from
this, not one: (1) different `N` across coordinators isn't a strictness
difference on the same replica set, it's a different replica set entirely
for the same key — `preferenceListForKey(key, N)` returns physically
different nodes depending on which coordinator computed it; (2) even with
matching `N`, `W + R > N`'s overlap guarantee is a claim about the whole
system, not a per-coordinator setting — a write coordinated under one
node's locally-configured `W` and a read coordinated under another node's
locally-configured `R` have no guaranteed overlap unless every node is
actually enforcing the same values, which nothing currently confirms or
requires.

Two directions considered, neither implemented:
- **(A) Admin-declared, uniform by convention only.** Keep the CLI flags;
  the contract becomes "every node in the cluster must be launched with
  identical `N`/`W`/`R`," enforced by operational discipline (harness
  scripts), not by the system. Cheapest, and has real precedent in this
  project: Q13 already accepted the same category of risk deliberately for
  the seed list (static, admin-declared, no runtime reconciliation,
  Cassandra's `seeds` config as precedent). Matches how the Dynamo paper
  itself describes `N`/`R`/`W` as configured "per instance." Leaves a real
  gap: nothing detects a misconfigured node — worse here than for seeds,
  since Phase 8's fault-injection harness will be scripting node restarts,
  exactly where a flag typo could silently corrupt correctness with no
  signal.
- **(B) Gossip `N`/`W`/`R` (or at minimum `N`) as part of membership,
  detect/refuse on mismatch.** Closes the silent-misconfiguration gap. Real
  new complexity: a new gossiped field, and an undecided question about
  what a node should do on startup if its own config disagrees with what it
  learns from peers (refuse to start? defer to the cluster's value? which
  value wins on a tie, mirroring the question `mergeInto` already answers
  for `NodeStatus`?).

*Status:* unresolved — currently running as (A) by default, without ever
having explicitly chosen it; `Put`/`Get` shipped with node-level `N`/`W`/`R`
flags before this question was written down. Worth a deliberate decision
before Phase 8's fault injection starts exercising restart/reconfiguration
scenarios where a silent mismatch would actually bite.
*Context:* raised while designing `Put`/`Get`, before either handler was
implemented — see `PROGRESS.md`'s Phase 3 implementation session for the
full comparison against `--vnodes` and against GTStore's single-process `K`.

**Update, Phase 4 design session:** stakes raised further, not resolved.
Phase 4's vector-clock dominance comparison implicitly assumes every
coordinator computes `preferenceListForKey(key, N)` against the same `N` —
if two coordinators disagree on `N`, they can compute different replica
sets for the identical key, which undermines the premise that a `Get`'s
fan-out is even comparing clocks for "the same" replica set in the first
place. Still not blocking Phase 4 specifically (a single-`N`-per-cluster
test setup sidesteps it), but the cost of leaving this unresolved keeps
compounding as more phases build on `N`/`W`/`R` meaning something
consistent cluster-wide.

**Update, `phase4/put-write-path` session:** a concrete new manifestation
of the same underlying risk. Write coordination is now restricted to the
key's preference list (`PROGRESS.md` decision A3), implemented as a
receiving node forwarding to a preference-list member when it isn't one
itself. `forwardPut` sends the original request on, and the target
recomputes its own preference list using its own locally-configured
`N_` — if that target's `N_` differs from the forwarding node's, the
target could legitimately conclude it isn't in the preference list
either (by its own, different N), tripping the `forwarded=true`
loop-breaker and failing a write that a uniformly-configured cluster
would have coordinated successfully. Doesn't change either option under
consideration, just adds one more concrete failure mode to the pile Q19
already names.

### Q21 — No RPC deadline on the coordinator's internal `Put`/`Get` fan-out
Neither `Put` nor `Get`'s internal `ReplicateWrite`/`ReplicateRead` calls
(the ones issued from inside the `std::async` fan-out lambdas) ever set a
deadline on their `grpc::ClientContext`, unlike `pingPeer`'s 500ms
deadline. Invisible against a clean `kill -9` — the kernel sends a TCP
RST immediately, so the call fails fast and looks bounded. Genuinely
unbounded against a peer that's unresponsive without cleanly closing the
connection (packet black-holed, not killed) — `f.get()` blocks forever,
and so does the entire coordinator call, regardless of any deadline the
client set talking to the coordinator itself.

*Status:* unresolved — originally logged as a known gap in `PROGRESS.md`
during the Put/Get implementation session, promoted to a tracked question
here after resurfacing, unprompted, during the Phase 3 checkpoint
self-explanation session (the coordinator flow was described as "bounded
by the slowest node," which isn't accurate yet). Low risk against
`kill -9`-style fault injection; a real gap against Phase 8's actual
partition simulation, which won't fail this cleanly.
*Context:* raised during the Put/Get implementation session; reconfirmed
during the Phase 3 checkpoint session.

### Q22 — Should `DriftClient` fail over on a `success=false` response, not just a transport failure?
Two scenarios make this concrete. (1) A seed in `DriftClient`'s list has
since been administratively `REMOVED` — its `Put` handler returns
`success=false` immediately, before touching the ring at all; failing
over to a different seed would very likely succeed. (2) A seed is a
perfectly valid coordinator, but two of the three replicas for the key
happen to be down — it returns `success=false, acks=1` (`W=2`), a
genuine fact about the world; failing over to a different seed would hit
the *same* replica set (`preferenceListForKey` depends only on the key
and the ring, not on who's coordinating) and get the same answer, at the
cost of a second full fan-out. `PutResponse`/`GetResponse` currently give
no way to tell these two cases apart — no `reason` field, same
`grpc::Status::OK` either way — so `DriftClient` can't write different
code for each.

Two directions: **(A)** only fail over on transport-level failure
(`!status.ok()`), accept that a stale/removed seed entry just fails
cleanly until the seed list is updated by hand. **(B)** extend the proto
with a distinguishable reason/status so the two cases can actually be
told apart, and only fail over on the first.

**Resolution: (A), for now.** Not chosen as a compromise — the concrete
harness this was designed against (`kill -9` mid-run) never exercises
this ambiguity at all, since that failure shows up as a transport
failure, which Option A already handles with zero compromise. The
scenario that *does* need the distinction — a long-lived `DriftClient`
still holding a since-removed seed — isn't in any harness or demo
currently planned; building (B) now would be solving it on spec, the
same category of thing Q13 already declined to do for seed discovery.
Revisit if a concrete scenario actually needs it, same as Q13.

*Context:* raised while designing `DriftClient`'s failover semantics,
Phase 3 close-out session.
*Resolved during:* same session.

### Q25 — Wall-clock skew risk on Phase 4's LWW tiebreak
Phase 4's conflict resolution (`PROGRESS.md`, decisions B2/B3) falls back
to comparing wall-clock timestamps only in the case vector-clock dominance
comparison genuinely returns concurrent — but that fallback is exactly as
exposed to inter-node clock skew as gossip's `last_updated`-based LWW
already is (Q1's related edge case). Concrete failure case discussed in
session: a write proven causally later by vector-clock dominance, if
LWW were ever consulted for it, could in principle be coordinated by a
node whose clock runs behind the peer that coordinated the causally-earlier
write — though this specific scenario is actually *ruled out* by design,
since dominance comparison is always checked first and is immune to clock
skew by construction; LWW only ever sees pairs where no causal ordering
exists at all. The residual exposure is narrower than it first sounds, but
not zero: two genuinely concurrent writes' relative "recency" as judged by
LWW is still only as trustworthy as how well-synchronized the two
coordinating nodes' clocks are.

**Resolution: accepted, not solved — same treatment Q1 already gave
gossip's `last_updated`.** No NTP-style clock discipline is in scope for
this project. Explicitly named as a defensible-but-real tradeoff in the
LWW-vs-siblings decision (see `PROGRESS.md`'s Phase 4 section): the
alternative (sibling versions) avoids this risk entirely by never
discarding a write based on timestamp comparison at all, but was rejected
on its own separate cost grounds (client-exposed reconciliation, API
surface) — not because this risk was judged acceptable in isolation.

*Status:* accepted risk, tracked — not blocking, but worth keeping visible
alongside Q1 rather than letting it look silently resolved.
*Context:* raised during the Phase 4 vector-clock design conversation,
decision B3.

**Update, `phase4/vector-clock-library` session:** Q24 (below, now
Resolved) added a `writer_id` secondary tiebreak, which narrows LWW's
exposure but is a distinct risk from this one, not a fix for it — Q24's
residual gap is about `writer_id` not being a provably unique secondary
key for every `CONCURRENT` pair, this question is about the trustworthiness
of `last_updated` itself once LWW is consulted at all. Both are now
accepted-and-named on the same terms, not stacked into a false sense that
one covers the other.

### Q26 — Neither the logs nor the CLI expose `last_updated`/`writer_id` for a stored clock
Surfaced while building both of branch 7/8's harnesses. `renderClock()`
(`node_kv.cpp`) and `client.cpp`'s `clockToString()` both render only the
counters map — sorted `{node:count,...}`, nothing else. Neither
`PUT_SUCCEEDED`/`GET_SUCCEEDED`'s new `clock=` field nor
`--replicate-read`'s `clock=` output carries `last_updated` or
`writer_id`, and there's no separate flag exposing either. Practical
consequence: `test_conflict_scenario_race.sh` can confirm a real
two-coordinator race produces a genuine conflict and that it resolves to
one of the two legitimate values, but cannot check that it resolved to
the *correct* one under LWW — there's no ground truth available anywhere
to check the winner against.

**Not yet resolved — open design call, not just a missing feature.**
Closing this needs `renderClock`/`clockToString` extended to include both
fields (mechanical), but also raises a question adjacent to B1/B2: is
exposing a write's exact wall-clock timestamp and coordinating node to
any caller who can run `--get`/`--replicate-read` fine for a debugging
CLI, or does it deserve more thought before widening what's observable?
Leaning toward "fine to expose, this is a debugging CLI, not the client
API `DriftClient` wraps" — but that's a design call, not decided yet.

*Status:* open — blocks verifying LWW winner-correctness under any
harness that uses real (non-injected) concurrent writes.
*Context:* raised during `phase4/conflict-scenario-harness` (branch 8),
while scoping what `test_conflict_scenario_race.sh` could and couldn't
verify.

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

**Update, Phase 3 implementation session:** now actually implemented.
`ring.hpp`'s `preferenceList()` takes a
`std::function<bool(const std::string&)>` predicate (default
always-`true`), and `NodeServiceImpl::preferenceListForKey` supplies it
from a snapshot of `unreachable_peers_`, taken and released *before*
`table_mutex_` is acquired specifically so the two locks are never held
at once. See `PROGRESS.md`'s Phase 3 implementation section for the two
rejected alternatives (a single merged lock; filtering the list after
generating it) and the three walk rules the predicate enforces.

### Q15 — Should a live node that's learned its own status is `REMOVED` stop participating in gossip?
Surfaced during Phase 2 design: two sub-questions — (1) should a
self-`REMOVED` node stop *initiating* gossip, (2) should it also decline
*inbound* `GossipExchange`.

**Resolution:** (1) yes — stop initiating once self-status is confirmed
`REMOVED`; nothing left to usefully learn or contribute at that point.
(2) no — inbound gossip must never be gated, for the opposite reason
originally given. The original lean toward "no" reasoned that refusing to
answer "doesn't buy anything it doesn't already not-get from going silent
as an initiator" — that reasoning was backwards. Gossip propagating a
removal to *other* nodes happens through the removing node's own outbound/
inbound gossip, entirely independent of whether the removed node itself
ever gossips again. What inbound gossip actually provides is the *only*
way the removed node itself finds out it's been removed — if it declined
inbound `GossipExchange` the moment it (incorrectly) believed itself
still-`UP`, it could never receive the fact that changes that belief, and
would keep coordinating/serving indefinitely, the exact failure Q16
exists to prevent. Gossip stays open so the removed node can *learn* it's
removed, not so others can learn it from *this* node specifically.

*Context:* raised during Phase 2 design conversation.
*Resolved during:* Phase 3 design conversation.

### Q16 — Once Phase 3 exists, should a `REMOVED` node decline to coordinate or hold data?
Related to Q15 but a separate layer: should a `REMOVED` node refuse to act
as coordinator or replica once it knows its own status.

**Resolution:** Yes — declines `Put`, `Get`, `ReplicateWrite`, and
`ReplicateRead` (client-facing and internal replication data-path RPCs).
Continues answering `GetStatus` and `Ping` — both diagnostic, and refusing
them would make it *harder* to verify a node correctly knows its own
status (reduced to inferring removal from a dropped connection instead of
reading its own status dump). `ReplicateRead` was folded in explicitly
after being added to the proto later than `Put`/`Get`/`ReplicateWrite` —
worth noting since it's exactly the kind of gap that's easy to miss once
a decision looks already-settled.

*Context:* raised alongside Q15, Phase 2 design conversation.
*Resolved during:* Phase 3 design conversation.

**Update, Phase 3 implementation session:** confirmed in code and by
test. `isSelfRemoved()` now guards `ReplicateWrite`/`ReplicateRead`;
`harness/test_replica_boundary.sh` verifies a self-removed node refuses
a `ReplicateRead` for a key it's known to already hold (written before
removal), not just a missing key — proving the guard actually fires
rather than merely proving the key's absence. `Put`/`Get`'s equivalent
guard (a separate call site from `ReplicateWrite`/`ReplicateRead`'s) is
still pending, since `Put`/`Get` themselves don't exist yet.

**Update, Put/Get implementation session:** `isSelfRemoved()` now also
guards `Put` and `Get`, closing the gap this file's previous update left
open. Unlike `ReplicateWrite`/`ReplicateRead`'s guard — verified by
`harness/test_replica_boundary.sh` against a replica that demonstrably
already held the key — `Put`/`Get`'s guard has not yet been verified by an
equivalent test; this session's manual verification exercised only the
non-removed path (a healthy 3-node cluster, `Put` then `Get` from all
three). Confirming a removed coordinator actually refuses `Put`/`Get`
remains undone.

**Update, Phase 3 close-out session:** still undone — carried forward as
accepted debt at Phase 3 close, same treatment as Q17's two accepted gaps
at Phase 2 close, rather than blocking the phase on it.

### Q23 — Should `ReplicateWrite`/`ReplicateRead` log anything, given `Put`/`Get` already do?
Raised when a harness assertion expected replica-side log evidence
(`grep`-ing for a key in a replica's log after a coordinated write) and
found none — `ReplicateWrite`/`ReplicateRead` have zero `logEvent` calls,
success or failure, unlike every other RPC in this codebase.

**Resolution: intentional, not a gap.** Logging every internal
replication call in addition to `Put`/`Get`'s own success/failure lines
was judged to add clutter without adding signal that `Put`/`Get`'s
already-`key=`-bearing lines don't provide — the coordinator's own log
line is sufficient to confirm a write happened and roughly how many
replicas acked it. The harness that surfaced this
(`test_coordinator_rotation.sh`) was rewritten to correlate against
`Put`'s log line instead (counting `PUT_SUCCEEDED key=<k>` occurrences
per node, which also happens to prove *which* node coordinated *which*
call — a stronger check than replica-log presence would have given
anyway).

*Context:* raised during the Phase 3 close-out harness session, prompted
by a failing assertion that turned out to be checking for something
deliberately absent, not something broken.
*Resolved during:* same session.

**Update, `phase4/observability-logging` session:** this resolution's
"no routine logging" stance now has a narrow, deliberate exception —
`storeReplicatedWrite` (reached via `ReplicateWrite`) gained a
`logEvent` call for `EQUAL_CLOCK_DETECTED`. Not a reversal of this
decision: that event is an anomaly a coordinator's own log genuinely
cannot see on its own (it has no visibility into what a specific remote
replica's dominance check decided), which is different from the routine
success/failure reporting this question was actually about. Noted here
so this entry doesn't read as contradicted by `node_kv.cpp`'s current
code.

### Q24 — Exact-timestamp-tie tiebreak for Phase 4's LWW conflict resolution
Phase 4's LWW tiebreak (see `PROGRESS.md`'s Phase 4 design section, decision
B2) resolves a genuinely-concurrent vector-clock comparison by comparing
each candidate version's stored `last_updated` timestamp. Two different
concurrent writes — different clocks, different values — could plausibly
tie at whatever timestamp resolution is chosen (e.g. both stamped in the
same millisecond). Nothing currently defines a secondary tiebreak for this
case. Note this is distinct from B1's equal-clock case (Phase 4 design
section): B1 is the *same* write's clock appearing twice, safely
order-independent; this is *two different* writes whose independently-
computed timestamps happen to collide.

**Precedent available but not yet applied:** Q1 resolved the equivalent
problem at the gossip-membership layer with a `writer_id` secondary key on
an exact timestamp tie. Carrying the same shape forward here (e.g. the
coordinating node's own id) is the obvious candidate, but hasn't been
decided.

**Resolution:** `VectorClock` gained a third field, `writer_id` (string,
mirrors `MembershipEntry.writer_id`'s type) — the coordinating node's own
id, stamped alongside `last_updated` by whichever node builds the clock in
`buildNewClock`. `resolveLWW` tiebreaks in two steps: `last_updated`
first, `writer_id` second (higher wins — the same "some deterministic
total order breaks it" shape Q1 already established, not a claim that
node_id ordering carries any real-world meaning). A true double-tie (same
`last_updated` **and** same `writer_id`) deterministically keeps whichever
entry was encountered first in the input — documented at the call site as
an intentional fallthrough, not an accident of control flow.

**Correction made to the original justification, during implementation
design:** the initial reasoning for reaching for `writer_id` at all
assumed two genuinely `CONCURRENT` versions could never share a
`writer_id` — "a node can only have one version it sees for a key." That's
false in one specific, real case: a coordinator that isn't a replica for
the key in question has `local_copy = nullopt` on every call (per decision
A1), so it retains no memory of its own prior writes to that key between
separate `Put`s — its contributed axis is driven entirely by whatever
`client_context` a given caller happens to supply. Two different callers,
routed through the same non-replica coordinator, with mutually-non-
dominating cached contexts, can produce genuinely `CONCURRENT` clocks that
both carry that coordinator's `writer_id`. Constructed and verified
against `compareVectorClocks` directly: `{X:1,Y:2}` and `{X:1,Z:5}`, both
`writer_id=X`, correctly classified `CONCURRENT` (`X` axis ties, `Y` favors
the first, `Z` favors the second — both `a_greater` and `b_greater` end up
true).

Given that, the `writer_id` tiebreak doesn't formally eliminate Q24's tie
risk — it narrows it. A residual tie now requires *both* an exact
`last_updated` collision *and* this specific same-non-replica-coordinator-
divergent-context scenario to coincide. Accepted on the same terms Q25
already accepts wall-clock skew: named honestly, not solved, and not
asserted as a stronger guarantee than it actually is.

*Context:* raised during the Phase 4 vector-clock design conversation,
while settling decision B2 (LWW timestamp placement).
*Resolved during:* `phase4/vector-clock-library` (branch 2/9)
implementation session — `writer_id` field added, `resolveLWW`
implemented and tested (`test_vector_clocks.cpp`: timestamp-decides-it
case, timestamp-tie-broken-by-writer_id case, full-tie-keeps-first-seen
case).

**Update, `phase4/put-write-path` session:** the specific counterexample
above — a non-replica coordinator building two genuinely `CONCURRENT`
clocks that share its own `writer_id` — can no longer occur. That
branch restricted write coordination to the key's preference list (see
`PROGRESS.md`'s decision A3): a non-replica node now only forwards a
`Put`, it never calls `buildNewClock`. Combined with B1's existing
serialization (which already prevents a replica-coordinator from
producing two colliding clocks for its own sequential writes to one
key), there may no longer be any path to two genuinely `CONCURRENT`
clocks sharing a `writer_id` for the same key — which would mean this
question's tiebreak is complete, not merely narrowed. Recorded here as
a strong claim worth independently re-deriving before treating it as
settled, not as a re-resolution — vector clock construction is squarely
the kind of thing this project's owner verifies personally rather than
accepting on Claude's say-so.

### Q20 — Should a coordinator's own local read winning arrival-order ties be an explicit policy, or just an accepted side effect?
`Get`'s first-arrived-response selection stamped the coordinator's own
local read (when it was in the preference list) with sequence `0`
unconditionally, because it happened synchronously, before any remote
`ReplicateRead` was even launched — not because of any comparison
against real completion times. So whenever the coordinator held a
replica for the key, its own local value always won a disagreement,
regardless of whether some other replica's RPC would genuinely have
completed first. This fell directly out of "local first, no self-RPC" —
a decision framed entirely around avoiding a loopback RPC and giving the
local write credit toward `W`/`R` — not out of any decision about how
ties should resolve when replicas disagree.

**Update, Phase 3 checkpoint session:** sharpened, not resolved. Self's
`arrival_order=0` was a **structural guarantee**, not a probabilistic
tendency — assigned synchronously, before any remote `std::async` future
was even launched, so no remote reply could beat it even hypothetically,
regardless of real network speed.

**Resolution:** superseded wholesale, not patched — exactly as Phase 4's
design conversation anticipated (see `PROGRESS.md`'s "What Phase 4
replaces in existing code"). `phase4/get-read-path` removed
`arrival_order` entirely; `Get` now filters collected replica responses
to the found-only subset and runs `resolveGetResult`
(`computeFrontier` + `resolveLWW`) over that subset, so the winning
value is whichever one is actually causally current (or wins the B2/B3
LWW tiebreak on genuine concurrency), never whichever one happened to be
appended to the results vector first. The coordinator's own local copy
now wins only when it's actually the dominant or resolved-current
value — not structurally, by construction, regardless of what any other
replica holds.

**New, narrower non-determinism introduced by the fix, accepted rather
than chased further:** with `arrival_order` gone, the order entries land
in the results vector is now whichever order the fan-out threads happen
to acquire `results_mutex` — nondeterministic across runs. This only
matters for `resolveLWW`'s full-tie fallback (identical `last_updated`
*and* identical `writer_id`, first-seen-wins, see Q24) — already
accepted there as a vanishingly narrow residual case. "First-seen" now
means "first-to-grab-the-lock" rather than anything meaningful, which is
a smaller and more honestly-scoped problem than the one this question
originally raised, not a new one.

*Context:* raised while reviewing the `Get` coordinator handler, Phase 3
implementation session; sharpened during the Phase 3 checkpoint session;
resolved during `phase4/get-read-path` (branch 5/9) implementation.