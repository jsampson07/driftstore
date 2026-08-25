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
scaffolding either way.

### Q3 — Does GTStore's `storage` / `test_app` need an explicit manager address?
`harness/smoke_test.sh` assumes `storage --port $PORT` and `test_app` locate
the manager via a hardcoded default (localhost + some fixed port), since no
manager-address flag was given for either. Unconfirmed against GTStore's
actual source.

*Status:* unresolved, low stakes — the harness passes under this assumption,
but worth confirming it isn't passing by accident (e.g. a stale process from
an earlier run happening to still be up).

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

*Status:* unresolved — explicitly deferred by request ("don't want to decide
now, don't want to lose momentum"). Revisit before Phase 2 design starts.

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

### Q8 — Should `SendGossip` surface RPC success/failure explicitly to its caller?
Currently `SendGossip` returns a plain `GossipResponse`, with the
`grpc::Status` checked (and logged as `GOSSIP_SUCCEEDED`/`GOSSIP_FAILED`)
only inside `SendGossip` itself, then discarded. `gossipRound` calls
`applyGossip(response.table())` unconditionally, with no way to know which
case it's in. This currently works out safely only because gRPC's contract
leaves the out-parameter untouched (default-constructed, empty `map`) on
failure, and a live responder's table can never actually be empty — but
`gossipRound` doesn't check for either of those things, it just happens to
be protected by both holding. Options: leave as-is (works, but the safety
is implicit and unverified by the caller); change `SendGossip`'s signature
to something like `std::optional<GossipResponse>` so `gossipRound` can skip
the `applyGossip` call outright on failure instead of relying on an
incidental no-op.

*Status:* unresolved, not blocking — current behavior is correct as far as
tested, but the correctness rests on an assumption the code doesn't
enforce. Raised while tracing the gossip round end-to-end from memory.

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
worth checking whether `client.cpp` sets any RPC deadline at all.

### Q5 — Does admin `remove` need to target a designated seed, same as bootstrap?
The ordered-fallback-with-backoff seed mechanism (250ms timeout, exponential
backoff, 3-try cap) was designed to solve "an actor with zero prior cluster
knowledge needs a first point of contact" — which is necessarily true for a
brand-new node joining (it only has its `--seed` launch flags to go on).
It's not obviously true for an admin issuing a `remove` command: the admin
already stood up the cluster and may already know the address of any live
node, not just a seed. ARCHITECTURE_1.md's draft answer says both join and
remove should require targeting a seed, but that may just be the join
answer copy-pasted without re-checking whether the restriction is necessary
for remove too. Raised during Phase 1 proto/schema work.

*Status:* unresolved, not blocking — doesn't affect the `GossipExchange`
proto/schema work already landed. Matters once the admin join/remove RPC
gets designed. Flagged as taste/consistency, not correctness.

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