# Driftstore — Project Plan

*A leaderless, gossip-coordinated distributed key-value store (the AP counterpart to GTStore)*

> **Revision note (this version):** Phase 1 was redesigned from SWIM-style failure detection to Dynamo's own gossip + local failure detection model (paper §4.7–4.8), per an explicit decision: gossip's job is membership convergence, not failure-accuracy — availability is protected by sloppy quorum + hinted handoff routing around a locally-unreachable node, not by getting failure detection itself right. Every section below that referenced SWIM has been updated accordingly. Ring changes (join/leave/remove) are triggered by an **explicit CLI/admin command** for now, with automatic "presumed dead after N minutes unreachable" promotion moved to stretch goals (Section 9).

---

## 0. Reality check on timeline (read this first)

You have ~30 hrs/week (±5) for 3–4 weeks, roughly **105–150 hours**, before this needs to be resume-worthy. That's the honest cost of the two hardest phases (vector clocks/hinted handoff/read-repair, and gossip membership convergence) done for real rather than stubbed.

Practical implication, revised: under SWIM, Phase 1's suspicion state machine (direct + indirect probing, incarnation refutation) was inherently distributed and timing-dependent — the kind of thing that tends to run long regardless of how it's scoped on paper. Dropping SWIM removes that specific state machine — Dynamo-style gossip is genuinely less code: periodic membership-table exchange + merge, and *local* reachability tracking with no suspicion tier and no refutation logic. That doesn't mean Phase 1 becomes trivial or risk-free — membership-table merge/convergence logic across N nodes is still a real distributed bug surface, you still can't `gdb` your way to "why hasn't node C seen node E's removal yet," and you'll still lean on correlated logs. Phase 4 (vector clocks/concurrent-write detection) is worth watching too — it's explicitly called out below as the project's first genuinely non-deterministic, timing-dependent test, and that kind of test is inherently harder to size up front than a straightforward build.

If time gets tight, the thing to protect is **correctness of the core mechanisms (Phases 0–6)** — gossip, consistent hashing, vector clocks, hinted handoff, read-repair. The dashboard (Phase 7) and the *full* fault-injection harness (Phase 8, beyond a minimal kill/partition script) are the first things that should slip past the 3–4 week mark.

---

## 1. Project goal and summary

**For a recruiter/interviewer, in your own voice:**

I built GTStore first — a sharded, replicated key-value store with a centralized manager, static modulo hashing, and a client that wrote directly to all K replicas. It worked, but building it left me with a specific, nagging question: production systems like Cassandra and DynamoDB explicitly reject almost every one of GTStore's design choices — no manager, no fixed node count, no client-side fan-out, no "block until every replica agrees." I understood *why* they do this in the CAP-theorem-textbook sense, but I hadn't actually built the AP side of that tradeoff, so I couldn't tell you concretely what it costs. Driftstore is that: the same key-value storage problem, with every one of GTStore's core decisions deliberately inverted — gossip instead of centralized heartbeats, consistent hashing instead of modulo, vector clocks and hinted handoff instead of write-all blocking. The goal isn't "build Cassandra," it's to walk away able to point at a specific mechanism and say exactly what it buys you and what it costs, with a system I built twice, two different ways, to back it up.

**Suggested project name:** **Driftstore** — the name is literal, not cute: vector clocks *drift* apart under concurrent writes, and read-repair/anti-entropy exist to pull them back together. (Alternates if you want options: *Ringmesh*, *Gossipring* — I'd go with Driftstore, it's the one that actually encodes a mechanism rather than just a topology.)

**Resume-line title:** `Leaderless, Gossip-Coordinated Key-Value Store (Dynamo-style)` — this is now accurate end-to-end, not just for the storage-layer mechanisms: gossip membership *and* failure detection follow Dynamo's actual design, not a SWIM hybrid.

---

## 2. Tech stack, with the two forks reasoned through

**Language:** C++ (carried over from GTStore — deliberate, per your framing: this project is about the concept gap, not a language ramp-up).

### Fork (a): Gossip transport — does the UDP-vs-gRPC question still matter post-SWIM?

- **Original reasoning (no longer applies as stated):** the prior recommendation for raw UDP was justified entirely by SWIM's design — indirect probing and incarnation-based refutation exist specifically to disambiguate "the target is down" from "my probe got dropped" over an unreliable transport. That's the part of SWIM that makes UDP *necessary* rather than incidental.
- **Dynamo's actual model removes that necessity.** Membership gossip is periodic exchange-and-merge of a membership table between random peers — no probabilistic reasoning about packet loss required. Local failure detection (§4.7) is derived from each node's own recent request outcomes to a peer, which can piggyback on whatever transport the data path already uses. Neither piece is *built around* unreliable delivery the way SWIM's suspicion mechanism is.
- **Decision (revised): reuse gRPC for gossip messages too.** Membership exchange becomes another typed RPC (e.g., `ExchangeMembership(local_table) -> merged_table`), alongside the existing KV data-path calls. This removes the "cost you're taking on" bullet from the original plan entirely — no custom message framing, no manual sequence-number rejection of stale/out-of-order gossip messages, since a single gRPC call gives you ordered delivery for free.
- **What you lose by dropping UDP:** direct practice with unreliable-transport protocol design (idempotent retries, out-of-order handling, application-level sequencing) — that was arguably valuable independent of SWIM specifically. If you want that experience for its own sake, UDP is still a legitimate, harder choice; it's just no longer *required* by the design the way it was under SWIM.
- **This is a taste/practice call, not a correctness one** — confirm before Phase 1 starts, since it determines whether Phase 1's socket-level work is "write a gRPC service method" or "hand-roll UDP framing and retry semantics."

### Fork (b): Sloppy quorum with tunable R/W vs. fixed N-of-N + hinted-handoff-only

*(Unaffected by the SWIM→Dynamo change — reasoning below is unchanged from the original plan.)*

- **Fixed N-of-N (the simpler alternative):** Write succeeds only when all N replicas ack — this is GTStore's write-all model again, just distributed differently. You could bolt hinted handoff on as an exception path *only* for the down-replica case. Simpler to reason about, but it reproduces GTStore's core availability problem and gives read-repair nothing to do, since R=N reads never see partial results.
- **Sloppy quorum, tunable N/R/W (recommended):** Coordinator requests W-of-N write acks and R-of-N read acks, not all N. This is what makes hinted handoff a *first-class* mechanism instead of an edge case, and what makes read-repair meaningful: a per-request, no-leader quorum computed by whichever coordinator happened to receive the request, explicitly tolerant of temporarily divergent histories that get reconciled later rather than prevented up front.
- **Decision:** tunable N/R/W, config-driven, default **N=3, W=2, R=2** (R+W=4 > N=3, guaranteeing read/write quorum overlap). Enforce R+W>N in config validation, not just documentation.

### Dashboard

A decoupled Python/FastAPI service that polls each node's status endpoint. For this system, each node's status endpoint should expose:

- **Ring position:** which virtual-node tokens this physical node owns.
- **Known peers + reachability (revised):** this node's own *local* view of each peer as reachable/unreachable — there's no suspected tier and no incarnation number, because reachability is a private, per-observer signal derived from recent request outcomes, not a cluster-agreed state anyone refutes. Alongside this, show the last-merged **membership table** (full ring membership, from gossip) — it's fine, and expected, for this to lag local reachability by a round or two; they converge on genuinely different timescales and for different reasons.
- **Vector clocks for keys held locally** (a snapshot, not a full dump if the keyspace is large).
- **Pending hints:** which keys, held on behalf of which down node, how long they've been held.
- **Current N/R/W config** and the quorum outcome of the most recent op.

This is your primary debugging tool for Phases 5–6.

---

## 3. Core concepts — brief definitions

- **Gossip membership + local failure detection (Dynamo-style, revised):** periodic random-peer exchange of a membership table (node id, ring tokens, administrative status), merged on receipt so the cluster eventually converges on one ring view. Failure detection is a *separate, local* concern (§4.7): a node infers a peer is unreachable from its own recent request outcomes to that peer, uses this only to route around it (sloppy quorum / hinted handoff), and never gossips or globally agrees on it. No indirect probing, no suspicion tiers, no incarnation numbers — the tradeoff is that different nodes can legitimately hold different, simultaneously-correct views of who's currently responsive, and that's a designed property, not a bug to reconcile.
- **Consistent hashing + virtual nodes:** hash keys and nodes onto a fixed ring (e.g., 64-bit space); a node owns the range up to it going clockwise. Virtual nodes = each physical node owns many scattered points on the ring, so (1) load balances evenly even with few physical nodes, and (2) a join/leave only redistributes that node's virtual-node ranges, not the whole ring.
- **Vector clocks & causality:** per-key `{node: counter}` map. Version A **dominates** B if every one of A's counters ≥ B's corresponding counter (safe to overwrite). Neither dominates → **concurrent** — a genuine conflict needing a resolution policy, not a bug.
- **Sloppy quorum:** an **availability/latency tuning knob**, not a safety mechanism — W acks from whichever members of a preference list respond, computed per-request by whichever node happened to receive it, explicitly tolerant of temporarily divergent histories that get reconciled later, not prevented.
- **Hinted handoff:** if a preference-list node is unreachable (from the coordinator's local view), another node holds the write and delivers it once gossip/local checks show the target reachable again. Risk: if the hint-holder also crashes before delivery, the hint is lost — a soft mechanism, not a durability guarantee, which is why read-repair still matters as a backstop.
- **Ring membership changes vs. transient unavailability (new, explicit distinction):** these are two different mechanisms, not one. A node being unreachable does **not** change the ring — its key ownership is untouched, and sloppy quorum + hinted handoff cover the gap. The ring only changes on an **explicit administrative action** (CLI add/remove command in core scope; see Section 9 for automatic promotion as a stretch goal), which then gets gossiped and triggers actual key redistribution. Conflating these two was an early misstep worth remembering: if unreachability auto-triggered a ring change, hinted handoff would have nothing to hand back to when the node returns, because the node wouldn't own those keys anymore.
- **Read-repair vs. anti-entropy:** read-repair is reconciliation *as a side effect of a client read*. Anti-entropy is background reconciliation that doesn't wait for a read (Merkle-tree-based, see stretch goals).
- **LWW vs. sibling versions:** Last-write-wins keeps the highest timestamp — simple, but silently discards real concurrent writes and is sensitive to clock skew. Sibling versions keep both conflicting versions for the client to merge (Dynamo's shopping-cart approach) — no data loss, but pushes reconciliation to every client.

---

## 4. Phased roadmap

Each phase includes: a concrete production failure scenario, the debugging technique that phase forces on you, and the direct GTStore cost/benefit comparison.

### Phase 0 — Scaffolding + comparison harness skeleton (Days 1–2)
**Build:** repo/build setup (new repo, reusing GTStore's Makefile as a template — not CMake, see addendum below), and a bare-bones comparison harness that can launch both GTStore's binary and Driftstore's binary and issue one op against each — even trivially.
**Debugging:** decide your structured log schema now (timestamp, node id, event type, relevant clock/round) — every later phase depends on this being consistent from the start.
**GTStore analog:** n/a — the harness *is* the comparison tool.

**Phase 0 execution notes (addendum):**
- **Build tooling: plain Make, not CMake.** GTStore's actual build is a Makefile + `pkg-config` (no `CMakeLists.txt` exists anywhere in that project) — "reuse GTStore's CMake" above is leftover imprecise phrasing from an earlier draft of this plan, not a real CMake setup to port. Start a new, separate repo for Driftstore and copy over the Makefile pattern (targets, `pkg-config` flags for protobuf/grpc, the proto-generation rule) as a template. No code is shared between the two repos.
- **One binary, not a manager/storage split.** GTStore's Makefile builds two distinct roles (`manager`, `storage`) because GTStore has a centralized manager. Driftstore doesn't — "any node can coordinate" means every process is the same symmetric peer, so Phase 0 only needs a single `node` build target, plus a client stub for issuing calls to it.
- **Harness scope for Phase 0 is a minimal smoke test, not the real comparison harness.** A short script that launches GTStore's manager+storage+one `test_app` call, launches Driftstore's `node` binary, hits it with one trivial call (a stub `Ping`/`Echo` RPC is enough — there's no real KV logic yet), prints pass/fail for each, and exits. No log-correlation, no comparison metrics, no retries — that machinery belongs to Phase 8/9, once there's something substantive to compare.

### Phase 1 — Gossip membership + Dynamo-style local failure detection (Days 3–6)
**Build:** periodic gossip exchange of the membership/ring table over gRPC — each node picks a random peer, exchanges its table (node id, tokens, admin status, a version/timestamp for merge), and merges on receipt so the cluster eventually converges. Separately: local per-peer reachability tracking, derived from recent request outcomes on the data path (consider whether you also want a lightweight periodic health-check RPC for peers you're not otherwise talking to — the paper is intentionally light on this specific mechanism, so this is your design call, not a fixed spec to match). A reachability flag is private to the observing node, used only to trigger hinted-handoff routing, and clears as soon as a request to that peer succeeds again — no cluster-wide agreement, no timeout tiers. A small CLI/admin command explicitly adds or removes a node from the ring; that's the only thing that changes ring membership, and it's what gets gossiped.
**Production scenario:** a node under a load spike causes several consecutive requests to it to fail/timeout — verify the coordinator correctly falls back to hinted handoff for those requests, and that the node is *never* removed from the ring by this alone; once it recovers, requests succeed again with no special recovery step. Separately: an explicit admin "remove node" command must propagate via gossip and be reflected in every node's preference-list computation within a bounded number of rounds.
**Debugging:** merge structured logs from all N processes to watch membership-table convergence after an explicit add/remove. There's no global suspicion state to reconstruct anymore — the debugging question shifts from "did nodes agree on who's suspected" to "did every node's ring view converge, and separately, is each node's local reachability tracking staying local rather than leaking into shared state by mistake."
**GTStore analog:** GTStore's manager was a single authority for both membership *and* reachability — one place, one truth, no possibility of disagreement, but a SPOF and scaling ceiling. Driftstore splits these into two mechanisms on two different timescales: membership is gossiped and eventually consistent (every node converges on the same ring, not instantly); reachability is local and *never* converges by design — each node's view of "who's responsive right now" can differ from every other node's, correctly, simultaneously. GTStore never had a "my view of who's up differs from yours" case at all; here it's not a bug, it's the mechanism that buys availability.

### Phase 2 — Consistent hashing ring + virtual nodes (Days 7–8)
**Build:** hash ring (64-bit space, MurmurHash3 or similar); V virtual-node tokens per physical node (start ~32–64, tune later); key→node lookup; preference-list computation (N distinct physical nodes walking clockwise, skipping nodes the *coordinator's own local view* currently considers unreachable — this is where Phase 1's reachability tracking actually gets used); ring update on explicit join/leave/remove.
**Vnode distribution experiment:** standalone script reusing the ring's hash function. Fix N=5 physical nodes, sweep V (1, 4, 16, 64, 256), hash 10k+ synthetic keys per V, compute per-node variance. Turns "why virtual nodes?" into a measured number.
**Production scenario:** a new node joins via the admin command — verify only virtual-node ranges adjacent to its tokens move.
**Debugging:** a bug here usually means two nodes computed *different* preference lists from the *same* membership snapshot — feed one snapshot to two nodes' ring logic and diff the output.
**GTStore analog:** GTStore's modulo hashing meant every key's owner changed whenever N changed. Consistent hashing bounds this to roughly 1/N of the keyspace per join/leave/remove.

### Phase 3 — Any-node coordinator + basic quorum read/write (Days 9–11)
**Build:** new client library (`connect(seed_nodes)` + `put(key, val)` / `get(key)` sent to *any* live node); coordinator logic (receive request → compute preference list via Phase 2 ring + Phase 1 reachability → fan out gRPC to N replicas → wait for W/R → reply).
**Production scenario:** client writes while one preference-list replica is unreachable — with W<N, the write still succeeds.
**Debugging:** correlate the coordinator's "sent to X,Y,Z, acked by X,Y" log line with each replica's own "received put for K" line. This is the debugging shape for the rest of the project.
**GTStore analog:** GTStore's client wrote directly to all K replicas and required all K acks. Here the client knows nothing about replicas; a write can succeed with fewer than N acks — name that tradeoff explicitly when you build it.

### Phase 4 — Vector clocks + conflict detection (Days 12–13)
**Build:** per-key vector clock; comparison logic (dominates / dominated / concurrent). **Decision point:** commit to LWW vs. sibling versions here — it changes the client API shape.
**Production scenario:** two clients write the same key through two *different* coordinators before gossip/replication catches up — genuinely causally concurrent, must be detected, not silently overwritten.
**Debugging:** log the full vector clock on every read/write. This is the project's first genuinely non-deterministic, timing-dependent test — lean on a scripted scenario.
**GTStore analog:** GTStore's write-all-K with synchronous fan-out means either all replicas agree or the write failed outright — no partial-success case with two valid divergent values. Vector clocks exist specifically because leaderless writes create exactly that case.

### Phase 5 — Hinted handoff (Days 14–15)
**Build:** when fan-out to a preference-list node fails (per the coordinator's local reachability view), hand the write to the next live node outside the natural list, tagged "belongs to X, deliver when X is reachable again."
**Production scenario:** an active key's replica is partitioned away mid-traffic, writes keep succeeding via a hint-holder, partition heals, hint delivers.
**Debugging:** log the full hint lifecycle (created → held → delivered/expired) per key per target.
**GTStore analog:** direct answer to GTStore's unresolved partial-write problem — GTStore would have blocked or errored on the same replica-down write Driftstore accepts via a hint.

### Phase 6 — Read-repair (Days 16–17)
**Build:** on a quorum read, compare vector clocks across the R replicas that responded; resolve divergence per your chosen policy; push the reconciled version back to stale replicas.
**Production scenario:** after Phase 5's partition heals, a replica that wasn't the hint target may still be a version behind — a subsequent read should catch and repair it.
**Debugging:** the dashboard's per-key vector-clock view is your primary tool now.
**GTStore analog:** nothing analogous exists in GTStore — write-all either fully succeeded or failed, no "replicas quietly disagree, then get repaired" state.

### Phase 7 — Dashboard (Day 18, can overlap with 6/8)
**Build:** FastAPI service polling each node's status endpoint (Section 2 spec).
**GTStore analog:** n/a — the dashboard is observability tooling, not a storage or coordination mechanism, so there's no GTStore design choice to contrast it against.

### Phase 8 — Fault-injection harness, minimum viable (Days 19–20)
**Build (minimum):** process kill/restart script; application-level partition simulation; finalize the structured/correlated log format.
**Production scenario:** a network partition isolating one or more nodes.
**Debugging:** this harness becomes the retroactive debugging tool for every earlier phase — worth re-running Phase 1's load-spike scenario and Phase 3's replica-down write through it.
**GTStore analog:** extending this harness *to* GTStore later is what makes the side-by-side comparison quantitative.

### Phase 9 — Convergence testing + full comparison run + polish (Days 21–22)
**Build:** convergence test — poll all replicas of a key until vector clocks agree or a timeout, record the bound; run the Phase 0 comparison harness end-to-end; write resume bullets and interview answers.
**GTStore analog:** n/a — synthesis phase.

---

## 5. Using Claude and Cursor without losing the understanding

**Fine to delegate to Cursor:** gRPC/protobuf boilerplate and codegen, CMake setup, dashboard/FastAPI glue, explaining a compiler or linker error, the kill/restart scripting once you've decided the schema, serialization glue.

**Design and reason through yourself before Cursor touches it:**
- **Vector clock comparison logic** (dominates/dominated/concurrent).
- **Conflict resolution policy** (LWW vs. siblings) and its implementation.
- **Consistent hashing ring math** (token placement, preference-list walk, wraparound, remap-on-join/leave).
- **Gossip membership merge/convergence logic, and local reachability state tracking (revised)** — this is the core of the "I implemented Dynamo-style decentralized membership and failure detection" resume claim. Write the merge logic and the reachability state transitions yourself, even if Cursor helps with the gRPC service scaffolding around them.

**How to prompt Cursor for the correctness-critical parts:** give it *your* pseudocode or state diagram and ask it to translate to idiomatic C++, not "implement Dynamo's gossip protocol for me"; ask it to critique a comparison function you already wrote rather than write one from scratch; scope prompts narrowly ("only implement the gRPC service wrapper, do not touch the membership merge logic").

**Self-check questions, per phase:**
- Could I redraw this phase's state machine or data flow from memory on a whiteboard right now?
- If I deleted Cursor's output for this file and had 20 minutes, could I reconstruct the core logic myself?
- Do I understand why this phase's test scenario would fail *without* this mechanism specifically?

---

## 6. What the finished product looks like, and how to demo it

**Live demo:** several clients writing/reading keys against the cluster via the dashboard-visible ring. A node holding replicas for an actively-written key gets partitioned away. The client keeps writing successfully — hinted handoff visibly holding writes on the dashboard's "pending hints" panel. The partition heals; the dashboard shows the hint deliver, and a subsequent read triggers read-repair.

**Side-by-side demo:** using the Phase 0 comparison harness, run the same logical operations against GTStore's client and Driftstore's client with the same failure timing. GTStore blocks or fails the write during the partition; Driftstore keeps serving.

---

## 7. Testing and debugging workflow

Structured/correlated logging first, `gdb`/`lldb` as fallback. This system's harness runs a scenario, then **polls repeatedly across time** until convergence is observed or a timeout fires — the interesting output is a *duration*, not a pass/fail on the first check. A correct eventually-consistent system *will* show temporary divergence; asserting "all replicas agree right now" immediately after a write asserts something the system never promised.

---

## 8. Draft resume bullets (adjust once you have real numbers)

- Designed and implemented Driftstore, a leaderless, gossip-coordinated distributed key-value store in C++, using Dynamo-style decentralized gossip for membership propagation and local, request-driven failure detection (no central heartbeat authority, no global agreement on node health) across N nodes with no central coordinator.
- Implemented consistent hashing with virtual nodes for data placement, bounding key remapping to ~1/N of the keyspace on node join/leave, replacing static modulo hashing from a prior project.
- Built per-key vector-clock versioning and causal conflict detection (dominates/concurrent), with [LWW / sibling-version — pick once you've decided] conflict resolution, hinted handoff, and read-repair, maintaining write availability and bounded convergence during injected node failures and network partitions.
- Built a fault-injection and comparison harness measuring convergence time after simulated partition-heal across N replicas, and demonstrating the availability tradeoff head-to-head against a prior strongly-consistent key-value store (GTStore) built with the same core problem and opposite design decisions.

*These are drafts — don't let placeholder claims become final resume text without real measured numbers.*

---

## 9. Stretch goals (explicitly not core scope)

- Automatic "presumed permanently dead after N minutes unreachable" promotion — escalating sustained local unreachability into an actual (gossiped) ring removal, replacing the explicit-admin-command-only trigger used in core scope. This is where the project would grow its own version of failure-detection-driven membership change, deliberately deferred so core scope keeps the two mechanisms (transient unavailability vs. permanent membership change) unambiguously separate first.
- Merkle-tree-based anti-entropy, replacing naive full-key-range comparison for background reconciliation.
- Pluggable/tunable conflict resolution beyond LWW (configurable per-namespace policy).
- Applying the fault-injection harness back to GTStore for a documented, quantitative comparison.

---

## 10. Likely interview questions, mapped to what they probe

1. *"A node under load gets a burst of failed/timed-out requests from its peers but hasn't crashed — walk me through what happens to it."* → Phase 1: local reachability tracking is per-observer and never auto-promotes to a ring change; explain why that split exists and what would break if it didn't.
2. *"Why virtual nodes instead of one ring position per physical node?"* → Phase 2: load balancing + bounded remap cost.
3. *"Two clients write the same key concurrently through two different coordinators — what does your system do?"* → Phase 4: vector clocks, concurrent detection, resolution tradeoffs.
4. *"Why compute quorum per-request from whichever preference-list nodes respond, instead of requiring a fixed, agreed-upon set of replicas to acknowledge?"* → Section 3: sloppy quorum as an availability/latency tuning knob rather than a safety mechanism — the cost is that a write can succeed against a different subset of replicas each time.
5. *"What happens if the node holding a hint also crashes before delivering it?"* → Phase 5's risk, and why read-repair still matters as a backstop.
6. *"How do you know your system actually converges, and within what bound?"* → Section 7's convergence-testing methodology.
7. *"Compare this specific mechanism to how GTStore handled the same failure."* → the GTStore-analog column threaded through every phase in Section 4.
8. *"Why did you drop SWIM in favor of Dynamo's own gossip and failure-detection model — what did you give up?"* → Section 2, Fork (a): SWIM buys detection *accuracy* (indirect probing, refutation) at the cost of protocol complexity; Dynamo bets availability doesn't need accurate detection, just a cheap way to route around a wrong guess (sloppy quorum + hinted handoff). Be ready to say which bet you'd defend and why.
9. *"Give me a concrete scenario where last-write-wins silently loses data — why might you pick it anyway?"* → Section 3's LWW/sibling tradeoff.
10. *"What would you need to add to get closer to real Cassandra/Dynamo?"* → Section 9 stretch goals.

---

## Open decisions

**Conflict resolution policy — LWW vs. sibling versions.** Deferred for now, but can't stay deferred past Phase 4 — it changes what the client API returns (does `get()` ever return a list?).

**Gossip transport — gRPC (new default) vs. raw UDP.** Set to gRPC in Section 2, Fork (a) as the direct consequence of dropping SWIM. Revisit before Phase 1 starts if you want the raw-transport practice for its own sake.

**Virtual node count:** resolved — measuring load-distribution variance as V increases (10k+ synthetic keys, N=5 physical nodes, sweeping V across a few values). Folded into Phase 2.
