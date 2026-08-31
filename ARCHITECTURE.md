# Driftstore — Architecture

> This file is yours to grow. Claude will help design individual mechanisms
> in conversation, but writing the explanation here — in your own words — is
> the actual point: it's the same "can you say it back to me" check applied
> to documentation instead of conversation. Every italicized line below is a
> prompt, not a placeholder to leave blank forever.

## Overview
_One or two sentences: what is this system, and what's it the inverted counterpart to?_

This system exists to build and understand the AP side of CAP theorem hands-on, as the deliberate inverted counterpart to GTStore. It directly addresses GTStore's architectural downsides: no dynamic membership, concurrent writes racing with no versioning to detect it, a centralized manager as a single point of failure, writes requiring synchronous acknowledgment from all K replicas, and a failure detector (a flat one-second heartbeat deadline) prone to false positives on a merely-slow node.

## Tech stack
- Language: C++17
- RPC: gRPC + Protocol Buffers
- Build: plain Make + `pkg-config` (no CMake)

## Repo layout
```
driftstore/
├── src/
├── bin/            (build output)
├── harness/
├── Makefile
├── .gitignore
```
_Update this tree as new files/directories get added._

## Process model
_Why is there one `node` binary instead of separate roles? What does a `node`
process actually do from the moment it starts, through steady state?_

## Node identity
_What is a node_id, where does it come from, and why was it chosen this way
over the alternatives that were considered?_

## RPC surface
_Table of every RPC as it gets added._

| RPC | Purpose | Added in phase |
|---|---|---|
| Ping | scaffolding smoke test | 0 |

## Logging
_Log line format, where logs are written, what event types exist, what
triggers each one._

| Timestamp | Node ID | Event |

## Phase 1: Membership & failure detection

**What fields actually live in a table entry?**
Every node has a membership table. In it is a mapping from <node_id, {information of the peer}> for every node in the system. For example:

| node_id | writer_id | address | status (up/removed) | last updated | tokens (placements in ring) |
| A | A | 10.0.0.1:5000 | up | t=12354245241 | [120, 500, 6000, 9400] |

`node_id` is who the entry is *about*. `writer_id` is which node processed/asserted
this change (the admin's join/remove target) — needed as the tiebreak below, since
two conflicting entries about the same `node_id` otherwise have nothing to compare.

Status is `up` / `removed` only — never `down`. "Down" describes transient
reachability, which is a separate, local, non-gossiped concern (Phase 3) precisely
so hinted handoff has something to hand a returning node back to. If unreachability
could ever flip an entry to `removed`-equivalent, that node's key ownership would
shift out from under it while it's just temporarily unreachable — see the
membership-vs-transient-unavailability distinction elsewhere in this project's plan.

Each entry in the table is the membership information that the node believes about the peer

During a gossip round, this entire table is serialized and sent over the wire to another node (Node A table --> Node B). Then Node B, walks through it row by row for every node_id and compares its information to its own table and applies the LWW (Last-Write Wins) rule to update its own entries.

**How do we actually merge the two tables together?**
- If there is a conflict, we take the one with the most "recent" timestamp (larger one, max)
- If timestamps are exactly equal, take the entry with the higher `writer_id` — doesn't
  recover true write order, but guarantees every node computes the same winner regardless
  of merge order, which is what actually makes the table converge
- If one table has information that another table doesn't, then take it always

**Open — not yet decided:** is a gossip round push-only (A sends its table to B, B merges,
done) or push-pull (B also sends its table back to A in the same exchange, A merges too)?
The example below only shows one direction. Push-pull is the standard technique and gets
info moving in both directions per round instead of relying on future rounds to reverse the
flow — but this is part of the merge design, worth deciding deliberately rather than
defaulting into whichever direction is easier to code first.

Gossip round is push-pull. Push-model means a nodes own freshness depends on how often it is selected as a peer
for gossip, and as the cluster grows, this is less and less likely. A pull-model means that no matter how fresh a nodes information is, no one can retrieve it unless they "pull" from it.

Push-pull model addresses both of these. Yes there is twice the amount of work needed for one gossip RPC, but
being a bidirectional interaction means, a node that is far more up to date than the requesting node, sends its freshness over to requesting node, so it can update itself too. This gossip for EVERY node is initiated EVERY `gossip_interval`, which means it is not dependent on other peers selecting it for gossip. Also, if interact with fresh node, will retrieve information, not just be one directional.

What if a node is "removed"? Does it get deleted from the table or does it simply just stay as a permanent tombstone entry marked as "removed" or "down"?
- Removed should not remove the table entry bc...

*Scenario:*
One node is removed, and all live nodes' membership tables are updated to account for this (after gossip), but then a previously marked "down" node comes back up and still has the stale "alive" status for the now dead node.

This dead node is now marked as alive and reentered into the membership tables of each node through each gossip and is now treated as a node that services requests and contains data

**How often does a node gossip?**
*More often*
Pros:
- Information spreads faster --> update tables quicker
Cons:
- More network chatter which can lead to network congestion
- CPU time spent serializing and merging tables

*Less often*
Pros:
- Less CPU and network overhead
Cons:
- Stale entries/differing views of membership information of cluster

In this system probably default to 1 second --> my system with handful of local processes and processing data but not at an extremely large scale

But allow CLI flag so that we can change this for test cases.

**What is a seed?**
A seed is a node that every node is configured to know about, specifically so every node has at least one node to reconcile against when initiated

Typically we have multiple seed nodes, why?

**Seed nodes: does `join`/`remove` admin command require targeting one of a designated seed set, or can it point at any live node?**

`join`/`remove` should require targeting one of a designated seed set. It should try the first seed in the set, and if the seed fails to respond, it should try the next seed, then the next, etc, until one responds. This can be implemented as a retry-with-backoff, where if all seeds do not respond, a second entire loop is attempted (on the whole seed-list), until something responds.

**How long do we retry for or indefinitely?**

Retry starting at 250ms, then perform exponential backoff, until reach some "max", then retry at that max (the whole list) 3 times, and if still fail, then do not allow node to bootstrap into the system and just kill it with indication.

**What is the tradeoff with this design?**

With this *ordered fallback*, if a script initializes the system, and each node targets the first seed in the seed "list", then this creates a thundering-herd scenario, where the first seed is bombarded with bootstrap attempts.

**What is the bootstrap connect-timeout value?**
250ms to account for the real RPC latency in the system. Need not be a rounded-human time metric i.e. ~3 seconds. This would particularly address deadlocked, CPU starved, or hung processes. This timeout value is addressing a completely different value (connection). Connection either succeeds or doesn't. If connection is unable to be established (RPC does not go through), then 250ms suffices to detect/provide adequate time for a "slow" network.

**What happens if a node is removed from the system? What happens to its entry in the membership table of each node?**

It is NOT removed. It is simply marked as "dead" or "down" and remains in the table. This means that the table grows indefinitely. This is a tradeoff worth accepting for a system of my intended scale (relatively small).

**What happens if empty-candidate-list during gossip loop?**

Initial proposed idea: backoff/cap/3-tries for the periodic scheduler's empty-candidate-list case (which runs indefinitely)
- This solution is fine for initial node startup, but not during actual life of a node that can still service other requests

What is wrong with this proposed idea for the general periodic scheduler?
- Say `UP` peers it knows happen to be empty (i.e. partition or every peer it knew about was `REMOVED`). Under my proposal:
--> it starts hammering the seed, and if seed is dead or partitioned, retry for a while then node KILLS ITSELF.

Problem: Node that was still capable or servicing requests (reads/writes) is now stuck attempting to initiate a gossip which won't happen until it kills itself. This creates a *self-inflicted* outage.

In an attempt for correctness, we now sacrifice AVAILABILITY, which is the priority for my system (AP).

**What can we do instead for empty-candidate-list case?**

?????????????????????

**Why can't we just set `table_` = `request.table()` after gossip response is received?**

Because we are not guaranteed that `table_` remains the same during the gossip request to the response. If it changes, say Node A iniates a gossip to Node B, and while Node B is merging, Node C gossips to Node A, updating Node A's table into a merged version, then Node B returns its response, Node B merged with an "older" Node A table. We MUST ALWAYS re-merge.

*Always merge, NEVER overwrite*.

## Phase 2: Consistent hashing / ring

Phase 2 treats all `UP` as reachable. Real filtering will be deferred to Phase 3.

**How are tokens determined/assigned?**

Tokens are deterministic via hash(node_id_ + ":" + i) for i in range [0, V]
  - NOTE: V is the number of tokens for a node
With this, no persistence is needed, because if a node dies and gets rebooted, its tokens (ring positions) would be the exact same

On the contrary, if we have tokens randomly assigned, then if a node were to reboot, it would be assigned completely different tokens, meaning it now owns a completely different subset of keys than originally. To prevent this, we would need some sort of log or disk-persistence layer so when a node is rebooted, it can just grab its recorded positions from there. (PERSISTENCE LAYER NEEDED)

**What hash function will we use?**

FNV-1a, 64-bit
There is no stdlib dependency, will produce the same hashes for same input across various runs.

**What data structure will support our ring? What are the different options?**

Our ring will be a std::map<uint64_t, node_id>: key=token (pos on ring), value=primary node owner.

Will behave like table_ where there is one instance across the lifepsan of a node. It is continuously added to as membership/gossiping facilitates OR removed from (ONLY when a node is marked as REMOVED).

The other option would be to derive the ring on demand. Everytime a key is queried, the rin would be derived based off of the current instance of table_ and its recorded tokens for each of the nodes. Then sort and construct 'preference list'.

**Where is the ring updated?**

- Three conditions inside of mergeInto:
  1) insert on new-and-UP
  2) insert on REMOVE->UP
  3) remove on UP->REMOVE
- When a node is removed (RemoveNode) --> for the node receives the `REMOVE` request
- When a node is initialized (constructor)
  ==> THIS IS IMPORTANT B/C: if node doesn't add itself to the ring, then its own node entry lives in the table, and when other nodes gossip with the node, new entries ONLY are added to the ring, but it already has its own node_id as an entry, so it treats it as "already added to the ring". Any new entries have their tokens added to the ring. The node's own tokens are NEVER added no matter how much gossiping is done.

**Preference-list walk**

1) call upper_bound(hash(key))
  - finds first entry larger than hash(key)
2) iterate forward (index 0 -> len - 1)
3) dedupe by physical node (using unordered_set to see if physical is unique or NOT)
4) iterate until N unique physical nodes OR after one full pass (meaning not enough unique N nodes)
  - if fewer than N distinct nodes --> return early for now

**Not yet written here — worth being able to say without notes before Phase 3:**
*Why can two different nodes, coordinating the same key at nearly the same moment, legitimately compute two different preference lists? What has to be true about membership for that to happen, and why is it not a bug?*

Two different nodes, coordinating the same key at nearly the same moment, can compute two different preference lists if there is a membership change that
hasn't yet been gossiped to both coordinators, rather only to one of the two. This leads to one of the node's ring reflecting the changes, where the other
doesn't reflect any change.

This is NOT a bug because they both stay true to their ring's state. There is NO single authoritative ring in the system, each node's ring is relative to its own local
information, with gossips for updates.

*Separately: is the preference list itself different for a read versus a write on the same key — and if not, what actually is different between how a read and a write use it?*

The preference list itself is NOT different for a read versus a write on the same key because there is no branch when computing *preference_list* for a read versus write. The same
deterministic function is applied for both.

A read uses the *preference_list* by reading from R of the N nodes, where each can return different versions of the same key. Then it has to decide which version
to hand back to the client. A write uses the *preference_list* by having successful writes to W of the N nodes. If < W writes succeed, then the write is considered failed.

## Client API

**How to determine a node's reachability?**
1) dedicated probe (like GTStore Ping) - O(N)
2) apart of gossipRound() - nodes picked randomly, as cluster size grows, degrades to update every N*RPC seconds
3) feed from both gossip outcomes and Get/Put RPCs - busy peers get fresher readings on "reachability" than idle ones
  --> this is an acceptable tradeoff, since "hot" nodes or nodes that are most likely routed to are the ones that need the most organic signal

  **NEW ADDITION:** Use Ping RPC that already exists to ONLY ping "unreachable" nodes so that it is doing useful work. This is better
  than pinging "reachable + unreachable" nodes as this leads to wasted RPCs and unnecessary network congestion for reachable nodes.
  In a healthy system, the "unreachable" nodes set should be empty.

**What are the tradeoffs with *dedicated probe*?**

Dedicated probe means we have O(N) probing, which means it grows as cluster size grows. The larger the cluster, the slower the probing becomes.

**What are the tradeoffs with *gossipRound()* only?**

Gossip protocol randomly selects a node to gossip with, which could mean a node doesn't get gossiped at all for some time, OR, in the best case, a node gets
gossiped with 1 out of N times. This means each entry will most likely be stale and untrue to the current state of the node. Also declines as cluster size
grows.

**What are the tradeoffs with *gossip* and *Get/Put RPC updates*? (THIS IS THE DESIGN I AM CHOOSING)**

More bookkeeping and places to manage reachability, but gets recency (of reachability - through two routes) as well as minimal RPC/network requests.

**Implement any form of timestamps of versioning or no, and why?**

No form of versioning, this is for later phase, For now just take the first node (in the order of preference_list) that responded with a value.

**Asynchronous or synchronous writes to replicas?**

Asynchronous writes is bounded by slowest node, whereas synchronous writes latency becomes sum of their response times (sum(RTs)).
Also want to write to >= W nodes, with room for a few nodes failing early. In GTStore, b/c consistency > availability, after one
node fails, write is considered failed and should behave in such a way so synchronous is slightly more sensible, because asynchronous
on a failed write, would still send off N-1 other RPCs.

**What data structure used to maintain reachability locally (for each node) and what locking mechanism might we use?**

std::unordered_map<std::string, bool> = <node_id, true/false> where T/F if reachable or not
Use separate lock for reachability because this is unrelated to membership. This is a completely different
concept that is has no "convergent" value, just dependent on each individual node.

**New stateful library:**

connect(seed_noes) with seed fallback which keeps the client alive

**What does reachability actually store per peer/node?**

Reachability stores a boolean for each peer (true = reachable, false = unreachable).

**What triggers this boolean to flip?**

Few options:
1) LastN-like behavior (symmetric, N consecutive failures)

For a node to be marked "unreachable", there need to be N consecutive failures. But if a node is stuck or backed up, and can't service requests for the next
say M requests, and M > N, then there are going to be a wasted N-1 calls to the "unreachable" node before it is officially marked as unreachable.
This leads to wasted RPC calls.

2) LastN-like behavior (asymmetric thresholds, trip on 1 failure, require K successes to recover)

Recover latency suffers. For a node that was slow just one time, it requires K successes before being flipped back to "reachable".

3) Most recent outcome

The moment a node fails to respond to a request, marked as "unreachable". Then, the moment its next contact suceeds, marked as "reachable".
Stays true to current known state of the node. Assisted by quorum-based reads and writes because the success of a read/write is not affected by a single node failing.
There is room for node failures/slow networks during a request that deems a request as "failed" because it only looks for W/R successes.
Marking "unreachable" immediately does NOT affect the correctness of the system and we maintain data availability by using quorums. If we had a single-fail behaving system,
then perhaps we would want N consecutive failures (and a retry on that before marking a node as "unreachable").

**How to run loop for unreachable nodes?**

1) Polling:
Background thread --> runs reachability check "unreachable" nodes every "polling-interval" (provided as command-line field)
Means needs to snapshot "unreachable" set
  - But this is very cheap operation, considering in a healthy system, this set will be small if not empty
Check if empty or not
  - if empty: sleep (no work, no CPU resources, no spinning)
  - else: Ping each unreachable node

2) Condition-variable-gated
Prevents "reachability" loop from running when set is known to be empty.
Signal when a new node is marked as "unreachable" which wakes up the sleeping thread to execute until "unreachable" set is empty again.
HOWEVER:
Introduces a new bug class that isn't necessary, why?
The work done by the polling implementation, is basically doing an O(1) check (when unreachable set is empty) and the thread still sleeps in between polls.
If misshandle a spurious wakeup or forget to call `notify_one()`, then bug introduced.

**Introducing reachability introduces new path for `preference_list.size() < N`. What is this new path, how does this happen, and what to do about it?**

New path: if there are > N nodes but enough are 'unreachable' so < N nodes are added to pref list.

Before path: the ring did not have N distinct UP physical nodes in cluster (no concept of reachability).

Treat both situations the same: `preference_list.size() < N`. Whether its because there are less than N physical nodes
versus less than N reachable nodes should lead to the same outcome. Failed write/read.

**How should N/W/R be configured across the system?**

PROBLEM I was thinking: if N/W/R are different across different nodes, then this leads to wildly inconsistent copies of data and retrievals.

## Vector clocks & conflict resolution
_Fill in once Phase 4 is designed._

## Hinted handoff
_Fill in once Phase 5 is designed._

## Read-repair
_Fill in once Phase 6 is designed._

## Dashboard
_Fill in once Phase 7 is built._

## Fault-injection harness
_Fill in once Phase 8 is built._