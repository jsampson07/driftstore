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

## Membership & failure detection

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

## Consistent hashing / ring
_Fill in once Phase 2 is designed._

## Client API
_Fill in once Phase 3 is designed._

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