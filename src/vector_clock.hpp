#pragma once
#include "driftstore.pb.h"
#include "logging.hpp"

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <algorithm>

// ---------------------------------------------------------------------
// Phase 4 scaffolding. Every body below is a stub — TODO markers, not
// implementations. Fill these in yourself before Cursor touches anything
// downstream of them (Put/Get/ReplicateWrite/ReplicateRead handlers).
// Each function's comment cross-references the PROGRESS.md decision
// (A1/B1/B2/B3/C1/D1) that defines its contract — re-read that section
// before implementing, not just this file.
// ---------------------------------------------------------------------

// A single (value, vector_clock) pair — the unit stored per key in
// kv_store_ under decision D1, and the unit exchanged wherever a resolved
// version needs to travel (ReplicateWrite payload, a candidate collected
// during Get's fan-out, etc).
struct VersionedValue {
    std::string value;
    driftstore::VectorClock clock;
};

// Result of comparing two vector clocks. Deliberately four-way, not
// three-way — EQUAL is its own case, not folded into CONCURRENT, per
// decision B1: identical clocks should only arise from the same write
// echoed twice (a retry), and that's only actually safe if the atomic
// critical section described below is implemented correctly. Don't treat
// EQUAL as a "should never happen" case you skip handling — implement it
// honestly.
enum class ClockComparison {
    EQUAL,
    DOMINATES,   // a is a causal descendant of b — safe to prefer a, discard b
    DOMINATED,   // the reverse — safe to prefer b, discard a
    CONCURRENT   // neither dominates — genuine conflict, falls through to resolveLWW
};

// Compares two vector clocks purely on their {node_id: counter} maps.
// A node_id present in one clock and absent in the other is treated as
// counter 0 on the side where it's absent — do not treat "missing" as
// "incomparable."
//
// MUST NOT consult last_updated. Timestamp comparison belongs entirely to
// resolveLWW below, invoked only when this function returns CONCURRENT —
// mixing the two here is exactly the "vector clocks computed then thrown
// away by a blanket timestamp rule" shape decision LWW-vs-siblings
// explicitly rejected.
inline ClockComparison compareVectorClocks(const driftstore::VectorClock& a,
                                            const driftstore::VectorClock& b) {
    // TODO (correctness-critical — design this yourself):
    // - union of node_ids across both clocks' counters map
    // - for each node_id, compare a's count vs b's count (missing = 0)
    // - track whether you've seen "a >= b on this axis" fail anywhere,
    //   and separately whether "b >= a on this axis" fail anywhere
    // - four outcomes fall out of those two booleans — work through the
    //   truth table yourself rather than guessing at it

    bool a_greater = false; // if this is on then we have a_map MORE CURRENT in one counter
    bool b_greater = false; // if this is on then we have b_map MORE CURRENT in one counter

    const auto& a_map = a.counters();
    const auto& b_map = b.counters();

    // First Pass: Check presence of A entries in B
    int shared_key_cnt = 0;
    for (const auto& [node_id, a_cnt] : a_map) {
        auto it = b_map.find(node_id);
        uint64_t b_cnt;
        if (it != b_map.end()) {
            b_cnt = it->second;
            shared_key_cnt++;
        } else {
            b_cnt = 0;
        }
        if (a_cnt > b_cnt) a_greater = true;
        if (b_cnt > a_cnt) b_greater = true;
        if (a_greater && b_greater) break;
    }

    if (shared_key_cnt < b_map.size() && !(a_greater && b_greater)) {
        // Second Pass: Check for presence of B entries not in A (NOT covered in first pass) --> ONLY if we did NOT iterate through all entries in B
        for (const auto& [node_id, b_cnt] : b_map) {
            if (a_map.find(node_id) == a_map.end()) {
                if (b_cnt > 0) {
                    b_greater = true;
                    break;
                }
            }
        }
    }

    if (!a_greater && !b_greater) {
        return ClockComparison::EQUAL;
    } else if (a_greater && !b_greater) {
        return ClockComparison::DOMINATES;
    } else if (!a_greater && b_greater) {
        return ClockComparison::DOMINATED;
    } else {
        return ClockComparison::CONCURRENT;
    }
}

// Component-wise max of two clocks' counters — union of node_ids, each
// mapped to whichever side has the higher count. Pure merge only: does
// NOT increment anything, does NOT touch last_updated. Decision A1's
// buildNewClock (below) is the caller responsible for the increment step;
// keep this function ignorant of who the coordinator is.
inline driftstore::VectorClock mergeClocks(const driftstore::VectorClock& a,
                                            const driftstore::VectorClock& b) {
    // TODO: for each node_id present in either a.counters() or
    // b.counters(), write max(a's count, b's count) into the result.
    // last_updated is intentionally left unset here — buildNewClock sets
    // it after this returns.
    driftstore::VectorClock vc;

    const auto& a_map = a.counters();
    const auto& b_map = b.counters();

    // First Pass: Check presence of A entries in B
    int shared_key_cnt = 0;
    for (const auto& [node_id, a_cnt] : a_map) {
        auto it = b_map.find(node_id);
        if (it != b_map.end()) {
            uint64_t max_val = std::max(a_cnt, it->second);
            (*vc.mutable_counters())[node_id] = max_val;
            shared_key_cnt++;
        } else {
            (*vc.mutable_counters())[node_id] = a_cnt;
        }
    }

    if (shared_key_cnt < b_map.size()) {
        // Second Pass: Check for presence of B entries not in A (NOT covered in first pass) --> ONLY if we did NOT iterate through all entries in B
        for (const auto& [node_id, b_cnt] : b_map) {
            if (a_map.find(node_id) == a_map.end()) {
                (*vc.mutable_counters())[node_id] = b_cnt;
            }
        }
    }
    return vc;
}

// Decision A1, full sequence: builds the vector clock for a new write.
//
//   client_context     — PutRequest.context if present, nullopt if the
//                         client wrote blind or skipped a prior Get.
//   local_copy         — the coordinator's own currently-stored clock for
//                         this key, nullopt if the coordinator isn't a
//                         replica for this key or holds nothing for it yet.
//   coordinator_node_id — whose axis gets incremented. Always node_id_ of
//                         whichever node is calling this (i.e. self).
//
// Both optionals absent (blind write, non-replica coordinator, brand-new
// key) is a valid input — treat it as merging against an empty clock.
//
// CALLER CONTRACT (decision B1): this function must be invoked while
// holding kv_store_mutex_ for this specific key, for the *entire*
// read-merge-increment-store sequence in Put's handler — not just around
// the final store. This function assumes that atomicity; it does not
// provide it. Get this wrong and two concurrent Puts at the same
// coordinator can race on the same stale base and silently collide (see
// PROGRESS.md's B1 section for the exact mechanism).
inline driftstore::VectorClock buildNewClock(
    const std::optional<driftstore::VectorClock>& client_context,
    const std::optional<driftstore::VectorClock>& local_copy,
    const std::string& coordinator_node_id) {
    // TODO:
    // 1. merge client_context and local_copy via mergeClocks (treat an
    //    absent optional as an empty VectorClock going into the merge)
    // 2. increment coordinator_node_id's own counter in the merged result
    //    by exactly 1 (insert at 1 if coordinator_node_id wasn't present)
    // 3. stamp last_updated with the current wall-clock time — this is
    //    the ONE place a new timestamp gets written; everywhere else
    //    (replication, reconciliation) just carries an existing one
    //    forward, per B2
    driftstore::VectorClock merged = mergeClocks(client_context.value_or(driftstore::VectorClock()), local_copy.value_or(driftstore::VectorClock()));
    (*merged.mutable_counters())[coordinator_node_id] += 1;
    merged.set_last_updated(nowMillis());
    merged.set_writer_id(coordinator_node_id);
    return merged;
}

// Decision B2/B3: applies the LWW tiebreak to a set of versions already
// known to be pairwise CONCURRENT (this function does not itself verify
// that — the caller, resolveGetResult below, is responsible for only
// calling this on a genuine conflict set).
//
// TODO (Q24, unresolved — see OPEN_QUESTIONS.md): exact-timestamp-tie
// behavior is not decided. Do not silently return concurrent_versions[0]
// on a tie without an explicit, deliberate rule — that's a correctness
// decision hiding as a default, not a real tiebreak.
inline const VersionedValue& resolveLWW(const std::vector<VersionedValue>& concurrent_versions) {
    // TODO: pick the entry with the highest clock.last_updated().
    // Precondition you can assume but should still assert/check in
    // debug builds: concurrent_versions is non-empty.
    // If curr winner ever wins or ties, then winner stays the same. Why?
    // In tiebreaker of writer_id, winner has come first and that is the second tiebreaker.
    // In winner > candidate timestamp, winner wins.
    // If candidate ever wins, then we want winner to now be candidate.
    assert(!concurrent_versions.empty());
    const VersionedValue* winner = &concurrent_versions[0];
    for (size_t i = 1; i < concurrent_versions.size(); ++i) {
        const auto& candidate = concurrent_versions[i];
        if (candidate.clock.last_updated() > winner->clock.last_updated()) {
            winner = &candidate;
        } else if (candidate.clock.last_updated() == winner->clock.last_updated() &&
                    candidate.clock.writer_id() > winner->clock.writer_id()) {
            winner = &candidate;
        }
    }
    return *winner;
}

// Decision D1: reduces a set of (value, vector_clock) pairs collected from
// however many of the R replica responses actually came back, down to the
// non-dominated "frontier" — discard any entry that some other entry in
// the set dominates. What's left is either:
//   - one entry: the common case, no real conflict, some replica(s) were
//     just behind another
//   - two or more entries: a genuine conflict, hand to resolveLWW
//
// O(n^2) pairwise comparison via compareVectorClocks is fine at this
// project's R scale (typically 2-3 responses) — don't over-engineer this.
inline std::vector<VersionedValue> computeFrontier(const std::vector<VersionedValue>& replica_responses) {
    // TODO: for each candidate, check whether any other candidate in the
    // list DOMINATES it (or EQUALs it and you've already kept one copy —
    // decide how you want to dedupe true duplicates). Keep only survivors.
    std::vector<VersionedValue> survivors;
    for(size_t i = 0; i < replica_responses.size(); ++i) {
        bool keep = true;
        for (size_t j = 0; j < replica_responses.size(); ++j) {
            if (i == j) continue;
            ClockComparison res = compareVectorClocks(replica_responses[i].clock, replica_responses[j].clock);
            if ((res == ClockComparison::EQUAL) && (j < i)) { // if dupicate of an entry before (can skip because logic already applied to first occurrence entry)
                keep = false;
                break;
            } else if (res == ClockComparison::DOMINATED) {
                keep = false;
                break;
            }
            // DOMINATES: no action - i beats j, keep i until find otherwise or done
            // CONCURRENT: no action - genuine conflict, resort to resolveLWW after
        }
        if (keep) survivors.push_back(replica_responses[i]);
    }
    return survivors;
}

// Decision D1, the function that actually replaces Get's old
// arrival-order-wins logic (see Q20 — arrival_order should no longer
// factor into which value wins at all once this is wired in).
//
// computeFrontier, then resolveLWW only if more than one entry survives.
// This is what Get's handler calls once it's collected VersionedValue
// from each replica that responded — the single VersionedValue this
// returns is what goes into GetResponse (value + the clock the client
// should cache as its next context, per decision D3).
inline const VersionedValue& resolveGetResult(const std::vector<VersionedValue>& replica_responses) {
    // TODO: compose computeFrontier + resolveLWW. Decide what happens on
    // an empty input (shouldn't reach here if Get already checked
    // responses.size() >= R before calling this, but don't assume — guard
    // it explicitly).
    throw std::logic_error("resolveGetResult: not yet implemented");
}