#pragma once
#include "driftstore.pb.h"
#include "logging.hpp"
#include <cstdint>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>
#include <functional>
#include <optional>
#include <queue>

struct PreferenceListEntry {
    std::string node_id;
    std::optional<std::string> hint_for_node_id; // nullopt - natural owner, else - the skipped node this node_id covers
};

inline uint64_t mix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

inline uint64_t fnv1a64(const std::string& data) {
    uint64_t hash = 0xcbf29ce484222325ULL;  // FNV offset basis
    for (unsigned char c : data) {
        hash ^= c;                          // XOR first...
        hash *= 0x100000001b3ULL;    // the real FNV-1a 64-bit prime
    }
    return hash;
}

inline void mergeInto(driftstore::MembershipTable& local,
                       const driftstore::MembershipTable& incoming,
                       const std::string& caller_node_id,
                       std::map<uint64_t, std::string>& ring) {
    auto* local_entries = local.mutable_entries();
    for (const auto& [node_id, incoming_entry] : incoming.entries()) {
        auto it = local_entries->find(node_id);
        if (it == local_entries->end()) {
            // We have a new node that we have just discovered
            for (const auto& token : incoming_entry.tokens()) {
                ring[token] = node_id;
            }
            (*local_entries)[node_id] = incoming_entry;
            continue;
        }
        const auto& local_entry = it->second;

        bool incoming_wins = (incoming_entry.last_updated() > local_entry.last_updated()
            || (incoming_entry.last_updated() == local_entry.last_updated()
                && incoming_entry.writer_id() > local_entry.writer_id()));
        // Add back to ring if REMOVED (local) --> UP (incoming)
        if (incoming_wins) {
            if (local_entry.status() == driftstore::REMOVED && incoming_entry.status() == driftstore::UP) {
                for (const auto& token : incoming_entry.tokens()) {
                    ring[token] = node_id;
                }
                logEvent(EventType::NODE_REBOOTED, caller_node_id, "node_id=" + node_id + " status=" + (local_entry.status() == driftstore::UP ? "UP" : "REMOVED") + 
                        " writer_id=" + incoming_entry.writer_id() + " last_updated=" + std::to_string(incoming_entry.last_updated()));
            } else if (local_entry.status() == driftstore::UP && incoming_entry.status() == driftstore::REMOVED) {
                for (const auto& token : local_entry.tokens()) {
                    ring.erase(token);
                }
            }
            (*local_entries)[node_id] = incoming_entry;
        }
    }
}

inline std::vector<PreferenceListEntry> preferenceList(const std::map<uint64_t, std::string>& ring,
                                                uint64_t key_hash,
                                                int N,
                                                std::function<bool(const std::string&)> is_reachable = [](const std::string&) { return true; }) {
    std::vector<PreferenceListEntry> preference_list;
    if (ring.empty()) {
        return preference_list;
    }
    std::unordered_set<std::string> seen;
    std::queue<std::string> pending_hints; // FIFO: natural owners ONLY that are skipped for reachability
                                            // other aspect of "hints" will be distributed on ReplicateWrite failure
    int natural_count = 0;
    auto it = ring.upper_bound(key_hash);
    if (it == ring.end()) {
        // start iterating from the beginning of the map
        it = ring.begin();
    }
    size_t visited = 0;
    while (true) {
        const std::string& candidate = it->second;
        if (seen.insert(candidate).second) { // already does the check for us if seen or not
            if (natural_count < N) {
                natural_count++;
                if (is_reachable(candidate)) preference_list.push_back({candidate, std::nullopt});
                else pending_hints.push(candidate);
            } else if (is_reachable(candidate) && !pending_hints.empty()) {
                std::string hint_target = pending_hints.front();
                pending_hints.pop();
                preference_list.push_back({candidate, hint_target});
            }
        }
        ++visited;
        if ((static_cast<int>(preference_list.size()) == N) || (visited >= ring.size())) {
            break;
        }
        ++it;
        if (it == ring.end()) {
            it = ring.begin();
        }
    }
    return preference_list;
}