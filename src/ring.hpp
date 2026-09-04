#pragma once
#include "driftstore.pb.h"
#include "logging.hpp"
#include <cstdint>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>
#include <functional>

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

inline std::vector<std::string> preferenceList(const std::map<uint64_t, std::string>& ring,
                                                uint64_t key_hash,
                                                int N,
                                                std::function<bool(const std::string&)> is_reachable = [](const std::string&) { return true; }) {
    std::vector<std::string> preference_list;
    if (ring.empty()) {
        return preference_list;
    }
    std::unordered_set<std::string> seen;
    auto it = ring.upper_bound(key_hash);
    if (it == ring.end()) {
        // start iterating from the beginning of the map
        it = ring.begin();
    }
    size_t visited = 0;
    while (true) {
        const std::string& candidate = it->second;
        if (is_reachable(candidate)) {
            if (seen.find(candidate) == seen.end()) { // if candidate is UNIQUE physical node
                seen.insert(candidate);
                preference_list.push_back(candidate);
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