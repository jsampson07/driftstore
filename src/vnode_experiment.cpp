// vnode_experiment.cpp — standalone script, not part of the live node binary.
// Reuses the real fnv1a64 and preferenceList from ring.hpp rather than
// reimplementing hashing here — the point is measuring *this system's*
// actual distribution behavior, not a hash function that merely resembles it.
#include "ring.hpp"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

int main() {
    constexpr int kNumNodes = 5;
    constexpr int kNumKeys = 100000;  // comfortably above the "10k+" floor
    const std::vector<int> vnode_counts = {1, 4, 16, 64, 256};

    std::vector<std::string> node_ids;
    for (int i = 0; i < kNumNodes; ++i) {
        node_ids.push_back("node" + std::to_string(i));
    }

    printf("%-6s %-10s %-10s %-8s %-8s\n", "V", "mean", "stddev", "min", "max");

    for (int V : vnode_counts) {
        std::map<uint64_t, std::string> ring;
        for (const auto& node_id : node_ids) {
            for (int i = 0; i < V; ++i) {
                uint64_t token = mix64(fnv1a64(node_id + ":" + std::to_string(i)));
                ring[token] = node_id;
            }
        }

        std::unordered_map<std::string, int> counts;
        for (const auto& node_id : node_ids) {
            counts[node_id] = 0;
        }

        for (int k = 0; k < kNumKeys; ++k) {
            std::string key = "key" + std::to_string(k);
            uint64_t key_hash = mix64(fnv1a64(key));
            auto owner = preferenceList(ring, key_hash, 1);
            counts[owner[0]]++;
        }

        double mean = static_cast<double>(kNumKeys) / kNumNodes;
        double sum_sq_dev = 0.0;
        int min_count = kNumKeys;
        int max_count = 0;
        for (const auto& [node_id, count] : counts) {
            double dev = count - mean;
            sum_sq_dev += dev * dev;
            min_count = std::min(min_count, count);
            max_count = std::max(max_count, count);
        }
        double stddev = std::sqrt(sum_sq_dev / kNumNodes);

        printf("%-6d %-10.1f %-10.2f %-8d %-8d\n", V, mean, stddev, min_count, max_count);
    }

    return 0;
}