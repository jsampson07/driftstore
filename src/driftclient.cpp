#include "driftclient.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// Renders a VectorClock's counters map as "node1:count1,node2:count2,...",
// sorted by node_id for deterministic, diffable output. Duplicated from
// client.cpp's identical helper rather than shared -- small enough that a
// shared header isn't worth it for two call sites.
std::string clockToString(const driftstore::VectorClock& clock) {
    std::vector<std::pair<std::string, uint64_t>> entries(
        clock.counters().begin(), clock.counters().end());
    std::sort(entries.begin(), entries.end());
    std::string out;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        if (i > 0) out += ",";
        out += entries[i].first + ":" + std::to_string(entries[i].second);
    }
    return out;
}

// Thin driver, not a unit test: connects ONE DriftClient, then calls
// put() `--calls` times against it, printing one line per call (now
// including the resolved context=... clock) so a bash harness can kill a
// node mid-run and grep this program's stdout for the outcome --
// correlated against each node's own PUT_INIT / PUT_SUCCEEDED /
// PUT_FAILED log lines, the debugging shape Phase 3 established. Finishes
// with a single get() against the same key, printing its own
// found/value/context -- exercising the read half of the context
// round-trip, which nothing else in this codebase calls at all.
//
// usage: test_driftclient_rotation --seeds=A,B,C --key=k --value=v [--calls=N]
int main(int argc, char** argv) {
    std::vector<std::string> seeds;
    std::string key;
    std::string value;
    int calls = 3;

    auto eat = [](const std::string& arg, const char* prefix) -> std::optional<std::string> {
        const std::string p = prefix;
        if (arg.rfind(p, 0) == 0) {
            return arg.substr(p.size());
        }
        return std::nullopt;
    };

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (auto v = eat(arg, "--seeds=")) {
            std::stringstream ss(*v);
            std::string addr;
            while (std::getline(ss, addr, ',')) {
                seeds.push_back(addr);
            }
        } else if (auto v = eat(arg, "--key=")) {
            key = *v;
        } else if (auto v = eat(arg, "--value=")) {
            value = *v;
        } else if (auto v = eat(arg, "--calls=")) {
            calls = std::stoi(*v);
        }
    }

    if (seeds.empty() || key.empty()) {
        std::fprintf(stderr,
            "usage: %s --seeds=A,B,C --key=k --value=v [--calls=N]\n", argv[0]);
        return 1;
    }

    auto client = DriftClient::connect(seeds);
    if (!client) {
        std::printf("connect result=failed\n");
        return 1;
    }
    std::printf("connect result=succeeded\n");

    for (int i = 0; i < calls; ++i) {
        DriftClient::PutResult result = client->put(key, value);
        std::printf("call=%d success=%s acks=%d context=%s\n",
                    i, result.success ? "true" : "false", result.acks,
                    clockToString(result.context).c_str());
    }

    // Exercises the other half of the round-trip: a get() after the loop
    // above should reflect whatever the accumulated puts converged to,
    // and should itself update the client's cached context for `key`.
    // Nothing else in this codebase calls DriftClient::get() today.
    DriftClient::GetResult get_result = client->get(key);
    std::printf("get found=%s value=%s responses=%d context=%s\n",
                get_result.found ? "true" : "false", get_result.value.c_str(),
                get_result.responses, clockToString(get_result.context).c_str());

    return 0;
}