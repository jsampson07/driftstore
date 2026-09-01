#include "driftclient.hpp"

#include <cstdio>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// Thin driver, not a unit test: connects ONE DriftClient, then calls
// put() `--calls` times against it, printing one line per call so a
// bash harness can kill a node mid-run and grep this program's stdout
// for the outcome -- correlated against each node's own PUT_INIT /
// PUT_SUCCEEDED / PUT_FAILED log lines, the debugging shape Phase 3
// established.
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
        std::printf("call=%d success=%s acks=%d\n",
                    i, result.success ? "true" : "false", result.acks);
    }

    return 0;
}