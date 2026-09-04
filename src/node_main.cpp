#include "node_service.hpp"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

/**
PUBLIC METHODS
*/


std::vector<std::string> splitSeeds(const std::string& raw) {
    std::vector<std::string> result;
    std::stringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            result.push_back(token);
        }
    }
    return result;
}

NodeServiceImpl::NodeServiceImpl(std::string node_id, int vnodes, int N, int W, int R) : node_id_(std::move(node_id)), vnodes_(vnodes), N_(N), W_(W), R_(R) {
    driftstore::MembershipEntry self_entry;
    self_entry.set_writer_id(node_id_);
    self_entry.set_address(node_id_);
    self_entry.set_status(driftstore::UP);
    self_entry.set_last_updated(nowMillis());
    std::vector<uint64_t> tokens = computeTokens(node_id_, vnodes_);
    for (uint64_t token : tokens) {
        self_entry.add_tokens(token);
        ring_[token] = node_id_;
    }
    (*table_.mutable_entries())[node_id_] = self_entry;
}

void NodeServiceImpl::start(int64_t gossip_interval_ms, int64_t reachability_interval_ms) {
    std::thread([this, gossip_interval_ms]() {
        gossipLoop(gossip_interval_ms);
    }).detach();
    std::thread([this, reachability_interval_ms]() {
        reachabilityLoop(reachability_interval_ms);
    }).detach();
}

int main(int argc, char** argv) {
    std::string listen;
    std::string seed_arg;  // possibly comma-separated
    std::string gossip_interval_ms = "1000";
    std::string reachability_interval_ms = "3000";
    std::string vnodes_str = "32";
    std::string n_str = "3";
    std::string w_str = "2";
    std::string r_str = "2";
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kListenPrefix[] = "--listen=";
        static constexpr char kSeedPrefix[] = "--seed=";
        static constexpr char kGossipIntervalPrefix[] = "--gossip-interval=";
        static constexpr char kReachabilityIntervalPrefix[] = "--probe-interval=";
        static constexpr char kVnodesPrefix[] = "--vnodes=";
        static constexpr char kNPrefix[] = "--N=";
        static constexpr char kWPrefix[] = "--W=";
        static constexpr char kRPrefix[] = "--R=";
        if (arg.rfind(kListenPrefix, 0) == 0) {
            listen = arg.substr(sizeof(kListenPrefix) - 1);
        } else if (arg.rfind(kSeedPrefix, 0) == 0) {
            seed_arg = arg.substr(sizeof(kSeedPrefix) - 1);
        } else if (arg.rfind(kGossipIntervalPrefix, 0) == 0) {
            gossip_interval_ms = arg.substr(sizeof(kGossipIntervalPrefix) - 1);
        } else if (arg.rfind(kReachabilityIntervalPrefix, 0) == 0) {
            reachability_interval_ms = arg.substr(sizeof(kReachabilityIntervalPrefix) - 1);
        } else if (arg.rfind(kVnodesPrefix, 0) == 0) {
            vnodes_str = arg.substr(sizeof(kVnodesPrefix) - 1);
        } else if (arg.rfind(kNPrefix, 0) == 0) {
            n_str = arg.substr(sizeof(kNPrefix) - 1);
        } else if (arg.rfind(kWPrefix, 0) == 0) {
            w_str = arg.substr(sizeof(kWPrefix) - 1);
        } else if (arg.rfind(kRPrefix, 0) == 0) {
            r_str = arg.substr(sizeof(kRPrefix) - 1);
        }
    }
    if (listen.empty()) {
        std::fprintf(stderr,
            "usage: %s --listen=<address> [--seed=<address>[,<address>...]] "
            "[--gossip-interval=<time_in_ms>] [--vnodes=<count>]\n",
            argv[0]);
        return 1;
    }

    const int N = std::stoi(n_str);
    const int W = std::stoi(w_str);
    const int R = std::stoi(r_str);
    // W + R > N, and W, R <= N
    if (N < 1 || W < 1 || R < 1 || W > N || R > N || (W + R) <= N) {
        std::fprintf(stderr,
            "invalid quorum config: N=%d W=%d R=%d "
            "(require N,W,R >= 1, W <= N, R <= N, W + R > N)\n",
            N, W, R);
        return 1;
    }

    const std::string& node_id = listen;
    const int vnodes = std::stoi(vnodes_str);
    logEvent(EventType::NODE_INIT, node_id);

    NodeServiceImpl service(node_id, vnodes, N, W, R);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();

    std::vector<std::string> seeds = splitSeeds(seed_arg);

    if (!seeds.empty()) {
        logEvent(EventType::BOOTSTRAP_INIT, node_id,
            "source=bootstrap seeds=" + seed_arg);
        if (!service.bootstrapFromSeed(seeds)) {
            // Do not call service.start().
            // server->Shutdown() before this return is the open question
            // from earlier. Explicitly deferred for now.
            return 1;
        }
    }

    service.start(std::stoll(gossip_interval_ms), std::stoll(reachability_interval_ms));

    server->Wait();
    return 0;
}
