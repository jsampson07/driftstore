#include "driftstore.grpc.pb.h"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <vector>
#include <iostream>
#include <sstream>

namespace {

    int64_t nowMillis() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    // permanent now — backs GetStatus. Sorted by node_id so output is
    // diffable across nodes when comparing status calls by hand.
    std::string dumpTable(const driftstore::MembershipTable& table) {
        std::vector<std::string> keys;
        for (const auto& [node_id, entry] : table.entries()) {
            keys.push_back(node_id);
        }
        std::sort(keys.begin(), keys.end());

        std::string out = "table_size=" + std::to_string(keys.size()) + " entries=[";
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto& entry = table.entries().at(keys[i]);
            out += keys[i] + ":" +
                (entry.status() == driftstore::UP ? "UP" : "REMOVED") +
                ":w=" + entry.writer_id() +
                ":t=" + std::to_string(entry.last_updated());
            if (i + 1 < keys.size()) out += ",";
        }
        out += "]";
        return out;
    }
    
}  // namespace


/**
PUBLIC METHODS
*/


void mergeInto(driftstore::MembershipTable& local, const driftstore::MembershipTable& incoming) {
    auto* local_entries = local.mutable_entries();
    for (const auto& [node_id, incoming_entry] : incoming.entries()) {
        auto it = local_entries->find(node_id);
        if (it == local_entries->end()) {
            (*local_entries)[node_id] = incoming_entry;
            continue;
        }
        const auto& local_entry = it->second;
        if (incoming_entry.last_updated() > local_entry.last_updated()) {
            (*local_entries)[node_id] = incoming_entry;
        } else if (incoming_entry.last_updated() == local_entry.last_updated()) {
            if (incoming_entry.writer_id() > local_entry.writer_id()) {
                (*local_entries)[node_id] = incoming_entry;
            }
        }
    }
}

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


class NodeServiceImpl final : public driftstore::DriftStoreNode::Service {
public:
    explicit NodeServiceImpl(std::string node_id) : node_id_(std::move(node_id)) {
        driftstore::MembershipEntry self_entry;
        self_entry.set_writer_id(node_id_);
        self_entry.set_address(node_id_);
        self_entry.set_status(driftstore::UP);
        self_entry.set_last_updated(nowMillis());
        (*table_.mutable_entries())[node_id_] = self_entry;
    }

    grpc::Status Ping(grpc::ServerContext* /*context*/,
                      const driftstore::PingRequest* request,
                      driftstore::PingResponse* response) override {
        logEvent(EventType::PING_RECEIVED, node_id_,
        "sender=" + request->sender_node_id());
        response->set_responder_node_id(node_id_);
        return grpc::Status::OK;
    }

    grpc::Status GetStatus(grpc::ServerContext* /*context*/,
                           const driftstore::StatusRequest* /*request*/,
                           driftstore::StatusResponse* response) override {
        driftstore::MembershipTable snapshot;
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            snapshot = table_;
        }
        response->set_node_id(node_id_);
        response->set_table_dump(dumpTable(snapshot));
        return grpc::Status::OK;
    }

    // Call once from main(), after BuildAndStart() — mirrors bootstrapFromSeed's
    // placement: the node must be reachable as a server before it starts acting
    // as an autonomous client. Spawns the loop and detaches it.
    //
    // Teardown note: detached + no stop signal is only correct as long as this
    // process's only exit path is SIGKILL. If that assumption ever changes, this needs 
    // a stop mechanism before it's safe. Otherwise if instance containing start()
    // is destroyed while exec'ing gossipLoop(), then we get undefined behavior.
    // Must have entire process killed immediately (SIGKILL).
    void start(int64_t gossip_interval_ms) {
        std::thread([this, gossip_interval_ms]() {
            gossipLoop(gossip_interval_ms);
        }).detach();
    }

    grpc::Status GossipExchange(grpc::ServerContext* /*context*/,
                                const driftstore::GossipRequest* request,
                                driftstore::GossipResponse* response) override {
        logEvent(EventType::GOSSIP_RECEIVED, node_id_,
                 "sender=" + request->sender_node_id());
        driftstore::MembershipTable merged = applyGossip(request->table());
        logEvent(EventType::GOSSIP_MERGED, node_id_,
            "sender=" + request->sender_node_id());
        response->set_responder_node_id(node_id_);
        *response->mutable_table() = merged;
        return grpc::Status::OK;
    }

    /**
     * Send a gossip request to a peer.
     * Initiates GossipExchange (which merges two tables on receiving node).
     * @param peer_address The address of the node to gossip with
     * @param table The table to send to the node to merge
     * @return The response from the peer (response_node_id + merged table)
     */
    std::optional<driftstore::GossipResponse> SendGossip(const std::string& peer_address,
                                          const driftstore::MembershipTable& table) {
        auto channel = grpc::CreateChannel(peer_address, grpc::InsecureChannelCredentials());
        std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
            driftstore::DriftStoreNode::NewStub(channel);

        logEvent(EventType::GOSSIP_SENT, node_id_, "target=" + peer_address);

        driftstore::GossipRequest request;
        request.set_sender_node_id(node_id_);
        *request.mutable_table() = table;
        driftstore::GossipResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub->GossipExchange(&context, request, &response);

        if (status.ok()) {
            logEvent(EventType::GOSSIP_SUCCEEDED, node_id_,
                     "responder=" + response.responder_node_id());
            return response;
        } else {
            logEvent(EventType::GOSSIP_FAILED, node_id_,
                     "target=" + peer_address + " error=" + status.error_message());
            return std::nullopt;
        }
    }

    /**
    * Wrapper function around merge that locks entire merge operation (read incoming table and write to local table)
    * @return Merged table between incoming (from gossip) and local
    */
    driftstore::MembershipTable applyGossip(const driftstore::MembershipTable& incoming) {
        std::lock_guard<std::mutex> lock(table_mutex_);
        mergeInto(table_, incoming);
        return table_;
    }
    
    bool bootstrapFromSeed(const std::vector<std::string>& seeds) {
        int64_t backoff_ms = 250;
        for (int pass = 0; pass < 3; pass++) {
            for (std::string seed : seeds) {
                // Snapshot for each seed (almost negligable work b/c most of the time it will be empty)
                driftstore::MembershipTable snapshot;
                {
                    std::lock_guard<std::mutex> lock(table_mutex_);
                    snapshot = table_;
                }
                std::optional<driftstore::GossipResponse> response = SendGossip(seed, snapshot);
                if (response) {
                    driftstore::MembershipTable merged = applyGossip(response->table()); // This new node will now update its membership table to what merged table returned by seed
                    logEvent(EventType::GOSSIP_MERGED, node_id_,
                        "source=bootstrap seed=" + seed);
                    logEvent(EventType::BOOTSTRAP_SUCCEEDED, node_id_);
                    return true;
                }
            }
            if (pass < 2) {
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms)); // Sleep for default = 1, or provided
                backoff_ms *= 2;
            }
        }
        logEvent(EventType::BOOTSTRAP_FAILED, node_id_, "passes=3 seeds=" + std::to_string(seeds.size()));
        return false;
    }

private:
    std::string node_id_;
    driftstore::MembershipTable table_; // need to initialize this !!!
    std::mutex table_mutex_;

    void gossipLoop(int64_t gossip_interval_ms) {
        while (true) {
            gossipRound(); // Gossip with peer
            std::this_thread::sleep_for(std::chrono::milliseconds(gossip_interval_ms)); // Sleep for default = 1, or provided
        }
    }

    /**
    * 1. Snapshot local table
    * 2. Select candidate peer
    * 2a. If none: LOG and return
    * 3. Send gossip (PUSH)
    * 4. Apply gossip response (merged table on peer with potentially updated local table)
    * - PULL
    */
    void gossipRound() {
        driftstore::MembershipTable snapshot;
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            snapshot = table_;
        }

        std::optional<std::string> peerAddr = selectGossipTarget(snapshot);
        if (!peerAddr) {
            logEvent(EventType::GOSSIP_NO_PEERS, node_id_);
            return;
        }
        std::optional<driftstore::GossipResponse> response = SendGossip(*peerAddr, snapshot);
        if (response) {
            driftstore::MembershipTable merged = applyGossip(response->table()); // This new node will now update its membership table to what merged table returned by seed
            logEvent(EventType::GOSSIP_MERGED, node_id_,
                "source=round peer=" + *peerAddr);
        }
    }

    /**
    * Selects a peer to gossip with. If none, log + return.
    * Cannot select self.
    * @param snapshot Snapshot of local table
    * @return Address of peer node selected used for SendGossip in gossipRound() OR nullopt
    *         if no candidates
    */
    std::optional<std::string> selectGossipTarget(const driftstore::MembershipTable& snapshot) {
        std::vector<std::string> candidates;
        for (const auto& [node_id, entry] : snapshot.entries()) {
            if ((node_id != node_id_) && (entry.status() == driftstore::UP)) {
                candidates.push_back(entry.address());
            }
        }
        if (candidates.empty()) {
            return std::nullopt;
        }
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<std::size_t> dist(0, candidates.size() - 1);
        return candidates[dist(gen)];
    }
};

int main(int argc, char** argv) {
    std::string listen;
    std::string seed_arg;  // possibly comma-separated
    std::string gossip_interval_ms = "1000";
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kListenPrefix[] = "--listen=";
        static constexpr char kSeedPrefix[] = "--seed=";
        static constexpr char kGossipIntervalPrefix[] = "--gossip-interval=";
        if (arg.rfind(kListenPrefix, 0) == 0) {
            listen = arg.substr(sizeof(kListenPrefix) - 1);
        } else if (arg.rfind(kSeedPrefix, 0) == 0) {
            seed_arg = arg.substr(sizeof(kSeedPrefix) - 1);
        } else if (arg.rfind(kGossipIntervalPrefix, 0) == 0) {
            gossip_interval_ms = arg.substr(sizeof(kGossipIntervalPrefix) - 1);
        }
    }
    if (listen.empty()) {
        std::fprintf(stderr,
            "usage: %s --listen=<address> [--seed=<address>[,<address>...]] [--gossip-interval=<time_in_ms>]\n",
            argv[0]);
        return 1;
    }

    const std::string& node_id = listen;
    logEvent(EventType::NODE_INIT, node_id);

    NodeServiceImpl service(node_id);
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

    service.start(std::stoll(gossip_interval_ms));

    server->Wait();
    return 0;
}
