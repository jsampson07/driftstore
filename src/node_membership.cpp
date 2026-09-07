#include "node_service.hpp"
#include "logging.hpp"
#include "ring.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

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

std::string dumpRing(const std::map<uint64_t, std::string>& ring) {
    std::string out = "ring_size=" + std::to_string(ring.size()) + " tokens=[";
    size_t i = 0;
    for (const auto& [token, node_id] : ring) {
        out += std::to_string(token) + ":" + node_id; // <token>:<node_id_owner_of_token>
        if (++i < ring.size()) {
            out += ",";
        }
    }
    out += "]";
    return out;
}

grpc::Status NodeServiceImpl::RemoveNode(grpc::ServerContext* /* context */,
                     const driftstore::RemoveRequest* request,
                     driftstore::RemoveResponse* response) {
    std::lock_guard<std::mutex> lock(table_mutex_);
    auto* entries = table_.mutable_entries();
    auto it = entries->find(request->removed_node_id());
    if (it == entries->end()) {
        response->set_accepted(false);
    } else {
        if (it->second.status() == driftstore::REMOVED) {
            // No-op
        } else {
            it->second.set_status(driftstore::REMOVED);
            it->second.set_writer_id(node_id_);
            for (const auto& token : it->second.tokens()) {
                ring_.erase(token);
            }
            it->second.clear_tokens();
            it->second.set_last_updated(nowMillis());
        }
        response->set_accepted(true);
        logEvent(EventType::REMOVE_SUCCEEDED, node_id_,
            "node_id=" + request->removed_node_id() +
            " status=" + (it->second.status() == driftstore::UP ? "UP" : "REMOVED") +
            " writer_id=" + node_id_ +
            " last_updated=" + std::to_string(it->second.last_updated()));
    }
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::GossipExchange(grpc::ServerContext* /*context*/,
                            const driftstore::GossipRequest* request,
                            driftstore::GossipResponse* response) {
    logEvent(EventType::GOSSIP_RECEIVED, node_id_,
             "sender=" + request->sender_node_id());
    driftstore::MembershipTable merged = applyGossip(request->table());
    logEvent(EventType::GOSSIP_MERGED, node_id_,
        "sender=" + request->sender_node_id());
    response->set_responder_node_id(node_id_);
    *response->mutable_table() = merged;
    return grpc::Status::OK;
}

std::optional<driftstore::GossipResponse> NodeServiceImpl::SendGossip(const std::string& peer_address,
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
        // If cannot gossip with another peer, mark as unreachable
        markUnreachable(peer_address);
        return std::nullopt;
    }
}

driftstore::MembershipTable NodeServiceImpl::applyGossip(const driftstore::MembershipTable& incoming) {
    std::lock_guard<std::mutex> lock(table_mutex_);
    mergeInto(table_, incoming, node_id_, ring_);
    return table_;
}

bool NodeServiceImpl::bootstrapFromSeed(const std::vector<std::string>& seeds) {
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

void NodeServiceImpl::gossipLoop(int64_t gossip_interval_ms) {
    while (true) {
        gossipRound(); // Gossip with peer
        std::this_thread::sleep_for(std::chrono::milliseconds(gossip_interval_ms)); // Sleep for default = 1, or provided
    }
}

void NodeServiceImpl::gossipRound() {
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

std::optional<std::string> NodeServiceImpl::selectGossipTarget(const driftstore::MembershipTable& snapshot) {
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

std::vector<uint64_t> NodeServiceImpl::computeTokens(const std::string& node_id, int V) {
    // We are given a node id and the number of tokens this node will have
    std::vector<uint64_t> tokens;
    for (int i = 0; i < V; i++) {
        uint64_t token_i = mix64(fnv1a64(node_id + ":" + std::to_string(i)));
        tokens.push_back(token_i);
    }
    return tokens;
}
