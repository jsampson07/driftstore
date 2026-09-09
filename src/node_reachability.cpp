#include "node_service.hpp"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

std::unordered_set<std::string> NodeServiceImpl::unreachableSnapshot() {
    std::lock_guard<std::mutex> lock(unreachable_peers_mutex_);
    return unreachable_peers_;
}

void NodeServiceImpl::markUnreachable(const std::string& peer_id) {
    std::lock_guard<std::mutex> lock(unreachable_peers_mutex_);
    unreachable_peers_.insert(peer_id);
}

void NodeServiceImpl::markReachable(const std::string& peer_id) {
    std::lock_guard<std::mutex> lock(unreachable_peers_mutex_);
    unreachable_peers_.erase(peer_id);
}

bool NodeServiceImpl::pingPeer(const std::string& peer_addr) {
    auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
    std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
        driftstore::DriftStoreNode::NewStub(channel);

    driftstore::PingRequest request;
    request.set_sender_node_id(node_id_);
    driftstore::PingResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));

    logEvent(EventType::PROBE_SENT, node_id_, "target=" + peer_addr);
    grpc::Status status = stub->Ping(&context, request, &response);

    if (status.ok()) {
        logEvent(EventType::PROBE_SUCCEEDED, node_id_, "target=" + peer_addr);
        return true;
    }
    logEvent(EventType::PROBE_FAILED, node_id_, "target=" + peer_addr);
    return false;
}

void NodeServiceImpl::reachabilityLoop(int64_t reachability_interval_ms) {
    while (true) {
        while (!unreachable_peers_.empty()) {
            reachabilityRound();
            std::this_thread::sleep_for(std::chrono::milliseconds(reachability_interval_ms));
        }
    }
}

void NodeServiceImpl::reachabilityRound() {
    std::unordered_set<std::string> snapshot = unreachableSnapshot();
    if (snapshot.empty()) {
        return;
    }
    std::vector<std::string> peers(snapshot.begin(), snapshot.end());
    std::vector<std::future<bool>> futures;
    futures.reserve(peers.size());
    for (const std::string& peer : peers) {
        futures.push_back(std::async(std::launch::async, [this, peer]() {
            return pingPeer(peer);
        }));
    }
    for (size_t i = 0; i < peers.size(); ++i) {
        if (futures[i].get()) {
            markReachable(peers[i]);
            deliverHints(peers[i]);
        }
    }
}

void NodeServiceImpl::deliverHints(const std::string& target_node_id) {
    std::unordered_map<std::string, HeldHint> hints_copy;
    {
        std::lock_guard<std::mutex> lock(hints_mutex_);
        auto it = hints_for_target_.find(target_node_id);
        if (it == hints_for_target_.end()) return;
        hints_copy = it->second;
    }
    std::vector<std::future<void>> futures;
    for (const auto& [key, hint] : hints_copy) {
        futures.push_back(std::async(std::launch::async,
            [this, target_node_id, key, hint]() {
                auto channel = grpc::CreateChannel(target_node_id, grpc::InsecureChannelCredentials());
                std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
                    driftstore::DriftStoreNode::NewStub(channel);
                driftstore::ReplicateWriteRequest request;
                request.set_key(hint.key);
                request.set_value(hint.value);
                *request.mutable_vector_clock() = hint.clock;
                driftstore::ReplicateWriteResponse response;
                grpc::ClientContext context;
                grpc::Status status = stub->ReplicateWrite(&context, request, &response);

                if (status.ok() && response.success()) {
                    std::lock_guard<std::mutex> lock(hints_mutex_);
                    auto it = hints_for_target_.find(target_node_id);
                    if (it != hints_for_target_.end()) {
                        auto it2 = it->second.find(key);
                        if (it2 != it->second.end() &&
                            compareVectorClocks(it2->second.clock, hint.clock) == ClockComparison::EQUAL) {
                                it->second.erase(key);
                        }
                    }
                    logEvent(EventType::HINT_DELIVERED, node_id_, "key=" + key + " owner=" + target_node_id);
                } else {
                    if (!status.ok()) markUnreachable(target_node_id);
                    logEvent(EventType::HINT_STORE_FAILED, node_id_, "key=" + key +
                                " target_owner=" + target_node_id);
                    return;
                }
            }));
    }

    for (auto& f : futures) f.get();
}