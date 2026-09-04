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
        }
    }
}
