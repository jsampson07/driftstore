#include "node_service.hpp"
#include "logging.hpp"
#include "ring.hpp"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <climits> // Required for INT_MAX
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

bool NodeServiceImpl::isSelfRemoved() {
    std::lock_guard<std::mutex> lock(table_mutex_);
    return table_.entries().at(node_id_).status() == driftstore::REMOVED; // constructor guarantees a self-entry always exists so use at() instead of find() in case this invariant is broken
}

grpc::Status NodeServiceImpl::ReplicateWrite(grpc::ServerContext* /*context*/,
                            const driftstore::ReplicateWriteRequest* request,
                            driftstore::ReplicateWriteResponse* response) {
    if (isSelfRemoved()) {
        response->set_success(false);
        return grpc::Status::OK;
    }
    localPut(request->key(), request->value());
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::ReplicateRead(grpc::ServerContext* /*context*/,
                           const driftstore::ReplicateReadRequest* request,
                           driftstore::ReplicateReadResponse* response) {
    if (isSelfRemoved()) {
        response->set_found(false);
        return grpc::Status::OK;
    }
    std::optional<std::string> value = localGet(request->key());
    response->set_found(value.has_value());
    if (value) {
        response->set_value(*value);
    }
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::Put(grpc::ServerContext* /*context*/,
                const driftstore::PutRequest* request,
                driftstore::PutResponse* response) {
    // If client had stale view of node membership and request routed to REMOVED node, MUST not handle/service request --> FAIL
    if (isSelfRemoved()) {
        logEvent(EventType::PUT_FAILED, node_id_, "reason=self_removed");
        response->set_success(false);
        response->set_acks(0);
        return grpc::Status::OK;
    }
    
    std::string key = request->key();
    std::string value = request->value();
    std::vector<std::string> pref_list = preferenceListForKey(key, N_);
    if (pref_list.size() < W_) {
        logEvent(EventType::PUT_FAILED, node_id_, "key=" + key + " val=" + value + " W=" + std::to_string(W_) + " preference_list_size=" + std::to_string(static_cast<int>(pref_list.size())));
        return grpc::Status::OK;
    }

    std::atomic<int64_t> acks{0}; // IMPORTANT to make it atomic as two threads can read same value, increment ==> LOSE an ack

    for (const auto& peer_id : pref_list) {
        if (peer_id == node_id_) {
            localPut(key, value);
            acks++;
            break;
        }
    }

    std::vector<std::future<void>> futures;
    for (const auto& peer_id : pref_list) {
        if (peer_id == node_id_) {
            continue;
        }
        futures.push_back(std::async(std::launch::async,
            [this, peer_id, key, value, &acks]() {
                auto channel = grpc::CreateChannel(peer_id, grpc::InsecureChannelCredentials());
                std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
                    driftstore::DriftStoreNode::NewStub(channel);
    
                driftstore::ReplicateWriteRequest req;
                req.set_key(key);
                req.set_value(value);
                driftstore::ReplicateWriteResponse resp;
                grpc::ClientContext context;
                grpc::Status status = stub->ReplicateWrite(&context, req, &resp);
                
                if (status.ok() && resp.success()) {
                    acks++;
                } // if NOT OK or NOT success then either Put RPC failed or wrote to REMOVED node --> treat the same way, do NOT increment 'acks'
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    if (acks < W_) {
        logEvent(EventType::PUT_FAILED, node_id_, "key=" + key + " val=" + value + " W=" + std::to_string(W_) + " acks=" + std::to_string(acks.load()));
        response->set_success(false);
        response->set_acks(acks.load());
        return grpc::Status::OK;
    }
    logEvent(EventType::PUT_SUCCEEDED, node_id_, "key=" + key + " val=" + value + " W=" + std::to_string(W_) + " acks=" + std::to_string(acks.load()));
    response->set_success(true);
    response->set_acks(acks.load());
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::Get(grpc::ServerContext* /*context*/,
                const driftstore::GetRequest* request,
                driftstore::GetResponse* response) {
    if (isSelfRemoved()) {
        logEvent(EventType::GET_FAILED, node_id_, "reason=self_removed");
        response->set_found(false);
        return grpc::Status::OK;
    }
    
    std::string key = request->key();
    // Generate preference list for this key
    std::vector<std::string> pref_list = preferenceListForKey(key, N_);
    // If pref list size < R then read will ALWAYS fail
    if (static_cast<int>(pref_list.size()) < R_) {
        logEvent(EventType::GET_FAILED, node_id_, "key=" + key + " R=" + std::to_string(R_) + " preference_list_size=" + std::to_string(pref_list.size()));
        return grpc::Status::OK;
    }

    // Used to represent a result (includes arrival_order to track each result)
    struct ReplicaResult {
        std::string peer_id;
        bool found = false;
        std::string value;
        int arrival_order = -1;
    };

    std::vector<ReplicaResult> results;
    std::mutex results_mutex;
    int next_arrival = 0;

    // First check if we can have local read AND no self-RPC

    logEvent(EventType::GET_INIT, node_id_);
    for (const auto& peer_id : pref_list) {
        if (peer_id == node_id_) {
            std::optional<std::string> local_value = localGet(key);
            ReplicaResult r;
            r.peer_id = peer_id;
            r.found = local_value.has_value();
            if (local_value) {
                r.value = *local_value;
            }
            r.arrival_order = next_arrival++;
            results.push_back(std::move(r));
            break;
        }
    }

    std::vector<std::future<void>> futures;
    for (const auto& peer_id : pref_list) {
        if (peer_id == node_id_) {
            continue;
        }
        futures.push_back(std::async(std::launch::async,
            [this, peer_id, key, &results, &results_mutex, &next_arrival]() {
                auto channel = grpc::CreateChannel(peer_id, grpc::InsecureChannelCredentials());
                std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
                    driftstore::DriftStoreNode::NewStub(channel);
    
                driftstore::ReplicateReadRequest req;
                req.set_key(key);
                driftstore::ReplicateReadResponse resp;
                grpc::ClientContext context;
                grpc::Status status = stub->ReplicateRead(&context, req, &resp);

                /**
                * There is clear distinction between status returning OK and key not being found.
                * If a key is NOT found, this is a LEGITIMATE response we want in our 'results'.
                * If a ReplicateRead FAILS, then this is a FAILED attempt, where we do NOT want our result to be recorded.
                *
                * KEY distinction: include results that are successful but may be found/NOT found versus exclude FAILED RPC requests
                */
                if (status.ok()) {
                    ReplicaResult r;
                    r.peer_id = peer_id;
                    r.found = resp.found();
                    if (r.found) {
                        r.value = resp.value();
                    }
                    std::lock_guard<std::mutex> lock(results_mutex);
                    r.arrival_order = next_arrival++;
                    results.push_back(std::move(r));
                }
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    if (static_cast<int>(results.size()) < R_) {
        logEvent(EventType::GET_FAILED, node_id_, "key=" + key + " R=" + std::to_string(R_) + " responses=" + std::to_string(results.size()));
        response->set_responses(results.size());
        return grpc::Status::OK;
    }

    // We have >= R responses
    ReplicaResult final_r;
    int min_arrival_order = INT_MAX;
    for (const auto& res : results) {
        if (res.arrival_order < min_arrival_order) {
            final_r = res;
            min_arrival_order = res.arrival_order;
        }
    }
    response->set_found(final_r.found);
    if (final_r.found) {
        response->set_value(final_r.value);
    }
    response->set_responses(results.size());
    logEvent(EventType::GET_SUCCEEDED, node_id_, "key=" + key);
    return grpc::Status::OK;
}

std::vector<std::string> NodeServiceImpl::preferenceListForKey(const std::string& key, int N) {
    std::unordered_set<std::string> unreachable_peers = unreachableSnapshot();
    auto predicate = [unreachable_peers = std::move(unreachable_peers)](const std::string& node_id) {
        return unreachable_peers.find(node_id) == unreachable_peers.end();  // true = reachable = passes
    };
    std::lock_guard<std::mutex> lock(table_mutex_);
    return preferenceList(ring_, mix64(fnv1a64(key)), N, predicate);
}

void NodeServiceImpl::localPut(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(kv_store_mutex_);
    kv_store_[key] = value;
}

std::optional<std::string> NodeServiceImpl::localGet(const std::string& key) {
    std::lock_guard<std::mutex> lock(kv_store_mutex_);
    auto it = kv_store_.find(key);
    if (it != kv_store_.end()) {
        return it->second;
    }
    return std::nullopt;
}
