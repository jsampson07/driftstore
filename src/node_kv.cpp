#include "node_service.hpp"
#include "logging.hpp"
#include "ring.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <atomic>
#include <climits> // Required for INT_MAX
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <random>

namespace {
    std::string renderClock(const driftstore::VectorClock& clock) {
        std::vector<std::pair<std::string, uint64_t>> entries(
            clock.counters().begin(), clock.counters().end());
        std::sort(entries.begin(), entries.end());
        std::string out = "{";
        for (size_t i = 0; i < entries.size(); ++i) {
            if (i) out += ",";
            out += entries[i].first + ":" + std::to_string(entries[i].second);
        }
        out += "}";
        return out;
    }
}

bool NodeServiceImpl::isSelfRemoved() {
    std::lock_guard<std::mutex> lock(table_mutex_);
    return table_.entries().at(node_id_).status() == driftstore::REMOVED; // constructor guarantees a self-entry always exists so use at() instead of find() in case this invariant is broken
}

grpc::Status NodeServiceImpl::ReplicateWrite(grpc::ServerContext* /*context*/,
                            const driftstore::ReplicateWriteRequest* request,
                            driftstore::ReplicateWriteResponse* response) {
    if (isSelfRemoved()) {
        response->set_success(false);
        response->set_outcome(driftstore::WriteOutcome::ALREADY_CURRENT); // Not parsed when success = false, but as safety
        return grpc::Status::OK;
    }
    VersionedValue incoming_vv;
    incoming_vv.value = request->value();
    incoming_vv.clock = request->vector_clock();
    driftstore::WriteOutcome res = storeReplicatedWrite(request->key(), incoming_vv);
    response->set_success(true);
    response->set_outcome(res);
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::ReplicateRead(grpc::ServerContext* /*context*/,
                           const driftstore::ReplicateReadRequest* request,
                           driftstore::ReplicateReadResponse* response) {
    if (isSelfRemoved()) {
        response->set_found(false);
        return grpc::Status::OK;
    }
    std::optional<VersionedValue> vv = localGet(request->key());
    response->set_found(vv.has_value());
    if (vv) {
        response->set_value(vv->value);
        *response->mutable_vector_clock() = vv->clock;
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
    driftstore::VectorClock clock = request->context();
    std::vector<PreferenceListEntry> pref_list = preferenceListForKey(key, N_);

    auto it = std::find_if(pref_list.begin(), pref_list.end(),
        [this](const PreferenceListEntry& entry) { return entry.node_id == node_id_; });
    grpc::Status status;
    if (it == pref_list.end()) {
        if (request->forwarded()) {
            logEvent(EventType::PUT_FAILED, node_id_, "reason=forward_loop_prevented");
            response->set_success(false);
            response->set_acks(0);
            return grpc::Status::OK;
        }
        return forwardPut(request, pref_list, response);
    } else {
        return coordinatePut(key, value, clock, pref_list, response);
    }
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
    std::vector<PreferenceListEntry> pref_list = preferenceListForKey(key, N_);
    // If pref list size < R then read will ALWAYS fail
    if (static_cast<int>(pref_list.size()) < R_) {
        logEvent(EventType::GET_FAILED, node_id_, "key=" + key + " R=" + std::to_string(R_) + " preference_list_size=" + std::to_string(pref_list.size()));
        return grpc::Status::OK;
    }

    struct ReplicaResult {
        std::string peer_id;
        bool found = false;
        std::string value;
        driftstore::VectorClock clock;
    };

    std::vector<ReplicaResult> results;
    std::mutex results_mutex;

    // First check if we can have local read AND no self-RPC

    logEvent(EventType::GET_INIT, node_id_);
    for (const auto& entry : pref_list) {
        if (entry.node_id == node_id_) {
            std::optional<VersionedValue> local_value = localGet(key);
            ReplicaResult r;
            r.peer_id = entry.node_id;
            r.found = local_value.has_value();
            if (local_value) {
                r.value = local_value->value;
                r.clock = local_value->clock;
            }
            std::lock_guard<std::mutex> lock(results_mutex);
            results.push_back(std::move(r));
            break;
        }
    }

    std::vector<std::future<void>> futures;
    for (const auto& entry : pref_list) {
        if (entry.node_id == node_id_) {
            continue;
        }
        futures.push_back(std::async(std::launch::async,
            [this, peer_id = entry.node_id, key, &results, &results_mutex]() {
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
                        r.clock = resp.vector_clock();
                    }
                    std::lock_guard<std::mutex> lock(results_mutex);
                    results.push_back(std::move(r));
                } else {
                    markUnreachable(peer_id);
                }
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    response->set_responses(results.size());
    if (static_cast<int>(results.size()) < R_) {
        logEvent(EventType::GET_FAILED, node_id_, "key=" + key + " R=" + std::to_string(R_) + " responses=" + std::to_string(results.size()));
        response->set_found(false);
        return grpc::Status::OK;
    }

    std::vector<VersionedValue> found_versions;
    for (const auto& res : results) {
        if (res.found) {
            found_versions.push_back(VersionedValue{res.value, res.clock});
        }
    }
    if (found_versions.empty()) {
        logEvent(EventType::GET_SUCCEEDED, node_id_, "key=" + key + " reason=key_not_found");
        response->set_found(false);
        return grpc::Status::OK;
    }
    GetResolution resolved = resolveGetResult(found_versions);
    if (resolved.concurrent_count > 1) {
        logEvent(EventType::CONFLICT_RESOLVED, node_id_, "key=" + key +
                " concurrent_versions=" + std::to_string(resolved.concurrent_count) +
                " winner_writer_id=" + resolved.winner.clock.writer_id() +
                " winner_last_updated=" + std::to_string(resolved.winner.clock.last_updated()));
    }
    response->set_found(true);
    response->set_value(resolved.winner.value);
    *response->mutable_context() = resolved.winner.clock;
    logEvent(EventType::GET_SUCCEEDED, node_id_, "key=" + key + " val=" + resolved.winner.value + " winning_clock=" + renderClock(resolved.winner.clock));
    return grpc::Status::OK;
}

std::vector<PreferenceListEntry> NodeServiceImpl::preferenceListForKey(const std::string& key, int N) {
    std::unordered_set<std::string> unreachable_peers = unreachableSnapshot();
    auto predicate = [unreachable_peers = std::move(unreachable_peers)](const std::string& node_id) {
        return unreachable_peers.find(node_id) == unreachable_peers.end();  // true = reachable = passes
    };
    std::lock_guard<std::mutex> lock(table_mutex_);
    return preferenceList(ring_, mix64(fnv1a64(key)), N, predicate);
}

grpc::Status NodeServiceImpl::forwardPut(const driftstore::PutRequest* request,
                                         const std::vector<PreferenceListEntry>& pref_list,
                                         driftstore::PutResponse* response) {
    driftstore::PutRequest forwarded_request = *request; // create a copy of the request
    forwarded_request.set_forwarded(true);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::size_t> dist(0, pref_list.size() - 1);
    const std::size_t start = dist(gen);

    for (std::size_t i = 0; i < pref_list.size(); ++i) {
        const std::string& target = pref_list[(start + i) % pref_list.size()].node_id;
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
            driftstore::DriftStoreNode::NewStub(channel);

        driftstore::PutResponse target_response;
        grpc::ClientContext context;
        grpc::Status status = stub->Put(&context, forwarded_request, &target_response);

        if (status.ok()) {
            logEvent(EventType::PUT_FORWARDED, node_id_,
                "key=" + request->key() + " target=" + target);
            *response = target_response; // relay target's coordinator result straight through
            return grpc::Status::OK;
        } else { // did live RPC fail? --> markUnreachable()
            markUnreachable(target); // target is node we are trying to forward Put to; if call fails, then unreachable (NOT same as "failing" b/c of double-forward for ex.)
        }
        logEvent(EventType::PUT_FORWARD_FAILED, node_id_,
            "key=" + request->key() + " target=" + target + " error=" + status.error_message());
        // fall through, try (start+i+1) % size next
    }

    // Every node in pref_list was unreachable
    logEvent(EventType::PUT_FAILED, node_id_, "key=" + request->key() + " reason=all_forward_targets_unreachable");
    response->set_success(false);
    response->set_acks(0);
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::coordinatePut(const std::string& key,
                            const std::string& value,
                            const std::optional<driftstore::VectorClock>& client_context,
                            const std::vector<PreferenceListEntry>& pref_list,
                            driftstore::PutResponse* response) {
    if (pref_list.size() < W_) {
        logEvent(EventType::PUT_FAILED, node_id_, "key=" + key + " val=" + value + " W=" + std::to_string(W_) + " preference_list_size=" + std::to_string(static_cast<int>(pref_list.size())));
        return grpc::Status::OK;
    }

    std::atomic<int64_t> acks{0}; // IMPORTANT to make it atomic as two threads can read same value, increment ==> LOSE an ack

    driftstore::VectorClock clock;
    for (const auto& entry : pref_list) {
        if (entry.node_id == node_id_) {
            clock = commitCoordinatedWrite(key, value, client_context);
            acks++;
            break;
        }
    }

    std::vector<std::future<void>> futures;
    for (const auto& entry : pref_list) {
        if (entry.node_id == node_id_) {
            continue;
        }
        futures.push_back(std::async(std::launch::async,
            [this, peer_id = entry.node_id, key, value, clock, &acks]() {
                auto channel = grpc::CreateChannel(peer_id, grpc::InsecureChannelCredentials());
                std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
                    driftstore::DriftStoreNode::NewStub(channel);
    
                driftstore::ReplicateWriteRequest req;
                req.set_key(key);
                req.set_value(value);
                *req.mutable_vector_clock() = clock;
                driftstore::ReplicateWriteResponse resp;
                grpc::ClientContext context;
                grpc::Status status = stub->ReplicateWrite(&context, req, &resp);
                
                if (status.ok()) {
                    if (resp.success()) {
                        acks++;
                    }
                } else {
                    markUnreachable(peer_id);
                }
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    *response->mutable_context() = clock;

    if (acks < W_) {
        logEvent(EventType::PUT_FAILED, node_id_, "key=" + key + " val=" + value + " W=" + std::to_string(W_) + " acks=" + std::to_string(acks.load()));
        response->set_success(false);
        response->set_acks(acks.load());
        return grpc::Status::OK;
    }
    logEvent(EventType::PUT_SUCCEEDED, node_id_, "key=" + key + " val=" + value + " clock=" + renderClock(clock) + " W=" + std::to_string(W_) + " acks=" + std::to_string(acks.load()));
    response->set_success(true);
    response->set_acks(acks.load());
    return grpc::Status::OK;
}

std::optional<VersionedValue> NodeServiceImpl::localGet(const std::string& key) {
    std::lock_guard<std::mutex> lock(kv_store_mutex_);
    auto it = kv_store_.find(key);
    if (it != kv_store_.end()) {
        return it->second;
    }
    return std::nullopt;
}

// Coordinator-side only. Caller (coordinatePut) guarantees node_id_ ∈ pref_list
// before calling this — that guarantee is what makes the lock meaningful now.
// One locked read→merge→increment→store, returns the resolved clock.
driftstore::VectorClock NodeServiceImpl::commitCoordinatedWrite(const std::string& key,
                                                                const std::string& value,
                                                                const std::optional<driftstore::VectorClock>& client_context) {
    std::lock_guard<std::mutex> lock(kv_store_mutex_);
    // Read
    auto it = kv_store_.find(key);
    std::optional<driftstore::VectorClock> local_copy;
    if (it != kv_store_.end()) {
        local_copy = it->second.clock;
    }
    // Merge + Increment
    driftstore::VectorClock updated = buildNewClock(client_context, local_copy, node_id_);
    // Store
    VersionedValue val;
    val.value = value;
    val.clock = updated;
    // The new clock always dominates the old local copy + client provided context (as it is built from a component-wise max for each)
    // The value being is stored is on the dominant form of the two clocks, so value will always be the newest updated version
    kv_store_[key] = val;
    return updated;
}

// Replica-side, used by ReplicateWrite. Clock already resolved by the
// coordinator — no buildNewClock call. Dominance check inside this function.
driftstore::WriteOutcome NodeServiceImpl::storeReplicatedWrite(const std::string& key, const VersionedValue& incoming) {

    std::lock_guard<std::mutex> lock(kv_store_mutex_);
    auto it = kv_store_.find(key);
    if (it == kv_store_.end()) {
        // If not a key,val pair, store
        kv_store_[key] = incoming;
        return driftstore::WriteOutcome::STORED;
    } else {
        ClockComparison res = compareVectorClocks(it->second.clock, incoming.clock);
        if (res == ClockComparison::EQUAL) { // local-copy DOMINATES
            // NOTE: if stored and incoming vals are different, then
            // either one of the writes got silently dropped by ALREADY_CURRENT
            // OR
            // a version update (clock) wasn't properly applied (did not increment)
            logEvent(EventType::EQUAL_CLOCK_DETECTED, node_id_,
                "key=" + key +
                " stored_val=" + it->second.value + " incoming_val=" + incoming.value +
                " clock=" + renderClock(incoming.clock));
            return driftstore::WriteOutcome::ALREADY_CURRENT;
        } else if (res == ClockComparison::DOMINATES) {
            return driftstore::WriteOutcome::ALREADY_CURRENT;
        } else {
            kv_store_[key] = incoming;
            return driftstore::WriteOutcome::STORED;
        }
    }
    // will perform dominance check again later: if NOT dominant then do not replace, if dominate, replace
}