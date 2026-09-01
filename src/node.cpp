#include "driftstore.grpc.pb.h"
#include "logging.hpp"
#include "ring.hpp"

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
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <future>
#include <climits> // Required for INT_MAX

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
    
}  // namespace


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


class NodeServiceImpl final : public driftstore::DriftStoreNode::Service {
public:
    explicit NodeServiceImpl(std::string node_id, int vnodes, int N, int W, int R) : node_id_(std::move(node_id)), vnodes_(vnodes), N_(N), W_(W), R_(R) {
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
        driftstore::MembershipTable table_snapshot;
        std::map<uint64_t, std::string> ring_snapshot;
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            table_snapshot = table_;
            ring_snapshot = ring_;

        }
        response->set_node_id(node_id_);
        response->set_table_dump(dumpTable(table_snapshot));
        response->set_ring_dump(dumpRing(ring_snapshot));
        return grpc::Status::OK;
    }

    grpc::Status RemoveNode(grpc::ServerContext* /* context */,
                         const driftstore::RemoveRequest* request,
                         driftstore::RemoveResponse* response) override {
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

    /**
    Nothing stops a request from being routed to a "REMOVED" node.
    If no check, if self is removed, would happily service the request.
    But also, if node is removed, but coordinator has no knowledge yet (not yet gossiped with)
    then also want to check because cannot assume route to UP nodes only.*/
    bool isSelfRemoved() {
        std::lock_guard<std::mutex> lock(table_mutex_);
        return table_.entries().at(node_id_).status() == driftstore::REMOVED; // constructor guarantees a self-entry always exists so use at() instead of find() in case this invariant is broken
    }

    grpc::Status ReplicateWrite(grpc::ServerContext* /*context*/,
                                const driftstore::ReplicateWriteRequest* request,
                                driftstore::ReplicateWriteResponse* response) override {
        if (isSelfRemoved()) {
            response->set_success(false);
            return grpc::Status::OK;
        }
        localPut(request->key(), request->value());
        response->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status ReplicateRead(grpc::ServerContext* /*context*/,
                               const driftstore::ReplicateReadRequest* request,
                               driftstore::ReplicateReadResponse* response) override {
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

    grpc::Status Put(grpc::ServerContext* /*context*/,
                    const driftstore::PutRequest* request,
                    driftstore::PutResponse* response) override {
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

    grpc::Status Get(grpc::ServerContext* /*context*/,
                    const driftstore::GetRequest* request,
                    driftstore::GetResponse* response) override {
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

    std::unordered_set<std::string> unreachableSnapshot() {
        std::lock_guard<std::mutex> lock(unreachable_peers_mutex_);
        return unreachable_peers_;
    }

    void markUnreachable(const std::string& peer_id) {
        std::lock_guard<std::mutex> lock(unreachable_peers_mutex_);
        unreachable_peers_.insert(peer_id);
    }

    void markReachable(const std::string& peer_id) {
        std::lock_guard<std::mutex> lock(unreachable_peers_mutex_);
        unreachable_peers_.erase(peer_id);
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
    void start(int64_t gossip_interval_ms, int64_t reachability_interval_ms) {
        std::thread([this, gossip_interval_ms]() {
            gossipLoop(gossip_interval_ms);
        }).detach();
        std::thread([this, reachability_interval_ms]() {
            reachabilityLoop(reachability_interval_ms);
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
        mergeInto(table_, incoming, node_id_, ring_);
        return table_;
    }

    std::vector<std::string> preferenceListForKey(const std::string& key, int N) {
        std::unordered_set<std::string> unreachable_peers = unreachableSnapshot();
        auto predicate = [unreachable_peers = std::move(unreachable_peers)](const std::string& node_id) {
            return unreachable_peers.find(node_id) == unreachable_peers.end();  // true = reachable = passes
        };
        std::lock_guard<std::mutex> lock(table_mutex_);
        return preferenceList(ring_, mix64(fnv1a64(key)), N, predicate);
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
    int vnodes_;
    driftstore::MembershipTable table_;
    std::mutex table_mutex_;
    std::map<uint64_t, std::string> ring_;
    std::unordered_set<std::string> unreachable_peers_;
    std::mutex unreachable_peers_mutex_;
    std::unordered_map<std::string, std::string> kv_store_;
    std::mutex kv_store_mutex_;
    int64_t N_;
    int64_t W_;
    int64_t R_;

    bool pingPeer(const std::string& peer_addr) {
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

    void localPut(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(kv_store_mutex_);
        kv_store_[key] = value;
    }

    std::optional<std::string> localGet(const std::string& key) {
        std::lock_guard<std::mutex> lock(kv_store_mutex_);
        auto it = kv_store_.find(key);
        if (it != kv_store_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void gossipLoop(int64_t gossip_interval_ms) {
        while (true) {
            gossipRound(); // Gossip with peer
            std::this_thread::sleep_for(std::chrono::milliseconds(gossip_interval_ms)); // Sleep for default = 1, or provided
        }
    }

    void reachabilityLoop(int64_t reachability_interval_ms) {
        while (true) {
            while (!unreachable_peers_.empty()) {
                reachabilityRound();
                std::this_thread::sleep_for(std::chrono::milliseconds(reachability_interval_ms));
            }
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

    void reachabilityRound() {
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

    std::vector<uint64_t> computeTokens(const std::string& node_id, int V) {
        // We are given a node id and the number of tokens this node will have
        std::vector<uint64_t> tokens;
        for (int i = 0; i < V; i++) {
            uint64_t token_i = mix64(fnv1a64(node_id + ":" + std::to_string(i)));
            tokens.push_back(token_i);
        }
        return tokens;
    }
};

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
