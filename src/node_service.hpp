#pragma once

#include "driftstore.grpc.pb.h"
#include "vector_clock.hpp"
#include "ring.hpp"

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct HeldHint {
    std::string key;
    // Normalize VersionedValue in appropriate places
    std::string value;
    driftstore::VectorClock clock;
    int64_t created_at;
};

std::string dumpTable(const driftstore::MembershipTable& table);
std::string dumpRing(const std::map<uint64_t, std::string>& ring);

class NodeServiceImpl final : public driftstore::DriftStoreNode::Service {
public:
    explicit NodeServiceImpl(std::string node_id, int vnodes, int N, int W, int R);

    grpc::Status Ping(grpc::ServerContext* /*context*/,
                      const driftstore::PingRequest* request,
                      driftstore::PingResponse* response) override;

    grpc::Status GetStatus(grpc::ServerContext* /*context*/,
                           const driftstore::StatusRequest* /*request*/,
                           driftstore::StatusResponse* response) override;

    grpc::Status RemoveNode(grpc::ServerContext* /* context */,
                         const driftstore::RemoveRequest* request,
                         driftstore::RemoveResponse* response) override;

    /**
    Nothing stops a request from being routed to a "REMOVED" node.
    If no check, if self is removed, would happily service the request.
    But also, if node is removed, but coordinator has no knowledge yet (not yet gossiped with)
    then also want to check because cannot assume route to UP nodes only.*/
    bool isSelfRemoved();

    grpc::Status ReplicateWrite(grpc::ServerContext* /*context*/,
                                const driftstore::ReplicateWriteRequest* request,
                                driftstore::ReplicateWriteResponse* response) override;

    grpc::Status ReplicateRead(grpc::ServerContext* /*context*/,
                               const driftstore::ReplicateReadRequest* request,
                               driftstore::ReplicateReadResponse* response) override;

    grpc::Status Put(grpc::ServerContext* /*context*/,
                    const driftstore::PutRequest* request,
                    driftstore::PutResponse* response) override;

    grpc::Status Get(grpc::ServerContext* /*context*/,
                    const driftstore::GetRequest* request,
                    driftstore::GetResponse* response) override;

    std::unordered_set<std::string> unreachableSnapshot();

    void markUnreachable(const std::string& peer_id);

    void markReachable(const std::string& peer_id);

    // Call once from main(), after BuildAndStart() — mirrors bootstrapFromSeed's
    // placement: the node must be reachable as a server before it starts acting
    // as an autonomous client. Spawns the loop and detaches it.
    //
    // Teardown note: detached + no stop signal is only correct as long as this
    // process's only exit path is SIGKILL. If that assumption ever changes, this needs
    // a stop mechanism before it's safe. Otherwise if instance containing start()
    // is destroyed while exec'ing gossipLoop(), then we get undefined behavior.
    // Must have entire process killed immediately (SIGKILL).
    void start(int64_t gossip_interval_ms, int64_t reachability_interval_ms);

    grpc::Status GossipExchange(grpc::ServerContext* /*context*/,
                                const driftstore::GossipRequest* request,
                                driftstore::GossipResponse* response) override;

    /**
     * Send a gossip request to a peer.
     * Initiates GossipExchange (which merges two tables on receiving node).
     * @param peer_address The address of the node to gossip with
     * @param table The table to send to the node to merge
     * @return The response from the peer (response_node_id + merged table)
     */
    std::optional<driftstore::GossipResponse> SendGossip(const std::string& peer_address,
                                          const driftstore::MembershipTable& table);

    /**
    * Wrapper function around merge that locks entire merge operation (read incoming table and write to local table)
    * @return Merged table between incoming (from gossip) and local
    */
    driftstore::MembershipTable applyGossip(const driftstore::MembershipTable& incoming);

    std::vector<PreferenceListEntry> preferenceListForKey(const std::string& key, int N);

    // This is to find a substitute during live RPC failure
    std::optional<std::string> findSubstitute(const std::string& key, const std::string& true_owner, const std::unordered_set<std::string>& excluded);

    bool bootstrapFromSeed(const std::vector<std::string>& seeds);

private:
    std::string node_id_;
    int vnodes_;
    driftstore::MembershipTable table_;
    std::map<uint64_t, std::string> ring_;
    std::mutex table_mutex_;
    std::unordered_set<std::string> unreachable_peers_;
    std::mutex unreachable_peers_mutex_;
    std::unordered_map<std::string, VersionedValue> kv_store_;
    std::mutex kv_store_mutex_;
    // Hints container is keyed by node_id to answer "which hints are held for Node X" when Node X responds again
    std::unordered_map<std::string, std::unordered_map<std::string, HeldHint>> hints_for_target_;
    std::mutex hints_mutex_;
    
    int64_t N_;
    int64_t W_;
    int64_t R_;

    bool pingPeer(const std::string& peer_addr);

    void localPut(const std::string& key, const VersionedValue& value);

    std::optional<VersionedValue> localGet(const std::string& key);

    grpc::Status forwardPut(const driftstore::PutRequest* request,
                            const std::vector<PreferenceListEntry>& pref_list,
                            driftstore::PutResponse* response);

    grpc::Status coordinatePut(const std::string& key,
                                const std::string& value,
                                const std::optional<driftstore::VectorClock>& client_context,
                                const std::vector<PreferenceListEntry>& pref_list,
                                driftstore::PutResponse* response);

    // Coordinator-side only. Caller (coordinatePut) guarantees node_id_ ∈ pref_list
    // before calling this — that guarantee is what makes the lock meaningful now.
    // One locked read→merge→increment→store, returns the resolved clock.
    driftstore::VectorClock commitCoordinatedWrite(
        const std::string& key,
        const std::string& value,
        const std::optional<driftstore::VectorClock>& client_context);

    // Replica-side, used by ReplicateWrite. Clock already resolved by the
    // coordinator — no buildNewClock call. Plain overwrite for now;
    // branch 4 adds the dominance check inside this same function.
    driftstore::WriteOutcome storeReplicatedWrite(const std::string& key, const VersionedValue& incoming);

    void gossipLoop(int64_t gossip_interval_ms);

    void reachabilityLoop(int64_t reachability_interval_ms);

    /**
    * 1. Snapshot local table
    * 2. Select candidate peer
    * 2a. If none: LOG and return
    * 3. Send gossip (PUSH)
    * 4. Apply gossip response (merged table on peer with potentially updated local table)
    * - PULL
    */
    void gossipRound();

    void reachabilityRound();

    void deliverHints(const std::string& target_node_id);

    void repairReplicas(const std::string& key,
                         const VersionedValue& winner,
                         std::vector<std::string> stale_peers);

    /**
    * Selects a peer to gossip with. If none, log + return.
    * Cannot select self.
    * @param snapshot Snapshot of local table
    * @return Address of peer node selected used for SendGossip in gossipRound() OR nullopt
    *         if no candidates
    */
    std::optional<std::string> selectGossipTarget(const driftstore::MembershipTable& snapshot);

    std::vector<uint64_t> computeTokens(const std::string& node_id, int V);
};
