#pragma once
#include "driftstore.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// DriftClient -- a real client library, not a node. It holds no ring
// position, doesn't gossip, doesn't run a server -- it only ever dials
// out as a caller against whichever node it currently picks to
// coordinate. "Any node can coordinate" is a statement about the
// servers being symmetric; DriftClient is the thing that actually lets
// a caller take advantage of that symmetry instead of hardcoding one
// address.
//
// connect(): dials each seed once, in order, verifying liveness via
// Ping. Fails (returns nullopt) if none respond. No retry/backoff here,
// unlike node.cpp's bootstrapFromSeed -- a client is assumed to connect
// after the cluster is already up, so a single pass is enough.
//
// put()/get(): round-robin which seed coordinates -- the starting index
// advances by one on every call (calls_made_ % seed_nodes_.size()),
// regardless of outcome, so repeated calls visibly spread across the
// seed list instead of sticking to whichever one happened to answer
// first. If the seed selected for a given call's rotation slot hits a
// transport-level failure (connection refused/reset/deadline -- NOT an
// application-level success=false), DriftClient walks forward through
// the remaining seeds, wrapping once, for THAT CALL ONLY. The rotation
// counter itself isn't touched by a mid-call failover, so one dead seed
// doesn't permanently skew future rotation onto the survivors.
//
// Deliberately does NOT retry on success=false (e.g. a REMOVED
// coordinator's refusal, or a genuine W/R quorum miss) -- the wire
// protocol currently gives no way to tell those two cases apart, and
// only the first would actually benefit from retrying elsewhere. See
// project chat notes / OPEN_QUESTIONS.md for the full reasoning; this is
// a deliberate, accepted gap, not an oversight.
class DriftClient {
public:
    struct PutResult {
        bool success = false;
        int32_t acks = 0;
    };

    struct GetResult {
        bool found = false;
        std::string value;
        int32_t responses = 0;
    };

    static std::optional<DriftClient> connect(const std::vector<std::string>& seed_nodes) {
        if (seed_nodes.empty()) {
            return std::nullopt;
        }
        for (const auto& addr : seed_nodes) {
            if (pingAddr(addr)) {
                return DriftClient(seed_nodes);
            }
        }
        return std::nullopt;
    }

    PutResult put(const std::string& key, const std::string& value) {
        driftstore::PutRequest req;
        req.set_key(key);
        req.set_value(value);

        PutResult out;
        const size_t start = nextStart();
        for (size_t attempt = 0; attempt < seed_nodes_.size(); ++attempt) {
            const std::string& addr = seed_nodes_[(start + attempt) % seed_nodes_.size()];
            auto stub = dial(addr);

            driftstore::PutResponse resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
            grpc::Status status = stub->Put(&ctx, req, &resp);

            if (status.ok()) {
                out.success = resp.success();
                out.acks = resp.acks();
                return out;
            }
            // transport failure -- try the next seed, this call only
        }
        return out; // every seed unreachable for this call
    }

    GetResult get(const std::string& key) {
        driftstore::GetRequest req;
        req.set_key(key);

        GetResult out;
        const size_t start = nextStart();
        for (size_t attempt = 0; attempt < seed_nodes_.size(); ++attempt) {
            const std::string& addr = seed_nodes_[(start + attempt) % seed_nodes_.size()];
            auto stub = dial(addr);

            driftstore::GetResponse resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
            grpc::Status status = stub->Get(&ctx, req, &resp);

            if (status.ok()) {
                out.found = resp.found();
                out.value = resp.value();
                out.responses = resp.responses();
                return out;
            }
        }
        return out;
    }

private:
    explicit DriftClient(std::vector<std::string> seed_nodes)
        : seed_nodes_(std::move(seed_nodes)) {}

    static std::unique_ptr<driftstore::DriftStoreNode::Stub> dial(const std::string& addr) {
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        return driftstore::DriftStoreNode::NewStub(channel);
    }

    static bool pingAddr(const std::string& addr) {
        auto stub = dial(addr);
        driftstore::PingRequest req;
        req.set_sender_node_id("driftclient");
        driftstore::PingResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
        return stub->Ping(&ctx, req, &resp).ok();
    }

    size_t nextStart() {
        size_t start = calls_made_ % seed_nodes_.size();
        calls_made_++;
        return start;
    }

    std::vector<std::string> seed_nodes_;
    size_t calls_made_ = 0;
};