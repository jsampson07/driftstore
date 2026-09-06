#pragma once
#include "driftstore.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
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
// Context round-trip (phase4/driftclient-context-roundtrip): DriftClient
// holds the last VectorClock it's seen per key. put() attaches it on the
// outgoing request (so the coordinator merges against it instead of
// treating the call as a blind write) and updates it from the response;
// get() updates it from a found=true response. This makes repeated
// put()/get() calls against the same DriftClient instance behave like a
// real client maintaining causal context across calls, rather than every
// call being an independent blind write. See put()'s/get()'s own comments
// for exactly which responses are trusted enough to update the cache.
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
        driftstore::VectorClock context;
    };

    struct GetResult {
        bool found = false;
        std::string value;
        int32_t responses = 0;
        driftstore::VectorClock context;
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
        // Attach whatever context we last saw for this key, so the
        // coordinator merges against it (decision A1) instead of treating
        // this as a blind write. No cached entry -> req.context() stays
        // default-empty, which the server currently can't distinguish
        // from "no context sent" anyway (VectorClock isn't declared
        // `optional` in the proto, so proto3 gives no real field-presence
        // tracking here) -- so this is safe either way.
        auto cached = last_context_.find(key);
        if (cached != last_context_.end()) {
            *req.mutable_context() = cached->second;
        }

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
                out.context = resp.context();
                // Only cache a response that actually carries a real
                // clock. Several of Put's early-refusal paths (self-
                // removed, forward-loop-prevented, preference list
                // smaller than W) return before ever touching
                // PutResponse.context, leaving it at its proto3 default
                // (empty). Caching that would silently wipe out a real
                // cached context on an unrelated refusal. A response that
                // actually ran coordinatePut's fan-out always has at
                // least one counter (buildNewClock unconditionally
                // increments the coordinator's own axis) -- including
                // the acks < W quorum-miss case, which PROGRESS.md's D3
                // decision explicitly calls safe to cache from (context
                // can only ever be honestly stale, never wrong about
                // causality).
                if (resp.context().counters().size() > 0) {
                    last_context_[key] = resp.context();
                }
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
                out.context = resp.context();
                // Get only ever populates context on the found=true path
                // (see node_kv.cpp's Get handler) -- unlike Put, there's
                // no ambiguous empty-default-from-refusal case to guard
                // against here, so found is a sufficient signal on its own.
                if (resp.found()) {
                    last_context_[key] = resp.context();
                }
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
    // Last-seen VectorClock per key, populated from put()/get() responses
    // (see put()'s and get()'s comments for exactly when each updates
    // this). Not thread-safe -- DriftClient isn't used concurrently
    // anywhere in this codebase today; add a mutex here if that changes.
    std::unordered_map<std::string, driftstore::VectorClock> last_context_;
};