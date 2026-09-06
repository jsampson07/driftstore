#include "driftstore.grpc.pb.h"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// Human-readable label for WriteOutcome, so --replicate-write's output can
// distinguish STORED from ALREADY_CURRENT without the caller having to know
// the underlying enum's integer values. Mirrors logging.hpp's toString(EventType)
// pattern rather than relying on protobuf's generated _Name() helper, to stay
// consistent with how this codebase already labels its other enums.
const char* outcomeToString(driftstore::WriteOutcome outcome) {
    switch (outcome) {
        case driftstore::WriteOutcome::STORED: return "STORED";
        case driftstore::WriteOutcome::ALREADY_CURRENT: return "ALREADY_CURRENT";
    }
    return "UNKNOWN";
}

// Renders a VectorClock's counters map as "node1:count1,node2:count2,...",
// the same shape --clock= accepts as input -- so a --replicate-read's output
// can be fed back into a later --clock= argument by eye. Sorted by node_id
// since protobuf map iteration order is unspecified and un-sorted output
// would make test assertions non-deterministic.
std::string clockToString(const driftstore::VectorClock& clock) {
    std::vector<std::pair<std::string, uint64_t>> entries(
        clock.counters().begin(), clock.counters().end());
    std::sort(entries.begin(), entries.end());
    std::string out;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        if (i > 0) out += ",";
        out += entries[i].first + ":" + std::to_string(entries[i].second);
    }
    return out;
}

// Parses a --clock= argument of the form "node1:count1,node2:count2,..."
// into a VectorClock's counters map. An empty `raw` is valid -- it means no
// --clock was given, i.e. a blind replicate-write with an empty clock
// (dominated by literally anything already stored). Returns false on any
// malformed pair so the caller can reject the whole command rather than
// silently sending a partially-parsed clock.
bool parseClockArg(const std::string& raw, driftstore::VectorClock* clock) {
    if (raw.empty()) {
        return true;
    }
    std::string remaining = raw;
    while (!remaining.empty()) {
        const auto comma = remaining.find(',');
        const std::string pair_str = (comma == std::string::npos) ? remaining : remaining.substr(0, comma);

        const auto colon = pair_str.find(':');
        if (colon == std::string::npos || colon == 0 || colon == pair_str.size() - 1) {
            return false; // missing ':', or empty node_id / empty counter
        }
        const std::string node_id = pair_str.substr(0, colon);
        const std::string count_str = pair_str.substr(colon + 1);
        for (char c : count_str) {
            if (!std::isdigit(static_cast<unsigned char>(c))) {
                return false;
            }
        }
        (*clock->mutable_counters())[node_id] = std::stoull(count_str);

        if (comma == std::string::npos) {
            break;
        }
        remaining = remaining.substr(comma + 1);
    }
    return true;
}

int main(int argc, char** argv) {
    std::string target;
    std::string self;
    std::string remove_target;
    std::string rwrite_kv;
    std::string rwrite_clock_str; // optional, only meaningful with --replicate-write
    std::string rread_key;
    std::string put_kv;
    std::string get_key;
    bool status_mode = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kTargetPrefix[] = "--target=";
        static constexpr char kSelfPrefix[] = "--self=";
        static constexpr char kRemovePrefix[] = "--remove=";
        static constexpr char kRWritePrefix[] = "--replicate-write=";
        static constexpr char kClockPrefix[] = "--clock=";
        static constexpr char kRReadPrefix[] = "--replicate-read=";
        static constexpr char kPutPrefix[] = "--put=";
        static constexpr char kGetPrefix[] = "--get=";
        if (arg.rfind(kTargetPrefix, 0) == 0) {
            target = arg.substr(sizeof(kTargetPrefix) - 1);
        } else if (arg.rfind(kSelfPrefix, 0) == 0) {
            self = arg.substr(sizeof(kSelfPrefix) - 1);
        } else if (arg.rfind(kRemovePrefix, 0) == 0) {
            remove_target = arg.substr(sizeof(kRemovePrefix) - 1);
        } else if (arg.rfind(kRWritePrefix, 0) == 0) {
            rwrite_kv = arg.substr(sizeof(kRWritePrefix) - 1);
        } else if (arg.rfind(kClockPrefix, 0) == 0) {
            rwrite_clock_str = arg.substr(sizeof(kClockPrefix) - 1);
        } else if (arg.rfind(kRReadPrefix, 0) == 0) {
            rread_key = arg.substr(sizeof(kRReadPrefix) - 1);
        } else if (arg.rfind(kPutPrefix, 0) == 0) {
            put_kv = arg.substr(sizeof(kPutPrefix) - 1);
        } else if (arg.rfind(kGetPrefix, 0) == 0) {
            get_key = arg.substr(sizeof(kGetPrefix) - 1);
        } else if (arg == "--status") {
            status_mode = true;
        }
    }

    const bool remove_mode = !remove_target.empty();
    const bool rwrite_mode = !rwrite_kv.empty();
    const bool rread_mode = !rread_key.empty();
    const bool put_mode = !put_kv.empty();
    const bool get_mode = !get_key.empty();

    if (target.empty() ||
        (!status_mode && !remove_mode && !rwrite_mode && !rread_mode && !put_mode && !get_mode && self.empty())) {
        std::fprintf(stderr,
            "usage: %s --target=<address> --self=<node_id>\n"
            "       %s --target=<address> --status\n"
            "       %s --target=<address> --remove=<node_id>\n"
            "       %s --target=<address> --replicate-write=<key>=<value> [--clock=<node_id>:<counter>[,<node_id>:<counter>...]]\n"
            "       %s --target=<address> --replicate-read=<key>\n"
            "       %s --target=<address> --put=<key>=<value>\n"
            "       %s --target=<address> --get=<key>\n",
            argv[0], argv[0], argv[0], argv[0], argv[0], argv[0], argv[0]);
        return 1;
    }

    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
        driftstore::DriftStoreNode::NewStub(channel);

    if (status_mode) {
        driftstore::StatusRequest request;
        driftstore::StatusResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        grpc::Status status = stub->GetStatus(&context, request, &response);

        if (status.ok()) {
            std::printf("node=%s %s\n%s\n", response.node_id().c_str(), response.table_dump().c_str(), response.ring_dump().c_str());
            return 0;
        }
        std::fprintf(stderr, "status query failed: target=%s error=%s\n",
                     target.c_str(), status.error_message().c_str());
        return 1;
    }

    if (remove_mode) {
        driftstore::RemoveRequest request;
        request.set_removed_node_id(remove_target);
        driftstore::RemoveResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        logEvent(EventType::REMOVE_INIT, remove_target);
        grpc::Status status = stub->RemoveNode(&context, request, &response);

        if (status.ok()) {
            std::printf("remove target=%s accepted=%s\n",
                        remove_target.c_str(), response.accepted() ? "true" : "false");
            return response.accepted() ? 0 : 1;
        }
        logEvent(EventType::REMOVE_FAILED, remove_target);
        std::fprintf(stderr, "remove RPC failed: target=%s removed_node_id=%s error=%s\n",
                     target.c_str(), remove_target.c_str(), status.error_message().c_str());
        return 1;
    }

    if (rwrite_mode) {
        auto eq = rwrite_kv.find('=');
        if (eq == std::string::npos) {
            std::fprintf(stderr, "usage: --replicate-write=<key>=<value> (no '=' found in \"%s\")\n",
                         rwrite_kv.c_str());
            return 1;
        }
        const std::string key = rwrite_kv.substr(0, eq);
        const std::string value = rwrite_kv.substr(eq + 1);

        driftstore::VectorClock clock;
        if (!parseClockArg(rwrite_clock_str, &clock)) {
            std::fprintf(stderr,
                "usage: --clock=<node_id>:<counter>[,<node_id>:<counter>...] (malformed: \"%s\")\n",
                rwrite_clock_str.c_str());
            return 1;
        }

        driftstore::ReplicateWriteRequest request;
        request.set_key(key);
        request.set_value(value);
        *request.mutable_vector_clock() = clock;
        driftstore::ReplicateWriteResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        grpc::Status status = stub->ReplicateWrite(&context, request, &response);

        if (status.ok()) {
            std::printf("replicate-write key=%s value=%s success=%s outcome=%s\n",
                        key.c_str(), value.c_str(), response.success() ? "true" : "false",
                        outcomeToString(response.outcome()));
            return response.success() ? 0 : 1;
        }
        std::fprintf(stderr, "replicate-write RPC failed: target=%s key=%s error=%s\n",
                     target.c_str(), key.c_str(), status.error_message().c_str());
        return 1;
    }

    if (rread_mode) {
        driftstore::ReplicateReadRequest request;
        request.set_key(rread_key);
        driftstore::ReplicateReadResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        grpc::Status status = stub->ReplicateRead(&context, request, &response);

        if (status.ok()) {
            std::printf("replicate-read key=%s found=%s value=%s clock=%s\n",
                        rread_key.c_str(), response.found() ? "true" : "false",
                        response.found() ? response.value().c_str() : "",
                        response.found() ? clockToString(response.vector_clock()).c_str() : "");
            return 0;
        }
        std::fprintf(stderr, "replicate-read RPC failed: target=%s key=%s error=%s\n",
                     target.c_str(), rread_key.c_str(), status.error_message().c_str());
        return 1;
    }

    if (put_mode) {
        auto eq = put_kv.find('=');
        if (eq == std::string::npos) {
            std::fprintf(stderr, "usage: --put=<key>=<value> (no '=' found in \"%s\")\n",
                         put_kv.c_str());
            return 1;
        }
        const std::string key = put_kv.substr(0, eq);
        const std::string value = put_kv.substr(eq + 1);

        driftstore::PutRequest request;
        request.set_key(key);
        request.set_value(value);
        driftstore::PutResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        grpc::Status status = stub->Put(&context, request, &response);

        if (status.ok()) {
            std::printf("put key=%s value=%s success=%s acks=%s\n",
                        key.c_str(), value.c_str(), response.success() ? "true" : "false",
                        std::to_string(response.acks()).c_str());
            return response.success() ? 0 : 1;
        }
        std::fprintf(stderr, "put RPC failed: target=%s key=%s error=%s\n",
                     target.c_str(), key.c_str(), status.error_message().c_str());
        return 1;
    }

    if (get_mode) {
        driftstore::GetRequest request;
        request.set_key(get_key);
        driftstore::GetResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        grpc::Status status = stub->Get(&context, request, &response);

        if (status.ok()) {
            std::printf("get key=%s found=%s value=%s responses=%s\n",
                        get_key.c_str(), response.found() ? "true" : "false",
                        response.found() ? response.value().c_str() : "",
                        std::to_string(response.responses()).c_str());
            return 0;
        }
        std::fprintf(stderr, "get RPC failed: target=%s key=%s error=%s\n",
                     target.c_str(), get_key.c_str(), status.error_message().c_str());
        return 1;
    }

    logEvent(EventType::PING_SENT, self, "target=" + target);

    driftstore::PingRequest request;
    request.set_sender_node_id(self);
    driftstore::PingResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub->Ping(&context, request, &response);

    if (status.ok()) {
        logEvent(EventType::PING_SUCCEEDED, self,
                 "responder=" + response.responder_node_id());
        return 0;
    }

    logEvent(EventType::PING_FAILED, self,
             "target=" + target + " error=" + status.error_message());
    return -1;
}