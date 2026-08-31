#include "driftstore.grpc.pb.h"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>

int main(int argc, char** argv) {
    std::string target;
    std::string self;
    std::string remove_target;
    std::string rwrite_kv;
    std::string rread_key;
    bool status_mode = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kTargetPrefix[] = "--target=";
        static constexpr char kSelfPrefix[] = "--self=";
        static constexpr char kRemovePrefix[] = "--remove=";
        static constexpr char kRWritePrefix[] = "--replicate-write=";
        static constexpr char kRReadPrefix[] = "--replicate-read=";
        if (arg.rfind(kTargetPrefix, 0) == 0) {
            target = arg.substr(sizeof(kTargetPrefix) - 1);
        } else if (arg.rfind(kSelfPrefix, 0) == 0) {
            self = arg.substr(sizeof(kSelfPrefix) - 1);
        } else if (arg.rfind(kRemovePrefix, 0) == 0) {
            remove_target = arg.substr(sizeof(kRemovePrefix) - 1);
        } else if (arg.rfind(kRWritePrefix, 0) == 0) {
            rwrite_kv = arg.substr(sizeof(kRWritePrefix) - 1);
        } else if (arg.rfind(kRReadPrefix, 0) == 0) {
            rread_key = arg.substr(sizeof(kRReadPrefix) - 1);
        } else if (arg == "--status") {
            status_mode = true;
        }
    }

    const bool remove_mode = !remove_target.empty();
    const bool rwrite_mode = !rwrite_kv.empty();
    const bool rread_mode = !rread_key.empty();

    if (target.empty() ||
        (!status_mode && !remove_mode && !rwrite_mode && !rread_mode && self.empty())) {
        std::fprintf(stderr,
            "usage: %s --target=<address> --self=<node_id>\n"
            "       %s --target=<address> --status\n"
            "       %s --target=<address> --remove=<node_id>\n"
            "       %s --target=<address> --replicate-write=<key>=<value>\n"
            "       %s --target=<address> --replicate-read=<key>\n",
            argv[0], argv[0], argv[0], argv[0], argv[0]);
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

        driftstore::ReplicateWriteRequest request;
        request.set_key(key);
        request.set_value(value);
        driftstore::ReplicateWriteResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        grpc::Status status = stub->ReplicateWrite(&context, request, &response);

        if (status.ok()) {
            std::printf("replicate-write key=%s value=%s success=%s\n",
                        key.c_str(), value.c_str(), response.success() ? "true" : "false");
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
            std::printf("replicate-read key=%s found=%s value=%s\n",
                        rread_key.c_str(), response.found() ? "true" : "false",
                        response.found() ? response.value().c_str() : "");
            return 0;
        }
        std::fprintf(stderr, "replicate-read RPC failed: target=%s key=%s error=%s\n",
                     target.c_str(), rread_key.c_str(), status.error_message().c_str());
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