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
    bool status_mode = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kTargetPrefix[] = "--target=";
        static constexpr char kSelfPrefix[] = "--self=";
        if (arg.rfind(kTargetPrefix, 0) == 0) {
            target = arg.substr(sizeof(kTargetPrefix) - 1);
        } else if (arg.rfind(kSelfPrefix, 0) == 0) {
            self = arg.substr(sizeof(kSelfPrefix) - 1);
        } else if (arg == "--status") {
            status_mode = true;
        }
    }

    if (target.empty() || (!status_mode && self.empty())) {
        std::fprintf(stderr,
            "usage: %s --target=<address> --self=<node_id>\n"
            "       %s --target=<address> --status\n",
            argv[0], argv[0]);
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
            std::printf("node=%s %s\n", response.node_id().c_str(), response.table_dump().c_str());
            return 0;
        }
        std::fprintf(stderr, "status query failed: target=%s error=%s\n",
                     target.c_str(), status.error_message().c_str());
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