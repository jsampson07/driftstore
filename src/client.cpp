#include "driftstore.grpc.pb.h"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <memory>
#include <string>

int main(int argc, char** argv) {
    std::string target;
    std::string self;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kTargetPrefix[] = "--target=";
        static constexpr char kSelfPrefix[] = "--self=";
        if (arg.rfind(kTargetPrefix, 0) == 0) {
            target = arg.substr(sizeof(kTargetPrefix) - 1);
        } else if (arg.rfind(kSelfPrefix, 0) == 0) {
            self = arg.substr(sizeof(kSelfPrefix) - 1);
        }
    }
    if (target.empty() || self.empty()) {
        std::fprintf(stderr, "usage: %s --target=<address> --self=<node_id>\n", argv[0]);
        return 1;
    }

    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    std::unique_ptr<driftstore::DriftStoreNode::Stub> stub =
        driftstore::DriftStoreNode::NewStub(channel);

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
    return 1;
}
