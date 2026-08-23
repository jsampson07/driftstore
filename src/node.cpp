#include "driftstore.grpc.pb.h"
#include "logging.hpp"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <memory>
#include <string>

class NodeServiceImpl final : public driftstore::DriftStoreNode::Service {
public:
    explicit NodeServiceImpl(std::string node_id) : node_id_(std::move(node_id)) {}

    grpc::Status Ping(grpc::ServerContext* /*context*/,
                      const driftstore::PingRequest* request,
                      driftstore::PingResponse* response) override {
        logEvent(EventType::PING_RECEIVED, node_id_,
                 "sender=" + request->sender_node_id());
        response->set_responder_node_id(node_id_);
        return grpc::Status::OK;
    }

private:
    std::string node_id_;
};

int main(int argc, char** argv) {
    std::string listen;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        static constexpr char kPrefix[] = "--listen=";
        if (arg.rfind(kPrefix, 0) == 0) {
            listen = arg.substr(sizeof(kPrefix) - 1);
        }
    }
    if (listen.empty()) {
        std::fprintf(stderr, "usage: %s --listen=<address>\n", argv[0]);
        return 1;
    }

    const std::string& node_id = listen;
    logEvent(EventType::NODE_INIT, node_id);

    NodeServiceImpl service(node_id);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait();
    return 0;
}
