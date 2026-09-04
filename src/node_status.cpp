#include "node_service.hpp"
#include "logging.hpp"

grpc::Status NodeServiceImpl::Ping(grpc::ServerContext* /*context*/,
                  const driftstore::PingRequest* request,
                  driftstore::PingResponse* response) {
    logEvent(EventType::PING_RECEIVED, node_id_,
    "sender=" + request->sender_node_id());
    response->set_responder_node_id(node_id_);
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::GetStatus(grpc::ServerContext* /*context*/,
                       const driftstore::StatusRequest* /*request*/,
                       driftstore::StatusResponse* response) {
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
