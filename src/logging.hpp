#pragma once
#include <chrono>
#include <cstdio>
#include <ctime>
#include <string>

enum class EventType {
    PING_SENT,
    PING_RECEIVED,
    PING_SUCCEEDED,
    PING_FAILED,
    NODE_INIT,
    GOSSIP_SENT,
    GOSSIP_RECEIVED,
    GOSSIP_SUCCEEDED,
    GOSSIP_FAILED, // this should mean that the gossip was attempted and it failed (e.g. peer was unreachable, connection refused, process dies midway)
    GOSSIP_NO_PEERS, // this should be distinct from GOSSIP_FAILED where failure case is no peers to even contact
    GOSSIP_MERGED,
    BOOTSTRAP_INIT,
    BOOTSTRAP_SUCCEEDED,
    BOOTSTRAP_FAILED,
    REMOVE_INIT,
    REMOVE_FAILED,
    REMOVE_SUCCEEDED,
    NODE_REBOOTED,
    PROBE_SENT,
    PROBE_SUCCEEDED,
    PROBE_FAILED,
    GET_INIT,
    GET_SUCCEEDED,
    GET_FAILED,
    PUT_INIT,
    PUT_SUCCEEDED,
    PUT_FAILED,
};

inline const char* toString(EventType e) {
    switch (e) {
        case EventType::PING_SENT: return "PING_SENT";
        case EventType::PING_RECEIVED: return "PING_RECEIVED";
        case EventType::PING_SUCCEEDED: return "PING_SUCCEEDED";
        case EventType::PING_FAILED: return "PING_FAILED";
        case EventType::NODE_INIT: return "NODE_INIT";
        case EventType::GOSSIP_SENT: return "GOSSIP_SENT";
        case EventType::GOSSIP_RECEIVED: return "GOSSIP_RECEIVED";
        case EventType::GOSSIP_SUCCEEDED: return "GOSSIP_SUCCEEDED";
        case EventType::GOSSIP_FAILED: return "GOSSIP_FAILED";
        case EventType::GOSSIP_MERGED: return "GOSSIP_MERGED";
        case EventType::GOSSIP_NO_PEERS: return "GOSSIP_NO_PEERS";
        case EventType::BOOTSTRAP_INIT: return "BOOTSTRAP_INIT";
        case EventType::BOOTSTRAP_SUCCEEDED: return "BOOTSTRAP_SUCCEEDED";
        case EventType::BOOTSTRAP_FAILED: return "BOOTSTRAP_FAILED";
        case EventType::REMOVE_INIT: return "REMOVE_INIT";
        case EventType::REMOVE_FAILED: return "REMOVE_FAILED";
        case EventType::REMOVE_SUCCEEDED: return "REMOVE_SUCCEEDED";
        case EventType::NODE_REBOOTED: return "NODE_REBOOTED";
        case EventType::PROBE_SENT: return "PROBE_SENT";
        case EventType::PROBE_SUCCEEDED: return "PROBE_SUCCEEDED";
        case EventType::PROBE_FAILED: return "PROBE_FAILED";
        case EventType::GET_INIT: return "GET_INIT";
        case EventType::GET_SUCCEEDED: return "GET_SUCCEEDED";
        case EventType::GET_FAILED: return "GET_FAILED";
        case EventType::PUT_INIT: return "PUT_INIT";
        case EventType::PUT_SUCCEEDED: return "PUT_SUCCEEDED";
        case EventType::PUT_FAILED: return "PUT_FAILED";
    }
}

inline std::string isoTimestampNow() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    std::time_t t = system_clock::to_time_t(now);
    std::tm tm_utc{};
    gmtime_r(&t, &tm_utc);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                  tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
                  tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec, (int)ms.count());
    return std::string(buf);
}

inline void logEvent(EventType event, const std::string& node_id, const std::string& extra_kv = "") {
    std::fprintf(stderr, "[%s] node=%s event=%s%s%s\n",
        isoTimestampNow().c_str(), // write the timestamp of the event
        node_id.c_str(), // write the node_id initiating the event
        toString(event), // display what event is occurring
        extra_kv.empty() ? "" : " ", // if message is empty then add nothing, otherwise add a space
        extra_kv.c_str()); // add the key:value
}