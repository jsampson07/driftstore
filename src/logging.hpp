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
};

inline const char* toString(EventType e) {
    switch (e) {
        case EventType::PING_SENT: return "PING_SENT";
        case EventType::PING_RECEIVED: return "PING_RECEIVED";
        case EventType::PING_SUCCEEDED: return "PING_SUCCEEDED";
        case EventType::PING_FAILED: return "PING_FAILED";
        case EventType::NODE_INIT: return "NODE_INIT";
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

