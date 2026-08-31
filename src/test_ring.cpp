#include "ring.hpp"
#include "logging.hpp"
#include <cassert>
#include <cstdio>

void testKillAndReboot() {
    driftstore::MembershipTable local;
    std::map<uint64_t, std::string> ring;

    driftstore::MembershipTable incoming;
    driftstore::MembershipEntry peer_entry;

    peer_entry.set_writer_id("peerB");
    peer_entry.set_address("peerB");
    peer_entry.set_status(driftstore::UP);
    peer_entry.set_last_updated(1000);
    uint64_t t0 = mix64(fnv1a64("peerB:0"));
    uint64_t t1 = mix64(fnv1a64("peerB:1"));
    peer_entry.add_tokens(t0);
    peer_entry.add_tokens(t1);
    (* incoming.mutable_entries())["peerB"] = peer_entry;

    mergeInto(local, incoming, "testcaller", ring);

    assert(local.entries().at("peerB").status() == driftstore::UP);
    assert(ring.size() == 2);
    assert(ring.at(t0) == "peerB" && ring.at(t1) == "peerB");

    peer_entry.set_status(driftstore::REMOVED);
    peer_entry.set_last_updated(2000);
    peer_entry.clear_tokens();

    (* incoming.mutable_entries())["peerB"] = peer_entry;

    mergeInto(local, incoming, "testcaller", ring);

    assert(local.entries().at("peerB").status() == driftstore::REMOVED);
    assert(ring.size() == 0);
    assert(ring.count(t0) == 0 && ring.count(t1) == 0);

    peer_entry.set_writer_id("peerA");
    peer_entry.set_address("peerB");
    peer_entry.set_status(driftstore::UP);
    peer_entry.set_last_updated(3000);
    t0 = mix64(fnv1a64("peerB:0"));
    t1 = mix64(fnv1a64("peerB:1"));
    peer_entry.add_tokens(t0);
    peer_entry.add_tokens(t1);
    (* incoming.mutable_entries())["peerB"] = peer_entry;

    mergeInto(local, incoming, "testcaller", ring);

    assert(local.entries().at("peerB").status() == driftstore::UP);
    assert(ring.size() == 2);
    assert(ring.at(t0) == "peerB" && ring.at(t1) == "peerB");

    printf("test remove node (ring effect) and reboot (ring effect): PASS\n");
}

void testRingAndPrefList() {
    driftstore::MembershipTable local;
    std::map<uint64_t, std::string> ring;
    
    driftstore::MembershipTable incoming;
    driftstore::MembershipEntry peer_entry;

    peer_entry.set_writer_id("peerB");
    peer_entry.set_address("peerB");
    peer_entry.set_status(driftstore::UP);
    peer_entry.set_last_updated(1000);
    uint64_t t0 = mix64(fnv1a64("peerB:0"));
    uint64_t t1 = mix64(fnv1a64("peerB:1"));
    peer_entry.add_tokens(t0);
    peer_entry.add_tokens(t1);
    (* incoming.mutable_entries())["peerB"] = peer_entry;

    mergeInto(local, incoming, "testcaller", ring);
    // this should populate local table with peerB entry and should add peerB to "ring"
    assert(local.entries().count("peerB") == 1); // only peerB is in 'local' table
    assert(local.entries().at("peerB").status() == driftstore::UP); // peerB is UP
    assert(ring.size() == 2); // 2 tokens for peerB
    assert(ring.at(t0) == "peerB");
    assert(ring.at(t1) == "peerB");

    auto prefs = preferenceList(ring, t0, 1);
    printf("%s\n", prefs[0].c_str());
    assert(prefs.size() == 1 && prefs[0] == "peerB");

    printf("test 1 (new-node ring insert): PASS\n");
}

void testReachabilityPredicate() {
    // Think of various cases to test for reachability with predicate passed in
    // What happens if we have nodes, and then one of them that would be in the preference_list is unreachable?
    // What happens if just add nodes? Make sure they are marked as "reachable" --> initial value
    // So overall, make sure that reachability is actually checked and different results that come from this.
    std::map<uint64_t, std::string> ring = {
        {10, "A"}, {20, "B"}, {30, "A"}, {40, "C"}, {50, "B"}, {60, "C"}
    };
    uint64_t key_hash = 5;
    // When we call excludeB, inside of preferenceList (where we pass in 'candidate')
    // if node is B, then return false --> indicating that this is UNreachable
    auto excludeB = [](const std::string& node_id) {
        return node_id != "B";
    };
    // Generate preferenceList and see if the "reachability" status is enforced (properly excluding node B)
    auto prefs1 = preferenceList(ring, key_hash, /*N=*/2, excludeB);
    for (const auto& node_id : prefs1) {
        assert(node_id != "B");
    }
    printf("test predicate case 1 (exclude one node, walk continues past it): PASS\n");

    auto onlyA = [](const std::string& node_id) {
        return node_id == "A";
    };
    std::vector<std::string> prefs2 = preferenceList(ring, key_hash, 2, onlyA);
    assert(prefs2.size() == 1);
    assert(prefs2[0] == "A");
    printf("test predicate case 2 (short list when predicate excludes below N): PASS\n");
}

int main() {
    testRingAndPrefList();
    testKillAndReboot();
    testReachabilityPredicate();

    return 0;
}