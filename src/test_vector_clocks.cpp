#include "vector_clock.hpp"
#include <cassert>
#include <cstdio>

void testBuildClock() {
    driftstore::VectorClock a;
    driftstore::VectorClock b;
    driftstore::VectorClock merged;
    (*a.mutable_counters())["node_A"] = 3;
    (*b.mutable_counters())["node_A"] = 1;
    merged = buildNewClock(a, b, "node_A");
    assert(merged.counters().size() == 1);
    assert(merged.counters().find("node_A")->second == 4);

    printf("test simple merge and increment: PASS\n");

    (*b.mutable_counters())["node_B"] = 3;
    merged = buildNewClock(a, b, "node_A");
    assert(merged.counters().size() == 2);
    assert(merged.counters().find("node_A")->second == 4);
    assert(merged.counters().find("node_B")->second == 3);

    printf("test two-entry merge: PASS\n");

    driftstore::VectorClock c;
    driftstore::VectorClock d;
    (*c.mutable_counters())["node_A"] = 4;
    (*c.mutable_counters())["node_B"] = 2;

    (*d.mutable_counters())["node_A"] = 4;
    (*d.mutable_counters())["node_B"] = 2;

    merged = buildNewClock(c, d, "node_B");
    assert(merged.counters().size() == 2);
    assert(merged.counters().find("node_A")->second == 4);
    assert(merged.counters().find("node_B")->second == 3);

    printf("test equal clocks: PASS\n");
}

int main() {
    testBuildClock();
    return 0;
}