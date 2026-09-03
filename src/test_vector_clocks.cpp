#include "vector_clock.hpp"
#include <cassert>
#include <cstdio>

void testResolveLWW() {
    driftstore::VectorClock a;
    driftstore::VectorClock b;
    (*a.mutable_counters())["node_A"] = 3;
    (*b.mutable_counters())["node_A"] = 3;
    a.set_last_updated(1);
    b.set_last_updated(1);
    a.set_writer_id("node_A");
    b.set_writer_id("node_B");
    std::vector<VersionedValue> input;

    VersionedValue val1;
    VersionedValue val2;
    val1.value = "test";
    val2.value = "test";
    val1.clock = a;
    val2.clock = b;
    input.push_back(val1);
    input.push_back(val2);

    const VersionedValue& res = resolveLWW(input);

    assert(res.clock.writer_id() == "node_B");
    assert(res.clock.last_updated() == 1);

    printf("test resolveLWW with timestamp tiebreak but writer winner: PASS\n");

    driftstore::VectorClock c;
    driftstore::VectorClock d;
    (*c.mutable_counters())["node_B"] = 3;
    (*d.mutable_counters())["node_B"] = 3;
    c.set_last_updated(1);
    d.set_last_updated(1);
    c.set_writer_id("node_B");
    d.set_writer_id("node_B");
    std::vector<VersionedValue> input2;

    VersionedValue val3;
    VersionedValue val4;
    val3.value = "test";
    val4.value = "test_will_be_excluded";
    val3.clock = c;
    val4.clock = d;
    input2.push_back(val3);
    input2.push_back(val4);

    const VersionedValue& res2 = resolveLWW(input2);

    assert(&res2 == &input2[0]);
    assert(res2.value == "test");
    assert(res2.clock.writer_id() == "node_B");
    assert(res2.clock.last_updated() == 1);

    printf("test resolveLWW with timestamp tiebreak + writer tie: PASS\n");

    driftstore::VectorClock e;
    driftstore::VectorClock f;
    (*e.mutable_counters())["node_B"] = 3;
    (*f.mutable_counters())["node_B"] = 3;
    e.set_last_updated(1);
    f.set_last_updated(5);
    e.set_writer_id("node_A");
    f.set_writer_id("node_B");
    std::vector<VersionedValue> input3;

    VersionedValue val5;
    VersionedValue val6;
    val5.value = "test_WILL_LOSE";
    val6.value = "test_WILL_WIN";
    val5.clock = e;
    val6.clock = f;
    input3.push_back(val5);
    input3.push_back(val6);

    const VersionedValue& res3 = resolveLWW(input3);

    assert(&res3 == &input3[1]);
    assert(res3.value == "test_WILL_WIN");
    assert(res3.clock.last_updated() == 5);

    printf("test last_updated win (common case): PASS\n");
}

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
    testResolveLWW();
    testBuildClock();
    return 0;
}