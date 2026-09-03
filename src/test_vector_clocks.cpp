#include "vector_clock.hpp"
#include <cassert>
#include <cstdio>

void testCompareVectorClocks() {
    // EQUAL: identical clocks
    driftstore::VectorClock a;
    driftstore::VectorClock b;
    (*a.mutable_counters())["node_A"] = 2;
    (*a.mutable_counters())["node_B"] = 5;
    (*b.mutable_counters())["node_A"] = 2;
    (*b.mutable_counters())["node_B"] = 5;
    assert(compareVectorClocks(a, b) == ClockComparison::EQUAL);
    printf("test compareVectorClocks EQUAL: PASS\n");

    // DOMINATES / DOMINATED: strictly ahead on one axis, tied elsewhere
    driftstore::VectorClock c;
    driftstore::VectorClock d;
    (*c.mutable_counters())["node_A"] = 3;
    (*c.mutable_counters())["node_B"] = 5;
    (*d.mutable_counters())["node_A"] = 2;
    (*d.mutable_counters())["node_B"] = 5;
    assert(compareVectorClocks(c, d) == ClockComparison::DOMINATES);
    assert(compareVectorClocks(d, c) == ClockComparison::DOMINATED);
    printf("test compareVectorClocks DOMINATES/DOMINATED: PASS\n");

    // CONCURRENT: each side ahead on a different axis
    driftstore::VectorClock e;
    driftstore::VectorClock f;
    (*e.mutable_counters())["node_A"] = 3;
    (*e.mutable_counters())["node_B"] = 1;
    (*f.mutable_counters())["node_A"] = 1;
    (*f.mutable_counters())["node_B"] = 3;
    assert(compareVectorClocks(e, f) == ClockComparison::CONCURRENT);
    assert(compareVectorClocks(f, e) == ClockComparison::CONCURRENT);
    printf("test compareVectorClocks CONCURRENT: PASS\n");

    // node_id present in one clock, absent in the other -> treated as 0,
    // not "incomparable"
    driftstore::VectorClock g;
    driftstore::VectorClock h;
    (*g.mutable_counters())["node_A"] = 1;
    (*h.mutable_counters())["node_A"] = 1;
    (*h.mutable_counters())["node_C"] = 4;
    assert(compareVectorClocks(g, h) == ClockComparison::DOMINATED);
    assert(compareVectorClocks(h, g) == ClockComparison::DOMINATES);
    printf("test compareVectorClocks missing-node-treated-as-zero: PASS\n");

    // explicit zero-valued counter entry behaves identically to a missing one
    driftstore::VectorClock i;
    driftstore::VectorClock j;
    (*i.mutable_counters())["node_A"] = 1;
    (*i.mutable_counters())["node_C"] = 0;
    (*j.mutable_counters())["node_A"] = 1;
    (*j.mutable_counters())["node_C"] = 4;
    assert(compareVectorClocks(i, j) == ClockComparison::DOMINATED);
    printf("test compareVectorClocks explicit-zero-entry: PASS\n");
}

void testMergeClocks() {
    // disjoint node sets -> union
    driftstore::VectorClock a;
    driftstore::VectorClock b;
    (*a.mutable_counters())["node_A"] = 2;
    (*b.mutable_counters())["node_B"] = 5;
    driftstore::VectorClock merged = mergeClocks(a, b);
    assert(merged.counters().size() == 2);
    assert(merged.counters().find("node_A")->second == 2);
    assert(merged.counters().find("node_B")->second == 5);
    printf("test mergeClocks disjoint union: PASS\n");

    // overlapping node -> component-wise max, not sum, not overwrite
    driftstore::VectorClock c;
    driftstore::VectorClock d;
    (*c.mutable_counters())["node_A"] = 7;
    (*c.mutable_counters())["node_B"] = 1;
    (*d.mutable_counters())["node_A"] = 3;
    (*d.mutable_counters())["node_B"] = 9;
    driftstore::VectorClock merged2 = mergeClocks(c, d);
    assert(merged2.counters().size() == 2);
    assert(merged2.counters().find("node_A")->second == 7);
    assert(merged2.counters().find("node_B")->second == 9);
    printf("test mergeClocks component-wise max: PASS\n");

    // explicit zero-valued entry, present on only one side, must survive
    // the merge — regression check for the b_cnt>0 guard you removed
    driftstore::VectorClock e;
    driftstore::VectorClock f;
    (*e.mutable_counters())["node_A"] = 0;
    (*f.mutable_counters())["node_B"] = 4;
    driftstore::VectorClock merged3 = mergeClocks(e, f);
    assert(merged3.counters().size() == 2);
    assert(merged3.counters().find("node_A")->second == 0);
    assert(merged3.counters().find("node_B")->second == 4);
    printf("test mergeClocks explicit-zero-entry preserved: PASS\n");

    // mergeClocks never touches last_updated — that's buildNewClock's job
    driftstore::VectorClock g;
    driftstore::VectorClock h;
    g.set_last_updated(123);
    h.set_last_updated(456);
    (*g.mutable_counters())["node_A"] = 1;
    driftstore::VectorClock merged4 = mergeClocks(g, h);
    assert(merged4.last_updated() == 0);
    printf("test mergeClocks leaves last_updated untouched: PASS\n");
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

void testComputeFrontier() {
    // single entry: trivially survives
    VersionedValue v1;
    v1.value = "only";
    (*v1.clock.mutable_counters())["node_A"] = 1;
    std::vector<VersionedValue> single = {v1};
    std::vector<VersionedValue> f1 = computeFrontier(single);
    assert(f1.size() == 1);
    assert(f1[0].value == "only");
    printf("test computeFrontier single entry: PASS\n");

    // dominated entry gets discarded
    VersionedValue v2, v3;
    v2.value = "stale";
    (*v2.clock.mutable_counters())["node_A"] = 1;
    v3.value = "fresh";
    (*v3.clock.mutable_counters())["node_A"] = 2;
    std::vector<VersionedValue> dom = {v2, v3};
    std::vector<VersionedValue> f2 = computeFrontier(dom);
    assert(f2.size() == 1);
    assert(f2[0].value == "fresh");
    printf("test computeFrontier drops dominated entry: PASS\n");

    // genuine 3-way concurrent set: all three must survive — this is the
    // regression check for the version that silently folded LWW into this
    // function and collapsed a concurrent set down to one entry
    VersionedValue v4, v5, v6;
    v4.value = "A_wrote";
    (*v4.clock.mutable_counters())["node_A"] = 1;
    v4.clock.set_last_updated(10);
    v5.value = "B_wrote";
    (*v5.clock.mutable_counters())["node_B"] = 1;
    v5.clock.set_last_updated(20);
    v6.value = "C_wrote";
    (*v6.clock.mutable_counters())["node_C"] = 1;
    v6.clock.set_last_updated(30);
    std::vector<VersionedValue> conc = {v4, v5, v6};
    std::vector<VersionedValue> f3 = computeFrontier(conc);
    assert(f3.size() == 3);
    printf("test computeFrontier keeps full concurrent set: PASS\n");

    // true duplicate (EQUAL clocks) dedupes to exactly one survivor
    VersionedValue v7, v8;
    v7.value = "retry_copy_1";
    (*v7.clock.mutable_counters())["node_A"] = 5;
    v8.value = "retry_copy_2";
    (*v8.clock.mutable_counters())["node_A"] = 5;
    std::vector<VersionedValue> dup = {v7, v8};
    std::vector<VersionedValue> f4 = computeFrontier(dup);
    assert(f4.size() == 1);
    printf("test computeFrontier dedupes EQUAL clocks: PASS\n");

    // mixed set: one dominated entry drops out, two concurrent entries remain
    VersionedValue v9, v10, v11;
    v9.value = "behind";
    (*v9.clock.mutable_counters())["node_A"] = 1;
    v10.value = "concurrent_1";
    (*v10.clock.mutable_counters())["node_A"] = 2;
    (*v10.clock.mutable_counters())["node_B"] = 1;
    v11.value = "concurrent_2";
    (*v11.clock.mutable_counters())["node_A"] = 2;
    (*v11.clock.mutable_counters())["node_C"] = 1;
    std::vector<VersionedValue> mixed = {v9, v10, v11};
    std::vector<VersionedValue> f5 = computeFrontier(mixed);
    assert(f5.size() == 2);
    printf("test computeFrontier mixed dominated+concurrent set: PASS\n");
}

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

void testResolveGetResult() {
    // single replica responded: trivial passthrough, no conflict possible
    VersionedValue only;
    only.value = "single_reply";
    (*only.clock.mutable_counters())["node_A"] = 1;
    std::vector<VersionedValue> one = {only};
    VersionedValue r1 = resolveGetResult(one);
    assert(r1.value == "single_reply");
    printf("test resolveGetResult single response: PASS\n");

    // all responses in a dominance chain: frontier alone resolves it,
    // resolveLWW never needs to run
    VersionedValue stale, fresh;
    stale.value = "old_replica";
    (*stale.clock.mutable_counters())["node_A"] = 1;
    fresh.value = "current_replica";
    (*fresh.clock.mutable_counters())["node_A"] = 3;
    std::vector<VersionedValue> chain = {stale, fresh};
    VersionedValue r2 = resolveGetResult(chain);
    assert(r2.value == "current_replica");
    printf("test resolveGetResult dominance chain (no conflict): PASS\n");

    // genuine conflict: two concurrent replicas, LWW has to decide
    VersionedValue leftReplica, rightReplica;
    leftReplica.value = "coordinator_X_write";
    (*leftReplica.clock.mutable_counters())["node_X"] = 1;
    leftReplica.clock.set_last_updated(100);
    leftReplica.clock.set_writer_id("node_X");
    rightReplica.value = "coordinator_Y_write";
    (*rightReplica.clock.mutable_counters())["node_Y"] = 1;
    rightReplica.clock.set_last_updated(200);
    rightReplica.clock.set_writer_id("node_Y");
    std::vector<VersionedValue> conflict = {leftReplica, rightReplica};
    VersionedValue r3 = resolveGetResult(conflict);
    assert(r3.value == "coordinator_Y_write");
    printf("test resolveGetResult genuine conflict resolved via LWW: PASS\n");
}

int main() {
    testCompareVectorClocks();
    testMergeClocks();
    testBuildClock();
    testComputeFrontier();
    testResolveLWW();
    testResolveGetResult();
    return 0;
}