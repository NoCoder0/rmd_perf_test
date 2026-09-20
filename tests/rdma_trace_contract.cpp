// SPDX-License-Identifier: MulanPSL-2.0
// CPU-only instrumentation contract; no transport or performance is simulated.
#include "hcom/hcom_rdma_trace.h"
#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

using namespace ock::hcom;
namespace {
std::atomic<uint64_t> epoch{0};
std::mutex mutex;
std::vector<UBSHcomRdmaTraceEvent> records;
uint64_t Epoch() { return epoch.load(); }
void Record(const UBSHcomRdmaTraceEvent &e) { std::lock_guard<std::mutex> guard(mutex); records.push_back(e); }
const UBSHcomRdmaTraceHooks hooks{Epoch, Record};
}
int main()
{
    assert(UBSHcomRdmaTraceEpoch() == 0);
    UBSHcomRdmaTraceConfigure(&hooks);
    UBSHcomRdmaTraceMark(UBSHcomRdmaTraceKind::DATA_CALLBACK_BEGIN, 21, 0, 0);
    { UBSHcomRdmaTraceDispatchScope scope(99, 7, 0, 0); }
    assert(records.empty());

    epoch = 1;
    {
        UBSHcomRdmaTraceOperationScope outer(true, 21, 0, 3);
        UBSHcomRdmaTracePost(1, 100, 110, 7, 99, 0, 30, 19680, 0);
        { UBSHcomRdmaTraceOperationScope inner(true, 22, 1, 4);
          UBSHcomRdmaTracePost(1, 120, 130, 8, 100, 0, 10, 6560, 0); }
        UBSHcomRdmaTracePost(1, 140, 150, 7, 101, 0, 30, 19680, 0);
    }
    assert(records[0].generation == 21 && records[0].chunk == 3);
    assert(records[2].generation == 22 && records[2].rail == 1);
    assert(records[4].generation == 21 && records[4].chunk == 3);
    assert(records[0].timestampNs == 100 && records[1].timestampNs == 110);

    UBSHcomRdmaTracePoll(1, 200, 210, 123, 0);
    UBSHcomRdmaTracePoll(1, 215, 225, 123, 0);
    UBSHcomRdmaTracePoll(1, 280, 295, 123, 2);
    UBSHcomRdmaTraceCqe(99, 7, 0, 0);
    UBSHcomRdmaTraceCqe(101, 7, 0, 0);
    assert(records[6].emptyPolls == 2 && records[6].maxPollGapNs == 55);
    assert(records[6].maxPollCallNs == 15 && records[6].previousPollEndNs == 225);
    assert(records[7].timestampNs == 295 && records[8].timestampNs == 295);
    assert(records[7].batchId == records[8].batchId && records[7].generation == 0);
    {
        UBSHcomRdmaTraceDispatchScope outer(99, 7, 0, 0);
        UBSHcomRdmaTraceMark(UBSHcomRdmaTraceKind::DATA_CALLBACK_BEGIN, 21, 0, 3);
        { UBSHcomRdmaTraceDispatchScope inner(101, 7, 0, 0);
          UBSHcomRdmaTraceMark(UBSHcomRdmaTraceKind::DATA_CALLBACK_BEGIN, 21, 0, 4); }
        UBSHcomRdmaTraceMark(UBSHcomRdmaTraceKind::DATA_CALLBACK_END, 21, 0, 3);
    }
    assert(records[10].wrId == 99 && records[10].batchId == records[7].batchId);
    assert(records[12].wrId == 101 && records[14].wrId == 99);
    UBSHcomRdmaTraceMark(UBSHcomRdmaTraceKind::DATA_CALLBACK_END, 21, 0, 3);
    assert(records.back().wrId == 0); // no stale CQ association outside dispatch

    epoch = 2;
    const size_t before = records.size();
    { UBSHcomRdmaTraceDispatchScope stale(99, 7, 0, 0); }
    assert(records.size() == before); // previous case's batch cannot leak
    UBSHcomRdmaTracePoll(2, 1000, 1010, 123, 1);
    assert(records.back().maxPollGapNs == 0 && records.back().emptyPolls == 0);

    std::thread producer([] {
        UBSHcomRdmaTraceOperationScope op(true, 23, 1, 26);
        UBSHcomRdmaTracePost(2, 1100, 1110, 8, 101, 0, 20, 13120, 0);
    });
    producer.join();
    assert(records.back().generation == 23 && records.back().rail == 1);
    UBSHcomRdmaTracePost(2, 1120, 1130, 7, 102, 0, 30, 19680, 0);
    assert(records.back().generation == 0 && records.back().rail == -1); // TLS isolation
    epoch = 0;
    const size_t stopped = records.size();
    UBSHcomRdmaTraceMark(UBSHcomRdmaTraceKind::DATA_CALLBACK_END, 23, 1, 26);
    assert(records.size() == stopped);
    UBSHcomRdmaTraceConfigure(nullptr);
    std::cout << "PASS: trace scopes, CQ batch timestamps, poll gaps, epoch and thread isolation\n";
}
