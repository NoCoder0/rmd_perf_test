// SPDX-License-Identifier: MulanPSL-2.0
#include "detailed_trace.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace rdma_bench {
namespace {
using ock::hcom::UBSHcomRdmaTraceEvent;
using ock::hcom::UBSHcomRdmaTraceKind;
constexpr size_t kThreads = 8;
constexpr size_t kMaxRecordsPerThread = 262144;
struct alignas(128) ThreadBuffer {
    std::unique_ptr<UBSHcomRdmaTraceEvent[]> records;
    size_t size = 0, dropped = 0, clockErrors = 0;
};
std::array<ThreadBuffer, kThreads> buffers;
std::atomic<uint64_t> activeCase{0};
std::atomic<size_t> nextThread{0}, unregisteredEvents{0};
size_t capacity = 0;
size_t expectedSglOperations = 0;
std::string hostRole;
bool initialized = false;

uint64_t Epoch() noexcept { return activeCase.load(std::memory_order_acquire); }
void Record(const UBSHcomRdmaTraceEvent &event) noexcept
{
    // Registration is once per producer thread; the steady-state write path has
    // no locks, allocation, shared write counter or console output.
    thread_local const size_t slot = nextThread.fetch_add(1, std::memory_order_relaxed);
    if (slot >= kThreads) { unregisteredEvents.fetch_add(1, std::memory_order_relaxed); return; }
    ThreadBuffer &buffer = buffers[slot];
    if (event.timestampNs == 0) ++buffer.clockErrors;
    if (buffer.size == capacity) { ++buffer.dropped; return; }
    buffer.records[buffer.size++] = event;
}
const ock::hcom::UBSHcomRdmaTraceHooks hooks{Epoch, Record};

const char *EventName(UBSHcomRdmaTraceKind kind)
{
    switch (kind) {
        case UBSHcomRdmaTraceKind::POST_BEGIN: return "verbs_post_begin";
        case UBSHcomRdmaTraceKind::POST_END: return "verbs_post_end";
        case UBSHcomRdmaTraceKind::CQ_POLL_BATCH: return "cq_poll_batch";
        case UBSHcomRdmaTraceKind::CQE_OBSERVED: return "cqe_observed";
        case UBSHcomRdmaTraceKind::CQ_DISPATCH_BEGIN: return "cq_dispatch_begin";
        case UBSHcomRdmaTraceKind::CQ_DISPATCH_END: return "cq_dispatch_end";
        case UBSHcomRdmaTraceKind::DATA_CALLBACK_BEGIN: return "data_callback_begin";
        case UBSHcomRdmaTraceKind::DATA_CALLBACK_END: return "data_callback_end";
        case UBSHcomRdmaTraceKind::NOTIFY_INCOMING: return "notify_incoming";
        case UBSHcomRdmaTraceKind::NOTIFY_HANDLER_BEGIN: return "notify_handler_begin";
        case UBSHcomRdmaTraceKind::NOTIFY_DECODED: return "notify_decoded";
        case UBSHcomRdmaTraceKind::NOTIFY_READY_PUBLISHED: return "notify_ready_published";
    }
    return "unknown";
}
} // namespace

void InitializeDetailedTrace(const char *role, size_t operations, bool requireSglPosts)
{
    if (initialized) throw std::logic_error("detailed trace initialized twice");
    // Bound memory even for a large case matrix. Overflow is reported and makes
    // the run unsuccessful; it must never masquerade as a complete trace.
    capacity = operations >= (kMaxRecordsPerThread - 1024) / 12 ? kMaxRecordsPerThread :
        std::max<size_t>(4096, operations * 12 + 1024);
    hostRole = role;
    expectedSglOperations = requireSglPosts ? operations : 0;
    for (auto &buffer : buffers) buffer.records.reset(new UBSHcomRdmaTraceEvent[capacity]());
    initialized = true;
    ock::hcom::UBSHcomRdmaTraceConfigure(&hooks);
}

void BeginDetailedTrace(uint64_t caseIndex) noexcept
{
    if (initialized) activeCase.store(caseIndex, std::memory_order_release);
}
void EndDetailedTrace() noexcept { activeCase.store(0, std::memory_order_release); }

bool FinishDetailedTrace(bool aborted)
{
    if (!initialized) return true;
    // Caller has joined BOTH application and HCOM worker threads. Plain reads
    // below are then safe, including any record that straddled EndDetailedTrace.
    EndDetailedTrace();
    ock::hcom::UBSHcomRdmaTraceConfigure(nullptr);
    size_t total = 0, dropped = unregisteredEvents.load(), clockErrors = 0;
    size_t posts = 0, cqes = 0, callbacks = 0;
    for (size_t thread = 0; thread < kThreads; ++thread) {
        const auto &buffer = buffers[thread];
        total += buffer.size; dropped += buffer.dropped; clockErrors += buffer.clockErrors;
        for (size_t i = 0; i < buffer.size; ++i) {
            const auto &e = buffer.records[i];
            posts += e.kind == UBSHcomRdmaTraceKind::POST_END;
            cqes += e.kind == UBSHcomRdmaTraceKind::CQE_OBSERVED;
            callbacks += e.kind == UBSHcomRdmaTraceKind::DATA_CALLBACK_END;
            std::cout << "{\"record_type\":\"hcom_trace\",\"trace_schema\":\"rdma-completion-v1\",\"host_role\":\""
                << hostRole << "\",\"case_index\":" << e.epoch << ",\"thread_slot\":" << thread
                << ",\"event\":\"" << EventName(e.kind) << "\",\"timestamp_ns\":" << e.timestampNs
                << ",\"generation\":";
            if (e.generation == 0) std::cout << "null"; else std::cout << e.generation;
            std::cout << ",\"rail\":";
            if (e.rail < 0) std::cout << "null"; else std::cout << e.rail;
            std::cout << ",\"chunk_id\":";
            if (e.chunk < 0) std::cout << "null"; else std::cout << e.chunk;
            // Pointer-valued identifiers are hex strings, preserving all bits
            // in JSON/JavaScript readers. wr_id can be reused after completion.
            std::cout << ",\"wr_id\":\"0x" << std::hex << e.wrId << "\",\"cq_id\":\"0x" << e.cqId
                << std::dec << "\",\"qp_num\":" << e.qpNum << ",\"batch_id\":" << e.batchId
                << ",\"opcode\":" << e.opcode << ",\"status\":" << e.status << ",\"count\":" << e.count
                << ",\"sge_count\":" << e.sgeCount << ",\"bytes\":" << e.bytes
                << ",\"poll_begin_ns\":" << e.pollBeginNs << ",\"previous_poll_end_ns\":" << e.previousPollEndNs
                << ",\"empty_polls\":" << e.emptyPolls << ",\"max_poll_gap_ns\":" << e.maxPollGapNs
                << ",\"max_poll_call_ns\":" << e.maxPollCallNs << "}\n";
        }
    }
    const bool complete = !aborted && total != 0 && cqes != 0 && dropped == 0 && clockErrors == 0 &&
        (expectedSglOperations == 0 || (posts >= expectedSglOperations && callbacks == expectedSglOperations));
    std::cout << "{\"record_type\":\"hcom_trace_summary\",\"trace_schema\":\"rdma-completion-v1\",\"host_role\":\""
        << hostRole << "\",\"status\":\"" << (complete ? "ok" : "incomplete")
        << "\",\"records\":" << total << ",\"dropped\":" << dropped << ",\"clock_errors\":" << clockErrors
        << ",\"post_records\":" << posts << ",\"cqe_records\":" << cqes
        << ",\"data_callback_records\":" << callbacks << ",\"expected_sgl_operations\":" << expectedSglOperations
        << ",\"thread_count\":" << nextThread.load() << ",\"capacity_per_thread\":" << capacity
        << ",\"clock\":\"CLOCK_MONOTONIC_RAW\",\"cqe_time_basis\":\"poll-observation-not-hardware-completion\""
        << ",\"poll_gap_basis\":\"max-since-previous-nonempty-poll-including-handler-and-trace-overhead\"}\n";
    return complete;
}
} // namespace rdma_bench
