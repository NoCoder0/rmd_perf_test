// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
//
// Requester-driven direct and SGL B1/B2/S1/S2 benchmark for configurable sparse blocks in the
// ubs-comm RDMA experiment. Local owns the final destination and measures the
// complete sparse_copy call. Remote owns the source and posts the RDMA writes.
// In B2 each fixed application thread owns one service/NIC/QP for its complete
// setup-to-drain lifetime; internal hcom multirail is disabled.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#if defined(_WIN32)
#include <malloc.h>
#endif
#include <mutex>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sched.h>
#include <time.h>

#include "hcom/hcom_service.h"
// This HCOM release only forward-declares UBSHcomServiceContext in hcom_service.h.
#include "hcom/hcom_service_context.h"

#ifndef RDMA_600_GIT_COMMIT
#define RDMA_600_GIT_COMMIT "unknown"
#endif
#ifndef RDMA_600_BUILD_TYPE
#define RDMA_600_BUILD_TYPE "unknown"
#endif

namespace rdma_bench {

using ock::hcom::Callback;
using ock::hcom::UBSHcomChannelBrokenPolicy;
using ock::hcom::UBSHcomChannelCallBackType;
using ock::hcom::UBSHcomChannelPtr;
using ock::hcom::UBSHcomClientPollingMode;
using ock::hcom::UBSHcomConnectOptions;
using ock::hcom::UBSHcomMemoryKey;
using ock::hcom::UBSHcomMultiRailOptions;
using ock::hcom::UBSHcomNewCallback;
using ock::hcom::UBSHcomOneSideRequest;
using ock::hcom::UBSHcomOneSideSglRequest;
using ock::hcom::UBSHcomRegMemoryRegion;
using ock::hcom::UBSHcomReplyContext;
using ock::hcom::UBSHcomRequest;
using ock::hcom::UBSHcomResponse;
using ock::hcom::UBSHcomService;
using ock::hcom::UBSHcomServiceContext;
using ock::hcom::UBSHcomServiceOptions;
using ock::hcom::UBSHcomServiceProtocol;
using ock::hcom::UBSHcomTlsOptions;
using ock::hcom::UBSHcomTwoSideThreshold;

// Version 8 negotiates notification grouping and gives CHUNK_DONE range semantics.
// Older peers must fail negotiation before any data request is accepted.
constexpr uint16_t kProtocolVersion = 8;
constexpr uint16_t kMaxLinks = 2;
constexpr uint32_t kMaxBlocks = 9600;
constexpr uint32_t kMaxBlocksPerRail = kMaxBlocks;
constexpr uint32_t kMaxBlockBytes = 1024;
constexpr uint32_t kStrideBytes = 4096;
constexpr uint32_t kMaxTraceRounds = 64;
constexpr uint32_t kDataDeadlineCheckInterval = 256;

constexpr uint8_t kDstGapSentinel = 0xa5;
constexpr uint8_t kSourceGapSentinel = 0x5a;
constexpr uint32_t kSourceRegionId = 1;
constexpr uint32_t kDestinationRegionId = 2;
constexpr uint32_t kStageRegionId = 3;
constexpr uint16_t kModeDirect = 1;
constexpr uint16_t kModeSgl = 2;
constexpr uint16_t kPipelineOff = 0;
constexpr uint16_t kPipelineOn = 1;
constexpr uint16_t kSourceFormatDirectPairs = 1;
constexpr uint16_t kSourceFormatSparsePairs = 2;
constexpr uint16_t kDefaultSglItems = 16;
constexpr uint16_t kDesignMaxSglItems = 30;
constexpr uint32_t kCompiledSgeMax = ock::hcom::NET_SGE_MAX_IOV;
constexpr uint32_t kCopyErrorStageRemoteProcess = 1;
constexpr uint32_t kCopyErrorCodeRequestFailed = 1;
#if defined(__cpp_lib_hardware_interference_size)
constexpr size_t kCounterAlignment = std::hardware_destructive_interference_size;
#else
constexpr size_t kCounterAlignment = 64;
#endif

static_assert((kDataDeadlineCheckInterval & (kDataDeadlineCheckInterval - 1)) == 0,
    "the data deadline interval must be a power of two");

constexpr uint16_t kOpHello = 700;
constexpr uint16_t kOpReady = 701;
constexpr uint16_t kOpCopyReq = 702;
constexpr uint16_t kOpDataDone = 703;
constexpr uint16_t kOpCopyError = 704;
constexpr uint16_t kOpFinish = 705;
constexpr uint16_t kOpFinishAck = 706;
constexpr uint16_t kOpChunkDone = 707;

constexpr uint32_t kHelloMagic = 0x53434836U;      // "SCH6"
constexpr uint32_t kReadyMagic = 0x53435236U;      // "SCR6"
constexpr uint32_t kCopyReqMagic = 0x53435036U;    // "SCP6"
constexpr uint32_t kDataDoneMagic = 0x53434436U;   // "SCD6"
constexpr uint32_t kCopyErrorMagic = 0x53434536U;  // "SCE6"
constexpr uint32_t kFinishMagic = 0x53434636U;     // "SCF6"
constexpr uint32_t kFinishAckMagic = 0x53434136U;  // "SCA6"
constexpr uint32_t kChunkDoneMagic = 0x53434336U;  // "SCC6"

// Params are 44 bytes in protocol v8. HELLO carries this rail's local
// destination and optional stage registrations; READY describes its source.
constexpr size_t kParametersWireBytes = 44;
constexpr size_t kMemoryKeyWireBytes = 80;
constexpr size_t kHelloPreambleWireBytes = 4 + kParametersWireBytes + 4;
constexpr size_t kHelloRegionWireBytes = 4 + 4 + 8 + 8 + kMemoryKeyWireBytes;
constexpr size_t kHelloWireBytes = kHelloPreambleWireBytes + 2 * kHelloRegionWireBytes;
constexpr size_t kReadyWireBytes = 4 + kParametersWireBytes + 4 + 16;
constexpr size_t kCopyReqHeaderBytes = 64;
constexpr size_t kCopyEntryWireBytes = 16;
constexpr size_t kMaxCopyReqDescriptorBytes = static_cast<size_t>(kMaxBlocks) * kCopyEntryWireBytes;
constexpr size_t kMaxCopyReqWireBytes = kCopyReqHeaderBytes + kMaxCopyReqDescriptorBytes;
constexpr uint32_t kFragmentHeaderBytes = 32;
constexpr uint32_t kRequestServiceMessageBytes = 256U * 1024U;
constexpr uint32_t kControlServiceMessageBytes = 16U * 1024U;
// HCOM's segment includes its transport header. TLS and internal split/RNDV
// are disabled in this benchmark; reserve the actual public-header size.
constexpr uint32_t kFragmentDataBytes = kRequestServiceMessageBytes -
    sizeof(ock::hcom::UBSHcomNetTransHeader) - kFragmentHeaderBytes;
constexpr uint32_t kFragmentWireBytes = kFragmentHeaderBytes + kFragmentDataBytes;
constexpr uint32_t kMaxFragments = (kMaxCopyReqWireBytes + kFragmentDataBytes - 1) / kFragmentDataBytes;
constexpr uint16_t kOpMatrix = 708;
constexpr uint16_t kOpCaseStart = 709;
constexpr uint16_t kOpCaseReady = 710;
constexpr uint32_t kMatrixMagic = 0x53434d36U;
constexpr uint32_t kCaseStartMagic = 0x53435336U;
constexpr uint32_t kCaseReadyMagic = 0x53435436U;
constexpr uint32_t kFragmentMagic = 0x53435046U;
constexpr size_t kMatrixItemWireBytes = 16 + kParametersWireBytes;
constexpr size_t kDataDoneWireBytes = 32;
constexpr size_t kCopyErrorWireBytes = 32;
constexpr size_t kTokenWireBytes = 24;
constexpr size_t kChunkDoneWireBytes = 40;

static_assert(kHelloWireBytes == 260, "HELLO wire size must include both complete region descriptors");
static_assert(kReadyWireBytes == 68, "READY wire size is fixed");
static_assert(kFragmentWireBytes + sizeof(ock::hcom::UBSHcomNetTransHeader) <= kRequestServiceMessageBytes,
    "fragment and HCOM header must fit the service segment");
static_assert(kMaxCopyReqWireBytes <= kFragmentDataBytes, "current matrix must fit one COPY_REQ Send");
static_assert(kHelloWireBytes + sizeof(ock::hcom::UBSHcomNetTransHeader) <= kControlServiceMessageBytes,
    "control messages must fit the smaller service segment");
static_assert(kChunkDoneWireBytes == 40, "CHUNK_DONE wire size is fixed");

enum class Role { Local, Remote };
enum class RunKind { Verify, Measure, Trace };
enum class CopyMode : uint16_t { Direct = kModeDirect, Sgl = kModeSgl };
enum class PipelineMode : uint16_t { Off = kPipelineOff, On = kPipelineOn };

struct Options {
    Role role = Role::Local;
    RunKind kind = RunKind::Measure;
    uint16_t links = 1;
    CopyMode mode = CopyMode::Direct;
    PipelineMode pipeline = PipelineMode::Off;
    uint16_t sglItems = 0;
    uint32_t notifyEveryWrs = 0; // SGL: 1..kMaxBlocks; direct: 0 (not applicable).
    uint32_t maxInflight = 0; // Remote SGL only: outstanding data PutV requests per rail; 0 = unlimited.
    std::vector<uint32_t> qpMaxSendSge;
    bool qpCapDeclared = false;
    std::vector<std::string> rdmaIps;
    std::vector<std::string> endpoints;
    uint32_t verifyRounds = 20;
    uint32_t warmupRounds = 100;
    uint32_t measureRounds = 1000;
    uint32_t traceRounds = 0;
    std::vector<uint32_t> blockCounts;
    std::vector<uint32_t> blockLengths;
    uint32_t timeoutSec = 10;
    std::vector<int> appCpus;
    std::vector<int> workerCpus;
};

struct CaseParameters {
    uint16_t version = kProtocolVersion;
    uint16_t links = 1;
    uint32_t blocks = 600;
    uint32_t blockBytes = 1024;
    uint32_t strideBytes = kStrideBytes;
    uint32_t verifyRounds = 0;
    uint32_t warmupRounds = 0;
    uint32_t measureRounds = 0;
    uint32_t traceRounds = 0;
    uint16_t mode = kModeDirect;
    uint16_t sglItems = 0;
    uint16_t pipeline = kPipelineOff;
    uint16_t sourceFormat = kSourceFormatDirectPairs;
    uint32_t notifyEveryWrs = 0;

    uint32_t RailCapacity() const { return (blocks + links - 1) / links; }
    uint32_t RailBlocks(uint16_t rail) const { return std::min(RailCapacity(), blocks - rail * RailCapacity()); }
    uint64_t PayloadBytes() const { return static_cast<uint64_t>(blocks) * blockBytes; }
    uint32_t RequestBytes() const { return kCopyReqHeaderBytes + blocks * kCopyEntryWireBytes; }
    uint32_t Fragments() const { return (RequestBytes() + kFragmentDataBytes - 1) / kFragmentDataBytes; }
    uint64_t TotalRounds() const
    {
        return static_cast<uint64_t>(verifyRounds) + warmupRounds + measureRounds + traceRounds;
    }
};

struct HelloInfo {
    CaseParameters params;
    uint16_t rail = 0;
    uint32_t destinationRegionId = 0;
    uint64_t destinationAddress = 0;
    uint64_t destinationBytes = 0;
    UBSHcomMemoryKey destinationKey{};
    uint32_t stageRegionId = 0;
    uint64_t stageAddress = 0;
    uint64_t stageBytes = 0;
    UBSHcomMemoryKey stageKey{};
};

struct ReadyInfo {
    CaseParameters params;
    uint16_t rail = 0;
    uint32_t sourceRegionId = 0;
    uint32_t sourceAlignment = 0;
    uint64_t sourceBytes = 0;
};

struct CopyEntry {
    uint64_t remoteSourceOffset = 0;
    uint64_t localDestinationOffset = 0;
};

struct ChunkDoneInfo {
    uint16_t rail = 0;
    uint64_t generation = 0;
    uint32_t chunkId = 0; // First chunk in this notification group, not a cumulative watermark.
    uint32_t firstItem = 0;
    uint32_t itemCount = 0;
    uint32_t payloadBytes = 0;
    uint32_t chunkCount = 0;
};

class AlignedBuffer {
public:
    AlignedBuffer() = default;
    AlignedBuffer(const AlignedBuffer &) = delete;
    AlignedBuffer &operator=(const AlignedBuffer &) = delete;

    ~AlignedBuffer()
    {
        Reset();
    }

    void Allocate(size_t size)
    {
        Reset();
#if defined(_WIN32)
        void *memory = _aligned_malloc(size, 4096);
        const int rc = memory == nullptr ? errno : 0;
#else
        void *memory = nullptr;
        const int rc = posix_memalign(&memory, 4096, size);
#endif
        if (rc != 0 || memory == nullptr) {
            throw std::runtime_error("posix_memalign failed for " + std::to_string(size) + " bytes: " +
                std::strerror(rc == 0 ? errno : rc));
        }
        mData = static_cast<uint8_t *>(memory);
        mSize = size;
    }

    void Reset()
    {
        if (mData != nullptr) {
#if defined(_WIN32)
            _aligned_free(mData);
#else
            std::free(mData);
#endif
            mData = nullptr;
            mSize = 0;
        }
    }

    uint8_t *Data() const { return mData; }
    size_t Size() const { return mSize; }

private:
    uint8_t *mData = nullptr;
    size_t mSize = 0;
};

constexpr uint64_t kChunkReadyPublishing = std::numeric_limits<uint64_t>::max();

struct TracePoint {
    std::atomic<uint64_t> timestampNs{0};
    std::atomic<bool> published{false};

    void Publish(uint64_t timestamp) noexcept
    {
        timestampNs.store(timestamp, std::memory_order_relaxed);
        published.store(true, std::memory_order_release);
    }

    uint64_t Read(const char *event) const
    {
        if (!published.load(std::memory_order_acquire)) {
            throw std::runtime_error(std::string("trace event was not published: ") + event);
        }
        return timestampNs.load(std::memory_order_relaxed);
    }
};

struct TraceRound {
    uint64_t generation = 0;
    TracePoint localBegin;
    TracePoint localRequestSubmitBegin;
    TracePoint localRequestPosted;
    std::array<TracePoint, kMaxLinks> localDataDone;
    TracePoint localEnd;
    TracePoint remoteRequestReceived;
    TracePoint remoteRequestObserved;
    TracePoint remoteRequestCopied;
    TracePoint remoteRequestDecoded;
    std::array<TracePoint, kMaxLinks> remoteSourcePrepared;
    std::array<TracePoint, kMaxLinks> remoteRequestsPrepared;
    std::array<TracePoint, kMaxLinks> remotePosted;
    std::array<TracePoint, kMaxLinks> remoteDataCallbacksDone;
    std::array<TracePoint, kMaxLinks> remoteDonePosted;
    std::array<std::array<TracePoint, kMaxBlocksPerRail>, kMaxLinks> localChunkReady;
    std::array<std::array<TracePoint, kMaxBlocksPerRail>, kMaxLinks> localReadyObserved;
    std::array<std::array<TracePoint, kMaxBlocksPerRail>, kMaxLinks> localScatterBegin;
    std::array<std::array<TracePoint, kMaxBlocksPerRail>, kMaxLinks> localScatterEnd;
    std::array<std::array<TracePoint, kMaxBlocksPerRail>, kMaxLinks> remoteChunkPosted;
    std::array<std::array<TracePoint, kMaxBlocksPerRail>, kMaxLinks> remoteChunkDonePosted;
};

struct SglSchedulerState {
    uint32_t scattered = 0;
    uint32_t cursor = 0;
    uint32_t examinedSinceCheckpoint = 0;
};

class ActiveCallbackGuard {
public:
    explicit ActiveCallbackGuard(std::atomic<uint64_t> &counter) : mCounter(counter)
    {
        mCounter.fetch_add(1, std::memory_order_acq_rel);
    }

    ~ActiveCallbackGuard()
    {
        mCounter.fetch_sub(1, std::memory_order_release);
    }

private:
    std::atomic<uint64_t> &mCounter;
};

// Each rail's attempted counters are owned by its fixed application thread.
// The coordinator reads them only after the rail command's release/acquire
// completion edge; callback threads never read or write them. Callback-owned
// counters remain atomic and separated onto another cache line.
struct alignas(kCounterAlignment) AppOwnedCounters {
    uint64_t attemptedDataCallbacks = 0;
    uint64_t attemptedSendCallbacks = 0;
};

struct alignas(kCounterAlignment) CallbackOwnedCounters {
    std::atomic<uint64_t> workerAttemptedSendCallbacks{0};
    std::atomic<uint64_t> dataDoneCallbacks{0};
    std::atomic<uint64_t> sendDoneCallbacks{0};
};

struct RailState {
    std::string serviceName;
    UBSHcomService *service = nullptr;
    UBSHcomChannelPtr channel;
    AlignedBuffer buffer;
    UBSHcomRegMemoryRegion memoryRegion;
    bool memoryRegistered = false;
    UBSHcomMemoryKey memoryKey{};
    UBSHcomMemoryKey peerDestinationKey{};
    AlignedBuffer stageBuffer;
    UBSHcomRegMemoryRegion stageMemoryRegion;
    bool stageMemoryRegistered = false;
    UBSHcomMemoryKey stageMemoryKey{};
    uintptr_t peerDestinationAddress = 0;
    uint64_t peerDestinationBytes = 0;
    uint64_t peerSourceBytes = 0;
    uintptr_t peerStageAddress = 0;
    uint64_t peerStageBytes = 0;
    UBSHcomMemoryKey peerStageKey{};
    std::array<UBSHcomOneSideRequest, kMaxBlocksPerRail> putRequests{};
    std::array<UBSHcomOneSideRequest, kMaxBlocksPerRail> sglIovs{};
    std::array<UBSHcomOneSideSglRequest, kMaxBlocksPerRail> sglRequests{};
    std::array<uint8_t, kHelloWireBytes> helloPayload{};
    std::array<uint8_t, kReadyWireBytes> readyPayload{};
    std::array<uint8_t, kReadyWireBytes> readyResponse{};
    std::array<uint8_t, kDataDoneWireBytes> dataDonePayload{};
    std::array<std::array<uint8_t, kChunkDoneWireBytes>, kMaxBlocksPerRail> chunkDonePayloads{};
    std::array<uint8_t, kTokenWireBytes> finishPayload{};
    std::array<uint8_t, kTokenWireBytes> finishAckPayload{};
    std::atomic<bool> helloClaimed{false};
    std::atomic<bool> helloSeen{false};
    std::atomic<uint64_t> dataDoneGeneration{0};
    // Only the first chunk index of each notification group owns a ready slot.
    std::array<std::atomic<uint64_t>, kMaxBlocksPerRail> groupReadyGeneration{};
    std::array<uint64_t, kMaxBlocksPerRail> chunkConsumedGeneration{};
    std::atomic<uint64_t> finishGeneration{0};
    std::atomic<uint64_t> finishAckGeneration{0};
    AppOwnedCounters appCounters;
    CallbackOwnedCounters callbackCounters;
};

enum class RailCommand : uint8_t {
    None,
    Setup,
    ConnectAndHandshake,
    ProcessRemoteRound,
    ScatterLocalRound,
    Finish,
    Teardown,
};

// Two-link runs keep rail 0 on the original application thread and give rail 1
// one persistent application thread, including local SGL scatter. Each scatter
// thread exclusively writes its own rail's destination and consumed generations.
// A release/acquire command sequence publishes
// all command arguments and makes command-side counter writes visible before
// the coordinator consumes them. No HCOM operation is dispatched through a
// transient thread.
struct alignas(kCounterAlignment) SecondaryRailExecutor {
    std::thread thread;
    std::atomic<bool> started{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> issued{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<RailCommand> command{RailCommand::None};
    std::atomic<uint64_t> generation{0};
    std::atomic<uint64_t> deadlineNs{0};
};

}  // namespace rdma_bench
