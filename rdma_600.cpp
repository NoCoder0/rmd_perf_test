// SPDX-License-Identifier: MulanPSL-2.0
//
// Requester-driven direct B1/B2 benchmark for the 600 x 1 KiB
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
#if defined(_WIN32)
#include <malloc.h>
#endif
#include <mutex>
#include <new>
#include <numeric>
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

namespace {

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

// Version 4 deliberately rejects the older sender-driven protocol, which also
// used version 3 but had incompatible roles and per-round ACK semantics.
constexpr uint16_t kProtocolVersion = 4;
constexpr uint16_t kMaxLinks = 2;
constexpr uint32_t kBlocks = 600;
constexpr uint32_t kMaxBlocksPerRail = kBlocks;
constexpr uint32_t kBlockBytes = 1024;
constexpr uint32_t kStrideBytes = 4096;
constexpr uint32_t kMaxTraceRounds = 64;
constexpr uint32_t kPutTraceInterval = 100;
constexpr uint32_t kMaxPutTraceCheckpoints = kMaxBlocksPerRail / kPutTraceInterval;
constexpr uint32_t kDataDeadlineCheckInterval = 256;
constexpr uint64_t kPayloadBytes = static_cast<uint64_t>(kBlocks) * kBlockBytes;
constexpr uint8_t kDstGapSentinel = 0xa5;
constexpr uint8_t kSourceGapSentinel = 0x5a;
constexpr uint32_t kSourceRegionId = 1;
constexpr uint32_t kDestinationRegionId = 2;
constexpr uint16_t kModeDirect = 1;
constexpr uint16_t kSourceFormatDirectPairs = 1;
constexpr uint32_t kCopyErrorStageRemoteProcess = 1;
constexpr uint32_t kCopyErrorCodeRequestFailed = 1;
#if defined(__cpp_lib_hardware_interference_size)
constexpr size_t kCounterAlignment = std::hardware_destructive_interference_size;
#else
constexpr size_t kCounterAlignment = 64;
#endif

static_assert(kPayloadBytes == 614400, "the benchmark payload is fixed by design");
static_assert(kBlocks % kMaxLinks == 0 && (kBlocks / kMaxLinks) % kPutTraceInterval == 0,
    "each supported rail count must have an integral number of Put trace checkpoints");
static_assert((kDataDeadlineCheckInterval & (kDataDeadlineCheckInterval - 1)) == 0,
    "the data deadline interval must be a power of two");

constexpr uint16_t kOpHello = 700;
constexpr uint16_t kOpReady = 701;
constexpr uint16_t kOpCopyReq = 702;
constexpr uint16_t kOpDataDone = 703;
constexpr uint16_t kOpCopyError = 704;
constexpr uint16_t kOpFinish = 705;
constexpr uint16_t kOpFinishAck = 706;

constexpr uint32_t kHelloMagic = 0x53434834U;      // "SCH4"
constexpr uint32_t kReadyMagic = 0x53435234U;      // "SCR4"
constexpr uint32_t kCopyReqMagic = 0x53435034U;    // "SCP4"
constexpr uint32_t kDataDoneMagic = 0x53434434U;   // "SCD4"
constexpr uint32_t kCopyErrorMagic = 0x53434534U;  // "SCE4"
constexpr uint32_t kFinishMagic = 0x53434634U;     // "SCF4"
constexpr uint32_t kFinishAckMagic = 0x53434134U;  // "SCA4"

// Params are 32 bytes in protocol v4. HELLO carries this rail's local
// destination registration; READY describes this rail's remote source region.
constexpr size_t kParametersWireBytes = 32;
constexpr size_t kMemoryKeyWireBytes = 80;
constexpr size_t kHelloWireBytes = 4 + kParametersWireBytes + 4 + 24 + kMemoryKeyWireBytes;
constexpr size_t kReadyWireBytes = 4 + kParametersWireBytes + 4 + 16;
constexpr size_t kCopyReqHeaderBytes = 64;
constexpr size_t kCopyEntryWireBytes = 16;
constexpr size_t kCopyReqDescriptorBytes = static_cast<size_t>(kBlocks) * kCopyEntryWireBytes;
constexpr size_t kCopyReqWireBytes = kCopyReqHeaderBytes + kCopyReqDescriptorBytes;
constexpr size_t kDataDoneWireBytes = 32;
constexpr size_t kCopyErrorWireBytes = 32;
constexpr size_t kTokenWireBytes = 24;

static_assert(kHelloWireBytes == 144, "HELLO wire size must include one destination key");
static_assert(kReadyWireBytes == 56, "READY wire size is fixed");
static_assert(kCopyReqWireBytes == 9664, "direct COPY_REQ must carry all 600 pairs");

enum class Role { Local, Remote };
enum class RunKind { Verify, Measure, Trace };

struct Options {
    Role role = Role::Local;
    RunKind kind = RunKind::Measure;
    uint16_t links = 1;
    std::vector<std::string> rdmaIps;
    std::vector<std::string> endpoints;
    uint32_t verifyRounds = 20;
    uint32_t warmupRounds = 1000;
    uint32_t measureRounds = 10000;
    uint32_t traceRounds = 0;
    uint32_t timeoutSec = 10;
    std::vector<int> appCpus;
    std::vector<int> workerCpus;
    bool selfTest = false;
};

struct CaseParameters {
    uint16_t version = kProtocolVersion;
    uint16_t links = 1;
    uint32_t blocks = kBlocks;
    uint32_t blockBytes = kBlockBytes;
    uint32_t strideBytes = kStrideBytes;
    uint32_t verifyRounds = 0;
    uint32_t warmupRounds = 0;
    uint32_t measureRounds = 0;
    uint32_t traceRounds = 0;

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

uint64_t NowNs()
{
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        throw std::runtime_error("clock_gettime(CLOCK_MONOTONIC_RAW) failed");
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}
bool TryNowNs(uint64_t &value) noexcept
{
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        return false;
    }
    value = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    return true;
}

#ifndef RDMA_600_SELF_TEST_ONLY
uint64_t ThreadCpuNowNs()
{
    timespec ts{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
        throw std::runtime_error("clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}
#endif

void PutU16(uint8_t *&cursor, uint16_t value)
{
    *cursor++ = static_cast<uint8_t>(value >> 8U);
    *cursor++ = static_cast<uint8_t>(value);
}

void PutU32(uint8_t *&cursor, uint32_t value)
{
    for (int shift = 24; shift >= 0; shift -= 8) {
        *cursor++ = static_cast<uint8_t>(value >> shift);
    }
}

void PutU64(uint8_t *&cursor, uint64_t value)
{
    for (int shift = 56; shift >= 0; shift -= 8) {
        *cursor++ = static_cast<uint8_t>(value >> shift);
    }
}

uint16_t GetU16(const uint8_t *&cursor)
{
    const uint16_t value = static_cast<uint16_t>(cursor[0]) << 8U | cursor[1];
    cursor += 2;
    return value;
}

uint32_t GetU32(const uint8_t *&cursor)
{
    uint32_t value = 0;
    for (int index = 0; index < 4; ++index) {
        value = (value << 8U) | cursor[index];
    }
    cursor += 4;
    return value;
}

uint64_t GetU64(const uint8_t *&cursor)
{
    uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | cursor[index];
    }
    cursor += 8;
    return value;
}

void EncodeParams(uint8_t *&cursor, const CaseParameters &params)
{
    PutU16(cursor, params.version);
    PutU16(cursor, params.links);
    PutU32(cursor, params.blocks);
    PutU32(cursor, params.blockBytes);
    PutU32(cursor, params.strideBytes);
    PutU32(cursor, params.verifyRounds);
    PutU32(cursor, params.warmupRounds);
    PutU32(cursor, params.measureRounds);
    PutU32(cursor, params.traceRounds);
}

CaseParameters DecodeParams(const uint8_t *&cursor)
{
    CaseParameters params;
    params.version = GetU16(cursor);
    params.links = GetU16(cursor);
    params.blocks = GetU32(cursor);
    params.blockBytes = GetU32(cursor);
    params.strideBytes = GetU32(cursor);
    params.verifyRounds = GetU32(cursor);
    params.warmupRounds = GetU32(cursor);
    params.measureRounds = GetU32(cursor);
    params.traceRounds = GetU32(cursor);
    return params;
}

bool SameParams(const CaseParameters &left, const CaseParameters &right)
{
    return left.version == right.version && left.links == right.links && left.blocks == right.blocks &&
        left.blockBytes == right.blockBytes && left.strideBytes == right.strideBytes &&
        left.verifyRounds == right.verifyRounds && left.warmupRounds == right.warmupRounds &&
        left.measureRounds == right.measureRounds && left.traceRounds == right.traceRounds;
}

void EncodeMemoryKey(uint8_t *&cursor, const UBSHcomMemoryKey &key)
{
    for (uint64_t value : key.keys) {
        PutU64(cursor, value);
    }
    for (uint64_t value : key.tokens) {
        PutU64(cursor, value);
    }
    std::memcpy(cursor, key.eid, sizeof(key.eid));
    cursor += sizeof(key.eid);
}

UBSHcomMemoryKey DecodeMemoryKey(const uint8_t *&cursor)
{
    UBSHcomMemoryKey key{};
    for (uint64_t &value : key.keys) {
        value = GetU64(cursor);
    }
    for (uint64_t &value : key.tokens) {
        value = GetU64(cursor);
    }
    std::memcpy(key.eid, cursor, sizeof(key.eid));
    cursor += sizeof(key.eid);
    return key;
}

std::array<uint8_t, kHelloWireBytes> EncodeHello(const HelloInfo &info)
{
    std::array<uint8_t, kHelloWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kHelloMagic);
    EncodeParams(cursor, info.params);
    PutU16(cursor, info.rail);
    PutU16(cursor, 0);
    PutU32(cursor, info.destinationRegionId);
    PutU32(cursor, 0);
    PutU64(cursor, info.destinationAddress);
    PutU64(cursor, info.destinationBytes);
    EncodeMemoryKey(cursor, info.destinationKey);
    return payload;
}

bool DecodeHello(const void *data, uint32_t size, HelloInfo &info)
{
    if (data == nullptr || size != kHelloWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kHelloMagic) {
        return false;
    }
    info.params = DecodeParams(cursor);
    info.rail = GetU16(cursor);
    if (GetU16(cursor) != 0) {
        return false;
    }
    info.destinationRegionId = GetU32(cursor);
    if (GetU32(cursor) != 0) {
        return false;
    }
    info.destinationAddress = GetU64(cursor);
    info.destinationBytes = GetU64(cursor);
    info.destinationKey = DecodeMemoryKey(cursor);
    return true;
}

std::array<uint8_t, kReadyWireBytes> EncodeReady(const ReadyInfo &info)
{
    std::array<uint8_t, kReadyWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kReadyMagic);
    EncodeParams(cursor, info.params);
    PutU16(cursor, info.rail);
    PutU16(cursor, 0);
    PutU32(cursor, info.sourceRegionId);
    PutU32(cursor, info.sourceAlignment);
    PutU64(cursor, info.sourceBytes);
    return payload;
}

bool DecodeReady(const void *data, uint32_t size, ReadyInfo &info)
{
    if (data == nullptr || size != kReadyWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kReadyMagic) {
        return false;
    }
    info.params = DecodeParams(cursor);
    info.rail = GetU16(cursor);
    if (GetU16(cursor) != 0) {
        return false;
    }
    info.sourceRegionId = GetU32(cursor);
    info.sourceAlignment = GetU32(cursor);
    info.sourceBytes = GetU64(cursor);
    return true;
}

uint16_t RailForRequestIndex(uint32_t index, uint16_t links)
{
    return static_cast<uint16_t>(index / (kBlocks / links));
}

uint32_t LocalIndexForRequest(uint32_t index, uint16_t links)
{
    return index % (kBlocks / links);
}

std::array<CopyEntry, kBlocks> MakeCopyEntries(uint64_t seed, uint16_t links)
{
    std::array<CopyEntry, kBlocks> entries{};
    const uint32_t blocksPerRail = kBlocks / links;
    const uint32_t shift = static_cast<uint32_t>(seed % blocksPerRail);
    for (uint32_t index = 0; index < kBlocks; ++index) {
        const uint32_t localIndex = LocalIndexForRequest(index, links);
        const uint32_t sourceSlot = (localIndex * 7U + shift * 13U) % blocksPerRail;
        const uint32_t destinationSlot = (localIndex * 11U + shift * 17U) % blocksPerRail;
        entries[index].remoteSourceOffset = static_cast<uint64_t>(sourceSlot) * kStrideBytes;
        entries[index].localDestinationOffset = static_cast<uint64_t>(destinationSlot) * kStrideBytes;
    }
    return entries;
}

bool ValidateCopyEntries(const std::array<CopyEntry, kBlocks> &entries, uint16_t links,
    uint64_t sourceBytesPerRail, uint64_t destinationBytesPerRail, std::string &error)
{
    const uint32_t blocksPerRail = kBlocks / links;
    std::array<std::array<bool, kMaxBlocksPerRail>, kMaxLinks> destinationsSeen{};
    for (uint32_t index = 0; index < kBlocks; ++index) {
        const uint16_t rail = RailForRequestIndex(index, links);
        const CopyEntry &entry = entries[index];
        if (entry.remoteSourceOffset % kStrideBytes != 0 || sourceBytesPerRail < kBlockBytes ||
            entry.remoteSourceOffset > sourceBytesPerRail - kBlockBytes) {
            error = "source offset is unaligned or out of range at request " + std::to_string(index) +
                " rail " + std::to_string(rail);
            return false;
        }
        if (entry.localDestinationOffset % kStrideBytes != 0 || destinationBytesPerRail < kBlockBytes ||
            entry.localDestinationOffset > destinationBytesPerRail - kBlockBytes) {
            error = "destination offset is unaligned or out of range at request " + std::to_string(index) +
                " rail " + std::to_string(rail);
            return false;
        }
        const uint64_t destinationSlot = entry.localDestinationOffset / kStrideBytes;
        if (destinationSlot >= blocksPerRail || destinationsSeen[rail][static_cast<size_t>(destinationSlot)]) {
            error = "destination slots must be unique within each request-index rail at request " +
                std::to_string(index);
            return false;
        }
        destinationsSeen[rail][static_cast<size_t>(destinationSlot)] = true;
    }
    return true;
}

std::array<uint8_t, kCopyReqWireBytes> EncodeCopyRequest(
    uint64_t generation, uint16_t links, const std::array<CopyEntry, kBlocks> &entries)
{
    std::array<uint8_t, kCopyReqWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kCopyReqMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpCopyReq);
    PutU64(cursor, generation);
    PutU32(cursor, kBlocks);
    PutU32(cursor, kBlocks);
    PutU32(cursor, kBlockBytes);
    PutU16(cursor, kModeDirect);
    PutU16(cursor, links);
    PutU16(cursor, 0);
    PutU16(cursor, kSourceFormatDirectPairs);
    PutU32(cursor, kSourceRegionId);
    PutU32(cursor, kDestinationRegionId);
    PutU32(cursor, 0);
    PutU32(cursor, kCopyReqHeaderBytes);
    PutU32(cursor, kCopyReqDescriptorBytes);
    PutU32(cursor, static_cast<uint32_t>(kPayloadBytes));
    PutU32(cursor, 0);
    for (const CopyEntry &entry : entries) {
        PutU64(cursor, entry.remoteSourceOffset);
        PutU64(cursor, entry.localDestinationOffset);
    }
    return payload;
}

bool DecodeCopyRequest(const void *data, uint32_t size, uint64_t expectedGeneration, uint16_t expectedLinks,
    uint64_t sourceBytesPerRail, uint64_t destinationBytesPerRail,
    std::array<CopyEntry, kBlocks> &entries, std::string &error)
{
    const auto fail = [&error](const std::string &message) { error = message; return false; };
    if (data == nullptr || size != kCopyReqWireBytes) {
        return fail("COPY_REQ size is not exactly 9664 bytes");
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kCopyReqMagic || GetU16(cursor) != kProtocolVersion ||
        GetU16(cursor) != kOpCopyReq) {
        return fail("COPY_REQ magic/version/opcode mismatch");
    }
    if (GetU64(cursor) != expectedGeneration || GetU32(cursor) != kBlocks ||
        GetU32(cursor) != kBlocks || GetU32(cursor) != kBlockBytes ||
        GetU16(cursor) != kModeDirect || GetU16(cursor) != expectedLinks ||
        GetU16(cursor) != 0 || GetU16(cursor) != kSourceFormatDirectPairs ||
        GetU32(cursor) != kSourceRegionId || GetU32(cursor) != kDestinationRegionId ||
        GetU32(cursor) != 0 || GetU32(cursor) != kCopyReqHeaderBytes ||
        GetU32(cursor) != kCopyReqDescriptorBytes || GetU32(cursor) != kPayloadBytes ||
        GetU32(cursor) != 0) {
        return fail("COPY_REQ header fields do not match direct case");
    }
    for (CopyEntry &entry : entries) {
        entry.remoteSourceOffset = GetU64(cursor);
        entry.localDestinationOffset = GetU64(cursor);
    }
    return ValidateCopyEntries(entries, expectedLinks, sourceBytesPerRail, destinationBytesPerRail, error);
}

std::array<uint8_t, kDataDoneWireBytes> EncodeDataDone(uint64_t generation, uint16_t rail,
    uint32_t blocksOnRail)
{
    std::array<uint8_t, kDataDoneWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kDataDoneMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpDataDone);
    PutU64(cursor, generation);
    PutU16(cursor, rail);
    PutU16(cursor, 0);
    PutU32(cursor, blocksOnRail);
    PutU32(cursor, blocksOnRail * kBlockBytes);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeDataDone(const void *data, uint32_t size, uint16_t expectedRail, uint32_t expectedBlocks,
    uint64_t &generation)
{
    if (data == nullptr || size != kDataDoneWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    return GetU32(cursor) == kDataDoneMagic && GetU16(cursor) == kProtocolVersion &&
        GetU16(cursor) == kOpDataDone && (generation = GetU64(cursor)) != 0 &&
        GetU16(cursor) == expectedRail && GetU16(cursor) == 0 &&
        GetU32(cursor) == expectedBlocks && GetU32(cursor) == expectedBlocks * kBlockBytes &&
        GetU32(cursor) == 0;
}

std::array<uint8_t, kCopyErrorWireBytes> EncodeCopyError(
    uint64_t generation, uint32_t stage, uint32_t errorCode, uint32_t detail)
{
    std::array<uint8_t, kCopyErrorWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kCopyErrorMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpCopyError);
    PutU64(cursor, generation);
    PutU32(cursor, stage);
    PutU32(cursor, errorCode);
    PutU32(cursor, detail);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeCopyError(const void *data, uint32_t size, uint64_t &generation,
    uint32_t &stage, uint32_t &errorCode, uint32_t &detail)
{
    if (data == nullptr || size != kCopyErrorWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kCopyErrorMagic || GetU16(cursor) != kProtocolVersion ||
        GetU16(cursor) != kOpCopyError) {
        return false;
    }
    generation = GetU64(cursor);
    stage = GetU32(cursor);
    errorCode = GetU32(cursor);
    detail = GetU32(cursor);
    return GetU32(cursor) == 0;
}

std::array<uint8_t, kTokenWireBytes> EncodeToken(
    uint32_t magic, uint16_t opcode, uint64_t generation, uint16_t rail)
{
    std::array<uint8_t, kTokenWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, magic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, opcode);
    PutU64(cursor, generation);
    PutU16(cursor, rail);
    PutU16(cursor, 0);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeToken(const void *data, uint32_t size, uint32_t magic, uint16_t opcode,
    uint64_t &generation, uint16_t &rail)
{
    if (data == nullptr || size != kTokenWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != magic || GetU16(cursor) != kProtocolVersion || GetU16(cursor) != opcode) {
        return false;
    }
    generation = GetU64(cursor);
    rail = GetU16(cursor);
    return GetU16(cursor) == 0 && GetU32(cursor) == 0;
}

uint64_t PatternWord(uint64_t generation, uint32_t globalBlock, uint32_t wordIndex)
{
    return 0x9e3779b97f4a7c15ULL ^ (generation * 0x100000001b3ULL) ^
        (static_cast<uint64_t>(globalBlock) << 32U) ^ wordIndex;
}

void FillBlock(uint8_t *address, uint64_t generation, uint32_t globalBlock)
{
    auto *words = reinterpret_cast<uint64_t *>(address);
    for (uint32_t word = 0; word < kBlockBytes / sizeof(uint64_t); ++word) {
        words[word] = PatternWord(generation, globalBlock, word);
    }
}

bool VerifyBlock(const uint8_t *address, uint64_t generation, uint32_t globalBlock, std::string &error)
{
    const auto *words = reinterpret_cast<const uint64_t *>(address);
    for (uint32_t word = 0; word < kBlockBytes / sizeof(uint64_t); ++word) {
        const uint64_t expected = PatternWord(generation, globalBlock, word);
        if (words[word] != expected) {
            std::ostringstream stream;
            stream << "data mismatch: generation=" << generation << " block=" << globalBlock << " word=" << word
                   << " expected=0x" << std::hex << expected << " actual=0x" << words[word];
            error = stream.str();
            return false;
        }
    }
    return true;
}

bool VerifyGap(const uint8_t *address, std::string &error)
{
    for (uint32_t offset = kBlockBytes; offset < kStrideBytes; ++offset) {
        if (address[offset] != kDstGapSentinel) {
            error = "destination stride gap was modified at offset " + std::to_string(offset);
            return false;
        }
    }
    return true;
}

std::string RoleName(Role role)
{
    return role == Role::Local ? "local" : "remote";
}

std::string KindName(RunKind kind)
{
    switch (kind) {
        case RunKind::Verify:
            return "verify";
        case RunKind::Measure:
            return "measure";
        case RunKind::Trace:
            return "trace";
    }
    return "unknown";
}

std::string CaseName(uint16_t links)
{
    return links == 1 ? "B1" : "B2";
}

uint64_t ParseUnsigned(const std::string &name, const std::string &value, uint64_t maximum)
{
    if (value.empty()) {
        throw std::runtime_error(name + " cannot be empty");
    }
    char *end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0' || parsed > maximum) {
        throw std::runtime_error("invalid " + name + ": " + value);
    }
    return parsed;
}

int ParseSignedCpu(const std::string &name, const std::string &value)
{
    if (value == "-1") {
        return -1;
    }
    return static_cast<int>(ParseUnsigned(name, value, static_cast<uint64_t>(std::numeric_limits<int>::max())));
}

std::vector<std::string> SplitCsv(const std::string &name, const std::string &value)
{
    std::vector<std::string> result;
    size_t start = 0;
    while (start <= value.size()) {
        const size_t comma = value.find(',', start);
        const std::string item = value.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (item.empty()) {
            throw std::runtime_error(name + " contains an empty item");
        }
        result.push_back(item);
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return result;
}

std::vector<int> ParseCpuCsv(const std::string &name, const std::string &value)
{
    std::vector<int> cpus;
    for (const std::string &item : SplitCsv(name, value)) {
        cpus.push_back(ParseSignedCpu(name, item));
    }
    return cpus;
}

void PrintUsage(std::ostream &stream)
{
    stream << "Usage:\n"
           << "  rdma_600 --role remote --rdma-ips <ip0[,ip1]> --listen <ep0[,ep1]> [options]\n"
           << "  rdma_600 --role local --rdma-ips <ip0[,ip1]> --peer <ep0[,ep1]> [options]\n"
           << "  rdma_600 --self-test\n\n"
           << "Requester-driven direct cases: --links 1 (B1) or --links 2 (B2), --mode direct.\n"
           << "Singular --rdma-ip/--app-cpu/--worker-cpu remain aliases for links=1.\n"
           << "Options: --kind verify|measure|trace --verify-rounds N --warmup N --rounds N\n"
           << "         --trace-rounds N (1..64 for trace) --timeout-sec N\n"
           << "         --app-cpus <cpu0[,cpu1]> --worker-cpus <cpu0[,cpu1]>\n"
           << "B2 uses two persistent rail-affine application threads; each app CPU must be distinct.\n";
}

Options ParseOptions(int argc, char **argv)
{
    std::map<std::string, std::string> values;
    bool selfTest = false;
    for (int index = 1; index < argc; ++index) {
        const std::string argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            PrintUsage(std::cout);
            std::exit(0);
        }
        if (argument == "--self-test") {
            selfTest = true;
            continue;
        }
        if (argument.rfind("--", 0) != 0 || index + 1 >= argc) {
            throw std::runtime_error("expected an option and value; use --help");
        }
        const std::string value(argv[++index]);
        if (!values.emplace(argument, value).second) {
            throw std::runtime_error("duplicate option: " + argument);
        }
    }

    Options options;
    options.selfTest = selfTest;
    if (selfTest) {
        if (!values.empty()) {
            throw std::runtime_error("--self-test cannot be combined with RDMA options");
        }
        return options;
    }

    static const std::array<std::string, 17> kAllowedOptions = {
        "--role", "--rdma-ip", "--rdma-ips", "--listen", "--peer", "--kind", "--verify-rounds", "--warmup",
        "--rounds", "--trace-rounds", "--timeout-sec", "--app-cpu", "--app-cpus", "--worker-cpu",
        "--worker-cpus", "--links", "--mode"};
    for (const auto &entry : values) {
        if (std::find(kAllowedOptions.begin(), kAllowedOptions.end(), entry.first) == kAllowedOptions.end()) {
            throw std::runtime_error("unknown option: " + entry.first);
        }
    }

    const auto required = [&values](const std::string &name) -> const std::string & {
        const auto it = values.find(name);
        if (it == values.end() || it->second.empty()) {
            throw std::runtime_error("missing required option " + name);
        }
        return it->second;
    };
    const auto optional = [&values](const std::string &name, const std::string &fallback) {
        const auto it = values.find(name);
        return it == values.end() ? fallback : it->second;
    };

    options.links = static_cast<uint16_t>(ParseUnsigned("--links", optional("--links", "1"), kMaxLinks));
    if (options.links == 0 || kBlocks % options.links != 0) {
        throw std::runtime_error("--links must be 1 or 2 and evenly divide 600");
    }
    if (optional("--mode", "direct") != "direct") {
        throw std::runtime_error("this binary supports only direct Put; no SGL/staging/scatter mode is available");
    }

    const std::string role = required("--role");
    if (role == "local") {
        options.role = Role::Local;
        options.endpoints = SplitCsv("--peer", required("--peer"));
        if (values.count("--listen") != 0) {
            throw std::runtime_error("--listen is only valid for remote");
        }
    } else if (role == "remote") {
        options.role = Role::Remote;
        options.endpoints = SplitCsv("--listen", required("--listen"));
        if (values.count("--peer") != 0) {
            throw std::runtime_error("--peer is only valid for local");
        }
    } else {
        throw std::runtime_error("--role must be local or remote");
    }

    if (values.count("--rdma-ip") != 0 && values.count("--rdma-ips") != 0) {
        throw std::runtime_error("use only one of --rdma-ip and --rdma-ips");
    }
    options.rdmaIps = SplitCsv("--rdma-ips",
        values.count("--rdma-ips") != 0 ? values.at("--rdma-ips") : required("--rdma-ip"));

    if (values.count("--worker-cpu") != 0 && values.count("--worker-cpus") != 0) {
        throw std::runtime_error("use only one of --worker-cpu and --worker-cpus");
    }
    if (values.count("--worker-cpus") != 0) {
        options.workerCpus = ParseCpuCsv("--worker-cpus", values.at("--worker-cpus"));
    } else {
        options.workerCpus = {ParseSignedCpu("--worker-cpu", optional("--worker-cpu", "-1"))};
    }

    if (values.count("--app-cpu") != 0 && values.count("--app-cpus") != 0) {
        throw std::runtime_error("use only one of --app-cpu and --app-cpus");
    }
    if (values.count("--app-cpus") != 0) {
        options.appCpus = ParseCpuCsv("--app-cpus", values.at("--app-cpus"));
    } else if (options.links == 1) {
        options.appCpus = {ParseSignedCpu("--app-cpu", optional("--app-cpu", "-1"))};
    } else if (values.count("--app-cpu") != 0) {
        throw std::runtime_error("B2 requires --app-cpus with one fixed application CPU per rail");
    } else {
        options.appCpus.assign(options.links, -1);
    }

    if (options.rdmaIps.size() != options.links || options.endpoints.size() != options.links ||
        options.appCpus.size() != options.links || options.workerCpus.size() != options.links) {
        throw std::runtime_error("RDMA IP, OOB endpoint, app CPU, and worker CPU counts must all equal --links");
    }
    if (options.links == 2 && (options.rdmaIps[0] == options.rdmaIps[1] ||
            options.endpoints[0] == options.endpoints[1] || options.appCpus[0] == options.appCpus[1] ||
            options.workerCpus[0] == options.workerCpus[1])) {
        throw std::runtime_error("B2 requires two distinct RDMA IPs, OOB endpoints, app CPUs, and worker CPUs");
    }

    const std::string kind = optional("--kind", "measure");
    if (kind == "verify") {
        options.kind = RunKind::Verify;
    } else if (kind == "measure") {
        options.kind = RunKind::Measure;
    } else if (kind == "trace") {
        options.kind = RunKind::Trace;
    } else {
        throw std::runtime_error("--kind must be verify, measure, or trace");
    }
    options.verifyRounds = static_cast<uint32_t>(ParseUnsigned("--verify-rounds", optional("--verify-rounds", "20"),
        std::numeric_limits<uint32_t>::max()));
    options.warmupRounds = static_cast<uint32_t>(ParseUnsigned("--warmup", optional("--warmup", "1000"),
        std::numeric_limits<uint32_t>::max()));
    options.measureRounds = static_cast<uint32_t>(ParseUnsigned("--rounds", optional("--rounds", "10000"),
        std::numeric_limits<uint32_t>::max()));
    options.traceRounds = static_cast<uint32_t>(ParseUnsigned("--trace-rounds", optional("--trace-rounds", "0"),
        kMaxTraceRounds));
    options.timeoutSec = static_cast<uint32_t>(ParseUnsigned("--timeout-sec", optional("--timeout-sec", "10"), 32767));
    if (options.verifyRounds == 0) {
        throw std::runtime_error("--verify-rounds must be greater than zero");
    }
    if (options.timeoutSec == 0) {
        throw std::runtime_error("--timeout-sec must be greater than zero");
    }
    for (int appCpu : options.appCpus) {
        for (int workerCpu : options.workerCpus) {
            if (appCpu >= 0 && appCpu == workerCpu) {
                throw std::runtime_error("every app CPU must differ from every worker CPU");
            }
        }
    }
    if (options.kind == RunKind::Verify) {
        if (options.traceRounds != 0) {
            throw std::runtime_error("--trace-rounds is only valid for --kind trace");
        }
        options.warmupRounds = 0;
        options.measureRounds = 0;
        options.traceRounds = 0;
    } else if (options.kind == RunKind::Measure) {
        if (options.traceRounds != 0) {
            throw std::runtime_error("--trace-rounds is only valid for --kind trace");
        }
        options.traceRounds = 0;
        if (options.measureRounds == 0) {
            throw std::runtime_error("--rounds must be greater than zero for --kind measure");
        }
    } else {
        options.warmupRounds = 0;
        options.measureRounds = 0;
        if (options.traceRounds == 0) {
            throw std::runtime_error("--trace-rounds must be in [1,64] for --kind trace");
        }
    }
    if (options.kind == RunKind::Measure &&
        (std::any_of(options.appCpus.begin(), options.appCpus.end(), [](int cpu) { return cpu < 0; }) ||
            std::any_of(options.workerCpus.begin(), options.workerCpus.end(), [](int cpu) { return cpu < 0; }))) {
        throw std::runtime_error(
            "measure requires explicit app and per-rail worker CPUs for pinned busy polling");
    }
    return options;
}

void PinCurrentThread(int cpu)
{
    if (cpu < 0) {
        return;
    }
    if (cpu >= CPU_SETSIZE) {
        throw std::runtime_error("requested CPU exceeds CPU_SETSIZE: " + std::to_string(cpu));
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        throw std::runtime_error("sched_setaffinity failed for CPU " + std::to_string(cpu) + ": " +
            std::strerror(errno));
    }
}

inline void CpuRelax() noexcept
{
#if defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

uint64_t PercentileNs(const std::vector<uint64_t> &samples, double percentile)
{
    if (samples.empty()) {
        return 0;
    }
    std::vector<uint64_t> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const size_t index = static_cast<size_t>(percentile * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

double AverageNs(const std::vector<uint64_t> &samples)
{
    if (samples.empty()) {
        return 0.0;
    }
    long double total = 0.0;
    for (const uint64_t sample : samples) {
        total += static_cast<long double>(sample);
    }
    return static_cast<double>(total / static_cast<long double>(samples.size()));
}

double NsToUs(uint64_t nanoseconds)
{
    return static_cast<double>(nanoseconds) / 1000.0;
}

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
    TracePoint localRequestPrepared;
    TracePoint localRequestPosted;
    std::array<TracePoint, kMaxLinks> localDataDone;
    TracePoint localEnd;
    TracePoint remoteRequestReceived;
    TracePoint remoteProcessingStarted;
    TracePoint remoteRequestDecoded;
    std::array<TracePoint, kMaxLinks> remotePutRequestsBuilt;
    std::array<TracePoint, kMaxLinks> remotePutLoopStarted;
    std::array<TracePoint, kMaxLinks> remotePutLoopThreadCpuStarted;
    std::array<std::array<TracePoint, kMaxPutTraceCheckpoints>, kMaxLinks> remotePutCheckpoints;
    std::array<TracePoint, kMaxLinks> remotePutLoopEnded;
    std::array<TracePoint, kMaxLinks> remotePutLoopThreadCpuEnded;
    std::array<TracePoint, kMaxLinks> remotePosted;
    std::array<TracePoint, kMaxLinks> remoteDataCallbacksDone;
    std::array<TracePoint, kMaxLinks> remoteDonePosted;
    std::array<TracePoint, kMaxLinks> remoteCallbacksDrained;
};

bool RunSelfTest()
{
    try {
        for (uint16_t links : {uint16_t{1}, uint16_t{2}}) {
            CaseParameters parameters;
            parameters.links = links;
            parameters.verifyRounds = 20;
            parameters.traceRounds = 3;
            const uint64_t railBytes = static_cast<uint64_t>(kBlocks / links) * kStrideBytes;
            for (uint16_t rail = 0; rail < links; ++rail) {
                UBSHcomMemoryKey key{};
                for (size_t index = 0; index < std::size(key.keys); ++index) {
                    key.keys[index] = 0x1000U + index + rail;
                    key.tokens[index] = 0x2000U + index + rail;
                }
                for (size_t index = 0; index < std::size(key.eid); ++index) {
                    key.eid[index] = static_cast<uint8_t>(index + rail);
                }

                HelloInfo helloInfo{parameters, rail, kDestinationRegionId, 0x12345000U + railBytes * rail,
                    railBytes, key};
                const auto hello = EncodeHello(helloInfo);
                HelloInfo decodedHello{};
                if (!DecodeHello(hello.data(), static_cast<uint32_t>(hello.size()), decodedHello) ||
                    !SameParams(parameters, decodedHello.params) || decodedHello.rail != rail ||
                    decodedHello.destinationRegionId != kDestinationRegionId ||
                    decodedHello.destinationAddress != helloInfo.destinationAddress ||
                    decodedHello.destinationBytes != railBytes ||
                    std::memcmp(&decodedHello.destinationKey, &key, sizeof(key)) != 0) {
                    throw std::runtime_error("HELLO wire round trip failed");
                }

                ReadyInfo ready{parameters, rail, kSourceRegionId, kStrideBytes, railBytes};
                const auto readyWire = EncodeReady(ready);
                ReadyInfo decodedReady{};
                if (!DecodeReady(readyWire.data(), static_cast<uint32_t>(readyWire.size()), decodedReady) ||
                    !SameParams(ready.params, decodedReady.params) || decodedReady.rail != rail ||
                    decodedReady.sourceRegionId != kSourceRegionId ||
                    decodedReady.sourceAlignment != kStrideBytes || decodedReady.sourceBytes != railBytes) {
                    throw std::runtime_error("READY wire round trip failed");
                }

                const auto token = EncodeToken(kFinishMagic, kOpFinish, 7, rail);
                uint64_t decodedGeneration = 0;
                uint16_t decodedRail = kMaxLinks;
                if (!DecodeToken(token.data(), static_cast<uint32_t>(token.size()), kFinishMagic, kOpFinish,
                        decodedGeneration, decodedRail) || decodedGeneration != 7 || decodedRail != rail) {
                    throw std::runtime_error("FINISH wire round trip failed");
                }
            }

            const uint32_t blocksPerRail = kBlocks / links;
            constexpr uint64_t generation = 7;
            const auto entries = MakeCopyEntries(generation, links);
            std::string validationError;
            if (!ValidateCopyEntries(entries, links, railBytes, railBytes, validationError)) {
                throw std::runtime_error("generated sparse mapping failed validation: " + validationError);
            }
            const auto request = EncodeCopyRequest(generation, links, entries);
            std::array<CopyEntry, kBlocks> decodedEntries{};
            if (!DecodeCopyRequest(request.data(), static_cast<uint32_t>(request.size()), generation, links,
                    railBytes, railBytes, decodedEntries, validationError) ||
                std::memcmp(entries.data(), decodedEntries.data(), sizeof(entries)) != 0) {
                throw std::runtime_error("COPY_REQ wire round trip failed: " + validationError);
            }
            if (DecodeCopyRequest(request.data(), static_cast<uint32_t>(request.size() - 1), generation, links,
                    railBytes, railBytes, decodedEntries, validationError)) {
                throw std::runtime_error("truncated COPY_REQ was accepted");
            }
            auto duplicateRequest = request;
            const uint32_t duplicateIndex = blocksPerRail > 1 ? 1 : 0;
            std::copy_n(duplicateRequest.data() + kCopyReqHeaderBytes + 8, 8,
                duplicateRequest.data() + kCopyReqHeaderBytes + duplicateIndex * kCopyEntryWireBytes + 8);
            if (DecodeCopyRequest(duplicateRequest.data(), static_cast<uint32_t>(duplicateRequest.size()),
                    generation, links, railBytes, railBytes, decodedEntries, validationError)) {
                throw std::runtime_error("duplicate destination within one rail was accepted");
            }

            std::array<AlignedBuffer, kMaxLinks> source;
            std::array<AlignedBuffer, kMaxLinks> destination;
            for (uint16_t rail = 0; rail < links; ++rail) {
                const size_t bytes = static_cast<size_t>(blocksPerRail) * kStrideBytes;
                source[rail].Allocate(bytes);
                destination[rail].Allocate(bytes);
                std::memset(source[rail].Data(), kSourceGapSentinel, bytes);
                std::memset(destination[rail].Data(), kDstGapSentinel, bytes);
                for (uint32_t localBlock = 0; localBlock < blocksPerRail; ++localBlock) {
                    const uint32_t globalBlock = rail * blocksPerRail + localBlock;
                    FillBlock(source[rail].Data() + static_cast<size_t>(localBlock) * kStrideBytes,
                        generation, globalBlock);
                }
            }
            for (uint32_t index = 0; index < kBlocks; ++index) {
                const uint16_t rail = RailForRequestIndex(index, links);
                std::memcpy(destination[rail].Data() + entries[index].localDestinationOffset,
                    source[rail].Data() + entries[index].remoteSourceOffset, kBlockBytes);
            }
            for (uint32_t index = 0; index < kBlocks; ++index) {
                    const uint16_t rail = RailForRequestIndex(index, links);
                    const uint32_t sourceSlot = static_cast<uint32_t>(entries[index].remoteSourceOffset / kStrideBytes);
                    const uint32_t globalBlock = rail * blocksPerRail + sourceSlot;
                    const uint8_t *address = destination[rail].Data() + entries[index].localDestinationOffset;
                    std::string error;
                    if (!VerifyBlock(address, generation, globalBlock, error) || !VerifyGap(address, error)) {
                        throw std::runtime_error(error);
                    }
            }

            for (uint16_t rail = 0; rail < links; ++rail) {
                const auto done = EncodeDataDone(generation, rail, blocksPerRail);
                uint64_t decodedGeneration = 0;
                if (!DecodeDataDone(done.data(), static_cast<uint32_t>(done.size()), rail, blocksPerRail,
                        decodedGeneration) || decodedGeneration != generation) {
                    throw std::runtime_error("DATA_DONE wire round trip failed");
                }
            }
        }
        std::cout << "SELF_TEST: PASS (sparse-copy-v4, B1/B2, 9664-byte request, 600 direct blocks)"
                  << std::endl;
        return true;
    } catch (const std::exception &error) {
        std::cerr << "SELF_TEST: FAIL: " << error.what() << std::endl;
        return false;
    }
}

#ifndef RDMA_600_SELF_TEST_ONLY

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
    uintptr_t peerDestinationAddress = 0;
    uint64_t peerDestinationBytes = 0;
    uint64_t peerSourceBytes = 0;
    std::array<UBSHcomOneSideRequest, kMaxBlocksPerRail> putRequests{};
    std::array<uint8_t, kHelloWireBytes> helloPayload{};
    std::array<uint8_t, kReadyWireBytes> readyPayload{};
    std::array<uint8_t, kReadyWireBytes> readyResponse{};
    std::array<uint8_t, kDataDoneWireBytes> dataDonePayload{};
    std::array<uint8_t, kTokenWireBytes> finishPayload{};
    std::array<uint8_t, kTokenWireBytes> finishAckPayload{};
    std::atomic<bool> helloClaimed{false};
    std::atomic<bool> helloSeen{false};
    std::atomic<uint64_t> dataDoneGeneration{0};
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
    Finish,
    Teardown,
};

// B2 keeps rail 0 on the original application thread and gives rail 1 one
// persistent application thread. A release/acquire command sequence publishes
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
};

class SparseCopyBenchmark {
public:
    explicit SparseCopyBenchmark(Options options) : mOptions(std::move(options))
    {
        mParams.links = mOptions.links;
        mParams.verifyRounds = mOptions.verifyRounds;
        mParams.warmupRounds = mOptions.warmupRounds;
        mParams.measureRounds = mOptions.measureRounds;
        mParams.traceRounds = mOptions.traceRounds;
        mBlocksPerRail = kBlocks / mOptions.links;
        if (TraceEnabled()) {
            const uint64_t first = static_cast<uint64_t>(mParams.verifyRounds) + 1;
            for (uint32_t i = 0; i < mParams.traceRounds; ++i) mTrace[i].generation = first + i;
        }
    }

    int Run()
    {
        try {
            PinCurrentThread(mOptions.appCpus[0]);
            StartSecondaryRailThread();
            SetupFixedRails();
            if (mOptions.role == Role::Remote) {
                mRemoteReady.store(true, std::memory_order_release);
                PrintListening();
                WaitForChannels();
                RunRemote();
                PrintRemoteStatus();
            } else {
                ConnectLocal();
                RunLocal();
                PrintLocalResult();
            }
            CheckFatal("normal completion");
            if (!DrainUntilComplete()) throw std::runtime_error("callbacks did not drain before teardown");
            TeardownFixedRails();
            StopSecondaryRailThread();
            if (TraceEnabled()) EmitTrace();
            return 0;
        } catch (const std::exception &error) {
            RecordFailure(error.what());
            std::cerr << "ERROR: " << error.what() << std::endl;
            if (!QuiesceSecondaryNoThrow()) {
                std::cerr << "FATAL: rail 1 did not quiesce; exiting without racing its counters" << std::endl;
                std::_Exit(2);
            }
            if (!DrainUntilComplete()) {
                std::cerr << "FATAL: callbacks did not drain; exiting without unsafe teardown" << std::endl;
                std::_Exit(2);
            }
            TryTeardownFixedRails();
            StopSecondaryRailThread();
            return 1;
        }
    }

private:
    bool TraceEnabled() const { return mOptions.kind == RunKind::Trace; }

    bool TraceIndex(uint64_t generation, size_t &index) const noexcept
    {
        if (!TraceEnabled()) return false;
        const uint64_t first = static_cast<uint64_t>(mParams.verifyRounds) + 1;
        if (generation < first || generation >= first + mParams.traceRounds) return false;
        index = static_cast<size_t>(generation - first);
        return true;
    }

    void PublishCallbackTrace(uint64_t generation, TracePoint &point, const char *event) noexcept
    {
        size_t index = 0;
        if (!TraceIndex(generation, index)) return;
        uint64_t timestamp = 0;
        if (!TryNowNs(timestamp)) {
            RecordFailure(std::string("clock_gettime failed while recording ") + event);
            return;
        }
        point.Publish(timestamp);
    }

    void StartSecondaryRailThread()
    {
        if (mOptions.links != 2) return;
        mSecondary.thread = std::thread([this] { SecondaryRailThreadMain(); });
        WaitControl("secondary rail thread startup",
            [this] { return mSecondary.started.load(std::memory_order_acquire); });
    }

    void SecondaryRailThreadMain() noexcept
    {
        try {
            PinCurrentThread(mOptions.appCpus[1]);
            mSecondary.started.store(true, std::memory_order_release);
            uint64_t seen = 0;
            while (!mSecondary.stop.load(std::memory_order_acquire)) {
                const uint64_t issued = mSecondary.issued.load(std::memory_order_acquire);
                if (issued == seen) { CpuRelax(); continue; }
                const RailCommand command = mSecondary.command.load(std::memory_order_relaxed);
                const uint64_t generation = mSecondary.generation.load(std::memory_order_relaxed);
                try {
                    switch (command) {
                        case RailCommand::Setup: SetupRail(1); break;
                        case RailCommand::ConnectAndHandshake: ConnectAndHandshakeRail(1); break;
                        case RailCommand::ProcessRemoteRound: ProcessRemoteRail(1, generation); break;
                        case RailCommand::Finish: FinishRail(1); break;
                        case RailCommand::Teardown: TeardownRail(1); break;
                        case RailCommand::None: throw std::runtime_error("empty secondary rail command");
                    }
                } catch (const std::exception &error) {
                    RecordFailure(std::string("rail 1 application thread: ") + error.what());
                } catch (...) {
                    RecordFailure("rail 1 application thread: unknown exception");
                }
                seen = issued;
                mSecondary.completed.store(seen, std::memory_order_release);
            }
        } catch (const std::exception &error) {
            mSecondary.started.store(true, std::memory_order_release);
            RecordFailure(std::string("rail 1 startup: ") + error.what());
        }
    }

    uint64_t IssueSecondaryRailCommand(RailCommand command, uint64_t generation = 0)
    {
        if (mOptions.links != 2 || !mSecondary.thread.joinable())
            throw std::runtime_error("secondary rail thread is unavailable");
        const uint64_t previous = mSecondary.issued.load(std::memory_order_relaxed);
        if (mSecondary.completed.load(std::memory_order_acquire) != previous)
            throw std::runtime_error("secondary rail already has an outstanding command");
        mSecondary.generation.store(generation, std::memory_order_relaxed);
        mSecondary.command.store(command, std::memory_order_relaxed);
        mSecondary.issued.store(previous + 1, std::memory_order_release);
        return previous + 1;
    }

    void WaitSecondaryRailCommand(uint64_t sequence, const char *what)
    {
        WaitData(what, [this, sequence] {
            return mSecondary.completed.load(std::memory_order_acquire) >= sequence;
        });
    }

    bool WaitSecondaryNoThrow(uint64_t sequence) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        while (mSecondary.completed.load(std::memory_order_acquire) < sequence &&
            std::chrono::steady_clock::now() < deadline) CpuRelax();
        return mSecondary.completed.load(std::memory_order_acquire) >= sequence;
    }

    bool QuiesceSecondaryNoThrow() noexcept
    {
        if (mOptions.links != 2 || !mSecondary.thread.joinable()) return true;
        return WaitSecondaryNoThrow(mSecondary.issued.load(std::memory_order_acquire));
    }

    void StopSecondaryRailThread() noexcept
    {
        if (!mSecondary.thread.joinable()) return;
        mSecondary.stop.store(true, std::memory_order_release);
        mSecondary.thread.join();
    }

    void SetupFixedRails()
    {
        if (mOptions.links == 2) {
            const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::Setup);
            WaitSecondaryRailCommand(sequence, "rail 1 setup");
        }
        SetupRail(0);
    }

    void SetupRail(uint16_t rail)
    {
        RailState &state = mRails[rail];
        state.serviceName = "rdma600_" + RoleName(mOptions.role) + "_" + std::to_string(rail);
        UBSHcomServiceOptions options{};
        options.maxSendRecvDataSize = 16384;
        options.workerGroupThreadCount = 1;
        options.workerGroupMode = ock::hcom::NET_BUSY_POLLING;
        if (mOptions.workerCpus[rail] >= 0) {
            const auto cpu = static_cast<uint32_t>(mOptions.workerCpus[rail]);
            options.workerGroupCpuIdsRange = {cpu, cpu};
        }
        state.service = UBSHcomService::Create(UBSHcomServiceProtocol::RDMA, state.serviceName, options);
        if (state.service == nullptr)
            throw std::runtime_error("Create RDMA service failed on rail " + std::to_string(rail));
        UBSHcomTlsOptions tls{};
        tls.enableTls = false;
        state.service->SetTlsOptions(tls);
        state.service->SetDeviceIpMask({mOptions.rdmaIps[rail] + "/32"});
        UBSHcomMultiRailOptions multiRail{};
        multiRail.enable = false;
        state.service->SetMultiRailOptions(multiRail);
        state.service->SetSendQueueSize(1024);
        state.service->SetRecvQueueSize(256);
        state.service->SetCompletionQueueDepth(2048);
        state.service->SetQueuePrePostSize(128);
        state.service->SetPollingBatchSize(16);
        state.service->SetEnableMrCache(false);
        state.service->RegisterRecvHandler(
            [this, rail](UBSHcomServiceContext &context) { return OnIncoming(rail, context); });
        state.service->RegisterSendHandler([this](const UBSHcomServiceContext &) {
            ActiveCallbackGuard guard(mActiveCallbacks); return 0;
        });
        state.service->RegisterOneSideHandler([this](const UBSHcomServiceContext &) {
            ActiveCallbackGuard guard(mActiveCallbacks); return 0;
        });
        state.service->RegisterChannelBrokenHandler([this, rail](const UBSHcomChannelPtr &) {
            ActiveCallbackGuard guard(mActiveCallbacks);
            if (!mTearingDown.load(std::memory_order_acquire))
                RecordFailure("hcom channel broken on rail " + std::to_string(rail));
        }, UBSHcomChannelBrokenPolicy::BROKEN_ALL);
        if (mOptions.role == Role::Remote) {
            const int rc = state.service->Bind("tcp://" + mOptions.endpoints[rail],
                [this, rail](const std::string &, const UBSHcomChannelPtr &channel, const std::string &) {
                    return OnNewChannel(rail, channel);
                });
            RequireOk(rc, ("Bind rail " + std::to_string(rail)).c_str());
        }
        RequireOk(state.service->Start(), ("Start rail " + std::to_string(rail)).c_str());

        const size_t railBytes = static_cast<size_t>(mBlocksPerRail) * kStrideBytes;
        state.buffer.Allocate(railBytes);
        std::memset(state.buffer.Data(),
            mOptions.role == Role::Remote ? kSourceGapSentinel : kDstGapSentinel, state.buffer.Size());
        RequireOk(state.service->RegisterMemoryRegion(
            reinterpret_cast<uintptr_t>(state.buffer.Data()), state.buffer.Size(), state.memoryRegion),
            ("RegisterMemoryRegion rail " + std::to_string(rail)).c_str());
        state.memoryRegistered = true;
        state.memoryKey = {};
        state.memoryRegion.GetMemoryKey(state.memoryKey);
        if (state.memoryRegion.GetAddress() != reinterpret_cast<uintptr_t>(state.buffer.Data()) ||
            state.memoryRegion.GetSize() < state.buffer.Size())
            throw std::runtime_error("MR does not cover rail " + std::to_string(rail));
        if (mOptions.role == Role::Remote) FillRemoteSourceRail(rail, 0);
    }

    void PrintListening() const
    {
        std::ostringstream out;
        out << "LISTENING role=remote links=" << mOptions.links;
        for (uint16_t rail = 0; rail < mOptions.links; ++rail)
            out << " rail" << rail << "=" << mOptions.endpoints[rail] << "/" << mOptions.rdmaIps[rail];
        std::cout << out.str() << std::endl;
        std::cout.flush();
    }

    int OnNewChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept
    {
        ActiveCallbackGuard guard(mActiveCallbacks);
        if (mOptions.role != Role::Remote || rail >= mOptions.links || channel == nullptr) {
            RecordFailure("unexpected new channel"); return -1;
        }
        if (!ConfigureChannel(rail, channel)) return -1;
        {
            std::lock_guard<std::mutex> lock(mChannelsMutex);
            if (mRails[rail].channel != nullptr) {
                RecordFailure("duplicate channel on rail " + std::to_string(rail)); return -1;
            }
            mRails[rail].channel = channel;
        }
        mChannelCv.notify_all();
        return 0;
    }

    bool ConfigureChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept
    {
        try {
            channel->SetChannelTimeOut(static_cast<int16_t>(mOptions.timeoutSec),
                static_cast<int16_t>(mOptions.timeoutSec));
            UBSHcomTwoSideThreshold thresholds{};
            thresholds.splitThreshold = UINT32_MAX;
            thresholds.rndvThreshold = UINT32_MAX;
            const int rc = channel->SetTwoSideThreshold(thresholds);
            if (rc != 0) {
                RecordFailure("SetTwoSideThreshold failed on rail " + std::to_string(rail) +
                    ": " + std::to_string(rc));
                return false;
            }
            return true;
        } catch (const std::exception &error) {
            RecordFailure("ConfigureChannel failed on rail " + std::to_string(rail) + ": " + error.what());
            return false;
        }
    }

    void ConnectLocal()
    {
        if (mOptions.links == 1) { ConnectAndHandshakeRail(0); return; }
        const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::ConnectAndHandshake);
        ConnectAndHandshakeRail(0);
        WaitSecondaryRailCommand(sequence, "rail 1 connect and handshake");
    }

    void ConnectAndHandshakeRail(uint16_t rail)
    {
        UBSHcomConnectOptions options{};
        options.linkCount = 1;
        options.mode = UBSHcomClientPollingMode::WORKER_POLL;
        options.cbType = UBSHcomChannelCallBackType::CHANNEL_FUNC_CB;
        UBSHcomChannelPtr channel;
        RequireOk(mRails[rail].service->Connect("tcp://" + mOptions.endpoints[rail], channel, options),
            ("Connect rail " + std::to_string(rail)).c_str());
        if (channel == nullptr) throw std::runtime_error("Connect returned null channel");
        if (!ConfigureChannel(rail, channel)) throw std::runtime_error("ConfigureChannel after Connect failed");
        {
            std::lock_guard<std::mutex> lock(mChannelsMutex);
            mRails[rail].channel = channel;
        }
        RailState &state = mRails[rail];
        HelloInfo hello{mParams, rail, kDestinationRegionId,
            reinterpret_cast<uintptr_t>(state.buffer.Data()), state.buffer.Size(), state.memoryKey};
        state.helloPayload = EncodeHello(hello);
        UBSHcomRequest request(state.helloPayload.data(), static_cast<uint32_t>(state.helloPayload.size()), kOpHello);
        UBSHcomResponse response(state.readyResponse.data(), static_cast<uint32_t>(state.readyResponse.size()));
        RequireOk(channel->Call(request, response, nullptr), ("HELLO rail " + std::to_string(rail)).c_str());
        ReadyInfo ready{};
        const uint64_t railBytes = static_cast<uint64_t>(mBlocksPerRail) * kStrideBytes;
        if (!DecodeReady(response.address, response.size, ready) || !SameParams(mParams, ready.params) ||
            ready.rail != rail || ready.sourceRegionId != kSourceRegionId ||
            ready.sourceAlignment != kStrideBytes || ready.sourceBytes != railBytes)
            throw std::runtime_error("invalid READY on rail " + std::to_string(rail));
        state.peerSourceBytes = ready.sourceBytes;
    }

    int OnIncoming(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        ActiveCallbackGuard guard(mActiveCallbacks);
        const UBSHcomChannelPtr expected = ChannelCopy(rail);
        if (expected == nullptr || context.Channel() != expected) {
            RecordFailure("message on unexpected channel rail " + std::to_string(rail)); return -1;
        }
        if (context.Result() != 0) {
            RecordFailure("incoming context failed: " + std::to_string(context.Result())); return context.Result();
        }
        switch (context.OpCode()) {
            case kOpHello: return OnHello(rail, context);
            case kOpCopyReq: return OnCopyReq(rail, context);
            case kOpDataDone: return OnDataDone(rail, context);
            case kOpCopyError: return OnCopyError(rail, context);
            case kOpFinish: return OnFinish(rail, context);
            case kOpFinishAck: return OnFinishAck(rail, context);
            default: RecordFailure("unexpected opcode on rail " + std::to_string(rail)); return -1;
        }
    }

    int OnHello(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Remote || !mRemoteReady.load(std::memory_order_acquire)) {
            RecordFailure("HELLO before remote setup"); return -1;
        }
        HelloInfo hello{};
        const uint64_t railBytes = static_cast<uint64_t>(mBlocksPerRail) * kStrideBytes;
        if (!DecodeHello(context.MessageData(), context.MessageDataLen(), hello) ||
            !SameParams(hello.params, mParams) || hello.rail != rail ||
            hello.destinationRegionId != kDestinationRegionId || hello.destinationAddress == 0 ||
            hello.destinationBytes != railBytes) {
            RecordFailure("invalid HELLO on rail " + std::to_string(rail)); return -1;
        }
        RailState &state = mRails[rail];
        bool expected = false;
        if (!state.helloClaimed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            RecordFailure("duplicate HELLO"); return -1;
        }
        state.peerDestinationAddress = static_cast<uintptr_t>(hello.destinationAddress);
        state.peerDestinationBytes = hello.destinationBytes;
        state.peerDestinationKey = hello.destinationKey;
        ReadyInfo ready{mParams, rail, kSourceRegionId, kStrideBytes, state.buffer.Size()};
        state.readyPayload = EncodeReady(ready);
        Callback *callback = NewSendCallback(rail);
        if (callback == nullptr) { RecordFailure("READY callback allocation failed"); return -1; }
        state.callbackCounters.workerAttemptedSendCallbacks.fetch_add(1, std::memory_order_release);
        // One release publishes peer destination metadata, READY storage, and
        // callback accounting to the remote app before it can process COPY_REQ.
        state.helloSeen.store(true, std::memory_order_release);
        const UBSHcomRequest reply(state.readyPayload.data(), static_cast<uint32_t>(state.readyPayload.size()), kOpReady);
        const int rc = context.Channel()->Reply(UBSHcomReplyContext(context.RspCtx(), 0), reply, callback);
        if (rc != 0) RecordFailure("READY Reply failed: " + std::to_string(rc));
        return rc;
    }

    int OnCopyReq(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Remote || rail != 0 || !AllHellosSeen()) {
            RecordFailure("COPY_REQ must use remote rail 0 after all HELLO"); return -1;
        }
        bool expected = false;
        if (!mPendingCopyReqOccupied.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            RecordFailure("pending COPY_REQ slot occupied"); return -1;
        }
        mPendingCopyReqBytes = context.MessageDataLen();
        if (context.MessageData() != nullptr && mPendingCopyReqBytes <= mPendingCopyReqPayload.size())
            std::memcpy(mPendingCopyReqPayload.data(), context.MessageData(), mPendingCopyReqBytes);
        if (mPendingCopyReqBytes == kCopyReqWireBytes && context.MessageData() != nullptr) {
            const uint8_t *cursor = static_cast<const uint8_t *>(context.MessageData());
            if (GetU32(cursor) == kCopyReqMagic && GetU16(cursor) == kProtocolVersion &&
                GetU16(cursor) == kOpCopyReq) {
                const uint64_t generation = GetU64(cursor);
                size_t i = 0;
                if (TraceIndex(generation, i))
                    PublishCallbackTrace(generation, mTrace[i].remoteRequestReceived, "remote_request_received");
            }
        }
        mPendingCopyReqPublished.store(true, std::memory_order_release);
        return 0;
    }

    int OnDataDone(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Local) { RecordFailure("remote received DATA_DONE"); return -1; }
        uint64_t generation = 0;
        if (!DecodeDataDone(context.MessageData(), context.MessageDataLen(), rail, mBlocksPerRail, generation)) {
            RecordFailure("invalid DATA_DONE rail " + std::to_string(rail)); return -1;
        }
        RailState &state = mRails[rail];
        const uint64_t previous = state.dataDoneGeneration.load(std::memory_order_relaxed);
        if (generation != previous + 1) {
            RecordFailure("non-increasing DATA_DONE rail " + std::to_string(rail)); return -1;
        }
        size_t i = 0;
        if (TraceIndex(generation, i))
            PublishCallbackTrace(generation, mTrace[i].localDataDone[rail], "local_data_done");
        state.dataDoneGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnCopyError(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Local || rail != 0) {
            RecordFailure("COPY_ERROR on wrong role/rail"); return -1;
        }
        uint64_t generation = 0; uint32_t stage = 0, code = 0, detail = 0;
        if (!DecodeCopyError(context.MessageData(), context.MessageDataLen(), generation, stage, code, detail)) {
            RecordFailure("invalid COPY_ERROR"); return -1;
        }
        RecordFailure("remote COPY_ERROR generation=" + std::to_string(generation) +
            " stage=" + std::to_string(stage) + " code=" + std::to_string(code) +
            " detail=" + std::to_string(detail));
        return 0;
    }

    bool DecodeTokenForRail(uint16_t rail, UBSHcomServiceContext &context, uint32_t magic,
        uint16_t opcode, uint64_t &generation) noexcept
    {
        uint16_t wireRail = kMaxLinks;
        return DecodeToken(context.MessageData(), context.MessageDataLen(), magic, opcode, generation, wireRail) &&
            generation != 0 && wireRail == rail;
    }

    int OnFinish(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        uint64_t generation = 0;
        if (mOptions.role != Role::Remote ||
            !DecodeTokenForRail(rail, context, kFinishMagic, kOpFinish, generation) ||
            generation != mParams.TotalRounds()) {
            RecordFailure("invalid FINISH"); return -1;
        }
        uint64_t expected = 0;
        if (!mRails[rail].finishGeneration.compare_exchange_strong(
            expected, generation, std::memory_order_release, std::memory_order_relaxed)) {
            RecordFailure("duplicate FINISH"); return -1;
        }
        return 0;
    }

    int OnFinishAck(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        uint64_t generation = 0;
        if (mOptions.role != Role::Local ||
            !DecodeTokenForRail(rail, context, kFinishAckMagic, kOpFinishAck, generation) ||
            generation != mParams.TotalRounds()) {
            RecordFailure("invalid FINISH_ACK"); return -1;
        }
        uint64_t expected = 0;
        if (!mRails[rail].finishAckGeneration.compare_exchange_strong(
            expected, generation, std::memory_order_release, std::memory_order_relaxed)) {
            RecordFailure("duplicate FINISH_ACK"); return -1;
        }
        return 0;
    }

    Callback *NewDataCallback(uint16_t rail, uint64_t generation, bool last)
    {
        return UBSHcomNewCallback([this, rail, generation, last](UBSHcomServiceContext &context) {
            ActiveCallbackGuard guard(mActiveCallbacks);
            if (context.Result() != 0)
                RecordFailure("Put callback failed rail " + std::to_string(rail) + ": " +
                    std::to_string(context.Result()));
            if (last) {
                size_t i = 0;
                if (TraceIndex(generation, i))
                    PublishCallbackTrace(generation, mTrace[i].remoteDataCallbacksDone[rail],
                        "remote_data_callbacks_done");
            }
            mRails[rail].callbackCounters.dataDoneCallbacks.fetch_add(1, std::memory_order_release);
        }, std::placeholders::_1);
    }

    Callback *NewSendCallback(uint16_t rail)
    {
        return UBSHcomNewCallback([this, rail](UBSHcomServiceContext &context) {
            ActiveCallbackGuard guard(mActiveCallbacks);
            if (context.Result() != 0)
                RecordFailure("Send callback failed rail " + std::to_string(rail) + ": " +
                    std::to_string(context.Result()));
            mRails[rail].callbackCounters.sendDoneCallbacks.fetch_add(1, std::memory_order_release);
        }, std::placeholders::_1);
    }

    void RunLocal()
    {
        uint64_t generation = 1;
        for (uint32_t i = 0; i < mParams.verifyRounds; ++i, ++generation) {
            SparseCopy(generation, false); VerifyLocalDestination(mCopyEntries, generation);
        }
        for (uint32_t i = 0; i < mParams.warmupRounds; ++i, ++generation) SparseCopy(generation, false);
        if (mParams.measureRounds != 0) {
            mSparseCopyNs.reserve(mParams.measureRounds);
            mMeasureWallStartNs = NowNs();
            for (uint32_t i = 0; i < mParams.measureRounds; ++i, ++generation) SparseCopy(generation, true);
            mMeasureWallEndNs = NowNs();
            VerifyLocalDestination(MakeCopyEntries(mParams.verifyRounds, mOptions.links), mParams.verifyRounds);
        }
        for (uint32_t i = 0; i < mParams.traceRounds; ++i, ++generation) SparseCopy(generation, false);
        if (mParams.traceRounds != 0)
            VerifyLocalDestination(MakeCopyEntries(mParams.verifyRounds, mOptions.links), mParams.verifyRounds);
        FinishAllRails();
    }

    void SparseCopy(uint64_t generation, bool measure)
    {
        const uint64_t start = NowNs();
        size_t trace = 0;
        if (TraceIndex(generation, trace)) mTrace[trace].localBegin.Publish(start);
        const uint64_t seed = generation <= mParams.verifyRounds ? generation : mParams.verifyRounds;
        mCopyEntries = MakeCopyEntries(seed, mOptions.links);
        const uint64_t railBytes = static_cast<uint64_t>(mBlocksPerRail) * kStrideBytes;
        std::string error;
        if (!ValidateCopyEntries(mCopyEntries, mOptions.links, railBytes, railBytes, error))
            throw std::runtime_error("local input validation: " + error);
        for (uint16_t rail = 0; rail < mOptions.links; ++rail)
            if (mRails[rail].peerSourceBytes != railBytes)
                throw std::runtime_error("invalid remote source metadata");
        mCopyReqPayload = EncodeCopyRequest(generation, mOptions.links, mCopyEntries);
        if (TraceIndex(generation, trace)) mTrace[trace].localRequestPrepared.Publish(NowNs());
        const uint64_t expectedSend = ExpectedSendCallbacks(0) + 1;
        PostAsyncSend(0, ChannelCopyRequired(0, "COPY_REQ"), mCopyReqPayload.data(),
            mCopyReqPayload.size(), kOpCopyReq);
        if (TraceIndex(generation, trace)) mTrace[trace].localRequestPosted.Publish(NowNs());
        WaitData("sparse_copy completion", [this, generation, expectedSend] {
            if (mRails[0].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) < expectedSend)
                return false;
            for (uint16_t rail = 0; rail < mOptions.links; ++rail)
                if (mRails[rail].dataDoneGeneration.load(std::memory_order_acquire) < generation) return false;
            return true;
        });
        const uint64_t end = NowNs();
        if (TraceIndex(generation, trace)) mTrace[trace].localEnd.Publish(end);
        if (measure) mSparseCopyNs.push_back(end - start);
    }

    void RunRemote()
    {
        WaitData("all HELLO", [this] { return AllHellosSeen(); });
        for (uint64_t generation = 1; generation <= mParams.TotalRounds(); ++generation) {
            try {
                ReceivePendingCopyRequest(generation);
                DecodeActiveCopyRequest(generation);
                size_t trace = 0;
                if (TraceIndex(generation, trace)) mTrace[trace].remoteRequestDecoded.Publish(NowNs());
                uint64_t sequence = 0;
                if (mOptions.links == 2)
                    sequence = IssueSecondaryRailCommand(RailCommand::ProcessRemoteRound, generation);
                ProcessRemoteRail(0, generation);
                if (mOptions.links == 2)
                    WaitSecondaryRailCommand(sequence, "rail 1 remote copy");
            } catch (...) {
                TrySendCopyError(generation, kCopyErrorStageRemoteProcess, kCopyErrorCodeRequestFailed, 0);
                throw;
            }
        }
        FinishAllRails();
    }

    bool AllHellosSeen() const noexcept
    {
        for (uint16_t rail = 0; rail < mOptions.links; ++rail)
            if (!mRails[rail].helloSeen.load(std::memory_order_acquire)) return false;
        return true;
    }

    void ReceivePendingCopyRequest(uint64_t generation)
    {
        WaitData("COPY_REQ", [this] { return mPendingCopyReqPublished.load(std::memory_order_acquire); });
        size_t trace = 0;
        if (TraceIndex(generation, trace)) mTrace[trace].remoteProcessingStarted.Publish(NowNs());
        mActiveCopyReqBytes = mPendingCopyReqBytes;
        if (mActiveCopyReqBytes <= mActiveCopyReqPayload.size())
            std::memcpy(mActiveCopyReqPayload.data(), mPendingCopyReqPayload.data(), mActiveCopyReqBytes);
        mPendingCopyReqPublished.store(false, std::memory_order_relaxed);
        mPendingCopyReqOccupied.store(false, std::memory_order_release);
    }

    void DecodeActiveCopyRequest(uint64_t generation)
    {
        const uint64_t railBytes = static_cast<uint64_t>(mBlocksPerRail) * kStrideBytes;
        std::string error;
        if (!DecodeCopyRequest(mActiveCopyReqPayload.data(), mActiveCopyReqBytes, generation, mOptions.links,
            railBytes, railBytes, mActiveCopyEntries, error))
            throw std::runtime_error("invalid COPY_REQ: " + error);
    }

    void ProcessRemoteRail(uint16_t rail, uint64_t generation)
    {
        RailState &state = mRails[rail];
        size_t trace = 0;
        const bool tracing = TraceIndex(generation, trace);
        if (generation <= mParams.verifyRounds) FillRemoteSourceRail(rail, generation);
        BuildRemotePutRequests(rail);
        if (tracing) mTrace[trace].remotePutRequestsBuilt[rail].Publish(NowNs());
        const uint64_t expectedData = state.appCounters.attemptedDataCallbacks + mBlocksPerRail;
        const UBSHcomChannelPtr channel = ChannelCopyRequired(rail, "remote copy");
        if (tracing) {
            mTrace[trace].remotePutLoopStarted[rail].Publish(NowNs());
            mTrace[trace].remotePutLoopThreadCpuStarted[rail].Publish(ThreadCpuNowNs());
        }
        for (uint32_t i = 0; i < mBlocksPerRail; ++i) {
            Callback *callback = NewDataCallback(rail, generation, i + 1 == mBlocksPerRail);
            if (callback == nullptr) throw std::runtime_error("Put callback allocation failed");
            ++state.appCounters.attemptedDataCallbacks;
            const int rc = channel->Put(state.putRequests[i], callback);
            if (rc != 0) throw std::runtime_error("Put failed: " + std::to_string(rc));
            if (tracing && (i + 1) % kPutTraceInterval == 0) {
                const size_t checkpoint = static_cast<size_t>((i + 1) / kPutTraceInterval - 1);
                mTrace[trace].remotePutCheckpoints[rail][checkpoint].Publish(NowNs());
            }
        }
        if (tracing) {
            mTrace[trace].remotePutLoopThreadCpuEnded[rail].Publish(ThreadCpuNowNs());
            const uint64_t loopEnd = NowNs();
            mTrace[trace].remotePutLoopEnded[rail].Publish(loopEnd);
            mTrace[trace].remotePosted[rail].Publish(loopEnd);
        }
        state.dataDonePayload = EncodeDataDone(generation, rail, mBlocksPerRail);
        const uint64_t expectedSend = ExpectedSendCallbacks(rail) + 1;
        PostAsyncSend(rail, channel, state.dataDonePayload.data(), state.dataDonePayload.size(), kOpDataDone);
        if (tracing) mTrace[trace].remoteDonePosted[rail].Publish(NowNs());
        WaitData("remote callbacks", [this, rail, expectedData, expectedSend] {
            return mRails[rail].callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
                mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend;
        });
        if (tracing) mTrace[trace].remoteCallbacksDrained[rail].Publish(NowNs());
    }

    void BuildRemotePutRequests(uint16_t rail)
    {
        RailState &state = mRails[rail];
        const uint32_t first = static_cast<uint32_t>(rail) * mBlocksPerRail;
        for (uint32_t i = 0; i < mBlocksPerRail; ++i) {
            const CopyEntry &entry = mActiveCopyEntries[first + i];
            UBSHcomOneSideRequest &request = state.putRequests[i];
            request.lAddress = reinterpret_cast<uintptr_t>(state.buffer.Data()) + entry.remoteSourceOffset;
            request.rAddress = state.peerDestinationAddress + entry.localDestinationOffset;
            request.lKey = state.memoryKey;
            request.rKey = state.peerDestinationKey;
            request.size = kBlockBytes;
        }
    }

    void FillRemoteSourceRail(uint16_t rail, uint64_t generation)
    {
        for (uint32_t slot = 0; slot < mBlocksPerRail; ++slot) {
            const uint32_t globalBlock = static_cast<uint32_t>(rail) * mBlocksPerRail + slot;
            FillBlock(mRails[rail].buffer.Data() + static_cast<size_t>(slot) * kStrideBytes,
                generation, globalBlock);
        }
    }

    void VerifyLocalDestination(const std::array<CopyEntry, kBlocks> &entries, uint64_t generation)
    {
        for (uint32_t index = 0; index < kBlocks; ++index) {
            const uint16_t rail = RailForRequestIndex(index, mOptions.links);
            const uint32_t sourceSlot = static_cast<uint32_t>(entries[index].remoteSourceOffset / kStrideBytes);
            const uint32_t globalBlock = static_cast<uint32_t>(rail) * mBlocksPerRail + sourceSlot;
            std::string error;
            const uint8_t *destination = mRails[rail].buffer.Data() + entries[index].localDestinationOffset;
            if (!VerifyBlock(destination, generation, globalBlock, error) || !VerifyGap(destination, error))
                throw std::runtime_error("request " + std::to_string(index) + ": " + error);
        }
    }

    void TrySendCopyError(uint64_t generation, uint32_t stage, uint32_t code, uint32_t detail) noexcept
    {
        try {
            const UBSHcomChannelPtr channel = ChannelCopy(0);
            if (channel == nullptr) return;
            mCopyErrorPayload = EncodeCopyError(generation, stage, code, detail);
            PostAsyncSend(0, channel, mCopyErrorPayload.data(), mCopyErrorPayload.size(), kOpCopyError);
        } catch (...) {}
    }

    void FinishAllRails()
    {
        uint64_t sequence = 0;
        if (mOptions.links == 2) sequence = IssueSecondaryRailCommand(RailCommand::Finish);
        FinishRail(0);
        if (mOptions.links == 2) WaitSecondaryRailCommand(sequence, "rail 1 finish");
    }

    void FinishRail(uint16_t rail)
    {
        RailState &state = mRails[rail];
        if (mOptions.role == Role::Local) {
            state.finishPayload = EncodeToken(kFinishMagic, kOpFinish, mParams.TotalRounds(), rail);
            PostAsyncSend(rail, ChannelCopyRequired(rail, "FINISH"), state.finishPayload.data(),
                state.finishPayload.size(), kOpFinish);
            const uint64_t target = ExpectedSendCallbacks(rail);
            WaitControl("FINISH_ACK", [this, rail, target] {
                return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target &&
                    mRails[rail].finishAckGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
            });
        } else {
            WaitControl("FINISH", [this, rail] {
                return mRails[rail].finishGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
            });
            state.finishAckPayload = EncodeToken(kFinishAckMagic, kOpFinishAck, mParams.TotalRounds(), rail);
            PostAsyncSend(rail, ChannelCopyRequired(rail, "FINISH_ACK"), state.finishAckPayload.data(),
                state.finishAckPayload.size(), kOpFinishAck);
            const uint64_t target = ExpectedSendCallbacks(rail);
            WaitControl("FINISH_ACK completion", [this, rail, target] {
                return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target;
            });
        }
    }

    void PostAsyncSend(uint16_t rail, const UBSHcomChannelPtr &channel,
        uint8_t *data, size_t size, uint16_t opcode)
    {
        Callback *callback = NewSendCallback(rail);
        if (callback == nullptr) throw std::runtime_error("Send callback allocation failed");
        ++mRails[rail].appCounters.attemptedSendCallbacks;
        const int rc = channel->Send(UBSHcomRequest(data, static_cast<uint32_t>(size), opcode), callback);
        if (rc != 0) throw std::runtime_error("Send failed: " + std::to_string(rc));
    }

    uint64_t ExpectedSendCallbacks(uint16_t rail) const noexcept
    {
        return mRails[rail].appCounters.attemptedSendCallbacks +
            mRails[rail].callbackCounters.workerAttemptedSendCallbacks.load(std::memory_order_acquire);
    }

    template <typename Predicate> void WaitData(const char *what, Predicate predicate)
    {
        CheckFatal(what);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        uint32_t spins = 0;
        while (!predicate()) {
            CheckFatal(what); CpuRelax();
            if ((++spins & (kDataDeadlineCheckInterval - 1)) == 0 &&
                std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error(std::string("timed out waiting for ") + what);
        }
        CheckFatal(what);
    }

    template <typename Predicate> void WaitControl(const char *what, Predicate predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        while (!predicate()) {
            CheckFatal(what);
            if (std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error(std::string("timed out waiting for ") + what);
            std::this_thread::yield();
        }
        CheckFatal(what);
    }

    void WaitForChannels()
    {
        std::unique_lock<std::mutex> lock(mChannelsMutex);
        const bool ok = mChannelCv.wait_for(lock, std::chrono::seconds(mOptions.timeoutSec), [this] {
            if (mFatal.load(std::memory_order_acquire)) return true;
            for (uint16_t rail = 0; rail < mOptions.links; ++rail)
                if (mRails[rail].channel == nullptr) return false;
            return true;
        });
        if (!ok) throw std::runtime_error("timed out waiting for channels");
        CheckFatal("peer channels");
    }

    UBSHcomChannelPtr ChannelCopy(uint16_t rail) const
    {
        std::lock_guard<std::mutex> lock(mChannelsMutex); return mRails[rail].channel;
    }

    UBSHcomChannelPtr ChannelCopyRequired(uint16_t rail, const char *operation) const
    {
        UBSHcomChannelPtr channel = ChannelCopy(rail);
        if (channel == nullptr)
            throw std::runtime_error(std::string(operation) + " without channel rail " + std::to_string(rail));
        return channel;
    }

    void RequireOk(int rc, const char *operation)
    {
        if (rc != 0) throw std::runtime_error(std::string(operation) + " failed: " + std::to_string(rc));
    }

    void RecordFailure(const std::string &message) noexcept
    {
        bool expected = false;
        if (!mFatal.compare_exchange_strong(expected, true, std::memory_order_release,
            std::memory_order_relaxed)) return;
        try { std::lock_guard<std::mutex> lock(mErrorMutex); mError = message; } catch (...) {}
        mChannelCv.notify_all();
    }

    void CheckFatal(const char *where) const
    {
        if (!mFatal.load(std::memory_order_acquire)) return;
        std::string error = "unknown asynchronous failure";
        { std::lock_guard<std::mutex> lock(mErrorMutex); if (!mError.empty()) error = mError; }
        throw std::runtime_error(std::string(where) + ": " + error);
    }

    bool CallbacksDrained() const noexcept
    {
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            if (mRails[rail].callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) <
                    mRails[rail].appCounters.attemptedDataCallbacks ||
                mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) <
                    ExpectedSendCallbacks(rail)) return false;
        }
        return mActiveCallbacks.load(std::memory_order_acquire) == 0;
    }

    bool DrainUntilComplete() noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        while (std::chrono::steady_clock::now() < deadline) {
            if (CallbacksDrained()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return CallbacksDrained();
    }

    void TeardownFixedRails()
    {
        if (mOptions.links == 2) {
            const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::Teardown);
            WaitSecondaryRailCommand(sequence, "rail 1 teardown");
        }
        TeardownRail(0);
    }

    void TryTeardownFixedRails() noexcept
    {
        mTearingDown.store(true, std::memory_order_release);
        if (mOptions.links == 2 && mSecondary.thread.joinable()) {
            const uint64_t issued = mSecondary.issued.load(std::memory_order_acquire);
            if (WaitSecondaryNoThrow(issued)) {
                try {
                    const uint64_t teardown = IssueSecondaryRailCommand(RailCommand::Teardown);
                    (void)WaitSecondaryNoThrow(teardown);
                } catch (...) {}
            }
        }
        TeardownRail(0);
    }

    void TeardownRail(uint16_t rail) noexcept
    {
        mTearingDown.store(true, std::memory_order_release);
        RailState &state = mRails[rail];
        UBSHcomChannelPtr channel;
        { std::lock_guard<std::mutex> lock(mChannelsMutex); channel = state.channel; state.channel.Set(nullptr); }
        if (state.service != nullptr && channel != nullptr) state.service->Disconnect(channel);
        if (state.service != nullptr && state.memoryRegistered) {
            state.service->DestroyMemoryRegion(state.memoryRegion); state.memoryRegistered = false;
        }
        if (state.service != nullptr) {
            UBSHcomService::Destroy(state.serviceName); state.service = nullptr;
        }
    }

    void EmitTracePoint(const char *event, uint64_t generation, int rail, const TracePoint &point,
        const char *clock = "monotonic_raw", int putCount = -1) const
    {
        std::cout << "{\"record_type\":\"trace\",\"trace_schema\":\"sparse-copy-v4-dual-rail-v3\","
                  << "\"host_role\":\"" << RoleName(mOptions.role) << "\",\"case\":\""
                  << CaseName(mOptions.links) << "\",\"generation\":" << generation << ",\"rail\":";
        if (rail < 0) std::cout << "null"; else std::cout << rail;
        std::cout << ",\"event\":\"" << event << "\",\"clock\":\"" << clock << "\"";
        if (putCount >= 0) std::cout << ",\"put_count\":" << putCount;
        std::cout << ",\"timestamp_ns\":" << point.Read(event) << "}" << std::endl;
    }

    void EmitTrace() const
    {
        for (uint32_t i = 0; i < mParams.traceRounds; ++i) {
            const TraceRound &t = mTrace[i];
            if (mOptions.role == Role::Local) {
                EmitTracePoint("local_begin", t.generation, -1, t.localBegin);
                EmitTracePoint("local_request_prepared", t.generation, -1, t.localRequestPrepared);
                EmitTracePoint("local_request_posted", t.generation, 0, t.localRequestPosted);
                for (uint16_t rail = 0; rail < mOptions.links; ++rail)
                    EmitTracePoint("local_data_done", t.generation, rail, t.localDataDone[rail]);
                EmitTracePoint("local_end", t.generation, -1, t.localEnd);
            } else {
                EmitTracePoint("remote_request_received", t.generation, 0, t.remoteRequestReceived);
                EmitTracePoint("remote_processing_started", t.generation, -1, t.remoteProcessingStarted);
                EmitTracePoint("remote_request_decoded", t.generation, -1, t.remoteRequestDecoded);
                for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                    EmitTracePoint("remote_put_requests_built", t.generation, rail,
                        t.remotePutRequestsBuilt[rail]);
                    EmitTracePoint("remote_put_loop_started", t.generation, rail,
                        t.remotePutLoopStarted[rail]);
                    EmitTracePoint("remote_put_loop_thread_cpu_started", t.generation, rail,
                        t.remotePutLoopThreadCpuStarted[rail], "thread_cpu");
                    for (uint32_t checkpoint = 0;
                         checkpoint < mBlocksPerRail / kPutTraceInterval; ++checkpoint) {
                        const int putCount = static_cast<int>((checkpoint + 1) * kPutTraceInterval);
                        EmitTracePoint("remote_put_checkpoint", t.generation, rail,
                            t.remotePutCheckpoints[rail][checkpoint], "monotonic_raw", putCount);
                    }
                    EmitTracePoint("remote_put_loop_ended", t.generation, rail,
                        t.remotePutLoopEnded[rail]);
                    EmitTracePoint("remote_put_loop_thread_cpu_ended", t.generation, rail,
                        t.remotePutLoopThreadCpuEnded[rail], "thread_cpu");
                    EmitTracePoint("remote_posted", t.generation, rail, t.remotePosted[rail]);
                    EmitTracePoint("remote_done_posted", t.generation, rail, t.remoteDonePosted[rail]);
                    EmitTracePoint("remote_data_callbacks_done", t.generation, rail,
                        t.remoteDataCallbacksDone[rail]);
                    EmitTracePoint("remote_callbacks_drained", t.generation, rail,
                        t.remoteCallbacksDrained[rail]);
                }
            }
        }
    }

    void PrintLocalResult() const
    {
        std::ostringstream out;
        out << std::fixed << std::setprecision(3)
            << "{\"schema_version\":4,\"protocol\":\"sparse-copy-v4-dual-rail\""
            << ",\"measurement\":\"local-sparse-copy\",\"result_role\":\"local\",\"case\":\""
            << CaseName(mOptions.links) << "\",\"status\":\"ok\",\"commit\":\"" << RDMA_600_GIT_COMMIT
            << "\",\"role\":\"local\",\"kind\":\"" << KindName(mOptions.kind)
            << "\",\"optimization\":\"stage1.5-AB\",\"data_wait\":\"busy-poll-relax\""
            << ",\"deadline_check_interval\":256,\"counter_alignment_bytes\":" << kCounterAlignment
            << ",\"callback_allocation\":\"per-request\",\"links\":" << mOptions.links
            << ",\"services\":" << mOptions.links << ",\"blocks\":600,\"blocks_per_rail\":" << mBlocksPerRail
            << ",\"block_bytes\":1024,\"payload_bytes_per_call\":614400,\"mode\":\"direct\""
            << ",\"remote_layout\":\"per-rail-direct-stride-4096\""
            << ",\"request_index_rail_partition\":\"contiguous-equal\",\"tls_enabled\":false"
            << ",\"internal_multirail\":false,\"channel_link_count\":1,\"rounds_in_flight\":1"
            << ",\"application_submit_threads\":" << mOptions.links
            << ",\"rail_thread_affinity\":\"fixed-setup-to-drain-one-thread-per-service\""
            << ",\"source_format\":\"direct-pairs\",\"source_address_count\":600"
            << ",\"destination_address_count\":600,\"request_descriptor_bytes\":9600"
            << ",\"request_bytes\":9664,\"request_send_wr_per_call\":1"
            << ",\"data_wr_per_call_expected\":600,\"completion_send_wr_per_call_expected\":"
            << mOptions.links << ",\"imm_events_per_call_expected\":0,\"ack_wr_per_call\":0"
            << ",\"verify_passed\":true,\"trace_rounds\":" << mParams.traceRounds;
        if (mOptions.kind != RunKind::Measure) {
            out << ",\"measure_rounds\":0,\"sparse_copy_avg_us\":null,\"sparse_copy_p50_us\":null"
                << ",\"sparse_copy_p95_us\":null,\"sparse_copy_p99_us\":null"
                << ",\"effective_GBps\":null,\"block_Mops\":null,\"request_GBps\":null"
                << ",\"measured_wall_seconds\":null";
        } else {
            const double seconds = static_cast<double>(mMeasureWallEndNs - mMeasureWallStartNs) / 1e9;
            out << ",\"measure_rounds\":" << mParams.measureRounds
                << ",\"sparse_copy_avg_us\":" << AverageNs(mSparseCopyNs) / 1000.0
                << ",\"sparse_copy_p50_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.50))
                << ",\"sparse_copy_p95_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.95))
                << ",\"sparse_copy_p99_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.99))
                << ",\"effective_GBps\":" << static_cast<double>(mParams.measureRounds) * kPayloadBytes / seconds / 1e9
                << ",\"block_Mops\":" << static_cast<double>(mParams.measureRounds) * kBlocks / seconds / 1e6
                << ",\"request_GBps\":" << static_cast<double>(mParams.measureRounds) * kCopyReqWireBytes / seconds / 1e9
                << ",\"measured_wall_seconds\":" << seconds;
        }
        out << "}";
        std::cout << out.str() << std::endl;
    }

    void PrintRemoteStatus() const
    {
        std::cout << "{\"schema_version\":4,\"protocol\":\"sparse-copy-v4-dual-rail\",\"case\":\""
                  << CaseName(mOptions.links) << "\",\"role\":\"remote\",\"status\":\"ok\",\"processed_calls\":"
                  << mParams.TotalRounds() << ",\"links\":" << mOptions.links << "}" << std::endl;
    }

    Options mOptions;
    CaseParameters mParams;
    uint32_t mBlocksPerRail = kBlocks;
    std::array<RailState, kMaxLinks> mRails{};
    std::atomic<bool> mRemoteReady{false};
    std::atomic<bool> mTearingDown{false};
    std::atomic<bool> mFatal{false};
    mutable std::mutex mErrorMutex;
    std::string mError;
    mutable std::mutex mChannelsMutex;
    std::condition_variable mChannelCv;
    alignas(kCounterAlignment) std::atomic<uint64_t> mActiveCallbacks{0};
    std::array<CopyEntry, kBlocks> mCopyEntries{};
    std::array<CopyEntry, kBlocks> mActiveCopyEntries{};
    std::array<uint8_t, kCopyReqWireBytes> mCopyReqPayload{};
    std::array<uint8_t, kCopyReqWireBytes> mPendingCopyReqPayload{};
    std::array<uint8_t, kCopyReqWireBytes> mActiveCopyReqPayload{};
    uint32_t mPendingCopyReqBytes = 0;
    uint32_t mActiveCopyReqBytes = 0;
    std::atomic<bool> mPendingCopyReqOccupied{false};
    std::atomic<bool> mPendingCopyReqPublished{false};
    std::array<uint8_t, kCopyErrorWireBytes> mCopyErrorPayload{};
    std::array<TraceRound, kMaxTraceRounds> mTrace{};
    std::vector<uint64_t> mSparseCopyNs;
    uint64_t mMeasureWallStartNs = 0;
    uint64_t mMeasureWallEndNs = 0;
    SecondaryRailExecutor mSecondary;
};

#endif  // RDMA_600_SELF_TEST_ONLY

}  // namespace

int main(int argc, char **argv)
{
#if defined(RDMA_600_SELF_TEST_ONLY)
    (void)argc;
    (void)argv;
    return RunSelfTest() ? 0 : 1;
#else
    try {
        const Options options = ParseOptions(argc, argv);
        if (options.selfTest) {
            return RunSelfTest() ? 0 : 1;
        }
        SparseCopyBenchmark benchmark(options);
        return benchmark.Run();
    } catch (const std::exception &error) {
        std::cerr << "ERROR: " << error.what() << std::endl;
        return 1;
    }
#endif
}
