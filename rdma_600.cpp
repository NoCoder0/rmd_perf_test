// SPDX-License-Identifier: MulanPSL-2.0
//
// Stage 1 requester-driven sparse_copy baseline for the 600 x 1 KiB
// ubs-comm RDMA experiment.
//
// This executable deliberately implements only SC-B1:
//   * one service / one RDMA device / one worker-poll QP;
//   * local sends all 600 source/destination offset pairs for every call;
//   * remote rebuilds and posts 600 asynchronous 1 KiB Put operations;
//   * one DATA_DONE Send follows the writes on the same QP;
//   * local measures call entry through data availability (no success ACK,
//     staging, or CPU scatter).
//
// It is intended to be built and run on the target Linux RDMA hosts.  The
// --self-test mode has no RDMA dependency at runtime and validates the local
// layout, pattern, and explicit wire encoders before a hardware run.

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
#include <iterator>
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
using ock::hcom::UBSHcomNewCallback;

constexpr uint16_t kProtocolVersion = 3;
constexpr uint16_t kLinks = 1;
constexpr uint32_t kBlocks = 600;
constexpr uint32_t kBlockBytes = 1024;
constexpr uint32_t kStrideBytes = 4096;
constexpr uint64_t kPayloadBytes = static_cast<uint64_t>(kBlocks) * kBlockBytes;
constexpr uint64_t kDestinationBytes = static_cast<uint64_t>(kBlocks) * kStrideBytes;
constexpr uint8_t kDstGapSentinel = 0xa5;
constexpr uint8_t kSourceGapSentinel = 0x5a;
constexpr uint32_t kDataDeadlineCheckInterval = 256;
constexpr uint32_t kSourceRegionId = 1;
constexpr uint32_t kDestinationRegionId = 2;
constexpr uint16_t kModeDirect = 1;
constexpr uint16_t kSourceFormatDirectPairs = 1;
constexpr uint32_t kCopyErrorStageRemoteProcess = 1;
constexpr uint32_t kCopyErrorCodeRequestFailed = 1;
#if defined(__cpp_lib_hardware_interference_size)
constexpr size_t kCounterAlignment = std::hardware_destructive_interference_size;
#else
// Older standard libraries do not expose the platform value.  This fallback
// is only an alignment choice; hardware runs must still record CPU topology.
constexpr size_t kCounterAlignment = 64;
#endif

static_assert(kPayloadBytes == 614400, "the benchmark payload is fixed by design");
static_assert((kDataDeadlineCheckInterval & (kDataDeadlineCheckInterval - 1)) == 0,
    "the data deadline interval must be a power of two");

constexpr uint16_t kOpHello = 700;
constexpr uint16_t kOpReady = 701;
constexpr uint16_t kOpCopyReq = 702;
constexpr uint16_t kOpDataDone = 703;
constexpr uint16_t kOpCopyError = 704;
constexpr uint16_t kOpFinish = 705;
constexpr uint16_t kOpFinishAck = 706;

constexpr uint32_t kHelloMagic = 0x52443630U;      // "RD60"
constexpr uint32_t kReadyMagic = 0x52445259U;      // "RDRY"
constexpr uint32_t kCopyReqMagic = 0x53435059U;    // "SCPY"
constexpr uint32_t kCompletionMagic = 0x5343444eU; // "SCDN"
constexpr uint32_t kCopyErrorMagic = 0x53434552U;  // "SCER"
constexpr uint32_t kFinishMagic = 0x5244464eU;     // "RDFN"
constexpr uint32_t kFinishAckMagic = 0x52444641U;  // "RDFA"

constexpr size_t kMemoryKeyWireBytes = 80;
constexpr size_t kParamsWireBytes = 28;
constexpr size_t kHelloWireBytes = 4 + kParamsWireBytes + 16 + kMemoryKeyWireBytes;
constexpr size_t kReadyWireBytes = 4 + kParamsWireBytes + 16;
constexpr size_t kCopyReqHeaderBytes = 64;
constexpr size_t kCopyEntryWireBytes = 16;
constexpr size_t kCopyReqDescriptorBytes = static_cast<size_t>(kBlocks) * kCopyEntryWireBytes;
constexpr size_t kCopyReqWireBytes = kCopyReqHeaderBytes + kCopyReqDescriptorBytes;
constexpr size_t kCompletionWireBytes = 32;
constexpr size_t kCopyErrorWireBytes = 32;
constexpr size_t kFinishWireBytes = 16;

static_assert(kHelloWireBytes == 128, "HELLO wire size must include the local destination key");
static_assert(kReadyWireBytes == 48, "READY wire size is fixed");
static_assert(kCopyReqWireBytes == 9664, "direct COPY_REQ must carry all 600 pairs");

enum class Role { Local, Remote };
enum class RunKind { Verify, Measure };

struct Options {
    Role role = Role::Local;
    RunKind kind = RunKind::Measure;
    std::string rdmaIp;
    std::string listen;
    std::string peer;
    uint32_t verifyRounds = 20;
    uint32_t warmupRounds = 1000;
    uint32_t measureRounds = 10000;
    uint32_t timeoutSec = 10;
    int appCpu = -1;
    int workerCpu = -1;
    bool selfTest = false;
};

struct CaseParameters {
    uint16_t version = kProtocolVersion;
    uint16_t links = kLinks;
    uint32_t blocks = kBlocks;
    uint32_t blockBytes = kBlockBytes;
    uint32_t strideBytes = kStrideBytes;
    uint32_t verifyRounds = 0;
    uint32_t warmupRounds = 0;
    uint32_t measureRounds = 0;

    uint64_t TotalRounds() const
    {
        return static_cast<uint64_t>(verifyRounds) + warmupRounds + measureRounds;
    }
};

struct HelloInfo {
    CaseParameters params;
    uint64_t destinationAddress = 0;
    uint64_t destinationBytes = 0;
    UBSHcomMemoryKey destinationKey{};
};

struct ReadyInfo {
    CaseParameters params;
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
    return params;
}

bool SameParams(const CaseParameters &left, const CaseParameters &right)
{
    return left.version == right.version && left.links == right.links && left.blocks == right.blocks &&
        left.blockBytes == right.blockBytes && left.strideBytes == right.strideBytes &&
        left.verifyRounds == right.verifyRounds && left.warmupRounds == right.warmupRounds &&
        left.measureRounds == right.measureRounds;
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
    info.sourceRegionId = GetU32(cursor);
    info.sourceAlignment = GetU32(cursor);
    info.sourceBytes = GetU64(cursor);
    return true;
}

std::array<CopyEntry, kBlocks> MakeCopyEntries(uint64_t seed)
{
    std::array<CopyEntry, kBlocks> entries{};
    const uint32_t shift = static_cast<uint32_t>(seed % kBlocks);
    for (uint32_t index = 0; index < kBlocks; ++index) {
        const uint32_t sourceSlot = (index * 7U + shift * 13U) % kBlocks;
        const uint32_t destinationSlot = (index * 11U + shift * 17U) % kBlocks;
        entries[index].remoteSourceOffset = static_cast<uint64_t>(sourceSlot) * kStrideBytes;
        entries[index].localDestinationOffset = static_cast<uint64_t>(destinationSlot) * kStrideBytes;
    }
    return entries;
}

bool ValidateCopyEntries(const std::array<CopyEntry, kBlocks> &entries, uint64_t sourceBytes,
    uint64_t destinationBytes, std::string &error)
{
    std::array<bool, kBlocks> destinationsSeen{};
    for (uint32_t index = 0; index < kBlocks; ++index) {
        const CopyEntry &entry = entries[index];
        if (entry.remoteSourceOffset % kStrideBytes != 0 || sourceBytes < kBlockBytes ||
            entry.remoteSourceOffset > sourceBytes - kBlockBytes) {
            error = "source offset is unaligned or out of range at index " + std::to_string(index);
            return false;
        }
        if (entry.localDestinationOffset % kStrideBytes != 0 || destinationBytes < kBlockBytes ||
            entry.localDestinationOffset > destinationBytes - kBlockBytes) {
            error = "destination offset is unaligned or out of range at index " + std::to_string(index);
            return false;
        }
        const uint64_t destinationSlot = entry.localDestinationOffset / kStrideBytes;
        if (destinationSlot >= kBlocks || destinationsSeen[static_cast<size_t>(destinationSlot)]) {
            error = "destination slots must be unique at index " + std::to_string(index);
            return false;
        }
        destinationsSeen[static_cast<size_t>(destinationSlot)] = true;
    }
    return true;
}

std::array<uint8_t, kCopyReqWireBytes> EncodeCopyRequest(
    uint64_t generation, const std::array<CopyEntry, kBlocks> &entries)
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
    PutU16(cursor, kLinks);
    PutU16(cursor, 0);  // direct has no SGL items
    PutU16(cursor, kSourceFormatDirectPairs);
    PutU32(cursor, kSourceRegionId);
    PutU32(cursor, kDestinationRegionId);
    PutU32(cursor, 0);  // direct has no staging region
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

bool DecodeCopyRequest(const void *data, uint32_t size, uint64_t expectedGeneration, uint64_t sourceBytes,
    uint64_t destinationBytes, std::array<CopyEntry, kBlocks> &entries, std::string &error)
{
    const auto fail = [&error](const std::string &message) {
        error = message;
        return false;
    };
    if (data == nullptr || size != kCopyReqWireBytes) {
        return fail("COPY_REQ length must be exactly 9664 bytes");
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kCopyReqMagic || GetU16(cursor) != kProtocolVersion || GetU16(cursor) != kOpCopyReq) {
        return fail("COPY_REQ magic/version/opcode mismatch");
    }
    if (GetU64(cursor) != expectedGeneration || GetU32(cursor) != kBlocks || GetU32(cursor) != kBlocks ||
        GetU32(cursor) != kBlockBytes || GetU16(cursor) != kModeDirect || GetU16(cursor) != kLinks ||
        GetU16(cursor) != 0 || GetU16(cursor) != kSourceFormatDirectPairs ||
        GetU32(cursor) != kSourceRegionId || GetU32(cursor) != kDestinationRegionId || GetU32(cursor) != 0 ||
        GetU32(cursor) != kCopyReqHeaderBytes || GetU32(cursor) != kCopyReqDescriptorBytes ||
        GetU32(cursor) != kPayloadBytes || GetU32(cursor) != 0) {
        return fail("COPY_REQ header fields do not match direct stage 1");
    }
    for (uint32_t index = 0; index < kBlocks; ++index) {
        CopyEntry &entry = entries[index];
        entry.remoteSourceOffset = GetU64(cursor);
        entry.localDestinationOffset = GetU64(cursor);
    }
    if (cursor != static_cast<const uint8_t *>(data) + size) {
        return fail("COPY_REQ decoder did not consume the exact message");
    }
    if (!ValidateCopyEntries(entries, sourceBytes, destinationBytes, error)) {
        error = "COPY_REQ " + error;
        return false;
    }
    return true;
}

std::array<uint8_t, kCompletionWireBytes> EncodeDataDone(uint64_t generation)
{
    std::array<uint8_t, kCompletionWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kCompletionMagic);
    PutU16(cursor, kProtocolVersion);
    PutU16(cursor, kOpDataDone);
    PutU64(cursor, generation);
    PutU16(cursor, 0);
    PutU16(cursor, 0);
    PutU32(cursor, 0);
    PutU32(cursor, kBlocks);
    PutU32(cursor, static_cast<uint32_t>(kPayloadBytes));
    return payload;
}

bool DecodeDataDone(const void *data, uint32_t size, uint64_t &generation)
{
    if (data == nullptr || size != kCompletionWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kCompletionMagic || GetU16(cursor) != kProtocolVersion ||
        GetU16(cursor) != kOpDataDone) {
        return false;
    }
    generation = GetU64(cursor);
    return GetU16(cursor) == 0 && GetU16(cursor) == 0 && GetU32(cursor) == 0 &&
        GetU32(cursor) == kBlocks && GetU32(cursor) == kPayloadBytes;
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

bool DecodeCopyError(
    const void *data, uint32_t size, uint64_t &generation, uint32_t &stage, uint32_t &errorCode, uint32_t &detail)
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

std::array<uint8_t, kFinishWireBytes> EncodeFinish(uint32_t magic, uint64_t generation)
{
    std::array<uint8_t, kFinishWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, magic);
    PutU64(cursor, generation);
    PutU32(cursor, 0);
    return payload;
}

bool DecodeFinish(const void *data, uint32_t size, uint32_t magic, uint64_t &generation)
{
    if (data == nullptr || size != kFinishWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != magic) {
        return false;
    }
    generation = GetU64(cursor);
    return GetU32(cursor) == 0;
}

uint64_t PatternWord(uint64_t generation, uint32_t globalBlock, uint32_t wordIndex)
{
    return 0x9e3779b97f4a7c15ULL ^ (generation * 0x100000001b3ULL) ^
        (static_cast<uint64_t>(globalBlock) << 32U) ^ wordIndex;
}

void FillBlock(uint8_t *address, uint64_t generation, uint32_t block)
{
    auto *words = reinterpret_cast<uint64_t *>(address);
    for (uint32_t word = 0; word < kBlockBytes / sizeof(uint64_t); ++word) {
        words[word] = PatternWord(generation, block, word);
    }
}

bool VerifyBlock(const uint8_t *address, uint64_t generation, uint32_t block, std::string &error)
{
    const auto *words = reinterpret_cast<const uint64_t *>(address);
    for (uint32_t word = 0; word < kBlockBytes / sizeof(uint64_t); ++word) {
        const uint64_t expected = PatternWord(generation, block, word);
        if (words[word] != expected) {
            std::ostringstream stream;
            stream << "data mismatch: generation=" << generation << " block=" << block << " word=" << word
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

#ifndef RDMA_600_SELF_TEST_ONLY

std::string RoleName(Role role)
{
    return role == Role::Local ? "local" : "remote";
}

std::string KindName(RunKind kind)
{
    return kind == RunKind::Measure ? "measure" : "verify";
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
    const uint64_t parsed = ParseUnsigned(name, value, static_cast<uint64_t>(std::numeric_limits<int>::max()));
    return static_cast<int>(parsed);
}

void PrintUsage(std::ostream &stream)
{
    stream << "Usage:\n"
           << "  rdma_600 --role remote --rdma-ip <ip> --listen <oob-ip:port> [options]\n"
           << "  rdma_600 --role local --rdma-ip <ip> --peer <remote-oob-ip:port> [options]\n"
           << "  rdma_600 --self-test\n\n"
           << "Stage 1 is fixed to: --links 1 --mode direct; local sends 600 source/destination "
              "pairs and remote performs 600 direct Put writes, with TLS disabled.\n"
           << "Options: --kind verify|measure --verify-rounds N --warmup N --rounds N\n"
           << "         --timeout-sec N --app-cpu N --worker-cpu N\n";
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

    static const std::array<std::string, 13> kAllowedOptions = {
        "--role", "--rdma-ip", "--listen", "--peer", "--kind", "--verify-rounds", "--warmup", "--rounds",
        "--timeout-sec", "--app-cpu", "--worker-cpu", "--links", "--mode"};
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

    const std::string role = required("--role");
    if (role == "local") {
        options.role = Role::Local;
        options.peer = required("--peer");
        if (values.count("--listen") != 0) {
            throw std::runtime_error("--listen is only valid for remote");
        }
    } else if (role == "remote") {
        options.role = Role::Remote;
        options.listen = required("--listen");
        if (values.count("--peer") != 0) {
            throw std::runtime_error("--peer is only valid for local");
        }
    } else {
        throw std::runtime_error("--role must be local or remote");
    }
    options.rdmaIp = required("--rdma-ip");

    const std::string kind = optional("--kind", "measure");
    if (kind == "verify") {
        options.kind = RunKind::Verify;
    } else if (kind == "measure") {
        options.kind = RunKind::Measure;
    } else {
        throw std::runtime_error("--kind must be verify or measure");
    }
    options.verifyRounds = static_cast<uint32_t>(ParseUnsigned("--verify-rounds", optional("--verify-rounds", "20"),
        std::numeric_limits<uint32_t>::max()));
    options.warmupRounds = static_cast<uint32_t>(ParseUnsigned("--warmup", optional("--warmup", "1000"),
        std::numeric_limits<uint32_t>::max()));
    options.measureRounds = static_cast<uint32_t>(ParseUnsigned("--rounds", optional("--rounds", "10000"),
        std::numeric_limits<uint32_t>::max()));
    options.timeoutSec = static_cast<uint32_t>(ParseUnsigned("--timeout-sec", optional("--timeout-sec", "10"), 32767));
    options.appCpu = ParseSignedCpu("--app-cpu", optional("--app-cpu", "-1"));
    options.workerCpu = ParseSignedCpu("--worker-cpu", optional("--worker-cpu", "-1"));

    const std::string mode = optional("--mode", "direct");
    if (optional("--links", "1") != "1" || mode != "direct") {
        throw std::runtime_error("this stage-1 binary only supports SC-B1: links=1 direct dst Put");
    }
    if (options.verifyRounds == 0) {
        throw std::runtime_error("--verify-rounds must be greater than zero");
    }
    if (options.timeoutSec == 0) {
        throw std::runtime_error("--timeout-sec must be greater than zero");
    }
    if (options.appCpu >= 0 && options.appCpu == options.workerCpu) {
        throw std::runtime_error("--app-cpu and --worker-cpu must use different cores");
    }
    if (options.kind == RunKind::Verify) {
        options.warmupRounds = 0;
        options.measureRounds = 0;
    } else if (options.measureRounds == 0) {
        throw std::runtime_error("--rounds must be greater than zero for --kind measure");
    }
    if (options.kind == RunKind::Measure && (options.appCpu < 0 || options.workerCpu < 0)) {
        throw std::runtime_error(
            "stage-1 measure requires explicit --app-cpu and --worker-cpu for pinned busy polling");
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
    // This is the architectural spin-loop hint, not an operating-system yield.
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

#endif  // RDMA_600_SELF_TEST_ONLY

bool RunSelfTest()
{
    try {
        CaseParameters parameters;
        parameters.verifyRounds = 20;
        parameters.warmupRounds = 0;
        parameters.measureRounds = 0;
        UBSHcomMemoryKey key{};
        for (size_t index = 0; index < std::size(key.keys); ++index) {
            key.keys[index] = 0x1000U + index;
            key.tokens[index] = 0x2000U + index;
        }
        for (size_t index = 0; index < std::size(key.eid); ++index) {
            key.eid[index] = static_cast<uint8_t>(index);
        }
        HelloInfo helloInfo{parameters, 0x12345000U, kDestinationBytes, key};
        const auto hello = EncodeHello(helloInfo);
        HelloInfo decodedHello{};
        if (!DecodeHello(hello.data(), static_cast<uint32_t>(hello.size()), decodedHello) ||
            !SameParams(parameters, decodedHello.params) ||
            helloInfo.destinationAddress != decodedHello.destinationAddress ||
            helloInfo.destinationBytes != decodedHello.destinationBytes ||
            std::memcmp(&helloInfo.destinationKey, &decodedHello.destinationKey, sizeof(key)) != 0) {
            throw std::runtime_error("HELLO wire round trip failed");
        }

        ReadyInfo ready{parameters, kSourceRegionId, kStrideBytes, kDestinationBytes};
        const auto readyWire = EncodeReady(ready);
        ReadyInfo decodedReady{};
        if (!DecodeReady(readyWire.data(), static_cast<uint32_t>(readyWire.size()), decodedReady) ||
            !SameParams(ready.params, decodedReady.params) ||
            ready.sourceRegionId != decodedReady.sourceRegionId ||
            ready.sourceAlignment != decodedReady.sourceAlignment || ready.sourceBytes != decodedReady.sourceBytes) {
            throw std::runtime_error("READY wire round trip failed");
        }

        constexpr uint64_t generation = 7;
        const auto entries = MakeCopyEntries(generation);
        const auto request = EncodeCopyRequest(generation, entries);
        std::array<CopyEntry, kBlocks> decodedEntries{};
        std::string decodeError;
        if (!DecodeCopyRequest(request.data(), static_cast<uint32_t>(request.size()), generation, kDestinationBytes,
                kDestinationBytes, decodedEntries, decodeError) ||
            std::memcmp(entries.data(), decodedEntries.data(), sizeof(entries)) != 0) {
            throw std::runtime_error("COPY_REQ wire round trip failed: " + decodeError);
        }
        if (DecodeCopyRequest(request.data(), static_cast<uint32_t>(request.size() - 1), generation,
                kDestinationBytes, kDestinationBytes, decodedEntries, decodeError)) {
            throw std::runtime_error("truncated COPY_REQ was accepted");
        }
        auto duplicateRequest = request;
        std::copy_n(duplicateRequest.data() + kCopyReqHeaderBytes + 8, 8,
            duplicateRequest.data() + kCopyReqHeaderBytes + kCopyEntryWireBytes + 8);
        if (DecodeCopyRequest(duplicateRequest.data(), static_cast<uint32_t>(duplicateRequest.size()), generation,
                kDestinationBytes, kDestinationBytes, decodedEntries, decodeError)) {
            throw std::runtime_error("duplicate destination in COPY_REQ was accepted");
        }

        const auto dataDone = EncodeDataDone(generation);
        uint64_t decodedGeneration = 0;
        if (!DecodeDataDone(dataDone.data(), static_cast<uint32_t>(dataDone.size()), decodedGeneration) ||
            decodedGeneration != generation) {
            throw std::runtime_error("DATA_DONE wire round trip failed");
        }
        const auto copyError = EncodeCopyError(generation, 2, 3, 4);
        uint32_t stage = 0;
        uint32_t errorCode = 0;
        uint32_t detail = 0;
        if (!DecodeCopyError(copyError.data(), static_cast<uint32_t>(copyError.size()), decodedGeneration, stage,
                errorCode, detail) || decodedGeneration != generation || stage != 2 || errorCode != 3 || detail != 4) {
            throw std::runtime_error("COPY_ERROR wire round trip failed");
        }

        AlignedBuffer source;
        AlignedBuffer destination;
        source.Allocate(static_cast<size_t>(kBlocks) * kStrideBytes);
        destination.Allocate(static_cast<size_t>(kBlocks) * kStrideBytes);
        std::memset(source.Data(), kSourceGapSentinel, source.Size());
        std::memset(destination.Data(), kDstGapSentinel, destination.Size());

        for (uint32_t block = 0; block < kBlocks; ++block) {
            FillBlock(source.Data() + static_cast<size_t>(block) * kStrideBytes, generation, block);
        }
        for (const CopyEntry &entry : entries) {
            std::memcpy(destination.Data() + entry.localDestinationOffset,
                source.Data() + entry.remoteSourceOffset, kBlockBytes);
        }
        for (const CopyEntry &entry : entries) {
            std::string error;
            const uint32_t sourceSlot = static_cast<uint32_t>(entry.remoteSourceOffset / kStrideBytes);
            const uint8_t *target = destination.Data() + entry.localDestinationOffset;
            if (!VerifyBlock(target, generation, sourceSlot, error) || !VerifyGap(target, error)) {
                throw std::runtime_error(error);
            }
        }
        std::cout << "SELF_TEST: PASS (sparse-copy-v3, 9664-byte request, 600 direct blocks)" << std::endl;
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

    ActiveCallbackGuard(const ActiveCallbackGuard &) = delete;
    ActiveCallbackGuard &operator=(const ActiveCallbackGuard &) = delete;

private:
    std::atomic<uint64_t> &mCounter;
};

// Only the application thread writes these counters.  Completion handlers
// never read or modify them, so ordinary integers preserve the ownership rule
// without a per-request atomic read-modify-write.
struct alignas(kCounterAlignment) AppOwnedCounters {
    uint64_t attemptedDataCallbacks = 0;
    uint64_t attemptedSendCallbacks = 0;
};

// The HCOM worker/callback side owns all writes in this cache-line-aligned
// group.  The application thread only observes them with acquire loads.
struct alignas(kCounterAlignment) CallbackOwnedCounters {
    std::atomic<uint64_t> workerAttemptedSendCallbacks{0};
    std::atomic<uint64_t> dataDoneCallbacks{0};
    std::atomic<uint64_t> sendDoneCallbacks{0};
    std::atomic<uint64_t> activeCallbacks{0};
};

class Stage1Benchmark {
public:
    explicit Stage1Benchmark(Options options) : mOptions(std::move(options))
    {
        mParams.verifyRounds = mOptions.verifyRounds;
        mParams.warmupRounds = mOptions.warmupRounds;
        mParams.measureRounds = mOptions.measureRounds;
    }

    int Run()
    {
        try {
            PinCurrentThread(mOptions.appCpu);
            SetupService();
            SetupMemory();
            if (mOptions.role == Role::Remote) {
                mRemoteReady.store(true, std::memory_order_release);
                std::cout << "LISTENING role=remote endpoint=" << mOptions.listen << " rdma_ip=" << mOptions.rdmaIp
                          << std::endl;
                std::cout.flush();
                WaitForChannel();
                RunRemote();
                PrintRemoteStatus();
            } else {
                ConnectLocal();
                Handshake();
                RunLocal();
                PrintLocalResult();
            }
            CheckFatal("normal completion");
            if (!DrainUntilComplete()) {
                throw std::runtime_error("completion counters did not drain before teardown");
            }
            Teardown();
            return 0;
        } catch (const std::exception &error) {
            RecordFailure(error.what());
            std::cerr << "ERROR: " << error.what() << std::endl;
            // Do not free callback-owned state while a worker can still run it.  A
            // bounded failed drain intentionally exits the process without C++
            // destructors; the OS then reclaims the process and its worker threads.
            if (!DrainUntilComplete()) {
                std::cerr << "FATAL: callbacks did not drain before the deadline; exiting without unsafe teardown"
                          << std::endl;
                std::_Exit(2);
            }
            Teardown();
            return 1;
        }
    }

private:
    void SetupService()
    {
        mServiceName = "rdma600_" + RoleName(mOptions.role) + "_0";
        UBSHcomServiceOptions options{};
        options.maxSendRecvDataSize = 16384;
        options.workerGroupThreadCount = 1;
        options.workerGroupMode = ock::hcom::NET_BUSY_POLLING;
        if (mOptions.workerCpu >= 0) {
            const auto cpu = static_cast<uint32_t>(mOptions.workerCpu);
            options.workerGroupCpuIdsRange = {cpu, cpu};
        }
        mService = UBSHcomService::Create(UBSHcomServiceProtocol::RDMA, mServiceName, options);
        if (mService == nullptr) {
            throw std::runtime_error("UBSHcomService::Create(RDMA) returned null");
        }
        // HCOM defaults TLS to enabled.  This benchmark uses an isolated,
        // trusted test path and does not provide certificates or PSK callbacks,
        // so disable it before Start() creates the RDMA/OOB TLS context.
        UBSHcomTlsOptions tlsOptions{};
        tlsOptions.enableTls = false;
        mService->SetTlsOptions(tlsOptions);
        mService->SetDeviceIpMask({mOptions.rdmaIp + "/32"});
        UBSHcomMultiRailOptions multiRail{};
        multiRail.enable = false;
        mService->SetMultiRailOptions(multiRail);
        mService->SetSendQueueSize(1024);
        mService->SetRecvQueueSize(256);
        mService->SetCompletionQueueDepth(2048);
        mService->SetQueuePrePostSize(128);
        mService->SetPollingBatchSize(16);
        mService->SetEnableMrCache(false);
        mService->RegisterRecvHandler([this](UBSHcomServiceContext &context) { return OnIncoming(context); });
        mService->RegisterSendHandler([this](const UBSHcomServiceContext &) {
            ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
            return 0;
        });
        mService->RegisterOneSideHandler([this](const UBSHcomServiceContext &) {
            ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
            return 0;
        });
        mService->RegisterChannelBrokenHandler(
            [this](const UBSHcomChannelPtr &) {
                ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
                if (!mTearingDown.load(std::memory_order_acquire)) {
                    RecordFailure("hcom channel broken");
                }
            },
            UBSHcomChannelBrokenPolicy::BROKEN_ALL);

        if (mOptions.role == Role::Remote) {
            const int rc = mService->Bind("tcp://" + mOptions.listen,
                [this](const std::string &, const UBSHcomChannelPtr &channel, const std::string &) {
                    return OnNewChannel(channel);
                });
            RequireOk(rc, "Bind");
        }
        RequireOk(mService->Start(), "Start");
    }

    void SetupMemory()
    {
        if (mOptions.role == Role::Remote) {
            mSource.Allocate(static_cast<size_t>(kBlocks) * kStrideBytes);
            std::memset(mSource.Data(), kSourceGapSentinel, mSource.Size());
            RequireOk(mService->RegisterMemoryRegion(reinterpret_cast<uintptr_t>(mSource.Data()), mSource.Size(), mSourceMr),
                "RegisterMemoryRegion(source)");
            mSourceMrRegistered = true;
            mSourceKey = {};
            mSourceMr.GetMemoryKey(mSourceKey);
            if (mSourceMr.GetAddress() != reinterpret_cast<uintptr_t>(mSource.Data()) || mSourceMr.GetSize() < mSource.Size()) {
                throw std::runtime_error("source MR does not cover the supplied source allocation");
            }
            return;
        }

        mDestination.Allocate(static_cast<size_t>(kBlocks) * kStrideBytes);
        std::memset(mDestination.Data(), kDstGapSentinel, mDestination.Size());
        RequireOk(mService->RegisterMemoryRegion(
                      reinterpret_cast<uintptr_t>(mDestination.Data()), mDestination.Size(), mDestinationMr),
            "RegisterMemoryRegion(destination)");
        mDestinationMrRegistered = true;
        mDestinationKey = {};
        mDestinationMr.GetMemoryKey(mDestinationKey);
        if (mDestinationMr.GetAddress() != reinterpret_cast<uintptr_t>(mDestination.Data()) ||
            mDestinationMr.GetSize() < mDestination.Size()) {
            throw std::runtime_error("destination MR does not cover the supplied destination allocation");
        }
    }

    int OnNewChannel(const UBSHcomChannelPtr &channel) noexcept
    {
        ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
        if (mOptions.role != Role::Remote || channel == nullptr) {
            RecordFailure("unexpected new channel");
            return -1;
        }
        if (!ConfigureChannel(channel)) {
            return -1;
        }
        {
            std::lock_guard<std::mutex> lock(mChannelMutex);
            if (mChannel != nullptr) {
                RecordFailure("remote accepted more than one channel in stage 1");
                return -1;
            }
            mChannel = channel;
        }
        mChannelCv.notify_all();
        return 0;
    }

    bool ConfigureChannel(const UBSHcomChannelPtr &channel) noexcept
    {
        try {
            channel->SetChannelTimeOut(static_cast<int16_t>(mOptions.timeoutSec),
                static_cast<int16_t>(mOptions.timeoutSec));
            UBSHcomTwoSideThreshold thresholds{};
            thresholds.splitThreshold = UINT32_MAX;
            thresholds.rndvThreshold = UINT32_MAX;
            const int rc = channel->SetTwoSideThreshold(thresholds);
            if (rc != 0) {
                RecordFailure("SetTwoSideThreshold failed: " + std::to_string(rc));
                return false;
            }
            return true;
        } catch (const std::exception &error) {
            RecordFailure(std::string("ConfigureChannel failed: ") + error.what());
            return false;
        }
    }

    void ConnectLocal()
    {
        UBSHcomConnectOptions options{};
        options.linkCount = 1;
        options.mode = UBSHcomClientPollingMode::WORKER_POLL;
        options.cbType = UBSHcomChannelCallBackType::CHANNEL_FUNC_CB;
        UBSHcomChannelPtr channel;
        RequireOk(mService->Connect("tcp://" + mOptions.peer, channel, options), "Connect");
        if (channel == nullptr) {
            throw std::runtime_error("Connect succeeded without a channel");
        }
        if (!ConfigureChannel(channel)) {
            CheckFatal("ConfigureChannel after Connect");
            throw std::runtime_error("ConfigureChannel failed after Connect");
        }
        {
            std::lock_guard<std::mutex> lock(mChannelMutex);
            mChannel = channel;
        }
        mChannelCv.notify_all();
    }

    void Handshake()
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("HELLO without a connected channel");
        }
        HelloInfo hello{};
        hello.params = mParams;
        hello.destinationAddress = reinterpret_cast<uintptr_t>(mDestination.Data());
        hello.destinationBytes = mDestination.Size();
        hello.destinationKey = mDestinationKey;
        mHelloPayload = EncodeHello(hello);
        UBSHcomRequest request(mHelloPayload.data(), static_cast<uint32_t>(mHelloPayload.size()), kOpHello);
        UBSHcomResponse response(mReadyResponse.data(), static_cast<uint32_t>(mReadyResponse.size()));
        RequireOk(channel->Call(request, response, nullptr), "HELLO Call");
        ReadyInfo ready{};
        if (!DecodeReady(response.address, response.size, ready) || !SameParams(mParams, ready.params) ||
            ready.sourceRegionId != kSourceRegionId || ready.sourceAlignment != kStrideBytes ||
            ready.sourceBytes != kDestinationBytes) {
            throw std::runtime_error("invalid READY response");
        }
        mPeerSourceBytes = ready.sourceBytes;
    }

    int OnIncoming(UBSHcomServiceContext &context) noexcept
    {
        ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
        if (context.Result() != 0) {
            RecordFailure("incoming hcom context failed: " + std::to_string(context.Result()));
            return context.Result();
        }
        switch (context.OpCode()) {
            case kOpHello:
                return OnHello(context);
            case kOpCopyReq:
                return OnCopyReq(context);
            case kOpDataDone:
                return OnDataDone(context);
            case kOpCopyError:
                return OnCopyError(context);
            case kOpFinish:
                return OnFinish(context);
            case kOpFinishAck:
                return OnFinishAck(context);
            default:
                RecordFailure("unexpected hcom opcode " + std::to_string(context.OpCode()));
                return -1;
        }
    }

    int OnHello(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Remote || !mRemoteReady.load(std::memory_order_acquire)) {
            RecordFailure("HELLO received before remote initialization");
            return -1;
        }
        HelloInfo hello{};
        if (!DecodeHello(context.MessageData(), context.MessageDataLen(), hello) ||
            !SameParams(hello.params, mParams) || hello.destinationAddress == 0 ||
            hello.destinationBytes != kDestinationBytes) {
            RecordFailure("HELLO parameters or local destination registration are invalid");
            return -1;
        }
        mPeerDestinationAddress = static_cast<uintptr_t>(hello.destinationAddress);
        mPeerDestinationBytes = hello.destinationBytes;
        mPeerDestinationKey = hello.destinationKey;
        mHandshakeComplete.store(true, std::memory_order_release);
        ReadyInfo ready{};
        ready.params = mParams;
        ready.sourceRegionId = kSourceRegionId;
        ready.sourceAlignment = kStrideBytes;
        ready.sourceBytes = mSource.Size();
        mReadyPayload = EncodeReady(ready);
        Callback *callback = NewSendCallback();
        if (callback == nullptr) {
            RecordFailure("unable to allocate READY callback");
            return -1;
        }
        const UBSHcomRequest reply(mReadyPayload.data(), static_cast<uint32_t>(mReadyPayload.size()), kOpReady);
        const UBSHcomReplyContext replyContext(context.RspCtx(), 0);
        const int rc = context.Channel()->Reply(replyContext, reply, callback);
        if (rc != 0) {
            // hcom owns or destroys the asynchronous callback on its own error
            // paths.  Do not delete it here and risk a double free.
            RecordFailure("READY Reply failed: " + std::to_string(rc));
            return rc;
        }
        mCallbackCounters.workerAttemptedSendCallbacks.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    int OnCopyReq(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Remote || !mHandshakeComplete.load(std::memory_order_acquire)) {
            RecordFailure("COPY_REQ received by the wrong role or before HELLO");
            return -1;
        }
        bool expected = false;
        if (!mPendingCopyReqReady.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            RecordFailure("COPY_REQ arrived while the pending slot was occupied");
            return -1;
        }
        const uint32_t size = context.MessageDataLen();
        mPendingCopyReqBytes = size;
        if (context.MessageData() != nullptr && size <= mPendingCopyReqPayload.size()) {
            std::memcpy(mPendingCopyReqPayload.data(), context.MessageData(), size);
        }
        // compare_exchange published the occupied state before the copy. Publish
        // content separately so the app never observes a partially copied request.
        mPendingCopyReqPublished.store(true, std::memory_order_release);
        return 0;
    }

    int OnDataDone(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Local) {
            RecordFailure("remote received DATA_DONE");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeDataDone(context.MessageData(), context.MessageDataLen(), generation) || generation == 0) {
            RecordFailure("invalid DATA_DONE payload");
            return -1;
        }
        const uint64_t previous = mDataDoneGeneration.load(std::memory_order_relaxed);
        if (generation != previous + 1) {
            RecordFailure("DATA_DONE generation is not strictly increasing");
            return -1;
        }
        // The SEND follows all 600 writes on the same QP. This release only
        // publishes that CQ event to the local app thread.
        mDataDoneGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnCopyError(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Local) {
            RecordFailure("remote received COPY_ERROR");
            return -1;
        }
        uint64_t generation = 0;
        uint32_t stage = 0;
        uint32_t errorCode = 0;
        uint32_t detail = 0;
        if (!DecodeCopyError(context.MessageData(), context.MessageDataLen(), generation, stage, errorCode, detail) ||
            generation == 0 || generation > mParams.TotalRounds()) {
            RecordFailure("invalid COPY_ERROR payload");
            return -1;
        }
        RecordFailure("remote COPY_ERROR: generation=" + std::to_string(generation) +
            " stage=" + std::to_string(stage) + " code=" + std::to_string(errorCode) +
            " detail=" + std::to_string(detail));
        return 0;
    }

    int OnFinish(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Remote) {
            RecordFailure("local received FINISH");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeFinish(context.MessageData(), context.MessageDataLen(), kFinishMagic, generation) ||
            generation != mParams.TotalRounds()) {
            RecordFailure("invalid FINISH payload");
            return -1;
        }
        mFinishGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnFinishAck(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Local) {
            RecordFailure("remote received FINISH_ACK");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeFinish(context.MessageData(), context.MessageDataLen(), kFinishAckMagic, generation) ||
            generation != mParams.TotalRounds()) {
            RecordFailure("invalid FINISH_ACK payload");
            return -1;
        }
        mFinishAckGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    Callback *NewDataCallback()
    {
        return UBSHcomNewCallback(
            [this](UBSHcomServiceContext &context) {
                ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
                if (context.Result() != 0) {
                    RecordFailure("Put callback failed: " + std::to_string(context.Result()));
                }
                // A callback, including a failed one, ends HCOM's ownership of
                // this posted request and must advance drain accounting.
                mCallbackCounters.dataDoneCallbacks.fetch_add(1, std::memory_order_release);
            },
            std::placeholders::_1);
    }

    Callback *NewSendCallback()
    {
        return UBSHcomNewCallback(
            [this](UBSHcomServiceContext &context) {
                ActiveCallbackGuard guard(mCallbackCounters.activeCallbacks);
                if (context.Result() != 0) {
                    RecordFailure("Send/Reply callback failed: " + std::to_string(context.Result()));
                }
                // Completion status and lifetime completion are separate facts.
                mCallbackCounters.sendDoneCallbacks.fetch_add(1, std::memory_order_release);
            },
            std::placeholders::_1);
    }

    void RunLocal()
    {
        uint64_t generation = 1;
        for (uint32_t round = 0; round < mParams.verifyRounds; ++round, ++generation) {
            SparseCopy(generation, false);
            VerifyLocalDestination(mCopyEntries, generation);
        }
        for (uint32_t round = 0; round < mParams.warmupRounds; ++round, ++generation) {
            SparseCopy(generation, false);
        }
        if (mParams.measureRounds != 0) {
            // Capacity preparation is deliberately outside the wall interval.
            mSparseCopyNs.reserve(mParams.measureRounds);
            mMeasureWallStartNs = NowNs();
            for (uint32_t round = 0; round < mParams.measureRounds; ++round, ++generation) {
                SparseCopy(generation, true);
            }
            mMeasureWallEndNs = NowNs();
            const auto finalEntries = MakeCopyEntries(mParams.verifyRounds);
            VerifyLocalDestination(finalEntries, mParams.verifyRounds);
        }
        SendFinish();
    }

    void SparseCopy(uint64_t generation, bool measure)
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("local lost its channel before sparse_copy");
        }
        const uint64_t startNs = NowNs();
        const uint64_t inputSeed = generation <= mParams.verifyRounds ? generation : mParams.verifyRounds;
        mCopyEntries = MakeCopyEntries(inputSeed);
        std::string validationError;
        if (!ValidateCopyEntries(mCopyEntries, mPeerSourceBytes, mDestination.Size(), validationError)) {
            throw std::runtime_error("local sparse_copy input validation failed: " + validationError);
        }
        mCopyReqPayload = EncodeCopyRequest(generation, mCopyEntries);
        const uint64_t expectedSend = ExpectedSendCallbacks() + 1;
        PostAsyncSend(channel, mCopyReqPayload.data(), mCopyReqPayload.size(), kOpCopyReq);
        WaitData("sparse_copy completion", [this, generation, expectedSend] {
            return mDataDoneGeneration.load(std::memory_order_acquire) >= generation &&
                mCallbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend;
        });
        const uint64_t endNs = NowNs();
        if (measure) {
            mSparseCopyNs.push_back(endNs - startNs);
        }
    }

    void RunRemote()
    {
        WaitData("HELLO completion", [this] { return mHandshakeComplete.load(std::memory_order_acquire); });
        for (uint64_t generation = 1; generation <= mParams.TotalRounds(); ++generation) {
            try {
                ReceivePendingCopyRequest();
                ProcessCopyRequest(generation);
            } catch (...) {
                TrySendCopyError(
                    generation, kCopyErrorStageRemoteProcess, kCopyErrorCodeRequestFailed, 0);
                throw;
            }
        }
        WaitControl("FINISH", [this] {
            return mFinishGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
        });
        SendFinishAck();
    }

    void ReceivePendingCopyRequest()
    {
        WaitData("COPY_REQ", [this] { return mPendingCopyReqPublished.load(std::memory_order_acquire); });
        mActiveCopyReqBytes = mPendingCopyReqBytes;
        if (mActiveCopyReqBytes <= mActiveCopyReqPayload.size()) {
            std::memcpy(mActiveCopyReqPayload.data(), mPendingCopyReqPayload.data(), mActiveCopyReqBytes);
        }
        mPendingCopyReqPublished.store(false, std::memory_order_relaxed);
        mPendingCopyReqReady.store(false, std::memory_order_release);
    }

    void ProcessCopyRequest(uint64_t generation)
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("remote lost its channel before processing COPY_REQ");
        }
        std::string decodeError;
        if (!DecodeCopyRequest(mActiveCopyReqPayload.data(), mActiveCopyReqBytes, generation, mSource.Size(),
                mPeerDestinationBytes, mActiveCopyEntries, decodeError)) {
            throw std::runtime_error("invalid COPY_REQ: " + decodeError);
        }
        if (generation <= mParams.verifyRounds) {
            FillRemoteSource(generation);
        }
        BuildRemotePutRequests(mActiveCopyEntries);

        const uint64_t expectedData = mAppCounters.attemptedDataCallbacks + kBlocks;
        for (uint32_t block = 0; block < kBlocks; ++block) {
            Callback *callback = NewDataCallback();
            if (callback == nullptr) {
                throw std::runtime_error("unable to allocate Put callback");
            }
            const int rc = channel->Put(mPutRequests[block], callback);
            if (rc != 0) {
                throw std::runtime_error("Put failed at request " + std::to_string(block) + ": " +
                    std::to_string(rc));
            }
            ++mAppCounters.attemptedDataCallbacks;
        }
        mDataDonePayload = EncodeDataDone(generation);
        const uint64_t expectedSend = ExpectedSendCallbacks() + 1;
        PostAsyncSend(channel, mDataDonePayload.data(), mDataDonePayload.size(), kOpDataDone);

        // The notification is already on the wire behind all writes. Waiting for
        // local completions here protects request/WR/notification storage and
        // surfaces asynchronous failures without adding a success message.
        WaitData("remote request callbacks", [this, expectedData, expectedSend] {
            return mCallbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
                mCallbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend;
        });
    }

    void BuildRemotePutRequests(const std::array<CopyEntry, kBlocks> &entries)
    {
        for (uint32_t index = 0; index < kBlocks; ++index) {
            const CopyEntry &entry = entries[index];
            UBSHcomOneSideRequest &request = mPutRequests[index];
            request.lAddress = reinterpret_cast<uintptr_t>(mSource.Data()) + entry.remoteSourceOffset;
            request.rAddress = mPeerDestinationAddress + entry.localDestinationOffset;
            request.lKey = mSourceKey;
            request.rKey = mPeerDestinationKey;
            request.size = kBlockBytes;
        }
    }

    void TrySendCopyError(uint64_t generation, uint32_t stage, uint32_t errorCode, uint32_t detail) noexcept
    {
        try {
            const UBSHcomChannelPtr channel = ChannelCopy();
            if (channel == nullptr) {
                return;
            }
            mCopyErrorPayload = EncodeCopyError(generation, stage, errorCode, detail);
            PostAsyncSend(channel, mCopyErrorPayload.data(), mCopyErrorPayload.size(), kOpCopyError);
        } catch (...) {
            // The outer failure path disconnects the channel, which is the
            // fallback wakeup when an error notification cannot be posted.
        }
    }

    void SendFinish()
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("local lost its channel before FINISH");
        }
        mFinishPayload = EncodeFinish(kFinishMagic, mParams.TotalRounds());
        PostAsyncSend(channel, mFinishPayload.data(), mFinishPayload.size(), kOpFinish);
        const uint64_t target = ExpectedSendCallbacks();
        WaitControl("FINISH local completion", [this, target] {
            return mCallbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
        WaitControl("FINISH_ACK", [this] {
            return mFinishAckGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
        });
    }

    void SendFinishAck()
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("remote lost its channel before FINISH_ACK");
        }
        mFinishAckPayload = EncodeFinish(kFinishAckMagic, mParams.TotalRounds());
        PostAsyncSend(channel, mFinishAckPayload.data(), mFinishAckPayload.size(), kOpFinishAck);
        const uint64_t target = ExpectedSendCallbacks();
        WaitControl("FINISH_ACK local completion", [this, target] {
            return mCallbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
    }

    void PostAsyncSend(const UBSHcomChannelPtr &channel, uint8_t *data, size_t size, uint16_t opcode)
    {
        Callback *callback = NewSendCallback();
        if (callback == nullptr) {
            throw std::runtime_error("unable to allocate Send callback");
        }
        const UBSHcomRequest request(data, static_cast<uint32_t>(size), opcode);
        const int rc = channel->Send(request, callback);
        if (rc != 0) {
            throw std::runtime_error("Send opcode " + std::to_string(opcode) + " failed: " + std::to_string(rc));
        }
        ++mAppCounters.attemptedSendCallbacks;
    }

    uint64_t ExpectedSendCallbacks() const noexcept
    {
        return mAppCounters.attemptedSendCallbacks +
            mCallbackCounters.workerAttemptedSendCallbacks.load(std::memory_order_acquire);
    }

    void FillRemoteSource(uint64_t generation)
    {
        for (uint32_t block = 0; block < kBlocks; ++block) {
            FillBlock(mSource.Data() + static_cast<size_t>(block) * kStrideBytes, generation, block);
        }
    }

    void VerifyLocalDestination(const std::array<CopyEntry, kBlocks> &entries, uint64_t patternGeneration)
    {
        for (uint32_t index = 0; index < kBlocks; ++index) {
            const CopyEntry &entry = entries[index];
            const uint32_t sourceSlot = static_cast<uint32_t>(entry.remoteSourceOffset / kStrideBytes);
            std::string error;
            const uint8_t *destination = mDestination.Data() + entry.localDestinationOffset;
            if (!VerifyBlock(destination, patternGeneration, sourceSlot, error) || !VerifyGap(destination, error)) {
                throw std::runtime_error("destination validation failed for request " + std::to_string(index) +
                    ": " + error);
            }
        }
    }

    template <typename Predicate>
    void WaitData(const char *what, Predicate predicate)
    {
        CheckFatal(what);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        uint32_t spins = 0;
        while (!predicate()) {
            CheckFatal(what);
            CpuRelax();
            ++spins;
            if ((spins & (kDataDeadlineCheckInterval - 1)) == 0 &&
                std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(std::string("timed out waiting for ") + what);
            }
        }
        CheckFatal(what);
    }

    template <typename Predicate>
    void WaitControl(const char *what, Predicate predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        while (!predicate()) {
            CheckFatal(what);
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(std::string("timed out waiting for ") + what);
            }
            std::this_thread::yield();
        }
        CheckFatal(what);
    }

    void WaitForChannel()
    {
        std::unique_lock<std::mutex> lock(mChannelMutex);
        const bool received = mChannelCv.wait_for(lock, std::chrono::seconds(mOptions.timeoutSec), [this] {
            return mChannel != nullptr || mFatal.load(std::memory_order_acquire);
        });
        if (!received) {
            throw std::runtime_error("timed out waiting for the peer channel");
        }
        if (mChannel == nullptr) {
            CheckFatal("peer channel");
            throw std::runtime_error("peer channel was not established");
        }
    }

    UBSHcomChannelPtr ChannelCopy() const
    {
        std::lock_guard<std::mutex> lock(mChannelMutex);
        return mChannel;
    }

    void RequireOk(int rc, const char *operation)
    {
        if (rc != 0) {
            throw std::runtime_error(std::string(operation) + " failed: " + std::to_string(rc));
        }
    }

    void RecordFailure(const std::string &message) noexcept
    {
        bool expected = false;
        if (!mFatal.compare_exchange_strong(expected, true, std::memory_order_release, std::memory_order_relaxed)) {
            return;
        }
        try {
            std::lock_guard<std::mutex> lock(mErrorMutex);
            mError = message;
        } catch (...) {
            // A callback must not throw through hcom.  The fatal flag is still
            // sufficient for the app thread to stop safely.
        }
        mChannelCv.notify_all();
    }

    void CheckFatal(const char *where) const
    {
        if (!mFatal.load(std::memory_order_acquire)) {
            return;
        }
        std::string error = "unknown asynchronous failure";
        {
            std::lock_guard<std::mutex> lock(mErrorMutex);
            if (!mError.empty()) {
                error = mError;
            }
        }
        throw std::runtime_error(std::string(where) + ": " + error);
    }

    bool DrainUntilComplete() noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
        while (std::chrono::steady_clock::now() < deadline) {
            const uint64_t expectedData = mAppCounters.attemptedDataCallbacks;
            const uint64_t expectedSend = ExpectedSendCallbacks();
            if (mCallbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
                mCallbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend &&
                mCallbackCounters.activeCallbacks.load(std::memory_order_acquire) == 0) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return mCallbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) >=
                   mAppCounters.attemptedDataCallbacks &&
            mCallbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= ExpectedSendCallbacks() &&
            mCallbackCounters.activeCallbacks.load(std::memory_order_acquire) == 0;
    }

    void Teardown() noexcept
    {
        if (mTearingDown.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        UBSHcomChannelPtr channel;
        {
            std::lock_guard<std::mutex> lock(mChannelMutex);
            channel = mChannel;
            mChannel.Set(nullptr);
        }
        if (mService != nullptr && channel != nullptr) {
            mService->Disconnect(channel);
        }
        if (mService != nullptr && mDestinationMrRegistered) {
            mService->DestroyMemoryRegion(mDestinationMr);
            mDestinationMrRegistered = false;
        }
        if (mService != nullptr && mSourceMrRegistered) {
            mService->DestroyMemoryRegion(mSourceMr);
            mSourceMrRegistered = false;
        }
        if (mService != nullptr) {
            UBSHcomService::Destroy(mServiceName);
            mService = nullptr;
        }
    }

    void PrintLocalResult() const
    {
        std::ostringstream output;
        output << std::fixed << std::setprecision(3);
        output << "{\"schema_version\":3,\"protocol\":\"sparse-copy-v3\""
               << ",\"measurement\":\"local-sparse-copy\",\"result_role\":\"local\""
               << ",\"case\":\"SC-B1\",\"status\":\"ok\",\"commit\":\"" << RDMA_600_GIT_COMMIT
               << "\",\"role\":\"local\",\"kind\":\"" << KindName(mOptions.kind)
               << "\",\"optimization\":\"stage1.5-AB\",\"data_wait\":\"busy-poll-relax\""
               << ",\"deadline_check_interval\":" << kDataDeadlineCheckInterval
               << ",\"counter_alignment_bytes\":" << kCounterAlignment
               << ",\"callback_allocation\":\"per-request\""
               << ",\"links\":1,\"blocks\":600,\"block_bytes\":1024,\"mode\":\"direct\""
               << ",\"remote_layout\":\"direct-stride-4096\",\"tls_enabled\":false,\"rounds_in_flight\":1"
               << ",\"source_format\":\"direct-pairs\",\"source_address_count\":600"
               << ",\"destination_address_count\":600,\"request_descriptor_bytes\":9600"
               << ",\"request_bytes\":9664,\"request_send_wr_per_call\":1"
               << ",\"data_wr_per_call_expected\":600,\"completion_send_wr_per_call_expected\":1"
               << ",\"imm_events_per_call_expected\":0,\"ack_wr_per_call\":0"
               << ",\"payload_bytes_per_call\":614400,\"verify_passed\":true";
        if (mOptions.kind == RunKind::Verify) {
            output << ",\"measure_rounds\":0,\"sparse_copy_avg_us\":null,\"sparse_copy_p50_us\":null"
                   << ",\"sparse_copy_p95_us\":null,\"sparse_copy_p99_us\":null"
                   << ",\"effective_GBps\":null,\"block_Mops\":null,\"request_GBps\":null"
                   << ",\"measured_wall_seconds\":null";
        } else {
            const uint64_t wallNs = mMeasureWallEndNs - mMeasureWallStartNs;
            const double wallSeconds = static_cast<double>(wallNs) / 1000000000.0;
            const double effectiveGbps = static_cast<double>(mParams.measureRounds) * kPayloadBytes / wallSeconds / 1e9;
            const double blockMops = static_cast<double>(mParams.measureRounds) * kBlocks / wallSeconds / 1e6;
            const double requestGbps = static_cast<double>(mParams.measureRounds) * kCopyReqWireBytes /
                wallSeconds / 1e9;
            output << ",\"measure_rounds\":" << mParams.measureRounds
                   << ",\"sparse_copy_avg_us\":" << AverageNs(mSparseCopyNs) / 1000.0
                   << ",\"sparse_copy_p50_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.50))
                   << ",\"sparse_copy_p95_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.95))
                   << ",\"sparse_copy_p99_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.99))
                   << ",\"effective_GBps\":" << effectiveGbps
                   << ",\"block_Mops\":" << blockMops << ",\"request_GBps\":" << requestGbps
                   << ",\"measured_wall_seconds\":" << wallSeconds;
        }
        output << "}";
        std::cout << output.str() << std::endl;
    }

    void PrintRemoteStatus() const
    {
        std::cout << "{\"schema_version\":3,\"protocol\":\"sparse-copy-v3\",\"case\":\"SC-B1\""
                  << ",\"role\":\"remote\",\"status\":\"ok\",\"processed_calls\":"
                  << mParams.TotalRounds() << "}" << std::endl;
    }

    Options mOptions;
    CaseParameters mParams;
    std::string mServiceName;
    UBSHcomService *mService = nullptr;
    mutable std::mutex mChannelMutex;
    std::condition_variable mChannelCv;
    UBSHcomChannelPtr mChannel;
    std::atomic<bool> mRemoteReady{false};
    std::atomic<bool> mHandshakeComplete{false};
    std::atomic<bool> mTearingDown{false};
    std::atomic<bool> mFatal{false};
    mutable std::mutex mErrorMutex;
    std::string mError;

    AlignedBuffer mSource;
    AlignedBuffer mDestination;
    UBSHcomRegMemoryRegion mSourceMr;
    UBSHcomRegMemoryRegion mDestinationMr;
    bool mSourceMrRegistered = false;
    bool mDestinationMrRegistered = false;
    UBSHcomMemoryKey mSourceKey{};
    UBSHcomMemoryKey mDestinationKey{};
    UBSHcomMemoryKey mPeerDestinationKey{};
    uintptr_t mPeerDestinationAddress = 0;
    uint64_t mPeerDestinationBytes = 0;
    uint64_t mPeerSourceBytes = 0;

    std::array<UBSHcomOneSideRequest, kBlocks> mPutRequests{};
    std::array<CopyEntry, kBlocks> mCopyEntries{};
    std::array<CopyEntry, kBlocks> mActiveCopyEntries{};
    std::array<uint8_t, kHelloWireBytes> mHelloPayload{};
    std::array<uint8_t, kReadyWireBytes> mReadyPayload{};
    std::array<uint8_t, kReadyWireBytes> mReadyResponse{};
    std::array<uint8_t, kCopyReqWireBytes> mCopyReqPayload{};
    std::array<uint8_t, kCopyReqWireBytes> mPendingCopyReqPayload{};
    std::array<uint8_t, kCopyReqWireBytes> mActiveCopyReqPayload{};
    uint32_t mPendingCopyReqBytes = 0;
    uint32_t mActiveCopyReqBytes = 0;
    std::atomic<bool> mPendingCopyReqReady{false};
    std::atomic<bool> mPendingCopyReqPublished{false};
    std::array<uint8_t, kCompletionWireBytes> mDataDonePayload{};
    std::array<uint8_t, kCopyErrorWireBytes> mCopyErrorPayload{};
    std::array<uint8_t, kFinishWireBytes> mFinishPayload{};
    std::array<uint8_t, kFinishWireBytes> mFinishAckPayload{};

    std::atomic<uint64_t> mDataDoneGeneration{0};
    std::atomic<uint64_t> mFinishGeneration{0};
    std::atomic<uint64_t> mFinishAckGeneration{0};
    AppOwnedCounters mAppCounters;
    CallbackOwnedCounters mCallbackCounters;

    std::vector<uint64_t> mSparseCopyNs;
    uint64_t mMeasureWallStartNs = 0;
    uint64_t mMeasureWallEndNs = 0;
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
        Stage1Benchmark benchmark(options);
        return benchmark.Run();
    } catch (const std::exception &error) {
        std::cerr << "ERROR: " << error.what() << std::endl;
        return 1;
    }
#endif
}
