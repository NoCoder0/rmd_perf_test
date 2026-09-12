// SPDX-License-Identifier: MulanPSL-2.0
//
// Stage 1 baseline for the 600 x 1 KiB ubs-comm RDMA experiment.
//
// This executable deliberately implements only B1:
//   * one service / one RDMA device / one worker-poll QP;
//   * 600 asynchronous 1 KiB Put operations per round, each directly targeting
//     receiver dst + i * 4096;
//   * one ROUND_READY Send after those writes on the same channel;
//   * receiver-side validation and a per-round ACK (no staging or CPU scatter).
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
#include <mutex>
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

constexpr uint16_t kProtocolVersion = 2;
constexpr uint16_t kLinks = 1;
constexpr uint32_t kBlocks = 600;
constexpr uint32_t kBlockBytes = 1024;
constexpr uint32_t kStrideBytes = 4096;
constexpr uint64_t kPayloadBytes = static_cast<uint64_t>(kBlocks) * kBlockBytes;
constexpr uint64_t kDestinationBytes = static_cast<uint64_t>(kBlocks) * kStrideBytes;
constexpr uint8_t kDstGapSentinel = 0xa5;
constexpr uint8_t kSourceGapSentinel = 0x5a;

static_assert(kPayloadBytes == 614400, "the benchmark payload is fixed by design");

constexpr uint16_t kOpHello = 700;
constexpr uint16_t kOpReady = 701;
constexpr uint16_t kOpRoundReady = 702;
constexpr uint16_t kOpRoundAck = 703;
constexpr uint16_t kOpFinish = 704;
constexpr uint16_t kOpFinishAck = 705;

constexpr uint32_t kHelloMagic = 0x52443630U;      // "RD60"
constexpr uint32_t kReadyMagic = 0x52445259U;      // "RDRY"
constexpr uint32_t kRoundReadyMagic = 0x52445244U; // "RDRD"
constexpr uint32_t kAckMagic = 0x5244414bU;        // "RDAK"
constexpr uint32_t kFinishMagic = 0x5244464eU;     // "RDFN"
constexpr uint32_t kFinishAckMagic = 0x52444641U;  // "RDFA"

constexpr size_t kHelloWireBytes = 32;
constexpr size_t kMemoryKeyWireBytes = 80;
constexpr size_t kReadyWireBytes = kHelloWireBytes + 16 + kMemoryKeyWireBytes;
constexpr size_t kAckWireBytes = 16;
constexpr size_t kFinishWireBytes = 16;

enum class Role { Sender, Receiver };
enum class RunKind { Verify, Measure };

struct Options {
    Role role = Role::Sender;
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

struct ReadyInfo {
    CaseParameters params;
    uint64_t destinationAddress = 0;
    uint64_t destinationBytes = 0;
    UBSHcomMemoryKey destinationKey{};
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
        void *memory = nullptr;
        const int rc = posix_memalign(&memory, 4096, size);
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
            std::free(mData);
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

std::array<uint8_t, kHelloWireBytes> EncodeHello(const CaseParameters &params)
{
    std::array<uint8_t, kHelloWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kHelloMagic);
    EncodeParams(cursor, params);
    return payload;
}

bool DecodeHello(const void *data, uint32_t size, CaseParameters &params)
{
    if (data == nullptr || size != kHelloWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kHelloMagic) {
        return false;
    }
    params = DecodeParams(cursor);
    return true;
}

std::array<uint8_t, kReadyWireBytes> EncodeReady(const ReadyInfo &info)
{
    std::array<uint8_t, kReadyWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kReadyMagic);
    EncodeParams(cursor, info.params);
    PutU64(cursor, info.destinationAddress);
    PutU64(cursor, info.destinationBytes);
    EncodeMemoryKey(cursor, info.destinationKey);
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
    info.destinationAddress = GetU64(cursor);
    info.destinationBytes = GetU64(cursor);
    info.destinationKey = DecodeMemoryKey(cursor);
    return true;
}

std::array<uint8_t, kAckWireBytes> EncodeAck(uint32_t magic, uint64_t generation)
{
    std::array<uint8_t, kAckWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, magic);
    PutU64(cursor, generation);
    PutU16(cursor, 0);  // rail 0 in stage 1
    PutU16(cursor, 0);
    return payload;
}

bool DecodeAck(const void *data, uint32_t size, uint32_t magic, uint64_t &generation)
{
    if (data == nullptr || size != kAckWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != magic) {
        return false;
    }
    generation = GetU64(cursor);
    return GetU16(cursor) == 0 && GetU16(cursor) == 0;
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

std::string RoleName(Role role)
{
    return role == Role::Sender ? "sender" : "receiver";
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
           << "  rdma_600 --role receiver --rdma-ip <ip> --listen <oob-ip:port> [options]\n"
           << "  rdma_600 --role sender --rdma-ip <ip> --peer <receiver-oob-ip:port> [options]\n"
           << "  rdma_600 --self-test\n\n"
           << "Stage 1 is fixed to: --links 1 --mode plain; 600 Put writes directly to "
              "dst + i * 4096, with TLS disabled.\n"
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
    if (role == "sender") {
        options.role = Role::Sender;
        options.peer = required("--peer");
        if (values.count("--listen") != 0) {
            throw std::runtime_error("--listen is only valid for receiver");
        }
    } else if (role == "receiver") {
        options.role = Role::Receiver;
        options.listen = required("--listen");
        if (values.count("--peer") != 0) {
            throw std::runtime_error("--peer is only valid for sender");
        }
    } else {
        throw std::runtime_error("--role must be sender or receiver");
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

    if (optional("--links", "1") != "1" || optional("--mode", "plain") != "plain") {
        throw std::runtime_error("this stage-1 binary only supports B1: links=1 plain direct dst Put");
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

bool RunSelfTest()
{
    try {
        CaseParameters parameters;
        parameters.verifyRounds = 20;
        parameters.warmupRounds = 0;
        parameters.measureRounds = 0;
        const auto hello = EncodeHello(parameters);
        CaseParameters decoded{};
        if (!DecodeHello(hello.data(), static_cast<uint32_t>(hello.size()), decoded) || !SameParams(parameters, decoded)) {
            throw std::runtime_error("HELLO wire round trip failed");
        }

        UBSHcomMemoryKey key{};
        for (size_t index = 0; index < std::size(key.keys); ++index) {
            key.keys[index] = 0x1000U + index;
            key.tokens[index] = 0x2000U + index;
        }
        for (size_t index = 0; index < std::size(key.eid); ++index) {
            key.eid[index] = static_cast<uint8_t>(index);
        }
        ReadyInfo ready{parameters, 0x12345000U, kDestinationBytes, key};
        const auto readyWire = EncodeReady(ready);
        ReadyInfo decodedReady{};
        if (!DecodeReady(readyWire.data(), static_cast<uint32_t>(readyWire.size()), decodedReady) ||
            !SameParams(ready.params, decodedReady.params) ||
            ready.destinationAddress != decodedReady.destinationAddress ||
            ready.destinationBytes != decodedReady.destinationBytes ||
            std::memcmp(&ready.destinationKey, &decodedReady.destinationKey, sizeof(key)) != 0) {
            throw std::runtime_error("READY wire round trip failed");
        }

        const auto roundReadyWire = EncodeAck(kRoundReadyMagic, 7);
        uint64_t decodedGeneration = 0;
        if (!DecodeAck(roundReadyWire.data(), static_cast<uint32_t>(roundReadyWire.size()), kRoundReadyMagic,
                decodedGeneration) ||
            decodedGeneration != 7) {
            throw std::runtime_error("ROUND_READY wire round trip failed");
        }

        AlignedBuffer source;
        AlignedBuffer destination;
        source.Allocate(static_cast<size_t>(kBlocks) * kStrideBytes);
        destination.Allocate(static_cast<size_t>(kBlocks) * kStrideBytes);
        std::memset(source.Data(), kSourceGapSentinel, source.Size());
        std::memset(destination.Data(), kDstGapSentinel, destination.Size());

        constexpr uint64_t generation = 7;
        for (uint32_t block = 0; block < kBlocks; ++block) {
            FillBlock(source.Data() + static_cast<size_t>(block) * kStrideBytes, generation, block);
            std::memcpy(destination.Data() + static_cast<size_t>(block) * kStrideBytes,
                source.Data() + static_cast<size_t>(block) * kStrideBytes, kBlockBytes);
        }
        for (uint32_t block = 0; block < kBlocks; ++block) {
            std::string error;
            if (!VerifyBlock(destination.Data() + static_cast<size_t>(block) * kStrideBytes, generation, block, error) ||
                !VerifyGap(destination.Data() + static_cast<size_t>(block) * kStrideBytes, error)) {
                throw std::runtime_error(error);
            }
        }
        std::cout << "SELF_TEST: PASS (600 direct blocks, 614400 bytes, dst stride 4096)" << std::endl;
        return true;
    } catch (const std::exception &error) {
        std::cerr << "SELF_TEST: FAIL: " << error.what() << std::endl;
        return false;
    }
}

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
            if (mOptions.role == Role::Receiver) {
                mReceiverReady.store(true, std::memory_order_release);
                std::cout << "LISTENING role=receiver endpoint=" << mOptions.listen << " rdma_ip=" << mOptions.rdmaIp
                          << std::endl;
                std::cout.flush();
                WaitForChannel();
                RunReceiver();
            } else {
                ConnectSender();
                Handshake();
                BuildPutRequests();
                RunSender();
                PrintSenderResult();
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
        options.maxSendRecvDataSize = 1024;
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
            ActiveCallbackGuard guard(mActiveCallbacks);
            return 0;
        });
        mService->RegisterOneSideHandler([this](const UBSHcomServiceContext &) {
            ActiveCallbackGuard guard(mActiveCallbacks);
            return 0;
        });
        mService->RegisterChannelBrokenHandler(
            [this](const UBSHcomChannelPtr &) {
                ActiveCallbackGuard guard(mActiveCallbacks);
                if (!mTearingDown.load(std::memory_order_acquire)) {
                    RecordFailure("hcom channel broken");
                }
            },
            UBSHcomChannelBrokenPolicy::BROKEN_ALL);

        if (mOptions.role == Role::Receiver) {
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
        if (mOptions.role == Role::Sender) {
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
            FillSenderMeasurePattern();
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
        ActiveCallbackGuard guard(mActiveCallbacks);
        if (mOptions.role != Role::Receiver || channel == nullptr) {
            RecordFailure("unexpected new channel");
            return -1;
        }
        if (!ConfigureChannel(channel)) {
            return -1;
        }
        {
            std::lock_guard<std::mutex> lock(mChannelMutex);
            if (mChannel != nullptr) {
                RecordFailure("receiver accepted more than one channel in stage 1");
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

    void ConnectSender()
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
        mHelloPayload = EncodeHello(mParams);
        UBSHcomRequest request(mHelloPayload.data(), static_cast<uint32_t>(mHelloPayload.size()), kOpHello);
        UBSHcomResponse response(mReadyResponse.data(), static_cast<uint32_t>(mReadyResponse.size()));
        RequireOk(channel->Call(request, response, nullptr), "HELLO Call");
        ReadyInfo ready{};
        if (!DecodeReady(response.address, response.size, ready) || !SameParams(mParams, ready.params) ||
            ready.destinationAddress == 0 || ready.destinationBytes != kDestinationBytes) {
            throw std::runtime_error("invalid READY response");
        }
        mPeerDestinationAddress = static_cast<uintptr_t>(ready.destinationAddress);
        mPeerDestinationKey = ready.destinationKey;
    }

    void BuildPutRequests()
    {
        if (mPeerDestinationAddress == 0) {
            throw std::runtime_error("cannot prepare Put descriptors without READY");
        }
        for (uint32_t block = 0; block < kBlocks; ++block) {
            UBSHcomOneSideRequest &request = mPutRequests[block];
            request.lAddress = reinterpret_cast<uintptr_t>(mSource.Data()) + static_cast<uintptr_t>(block) * kStrideBytes;
            request.rAddress = mPeerDestinationAddress + static_cast<uintptr_t>(block) * kStrideBytes;
            request.lKey = mSourceKey;
            request.rKey = mPeerDestinationKey;
            request.size = kBlockBytes;
        }
    }

    int OnIncoming(UBSHcomServiceContext &context) noexcept
    {
        ActiveCallbackGuard guard(mActiveCallbacks);
        if (context.Result() != 0) {
            RecordFailure("incoming hcom context failed: " + std::to_string(context.Result()));
            return context.Result();
        }
        switch (context.OpCode()) {
            case kOpHello:
                return OnHello(context);
            case kOpRoundReady:
                return OnRoundReady(context);
            case kOpRoundAck:
                return OnRoundAck(context);
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
        if (mOptions.role != Role::Receiver || !mReceiverReady.load(std::memory_order_acquire)) {
            RecordFailure("HELLO received before receiver initialization");
            return -1;
        }
        CaseParameters peer{};
        if (!DecodeHello(context.MessageData(), context.MessageDataLen(), peer) || !SameParams(peer, mParams)) {
            RecordFailure("HELLO parameters do not match the local B1 configuration");
            return -1;
        }
        ReadyInfo ready{};
        ready.params = mParams;
        ready.destinationAddress = reinterpret_cast<uintptr_t>(mDestination.Data());
        ready.destinationBytes = mDestination.Size();
        ready.destinationKey = mDestinationKey;
        mReadyPayload = EncodeReady(ready);
        Callback *callback = NewSendCallback();
        if (callback == nullptr) {
            RecordFailure("unable to allocate READY callback");
            return -1;
        }
        mExpectedSendCallbacks.fetch_add(1, std::memory_order_relaxed);
        const UBSHcomRequest reply(mReadyPayload.data(), static_cast<uint32_t>(mReadyPayload.size()), kOpReady);
        const UBSHcomReplyContext replyContext(context.RspCtx(), 0);
        const int rc = context.Channel()->Reply(replyContext, reply, callback);
        if (rc != 0) {
            // hcom owns or destroys the asynchronous callback on its own error
            // paths.  Do not delete it here and risk a double free.
            RecordFailure("READY Reply failed: " + std::to_string(rc));
            return rc;
        }
        return 0;
    }

    int OnRoundReady(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Receiver) {
            RecordFailure("sender received ROUND_READY");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeAck(context.MessageData(), context.MessageDataLen(), kRoundReadyMagic, generation) || generation == 0) {
            RecordFailure("invalid ROUND_READY payload");
            return -1;
        }
        const uint64_t previous = mRoundReadyGeneration.load(std::memory_order_relaxed);
        if (generation != previous + 1) {
            RecordFailure("ROUND_READY generation is not strictly increasing");
            return -1;
        }
        // A successful SEND on this QP follows all 600 direct WRITE requests.
        // The release store only publishes that complete-round state to the app
        // thread; it is not a substitute for RDMA DMA ordering.
        mRoundReadyGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnRoundAck(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Sender) {
            RecordFailure("receiver received ROUND_ACK");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeAck(context.MessageData(), context.MessageDataLen(), kAckMagic, generation) || generation == 0) {
            RecordFailure("invalid ROUND_ACK payload");
            return -1;
        }
        const uint64_t previous = mAckGeneration.load(std::memory_order_relaxed);
        if (generation != previous + 1) {
            RecordFailure("ROUND_ACK generation is not strictly increasing");
            return -1;
        }
        mAckGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnFinish(UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Receiver) {
            RecordFailure("sender received FINISH");
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
        if (mOptions.role != Role::Sender) {
            RecordFailure("receiver received FINISH_ACK");
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
                ActiveCallbackGuard guard(mActiveCallbacks);
                if (context.Result() != 0) {
                    RecordFailure("Put callback failed: " + std::to_string(context.Result()));
                    return;
                }
                mDataDoneCallbacks.fetch_add(1, std::memory_order_release);
            },
            std::placeholders::_1);
    }

    Callback *NewSendCallback()
    {
        return UBSHcomNewCallback(
            [this](UBSHcomServiceContext &context) {
                ActiveCallbackGuard guard(mActiveCallbacks);
                if (context.Result() != 0) {
                    RecordFailure("Send/Reply callback failed: " + std::to_string(context.Result()));
                    return;
                }
                mSendDoneCallbacks.fetch_add(1, std::memory_order_release);
            },
            std::placeholders::_1);
    }

    void RunSender()
    {
        uint64_t generation = 1;
        for (uint32_t round = 0; round < mParams.verifyRounds; ++round, ++generation) {
            FillSenderVerifyPattern(generation);
            RunSenderRound(generation, false);
        }
        // The formal path uses stable pre-generated content.  It does not spend
        // 600 KiB of per-round generation or validation work inside the timing.
        FillSenderMeasurePattern();
        for (uint32_t round = 0; round < mParams.warmupRounds; ++round, ++generation) {
            RunSenderRound(generation, false);
        }
        if (mParams.measureRounds != 0) {
            mMeasureWallStartNs = NowNs();
            mSubmitNs.reserve(mParams.measureRounds);
            mE2eNs.reserve(mParams.measureRounds);
            for (uint32_t round = 0; round < mParams.measureRounds; ++round, ++generation) {
                RunSenderRound(generation, true);
            }
            mMeasureWallEndNs = NowNs();
        }
        SendFinish();
    }

    void RunSenderRound(uint64_t generation, bool measure)
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("sender lost its channel before a round");
        }
        const uint64_t expectedData = mExpectedDataCallbacks.load(std::memory_order_relaxed) + kBlocks;
        const uint64_t expectedSend = mExpectedSendCallbacks.load(std::memory_order_relaxed) + 1;
        const uint64_t startNs = NowNs();

        for (uint32_t block = 0; block < kBlocks; ++block) {
            Callback *callback = NewDataCallback();
            if (callback == nullptr) {
                throw std::runtime_error("unable to allocate Put callback");
            }
            mExpectedDataCallbacks.fetch_add(1, std::memory_order_relaxed);
            const int rc = channel->Put(mPutRequests[block], callback);
            if (rc != 0) {
                throw std::runtime_error("Put failed: " + std::to_string(rc));
            }
        }
        mRoundReadyPayload = EncodeAck(kRoundReadyMagic, generation);
        PostAsyncSend(channel, mRoundReadyPayload.data(), mRoundReadyPayload.size(), kOpRoundReady);
        const uint64_t submitNs = NowNs();
        WaitUntil("round completion", [this, generation, expectedData, expectedSend] {
            return mAckGeneration.load(std::memory_order_acquire) >= generation &&
                mDataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
                mSendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend;
        });
        const uint64_t endNs = NowNs();
        if (measure) {
            mSubmitNs.push_back(submitNs - startNs);
            mE2eNs.push_back(endNs - startNs);
        }
    }

    void RunReceiver()
    {
        for (uint64_t generation = 1; generation <= mParams.TotalRounds(); ++generation) {
            WaitUntil("ROUND_READY", [this, generation] {
                return mRoundReadyGeneration.load(std::memory_order_acquire) >= generation;
            });
            if (generation <= mParams.verifyRounds) {
                VerifyReceiverDestination(generation);
            }
            SendRoundAck(generation);
        }
        if (mParams.measureRounds != 0) {
            VerifyFinalMeasureDestination();
        }
        WaitUntil("FINISH", [this] {
            return mFinishGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
        });
        SendFinishAck();
    }

    void VerifyReceiverDestination(uint64_t generation)
    {
        for (uint32_t block = 0; block < kBlocks; ++block) {
            const uint8_t *destination = mDestination.Data() + static_cast<size_t>(block) * kStrideBytes;
            std::string error;
            if (!VerifyBlock(destination, generation, block, error) || !VerifyGap(destination, error)) {
                throw std::runtime_error(error);
            }
        }
    }

    void SendRoundAck(uint64_t generation)
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("receiver lost its channel before ROUND_ACK");
        }
        mAckPayload = EncodeAck(kAckMagic, generation);
        PostAsyncSend(channel, mAckPayload.data(), mAckPayload.size(), kOpRoundAck);
        const uint64_t target = mExpectedSendCallbacks.load(std::memory_order_acquire);
        WaitUntil("ROUND_ACK local completion", [this, target] {
            return mSendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
    }

    void SendFinish()
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("sender lost its channel before FINISH");
        }
        mFinishPayload = EncodeFinish(kFinishMagic, mParams.TotalRounds());
        PostAsyncSend(channel, mFinishPayload.data(), mFinishPayload.size(), kOpFinish);
        const uint64_t target = mExpectedSendCallbacks.load(std::memory_order_acquire);
        WaitUntil("FINISH local completion", [this, target] {
            return mSendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
        WaitUntil("FINISH_ACK", [this] {
            return mFinishAckGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
        });
    }

    void SendFinishAck()
    {
        const UBSHcomChannelPtr channel = ChannelCopy();
        if (channel == nullptr) {
            throw std::runtime_error("receiver lost its channel before FINISH_ACK");
        }
        mFinishAckPayload = EncodeFinish(kFinishAckMagic, mParams.TotalRounds());
        PostAsyncSend(channel, mFinishAckPayload.data(), mFinishAckPayload.size(), kOpFinishAck);
        const uint64_t target = mExpectedSendCallbacks.load(std::memory_order_acquire);
        WaitUntil("FINISH_ACK local completion", [this, target] {
            return mSendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
    }

    void PostAsyncSend(const UBSHcomChannelPtr &channel, uint8_t *data, size_t size, uint16_t opcode)
    {
        Callback *callback = NewSendCallback();
        if (callback == nullptr) {
            throw std::runtime_error("unable to allocate Send callback");
        }
        mExpectedSendCallbacks.fetch_add(1, std::memory_order_relaxed);
        const UBSHcomRequest request(data, static_cast<uint32_t>(size), opcode);
        const int rc = channel->Send(request, callback);
        if (rc != 0) {
            throw std::runtime_error("Send opcode " + std::to_string(opcode) + " failed: " + std::to_string(rc));
        }
    }

    void FillSenderVerifyPattern(uint64_t generation)
    {
        for (uint32_t block = 0; block < kBlocks; ++block) {
            FillBlock(mSource.Data() + static_cast<size_t>(block) * kStrideBytes, generation, block);
        }
    }

    void FillSenderMeasurePattern()
    {
        for (uint32_t block = 0; block < kBlocks; ++block) {
            FillBlock(mSource.Data() + static_cast<size_t>(block) * kStrideBytes, 0, block);
        }
    }

    void VerifyFinalMeasureDestination()
    {
        for (uint32_t block = 0; block < kBlocks; ++block) {
            std::string error;
            const uint8_t *destination = mDestination.Data() + static_cast<size_t>(block) * kStrideBytes;
            if (!VerifyBlock(destination, 0, block, error) || !VerifyGap(destination, error)) {
                throw std::runtime_error("final measure validation failed: " + error);
            }
        }
    }

    template <typename Predicate>
    void WaitUntil(const char *what, Predicate predicate)
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
            const uint64_t expectedData = mExpectedDataCallbacks.load(std::memory_order_acquire);
            const uint64_t expectedSend = mExpectedSendCallbacks.load(std::memory_order_acquire);
            if (mDataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
                mSendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend &&
                mActiveCallbacks.load(std::memory_order_acquire) == 0) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return mDataDoneCallbacks.load(std::memory_order_acquire) >=
                   mExpectedDataCallbacks.load(std::memory_order_acquire) &&
            mSendDoneCallbacks.load(std::memory_order_acquire) >=
                   mExpectedSendCallbacks.load(std::memory_order_acquire) &&
            mActiveCallbacks.load(std::memory_order_acquire) == 0;
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

    void PrintSenderResult() const
    {
        std::ostringstream output;
        output << std::fixed << std::setprecision(3);
        output << "{\"case\":\"B1\",\"status\":\"ok\",\"commit\":\"" << RDMA_600_GIT_COMMIT
               << "\",\"role\":\"sender\",\"kind\":\"" << KindName(mOptions.kind)
               << "\",\"links\":1,\"blocks\":600,\"block_bytes\":1024,\"payload_bytes\":614400"
               << ",\"mode\":\"plain\",\"remote_layout\":\"direct-stride-4096\",\"tls_enabled\":false"
               << ",\"rounds_in_flight\":1,\"data_wr_per_round\":600,\"round_ready_wr_per_round\":1"
               << ",\"ack_wr_per_round\":1,\"verify_passed\":true";
        if (mOptions.kind == RunKind::Verify) {
            output << ",\"measure_rounds\":0,\"submit_p50_us\":null,\"e2e_avg_us\":null,\"e2e_p50_us\":null"
                   << ",\"e2e_p95_us\":null,\"e2e_p99_us\":null,\"effective_GBps\":null,\"block_Mops\":null";
        } else {
            const uint64_t wallNs = mMeasureWallEndNs - mMeasureWallStartNs;
            const double wallSeconds = static_cast<double>(wallNs) / 1000000000.0;
            const double effectiveGbps = static_cast<double>(mParams.measureRounds) * kPayloadBytes / wallSeconds / 1e9;
            const double blockMops = static_cast<double>(mParams.measureRounds) * kBlocks / wallSeconds / 1e6;
            output << ",\"measure_rounds\":" << mParams.measureRounds
                   << ",\"submit_p50_us\":" << NsToUs(PercentileNs(mSubmitNs, 0.50))
                   << ",\"e2e_avg_us\":" << AverageNs(mE2eNs) / 1000.0
                   << ",\"e2e_p50_us\":" << NsToUs(PercentileNs(mE2eNs, 0.50))
                   << ",\"e2e_p95_us\":" << NsToUs(PercentileNs(mE2eNs, 0.95))
                   << ",\"e2e_p99_us\":" << NsToUs(PercentileNs(mE2eNs, 0.99))
                   << ",\"effective_GBps\":" << effectiveGbps
                   << ",\"block_Mops\":" << blockMops;
        }
        output << "}";
        std::cout << output.str() << std::endl;
    }

    Options mOptions;
    CaseParameters mParams;
    std::string mServiceName;
    UBSHcomService *mService = nullptr;
    mutable std::mutex mChannelMutex;
    std::condition_variable mChannelCv;
    UBSHcomChannelPtr mChannel;
    std::atomic<bool> mReceiverReady{false};
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

    std::array<UBSHcomOneSideRequest, kBlocks> mPutRequests{};
    std::array<uint8_t, kHelloWireBytes> mHelloPayload{};
    std::array<uint8_t, kReadyWireBytes> mReadyPayload{};
    std::array<uint8_t, kReadyWireBytes> mReadyResponse{};
    std::array<uint8_t, kAckWireBytes> mRoundReadyPayload{};
    std::array<uint8_t, kAckWireBytes> mAckPayload{};
    std::array<uint8_t, kFinishWireBytes> mFinishPayload{};
    std::array<uint8_t, kFinishWireBytes> mFinishAckPayload{};

    std::atomic<uint64_t> mRoundReadyGeneration{0};
    std::atomic<uint64_t> mAckGeneration{0};
    std::atomic<uint64_t> mFinishGeneration{0};
    std::atomic<uint64_t> mFinishAckGeneration{0};
    std::atomic<uint64_t> mExpectedDataCallbacks{0};
    std::atomic<uint64_t> mDataDoneCallbacks{0};
    std::atomic<uint64_t> mExpectedSendCallbacks{0};
    std::atomic<uint64_t> mSendDoneCallbacks{0};
    std::atomic<uint64_t> mActiveCallbacks{0};

    std::vector<uint64_t> mSubmitNs;
    std::vector<uint64_t> mE2eNs;
    uint64_t mMeasureWallStartNs = 0;
    uint64_t mMeasureWallEndNs = 0;
};

}  // namespace

int main(int argc, char **argv)
{
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
}
