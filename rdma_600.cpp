// SPDX-License-Identifier: MulanPSL-2.0
//
// Stage 2 direct B1/B2 benchmark for the 600 x 1 KiB ubs-comm RDMA
// experiment. Each rail owns one service, one explicitly selected RDMA NIC,
// one linkCount=1 worker-poll channel/QP, one MR, and independent protocol
// state. Internal hcom multirail is disabled.

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

constexpr uint16_t kProtocolVersion = 3;
constexpr uint16_t kMaxLinks = 2;
constexpr uint32_t kBlocks = 600;
constexpr uint32_t kMaxBlocksPerRail = kBlocks;
constexpr uint32_t kBlockBytes = 1024;
constexpr uint32_t kStrideBytes = 4096;
constexpr uint32_t kMaxTraceRounds = 64;
constexpr uint32_t kDataDeadlineCheckInterval = 256;
constexpr uint64_t kPayloadBytes = static_cast<uint64_t>(kBlocks) * kBlockBytes;
constexpr uint8_t kDstGapSentinel = 0xa5;
constexpr uint8_t kSourceGapSentinel = 0x5a;
#if defined(__cpp_lib_hardware_interference_size)
constexpr size_t kCounterAlignment = std::hardware_destructive_interference_size;
#else
constexpr size_t kCounterAlignment = 64;
#endif

static_assert(kPayloadBytes == 614400, "the benchmark payload is fixed by design");
static_assert((kDataDeadlineCheckInterval & (kDataDeadlineCheckInterval - 1)) == 0,
    "the data deadline interval must be a power of two");

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

// Params are 32 bytes in protocol v3. HELLO/READY also carry the rail id so
// that a swapped OOB endpoint cannot silently become a valid dual-rail run.
constexpr size_t kParametersWireBytes = 32;
constexpr size_t kHelloWireBytes = 4 + kParametersWireBytes + 4;
constexpr size_t kMemoryKeyWireBytes = 80;
constexpr size_t kReadyWireBytes = 4 + kParametersWireBytes + 4 + 16 + kMemoryKeyWireBytes;
constexpr size_t kTokenWireBytes = 16;

enum class Role { Sender, Receiver };
enum class RunKind { Verify, Measure, Trace };

struct Options {
    Role role = Role::Sender;
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

struct ReadyInfo {
    CaseParameters params;
    uint16_t rail = 0;
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

bool TryNowNs(uint64_t &value) noexcept
{
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        return false;
    }
    value = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    return true;
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

std::array<uint8_t, kHelloWireBytes> EncodeHello(const CaseParameters &params, uint16_t rail)
{
    std::array<uint8_t, kHelloWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kHelloMagic);
    EncodeParams(cursor, params);
    PutU16(cursor, rail);
    PutU16(cursor, 0);
    return payload;
}

bool DecodeHello(const void *data, uint32_t size, CaseParameters &params, uint16_t &rail)
{
    if (data == nullptr || size != kHelloWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != kHelloMagic) {
        return false;
    }
    params = DecodeParams(cursor);
    rail = GetU16(cursor);
    return GetU16(cursor) == 0;
}

std::array<uint8_t, kReadyWireBytes> EncodeReady(const ReadyInfo &info)
{
    std::array<uint8_t, kReadyWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, kReadyMagic);
    EncodeParams(cursor, info.params);
    PutU16(cursor, info.rail);
    PutU16(cursor, 0);
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
    info.rail = GetU16(cursor);
    if (GetU16(cursor) != 0) {
        return false;
    }
    info.destinationAddress = GetU64(cursor);
    info.destinationBytes = GetU64(cursor);
    info.destinationKey = DecodeMemoryKey(cursor);
    return true;
}

std::array<uint8_t, kTokenWireBytes> EncodeToken(uint32_t magic, uint64_t generation, uint16_t rail)
{
    std::array<uint8_t, kTokenWireBytes> payload{};
    uint8_t *cursor = payload.data();
    PutU32(cursor, magic);
    PutU64(cursor, generation);
    PutU16(cursor, rail);
    PutU16(cursor, 0);
    return payload;
}

bool DecodeToken(const void *data, uint32_t size, uint32_t magic, uint64_t &generation, uint16_t &rail)
{
    if (data == nullptr || size != kTokenWireBytes) {
        return false;
    }
    const uint8_t *cursor = static_cast<const uint8_t *>(data);
    if (GetU32(cursor) != magic) {
        return false;
    }
    generation = GetU64(cursor);
    rail = GetU16(cursor);
    return GetU16(cursor) == 0;
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
    return role == Role::Sender ? "sender" : "receiver";
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
           << "  rdma_600 --role receiver --rdma-ips <ip0[,ip1]> --listen <ep0[,ep1]> [options]\n"
           << "  rdma_600 --role sender --rdma-ips <ip0[,ip1]> --peer <ep0[,ep1]> [options]\n"
           << "  rdma_600 --self-test\n\n"
           << "Stage 2 direct cases: --links 1 (B1) or --links 2 (B2), --mode plain.\n"
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
    if (optional("--mode", "plain") != "plain") {
        throw std::runtime_error("stage 2 supports only plain direct Put; no SGL/staging/scatter mode is available");
    }

    const std::string role = required("--role");
    if (role == "sender") {
        options.role = Role::Sender;
        options.endpoints = SplitCsv("--peer", required("--peer"));
        if (values.count("--listen") != 0) {
            throw std::runtime_error("--listen is only valid for receiver");
        }
    } else if (role == "receiver") {
        options.role = Role::Receiver;
        options.endpoints = SplitCsv("--listen", required("--listen"));
        if (values.count("--peer") != 0) {
            throw std::runtime_error("--peer is only valid for sender");
        }
    } else {
        throw std::runtime_error("--role must be sender or receiver");
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
            "stage-1.5 measure requires explicit app and per-rail worker CPUs for pinned busy polling");
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
    TracePoint s0;
    std::array<TracePoint, kMaxLinks> sPost;
    TracePoint s1;
    std::array<TracePoint, kMaxLinks> sData;
    std::array<TracePoint, kMaxLinks> sAck;
    TracePoint s2;
    std::array<TracePoint, kMaxLinks> rReady;
    std::array<TracePoint, kMaxLinks> rAck;
};

bool RunSelfTest()
{
    try {
        for (uint16_t links : {uint16_t{1}, uint16_t{2}}) {
            CaseParameters parameters;
            parameters.links = links;
            parameters.verifyRounds = 20;
            parameters.traceRounds = 3;
            for (uint16_t rail = 0; rail < links; ++rail) {
                const auto hello = EncodeHello(parameters, rail);
                CaseParameters decoded{};
                uint16_t decodedRail = kMaxLinks;
                if (!DecodeHello(hello.data(), static_cast<uint32_t>(hello.size()), decoded, decodedRail) ||
                    !SameParams(parameters, decoded) || decodedRail != rail) {
                    throw std::runtime_error("HELLO wire round trip failed");
                }

                UBSHcomMemoryKey key{};
                for (size_t index = 0; index < std::size(key.keys); ++index) {
                    key.keys[index] = 0x1000U + index + rail;
                    key.tokens[index] = 0x2000U + index + rail;
                }
                for (size_t index = 0; index < std::size(key.eid); ++index) {
                    key.eid[index] = static_cast<uint8_t>(index + rail);
                }
                ReadyInfo ready{parameters, rail, 0x12345000U, (kBlocks / links) * kStrideBytes, key};
                const auto readyWire = EncodeReady(ready);
                ReadyInfo decodedReady{};
                if (!DecodeReady(readyWire.data(), static_cast<uint32_t>(readyWire.size()), decodedReady) ||
                    !SameParams(ready.params, decodedReady.params) || decodedReady.rail != rail ||
                    ready.destinationAddress != decodedReady.destinationAddress ||
                    ready.destinationBytes != decodedReady.destinationBytes ||
                    std::memcmp(&ready.destinationKey, &decodedReady.destinationKey, sizeof(key)) != 0) {
                    throw std::runtime_error("READY wire round trip failed");
                }

                const auto token = EncodeToken(kRoundReadyMagic, 7, rail);
                uint64_t decodedGeneration = 0;
                decodedRail = kMaxLinks;
                if (!DecodeToken(token.data(), static_cast<uint32_t>(token.size()), kRoundReadyMagic,
                        decodedGeneration, decodedRail) || decodedGeneration != 7 || decodedRail != rail) {
                    throw std::runtime_error("ROUND_READY wire round trip failed");
                }
            }

            const uint32_t blocksPerRail = kBlocks / links;
            std::array<AlignedBuffer, kMaxLinks> source;
            std::array<AlignedBuffer, kMaxLinks> destination;
            constexpr uint64_t generation = 7;
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
                    std::memcpy(destination[rail].Data() + static_cast<size_t>(localBlock) * kStrideBytes,
                        source[rail].Data() + static_cast<size_t>(localBlock) * kStrideBytes, kBlockBytes);
                }
                for (uint32_t localBlock = 0; localBlock < blocksPerRail; ++localBlock) {
                    const uint32_t globalBlock = rail * blocksPerRail + localBlock;
                    const uint8_t *address = destination[rail].Data() + static_cast<size_t>(localBlock) * kStrideBytes;
                    std::string error;
                    if (!VerifyBlock(address, generation, globalBlock, error) || !VerifyGap(address, error)) {
                        throw std::runtime_error(error);
                    }
                }
            }
        }
        std::cout << "SELF_TEST: PASS (B1/B2 direct partition, 600 blocks, 614400 bytes, stride 4096, wire v3)"
                  << std::endl;
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
    std::array<UBSHcomOneSideRequest, kMaxBlocksPerRail> putRequests{};
    std::array<uint8_t, kHelloWireBytes> helloPayload{};
    std::array<uint8_t, kReadyWireBytes> readyPayload{};
    std::array<uint8_t, kReadyWireBytes> readyResponse{};
    std::array<uint8_t, kTokenWireBytes> roundReadyPayload{};
    std::array<uint8_t, kTokenWireBytes> ackPayload{};
    std::array<uint8_t, kTokenWireBytes> finishPayload{};
    std::array<uint8_t, kTokenWireBytes> finishAckPayload{};
    std::atomic<bool> helloSeen{false};
    std::atomic<uint64_t> roundReadyGeneration{0};
    std::atomic<uint64_t> ackGeneration{0};
    std::atomic<uint64_t> finishGeneration{0};
    std::atomic<uint64_t> finishAckGeneration{0};
    AppOwnedCounters appCounters;
    CallbackOwnedCounters callbackCounters;
};

enum class RailCommand : uint8_t {
    None,
    ConnectAndHandshake,
    SubmitRound,
    RunReceiver,
    SendFinish,
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
    std::atomic<uint64_t> submitEndNs{0};
};

class DirectBenchmark {
public:
    explicit DirectBenchmark(Options options) : mOptions(std::move(options))
    {
        mParams.links = mOptions.links;
        mParams.verifyRounds = mOptions.verifyRounds;
        mParams.warmupRounds = mOptions.warmupRounds;
        mParams.measureRounds = mOptions.measureRounds;
        mParams.traceRounds = mOptions.traceRounds;
        mBlocksPerRail = kBlocks / mOptions.links;
        if (TraceEnabled()) {
            const uint64_t firstGeneration = static_cast<uint64_t>(mParams.verifyRounds) + 1;
            for (uint32_t index = 0; index < mParams.traceRounds; ++index) {
                mTrace[index].generation = firstGeneration + index;
            }
        }
    }

    int Run()
    {
        try {
            PinCurrentThread(mOptions.appCpus[0]);
            SetupServices();
            SetupMemory();
            StartSecondaryRailThread();
            if (mOptions.role == Role::Receiver) {
                mReceiverReady.store(true, std::memory_order_release);
                PrintListening();
                WaitForChannels();
                RunReceiver();
            } else {
                ConnectSender();
                Handshake();
                BuildPutRequests();
                RunSender();
            }
            CheckFatal("normal completion");
            if (!DrainUntilComplete()) {
                throw std::runtime_error("completion counters did not drain before teardown");
            }
            StopSecondaryRailThread();
            if (TraceEnabled()) {
                EmitTrace();
            }
            if (mOptions.role == Role::Sender) {
                PrintSenderResult();
            }
            Teardown();
            return 0;
        } catch (const std::exception &error) {
            RecordFailure(error.what());
            std::cerr << "ERROR: " << error.what() << std::endl;
            StopSecondaryRailThread();
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
    bool TraceEnabled() const
    {
        return mOptions.kind == RunKind::Trace;
    }

    bool TraceIndex(uint64_t generation, size_t &index) const noexcept
    {
        if (!TraceEnabled()) {
            return false;
        }
        const uint64_t first = static_cast<uint64_t>(mParams.verifyRounds) + 1;
        if (generation < first || generation >= first + mParams.traceRounds) {
            return false;
        }
        index = static_cast<size_t>(generation - first);
        return true;
    }

    void PublishCallbackTrace(uint64_t generation, TracePoint &point, const char *event) noexcept
    {
        size_t index = 0;
        if (!TraceIndex(generation, index)) {
            return;
        }
        uint64_t timestamp = 0;
        if (!TryNowNs(timestamp)) {
            RecordFailure(std::string("clock_gettime failed while recording ") + event);
            return;
        }
        point.Publish(timestamp);
    }

    void StartSecondaryRailThread()
    {
        if (mOptions.links != 2) {
            return;
        }
        mSecondary.thread = std::thread([this] { SecondaryRailThreadMain(); });
        WaitControl("secondary rail application thread startup", [this] {
            return mSecondary.started.load(std::memory_order_acquire);
        });
    }

    void SecondaryRailThreadMain() noexcept
    {
        try {
            PinCurrentThread(mOptions.appCpus[1]);
            mSecondary.started.store(true, std::memory_order_release);
            uint64_t seen = 0;
            while (!mSecondary.stop.load(std::memory_order_acquire)) {
                const uint64_t issued = mSecondary.issued.load(std::memory_order_acquire);
                if (issued == seen) {
                    CpuRelax();
                    continue;
                }
                const RailCommand command = mSecondary.command.load(std::memory_order_relaxed);
                const uint64_t generation = mSecondary.generation.load(std::memory_order_relaxed);
                switch (command) {
                    case RailCommand::ConnectAndHandshake:
                        ConnectAndHandshakeRail(1);
                        break;
                    case RailCommand::SubmitRound:
                        mSecondary.submitEndNs.store(SubmitRailRound(1, generation), std::memory_order_relaxed);
                        break;
                    case RailCommand::RunReceiver:
                        RunReceiverRail(1);
                        break;
                    case RailCommand::SendFinish:
                        SendFinishRail(1);
                        break;
                    case RailCommand::None:
                        throw std::runtime_error("secondary rail received an empty command");
                }
                seen = issued;
                mSecondary.completed.store(seen, std::memory_order_release);
            }
        } catch (const std::exception &error) {
            mSecondary.started.store(true, std::memory_order_release);
            RecordFailure(std::string("rail 1 application thread: ") + error.what());
        } catch (...) {
            mSecondary.started.store(true, std::memory_order_release);
            RecordFailure("rail 1 application thread: unknown exception");
        }
    }

    uint64_t IssueSecondaryRailCommand(RailCommand command, uint64_t generation = 0)
    {
        if (mOptions.links != 2 || !mSecondary.thread.joinable()) {
            throw std::runtime_error("secondary rail command issued without its persistent thread");
        }
        const uint64_t previous = mSecondary.issued.load(std::memory_order_relaxed);
        if (mSecondary.completed.load(std::memory_order_acquire) != previous) {
            throw std::runtime_error("secondary rail already has an outstanding command");
        }
        mSecondary.generation.store(generation, std::memory_order_relaxed);
        mSecondary.command.store(command, std::memory_order_relaxed);
        const uint64_t sequence = previous + 1;
        mSecondary.issued.store(sequence, std::memory_order_release);
        return sequence;
    }

    void WaitSecondaryRailCommand(uint64_t sequence, const char *what)
    {
        WaitData(what, [this, sequence] {
            return mSecondary.completed.load(std::memory_order_acquire) >= sequence;
        });
    }

    void StopSecondaryRailThread() noexcept
    {
        if (!mSecondary.thread.joinable()) {
            return;
        }
        mSecondary.stop.store(true, std::memory_order_release);
        mSecondary.thread.join();
    }

    void SetupServices()
    {
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            SetupService(rail);
        }
    }

    void SetupService(uint16_t rail)
    {
        RailState &state = mRails[rail];
        state.serviceName = "rdma600_" + RoleName(mOptions.role) + "_" + std::to_string(rail);
        UBSHcomServiceOptions options{};
        options.maxSendRecvDataSize = 1024;
        options.workerGroupThreadCount = 1;
        options.workerGroupMode = ock::hcom::NET_BUSY_POLLING;
        if (mOptions.workerCpus[rail] >= 0) {
            const auto cpu = static_cast<uint32_t>(mOptions.workerCpus[rail]);
            options.workerGroupCpuIdsRange = {cpu, cpu};
        }
        state.service = UBSHcomService::Create(UBSHcomServiceProtocol::RDMA, state.serviceName, options);
        if (state.service == nullptr) {
            throw std::runtime_error("UBSHcomService::Create(RDMA) returned null for rail " + std::to_string(rail));
        }
        UBSHcomTlsOptions tlsOptions{};
        tlsOptions.enableTls = false;
        state.service->SetTlsOptions(tlsOptions);
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
            ActiveCallbackGuard guard(mActiveCallbacks);
            return 0;
        });
        state.service->RegisterOneSideHandler([this](const UBSHcomServiceContext &) {
            ActiveCallbackGuard guard(mActiveCallbacks);
            return 0;
        });
        state.service->RegisterChannelBrokenHandler(
            [this, rail](const UBSHcomChannelPtr &) {
                ActiveCallbackGuard guard(mActiveCallbacks);
                if (!mTearingDown.load(std::memory_order_acquire)) {
                    RecordFailure("hcom channel broken on rail " + std::to_string(rail));
                }
            },
            UBSHcomChannelBrokenPolicy::BROKEN_ALL);

        if (mOptions.role == Role::Receiver) {
            const int rc = state.service->Bind("tcp://" + mOptions.endpoints[rail],
                [this, rail](const std::string &, const UBSHcomChannelPtr &channel, const std::string &) {
                    return OnNewChannel(rail, channel);
                });
            RequireOk(rc, ("Bind rail " + std::to_string(rail)).c_str());
        }
        RequireOk(state.service->Start(), ("Start rail " + std::to_string(rail)).c_str());
    }

    void SetupMemory()
    {
        const size_t railBytes = static_cast<size_t>(mBlocksPerRail) * kStrideBytes;
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            RailState &state = mRails[rail];
            state.buffer.Allocate(railBytes);
            std::memset(state.buffer.Data(),
                mOptions.role == Role::Sender ? kSourceGapSentinel : kDstGapSentinel, state.buffer.Size());
            RequireOk(state.service->RegisterMemoryRegion(
                          reinterpret_cast<uintptr_t>(state.buffer.Data()), state.buffer.Size(), state.memoryRegion),
                ("RegisterMemoryRegion rail " + std::to_string(rail)).c_str());
            state.memoryRegistered = true;
            state.memoryKey = {};
            state.memoryRegion.GetMemoryKey(state.memoryKey);
            if (state.memoryRegion.GetAddress() != reinterpret_cast<uintptr_t>(state.buffer.Data()) ||
                state.memoryRegion.GetSize() < state.buffer.Size()) {
                throw std::runtime_error("MR does not cover rail " + std::to_string(rail) + " allocation");
            }
        }
        if (mOptions.role == Role::Sender) {
            FillSenderPattern(0);
        }
    }

    void PrintListening() const
    {
        std::ostringstream output;
        output << "LISTENING role=receiver links=" << mOptions.links;
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            output << " rail" << rail << "=" << mOptions.endpoints[rail] << "/" << mOptions.rdmaIps[rail];
        }
        std::cout << output.str() << std::endl;
        std::cout.flush();
    }

    int OnNewChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept
    {
        ActiveCallbackGuard guard(mActiveCallbacks);
        if (mOptions.role != Role::Receiver || rail >= mOptions.links || channel == nullptr) {
            RecordFailure("unexpected new channel");
            return -1;
        }
        if (!ConfigureChannel(rail, channel)) {
            return -1;
        }
        RailState &state = mRails[rail];
        {
            std::lock_guard<std::mutex> lock(mChannelsMutex);
            if (state.channel != nullptr) {
                RecordFailure("receiver accepted more than one channel on rail " + std::to_string(rail));
                return -1;
            }
            state.channel = channel;
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
                RecordFailure("SetTwoSideThreshold failed on rail " + std::to_string(rail) + ": " +
                    std::to_string(rc));
                return false;
            }
            return true;
        } catch (const std::exception &error) {
            RecordFailure("ConfigureChannel failed on rail " + std::to_string(rail) + ": " + error.what());
            return false;
        }
    }

    void ConnectSender()
    {
        if (mOptions.links == 1) {
            ConnectRail(0);
            return;
        }
        const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::ConnectAndHandshake);
        ConnectRail(0);
        HandshakeRail(0);
        WaitSecondaryRailCommand(sequence, "rail 1 connect and HELLO handshake");
        mChannelCv.notify_all();
    }

    void Handshake()
    {
        if (mOptions.links == 2) {
            // B2 handshakes were completed by their fixed rail threads in
            // ConnectSender(). Keeping this call makes the B1/B2 outer flow
            // identical without letting rail 0 touch service 1.
            return;
        }
        HandshakeRail(0);
    }

    void ConnectRail(uint16_t rail)
    {
        UBSHcomConnectOptions options{};
        options.linkCount = 1;
        options.mode = UBSHcomClientPollingMode::WORKER_POLL;
        options.cbType = UBSHcomChannelCallBackType::CHANNEL_FUNC_CB;
        UBSHcomChannelPtr channel;
        RequireOk(mRails[rail].service->Connect("tcp://" + mOptions.endpoints[rail], channel, options),
            ("Connect rail " + std::to_string(rail)).c_str());
        if (channel == nullptr) {
            throw std::runtime_error("Connect succeeded without a channel on rail " + std::to_string(rail));
        }
        if (!ConfigureChannel(rail, channel)) {
            CheckFatal("ConfigureChannel after Connect");
            throw std::runtime_error("ConfigureChannel failed after Connect");
        }
        {
            std::lock_guard<std::mutex> lock(mChannelsMutex);
            mRails[rail].channel = channel;
        }
    }

    void ConnectAndHandshakeRail(uint16_t rail)
    {
        ConnectRail(rail);
        HandshakeRail(rail);
    }

    void HandshakeRail(uint16_t rail)
    {
        const uint64_t expectedBytes = static_cast<uint64_t>(mBlocksPerRail) * kStrideBytes;
        RailState &state = mRails[rail];
        const UBSHcomChannelPtr channel = ChannelCopy(rail);
        if (channel == nullptr) {
            throw std::runtime_error("HELLO without a connected channel on rail " + std::to_string(rail));
        }
        state.helloPayload = EncodeHello(mParams, rail);
        UBSHcomRequest request(state.helloPayload.data(), static_cast<uint32_t>(state.helloPayload.size()), kOpHello);
        UBSHcomResponse response(state.readyResponse.data(), static_cast<uint32_t>(state.readyResponse.size()));
        RequireOk(channel->Call(request, response, nullptr), ("HELLO Call rail " + std::to_string(rail)).c_str());
        ReadyInfo ready{};
        if (!DecodeReady(response.address, response.size, ready) || !SameParams(mParams, ready.params) ||
            ready.rail != rail || ready.destinationAddress == 0 || ready.destinationBytes != expectedBytes) {
            throw std::runtime_error("invalid READY response on rail " + std::to_string(rail));
        }
        state.peerDestinationAddress = static_cast<uintptr_t>(ready.destinationAddress);
        state.peerDestinationKey = ready.destinationKey;
    }

    void BuildPutRequests()
    {
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            RailState &state = mRails[rail];
            if (state.peerDestinationAddress == 0) {
                throw std::runtime_error("cannot prepare Put descriptors without READY on rail " +
                    std::to_string(rail));
            }
            for (uint32_t localBlock = 0; localBlock < mBlocksPerRail; ++localBlock) {
                UBSHcomOneSideRequest &request = state.putRequests[localBlock];
                request.lAddress = reinterpret_cast<uintptr_t>(state.buffer.Data()) +
                    static_cast<uintptr_t>(localBlock) * kStrideBytes;
                request.rAddress = state.peerDestinationAddress + static_cast<uintptr_t>(localBlock) * kStrideBytes;
                request.lKey = state.memoryKey;
                request.rKey = state.peerDestinationKey;
                request.size = kBlockBytes;
            }
        }
    }

    int OnIncoming(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        ActiveCallbackGuard guard(mActiveCallbacks);
        const UBSHcomChannelPtr expectedChannel = ChannelCopy(rail);
        if (expectedChannel == nullptr || context.Channel() != expectedChannel) {
            RecordFailure("incoming message arrived on an unexpected channel for rail " + std::to_string(rail));
            return -1;
        }
        if (context.Result() != 0) {
            RecordFailure("incoming hcom context failed on rail " + std::to_string(rail) + ": " +
                std::to_string(context.Result()));
            return context.Result();
        }
        switch (context.OpCode()) {
            case kOpHello:
                return OnHello(rail, context);
            case kOpRoundReady:
                return OnRoundReady(rail, context);
            case kOpRoundAck:
                return OnRoundAck(rail, context);
            case kOpFinish:
                return OnFinish(rail, context);
            case kOpFinishAck:
                return OnFinishAck(rail, context);
            default:
                RecordFailure("unexpected hcom opcode " + std::to_string(context.OpCode()) + " on rail " +
                    std::to_string(rail));
                return -1;
        }
    }

    int OnHello(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Receiver || !mReceiverReady.load(std::memory_order_acquire)) {
            RecordFailure("HELLO received before receiver initialization");
            return -1;
        }
        CaseParameters peer{};
        uint16_t wireRail = kMaxLinks;
        if (!DecodeHello(context.MessageData(), context.MessageDataLen(), peer, wireRail) ||
            !SameParams(peer, mParams) || wireRail != rail) {
            RecordFailure("HELLO parameters/rail do not match local configuration on rail " + std::to_string(rail));
            return -1;
        }
        bool expected = false;
        if (!mRails[rail].helloSeen.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            RecordFailure("duplicate HELLO on rail " + std::to_string(rail));
            return -1;
        }
        RailState &state = mRails[rail];
        ReadyInfo ready{};
        ready.params = mParams;
        ready.rail = rail;
        ready.destinationAddress = reinterpret_cast<uintptr_t>(state.buffer.Data());
        ready.destinationBytes = state.buffer.Size();
        ready.destinationKey = state.memoryKey;
        state.readyPayload = EncodeReady(ready);
        Callback *callback = NewSendCallback(rail);
        if (callback == nullptr) {
            RecordFailure("unable to allocate READY callback");
            return -1;
        }
        state.callbackCounters.workerAttemptedSendCallbacks.fetch_add(1, std::memory_order_relaxed);
        const UBSHcomRequest reply(state.readyPayload.data(), static_cast<uint32_t>(state.readyPayload.size()), kOpReady);
        const UBSHcomReplyContext replyContext(context.RspCtx(), 0);
        const int rc = context.Channel()->Reply(replyContext, reply, callback);
        if (rc != 0) {
            RecordFailure("READY Reply failed on rail " + std::to_string(rail) + ": " + std::to_string(rc));
            return rc;
        }
        return 0;
    }

    bool DecodeAndValidateToken(uint16_t rail, UBSHcomServiceContext &context, uint32_t magic,
        const char *name, uint64_t &generation) noexcept
    {
        uint16_t wireRail = kMaxLinks;
        if (!DecodeToken(context.MessageData(), context.MessageDataLen(), magic, generation, wireRail) ||
            generation == 0 || wireRail != rail) {
            RecordFailure(std::string("invalid ") + name + " payload/rail on rail " + std::to_string(rail));
            return false;
        }
        return true;
    }

    int OnRoundReady(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Receiver) {
            RecordFailure("sender received ROUND_READY");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeAndValidateToken(rail, context, kRoundReadyMagic, "ROUND_READY", generation)) {
            return -1;
        }
        RailState &state = mRails[rail];
        const uint64_t previous = state.roundReadyGeneration.load(std::memory_order_relaxed);
        if (generation != previous + 1) {
            RecordFailure("ROUND_READY generation is not strictly increasing on rail " + std::to_string(rail));
            return -1;
        }
        size_t traceIndex = 0;
        if (TraceIndex(generation, traceIndex)) {
            PublishCallbackTrace(generation, mTrace[traceIndex].rReady[rail], "R_ready");
        }
        // This release publishes both DMA-complete data and R_ready to the app
        // thread. It relies on ordered WRITE then SEND on this rail's real QP.
        state.roundReadyGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnRoundAck(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Sender) {
            RecordFailure("receiver received ROUND_ACK");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeAndValidateToken(rail, context, kAckMagic, "ROUND_ACK", generation)) {
            return -1;
        }
        RailState &state = mRails[rail];
        const uint64_t previous = state.ackGeneration.load(std::memory_order_relaxed);
        if (generation != previous + 1) {
            RecordFailure("ROUND_ACK generation is not strictly increasing on rail " + std::to_string(rail));
            return -1;
        }
        size_t traceIndex = 0;
        if (TraceIndex(generation, traceIndex)) {
            PublishCallbackTrace(generation, mTrace[traceIndex].sAck[rail], "S_ack");
        }
        state.ackGeneration.store(generation, std::memory_order_release);
        return 0;
    }

    int OnFinish(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Receiver) {
            RecordFailure("sender received FINISH");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeAndValidateToken(rail, context, kFinishMagic, "FINISH", generation) ||
            generation != mParams.TotalRounds()) {
            RecordFailure("invalid FINISH generation on rail " + std::to_string(rail));
            return -1;
        }
        uint64_t expected = 0;
        if (!mRails[rail].finishGeneration.compare_exchange_strong(
                expected, generation, std::memory_order_release, std::memory_order_relaxed)) {
            RecordFailure("duplicate FINISH on rail " + std::to_string(rail));
            return -1;
        }
        return 0;
    }

    int OnFinishAck(uint16_t rail, UBSHcomServiceContext &context) noexcept
    {
        if (mOptions.role != Role::Sender) {
            RecordFailure("receiver received FINISH_ACK");
            return -1;
        }
        uint64_t generation = 0;
        if (!DecodeAndValidateToken(rail, context, kFinishAckMagic, "FINISH_ACK", generation) ||
            generation != mParams.TotalRounds()) {
            RecordFailure("invalid FINISH_ACK generation on rail " + std::to_string(rail));
            return -1;
        }
        uint64_t expected = 0;
        if (!mRails[rail].finishAckGeneration.compare_exchange_strong(
                expected, generation, std::memory_order_release, std::memory_order_relaxed)) {
            RecordFailure("duplicate FINISH_ACK on rail " + std::to_string(rail));
            return -1;
        }
        return 0;
    }

    Callback *NewDataCallback(uint16_t rail, uint64_t generation, bool lastDataApi)
    {
        return UBSHcomNewCallback(
            [this, rail, generation, lastDataApi](UBSHcomServiceContext &context) {
                ActiveCallbackGuard guard(mActiveCallbacks);
                if (context.Result() != 0) {
                    RecordFailure("Put callback failed on rail " + std::to_string(rail) + ": " +
                        std::to_string(context.Result()));
                    return;
                }
                if (lastDataApi) {
                    size_t traceIndex = 0;
                    if (TraceIndex(generation, traceIndex)) {
                        PublishCallbackTrace(generation, mTrace[traceIndex].sData[rail], "S_data");
                    }
                }
                mRails[rail].callbackCounters.dataDoneCallbacks.fetch_add(1, std::memory_order_release);
            },
            std::placeholders::_1);
    }

    Callback *NewSendCallback(uint16_t rail)
    {
        return UBSHcomNewCallback(
            [this, rail](UBSHcomServiceContext &context) {
                ActiveCallbackGuard guard(mActiveCallbacks);
                if (context.Result() != 0) {
                    RecordFailure("Send/Reply callback failed on rail " + std::to_string(rail) + ": " +
                        std::to_string(context.Result()));
                    return;
                }
                mRails[rail].callbackCounters.sendDoneCallbacks.fetch_add(1, std::memory_order_release);
            },
            std::placeholders::_1);
    }

    void RunSender()
    {
        uint64_t generation = 1;
        for (uint32_t round = 0; round < mParams.verifyRounds; ++round, ++generation) {
            FillSenderPattern(generation);
            RunSenderRound(generation, false);
        }
        FillSenderPattern(0);
        for (uint32_t round = 0; round < mParams.warmupRounds; ++round, ++generation) {
            RunSenderRound(generation, false);
        }
        if (mParams.measureRounds != 0) {
            mSubmitNs.reserve(mParams.measureRounds);
            mE2eNs.reserve(mParams.measureRounds);
            mMeasureWallStartNs = NowNs();
            for (uint32_t round = 0; round < mParams.measureRounds; ++round, ++generation) {
                RunSenderRound(generation, true);
            }
            mMeasureWallEndNs = NowNs();
        }
        for (uint32_t round = 0; round < mParams.traceRounds; ++round, ++generation) {
            RunSenderRound(generation, false);
        }
        SendFinish();
    }

    void RunSenderRound(uint64_t generation, bool measure)
    {
        std::array<uint64_t, kMaxLinks> expectedData{};
        std::array<uint64_t, kMaxLinks> expectedSend{};
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            if (ChannelCopy(rail) == nullptr) {
                throw std::runtime_error("sender lost channel before round on rail " + std::to_string(rail));
            }
            expectedData[rail] = mRails[rail].appCounters.attemptedDataCallbacks + mBlocksPerRail;
            expectedSend[rail] = ExpectedSendCallbacks(rail) + 1;
        }

        const uint64_t startNs = NowNs();
        size_t traceIndex = 0;
        if (TraceIndex(generation, traceIndex)) {
            mTrace[traceIndex].s0.Publish(startNs);
        }

        uint64_t secondarySequence = 0;
        if (mOptions.links == 2) {
            secondarySequence = IssueSecondaryRailCommand(RailCommand::SubmitRound, generation);
        }
        const uint64_t rail0SubmitEndNs = SubmitRailRound(0, generation);
        uint64_t submitNs = rail0SubmitEndNs;
        if (mOptions.links == 2) {
            WaitSecondaryRailCommand(secondarySequence, "rail 1 round submission");
            submitNs = std::max(submitNs, mSecondary.submitEndNs.load(std::memory_order_relaxed));
        }
        if (TraceIndex(generation, traceIndex)) {
            mTrace[traceIndex].s1.Publish(submitNs);
        }

        WaitData("round completion", [this, generation, &expectedData, &expectedSend] {
            for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                if (mRails[rail].ackGeneration.load(std::memory_order_acquire) < generation ||
                    mRails[rail].callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) <
                        expectedData[rail] ||
                    mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) <
                        expectedSend[rail]) {
                    return false;
                }
            }
            return true;
        });
        const uint64_t endNs = NowNs();
        if (TraceIndex(generation, traceIndex)) {
            mTrace[traceIndex].s2.Publish(endNs);
        }
        if (measure) {
            mSubmitNs.push_back(submitNs - startNs);
            mE2eNs.push_back(endNs - startNs);
        }
    }

    uint64_t SubmitRailRound(uint16_t rail, uint64_t generation)
    {
        const UBSHcomChannelPtr channel = ChannelCopyRequired(rail, "round submission");
        RailState &state = mRails[rail];
        for (uint32_t localBlock = 0; localBlock < mBlocksPerRail; ++localBlock) {
            Callback *callback = NewDataCallback(rail, generation, localBlock + 1 == mBlocksPerRail);
            if (callback == nullptr) {
                throw std::runtime_error("unable to allocate Put callback on rail " + std::to_string(rail));
            }
            ++state.appCounters.attemptedDataCallbacks;
            const int rc = channel->Put(state.putRequests[localBlock], callback);
            if (rc != 0) {
                throw std::runtime_error("Put failed on rail " + std::to_string(rail) + ": " +
                    std::to_string(rc));
            }
        }

        state.roundReadyPayload = EncodeToken(kRoundReadyMagic, generation, rail);
        PostAsyncSend(rail, channel, state.roundReadyPayload.data(), state.roundReadyPayload.size(), kOpRoundReady);
        const uint64_t submitEndNs = NowNs();
        size_t traceIndex = 0;
        if (TraceIndex(generation, traceIndex)) {
            mTrace[traceIndex].sPost[rail].Publish(submitEndNs);
        }
        return submitEndNs;
    }

    void RunReceiver()
    {
        uint64_t secondarySequence = 0;
        if (mOptions.links == 2) {
            secondarySequence = IssueSecondaryRailCommand(RailCommand::RunReceiver);
        }
        RunReceiverRail(0);
        if (mOptions.links == 2) {
            WaitSecondaryRailCommand(secondarySequence, "rail 1 receiver loop and FINISH_ACK");
        }
    }

    void RunReceiverRail(uint16_t rail)
    {
        RailState &state = mRails[rail];
        for (uint64_t generation = 1; generation <= mParams.TotalRounds(); ++generation) {
            WaitData("next per-rail ROUND_READY", [this, rail, generation] {
                return mRails[rail].roundReadyGeneration.load(std::memory_order_acquire) >= generation;
            });
            if (generation <= mParams.verifyRounds) {
                VerifyReceiverRail(rail, generation);
            }
            size_t traceIndex = 0;
            if (TraceIndex(generation, traceIndex)) {
                mTrace[traceIndex].rAck[rail].Publish(NowNs());
            }
            state.ackPayload = EncodeToken(kAckMagic, generation, rail);
            PostAsyncSend(rail, ChannelCopyRequired(rail, "ROUND_ACK"), state.ackPayload.data(),
                state.ackPayload.size(), kOpRoundAck);
            const uint64_t sendTarget = ExpectedSendCallbacks(rail);
            WaitData("per-rail ROUND_ACK local completion", [this, rail, sendTarget] {
                return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= sendTarget;
            });
        }
        if (mParams.measureRounds != 0 || mParams.traceRounds != 0) {
            VerifyReceiverRail(rail, 0);
        }
        WaitControl("per-rail FINISH", [this, rail] {
            return mRails[rail].finishGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
        });
        SendFinishAckRail(rail);
    }

    void VerifyReceiverRail(uint16_t rail, uint64_t generation)
    {
        for (uint32_t localBlock = 0; localBlock < mBlocksPerRail; ++localBlock) {
            const uint32_t globalBlock = rail * mBlocksPerRail + localBlock;
            const uint8_t *destination = mRails[rail].buffer.Data() +
                static_cast<size_t>(localBlock) * kStrideBytes;
            std::string error;
            if (!VerifyBlock(destination, generation, globalBlock, error) || !VerifyGap(destination, error)) {
                throw std::runtime_error("rail " + std::to_string(rail) + ": " + error);
            }
        }
    }

    void SendFinish()
    {
        uint64_t secondarySequence = 0;
        if (mOptions.links == 2) {
            secondarySequence = IssueSecondaryRailCommand(RailCommand::SendFinish);
        }
        SendFinishRail(0);
        if (mOptions.links == 2) {
            WaitSecondaryRailCommand(secondarySequence, "rail 1 FINISH/FINISH_ACK drain");
        }
    }

    void SendFinishRail(uint16_t rail)
    {
        RailState &state = mRails[rail];
        state.finishPayload = EncodeToken(kFinishMagic, mParams.TotalRounds(), rail);
        PostAsyncSend(rail, ChannelCopyRequired(rail, "FINISH"), state.finishPayload.data(),
            state.finishPayload.size(), kOpFinish);
        const uint64_t target = ExpectedSendCallbacks(rail);
        WaitControl("per-rail FINISH local completion and FINISH_ACK", [this, rail, target] {
            return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target &&
                mRails[rail].finishAckGeneration.load(std::memory_order_acquire) == mParams.TotalRounds();
        });
    }

    void SendFinishAckRail(uint16_t rail)
    {
        RailState &state = mRails[rail];
        state.finishAckPayload = EncodeToken(kFinishAckMagic, mParams.TotalRounds(), rail);
        PostAsyncSend(rail, ChannelCopyRequired(rail, "FINISH_ACK"), state.finishAckPayload.data(),
            state.finishAckPayload.size(), kOpFinishAck);
        const uint64_t target = ExpectedSendCallbacks(rail);
        WaitControl("per-rail FINISH_ACK local completion", [this, rail, target] {
            return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
    }

    void PostAsyncSend(uint16_t rail, const UBSHcomChannelPtr &channel, uint8_t *data, size_t size, uint16_t opcode)
    {
        if (channel == nullptr) {
            throw std::runtime_error("null channel for Send on rail " + std::to_string(rail));
        }
        Callback *callback = NewSendCallback(rail);
        if (callback == nullptr) {
            throw std::runtime_error("unable to allocate Send callback");
        }
        ++mRails[rail].appCounters.attemptedSendCallbacks;
        const UBSHcomRequest request(data, static_cast<uint32_t>(size), opcode);
        const int rc = channel->Send(request, callback);
        if (rc != 0) {
            throw std::runtime_error("Send opcode " + std::to_string(opcode) + " failed on rail " +
                std::to_string(rail) + ": " + std::to_string(rc));
        }
    }

    uint64_t ExpectedSendCallbacks(uint16_t rail) const noexcept
    {
        const RailState &state = mRails[rail];
        return state.appCounters.attemptedSendCallbacks +
            state.callbackCounters.workerAttemptedSendCallbacks.load(std::memory_order_acquire);
    }

    void FillSenderPattern(uint64_t generation)
    {
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            for (uint32_t localBlock = 0; localBlock < mBlocksPerRail; ++localBlock) {
                const uint32_t globalBlock = rail * mBlocksPerRail + localBlock;
                FillBlock(mRails[rail].buffer.Data() + static_cast<size_t>(localBlock) * kStrideBytes,
                    generation, globalBlock);
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

    void WaitForChannels()
    {
        std::unique_lock<std::mutex> lock(mChannelsMutex);
        const bool received = mChannelCv.wait_for(lock, std::chrono::seconds(mOptions.timeoutSec), [this] {
            if (mFatal.load(std::memory_order_acquire)) {
                return true;
            }
            for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                if (mRails[rail].channel == nullptr) {
                    return false;
                }
            }
            return true;
        });
        if (!received) {
            throw std::runtime_error("timed out waiting for all peer channels");
        }
        CheckFatal("peer channels");
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            if (mRails[rail].channel == nullptr) {
                throw std::runtime_error("peer channel was not established on rail " + std::to_string(rail));
            }
        }
    }

    UBSHcomChannelPtr ChannelCopy(uint16_t rail) const
    {
        std::lock_guard<std::mutex> lock(mChannelsMutex);
        return mRails[rail].channel;
    }

    UBSHcomChannelPtr ChannelCopyRequired(uint16_t rail, const char *operation) const
    {
        UBSHcomChannelPtr channel = ChannelCopy(rail);
        if (channel == nullptr) {
            throw std::runtime_error(std::string(operation) + " without a channel on rail " + std::to_string(rail));
        }
        return channel;
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
            if (CallbacksDrained()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return CallbacksDrained();
    }

    bool CallbacksDrained() const noexcept
    {
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            const RailState &state = mRails[rail];
            if (state.callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) <
                    state.appCounters.attemptedDataCallbacks ||
                state.callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) <
                    ExpectedSendCallbacks(rail)) {
                return false;
            }
        }
        return mActiveCallbacks.load(std::memory_order_acquire) == 0;
    }

    void Teardown() noexcept
    {
        if (mTearingDown.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            RailState &state = mRails[rail];
            UBSHcomChannelPtr channel;
            {
                std::lock_guard<std::mutex> lock(mChannelsMutex);
                channel = state.channel;
                state.channel.Set(nullptr);
            }
            if (state.service != nullptr && channel != nullptr) {
                state.service->Disconnect(channel);
            }
            if (state.service != nullptr && state.memoryRegistered) {
                state.service->DestroyMemoryRegion(state.memoryRegion);
                state.memoryRegistered = false;
            }
            if (state.service != nullptr) {
                UBSHcomService::Destroy(state.serviceName);
                state.service = nullptr;
            }
        }
    }

    void EmitTracePoint(const char *event, uint64_t generation, int rail, const TracePoint &point) const
    {
        std::cout << "{\"record_type\":\"trace\",\"trace_schema\":\"rdma600-stage2-v1\",\"host_role\":\""
                  << RoleName(mOptions.role) << "\",\"case\":\"" << CaseName(mOptions.links)
                  << "\",\"generation\":" << generation << ",\"rail\":";
        if (rail < 0) {
            std::cout << "null";
        } else {
            std::cout << rail;
        }
        std::cout << ",\"event\":\"" << event << "\",\"timestamp_ns\":" << point.Read(event) << "}"
                  << std::endl;
    }

    void EmitTrace() const
    {
        for (uint32_t index = 0; index < mParams.traceRounds; ++index) {
            const TraceRound &trace = mTrace[index];
            if (mOptions.role == Role::Sender) {
                EmitTracePoint("S0", trace.generation, -1, trace.s0);
                for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                    EmitTracePoint("S_post", trace.generation, rail, trace.sPost[rail]);
                    EmitTracePoint("S_data", trace.generation, rail, trace.sData[rail]);
                    EmitTracePoint("S_ack", trace.generation, rail, trace.sAck[rail]);
                }
                EmitTracePoint("S1", trace.generation, -1, trace.s1);
                EmitTracePoint("S2", trace.generation, -1, trace.s2);
            } else {
                for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                    EmitTracePoint("R_ready", trace.generation, rail, trace.rReady[rail]);
                    EmitTracePoint("R_ack", trace.generation, rail, trace.rAck[rail]);
                }
            }
        }
    }

    void PrintSenderResult() const
    {
        std::ostringstream output;
        output << std::fixed << std::setprecision(3);
        output << "{\"case\":\"" << CaseName(mOptions.links) << "\",\"status\":\"ok\",\"commit\":\""
               << RDMA_600_GIT_COMMIT << "\",\"role\":\"sender\",\"kind\":\"" << KindName(mOptions.kind)
               << "\",\"optimization\":\"stage1.5-AB\",\"data_wait\":\"busy-poll-relax\""
               << ",\"deadline_check_interval\":" << kDataDeadlineCheckInterval
               << ",\"counter_alignment_bytes\":" << kCounterAlignment
               << ",\"callback_allocation\":\"per-request\""
               << ",\"links\":" << mOptions.links
               << ",\"services\":" << mOptions.links << ",\"application_cpus\":[";
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
            if (rail != 0) {
                output << ',';
            }
            output << mOptions.appCpus[rail];
        }
        output << "],\"blocks\":600,\"blocks_per_rail\":" << mBlocksPerRail
               << ",\"block_bytes\":1024,\"payload_bytes\":614400"
               << ",\"mode\":\"plain\",\"remote_layout\":\"direct-stride-4096\",\"tls_enabled\":false"
               << ",\"internal_multirail\":false,\"channel_link_count\":1,\"rounds_in_flight\":1"
               << ",\"application_submit_threads\":" << mOptions.links
               << ",\"rail_thread_affinity\":\"fixed-one-thread-per-service\""
               << ",\"multi_service_scope\":\""
               << (mOptions.links == 1 ? "single-service-supported" : "diagnostic-unsupported-by-hcom-contract")
               << "\""
               << ",\"data_wr_per_round\":600,\"round_ready_wr_per_round\":"
               << mOptions.links << ",\"ack_wr_per_round\":" << mOptions.links
               << ",\"direct_optimization\":\"stage1.5-AB\",\"verify_passed\":true"
               << ",\"trace_rounds\":" << mParams.traceRounds;
        if (mOptions.kind != RunKind::Measure) {
            output << ",\"measure_rounds\":0,\"submit_avg_us\":null,\"submit_p50_us\":null"
                   << ",\"submit_p95_us\":null,\"submit_p99_us\":null,\"e2e_avg_us\":null"
                   << ",\"e2e_p50_us\":null,\"e2e_p95_us\":null,\"e2e_p99_us\":null"
                   << ",\"effective_GBps\":null,\"block_Mops\":null";
        } else {
            const uint64_t wallNs = mMeasureWallEndNs - mMeasureWallStartNs;
            const double wallSeconds = static_cast<double>(wallNs) / 1000000000.0;
            const double effectiveGbps = static_cast<double>(mParams.measureRounds) * kPayloadBytes / wallSeconds / 1e9;
            const double blockMops = static_cast<double>(mParams.measureRounds) * kBlocks / wallSeconds / 1e6;
            output << ",\"measure_rounds\":" << mParams.measureRounds
                   << ",\"submit_avg_us\":" << AverageNs(mSubmitNs) / 1000.0
                   << ",\"submit_p50_us\":" << NsToUs(PercentileNs(mSubmitNs, 0.50))
                   << ",\"submit_p95_us\":" << NsToUs(PercentileNs(mSubmitNs, 0.95))
                   << ",\"submit_p99_us\":" << NsToUs(PercentileNs(mSubmitNs, 0.99))
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
    uint32_t mBlocksPerRail = kBlocks;
    std::array<RailState, kMaxLinks> mRails{};
    std::atomic<bool> mReceiverReady{false};
    std::atomic<bool> mTearingDown{false};
    std::atomic<bool> mFatal{false};
    mutable std::mutex mErrorMutex;
    std::string mError;
    mutable std::mutex mChannelsMutex;
    std::condition_variable mChannelCv;
    alignas(kCounterAlignment) std::atomic<uint64_t> mActiveCallbacks{0};
    std::array<TraceRound, kMaxTraceRounds> mTrace{};
    std::vector<uint64_t> mSubmitNs;
    std::vector<uint64_t> mE2eNs;
    uint64_t mMeasureWallStartNs = 0;
    uint64_t mMeasureWallEndNs = 0;
    SecondaryRailExecutor mSecondary;
};

}  // namespace

int main(int argc, char **argv)
{
    try {
        const Options options = ParseOptions(argc, argv);
        if (options.selfTest) {
            return RunSelfTest() ? 0 : 1;
        }
        DirectBenchmark benchmark(options);
        return benchmark.Run();
    } catch (const std::exception &error) {
        std::cerr << "ERROR: " << error.what() << std::endl;
        return 1;
    }
}
