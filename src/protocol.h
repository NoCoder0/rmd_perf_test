// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include "common.h"

namespace rdma_bench {

inline void PutU16(uint8_t *&cursor, uint16_t value)
{
    *cursor++ = static_cast<uint8_t>(value >> 8U);
    *cursor++ = static_cast<uint8_t>(value);
}

inline void PutU32(uint8_t *&cursor, uint32_t value)
{
    for (int shift = 24; shift >= 0; shift -= 8) {
        *cursor++ = static_cast<uint8_t>(value >> shift);
    }
}

inline void PutU64(uint8_t *&cursor, uint64_t value)
{
    for (int shift = 56; shift >= 0; shift -= 8) {
        *cursor++ = static_cast<uint8_t>(value >> shift);
    }
}

inline uint16_t GetU16(const uint8_t *&cursor)
{
    const uint16_t value = static_cast<uint16_t>(cursor[0]) << 8U | cursor[1];
    cursor += 2;
    return value;
}

inline uint32_t GetU32(const uint8_t *&cursor)
{
    uint32_t value = 0;
    for (int index = 0; index < 4; ++index) {
        value = (value << 8U) | cursor[index];
    }
    cursor += 4;
    return value;
}

inline uint64_t GetU64(const uint8_t *&cursor)
{
    uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | cursor[index];
    }
    cursor += 8;
    return value;
}

void EncodeParams(uint8_t *&cursor, const CaseParameters &params);

CaseParameters DecodeParams(const uint8_t *&cursor);

bool SameParams(const CaseParameters &left, const CaseParameters &right);

bool IsZeroMemoryKey(const UBSHcomMemoryKey &key);

inline bool CheckedAddAddress(uint64_t base, uint64_t offset, uint64_t bytes, uint64_t regionBytes, uint64_t &address)
{
    if (bytes > regionBytes || offset > regionBytes - bytes || base > std::numeric_limits<uint64_t>::max() - offset) {
        return false;
    }
    address = base + offset;
    return address <= std::numeric_limits<uint64_t>::max() - bytes;
}

inline uint32_t ChunkCount(uint32_t blocksPerRail, uint16_t sglItems)
{
    return (blocksPerRail + sglItems - 1U) / sglItems;
}

inline uint32_t ChunkItemCount(uint32_t blocksPerRail, uint16_t sglItems, uint32_t chunkId)
{
    const uint64_t first = static_cast<uint64_t>(chunkId) * sglItems;
    return first >= blocksPerRail ? 0 : std::min<uint32_t>(sglItems, blocksPerRail - static_cast<uint32_t>(first));
}

inline uint32_t NotificationCount(uint32_t chunks, uint32_t notifyEveryWrs)
{
    return notifyEveryWrs == 0 ? 0 : (chunks + notifyEveryWrs - 1U) / notifyEveryWrs;
}

inline uint32_t NotificationFirstChunk(uint32_t chunk, uint32_t notifyEveryWrs)
{
    return (chunk / notifyEveryWrs) * notifyEveryWrs;
}

inline bool EndsNotificationGroup(uint32_t chunk, uint32_t chunks, uint32_t notifyEveryWrs)
{
    return (chunk + 1U) % notifyEveryWrs == 0 || chunk + 1U == chunks;
}

ChunkDoneInfo MakeChunkDone(uint16_t rail, uint64_t generation, uint32_t firstChunk,
    uint32_t blocksPerRail, uint16_t sglItems, uint32_t blockBytes, uint32_t notifyEveryWrs);

void EncodeMemoryKey(uint8_t *&cursor, const UBSHcomMemoryKey &key);

UBSHcomMemoryKey DecodeMemoryKey(const uint8_t *&cursor);

std::array<uint8_t, kHelloWireBytes> EncodeHello(const HelloInfo &info);

bool DecodeHello(const void *data, uint32_t size, HelloInfo &info);

std::array<uint8_t, kReadyWireBytes> EncodeReady(const ReadyInfo &info);

bool DecodeReady(const void *data, uint32_t size, ReadyInfo &info);

void EncodeCopyRequest(uint64_t generation, const CaseParameters &params,
    const std::vector<CopyEntry> &entries, uint8_t *payload);

bool DecodeCopyRequest(const void *data, uint32_t size, uint64_t expectedGeneration, const CaseParameters &params,
    uint64_t sourceBytesPerRail, uint64_t destinationBytesPerRail,
    std::vector<CopyEntry> &entries, std::string &error);

uint32_t EncodeRequestFragment(uint64_t generation, uint32_t total, uint32_t offset,
    const uint8_t *request, uint8_t *fragment);

bool AppendRequestFragment(const void *data, uint32_t size, uint64_t generation,
    const CaseParameters &params, uint8_t *request, uint32_t &received);

std::array<uint8_t, kChunkDoneWireBytes> EncodeChunkDone(const ChunkDoneInfo &info);

bool DecodeChunkDone(const void *data, uint32_t size, ChunkDoneInfo &info);

bool ValidateChunkDone(const ChunkDoneInfo &info, uint16_t expectedRail, uint64_t expectedGeneration,
    uint32_t blocksPerRail, uint16_t sglItems, uint32_t blockBytes, std::string &error,
    uint32_t notifyEveryWrs = 1);

template <typename Observer>
bool PublishChunkReadyAfterObserver(
    std::atomic<uint64_t> &slot, uint64_t generation, Observer observer, std::string &error)
{
    uint64_t previous = slot.load(std::memory_order_relaxed);
    if (generation == 0 || generation == kChunkReadyPublishing || previous == generation ||
        previous == kChunkReadyPublishing || !slot.compare_exchange_strong(previous, kChunkReadyPublishing,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        error = "duplicate/concurrent CHUNK_DONE";
        return false;
    }
    try {
        if (!observer()) {
            slot.store(previous, std::memory_order_release);
            error = "CHUNK_DONE observer failed before ready publication";
            return false;
        }
    } catch (...) {
        slot.store(previous, std::memory_order_release);
        throw;
    }
    slot.store(generation, std::memory_order_release);
    return true;
}

template <typename Observer>
bool PublishChunkReadyAfterOptionalObserver(std::atomic<uint64_t> &slot, uint64_t generation,
    bool observe, Observer observer, std::string &error)
{
    return PublishChunkReadyAfterObserver(slot, generation,
        [&] { return !observe || observer(); }, error);
}

std::array<uint8_t, kDataDoneWireBytes> EncodeDataDone(uint64_t generation, uint16_t rail,
    uint32_t blocksOnRail, uint32_t blockBytes);

bool DecodeDataDone(const void *data, uint32_t size, uint16_t expectedRail, uint32_t expectedBlocks,
    uint64_t &generation, uint32_t blockBytes);

std::array<uint8_t, kCopyErrorWireBytes> EncodeCopyError(
    uint64_t generation, uint32_t stage, uint32_t errorCode, uint32_t detail);

bool DecodeCopyError(const void *data, uint32_t size, uint64_t &generation,
    uint32_t &stage, uint32_t &errorCode, uint32_t &detail);

std::array<uint8_t, kTokenWireBytes> EncodeToken(
    uint32_t magic, uint16_t opcode, uint64_t generation, uint16_t rail);

bool DecodeToken(const void *data, uint32_t size, uint32_t magic, uint16_t opcode,
    uint64_t &generation, uint16_t &rail);

bool ValidateHelloMetadata(const HelloInfo &hello, const CaseParameters &parameters, uint16_t rail,
    uint64_t railBytes, uint64_t stageBytes);

}  // namespace rdma_bench
