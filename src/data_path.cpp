// SPDX-License-Identifier: MulanPSL-2.0
#include "data_path.h"
#include "protocol.h"

namespace rdma_bench {

bool SequentialSglSourceOrder()
{
    static const bool enabled = [] {
        const char *value = std::getenv("RDMA_600_SOURCE_SEQUENTIAL");
        if (value == nullptr || std::strcmp(value, "0") == 0) return false;
        if (std::strcmp(value, "1") == 0) return true;
        throw std::runtime_error("RDMA_600_SOURCE_SEQUENTIAL must be 0 or 1");
    }();
    return enabled;
}

void MakeCopyEntries(uint64_t seed, const CaseParameters &params, std::vector<CopyEntry> &entries)
{
    const bool sequentialSource = params.mode == kModeSgl && SequentialSglSourceOrder();
    for (uint32_t index = 0; index < params.blocks; ++index) {
        const uint16_t rail = RailForRequestIndex(index, params);
        const uint32_t count = params.RailBlocks(rail);
        const uint32_t localIndex = index - rail * params.RailCapacity();
        const uint32_t shift = static_cast<uint32_t>(seed % count);
        const uint32_t sourceSlot = sequentialSource ? localIndex : (localIndex * 7U + shift * 13U) % count;
        // A reverse rotation is a permutation for EVERY count, including multiples of 11.
        const uint32_t destinationSlot = (count - 1 - localIndex + shift) % count;
        entries[index] = {static_cast<uint64_t>(sourceSlot) * kStrideBytes,
            static_cast<uint64_t>(destinationSlot) * kStrideBytes};
    }
}

bool ValidateCopyEntries(const std::vector<CopyEntry> &entries, const CaseParameters &params,
    uint64_t sourceBytesPerRail, uint64_t destinationBytesPerRail, std::string &error)
{
    if (entries.size() != params.blocks) { error = "entry count mismatch"; return false; }
    // Keep validation inside each request, with one bit per destination slot.
    constexpr size_t wordsPerRail = (kMaxBlocksPerRail + 63U) / 64U;
    std::array<std::array<uint64_t, wordsPerRail>, kMaxLinks> destinationsSeen{};
    for (uint32_t index = 0; index < params.blocks; ++index) {
        const uint16_t rail = RailForRequestIndex(index, params);
        const CopyEntry &entry = entries[index];
        const uint64_t activeBytes = static_cast<uint64_t>(params.RailBlocks(rail)) * kStrideBytes;
        if (entry.remoteSourceOffset % kStrideBytes != 0 || sourceBytesPerRail < params.blockBytes ||
            entry.remoteSourceOffset > sourceBytesPerRail - params.blockBytes ||
            entry.remoteSourceOffset >= activeBytes) {
            error = "source offset unaligned/out of range at request " + std::to_string(index); return false;
        }
        if (entry.localDestinationOffset % kStrideBytes != 0 || destinationBytesPerRail < params.blockBytes ||
            entry.localDestinationOffset > destinationBytesPerRail - params.blockBytes ||
            entry.localDestinationOffset >= activeBytes) {
            error = "destination offset unaligned/out of range at request " + std::to_string(index); return false;
        }
        const size_t slot = static_cast<size_t>(entry.localDestinationOffset / kStrideBytes);
        uint64_t &word = destinationsSeen[rail][slot / 64U];
        const uint64_t mask = uint64_t{1} << (slot % 64U);
        if ((word & mask) != 0) { error = "duplicate destination slot"; return false; }
        word |= mask;
    }
    return true;
}

void FillBlock(uint8_t *address, uint64_t generation, uint32_t globalBlock, uint32_t blockBytes)
{
    auto *words = reinterpret_cast<uint64_t *>(address);
    for (uint32_t word = 0; word < blockBytes / sizeof(uint64_t); ++word) {
        words[word] = PatternWord(generation, globalBlock, word);
    }
}

bool VerifyBlock(const uint8_t *address, uint64_t generation, uint32_t globalBlock, uint32_t blockBytes, uint64_t bodySeed, std::string &error)
{
    const auto *words = reinterpret_cast<const uint64_t *>(address);
    for (uint32_t word = 0; word < blockBytes / sizeof(uint64_t); ++word) {
        const uint64_t expected = PatternWord((word == 0 || word + 1 == blockBytes / 8) ? generation : bodySeed, globalBlock, word);
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

bool VerifyGap(const uint8_t *address, uint32_t blockBytes, std::string &error)
{
    for (uint32_t offset = blockBytes; offset < kStrideBytes; ++offset) {
        if (address[offset] != kDstGapSentinel) {
            error = "destination stride gap was modified at offset " + std::to_string(offset);
            return false;
        }
    }
    return true;
}

void ScatterChunkPayload(uint16_t rail, uint32_t chunk, uint32_t blocksPerRail, uint32_t railBlocks, uint16_t sglItems,
    const std::vector<CopyEntry> &entries, uint32_t blockBytes, void *destinationBase, size_t destinationBytes,
    const void *stageBase, size_t stageBytes)
{
    const uint32_t first = chunk * sglItems;
    const uint32_t count = ChunkItemCount(railBlocks, sglItems, chunk);
    const uint32_t globalFirst = static_cast<uint32_t>(rail) * blocksPerRail;
    for (uint32_t item = 0; item < count; ++item) {
        const CopyEntry &entry = entries[globalFirst + first + item];
        uint64_t destination = 0;
        uint64_t stage = 0;
        if (!CheckedAddAddress(reinterpret_cast<uintptr_t>(destinationBase), entry.localDestinationOffset,
                blockBytes, destinationBytes, destination) ||
            !CheckedAddAddress(reinterpret_cast<uintptr_t>(stageBase),
                static_cast<uint64_t>(first + item) * blockBytes, blockBytes, stageBytes, stage))
            throw std::runtime_error("scatter address overflow/out of range rail=" + std::to_string(rail));
        std::memcpy(reinterpret_cast<void *>(static_cast<uintptr_t>(destination)),
            reinterpret_cast<const void *>(static_cast<uintptr_t>(stage)), blockBytes);
    }
}

}  // namespace rdma_bench
