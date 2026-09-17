// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include "common.h"

namespace rdma_bench {

inline uint16_t RailForRequestIndex(uint32_t index, const CaseParameters &params)
{
    return static_cast<uint16_t>(index / params.RailCapacity());
}

void MakeCopyEntries(uint64_t seed, const CaseParameters &params, std::vector<CopyEntry> &entries);

bool ValidateCopyEntries(const std::vector<CopyEntry> &entries, const CaseParameters &params,
    uint64_t sourceBytesPerRail, uint64_t destinationBytesPerRail, std::string &error);

inline uint64_t PatternWord(uint64_t generation, uint32_t globalBlock, uint32_t wordIndex)
{
    return 0x9e3779b97f4a7c15ULL ^ (generation * 0x100000001b3ULL) ^
        (static_cast<uint64_t>(globalBlock) << 32U) ^ wordIndex;
}

void FillBlock(uint8_t *address, uint64_t generation, uint32_t globalBlock, uint32_t blockBytes);

bool VerifyBlock(const uint8_t *address, uint64_t generation, uint32_t globalBlock, uint32_t blockBytes, uint64_t bodySeed, std::string &error);

bool VerifyGap(const uint8_t *address, uint32_t blockBytes, std::string &error);

void ScatterChunkPayload(uint16_t rail, uint32_t chunk, uint32_t blocksPerRail, uint32_t railBlocks, uint16_t sglItems,
    const std::vector<CopyEntry> &entries, uint32_t blockBytes, void *destinationBase, size_t destinationBytes,
    const void *stageBase, size_t stageBytes);

template <typename Ready, typename Scatter, typename Checkpoint>
bool ScanReadySglChunks(uint16_t links, uint32_t chunksPerRail, SglSchedulerState &state,
    Ready ready, Scatter scatter, Checkpoint checkpoint)
{
    const uint32_t totalChunks = chunksPerRail * links;
    if (totalChunks == 0 || state.cursor >= totalChunks || state.scattered > totalChunks)
        throw std::logic_error("invalid SGL scheduler state");
    const uint32_t scanStart = state.cursor;
    bool progress = false;
    for (uint32_t examined = 0; examined < totalChunks; ++examined) {
        const uint32_t linear = (scanStart + examined) % totalChunks;
        const uint16_t rail = static_cast<uint16_t>(linear % links);
        const uint32_t chunk = linear / links;
        if (ready(rail, chunk)) {
            scatter(rail, chunk);
            ++state.scattered;
            progress = true;
        }
        if (++state.examinedSinceCheckpoint == kDataDeadlineCheckInterval) {
            state.examinedSinceCheckpoint = 0;
            checkpoint();
        }
    }
    // Rotate only after the fixed-start scan has visited every slot exactly once.
    state.cursor = (scanStart + 1) % totalChunks;
    return progress;
}

inline bool SglCompletionReached(uint32_t scattered, uint32_t totalChunks, bool requestCallbackDone) noexcept
{
    return scattered == totalChunks && requestCallbackDone;
}

template <typename Ready>
bool AllSglChunksReady(uint16_t links, uint32_t chunksPerRail, Ready ready)
{
    for (uint16_t rail = 0; rail < links; ++rail)
        for (uint32_t chunk = 0; chunk < chunksPerRail; ++chunk)
            if (!ready(rail, chunk)) return false;
    return true;
}

}  // namespace rdma_bench
