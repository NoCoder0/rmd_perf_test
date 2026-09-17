// SPDX-License-Identifier: MulanPSL-2.0
#include "benchmark.h"

namespace rdma_bench {

int SparseCopyBenchmark::OnDataDone(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    if (mOptions.role != Role::Local) { RecordFailure("remote received DATA_DONE"); return -1; }
    uint64_t generation = 0;
    if (!DecodeDataDone(context.MessageData(), context.MessageDataLen(), rail, mParams.RailBlocks(rail), generation, mParams.blockBytes)) {
        RecordFailure("invalid DATA_DONE rail " + std::to_string(rail)); return -1;
    }
    RailState &state = mRails[rail];
    const uint64_t previous = state.dataDoneGeneration.load(std::memory_order_relaxed);
    if (generation != previous + 1 || generation != mExpectedGeneration.load(std::memory_order_acquire) ||
        mOptions.mode != CopyMode::Direct) {
        RecordFailure("non-increasing DATA_DONE rail " + std::to_string(rail)); return -1;
    }
    size_t i = 0;
    if (TraceIndex(generation, i))
        PublishCallbackTrace(generation, mTrace[i].localDataDone[rail], "local_data_done");
    state.dataDoneGeneration.store(generation, std::memory_order_release);
    return 0;
}

int SparseCopyBenchmark::OnChunkDone(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    if (mOptions.role != Role::Local || mOptions.mode != CopyMode::Sgl) {
        RecordFailure("CHUNK_DONE on wrong role/mode"); return -1;
    }
    ChunkDoneInfo info{};
    if (!DecodeChunkDone(context.MessageData(), context.MessageDataLen(), info)) {
        RecordFailure("invalid CHUNK_DONE wire on rail " + std::to_string(rail)); return -1;
    }
    const uint64_t expectedGeneration = mExpectedGeneration.load(std::memory_order_acquire);
    std::string error;
    if (!ValidateChunkDone(info, rail, expectedGeneration, mParams.RailBlocks(rail), mOptions.sglItems, mParams.blockBytes, error)) {
        RecordFailure(error + " rail=" + std::to_string(rail)); return -1;
    }
    std::atomic<uint64_t> &slot = mRails[rail].chunkReadyGeneration[info.chunkId];
    size_t trace = 0;
    const bool traceEnabled = TraceIndex(info.generation, trace);
    if (!PublishChunkReadyAfterOptionalObserver(slot, info.generation, traceEnabled,
        [this, &info, rail, trace] {
            return PublishCallbackTrace(info.generation,
                mTrace[trace].localChunkReady[rail][info.chunkId], "local_chunk_ready");
        }, error)) {
        RecordFailure(error + " rail=" + std::to_string(rail) +
            " chunk=" + std::to_string(info.chunkId));
        return -1;
    }
    return 0;
}

int SparseCopyBenchmark::OnCopyError(uint16_t rail, UBSHcomServiceContext &context) noexcept
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

void SparseCopyBenchmark::RunLocal()
{
    NegotiateMatrix();
    if (!DrainUntilComplete()) throw std::runtime_error("handshake callbacks did not drain");
    for (size_t index = 0; index < mCases.size(); ++index) {
        SelectCase(index);
        BeginCase();
        uint64_t generation = mCaseFirstGeneration;
        for (uint32_t i = 0; i < mParams.verifyRounds; ++i, ++generation) {
            SparseCopy(generation, false);
            VerifyLocalDestination(mCopyEntries, generation);
        }
        for (uint32_t i = 0; i < mParams.warmupRounds; ++i, ++generation) {
            SparseCopy(generation, false);
            VerifyLocalMarkers(generation);
        }
        if (mParams.measureRounds != 0) {
            mMeasureWallStartNs = NowNs();
            for (uint32_t i = 0; i < mParams.measureRounds; ++i, ++generation) {
                SparseCopy(generation, true);
                VerifyLocalMarkers(generation);
            }
            mMeasureWallEndNs = NowNs();
            if (mMeasureWallEndNs <= mMeasureWallStartNs || mSparseCopyNs.size() != mParams.measureRounds ||
                std::any_of(mSparseCopyNs.begin(), mSparseCopyNs.end(), [](uint64_t sample) { return sample == 0; }))
                throw std::runtime_error("invalid/non-positive measurement timing");
        }
        for (uint32_t i = 0; i < mParams.traceRounds; ++i, ++generation) {
            SparseCopy(generation, false);
            VerifyLocalMarkers(generation);
        }
        VerifyLocalDestination(mCopyEntries, generation - 1);
        EndCase();
        mResults.push_back(FormatLocalResult());
        mSummary.push_back({mParams, AverageNs(mSparseCopyNs) / 1000.0,
            NsToUs(PercentileNs(mSparseCopyNs, 0.50)), NsToUs(PercentileNs(mSparseCopyNs, 0.95)),
            NsToUs(PercentileNs(mSparseCopyNs, 0.99)),
            mParams.measureRounds ? mParams.PayloadBytes() / AverageNs(mSparseCopyNs) : 0.0,
            mParams.measureRounds ? static_cast<double>(mParams.measureRounds) * mParams.PayloadBytes() /
                (mMeasureWallEndNs - mMeasureWallStartNs) : 0.0});
    }
}

void SparseCopyBenchmark::SparseCopy(uint64_t generation, bool measure)
{
    const uint64_t start = NowNs();
    const uint64_t deadlineNs = DeadlineFrom(start);
    size_t trace = 0;
    if (TraceIndex(generation, trace)) mTrace[trace].localBegin.Publish(start);
    MakeCopyEntries(generation, mParams, mCopyEntries);
    const uint64_t railBytes = static_cast<uint64_t>(mMaxRailBlocks) * kStrideBytes;
    std::string error;
    if (!ValidateCopyEntries(mCopyEntries, mParams, railBytes, railBytes, error))
        throw std::runtime_error("local input validation: " + error);
    for (uint16_t rail = 0; rail < mOptions.links; ++rail)
        if (mRails[rail].peerSourceBytes != railBytes)
            throw std::runtime_error("invalid remote source metadata");
    EncodeCopyRequest(generation, mParams, mCopyEntries, mCopyReqPayload.data());
    if (NowNs() >= deadlineNs) throw std::runtime_error("deadline exceeded while preparing COPY_REQ");
    mExpectedGeneration.store(generation, std::memory_order_release);
    const uint64_t expectedSend = ExpectedSendCallbacks(0) + mParams.Fragments();
    const UBSHcomChannelPtr channel = ChannelCopyRequired(0, "COPY_REQ fragments");
    for (uint32_t part = 0, offset = 0; offset < mParams.RequestBytes(); ++part) {
        const uint32_t size = EncodeRequestFragment(generation, mParams.RequestBytes(), offset,
            mCopyReqPayload.data(), mRequestFragments[part].data());
        PostAsyncSend(0, channel, mRequestFragments[part].data(), size, kOpCopyReq);
        offset += size - kFragmentHeaderBytes;
    }

    if (TraceIndex(generation, trace)) mTrace[trace].localRequestPosted.Publish(NowNs());
    if (mOptions.mode == CopyMode::Direct) {
        WaitDataUntil("sparse_copy completion", deadlineNs, [this, generation, expectedSend] {
            if (mRails[0].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) < expectedSend)
                return false;
            for (uint16_t rail = 0; rail < mOptions.links; ++rail)
                if (mRails[rail].dataDoneGeneration.load(std::memory_order_acquire) < generation) return false;
            return true;
        });
    } else {
        WaitAndScatterSgl(generation, expectedSend, deadlineNs);
    }
    const uint64_t end = NowNs();
    if (end >= deadlineNs) throw std::runtime_error("sparse_copy absolute deadline exceeded");
    if (TraceIndex(generation, trace)) mTrace[trace].localEnd.Publish(end);
    if (measure) mSparseCopyNs.push_back(end - start);
}

void SparseCopyBenchmark::ScatterChunk(uint16_t rail, uint32_t chunk, uint64_t generation)
{
    RailState &state = mRails[rail];
    if (state.chunkConsumedGeneration[chunk] == generation)
        throw std::runtime_error("chunk scattered twice");
    size_t trace = 0;
    if (TraceIndex(generation, trace)) mTrace[trace].localScatterBegin[rail][chunk].Publish(NowNs());
    ScatterChunkPayload(rail, chunk, mBlocksPerRail, mParams.RailBlocks(rail), mOptions.sglItems, mCopyEntries, mParams.blockBytes,
        state.buffer.Data(), state.buffer.Size(), state.stageBuffer.Data(), state.stageBuffer.Size());
    state.chunkConsumedGeneration[chunk] = generation;
    if (TraceIndex(generation, trace)) mTrace[trace].localScatterEnd[rail][chunk].Publish(NowNs());
}

bool SparseCopyBenchmark::AllChunksReady(uint64_t generation) const noexcept
{
    return AllSglChunksReady(mOptions.links, mChunksPerRail, [this, generation](uint16_t rail, uint32_t chunk) {
        return chunk >= RailChunks(rail) || mRails[rail].chunkReadyGeneration[chunk].load(std::memory_order_acquire) == generation;
    });
}

void SparseCopyBenchmark::WaitAndScatterSgl(uint64_t generation, uint64_t expectedRequestSend, uint64_t deadlineNs)
{
    const uint32_t totalChunks = TotalChunks();
    SglSchedulerState scheduler{};
    auto ready = [this, generation](uint16_t rail, uint32_t chunk) {
        const RailState &state = mRails[rail];
        return chunk < RailChunks(rail) && state.chunkConsumedGeneration[chunk] != generation &&
            state.chunkReadyGeneration[chunk].load(std::memory_order_acquire) == generation;
    };
    auto scatter = [this, generation](uint16_t rail, uint32_t chunk) {
        ScatterChunk(rail, chunk, generation);
    };
    auto checkpoint = [this, deadlineNs] {
        CheckFatal("SGL scatter scheduler");
        if (NowNs() >= deadlineNs)
            throw std::runtime_error("timed out waiting for SGL scatter scheduler");
    };
    if (mOptions.pipeline == PipelineMode::Off) {
        WaitDataUntil("all SGL chunks ready", deadlineNs, [this, generation] { return AllChunksReady(generation); });
        if (!ScanReadySglChunks(mOptions.links, mChunksPerRail, scheduler, ready, scatter, checkpoint))
            throw std::runtime_error("all-ready SGL scan made no progress");
    } else {
        while (scheduler.scattered < totalChunks) {
            const bool progress = ScanReadySglChunks(
                mOptions.links, mChunksPerRail, scheduler, ready, scatter, checkpoint);
            if (!progress) CpuRelax();
        }
    }
    if (scheduler.scattered != totalChunks)
        throw std::runtime_error("scatter completed with wrong chunk count");
    WaitDataUntil("SGL completion gate", deadlineNs, [this, expectedRequestSend, &scheduler, totalChunks] {
        return SglCompletionReached(scheduler.scattered, totalChunks,
            mRails[0].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedRequestSend);
    });
}

void SparseCopyBenchmark::VerifyLocalMarkers(uint64_t generation)
{
    for (uint32_t index = 0; index < mParams.blocks; ++index) {
        const uint16_t rail = RailForRequestIndex(index, mParams);
        const CopyEntry &entry = mCopyEntries[index];
        const uint32_t source = rail * mBlocksPerRail + static_cast<uint32_t>(entry.remoteSourceOffset / kStrideBytes);
        const auto *words = reinterpret_cast<const uint64_t *>(mRails[rail].buffer.Data() + entry.localDestinationOffset);
        const uint32_t last = mParams.blockBytes / 8 - 1;
        if (words[0] != PatternWord(generation, source, 0) || words[last] != PatternWord(generation, source, last))
            throw std::runtime_error("stale/missing data at generation=" + std::to_string(generation) +
                " request=" + std::to_string(index));
    }
}

void SparseCopyBenchmark::VerifyLocalDestination(const std::vector<CopyEntry> &entries, uint64_t generation)
{
    for (uint32_t index = 0; index < mParams.blocks; ++index) {
        const uint16_t rail = RailForRequestIndex(index, mParams);
        const uint32_t sourceSlot = static_cast<uint32_t>(entries[index].remoteSourceOffset / kStrideBytes);
        const uint32_t globalBlock = static_cast<uint32_t>(rail) * mBlocksPerRail + sourceSlot;
        std::string error;
        uint64_t destinationAddress = 0;
        if (!CheckedAddAddress(reinterpret_cast<uintptr_t>(mRails[rail].buffer.Data()),
                entries[index].localDestinationOffset, kStrideBytes, mRails[rail].buffer.Size(), destinationAddress))
            throw std::runtime_error("verification address overflow/out of range");
        const auto *destination = reinterpret_cast<const uint8_t *>(static_cast<uintptr_t>(destinationAddress));
        if (!VerifyBlock(destination, generation, globalBlock, mParams.blockBytes, BodySeed(generation), error) ||
            !VerifyGap(destination, mParams.blockBytes, error))
            throw std::runtime_error("request " + std::to_string(index) + ": " + error);
    }
}

}  // namespace rdma_bench
