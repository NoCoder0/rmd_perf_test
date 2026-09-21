// SPDX-License-Identifier: MulanPSL-2.0
#include "benchmark.h"
#include "inflight_window.h"

namespace rdma_bench {

int SparseCopyBenchmark::OnCopyReq(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    if (mOptions.role != Role::Remote || rail != 0 || !AllHellosSeen() ||
        !mCaseAcceptRequests.load(std::memory_order_acquire)) {
        RecordFailure("COPY_REQ outside an active negotiated case"); return -1;
    }
    std::lock_guard<std::mutex> lock(mPendingMutex);
    const uint64_t generation = mNextReceiveGeneration;
    if (generation > mCaseLastGeneration || mPendingCopyReqPublished.load(std::memory_order_acquire) ||
        !AppendRequestFragment(context.MessageData(), context.MessageDataLen(), generation, mParams,
            mPendingCopyReqPayload.data(), mPendingCopyReqBytes)) {
        RecordFailure("COPY_REQ fragment sequence/extent/generation mismatch"); return -1;
    }
    if (mPendingCopyReqBytes == mParams.RequestBytes()) {
        size_t trace = 0;
        if (TraceIndex(generation, trace))
            PublishCallbackTrace(generation, mTrace[trace].remoteRequestReceived, "remote_request_received");
        ++mNextReceiveGeneration;
        mPendingCopyReqPublished.store(true, std::memory_order_release);
    }
    return 0;
}

void SparseCopyBenchmark::RunRemote()
{
    WaitData("all HELLO", [this] { return AllHellosSeen(); });
    for (size_t index = 0; index < mCases.size(); ++index)
        WaitControl("matrix item", [this, index] { return mMatrixReceived.load(std::memory_order_acquire) > index; });
    if (!DrainUntilComplete()) throw std::runtime_error("matrix callbacks did not drain");
    for (size_t index = 0; index < mCases.size(); ++index) {
        SelectCase(index);
        BeginCase();
        for (uint64_t generation = mCaseFirstGeneration; generation <= mCaseLastGeneration; ++generation) {
            try {
                if (TraceEnabled() && generation == mCaseFirstGeneration + mParams.verifyRounds +
                    mParams.warmupRounds + mParams.measureRounds)
                    BeginDetailedTrace(mCaseIndex + 1);
                const uint64_t deadlineNs = DeadlineFrom(NowNs());
                ReceivePendingCopyRequest(deadlineNs, generation);
                size_t trace = 0;
                const bool tracing = TraceIndex(generation, trace);
                if (tracing) mTrace[trace].remoteRequestCopied.Publish(NowNs());
                DecodeActiveCopyRequest(generation);
                if (tracing) mTrace[trace].remoteRequestDecoded.Publish(NowNs());
                uint64_t sequence = 0;
                if (mOptions.links == 2)
                    sequence = IssueSecondaryRailCommand(RailCommand::ProcessRemoteRound, generation, deadlineNs);
                ProcessRemoteRail(0, generation, deadlineNs);
                if (mOptions.links == 2)
                    WaitDataUntil("rail 1 remote copy", deadlineNs, [this, sequence] {
                        return mSecondary.completed.load(std::memory_order_acquire) >= sequence;
                    });
            } catch (...) {
                TrySendCopyError(generation, kCopyErrorStageRemoteProcess, kCopyErrorCodeRequestFailed, 0);
                throw;
            }
        }
        EndCase();
    }
}

void SparseCopyBenchmark::ReceivePendingCopyRequest(uint64_t deadlineNs, uint64_t generation)
{
    WaitDataUntil("COPY_REQ", deadlineNs,
        [this] { return mPendingCopyReqPublished.load(std::memory_order_acquire); });
    size_t trace = 0;
    if (TraceIndex(generation, trace)) mTrace[trace].remoteRequestObserved.Publish(NowNs());
    std::lock_guard<std::mutex> lock(mPendingMutex);
    mActiveCopyReqBytes = mPendingCopyReqBytes;
    if (mActiveCopyReqBytes <= mActiveCopyReqPayload.size())
        std::memcpy(mActiveCopyReqPayload.data(), mPendingCopyReqPayload.data(), mActiveCopyReqBytes);
    mPendingCopyReqPublished.store(false, std::memory_order_relaxed);
    mPendingCopyReqBytes = 0;
}

void SparseCopyBenchmark::DecodeActiveCopyRequest(uint64_t generation)
{
    const uint64_t railBytes = static_cast<uint64_t>(mMaxRailBlocks) * kStrideBytes;
    std::string error;
    if (!DecodeCopyRequest(mActiveCopyReqPayload.data(), mActiveCopyReqBytes, generation, mParams,
        railBytes, railBytes, mActiveCopyEntries, error))
        throw std::runtime_error("invalid COPY_REQ: " + error);
}

void SparseCopyBenchmark::ProcessRemoteRail(uint16_t rail, uint64_t generation, uint64_t deadlineNs)
{
    RailState &state = mRails[rail];
    size_t stageTrace = 0;
    const bool tracing = TraceIndex(generation, stageTrace);
    if (tracing) mTrace[stageTrace].remoteSourcePrepareBegin[rail].Publish(NowNs());
    FillRemoteSourceRail(rail, generation);
    if (tracing) mTrace[stageTrace].remoteSourcePrepared[rail].Publish(NowNs());
    if (mOptions.mode == CopyMode::Sgl) {
        ProcessRemoteSglRail(rail, generation, deadlineNs);
        return;
    }
    BuildRemotePutRequests(rail);
    const uint64_t expectedData = state.appCounters.attemptedDataCallbacks + mParams.RailBlocks(rail);
    const UBSHcomChannelPtr channel = ChannelCopyRequired(rail, "remote copy");
    for (uint32_t i = 0; i < mParams.RailBlocks(rail); ++i) {
        Callback *callback = NewDataCallback(rail, generation, expectedData);
        if (callback == nullptr) throw std::runtime_error("Put callback allocation failed");
        ++state.appCounters.attemptedDataCallbacks;
        const int rc = channel->Put(state.putRequests[i], callback);
        if (rc != 0) throw std::runtime_error("Put failed: " + std::to_string(rc));
        if (((i + 1) & (kDataDeadlineCheckInterval - 1)) == 0 && NowNs() >= deadlineNs)
            throw std::runtime_error("deadline exceeded while posting direct writes");
    }
    size_t trace = 0;
    if (TraceIndex(generation, trace)) mTrace[trace].remotePosted[rail].Publish(NowNs());
    state.dataDonePayload = EncodeDataDone(generation, rail, mParams.RailBlocks(rail), mParams.blockBytes);
    const uint64_t expectedSend = ExpectedSendCallbacks(rail) + 1;
    PostAsyncSend(rail, channel, state.dataDonePayload.data(), state.dataDonePayload.size(), kOpDataDone);
    if (TraceIndex(generation, trace)) mTrace[trace].remoteDonePosted[rail].Publish(NowNs());
    WaitDataUntil("remote callbacks", deadlineNs, [this, rail, expectedData, expectedSend] {
        return mRails[rail].callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
            mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend;
    });
}

void SparseCopyBenchmark::WaitForSglWindow(uint16_t rail, uint64_t deadlineNs)
{
    const RailState &state = mRails[rail];
    const uint64_t submitted = state.appCounters.attemptedDataCallbacks;
    const auto hasCredit = [&state, submitted, this] {
        return HasSglWindowCredit(submitted,
            state.callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire), mOptions.maxInflight);
    };
    if (!hasCredit()) WaitDataUntil("SGL inflight window", deadlineNs, hasCredit);
}

void SparseCopyBenchmark::ProcessRemoteSglRail(uint16_t rail, uint64_t generation, uint64_t deadlineNs)
{
    RailState &state = mRails[rail];
    BuildRemoteSglRequests(rail);
    size_t stageTrace = 0;
    if (TraceIndex(generation, stageTrace)) mTrace[stageTrace].remoteRequestsPrepared[rail].Publish(NowNs());
    const uint64_t expectedData = state.appCounters.attemptedDataCallbacks + RailChunks(rail);
    const uint64_t expectedSend = ExpectedSendCallbacks(rail) + RailNotifications(rail);
    const UBSHcomChannelPtr channel = ChannelCopyRequired(rail, "remote SGL copy");
    for (uint32_t chunk = 0; chunk < RailChunks(rail); ++chunk) {
        // Disabled path: no extra atomic load or clock read.
        if (mOptions.maxInflight != 0) WaitForSglWindow(rail, deadlineNs);
        Callback *callback = NewDataCallback(rail, generation, expectedData, static_cast<int>(chunk));
        if (callback == nullptr) throw std::runtime_error("PutV callback allocation failed");
        ++state.appCounters.attemptedDataCallbacks;
        size_t trace = 0;
        const bool tracing = TraceIndex(generation, trace);
        int rc;
        if (tracing) {
            ock::hcom::UBSHcomRdmaTraceOperationScope operation(tracing, generation, rail, chunk);
            rc = channel->PutV(state.sglRequests[chunk], callback);
        } else rc = channel->PutV(state.sglRequests[chunk], callback);
        if (rc != 0) throw std::runtime_error("PutV failed: " + std::to_string(rc));
        if (tracing)
            mTrace[trace].remoteChunkPosted[rail][chunk].Publish(NowNs());
        if (EndsNotificationGroup(chunk, RailChunks(rail), mOptions.notifyEveryWrs)) {
            const uint32_t first = NotificationFirstChunk(chunk, mOptions.notifyEveryWrs);
            const ChunkDoneInfo done = MakeChunkDone(rail, generation, first, mParams.RailBlocks(rail),
                mOptions.sglItems, mParams.blockBytes, mOptions.notifyEveryWrs);
            state.chunkDonePayloads[first] = EncodeChunkDone(done);
            // Ordered after all data WRs in this group on the same channel/QP.
            if (tracing) {
                ock::hcom::UBSHcomRdmaTraceOperationScope operation(true, generation, rail, first);
                PostAsyncSend(rail, channel, state.chunkDonePayloads[first].data(), kChunkDoneWireBytes, kOpChunkDone);
            } else {
                PostAsyncSend(rail, channel, state.chunkDonePayloads[first].data(), kChunkDoneWireBytes, kOpChunkDone);
            }
            if (TraceIndex(generation, trace))
                mTrace[trace].remoteChunkDonePosted[rail][first].Publish(NowNs());
        }
        if (((chunk + 1) & (kDataDeadlineCheckInterval - 1)) == 0 && NowNs() >= deadlineNs)
            throw std::runtime_error("deadline exceeded while posting SGL chunks");
    }
    WaitDataUntil("remote SGL callbacks", deadlineNs, [this, rail, expectedData, expectedSend] {
        return mRails[rail].callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) >= expectedData &&
            mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= expectedSend;
    });
}

void SparseCopyBenchmark::BuildRemoteSglRequests(uint16_t rail)
{
    RailState &state = mRails[rail];
    const uint32_t globalFirst = static_cast<uint32_t>(rail) * mBlocksPerRail;
    const uint64_t sourceBase = reinterpret_cast<uintptr_t>(state.buffer.Data());
    for (uint32_t chunk = 0; chunk < RailChunks(rail); ++chunk) {
        const uint32_t first = chunk * mOptions.sglItems;
        const uint32_t count = ChunkItemCount(mParams.RailBlocks(rail), mOptions.sglItems, chunk);
        for (uint32_t item = 0; item < count; ++item) {
            const CopyEntry &entry = mActiveCopyEntries[globalFirst + first + item];
            uint64_t localAddress = 0;
            uint64_t remoteAddress = 0;
            const uint64_t stageOffset = static_cast<uint64_t>(first + item) * mParams.blockBytes;
            if (!CheckedAddAddress(sourceBase, entry.remoteSourceOffset, mParams.blockBytes, state.buffer.Size(),
                    localAddress) ||
                !CheckedAddAddress(state.peerStageAddress, stageOffset, mParams.blockBytes, state.peerStageBytes,
                    remoteAddress))
                throw std::runtime_error("SGL address overflow/out of range rail=" + std::to_string(rail));
            UBSHcomOneSideRequest &iov = state.sglIovs[first + item];
            iov.lAddress = static_cast<uintptr_t>(localAddress);
            iov.rAddress = static_cast<uintptr_t>(remoteAddress);
            iov.lKey = state.memoryKey;
            iov.rKey = state.peerStageKey;
            iov.size = mParams.blockBytes;
        }
        state.sglRequests[chunk].iov = state.sglIovs.data() + first;
        state.sglRequests[chunk].iovCount = static_cast<uint16_t>(count);
    }
}

void SparseCopyBenchmark::BuildRemotePutRequests(uint16_t rail)
{
    RailState &state = mRails[rail];
    const uint32_t first = static_cast<uint32_t>(rail) * mBlocksPerRail;
    for (uint32_t i = 0; i < mParams.RailBlocks(rail); ++i) {
        const CopyEntry &entry = mActiveCopyEntries[first + i];
        UBSHcomOneSideRequest &request = state.putRequests[i];
        uint64_t localAddress = 0;
        uint64_t remoteAddress = 0;
        if (!CheckedAddAddress(reinterpret_cast<uintptr_t>(state.buffer.Data()), entry.remoteSourceOffset,
                mParams.blockBytes, state.buffer.Size(), localAddress) ||
            !CheckedAddAddress(state.peerDestinationAddress, entry.localDestinationOffset, mParams.blockBytes,
                state.peerDestinationBytes, remoteAddress))
            throw std::runtime_error("direct address overflow/out of range rail=" + std::to_string(rail));
        request.lAddress = static_cast<uintptr_t>(localAddress);
        request.rAddress = static_cast<uintptr_t>(remoteAddress);
        request.lKey = state.memoryKey;
        request.rKey = state.peerDestinationKey;
        request.size = mParams.blockBytes;
    }
}

void SparseCopyBenchmark::FillRemoteSourceRail(uint16_t rail, uint64_t generation)
{
    // Verify rounds initialize and fully validate the body. Static then retains the last verified content.
    if (mOptions.sourceUpdate == "static" && generation >= mCaseFirstGeneration + mParams.verifyRounds) return;
    for (uint32_t slot = 0; slot < mParams.RailBlocks(rail); ++slot) {
        const uint32_t globalBlock = static_cast<uint32_t>(rail) * mBlocksPerRail + slot;
        uint8_t *address = mRails[rail].buffer.Data() + static_cast<size_t>(slot) * kStrideBytes;
        if (generation < mCaseFirstGeneration + mParams.verifyRounds) {
            FillBlock(address, generation, globalBlock, mParams.blockBytes);
        } else {
            auto *words = reinterpret_cast<uint64_t *>(address);
            words[0] = PatternWord(generation, globalBlock, 0);
            const uint32_t last = mParams.blockBytes / 8 - 1;
            words[last] = PatternWord(generation, globalBlock, last);
        }
    }
}

void SparseCopyBenchmark::TrySendCopyError(uint64_t generation, uint32_t stage, uint32_t code, uint32_t detail) noexcept
{
    try {
        const UBSHcomChannelPtr channel = ChannelCopy(0);
        if (channel == nullptr) return;
        mCopyErrorPayload = EncodeCopyError(generation, stage, code, detail);
        PostAsyncSend(0, channel, mCopyErrorPayload.data(), mCopyErrorPayload.size(), kOpCopyError);
    } catch (...) {}
}

}  // namespace rdma_bench
