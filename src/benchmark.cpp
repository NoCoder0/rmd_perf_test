// SPDX-License-Identifier: MulanPSL-2.0
#include "benchmark.h"

namespace rdma_bench {

SparseCopyBenchmark::SparseCopyBenchmark(Options options) : mOptions(std::move(options))
{
    mParams.links = mOptions.links;
    mParams.verifyRounds = mOptions.verifyRounds;
    mParams.warmupRounds = mOptions.warmupRounds;
    mParams.measureRounds = mOptions.measureRounds;
    mParams.traceRounds = mOptions.traceRounds;
    mParams.mode = static_cast<uint16_t>(mOptions.mode);
    mParams.sglItems = mOptions.sglItems;
    mParams.notifyEveryWrs = mOptions.notifyEveryWrs;
    mParams.pipeline = static_cast<uint16_t>(mOptions.pipeline);
    mParams.sourceFormat = mOptions.mode == CopyMode::Direct ? kSourceFormatDirectPairs : kSourceFormatSparsePairs;
    for (const uint32_t bytes : mOptions.blockLengths) {
        for (const uint32_t blocks : mOptions.blockCounts) {
            CaseParameters item = mParams;
            item.blocks = blocks;
            item.blockBytes = bytes;
            mCases.push_back(item);
            mTotalGenerations += item.TotalRounds();
            mMaxRailBlocks = std::max(mMaxRailBlocks, item.RailCapacity());
            mMaxStageBytes = std::max(mMaxStageBytes, static_cast<size_t>(item.RailCapacity()) * bytes);
        }
    }
    mParams = mCases.front();
    mBlocksPerRail = mParams.RailCapacity();
    mChunksPerRail = mOptions.mode == CopyMode::Sgl ? ChunkCount(mBlocksPerRail, mOptions.sglItems) : 0;
    mMatrixItems.resize(mCases.size());
    for (size_t i = 0; i < mCases.size(); ++i) {
        uint8_t *cursor = mMatrixItems[i].data();
        PutU32(cursor, kMatrixMagic);
        PutU16(cursor, kProtocolVersion);
        PutU16(cursor, 0);
        PutU32(cursor, static_cast<uint32_t>(mCases.size()));
        PutU32(cursor, static_cast<uint32_t>(i));
        EncodeParams(cursor, mCases[i]);
    }
    mCopyEntries.reserve(kMaxBlocks);
    mActiveCopyEntries.reserve(kMaxBlocks);
    mSparseCopyNs.reserve(mParams.measureRounds);
    mResults.reserve(mCases.size());
    mSummary.reserve(mCases.size());
}

int SparseCopyBenchmark::Run()
{
    try {
        if (TraceEnabled()) {
            size_t operations = 0;
            for (const auto &item : mCases) {
                for (uint16_t rail = 0; rail < item.links; ++rail)
                    operations += static_cast<size_t>(item.traceRounds) *
                        (mOptions.mode == CopyMode::Sgl ?
                            ChunkCount(item.RailBlocks(rail), mOptions.sglItems) : item.RailBlocks(rail));
            }
            InitializeDetailedTrace(RoleName(mOptions.role).c_str(), operations,
                mOptions.role == Role::Remote && mOptions.mode == CopyMode::Sgl);
        }
        PinCurrentThread(mOptions.appCpus[0]);
        StartSecondaryRailThread();
        SetupFixedRails();
        if (mOptions.role == Role::Remote) {
            mRemoteReady.store(true, std::memory_order_release);
            PrintListening();
            WaitForChannels();
            RunRemote();
        } else {
            ConnectLocal();
            RunLocal();
        }
        CheckFatal("normal completion");
        if (!DrainUntilComplete()) throw std::runtime_error("callbacks did not drain before teardown");
        TeardownFixedRails();
        StopSecondaryRailThread();
        const bool detailedTraceOk = !TraceEnabled() || FinishDetailedTrace();
        if (!detailedTraceOk)
            std::cerr << "ERROR: incomplete detailed trace; inspect hcom_trace_summary and rebuild HCOM if hooks are missing\n";
        if (mOptions.role == Role::Local) PrintBatchResult(); else PrintRemoteStatus();
        return detailedTraceOk ? 0 : 1;
    } catch (const std::exception &error) {
        EndDetailedTrace();
        RecordFailure(error.what());
        std::cerr << "ERROR case=" << mCaseIndex + 1 << " blocks=" << mParams.blocks << " block_bytes=" << mParams.blockBytes << ": " << error.what() << std::endl;
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
        if (TraceEnabled()) FinishDetailedTrace(true);
        return 1;
    }
}

void SparseCopyBenchmark::NegotiateMatrix()
{
    for (const auto &item : mMatrixItems) {
        std::array<uint8_t, kMatrixItemWireBytes> responseBytes{};
        UBSHcomRequest request(const_cast<uint8_t *>(item.data()), item.size(), kOpMatrix);
        UBSHcomResponse response(responseBytes.data(), responseBytes.size());
        RequireOk(ChannelCopyRequired(0, "matrix")->Call(request, response, nullptr), "matrix Call");
        if (response.size != item.size() || std::memcmp(response.address, item.data(), item.size()) != 0)
            throw std::runtime_error("matrix reply differs");
    }
}

void SparseCopyBenchmark::SelectCase(size_t index)
{
    if (mSecondary.completed.load(std::memory_order_acquire) != mSecondary.issued.load(std::memory_order_acquire))
        throw std::logic_error("case transition before secondary rail completed");
    // Both app threads are quiescent, and previous case callbacks are drained.
    // The mutex also excludes unexpected late control/data handlers during mutation.
    std::lock_guard<std::mutex> caseLock(mCaseMutex);
    std::lock_guard<std::mutex> pendingLock(mPendingMutex);
    if (mPendingCopyReqPublished.load(std::memory_order_acquire) || mPendingCopyReqBytes != 0)
        throw std::runtime_error("pending request at case boundary");
    mCaseIndex = index;
    mParams = mCases[index];
    mCaseFirstGeneration = mCaseLastGeneration + 1;
    mCaseLastGeneration += mParams.TotalRounds();
    mBlocksPerRail = mParams.RailCapacity();
    mChunksPerRail = mOptions.mode == CopyMode::Sgl ? ChunkCount(mBlocksPerRail, mOptions.sglItems) : 0;
    mCopyEntries.resize(mParams.blocks);
    mActiveCopyEntries.resize(mParams.blocks);
    mSparseCopyNs.clear();
    mMeasureWallStartNs = mMeasureWallEndNs = 0;
    if (TraceEnabled()) {
        mTrace.reset(new TraceRound[mParams.traceRounds]);
        for (uint32_t i = 0; i < mParams.traceRounds; ++i)
            mTrace[i].generation = mCaseFirstGeneration + mParams.verifyRounds + i;
    }
    // Reset gaps before granting the remote permission to write the new case.
    if (mOptions.role == Role::Local)
        for (uint16_t rail = 0; rail < mOptions.links; ++rail)
            std::memset(mRails[rail].buffer.Data(), kDstGapSentinel, mRails[rail].buffer.Size());
}

void SparseCopyBenchmark::BeginCase()
{
    const uint64_t id = mCaseIndex + 1;
    if (mOptions.role == Role::Local) {
        mCaseTokenPayload = EncodeToken(kCaseStartMagic, kOpCaseStart, id, 0);
        PostAsyncSend(0, ChannelCopyRequired(0, "case start"), mCaseTokenPayload.data(),
            mCaseTokenPayload.size(), kOpCaseStart);
        const uint64_t sends = ExpectedSendCallbacks(0);
        WaitControl("case READY", [this, id, sends] {
            return mCaseReadyReceived.load(std::memory_order_acquire) == id &&
                mRails[0].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= sends;
        });
    } else {
        WaitControl("case START", [this, id] { return mCaseStartReceived.load(std::memory_order_acquire) == id; });
        mCaseAcceptRequests.store(true, std::memory_order_release);
        mCaseTokenPayload = EncodeToken(kCaseReadyMagic, kOpCaseReady, id, 0);
        PostAsyncSend(0, ChannelCopyRequired(0, "case ready"), mCaseTokenPayload.data(),
            mCaseTokenPayload.size(), kOpCaseReady);
    }
}

void SparseCopyBenchmark::EndCase()
{
    FinishAllRails();
    if (!DrainUntilComplete()) throw std::runtime_error("case callbacks did not drain");
    if (TraceEnabled()) EndDetailedTrace();
    CheckFatal("case boundary");
    mCaseAcceptRequests.store(false, std::memory_order_release);
    if (TraceEnabled()) EmitTrace();
}

void SparseCopyBenchmark::RecordFailure(const std::string &message) noexcept
{
    bool expected = false;
    if (!mFatal.compare_exchange_strong(expected, true, std::memory_order_release,
        std::memory_order_relaxed)) return;
    try { std::lock_guard<std::mutex> lock(mErrorMutex); mError = message; } catch (...) {}
    mChannelCv.notify_all();
}

}  // namespace rdma_bench
