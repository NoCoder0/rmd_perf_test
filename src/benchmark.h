// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include "config.h"
#include "protocol.h"
#include "data_path.h"
#include "support.h"
#include "detailed_trace.h"

namespace rdma_bench {

class SparseCopyBenchmark {
public:
    explicit SparseCopyBenchmark(Options options);
    int Run();

private:
    bool TraceEnabled() const
    { return mOptions.kind == RunKind::Trace; }

    bool TraceIndex(uint64_t generation, size_t &index) const noexcept
    {
        if (!TraceEnabled()) return false;
        const uint64_t first = mCaseFirstGeneration + mParams.verifyRounds + mParams.warmupRounds + mParams.measureRounds;
        if (generation < first || generation >= first + mParams.traceRounds) return false;
        index = static_cast<size_t>(generation - first);
        return true;
    }

    bool PublishCallbackTrace(uint64_t generation, TracePoint &point, const char *event) noexcept;
    void StartSecondaryRailThread();
    void SecondaryRailThreadMain() noexcept;
    uint64_t IssueSecondaryRailCommand(RailCommand command, uint64_t generation = 0, uint64_t deadlineNs = 0);
    void WaitSecondaryRailCommand(uint64_t sequence, const char *what);
    bool WaitSecondaryNoThrow(uint64_t sequence) noexcept;
    bool QuiesceSecondaryNoThrow() noexcept;
    void StopSecondaryRailThread() noexcept;
    void SetupFixedRails();
    void SetupRail(uint16_t rail);
    void PrintListening() const;
    int OnNewChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept;
    bool ConfigureChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept;
    void ConnectLocal();
    void ConnectAndHandshakeRail(uint16_t rail);
    int OnIncoming(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnHello(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnMatrix(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnCaseToken(uint16_t rail, UBSHcomServiceContext &context, bool ready) noexcept;
    int OnCopyReq(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnDataDone(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnChunkDone(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnCopyError(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    bool DecodeTokenForRail(uint16_t rail, UBSHcomServiceContext &context, uint32_t magic,
        uint16_t opcode, uint64_t &generation) noexcept;
    int OnFinish(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    int OnFinishAck(uint16_t rail, UBSHcomServiceContext &context) noexcept;
    Callback *NewDataCallback(uint16_t rail, uint64_t generation, uint64_t completionTarget, int chunk = -1);
    Callback *NewSendCallback(uint16_t rail);
    void NegotiateMatrix();
    uint32_t RailChunks(uint16_t rail) const
    {
        return mOptions.mode == CopyMode::Sgl ? ChunkCount(mParams.RailBlocks(rail), mOptions.sglItems) : 0;
    }

    uint32_t TotalChunks() const
    {
        uint32_t total = 0;
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) total += RailChunks(rail);
        return total;
    }

    uint32_t RailNotifications(uint16_t rail) const
    {
        return NotificationCount(RailChunks(rail), mOptions.notifyEveryWrs);
    }

    uint32_t TotalNotifications() const
    {
        uint32_t total = 0;
        for (uint16_t rail = 0; rail < mOptions.links; ++rail) total += RailNotifications(rail);
        return total;
    }

    uint64_t BodySeed(uint64_t generation) const
    {
        return std::min(generation, mCaseFirstGeneration + mParams.verifyRounds - 1);
    }

    void SelectCase(size_t index);
    void BeginCase();
    void EndCase();
    void RunLocal();
    void SparseCopy(uint64_t generation, bool measure);
    void RunRemote();
    bool AllHellosSeen() const noexcept;
    void ReceivePendingCopyRequest(uint64_t deadlineNs, uint64_t generation);
    void DecodeActiveCopyRequest(uint64_t generation);
    void ProcessRemoteRail(uint16_t rail, uint64_t generation, uint64_t deadlineNs);
    void ProcessRemoteSglRail(uint16_t rail, uint64_t generation, uint64_t deadlineNs);
    void WaitForSglWindow(uint16_t rail, uint64_t deadlineNs);
    void BuildRemoteSglRequests(uint16_t rail);
    void ScatterChunk(uint16_t rail, uint32_t chunk, uint64_t generation);
    void ScatterSglRail(uint16_t rail, uint64_t generation, uint64_t deadlineNs);
    bool AllChunksReady(uint64_t generation) const noexcept;
    void WaitAndScatterSgl(uint64_t generation, uint64_t expectedRequestSend, uint64_t deadlineNs);
    void BuildRemotePutRequests(uint16_t rail);
    void FillRemoteSourceRail(uint16_t rail, uint64_t generation);
    void VerifyLocalMarkers(uint64_t generation);
    void VerifyLocalDestination(const std::vector<CopyEntry> &entries, uint64_t generation);
    void TrySendCopyError(uint64_t generation, uint32_t stage, uint32_t code, uint32_t detail) noexcept;
    void FinishAllRails();
    void FinishRail(uint16_t rail);
    void PostAsyncSend(uint16_t rail, const UBSHcomChannelPtr &channel,
        uint8_t *data, size_t size, uint16_t opcode);
    uint64_t ExpectedSendCallbacks(uint16_t rail) const noexcept
    {
        return mRails[rail].appCounters.attemptedSendCallbacks +
            mRails[rail].callbackCounters.workerAttemptedSendCallbacks.load(std::memory_order_acquire);
    }

    uint64_t DeadlineFrom(uint64_t startNs) const
    {
        const uint64_t budget = static_cast<uint64_t>(mOptions.timeoutSec) * 1000000000ULL;
        if (startNs > std::numeric_limits<uint64_t>::max() - budget)
            throw std::runtime_error("deadline overflow");
        return startNs + budget;
    }

    template <typename Predicate> void WaitDataUntil(const char *what, uint64_t deadlineNs, Predicate predicate)
    {
        CheckFatal(what);
        uint32_t spins = 0;
        while (!predicate()) {
            CheckFatal(what); CpuRelax();
            if ((++spins & (kDataDeadlineCheckInterval - 1)) == 0 && NowNs() >= deadlineNs)
                throw std::runtime_error(std::string("timed out waiting for ") + what);
        }
        CheckFatal(what);
        if (NowNs() >= deadlineNs) throw std::runtime_error(std::string("deadline exceeded while completing ") + what);
    }

    template <typename Predicate> void WaitData(const char *what, Predicate predicate)
    {
        WaitDataUntil(what, DeadlineFrom(NowNs()), predicate);
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

    void WaitForChannels();
    UBSHcomChannelPtr ChannelCopy(uint16_t rail) const;
    UBSHcomChannelPtr ChannelCopyRequired(uint16_t rail, const char *operation) const;
    void RequireOk(int rc, const char *operation);
    void RecordFailure(const std::string &message) noexcept;
    void CheckFatal(const char *where) const
    {
        if (!mFatal.load(std::memory_order_acquire)) return;
        std::string error = "unknown asynchronous failure";
        { std::lock_guard<std::mutex> lock(mErrorMutex); if (!mError.empty()) error = mError; }
        throw std::runtime_error(std::string(where) + ": " + error);
    }

    bool CallbacksDrained() const noexcept;
    bool DrainUntilComplete() noexcept;
    void TeardownFixedRails();
    void TryTeardownFixedRails() noexcept;
    void TeardownRail(uint16_t rail) noexcept;
    void EmitTracePoint(const char *event, uint64_t generation, int rail, const TracePoint &point,
        int chunk = -1) const;
    void EmitTrace() const;
    std::string FormatLocalResult() const;
    void PrintBatchResult() const;
    void PrintRemoteStatus() const;
    Options mOptions;
    CaseParameters mParams;
    struct Summary {
        CaseParameters params;
        double avg, p50, p95, p99, gbps, wallGbps;
    };
    std::vector<CaseParameters> mCases;
    std::vector<std::array<uint8_t, kMatrixItemWireBytes>> mMatrixItems;
    std::vector<std::string> mResults;
    std::vector<Summary> mSummary;
    std::atomic<uint32_t> mMatrixReceived{0};
    std::atomic<uint64_t> mCaseStartReceived{0}, mCaseReadyReceived{0};
    std::atomic<bool> mCaseAcceptRequests{false};
    std::array<uint8_t, kTokenWireBytes> mCaseTokenPayload{};
    size_t mCaseIndex = 0;
    uint64_t mCaseFirstGeneration = 1, mCaseLastGeneration = 0;
    uint64_t mTotalGenerations = 0;
    uint64_t mNextReceiveGeneration = 1;
    uint32_t mMaxRailBlocks = 0;
    size_t mMaxStageBytes = 0;
    std::mutex mCaseMutex, mPendingMutex;
    uint32_t mBlocksPerRail = 0;
    uint32_t mChunksPerRail = 0;
    std::array<RailState, kMaxLinks> mRails{};
    std::atomic<bool> mRemoteReady{false};
    std::atomic<bool> mTearingDown{false};
    std::atomic<bool> mFatal{false};
    mutable std::mutex mErrorMutex;
    std::string mError;
    mutable std::mutex mChannelsMutex;
    std::condition_variable mChannelCv;
    alignas(kCounterAlignment) std::atomic<uint64_t> mActiveCallbacks{0};
    alignas(kCounterAlignment) std::atomic<uint64_t> mExpectedGeneration{0};
    std::vector<CopyEntry> mCopyEntries{};
    std::vector<CopyEntry> mActiveCopyEntries{};
    std::array<uint8_t, kMaxCopyReqWireBytes> mCopyReqPayload{};
    std::array<uint8_t, kMaxCopyReqWireBytes> mPendingCopyReqPayload{};
    std::array<uint8_t, kMaxCopyReqWireBytes> mActiveCopyReqPayload{};
    uint32_t mPendingCopyReqBytes = 0;
    uint32_t mActiveCopyReqBytes = 0;
    std::array<std::array<uint8_t, kFragmentWireBytes>, kMaxFragments> mRequestFragments{};
    std::atomic<bool> mPendingCopyReqPublished{false};
    std::array<uint8_t, kCopyErrorWireBytes> mCopyErrorPayload{};
    std::unique_ptr<TraceRound[]> mTrace;
    std::vector<uint64_t> mSparseCopyNs;
    uint64_t mMeasureWallStartNs = 0;
    uint64_t mMeasureWallEndNs = 0;
    SecondaryRailExecutor mSecondary;
};

}  // namespace rdma_bench
