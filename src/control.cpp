// SPDX-License-Identifier: MulanPSL-2.0
#include "benchmark.h"

namespace rdma_bench {

int SparseCopyBenchmark::OnIncoming(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    // Before the case mutex: distinguish dispatch/lock delay from wire decoding.
    if (TraceEnabled() && context.OpCode() == kOpChunkDone)
        ock::hcom::UBSHcomRdmaTraceMark(ock::hcom::UBSHcomRdmaTraceKind::NOTIFY_INCOMING, 0, rail);
    ActiveCallbackGuard guard(mActiveCallbacks);
    std::lock_guard<std::mutex> caseLock(mCaseMutex);
    const UBSHcomChannelPtr expected = ChannelCopy(rail);
    if (expected == nullptr || context.Channel() != expected) {
        RecordFailure("message on unexpected channel rail " + std::to_string(rail)); return -1;
    }
    if (context.Result() != 0) {
        RecordFailure("incoming context failed: " + std::to_string(context.Result())); return context.Result();
    }
    switch (context.OpCode()) {
        case kOpMatrix: return OnMatrix(rail, context);
        case kOpCaseStart: return OnCaseToken(rail, context, false);
        case kOpCaseReady: return OnCaseToken(rail, context, true);
        case kOpHello: return OnHello(rail, context);
        case kOpCopyReq: return OnCopyReq(rail, context);
        case kOpDataDone: return OnDataDone(rail, context);
        case kOpChunkDone: return OnChunkDone(rail, context);
        case kOpCopyError: return OnCopyError(rail, context);
        case kOpFinish: return OnFinish(rail, context);
        case kOpFinishAck: return OnFinishAck(rail, context);
        default: RecordFailure("unexpected opcode on rail " + std::to_string(rail)); return -1;
    }
}

int SparseCopyBenchmark::OnHello(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    if (mOptions.role != Role::Remote || !mRemoteReady.load(std::memory_order_acquire)) {
        RecordFailure("HELLO before remote setup"); return -1;
    }
    HelloInfo hello{};
    const uint64_t railBytes = static_cast<uint64_t>(mMaxRailBlocks) * kStrideBytes;
    const uint64_t stageBytes = mMaxStageBytes;
    if (!DecodeHello(context.MessageData(), context.MessageDataLen(), hello) ||
        !ValidateHelloMetadata(hello, mParams, rail, railBytes, stageBytes)) {
        RecordFailure("invalid HELLO on rail " + std::to_string(rail)); return -1;
    }
    RailState &state = mRails[rail];
    bool expected = false;
    if (!state.helloClaimed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        RecordFailure("duplicate HELLO"); return -1;
    }
    state.peerDestinationAddress = static_cast<uintptr_t>(hello.destinationAddress);
    state.peerDestinationBytes = hello.destinationBytes;
    state.peerDestinationKey = hello.destinationKey;
    state.peerStageAddress = static_cast<uintptr_t>(hello.stageAddress);
    state.peerStageBytes = hello.stageBytes;
    state.peerStageKey = hello.stageKey;
    ReadyInfo ready{mParams, rail, kSourceRegionId, kStrideBytes, state.buffer.Size()};
    state.readyPayload = EncodeReady(ready);
    Callback *callback = NewSendCallback(rail);
    if (callback == nullptr) { RecordFailure("READY callback allocation failed"); return -1; }
    state.callbackCounters.workerAttemptedSendCallbacks.fetch_add(1, std::memory_order_release);
    // One release publishes peer destination metadata, READY storage, and
    // callback accounting to the remote app before it can process COPY_REQ.
    state.helloSeen.store(true, std::memory_order_release);
    const UBSHcomRequest reply(state.readyPayload.data(), static_cast<uint32_t>(state.readyPayload.size()), kOpReady);
    const int rc = context.Channel()->Reply(UBSHcomReplyContext(context.RspCtx(), 0), reply, callback);
    if (rc != 0) RecordFailure("READY Reply failed: " + std::to_string(rc));
    return rc;
}

int SparseCopyBenchmark::OnMatrix(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    const uint32_t index = mMatrixReceived.load(std::memory_order_acquire);
    if (mOptions.role != Role::Remote || rail != 0 || !AllHellosSeen() || index >= mMatrixItems.size() ||
        context.MessageData() == nullptr || context.MessageDataLen() != kMatrixItemWireBytes ||
        std::memcmp(context.MessageData(), mMatrixItems[index].data(), kMatrixItemWireBytes) != 0) {
        RecordFailure("matrix differs between peers (count/order/parameters)"); return -1;
    }
    Callback *callback = NewSendCallback(0);
    if (callback == nullptr) { RecordFailure("matrix reply allocation failed"); return -1; }
    mRails[0].callbackCounters.workerAttemptedSendCallbacks.fetch_add(1, std::memory_order_release);
    const UBSHcomRequest reply(mMatrixItems[index].data(), kMatrixItemWireBytes, kOpMatrix);
    const int rc = context.Channel()->Reply(UBSHcomReplyContext(context.RspCtx(), 0), reply, callback);
    if (rc != 0) { RecordFailure("matrix Reply failed"); return rc; }
    mMatrixReceived.store(index + 1, std::memory_order_release);
    return 0;
}

int SparseCopyBenchmark::OnCaseToken(uint16_t rail, UBSHcomServiceContext &context, bool ready) noexcept
{
    uint64_t id = 0;
    if (rail != 0 || mOptions.role != (ready ? Role::Local : Role::Remote) ||
        !DecodeTokenForRail(rail, context, ready ? kCaseReadyMagic : kCaseStartMagic,
            ready ? kOpCaseReady : kOpCaseStart, id) || id > mCases.size()) {
        RecordFailure("invalid case transition token"); return -1;
    }
    auto &slot = ready ? mCaseReadyReceived : mCaseStartReceived;
    uint64_t previous = id - 1;
    if (!slot.compare_exchange_strong(previous, id, std::memory_order_release, std::memory_order_relaxed)) {
        RecordFailure("duplicate/out-of-order case transition"); return -1;
    }
    return 0;
}

bool SparseCopyBenchmark::DecodeTokenForRail(uint16_t rail, UBSHcomServiceContext &context, uint32_t magic,
    uint16_t opcode, uint64_t &generation) noexcept
{
    uint16_t wireRail = kMaxLinks;
    return DecodeToken(context.MessageData(), context.MessageDataLen(), magic, opcode, generation, wireRail) &&
        generation != 0 && wireRail == rail;
}

int SparseCopyBenchmark::OnFinish(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    uint64_t generation = 0;
    if (mOptions.role != Role::Remote ||
        !DecodeTokenForRail(rail, context, kFinishMagic, kOpFinish, generation) ||
        generation != mCaseLastGeneration) {
        RecordFailure("invalid FINISH"); return -1;
    }
    uint64_t expected = mCaseFirstGeneration - 1;
    if (!mRails[rail].finishGeneration.compare_exchange_strong(
        expected, generation, std::memory_order_release, std::memory_order_relaxed)) {
        RecordFailure("duplicate FINISH"); return -1;
    }
    return 0;
}

int SparseCopyBenchmark::OnFinishAck(uint16_t rail, UBSHcomServiceContext &context) noexcept
{
    uint64_t generation = 0;
    if (mOptions.role != Role::Local ||
        !DecodeTokenForRail(rail, context, kFinishAckMagic, kOpFinishAck, generation) ||
        generation != mCaseLastGeneration) {
        RecordFailure("invalid FINISH_ACK"); return -1;
    }
    uint64_t expected = mCaseFirstGeneration - 1;
    if (!mRails[rail].finishAckGeneration.compare_exchange_strong(
        expected, generation, std::memory_order_release, std::memory_order_relaxed)) {
        RecordFailure("duplicate FINISH_ACK"); return -1;
    }
    return 0;
}

Callback *SparseCopyBenchmark::NewDataCallback(uint16_t rail, uint64_t generation, uint64_t completionTarget, int chunk)
{
    // Preserve the original closure/callback work outside diagnostic rounds.
    size_t trace = 0;
    if (TraceIndex(generation, trace)) {
        return UBSHcomNewCallback([this, rail, generation, completionTarget, chunk, trace](UBSHcomServiceContext &context) {
            ActiveCallbackGuard guard(mActiveCallbacks);
            ock::hcom::UBSHcomRdmaTraceMark(
                ock::hcom::UBSHcomRdmaTraceKind::DATA_CALLBACK_BEGIN, generation, rail, chunk);
            if (context.Result() != 0)
                RecordFailure("Put callback failed rail " + std::to_string(rail) + ": " +
                    std::to_string(context.Result()));
            const uint64_t completed =
                mRails[rail].callbackCounters.dataDoneCallbacks.fetch_add(1, std::memory_order_acq_rel) + 1;
            if (completed == completionTarget)
                PublishCallbackTrace(generation, mTrace[trace].remoteDataCallbacksDone[rail],
                    "remote_data_callbacks_done");
            ock::hcom::UBSHcomRdmaTraceMark(
                ock::hcom::UBSHcomRdmaTraceKind::DATA_CALLBACK_END, generation, rail, chunk);
        }, std::placeholders::_1);
    }
    return UBSHcomNewCallback([this, rail, generation, completionTarget](UBSHcomServiceContext &context) {
        ActiveCallbackGuard guard(mActiveCallbacks);
        if (context.Result() != 0)
            RecordFailure("Put callback failed rail " + std::to_string(rail) + ": " +
                std::to_string(context.Result()));
        const uint64_t completed =
            mRails[rail].callbackCounters.dataDoneCallbacks.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (completed == completionTarget) {
            size_t i = 0;
            if (TraceIndex(generation, i))
                PublishCallbackTrace(generation, mTrace[i].remoteDataCallbacksDone[rail],
                    "remote_data_callbacks_done");
        }
    }, std::placeholders::_1);
}

Callback *SparseCopyBenchmark::NewSendCallback(uint16_t rail)
{
    return UBSHcomNewCallback([this, rail](UBSHcomServiceContext &context) {
        ActiveCallbackGuard guard(mActiveCallbacks);
        if (context.Result() != 0)
            RecordFailure("Send callback failed rail " + std::to_string(rail) + ": " +
                std::to_string(context.Result()));
        mRails[rail].callbackCounters.sendDoneCallbacks.fetch_add(1, std::memory_order_release);
    }, std::placeholders::_1);
}

bool SparseCopyBenchmark::AllHellosSeen() const noexcept
{
    for (uint16_t rail = 0; rail < mOptions.links; ++rail)
        if (!mRails[rail].helloSeen.load(std::memory_order_acquire)) return false;
    return true;
}

void SparseCopyBenchmark::FinishAllRails()
{
    uint64_t sequence = 0;
    if (mOptions.links == 2) sequence = IssueSecondaryRailCommand(RailCommand::Finish);
    FinishRail(0);
    if (mOptions.links == 2) WaitSecondaryRailCommand(sequence, "rail 1 finish");
}

void SparseCopyBenchmark::FinishRail(uint16_t rail)
{
    RailState &state = mRails[rail];
    if (mOptions.role == Role::Local) {
        state.finishPayload = EncodeToken(kFinishMagic, kOpFinish, mCaseLastGeneration, rail);
        PostAsyncSend(rail, ChannelCopyRequired(rail, "FINISH"), state.finishPayload.data(),
            state.finishPayload.size(), kOpFinish);
        const uint64_t target = ExpectedSendCallbacks(rail);
        WaitControl("FINISH_ACK", [this, rail, target] {
            return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target &&
                mRails[rail].finishAckGeneration.load(std::memory_order_acquire) == mCaseLastGeneration;
        });
    } else {
        WaitControl("FINISH", [this, rail] {
            return mRails[rail].finishGeneration.load(std::memory_order_acquire) == mCaseLastGeneration;
        });
        state.finishAckPayload = EncodeToken(kFinishAckMagic, kOpFinishAck, mCaseLastGeneration, rail);
        PostAsyncSend(rail, ChannelCopyRequired(rail, "FINISH_ACK"), state.finishAckPayload.data(),
            state.finishAckPayload.size(), kOpFinishAck);
        const uint64_t target = ExpectedSendCallbacks(rail);
        WaitControl("FINISH_ACK completion", [this, rail, target] {
            return mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) >= target;
        });
    }
}

void SparseCopyBenchmark::PostAsyncSend(uint16_t rail, const UBSHcomChannelPtr &channel,
    uint8_t *data, size_t size, uint16_t opcode)
{
    Callback *callback = NewSendCallback(rail);
    if (callback == nullptr) throw std::runtime_error("Send callback allocation failed");
    ++mRails[rail].appCounters.attemptedSendCallbacks;
    const int rc = channel->Send(UBSHcomRequest(data, static_cast<uint32_t>(size), opcode), callback);
    if (rc != 0) throw std::runtime_error("Send failed: " + std::to_string(rc));
}

}  // namespace rdma_bench
