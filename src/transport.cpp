// SPDX-License-Identifier: MulanPSL-2.0
#include "benchmark.h"

namespace rdma_bench {

void SparseCopyBenchmark::StartSecondaryRailThread()
{
    if (mOptions.links != 2) return;
    mSecondary.thread = std::thread([this] { SecondaryRailThreadMain(); });
    WaitControl("secondary rail thread startup",
        [this] { return mSecondary.started.load(std::memory_order_acquire); });
}

void SparseCopyBenchmark::SecondaryRailThreadMain() noexcept
{
    try {
        PinCurrentThread(mOptions.appCpus[1]);
        mSecondary.started.store(true, std::memory_order_release);
        uint64_t seen = 0;
        while (!mSecondary.stop.load(std::memory_order_acquire)) {
            const uint64_t issued = mSecondary.issued.load(std::memory_order_acquire);
            if (issued == seen) { CpuRelax(); continue; }
            const RailCommand command = mSecondary.command.load(std::memory_order_relaxed);
            const uint64_t generation = mSecondary.generation.load(std::memory_order_relaxed);
            const uint64_t deadlineNs = mSecondary.deadlineNs.load(std::memory_order_relaxed);
            try {
                switch (command) {
                    case RailCommand::Setup: SetupRail(1); break;
                    case RailCommand::ConnectAndHandshake: ConnectAndHandshakeRail(1); break;
                    case RailCommand::ProcessRemoteRound: ProcessRemoteRail(1, generation, deadlineNs); break;
                    case RailCommand::Finish: FinishRail(1); break;
                    case RailCommand::Teardown: TeardownRail(1); break;
                    case RailCommand::None: throw std::runtime_error("empty secondary rail command");
                }
            } catch (const std::exception &error) {
                RecordFailure(std::string("rail 1 application thread: ") + error.what());
            } catch (...) {
                RecordFailure("rail 1 application thread: unknown exception");
            }
            seen = issued;
            mSecondary.completed.store(seen, std::memory_order_release);
        }
    } catch (const std::exception &error) {
        mSecondary.started.store(true, std::memory_order_release);
        RecordFailure(std::string("rail 1 startup: ") + error.what());
    }
}

uint64_t SparseCopyBenchmark::IssueSecondaryRailCommand(RailCommand command, uint64_t generation, uint64_t deadlineNs)
{
    if (mOptions.links != 2 || !mSecondary.thread.joinable())
        throw std::runtime_error("secondary rail thread is unavailable");
    const uint64_t previous = mSecondary.issued.load(std::memory_order_relaxed);
    if (mSecondary.completed.load(std::memory_order_acquire) != previous)
        throw std::runtime_error("secondary rail already has an outstanding command");
    mSecondary.generation.store(generation, std::memory_order_relaxed);
    mSecondary.deadlineNs.store(deadlineNs, std::memory_order_relaxed);
    mSecondary.command.store(command, std::memory_order_relaxed);
    mSecondary.issued.store(previous + 1, std::memory_order_release);
    return previous + 1;
}

void SparseCopyBenchmark::WaitSecondaryRailCommand(uint64_t sequence, const char *what)
{
    WaitData(what, [this, sequence] {
        return mSecondary.completed.load(std::memory_order_acquire) >= sequence;
    });
}

bool SparseCopyBenchmark::WaitSecondaryNoThrow(uint64_t sequence) noexcept
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
    while (mSecondary.completed.load(std::memory_order_acquire) < sequence &&
        std::chrono::steady_clock::now() < deadline) CpuRelax();
    return mSecondary.completed.load(std::memory_order_acquire) >= sequence;
}

bool SparseCopyBenchmark::QuiesceSecondaryNoThrow() noexcept
{
    if (mOptions.links != 2 || !mSecondary.thread.joinable()) return true;
    return WaitSecondaryNoThrow(mSecondary.issued.load(std::memory_order_acquire));
}

void SparseCopyBenchmark::StopSecondaryRailThread() noexcept
{
    if (!mSecondary.thread.joinable()) return;
    mSecondary.stop.store(true, std::memory_order_release);
    mSecondary.thread.join();
}

void SparseCopyBenchmark::SetupFixedRails()
{
    if (mOptions.links == 2) {
        const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::Setup);
        WaitSecondaryRailCommand(sequence, "rail 1 setup");
    }
    SetupRail(0);
}

void SparseCopyBenchmark::SetupRail(uint16_t rail)
{
    RailState &state = mRails[rail];
    state.serviceName = "rdma600_" + RoleName(mOptions.role) + "_" + std::to_string(rail);
    UBSHcomServiceOptions options{};
    options.maxSendRecvDataSize = 16384;
    options.workerGroupThreadCount = 1;
    options.workerGroupMode = ock::hcom::NET_BUSY_POLLING;
    if (mOptions.workerCpus[rail] >= 0) {
        const auto cpu = static_cast<uint32_t>(mOptions.workerCpus[rail]);
        options.workerGroupCpuIdsRange = {cpu, cpu};
    }
    state.service = UBSHcomService::Create(UBSHcomServiceProtocol::RDMA, state.serviceName, options);
    if (state.service == nullptr)
        throw std::runtime_error("Create RDMA service failed on rail " + std::to_string(rail));
    UBSHcomTlsOptions tls{};
    tls.enableTls = false;
    state.service->SetTlsOptions(tls);
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
        ActiveCallbackGuard guard(mActiveCallbacks); return 0;
    });
    state.service->RegisterOneSideHandler([this](const UBSHcomServiceContext &) {
        ActiveCallbackGuard guard(mActiveCallbacks); return 0;
    });
    state.service->RegisterChannelBrokenHandler([this, rail](const UBSHcomChannelPtr &) {
        ActiveCallbackGuard guard(mActiveCallbacks);
        const uint64_t finish = mOptions.role == Role::Local ?
            mRails[rail].finishAckGeneration.load(std::memory_order_acquire) :
            mRails[rail].finishGeneration.load(std::memory_order_acquire);
        // Once this rail's final exit token is received, peer disconnect is
        // expected; local outstanding callback errors/drain still fail the run.
        if (!mTearingDown.load(std::memory_order_acquire) && finish != mTotalGenerations)
            RecordFailure("hcom channel broken on rail " + std::to_string(rail));
    }, UBSHcomChannelBrokenPolicy::BROKEN_ALL);
    if (mOptions.role == Role::Remote) {
        const int rc = state.service->Bind("tcp://" + mOptions.endpoints[rail],
            [this, rail](const std::string &, const UBSHcomChannelPtr &channel, const std::string &) {
                return OnNewChannel(rail, channel);
            });
        RequireOk(rc, ("Bind rail " + std::to_string(rail)).c_str());
    }
    RequireOk(state.service->Start(), ("Start rail " + std::to_string(rail)).c_str());

    const size_t railBytes = static_cast<size_t>(mMaxRailBlocks) * kStrideBytes;
    state.buffer.Allocate(railBytes);
    std::memset(state.buffer.Data(),
        mOptions.role == Role::Remote ? kSourceGapSentinel : kDstGapSentinel, state.buffer.Size());
    RequireOk(state.service->RegisterMemoryRegion(
        reinterpret_cast<uintptr_t>(state.buffer.Data()), state.buffer.Size(), state.memoryRegion),
        ("RegisterMemoryRegion rail " + std::to_string(rail)).c_str());
    state.memoryRegistered = true;
    state.memoryKey = {};
    state.memoryRegion.GetMemoryKey(state.memoryKey);
    if (state.memoryRegion.GetAddress() != reinterpret_cast<uintptr_t>(state.buffer.Data()) ||
        state.memoryRegion.GetSize() < state.buffer.Size())
        throw std::runtime_error("MR does not cover rail " + std::to_string(rail));

    if (mOptions.role == Role::Local && mOptions.mode == CopyMode::Sgl) {
        const size_t stageBytes = mMaxStageBytes;
        state.stageBuffer.Allocate(stageBytes);
        std::memset(state.stageBuffer.Data(), 0, state.stageBuffer.Size());
        RequireOk(state.service->RegisterMemoryRegion(reinterpret_cast<uintptr_t>(state.stageBuffer.Data()),
            state.stageBuffer.Size(), state.stageMemoryRegion),
            ("Register stage memory region rail " + std::to_string(rail)).c_str());
        state.stageMemoryRegistered = true;
        state.stageMemoryKey = {};
        state.stageMemoryRegion.GetMemoryKey(state.stageMemoryKey);
        if (state.stageMemoryRegion.GetAddress() != reinterpret_cast<uintptr_t>(state.stageBuffer.Data()) ||
            state.stageMemoryRegion.GetSize() < state.stageBuffer.Size())
            throw std::runtime_error("stage MR does not cover rail " + std::to_string(rail));
    }
}

void SparseCopyBenchmark::PrintListening() const
{
    std::ostringstream out;
    out << "LISTENING role=remote links=" << mOptions.links;
    for (uint16_t rail = 0; rail < mOptions.links; ++rail)
        out << " rail" << rail << "=" << mOptions.endpoints[rail] << "/" << mOptions.rdmaIps[rail];
    std::cout << out.str() << std::endl;
    std::cout.flush();
}

int SparseCopyBenchmark::OnNewChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept
{
    ActiveCallbackGuard guard(mActiveCallbacks);
    if (mOptions.role != Role::Remote || rail >= mOptions.links || channel == nullptr) {
        RecordFailure("unexpected new channel"); return -1;
    }
    if (!ConfigureChannel(rail, channel)) return -1;
    {
        std::lock_guard<std::mutex> lock(mChannelsMutex);
        if (mRails[rail].channel != nullptr) {
            RecordFailure("duplicate channel on rail " + std::to_string(rail)); return -1;
        }
        mRails[rail].channel = channel;
    }
    mChannelCv.notify_all();
    return 0;
}

bool SparseCopyBenchmark::ConfigureChannel(uint16_t rail, const UBSHcomChannelPtr &channel) noexcept
{
    try {
        channel->SetChannelTimeOut(static_cast<int16_t>(mOptions.timeoutSec),
            static_cast<int16_t>(mOptions.timeoutSec));
        UBSHcomTwoSideThreshold thresholds{};
        thresholds.splitThreshold = UINT32_MAX;
        thresholds.rndvThreshold = UINT32_MAX;
        const int rc = channel->SetTwoSideThreshold(thresholds);
        if (rc != 0) {
            RecordFailure("SetTwoSideThreshold failed on rail " + std::to_string(rail) +
                ": " + std::to_string(rc));
            return false;
        }
        return true;
    } catch (const std::exception &error) {
        RecordFailure("ConfigureChannel failed on rail " + std::to_string(rail) + ": " + error.what());
        return false;
    }
}

void SparseCopyBenchmark::ConnectLocal()
{
    if (mOptions.links == 1) { ConnectAndHandshakeRail(0); return; }
    const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::ConnectAndHandshake);
    ConnectAndHandshakeRail(0);
    WaitSecondaryRailCommand(sequence, "rail 1 connect and handshake");
}

void SparseCopyBenchmark::ConnectAndHandshakeRail(uint16_t rail)
{
    UBSHcomConnectOptions options{};
    options.linkCount = 1;
    options.mode = UBSHcomClientPollingMode::WORKER_POLL;
    options.cbType = UBSHcomChannelCallBackType::CHANNEL_FUNC_CB;
    UBSHcomChannelPtr channel;
    RequireOk(mRails[rail].service->Connect("tcp://" + mOptions.endpoints[rail], channel, options),
        ("Connect rail " + std::to_string(rail)).c_str());
    if (channel == nullptr) throw std::runtime_error("Connect returned null channel");
    if (!ConfigureChannel(rail, channel)) throw std::runtime_error("ConfigureChannel after Connect failed");
    {
        std::lock_guard<std::mutex> lock(mChannelsMutex);
        mRails[rail].channel = channel;
    }
    RailState &state = mRails[rail];
    HelloInfo hello{};
    hello.params = mParams;
    hello.rail = rail;
    hello.destinationRegionId = kDestinationRegionId;
    hello.destinationAddress = reinterpret_cast<uintptr_t>(state.buffer.Data());
    hello.destinationBytes = state.buffer.Size();
    hello.destinationKey = state.memoryKey;
    if (mOptions.mode == CopyMode::Sgl) {
        hello.stageRegionId = kStageRegionId;
        hello.stageAddress = reinterpret_cast<uintptr_t>(state.stageBuffer.Data());
        hello.stageBytes = state.stageBuffer.Size();
        hello.stageKey = state.stageMemoryKey;
    }
    state.helloPayload = EncodeHello(hello);
    UBSHcomRequest request(state.helloPayload.data(), static_cast<uint32_t>(state.helloPayload.size()), kOpHello);
    UBSHcomResponse response(state.readyResponse.data(), static_cast<uint32_t>(state.readyResponse.size()));
    RequireOk(channel->Call(request, response, nullptr), ("HELLO rail " + std::to_string(rail)).c_str());
    ReadyInfo ready{};
    const uint64_t railBytes = static_cast<uint64_t>(mMaxRailBlocks) * kStrideBytes;
    if (!DecodeReady(response.address, response.size, ready) || !SameParams(mParams, ready.params) ||
        ready.rail != rail || ready.sourceRegionId != kSourceRegionId ||
        ready.sourceAlignment != kStrideBytes || ready.sourceBytes != railBytes)
        throw std::runtime_error("invalid READY on rail " + std::to_string(rail));
    state.peerSourceBytes = ready.sourceBytes;
}

void SparseCopyBenchmark::WaitForChannels()
{
    std::unique_lock<std::mutex> lock(mChannelsMutex);
    const bool ok = mChannelCv.wait_for(lock, std::chrono::seconds(mOptions.timeoutSec), [this] {
        if (mFatal.load(std::memory_order_acquire)) return true;
        for (uint16_t rail = 0; rail < mOptions.links; ++rail)
            if (mRails[rail].channel == nullptr) return false;
        return true;
    });
    if (!ok) throw std::runtime_error("timed out waiting for channels");
    CheckFatal("peer channels");
}

UBSHcomChannelPtr SparseCopyBenchmark::ChannelCopy(uint16_t rail) const
{
    std::lock_guard<std::mutex> lock(mChannelsMutex); return mRails[rail].channel;
}

UBSHcomChannelPtr SparseCopyBenchmark::ChannelCopyRequired(uint16_t rail, const char *operation) const
{
    UBSHcomChannelPtr channel = ChannelCopy(rail);
    if (channel == nullptr)
        throw std::runtime_error(std::string(operation) + " without channel rail " + std::to_string(rail));
    return channel;
}

void SparseCopyBenchmark::RequireOk(int rc, const char *operation)
{
    if (rc != 0) throw std::runtime_error(std::string(operation) + " failed: " + std::to_string(rc));
}

bool SparseCopyBenchmark::CallbacksDrained() const noexcept
{
    for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
        if (mRails[rail].callbackCounters.dataDoneCallbacks.load(std::memory_order_acquire) <
                mRails[rail].appCounters.attemptedDataCallbacks ||
            mRails[rail].callbackCounters.sendDoneCallbacks.load(std::memory_order_acquire) <
                ExpectedSendCallbacks(rail)) return false;
    }
    return mActiveCallbacks.load(std::memory_order_acquire) == 0;
}

bool SparseCopyBenchmark::DrainUntilComplete() noexcept
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(mOptions.timeoutSec);
    while (std::chrono::steady_clock::now() < deadline) {
        if (CallbacksDrained()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return CallbacksDrained();
}

void SparseCopyBenchmark::TeardownFixedRails()
{
    if (mOptions.links == 2) {
        const uint64_t sequence = IssueSecondaryRailCommand(RailCommand::Teardown);
        WaitSecondaryRailCommand(sequence, "rail 1 teardown");
    }
    TeardownRail(0);
}

void SparseCopyBenchmark::TryTeardownFixedRails() noexcept
{
    mTearingDown.store(true, std::memory_order_release);
    if (mOptions.links == 2 && mSecondary.thread.joinable()) {
        const uint64_t issued = mSecondary.issued.load(std::memory_order_acquire);
        if (WaitSecondaryNoThrow(issued)) {
            try {
                const uint64_t teardown = IssueSecondaryRailCommand(RailCommand::Teardown);
                (void)WaitSecondaryNoThrow(teardown);
            } catch (...) {}
        }
    }
    TeardownRail(0);
}

void SparseCopyBenchmark::TeardownRail(uint16_t rail) noexcept
{
    mTearingDown.store(true, std::memory_order_release);
    RailState &state = mRails[rail];
    UBSHcomChannelPtr channel;
    { std::lock_guard<std::mutex> lock(mChannelsMutex); channel = state.channel; state.channel.Set(nullptr); }
    if (state.service != nullptr && channel != nullptr) state.service->Disconnect(channel);
    if (state.service != nullptr && state.stageMemoryRegistered) {
        state.service->DestroyMemoryRegion(state.stageMemoryRegion); state.stageMemoryRegistered = false;
    }
    if (state.service != nullptr && state.memoryRegistered) {
        state.service->DestroyMemoryRegion(state.memoryRegion); state.memoryRegistered = false;
    }
    if (state.service != nullptr) {
        UBSHcomService::Destroy(state.serviceName); state.service = nullptr;
    }
}

}  // namespace rdma_bench
