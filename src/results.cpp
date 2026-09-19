// SPDX-License-Identifier: MulanPSL-2.0
#include "benchmark.h"

namespace rdma_bench {

bool SparseCopyBenchmark::PublishCallbackTrace(uint64_t generation, TracePoint &point, const char *event) noexcept
{
    size_t index = 0;
    if (!TraceIndex(generation, index)) return true;
    uint64_t timestamp = 0;
    if (!TryNowNs(timestamp)) {
        RecordFailure(std::string("clock_gettime failed while recording ") + event);
        return false;
    }
    point.Publish(timestamp);
    return true;
}

void SparseCopyBenchmark::EmitTracePoint(const char *event, uint64_t generation, int rail, const TracePoint &point,
    int chunk) const
{
    std::cout << "{\"record_type\":\"trace\",\"trace_schema\":\"sparse-copy-v8-group-notify-v1\","
              << "\"host_role\":\"" << RoleName(mOptions.role) << "\",\"case\":\""
              << CaseName(mOptions.mode, mOptions.links, mOptions.sglItems, mOptions.pipeline)
              << "\",\"case_index\":" << mCaseIndex + 1 << ",\"blocks\":" << mParams.blocks
              << ",\"block_bytes\":" << mParams.blockBytes << ",\"notify_every_wrs\":" << mParams.notifyEveryWrs
              << ",\"generation\":" << generation << ",\"rail\":";
    if (rail < 0) std::cout << "null"; else std::cout << rail;
    std::cout << ",\"chunk_id\":";
    if (chunk < 0) std::cout << "null"; else std::cout << chunk;
    std::cout << ",\"event\":\"" << event << "\",\"timestamp_ns\":" << point.Read(event) << "}" << std::endl;
}

void SparseCopyBenchmark::EmitTrace() const
{
    for (uint32_t i = 0; i < mParams.traceRounds; ++i) {
        const TraceRound &t = mTrace[i];
        if (mOptions.role == Role::Local) {
            EmitTracePoint("local_begin", t.generation, -1, t.localBegin);
            EmitTracePoint("local_request_posted", t.generation, 0, t.localRequestPosted);
            for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                if (mOptions.mode == CopyMode::Direct) {
                    EmitTracePoint("local_data_done", t.generation, rail, t.localDataDone[rail]);
                } else {
                    for (uint32_t chunk = 0; chunk < RailChunks(rail); ++chunk) {
                        EmitTracePoint("local_chunk_ready", t.generation, rail,
                            t.localChunkReady[rail][chunk], chunk);
                        EmitTracePoint("local_scatter_begin", t.generation, rail,
                            t.localScatterBegin[rail][chunk], chunk);
                        EmitTracePoint("local_scatter_end", t.generation, rail,
                            t.localScatterEnd[rail][chunk], chunk);
                    }
                }
            }
            EmitTracePoint("local_end", t.generation, -1, t.localEnd);
        } else {
            EmitTracePoint("remote_request_received", t.generation, 0, t.remoteRequestReceived);
            for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
                if (mOptions.mode == CopyMode::Direct) {
                    EmitTracePoint("remote_posted", t.generation, rail, t.remotePosted[rail]);
                    EmitTracePoint("remote_done_posted", t.generation, rail, t.remoteDonePosted[rail]);
                } else {
                    for (uint32_t chunk = 0; chunk < RailChunks(rail); ++chunk) {
                        EmitTracePoint("remote_chunk_posted", t.generation, rail,
                            t.remoteChunkPosted[rail][chunk], chunk);
                        if (chunk % mOptions.notifyEveryWrs == 0)
                            EmitTracePoint("remote_chunk_group_done_posted", t.generation, rail,
                                t.remoteChunkDonePosted[rail][chunk], chunk);
                    }
                }
                EmitTracePoint("remote_data_callbacks_done", t.generation, rail, t.remoteDataCallbacksDone[rail]);
            }
        }
    }
}

std::string SparseCopyBenchmark::FormatLocalResult() const
{
    std::ostringstream out;
    out << std::fixed << std::setprecision(3)
        << "{\"schema_version\":8,\"protocol\":\"sparse-copy-v8-group-notify\""
        << ",\"measurement\":\"local-sparse-copy\",\"role\":\"local\",\"status\":\"ok\",\"case_index\":" << mCaseIndex + 1
        << ",\"case_count\":" << mCases.size() << ",\"case\":\""
        << CaseName(mOptions.mode, mOptions.links, mOptions.sglItems, mOptions.pipeline)
        << "\",\"commit\":\"" << RDMA_600_GIT_COMMIT << "\",\"build_type\":\"" << RDMA_600_BUILD_TYPE
        << "\",\"kind\":\"" << KindName(mOptions.kind) << "\",\"mode\":\""
        << (mOptions.mode == CopyMode::Sgl ? "sgl" : "direct")
        << "\",\"links\":" << mOptions.links << ",\"sgl_items\":" << mOptions.sglItems
        << ",\"notify_every_wrs\":" << mParams.notifyEveryWrs
        << ",\"pipeline\":\"" << (mOptions.pipeline == PipelineMode::On ? "on" : "off")
        << "\",\"blocks\":" << mParams.blocks << ",\"block_bytes\":" << mParams.blockBytes
        << ",\"payload_bytes_per_call\":" << mParams.PayloadBytes()
        << ",\"verify_rounds\":" << mParams.verifyRounds << ",\"warmup_rounds\":" << mParams.warmupRounds
        << ",\"measure_rounds\":" << mParams.measureRounds << ",\"trace_rounds\":" << mParams.traceRounds
        << ",\"first_generation\":" << mCaseFirstGeneration << ",\"last_generation\":" << mCaseLastGeneration
        << ",\"source_format\":\"" << (mOptions.mode == CopyMode::Direct ? "direct-pairs" : "sparse-pairs")
        << "\",\"source_address_count\":" << mParams.blocks
        << ",\"destination_address_count\":" << mParams.blocks
        << ",\"request_descriptor_bytes\":" << mParams.blocks * kCopyEntryWireBytes
        << ",\"request_logical_bytes\":" << mParams.RequestBytes()
        << ",\"request_transport_payload_bytes\":" << mParams.RequestBytes() + mParams.Fragments() * kFragmentHeaderBytes
        << ",\"request_send_calls_per_round\":" << mParams.Fragments()
        << ",\"request_fragment_data_capacity\":" << kFragmentDataBytes
        << ",\"request_service_segment_bytes\":" << kRequestServiceMessageBytes
        << ",\"control_service_segment_bytes\":" << kControlServiceMessageBytes
        << ",\"stage_active_bytes\":" << (mOptions.mode == CopyMode::Sgl ? mParams.PayloadBytes() : 0)
        << ",\"stage_registered_bytes\":" << (mOptions.mode == CopyMode::Sgl ? mMaxStageBytes * mOptions.links : 0)
        << ",\"sparse_registered_bytes\":" << static_cast<uint64_t>(mMaxRailBlocks) * kStrideBytes * mOptions.links
        << ",\"blocks_per_rail\":[";
    for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
        if (rail) out << ',';
        out << mParams.RailBlocks(rail);
    }
    out << "],\"chunks_per_rail\":[";
    for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
        if (rail) out << ',';
        out << RailChunks(rail);
    }
    out << "],\"notifications_per_rail\":[";
    for (uint16_t rail = 0; rail < mOptions.links; ++rail) {
        if (rail) out << ',';
        out << (mOptions.mode == CopyMode::Sgl ? RailNotifications(rail) : 1);
    }
    out << "],\"data_wr_per_round_expected\":" << (mOptions.mode == CopyMode::Sgl ? TotalChunks() : mParams.blocks)
        << ",\"completion_send_wr_per_round_expected\":" << (mOptions.mode == CopyMode::Sgl ? TotalNotifications() : mOptions.links)
        << ",\"round_success_ack_count\":0,\"case_boundary_barrier\":true,\"connections_reused\":true"
        << ",\"data_wait\":\"busy-poll-relax\",\"deadline_check_interval\":256"
        << ",\"callback_allocation\":\"per-request\",\"internal_multirail\":false,\"channel_link_count\":1"
        << ",\"receive_handler_case_lock\":true"
        << ",\"tls_enabled\":false,\"application_submit_threads\":" << mOptions.links
        << ",\"hcom_multiservice_contract\":\""
        << (mOptions.links == 2 ? "diagnostic-unsupported-by-hcom-contract" : "not-applicable")
        << "\",\"compiled_sge_cap\":" << kCompiledSgeMax
        << ",\"linked_library_sge_cap_validation\":\"not-programmatically-verified\""
        << ",\"qp_cap_source\":\"" << (mOptions.qpCapDeclared ? "external-declaration" : "unknown")
        << "\",\"qp_cap_validation\":\"" << (mOptions.mode == CopyMode::Direct ? "not-applicable" :
            (mOptions.qpCapDeclared ? "declared-not-programmatically-verified" : "QP_CAP_PENDING"))
        << "\",\"qp_max_send_sge_declared\":[";
    for (size_t rail = 0; rail < mOptions.qpMaxSendSge.size(); ++rail) {
        if (rail) out << ',';
        out << mOptions.qpMaxSendSge[rail];
    }
    out << "],\"verify_passed\":true,\"per_round_head_tail_generation_check\":true"
        << ",\"source_marker_update_timing\":\"inside-sparse-copy\""
        << ",\"destination_marker_check_timing\":\"outside-sample-inside-wall\""
        << ",\"latency_clock\":\"local-CLOCK_MONOTONIC_RAW\",\"percentile_method\":\"floor(p*(n-1))\""
        << ",\"effective_GBps_basis\":\"effective-bytes/sum-sparse-copy-ns\""
        << ",\"wall_GBps_basis\":\"effective-bytes/measure-loop-wall-ns\""
        << ",\"verbs_trace_status\":\"not-collected\"";
    if (mOptions.kind != RunKind::Measure) {
        out << ",\"sparse_copy_avg_us\":null,\"sparse_copy_p50_us\":null,\"sparse_copy_p95_us\":null,\"sparse_copy_p99_us\":null"
            << ",\"effective_GBps\":null,\"wall_effective_GBps\":null,\"measured_wall_seconds\":null";
    } else {
        const double avg = AverageNs(mSparseCopyNs);
        const double wallNs = static_cast<double>(mMeasureWallEndNs - mMeasureWallStartNs);
        out << ",\"sparse_copy_avg_us\":" << avg / 1000.0
            << ",\"sparse_copy_p50_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.50))
            << ",\"sparse_copy_p95_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.95))
            << ",\"sparse_copy_p99_us\":" << NsToUs(PercentileNs(mSparseCopyNs, 0.99))
            << ",\"effective_GBps\":" << mParams.PayloadBytes() / avg
            << ",\"wall_effective_GBps\":" << mParams.measureRounds * static_cast<double>(mParams.PayloadBytes()) / wallNs
            << ",\"block_Mops\":" << mParams.blocks * 1000.0 / avg
            << ",\"request_payload_GBps\":" << (mParams.RequestBytes() + mParams.Fragments() * kFragmentHeaderBytes) / avg
            << ",\"measured_wall_seconds\":" << std::setprecision(9) << wallNs / 1e9;
    }
    out << '}';
    return out.str();
}

void SparseCopyBenchmark::PrintBatchResult() const
{
    for (const auto &result : mResults) std::cout << result << '\n';
    std::cout << "SUMMARY status=ok cases=" << mSummary.size()
        << " latency=local_sparse_copy_us throughput=decimal_GB/s\n"
        << "# sparse_copy: request preparation through all scatter and request Send callbacks.\n"
        << "# GB/s=effective bytes/sum(latency); wall_GB/s includes per-round marker validation.\n"
        << "case blocks bytes payload_B mode links K notify_every_wrs pipeline verify warmup measure avg_us p50_us p95_us p99_us GB/s wall_GB/s status\n";
    for (size_t index = 0; index < mSummary.size(); ++index) {
        const auto &s = mSummary[index];
        std::cout << std::fixed << std::setprecision(3) << index + 1 << ' ' << s.params.blocks << ' '
            << s.params.blockBytes << ' ' << s.params.PayloadBytes() << ' '
            << (mOptions.mode == CopyMode::Sgl ? "sgl" : "direct") << ' ' << mOptions.links << ' '
            << mOptions.sglItems << ' ' << s.params.notifyEveryWrs << ' '
            << (mOptions.pipeline == PipelineMode::On ? "on" : "off") << ' '
            << s.params.verifyRounds << ' ' << s.params.warmupRounds << ' ' << s.params.measureRounds << ' ';
        if (s.params.measureRounds)
            std::cout << s.avg << ' ' << s.p50 << ' ' << s.p95 << ' ' << s.p99 << ' ' << s.gbps << ' ' << s.wallGbps;
        else std::cout << "- - - - - -";
        std::cout << " ok\n";
    }
    std::cout.flush();
}

void SparseCopyBenchmark::PrintRemoteStatus() const
{
    std::ostringstream out;
    out << "{\"schema_version\":8,\"protocol\":\"sparse-copy-v8-group-notify\",\"case\":\""
        << CaseName(mOptions.mode, mOptions.links, mOptions.sglItems, mOptions.pipeline)
        << "\",\"role\":\"remote\",\"status\":\"ok\",\"commit\":\""
        << RDMA_600_GIT_COMMIT << "\",\"build_type\":\"" << RDMA_600_BUILD_TYPE
        << "\",\"processed_calls\":" << mCaseLastGeneration << ",\"case_count\":" << mCases.size() << ",\"links\":" << mOptions.links
        << ",\"mode\":\"" << (mOptions.mode == CopyMode::Direct ? "direct" : "sgl")
        << "\",\"sgl_items\":" << mOptions.sglItems
        << ",\"notify_every_wrs\":" << mOptions.notifyEveryWrs << ",\"pipeline\":\""
        << (mOptions.pipeline == PipelineMode::On ? "on" : "off")
        << "\",\"hcom_multiservice_contract\":\""
        << (mOptions.links == 2 ? "diagnostic-unsupported-by-hcom-contract" : "not-applicable")
        << "\",\"compiled_sge_cap\":" << kCompiledSgeMax
        << ",\"compiled_sge_cap_source\":\"ubs-comm-public-header\""
        << ",\"linked_library_sge_cap_validation\":\"not-programmatically-verified\""
        << ",\"qp_cap_source\":\""
        << (mOptions.mode == CopyMode::Direct ? "not-applicable" :
            (mOptions.qpCapDeclared ? "external-declaration" : "unknown"))
        << "\",\"qp_cap_validation\":\""
        << (mOptions.mode == CopyMode::Direct ? "not-applicable" :
            (mOptions.qpCapDeclared ? "declared-not-programmatically-verified" : "QP_CAP_PENDING"))
        << "\",\"qp_max_send_sge_declared\":[";
    for (size_t rail = 0; rail < mOptions.qpMaxSendSge.size(); ++rail) {
        if (rail != 0) out << ',';
        out << mOptions.qpMaxSendSge[rail];
    }
    out << "]}";
    std::cout << out.str() << std::endl;
}

}  // namespace rdma_bench
