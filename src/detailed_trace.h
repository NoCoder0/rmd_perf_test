// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include <cstddef>
#include <cstdint>
#include "hcom/hcom_rdma_trace.h"

namespace rdma_bench {
// A process has one benchmark instance. Buffers live until all workers join;
// case boundaries only change the capture epoch, never reset/free live storage.
void InitializeDetailedTrace(const char *role, size_t operations, bool requireSglPosts = false);
void BeginDetailedTrace(uint64_t caseIndex) noexcept;
void EndDetailedTrace() noexcept;
bool FinishDetailedTrace(bool aborted = false);
} // namespace rdma_bench
