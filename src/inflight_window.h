// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include <cstdint>

namespace rdma_bench {

// Cumulative counters include prior rounds. Limit data PutV requests per rail,
// not notification SENDs or hardware WRs (one PutV may be split by HCOM).
inline bool HasSglWindowCredit(uint64_t submitted, uint64_t completed, uint32_t limit) noexcept
{
    return limit == 0 || completed >= submitted || submitted - completed < limit;
}

} // namespace rdma_bench
