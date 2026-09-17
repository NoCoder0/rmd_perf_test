// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include "common.h"

namespace rdma_bench {

inline uint64_t NowNs()
{
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        throw std::runtime_error("clock_gettime(CLOCK_MONOTONIC_RAW) failed");
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

inline bool TryNowNs(uint64_t &value) noexcept
{
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        return false;
    }
    value = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    return true;
}

void PinCurrentThread(int cpu);

inline void CpuRelax() noexcept
{
#if defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

uint64_t PercentileNs(const std::vector<uint64_t> &samples, double percentile);

double AverageNs(const std::vector<uint64_t> &samples);

double NsToUs(uint64_t nanoseconds);

}  // namespace rdma_bench
