// SPDX-License-Identifier: MulanPSL-2.0
#include "support.h"

namespace rdma_bench {

void PinCurrentThread(int cpu)
{
    if (cpu < 0) {
        return;
    }
    if (cpu >= CPU_SETSIZE) {
        throw std::runtime_error("requested CPU exceeds CPU_SETSIZE: " + std::to_string(cpu));
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        throw std::runtime_error("sched_setaffinity failed for CPU " + std::to_string(cpu) + ": " +
            std::strerror(errno));
    }
}

uint64_t PercentileNs(const std::vector<uint64_t> &samples, double percentile)
{
    if (samples.empty()) {
        return 0;
    }
    std::vector<uint64_t> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const size_t index = static_cast<size_t>(percentile * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

double AverageNs(const std::vector<uint64_t> &samples)
{
    if (samples.empty()) {
        return 0.0;
    }
    long double total = 0.0;
    for (const uint64_t sample : samples) {
        total += static_cast<long double>(sample);
    }
    return static_cast<double>(total / static_cast<long double>(samples.size()));
}

double NsToUs(uint64_t nanoseconds)
{
    return static_cast<double>(nanoseconds) / 1000.0;
}

}  // namespace rdma_bench
