// SPDX-License-Identifier: MulanPSL-2.0
#include "detailed_trace.h"
#include <cassert>
#include <string>
#include <thread>
#include <vector>
using namespace ock::hcom;
int main(int argc, char **argv)
{
    const bool overflow = argc > 1 && std::string(argv[1]) == "overflow";
    const bool tooManyThreads = argc > 1 && std::string(argv[1]) == "threads";
    const bool missingPosts = argc > 1 && std::string(argv[1]) == "missing-posts";
    rdma_bench::InitializeDetailedTrace(missingPosts ? "remote" : "local", 1, missingPosts);
    rdma_bench::BeginDetailedTrace(1);
    std::vector<std::thread> producers;
    for (int r = 0; r < (tooManyThreads ? 10 : 4); ++r) producers.emplace_back([r, overflow] {
        for (int i = 0; i < (overflow ? 3000 : 2); ++i) {
            UBSHcomRdmaTracePoll(1, 100 + i * 20, 110 + i * 20, r + 1, 1);
            UBSHcomRdmaTraceCqe(0xffff000000000001ULL + i, r + 1, 0, 0);
        }
    });
    for (auto &producer : producers) producer.join();
    rdma_bench::EndDetailedTrace();
    const bool complete = rdma_bench::FinishDetailedTrace();
    assert(complete == (!overflow && !tooManyThreads && !missingPosts));
}
