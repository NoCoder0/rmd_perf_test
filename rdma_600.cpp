// SPDX-License-Identifier: MulanPSL-2.0
#include "src/benchmark.h"

int main(int argc, char **argv)
{
    try {
        const rdma_bench::Options options = rdma_bench::ParseOptions(argc, argv);
        auto benchmark = std::make_unique<rdma_bench::SparseCopyBenchmark>(options);
        return benchmark->Run();
    } catch (const std::exception &error) {
        std::cerr << "ERROR: " << error.what() << std::endl;
        return 1;
    }
}
