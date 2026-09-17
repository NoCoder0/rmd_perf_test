// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include "common.h"

namespace rdma_bench {

std::string RoleName(Role role);

std::string KindName(RunKind kind);

std::string CaseName(uint16_t links);

std::string CaseName(CopyMode mode, uint16_t links, uint16_t sglItems, PipelineMode pipeline);

uint64_t ParseStrictDecimal(const std::string &name, const std::string &value, uint64_t minimum, uint64_t maximum);

uint16_t ResolveSglItems(CopyMode mode, const char *environment);

std::vector<uint32_t> ParseQpCaps(const char *environment, uint16_t links, bool &declared);

void ResolveModeAndPipeline(Options &options, const std::string &mode,
    bool pipelineSpecified, const std::string &pipeline);

void ValidateSglCapability(const Options &options);

uint64_t ParseUnsigned(const std::string &name, const std::string &value, uint64_t maximum);

int ParseSignedCpu(const std::string &name, const std::string &value);

std::vector<std::string> SplitCsv(const std::string &name, const std::string &value);

std::vector<int> ParseCpuCsv(const std::string &name, const std::string &value);

void PrintUsage(std::ostream &stream);

Options ParseOptions(int argc, char **argv);

}  // namespace rdma_bench
