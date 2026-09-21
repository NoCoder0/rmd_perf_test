// SPDX-License-Identifier: MulanPSL-2.0
#include "config.h"

namespace rdma_bench {

std::string RoleName(Role role)
{
    return role == Role::Local ? "local" : "remote";
}

std::string KindName(RunKind kind)
{
    switch (kind) {
        case RunKind::Verify:
            return "verify";
        case RunKind::Measure:
            return "measure";
        case RunKind::Trace:
            return "trace";
    }
    return "unknown";
}

std::string CaseName(uint16_t links)
{
    return links == 1 ? "B1" : "B2";
}

std::string CaseName(CopyMode mode, uint16_t links, uint16_t sglItems, PipelineMode pipeline)
{
    if (mode == CopyMode::Direct) return CaseName(links);
    return std::string(links == 1 ? "S1-" : "S2-") + std::to_string(sglItems) + "-" +
        (pipeline == PipelineMode::On ? "on" : "off");
}

uint64_t ParseStrictDecimal(const std::string &name, const std::string &value, uint64_t minimum, uint64_t maximum)
{
    if (value.empty() || std::any_of(value.begin(), value.end(), [](unsigned char c) { return c < '0' || c > '9'; })) {
        throw std::runtime_error("invalid " + name + ": " + value);
    }
    uint64_t result = 0;
    for (const char c : value) {
        const uint64_t digit = static_cast<uint64_t>(c - '0');
        if (result > maximum / 10U || (result == maximum / 10U && digit > maximum % 10U))
            throw std::runtime_error("invalid " + name + ": " + value);
        result = result * 10U + digit;
    }
    if (result < minimum || result > maximum) throw std::runtime_error("invalid " + name + ": " + value);
    return result;
}

uint16_t ResolveSglItems(CopyMode mode, const char *environment)
{
    if (mode == CopyMode::Direct) return 0;
    const std::string value = environment == nullptr ? std::to_string(kDefaultSglItems) : std::string(environment);
    return static_cast<uint16_t>(ParseStrictDecimal("RDMA_600_SGL_ITEMS", value, 1, kDesignMaxSglItems));
}

std::vector<uint32_t> ParseQpCaps(const char *environment, uint16_t links, bool &declared)
{
    declared = environment != nullptr;
    if (!declared) return {};
    const std::vector<std::string> fields = SplitCsv("RDMA_600_QP_MAX_SEND_SGE", environment);
    if (fields.size() != links) {
        throw std::runtime_error("RDMA_600_QP_MAX_SEND_SGE item count must equal --links");
    }
    std::vector<uint32_t> caps;
    for (const std::string &field : fields) {
        caps.push_back(static_cast<uint32_t>(ParseStrictDecimal("RDMA_600_QP_MAX_SEND_SGE", field, 1,
            std::numeric_limits<uint32_t>::max())));
    }
    return caps;
}

void ResolveModeAndPipeline(Options &options, const std::string &mode,
    bool pipelineSpecified, const std::string &pipeline)
{
    if (mode == "direct") {
        if (pipelineSpecified) throw std::runtime_error("--pipeline is only applicable to --mode sgl");
        options.mode = CopyMode::Direct;
        options.pipeline = PipelineMode::Off;
        return;
    }
    if (mode != "sgl") throw std::runtime_error("--mode must be direct or sgl");
    options.mode = CopyMode::Sgl;
    if (pipeline == "on") options.pipeline = PipelineMode::On;
    else if (pipeline == "off") options.pipeline = PipelineMode::Off;
    else throw std::runtime_error("--pipeline must be on or off");
}

void ValidateSglCapability(const Options &options)
{
    if (options.mode == CopyMode::Direct) return;
    if (options.sglItems == 0 || options.sglItems > kCompiledSgeMax) {
        throw std::runtime_error("UNSUPPORTED: requested SGL K=" + std::to_string(options.sglItems) +
            " exceeds compiled NET_SGE_MAX_IOV=" + std::to_string(kCompiledSgeMax));
    }
    if (options.qpCapDeclared) {
        if (options.qpMaxSendSge.size() != options.links)
            throw std::runtime_error("QP cap declaration count does not equal --links");
        for (uint16_t rail = 0; rail < options.links; ++rail)
            if (options.qpMaxSendSge[rail] < options.sglItems)
                throw std::runtime_error("UNSUPPORTED: declared QP max_send_sge on rail " +
                    std::to_string(rail) + " is below requested SGL K");
    } else if (options.kind == RunKind::Measure) {
        throw std::runtime_error(
            "SGL measure requires RDMA_600_QP_MAX_SEND_SGE from a deployment-time real-QP query");
    }
}

uint64_t ParseUnsigned(const std::string &name, const std::string &value, uint64_t maximum)
{
    if (value.empty()) {
        throw std::runtime_error(name + " cannot be empty");
    }
    char *end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0' || parsed > maximum) {
        throw std::runtime_error("invalid " + name + ": " + value);
    }
    return parsed;
}

int ParseSignedCpu(const std::string &name, const std::string &value)
{
    if (value == "-1") {
        return -1;
    }
    return static_cast<int>(ParseUnsigned(name, value, static_cast<uint64_t>(std::numeric_limits<int>::max())));
}

std::vector<std::string> SplitCsv(const std::string &name, const std::string &value)
{
    std::vector<std::string> result;
    size_t start = 0;
    while (start <= value.size()) {
        const size_t comma = value.find(',', start);
        const std::string item = value.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (item.empty()) {
            throw std::runtime_error(name + " contains an empty item");
        }
        result.push_back(item);
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return result;
}

std::vector<int> ParseCpuCsv(const std::string &name, const std::string &value)
{
    std::vector<int> cpus;
    for (const std::string &item : SplitCsv(name, value)) {
        cpus.push_back(ParseSignedCpu(name, item));
    }
    return cpus;
}

void PrintUsage(std::ostream &stream)
{
    stream << "Usage:\n"
           << "  rdma_600 --role remote --rdma-ips <ip0[,ip1]> --listen <ep0[,ep1]> [options]\n"
           << "  rdma_600 --role local --rdma-ips <ip0[,ip1]> --peer <ep0[,ep1]> [options]\n\n"
           << "Modes: --mode direct (B1/B2), or --mode sgl --pipeline on|off (S1/S2).\n"
           << "SGL K comes from RDMA_600_SGL_ITEMS (default 16, design range 1..30).\n"
           << "SGL measure also requires RDMA_600_QP_MAX_SEND_SGE=<cap0[,cap1]>.\n"
           << "SGL: --notify-every-wrs G (default 1, range 1..9600), per rail; flush the tail group.\n"
           << "Remote SGL: --max-inflight N (default 0=unlimited, range 0..9600), data PutV requests per rail.\n"
           << "Payload: --memory-backend aligned|hugetlb (default aligned; Linux hugetlb has no fallback).\n"
           << "         --hugepage-kb N (hugetlb only; default from /proc/meminfo; power-of-two KiB).\n"
           << "Source: --source-update static|markers (default markers; static freezes after verify rounds).\n"
           << "SGL scatter: one persistent app thread per rail, pinned by --app-cpus in rail order.\n"
           << "Singular --rdma-ip/--app-cpu/--worker-cpu remain aliases for links=1.\n"
           << "Options: --kind verify|measure|trace --verify-rounds N --warmup N --rounds N\n"
           << "         --trace-rounds N (1..64 for trace) --timeout-sec N\n"
           << "         --app-cpus <cpu0[,cpu1]> --worker-cpus <cpu0[,cpu1]>\n"
           << "         --blocks N[,N...] OR --block-start 100 --block-end 9600 --block-step 100\n"
           << "         --block-bytes 1024,656 (default; one length selects a single scenario)\n"
           << "Default blocks: 100,200,400,600,800,1200,1600,2400,3200,4800,6400,9600 (24 cases with both lengths).\n"
           << "Default: complete count list for each length; per case verify=20 warmup=100 measure=1000.\n"
           << "Single case: --blocks 600 --block-bytes 1024. Connections/MRs reused across cases.\n"
           << "B2 uses two persistent rail-affine application threads; each app CPU must be distinct.\n";
}

Options ParseOptions(int argc, char **argv)
{
    std::map<std::string, std::string> values;
    for (int index = 1; index < argc; ++index) {
        const std::string argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            PrintUsage(std::cout);
            std::exit(0);
        }
        if (argument.rfind("--", 0) != 0 || index + 1 >= argc) {
            throw std::runtime_error("expected an option and value; use --help");
        }
        const std::string value(argv[++index]);
        if (!values.emplace(argument, value).second) {
            throw std::runtime_error("duplicate option: " + argument);
        }
    }

    Options options;
    static const std::array<std::string, 28> kAllowedOptions = {
        "--role", "--rdma-ip", "--rdma-ips", "--listen", "--peer", "--kind", "--verify-rounds", "--warmup",
        "--rounds", "--trace-rounds", "--timeout-sec", "--app-cpu", "--app-cpus", "--worker-cpu",
        "--worker-cpus", "--links", "--mode", "--pipeline", "--blocks", "--block-bytes",
        "--block-start", "--block-end", "--block-step", "--notify-every-wrs", "--max-inflight",
        "--memory-backend", "--hugepage-kb", "--source-update"};
    for (const auto &entry : values) {
        if (std::find(kAllowedOptions.begin(), kAllowedOptions.end(), entry.first) == kAllowedOptions.end()) {
            throw std::runtime_error("unknown option: " + entry.first);
        }
    }

    const auto required = [&values](const std::string &name) -> const std::string & {
        const auto it = values.find(name);
        if (it == values.end() || it->second.empty()) {
            throw std::runtime_error("missing required option " + name);
        }
        return it->second;
    };
    const auto optional = [&values](const std::string &name, const std::string &fallback) {
        const auto it = values.find(name);
        return it == values.end() ? fallback : it->second;
    };

    options.sourceUpdate = optional("--source-update", "markers");
    if (options.sourceUpdate != "static" && options.sourceUpdate != "markers")
        throw std::runtime_error("--source-update must be static or markers");
    const std::string backend = optional("--memory-backend", "aligned");
    if (backend == "hugetlb") options.memoryBackend = MemoryBackend::Hugetlb;
    else if (backend != "aligned") throw std::runtime_error("--memory-backend must be aligned or hugetlb");
    if (values.count("--hugepage-kb")) {
        if (options.memoryBackend != MemoryBackend::Hugetlb)
            throw std::runtime_error("--hugepage-kb requires --memory-backend hugetlb");
        const auto kb = ParseStrictDecimal("--hugepage-kb", values.at("--hugepage-kb"), 1,
            std::numeric_limits<size_t>::max() / 1024);
        if (kb & (kb - 1)) throw std::runtime_error("--hugepage-kb must be a power of two");
        options.hugePageBytes = static_cast<size_t>(kb) * 1024;
    }

    options.links = static_cast<uint16_t>(ParseUnsigned("--links", optional("--links", "1"), kMaxLinks));
    if (options.links == 0) {
        throw std::runtime_error("--links must be 1 or 2");
    }
    if (values.count("--blocks")) {
        if (values.count("--block-start") || values.count("--block-end") || values.count("--block-step"))
            throw std::runtime_error("--blocks cannot be combined with block range options");
        for (const auto &item : SplitCsv("--blocks", values.at("--blocks")))
            options.blockCounts.push_back(static_cast<uint32_t>(ParseStrictDecimal("--blocks", item, 100, kMaxBlocks)));
    } else if (values.count("--block-start") || values.count("--block-end") || values.count("--block-step")) {
        const uint32_t first = static_cast<uint32_t>(ParseStrictDecimal("--block-start", optional("--block-start", "100"), 100, kMaxBlocks));
        const uint32_t last = static_cast<uint32_t>(ParseStrictDecimal("--block-end", optional("--block-end", "9600"), 100, kMaxBlocks));
        const uint32_t step = static_cast<uint32_t>(ParseStrictDecimal("--block-step", optional("--block-step", "100"), 1, kMaxBlocks));
        if (last < first) throw std::runtime_error("--block-end must be >= --block-start");
        for (uint32_t count = first; count <= last; count += step) options.blockCounts.push_back(count);
    } else {
        options.blockCounts = {100, 200, 400, 600, 800, 1200, 1600, 2400, 3200, 4800, 6400, 9600};
    }
    for (const auto &item : SplitCsv("--block-bytes", optional("--block-bytes", "1024,656"))) {
        const auto bytes = static_cast<uint32_t>(ParseStrictDecimal("--block-bytes", item, 656, 1024));
        if (bytes != 656 && bytes != 1024) throw std::runtime_error("--block-bytes accepts only 1024 and 656");
        options.blockLengths.push_back(bytes);
    }
    for (auto list : {options.blockCounts, options.blockLengths}) {
        std::sort(list.begin(), list.end());
        if (std::adjacent_find(list.begin(), list.end()) != list.end())
            throw std::runtime_error("duplicate block count/length in matrix");
    }
    ResolveModeAndPipeline(options, optional("--mode", "direct"), values.count("--pipeline") != 0,
        optional("--pipeline", "on"));
    options.sglItems = ResolveSglItems(options.mode, std::getenv("RDMA_600_SGL_ITEMS"));
    if (options.mode == CopyMode::Direct && values.count("--notify-every-wrs"))
        throw std::runtime_error("--notify-every-wrs is only applicable to --mode sgl");
    if (options.mode == CopyMode::Sgl) {
        options.notifyEveryWrs = static_cast<uint32_t>(ParseStrictDecimal(
            "--notify-every-wrs", optional("--notify-every-wrs", "1"), 1, kMaxBlocks));
        options.qpMaxSendSge = ParseQpCaps(std::getenv("RDMA_600_QP_MAX_SEND_SGE"), options.links,
            options.qpCapDeclared);
    }

    const std::string role = required("--role");
    if (role == "local") {
        options.role = Role::Local;
        options.endpoints = SplitCsv("--peer", required("--peer"));
        if (values.count("--listen") != 0) {
            throw std::runtime_error("--listen is only valid for remote");
        }
    } else if (role == "remote") {
        options.role = Role::Remote;
        options.endpoints = SplitCsv("--listen", required("--listen"));
        if (values.count("--peer") != 0) {
            throw std::runtime_error("--peer is only valid for local");
        }
    } else {
        throw std::runtime_error("--role must be local or remote");
    }

    if (values.count("--max-inflight")) {
        if (options.role != Role::Remote || options.mode != CopyMode::Sgl)
            throw std::runtime_error("--max-inflight is only applicable to --role remote --mode sgl");
        options.maxInflight = static_cast<uint32_t>(ParseStrictDecimal(
            "--max-inflight", values.at("--max-inflight"), 0, kMaxBlocks));
    }

    if (values.count("--rdma-ip") != 0 && values.count("--rdma-ips") != 0) {
        throw std::runtime_error("use only one of --rdma-ip and --rdma-ips");
    }
    options.rdmaIps = SplitCsv("--rdma-ips",
        values.count("--rdma-ips") != 0 ? values.at("--rdma-ips") : required("--rdma-ip"));

    if (values.count("--worker-cpu") != 0 && values.count("--worker-cpus") != 0) {
        throw std::runtime_error("use only one of --worker-cpu and --worker-cpus");
    }
    if (values.count("--worker-cpus") != 0) {
        options.workerCpus = ParseCpuCsv("--worker-cpus", values.at("--worker-cpus"));
    } else {
        options.workerCpus = {ParseSignedCpu("--worker-cpu", optional("--worker-cpu", "-1"))};
    }

    if (values.count("--app-cpu") != 0 && values.count("--app-cpus") != 0) {
        throw std::runtime_error("use only one of --app-cpu and --app-cpus");
    }
    if (values.count("--app-cpus") != 0) {
        options.appCpus = ParseCpuCsv("--app-cpus", values.at("--app-cpus"));
    } else if (options.links == 1) {
        options.appCpus = {ParseSignedCpu("--app-cpu", optional("--app-cpu", "-1"))};
    } else if (values.count("--app-cpu") != 0) {
        throw std::runtime_error("B2 requires --app-cpus with one fixed application CPU per rail");
    } else {
        options.appCpus.assign(options.links, -1);
    }

    if (options.rdmaIps.size() != options.links || options.endpoints.size() != options.links ||
        options.appCpus.size() != options.links || options.workerCpus.size() != options.links) {
        throw std::runtime_error("RDMA IP, OOB endpoint, app CPU, and worker CPU counts must all equal --links");
    }
    if (options.links == 2 && (options.rdmaIps[0] == options.rdmaIps[1] ||
            options.endpoints[0] == options.endpoints[1] || options.appCpus[0] == options.appCpus[1] ||
            options.workerCpus[0] == options.workerCpus[1])) {
        throw std::runtime_error("B2 requires two distinct RDMA IPs, OOB endpoints, app CPUs, and worker CPUs");
    }

    const std::string kind = optional("--kind", "measure");
    if (kind == "verify") {
        options.kind = RunKind::Verify;
    } else if (kind == "measure") {
        options.kind = RunKind::Measure;
    } else if (kind == "trace") {
        options.kind = RunKind::Trace;
    } else {
        throw std::runtime_error("--kind must be verify, measure, or trace");
    }
    options.verifyRounds = static_cast<uint32_t>(ParseUnsigned("--verify-rounds", optional("--verify-rounds", "20"),
        std::numeric_limits<uint32_t>::max()));
    options.warmupRounds = static_cast<uint32_t>(ParseUnsigned("--warmup", optional("--warmup", "100"),
        std::numeric_limits<uint32_t>::max()));
    options.measureRounds = static_cast<uint32_t>(ParseUnsigned("--rounds", optional("--rounds", "1000"),
        std::numeric_limits<uint32_t>::max()));
    options.traceRounds = static_cast<uint32_t>(ParseUnsigned("--trace-rounds", optional("--trace-rounds", "0"),
        kMaxTraceRounds));
    options.timeoutSec = static_cast<uint32_t>(ParseUnsigned("--timeout-sec", optional("--timeout-sec", "10"), 32767));
    if (options.verifyRounds == 0) {
        throw std::runtime_error("--verify-rounds must be greater than zero");
    }
    if (options.timeoutSec == 0) {
        throw std::runtime_error("--timeout-sec must be greater than zero");
    }
    for (int appCpu : options.appCpus) {
        for (int workerCpu : options.workerCpus) {
            if (appCpu >= 0 && appCpu == workerCpu) {
                throw std::runtime_error("every app CPU must differ from every worker CPU");
            }
        }
    }
    if (options.kind == RunKind::Verify) {
        if (options.traceRounds != 0) {
            throw std::runtime_error("--trace-rounds is only valid for --kind trace");
        }
        options.warmupRounds = 0;
        options.measureRounds = 0;
        options.traceRounds = 0;
    } else if (options.kind == RunKind::Measure) {
        if (options.traceRounds != 0) {
            throw std::runtime_error("--trace-rounds is only valid for --kind trace");
        }
        options.traceRounds = 0;
        if (options.measureRounds == 0) {
            throw std::runtime_error("--rounds must be greater than zero for --kind measure");
        }
    } else {
        // Diagnostic rounds follow the same requested warmup as measurement.
        options.measureRounds = 0;
        if (options.traceRounds == 0) {
            throw std::runtime_error("--trace-rounds must be in [1,64] for --kind trace");
        }
    }
    if (options.kind == RunKind::Measure &&
        (std::any_of(options.appCpus.begin(), options.appCpus.end(), [](int cpu) { return cpu < 0; }) ||
            std::any_of(options.workerCpus.begin(), options.workerCpus.end(), [](int cpu) { return cpu < 0; }))) {
        throw std::runtime_error(
            "measure requires explicit app and per-rail worker CPUs for pinned busy polling");
    }
    ValidateSglCapability(options);
    return options;
}

}  // namespace rdma_bench
