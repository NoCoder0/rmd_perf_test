// SPDX-License-Identifier: MulanPSL-2.0
#include "aligned_buffer.h"
#include <algorithm>
#include <cstdio>
#include <limits>
#include <map>
#include <sstream>
#include <vector>

namespace rdma_bench {
namespace {
struct Mapping {
    uint64_t begin, end;
    std::map<std::string, uint64_t> kb;
    std::map<std::string, uint64_t> nodes;
    bool flagsKnown = false, hugetlb = false, numaKnown = false;
    uint64_t numaPageKb = 0;
};
const std::vector<std::string> kFields = {
    "KernelPageSize", "MMUPageSize", "AnonHugePages", "Private_Hugetlb", "Shared_Hugetlb"};

std::vector<Mapping> ReadSmaps(std::istream &input, uint64_t begin, uint64_t end)
{
    std::vector<Mapping> maps;
    Mapping *current = nullptr;
    std::string line;
    while (std::getline(input, line)) {
        unsigned long long first = 0, last = 0;
        char permission = 0;
        if (std::sscanf(line.c_str(), "%llx-%llx %c", &first, &last, &permission) == 3) {
            current = nullptr;
            if (first < end && last > begin) {
                maps.push_back(Mapping{});
                current = &maps.back();
                current->begin = first;
                current->end = last;
            }
        } else if (current) {
            std::istringstream fields(line);
            std::string key;
            fields >> key;
            if (key == "VmFlags:") {
                current->flagsKnown = true;
                while (fields >> key) if (key == "ht") current->hugetlb = true;
            } else if (!key.empty()) {
                key.pop_back();
                uint64_t value = 0;
                std::string unit;
                if (std::find(kFields.begin(), kFields.end(), key) != kFields.end() &&
                    (fields >> value >> unit) && unit == "kB") current->kb[key] = value;
            }
        }
    }
    return maps;
}

void ReadNuma(std::istream &input, std::vector<Mapping> &maps)
{
    std::string line;
    while (std::getline(input, line)) {
        std::istringstream fields(line);
        uint64_t address = 0;
        if (!(fields >> std::hex >> address >> std::dec)) continue;
        auto found = std::find_if(maps.begin(), maps.end(), [address](const Mapping &m) { return m.begin == address; });
        if (found == maps.end()) continue;
        found->numaKnown = true;
        std::string token;
        while (fields >> token) {
            const auto equal = token.find('=');
            if (equal == std::string::npos) continue;
            const std::string key = token.substr(0, equal);
            std::istringstream number(token.substr(equal + 1));
            uint64_t value = 0;
            if (!(number >> value)) continue;
            if (key == "kernelpagesize_kB") found->numaPageKb = value;
            else if (key.size() > 1 && key[0] == 'N' &&
                key.find_first_not_of("0123456789", 1) == std::string::npos) found->nodes[key] = value;
        }
    }
}
} // namespace

std::string MappingSnapshot(uintptr_t address, size_t bytes, std::istream &smaps, std::istream &numaMaps)
{
    if (!address || !bytes || bytes > std::numeric_limits<uintptr_t>::max() - address || !smaps)
        return "{\"status\":\"unknown\"}";
    auto maps = ReadSmaps(smaps, address, address + bytes);
    ReadNuma(numaMaps, maps);
    uint64_t covered = 0;
    for (const auto &m : maps)
        covered += std::min<uint64_t>(m.end, address + bytes) - std::max<uint64_t>(m.begin, address);
    std::ostringstream out;
    out << "{\"status\":\"" << (covered == bytes ? "covered" : "partial-or-unknown")
        << "\",\"scope\":\"whole-intersecting-vmas\",\"covered_bytes\":" << covered << ",\"vmas\":[";
    bool comma = false;
    for (const auto &m : maps) {
        if (comma) out << ',';
        comma = true;
        out << "{\"vma_bytes\":" << m.end - m.begin << ",\"hugetlb_flag\":"
            << (m.flagsKnown ? (m.hugetlb ? "true" : "false") : "null");
        for (const auto &key : kFields) {
            const auto it = m.kb.find(key);
            out << ",\"" << key << "_kb\":" << (it == m.kb.end() ? "null" : std::to_string(it->second));
        }
        out << ",\"numa_page_kb\":" << (m.numaPageKb ? std::to_string(m.numaPageKb) : "null")
            << ",\"numa_pages\":";
        if (!m.numaKnown) out << "null";
        else {
            out << '{';
            bool nodeComma = false;
            for (const auto &node : m.nodes) {
                if (nodeComma) out << ',';
                nodeComma = true;
                out << '"' << node.first << "\":" << node.second;
            }
            out << '}';
        }
        out << '}';
    }
    out << "]}";
    return out.str();
}
} // namespace rdma_bench
