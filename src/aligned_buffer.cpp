// SPDX-License-Identifier: MulanPSL-2.0
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "aligned_buffer.h"
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#if defined(_WIN32)
#include <malloc.h>
#endif
#if defined(__linux__)
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace rdma_bench {

size_t RoundMappingBytes(size_t logical, size_t page)
{
    if (!logical || !page || (page & (page - 1)))
        throw std::invalid_argument("buffer length must be positive and page size a power of two");
    if (logical > std::numeric_limits<size_t>::max() - (page - 1))
        throw std::overflow_error("hugetlb mapping length overflow");
    return (logical + page - 1) & ~(page - 1);
}

size_t ReadDefaultHugePageBytes(std::istream &input)
{
    std::string line;
    while (std::getline(input, line)) {
        std::istringstream fields(line);
        std::string key, unit;
        uint64_t kb = 0;
        if ((fields >> key >> kb >> unit) && key == "Hugepagesize:" && unit == "kB" && kb &&
            kb <= std::numeric_limits<size_t>::max() / 1024) {
            const size_t bytes = static_cast<size_t>(kb) * 1024;
            (void)RoundMappingBytes(1, bytes);
            return bytes;
        }
    }
    throw std::runtime_error("cannot read default Hugepagesize from /proc/meminfo; specify --hugepage-kb");
}

void AlignedBuffer::Allocate(size_t size, MemoryBackend backend, size_t hugePageBytes)
{
    if (mData) throw std::logic_error("buffer already allocated; deregister MR and Reset before reallocation");
    if (!size) throw std::invalid_argument("cannot allocate a zero-length payload buffer");
    void *memory = nullptr;
    size_t mapped = 0, page = 0;
    if (backend == MemoryBackend::Hugetlb) {
#if defined(__linux__)
        std::ifstream meminfo("/proc/meminfo");
        page = hugePageBytes ? hugePageBytes : ReadDefaultHugePageBytes(meminfo);
        mapped = RoundMappingBytes(size, page);
        const long basePage = sysconf(_SC_PAGESIZE);
        if (basePage <= 0 || page <= static_cast<size_t>(basePage) || page < 4096)
            throw std::invalid_argument("hugetlb size must exceed the system base page and preserve 4096 alignment");
        unsigned shift = 0;
        for (size_t n = page; n > 1; n >>= 1) ++shift;
        // Linux UAPI encodes log2(page bytes) in bits [26,31]. Select explicitly
        // even for the discovered default, so successful mmap proves the size.
        const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB |
            static_cast<int>(shift << 26);
        memory = mmap(nullptr, mapped, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (memory == MAP_FAILED) {
            const int error = errno;
            throw std::runtime_error("hugetlb mmap failed: logical=" + std::to_string(size) +
                " mapped=" + std::to_string(mapped) + " page=" + std::to_string(page) +
                " errno=" + std::to_string(error) + " (" + std::strerror(error) +
                "). No fallback. Check /sys/kernel/mm/hugepages, per-node pools, allowed NUMA nodes, "
                "hugetlb cgroup limits and permissions; use --memory-backend aligned for baseline.");
        }
#else
        (void)hugePageBytes;
        throw std::runtime_error("--memory-backend hugetlb requires Linux; no fallback");
#endif
    } else {
        if (hugePageBytes) throw std::invalid_argument("--hugepage-kb requires --memory-backend hugetlb");
#if defined(_WIN32)
        memory = _aligned_malloc(size, 4096);
        const int rc = memory ? 0 : ENOMEM;
#else
        const int rc = posix_memalign(&memory, 4096, size);
#endif
        if (rc || !memory)
            throw std::runtime_error("aligned allocation failed for " + std::to_string(size) +
                " bytes: " + std::strerror(rc ? rc : ENOMEM));
    }
    mData = static_cast<uint8_t *>(memory);
    mSize = size;
    mMappingSize = mapped; // malloc's containing mapping length is not known.
    mHugePageSize = page;
    mBackend = backend;
}

void AlignedBuffer::Reset() noexcept
{
    if (!mData) return;
    if (mBackend == MemoryBackend::Hugetlb) {
#if defined(__linux__)
        if (munmap(mData, mMappingSize) != 0) {
            std::fprintf(stderr, "FATAL: munmap payload failed: %s\n", std::strerror(errno));
            std::abort();
        }
#endif
    } else {
#if defined(_WIN32)
        _aligned_free(mData);
#else
        std::free(mData);
#endif
    }
    mData = nullptr;
    mSize = mMappingSize = mHugePageSize = 0;
    mBackend = MemoryBackend::Aligned;
}

std::string AlignedBuffer::IdentityJson(const std::string &role, unsigned rail,
    const char *purpose, const char *phase) const
{
    std::ifstream smaps("/proc/self/smaps"), numa("/proc/self/numa_maps");
    std::ostringstream out;
    out << "{\"record_type\":\"memory_identity\",\"role\":\"" << role << "\",\"rail\":" << rail
        << ",\"purpose\":\"" << purpose << "\",\"phase\":\"" << phase
        << "\",\"requested\":\"" << MemoryBackendName(mBackend)
        << "\",\"actual\":\"" << MemoryBackendName(mBackend) << "\",\"fallback\":false"
        << ",\"logical_bytes\":" << mSize << ",\"mapping_bytes\":"
        << (mMappingSize ? std::to_string(mMappingSize) : "null")
        << ",\"hugetlb_page_bytes\":" << (mHugePageSize ? std::to_string(mHugePageSize) : "null")
        << ",\"evidence\":\"" << (mBackend == MemoryBackend::Hugetlb ? "MAP_HUGETLB-success" : "allocator-only")
        << "\",\"address\":\"0x" << std::hex << reinterpret_cast<uintptr_t>(mData) << std::dec
        << "\",\"mapping\":" << MappingSnapshot(reinterpret_cast<uintptr_t>(mData), mSize, smaps, numa) << '}';
    return out.str();
}
} // namespace rdma_bench
