// SPDX-License-Identifier: MulanPSL-2.0
#pragma once
#include <cstddef>
#include <cstdint>
#include <istream>
#include <string>

namespace rdma_bench {

enum class MemoryBackend { Aligned, Hugetlb };
inline const char *MemoryBackendName(MemoryBackend backend)
{
    return backend == MemoryBackend::Hugetlb ? "hugetlb" : "aligned";
}

size_t RoundMappingBytes(size_t logical, size_t page);
size_t ReadDefaultHugePageBytes(std::istream &input);

// Owns payload storage only. Callers must drain callbacks and deregister MRs
// before destruction. Size() remains the protocol/MR length, never the padding.
class AlignedBuffer {
public:
    AlignedBuffer() = default;
    AlignedBuffer(const AlignedBuffer &) = delete;
    AlignedBuffer &operator=(const AlignedBuffer &) = delete;
    ~AlignedBuffer() { Reset(); }
    void Allocate(size_t size, MemoryBackend backend = MemoryBackend::Aligned, size_t hugePageBytes = 0);
    void Reset() noexcept;
    uint8_t *Data() const { return mData; }
    size_t Size() const { return mSize; }
    size_t MappingSize() const { return mMappingSize; }
    size_t HugePageSize() const { return mHugePageSize; }
    MemoryBackend Backend() const { return mBackend; }
    std::string IdentityJson(const std::string &role, unsigned rail, const char *purpose, const char *phase) const;

private:
    uint8_t *mData = nullptr;
    size_t mSize = 0;
    size_t mMappingSize = 0;
    size_t mHugePageSize = 0;
    MemoryBackend mBackend = MemoryBackend::Aligned;
};

// Read-only, outside timed rounds. Counters are whole intersecting VMAs,
// which may include other heap allocations; never pretend they are per-buffer.
std::string MappingSnapshot(uintptr_t address, size_t bytes, std::istream &smaps, std::istream &numaMaps);
} // namespace rdma_bench
