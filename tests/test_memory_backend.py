#!/usr/bin/env python3
"""CPU allocator tests; fake mmap validates Linux branches without claiming real hugetlb/RDMA."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class MemoryTests(unittest.TestCase):
    def test_real_allocator_and_linux_syscall_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sys").mkdir()
            (root / "sys/mman.h").write_text(r"""
#pragma once
#include <cstddef>
#define PROT_READ 1
#define PROT_WRITE 2
#define MAP_PRIVATE 2
#define MAP_ANONYMOUS 32
#define MAP_HUGETLB 0x40000
#define MAP_FAILED ((void*)-1)
void *mmap(void *, size_t, int, int, int, long);
int munmap(void *, size_t);
""")
            (root / "unistd.h").write_text("#define _SC_PAGESIZE 30\nlong sysconf(int);\n")
            harness = root / "memory.cpp"
            harness.write_text(r"""
#include "aligned_buffer.h"
#include <cassert>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#ifdef FAKE_MMAP
#include <sys/mman.h>
alignas(4096) unsigned char storage[4 * 1024 * 1024];
int maps = 0, unmaps = 0;
bool fail = false;
size_t mappedBytes = 0;
long basePage = 65536;
long sysconf(int) { return basePage; }
void *mmap(void *address, size_t bytes, int prot, int flags, int fd, long offset) {
    assert(!address && prot == 3 && fd == -1 && !offset);
    assert((flags & 0x40022) == 0x40022 && (static_cast<unsigned>(flags) >> 26) == 21);
    ++maps;
    if (fail) { errno = ENOMEM; return MAP_FAILED; }
    assert(bytes <= sizeof(storage)); mappedBytes = bytes; return storage;
}
int munmap(void *address, size_t bytes) {
    assert(address == storage && bytes == mappedBytes); ++unmaps; return 0;
}
#endif
template<class F> void throws(F f) {
    bool caught = false; try { f(); } catch (const std::exception &) { caught = true; } assert(caught);
}
int main() {
    using namespace rdma_bench;
    assert(RoundMappingBytes(656, 65536) == 65536);
    assert(RoundMappingBytes(2097152, 2097152) == 2097152);
    throws([] { RoundMappingBytes(SIZE_MAX, 2097152); });
    throws([] { RoundMappingBytes(0, 2097152); });
    throws([] { RoundMappingBytes(4, 3); });
    std::istringstream meminfo("MemTotal: 123 kB\nHugepagesize: 32768 kB\n");
    assert(ReadDefaultHugePageBytes(meminfo) == 33554432);
    std::istringstream absent("Hugepagesize: 0 kB\n");
    throws([&] { ReadDefaultHugePageBytes(absent); });
    AlignedBuffer ordinary;
    ordinary.Allocate(65537);
    assert(ordinary.Size() == 65537 && !ordinary.MappingSize() && !ordinary.HugePageSize());
    assert(reinterpret_cast<uintptr_t>(ordinary.Data()) % 4096 == 0);
    std::memset(ordinary.Data(), 0xa5, ordinary.Size());
    throws([&] { ordinary.Allocate(5); });
    assert(ordinary.Size() == 65537 && ordinary.Data()[65536] == 0xa5);
    ordinary.Reset(); ordinary.Reset(); assert(!ordinary.Data() && !ordinary.Size());
    ordinary.Allocate(7);
#ifdef FAKE_MMAP
    {
        AlignedBuffer huge;
        huge.Allocate(2097153, MemoryBackend::Hugetlb, 2097152);
        assert(huge.Size() == 2097153 && huge.MappingSize() == 4194304);
        assert(huge.HugePageSize() == 2097152);
        std::memset(huge.Data(), 0x5a, huge.Size());
        std::cout << huge.IdentityJson("remote", 1, "source", "init") << '\n';
    }
    assert(maps == 1 && unmaps == 1);
    AlignedBuffer failed;
    fail = true;
    try { failed.Allocate(656, MemoryBackend::Hugetlb, 2097152); assert(false); }
    catch (const std::runtime_error &e) { assert(std::string(e.what()).find("No fallback") != std::string::npos); }
    assert(!failed.Data() && !failed.Size() && maps == 2 && unmaps == 1);
    throws([&] { failed.Allocate(656, MemoryBackend::Hugetlb, 65536); });
    assert(maps == 2); // 64 KiB base page is not a hugetlb size on this fake host.
    failed.Allocate(17); // failed huge allocation left ownership empty.
    throws([] {
        fail = false;
        AlignedBuffer first, second;
        first.Allocate(656, MemoryBackend::Hugetlb, 2097152);
        fail = true;
        second.Allocate(656, MemoryBackend::Hugetlb, 2097152);
    });
    assert(maps == 4 && unmaps == 2); // Partial initialization unwinds the first mapping exactly once.
    fail = false;
    AlignedBuffer reset;
    reset.Allocate(656, MemoryBackend::Hugetlb, 2097152);
    reset.Reset(); reset.Reset(); assert(!reset.Data() && !reset.Size() && !reset.MappingSize());
    assert(maps == 5 && unmaps == 3);
#elif !defined(__linux__)
    AlignedBuffer unsupported;
    throws([&] { unsupported.Allocate(656, MemoryBackend::Hugetlb, 2097152); });
    assert(!unsupported.Data());
#endif
    std::istringstream smaps(
        "1000-3000 rw-p 00000000 00:00 0\nKernelPageSize: 64 kB\nMMUPageSize: 64 kB\n"
        "AnonHugePages: 0 kB\nVmFlags: rd wr\n"
        "3000-9000 rw-p 00000000 00:00 0\nKernelPageSize: 2048 kB\n"
        "AnonHugePages: 0 kB\nPrivate_Hugetlb: 2048 kB\nVmFlags: rd wr ht\n");
    std::istringstream numa("1000 default N0=2 kernelpagesize_kB=64\n"
        "3000 default huge N1=1 kernelpagesize_kB=2048\n");
    std::cout << MappingSnapshot(0x2000, 0x3000, smaps, numa) << '\n';
}
""")
            for fake in (False, True):
                executable = root / ("fake.exe" if fake else "native.exe")
                command = [
                    os.environ.get("CXX", "g++"),
                    "-std=c++17",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-I" + str(ROOT / "src"),
                    str(harness),
                    str(ROOT / "src/aligned_buffer.cpp"),
                    str(ROOT / "src/mapping_snapshot.cpp"),
                    "-o",
                    str(executable),
                ]
                if fake:
                    command += ["-D__linux__", "-DFAKE_MMAP", "-I" + str(root)]
                built = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(built.returncode, 0, built.stderr)
                ran = subprocess.run([str(executable)], capture_output=True, text=True)
                self.assertEqual(ran.returncode, 0, ran.stderr)
                records = [json.loads(line) for line in ran.stdout.splitlines()]
                snapshot = records[-1]
                self.assertEqual(snapshot["status"], "covered")
                self.assertEqual(snapshot["covered_bytes"], 0x3000)
                self.assertEqual(snapshot["vmas"][0]["KernelPageSize_kb"], 64)
                self.assertIsNone(snapshot["vmas"][1]["MMUPageSize_kb"])
                self.assertTrue(snapshot["vmas"][1]["hugetlb_flag"])
                self.assertEqual(snapshot["vmas"][1]["numa_pages"], {"N1": 1})
                if fake:
                    self.assertEqual(records[0]["mapping_bytes"], 4194304)
                    self.assertEqual(records[0]["hugetlb_page_bytes"], 2097152)


if __name__ == "__main__":
    unittest.main()
