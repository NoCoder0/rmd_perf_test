#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Execute production fill/verification methods on CPU buffers, without HCOM or RDMA."""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from test_inflight_window import body

ROOT = Path(__file__).resolve().parents[1]


class SourceUpdateTests(unittest.TestCase):
    def test_content_policy_and_verification(self):
        remote = (ROOT / "src/remote.cpp").read_text()
        local = (ROOT / "src/local.cpp").read_text()
        header = (ROOT / "src/benchmark.h").read_text()
        data = (ROOT / "src/data_path.cpp").read_text()
        pattern = (ROOT / "src/data_path.h").read_text()
        source = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
constexpr uint32_t kStrideBytes = 4096;
struct Buffer {
    alignas(8) uint8_t bytes[3 * kStrideBytes];
    uint8_t *Data() { return bytes; }
};
struct CopyEntry { uint64_t remoteSourceOffset, localDestinationOffset; };
struct Params { uint32_t verifyRounds = 2, blocks = 3, blockBytes = 656;
    uint32_t RailBlocks(uint16_t) { return blocks; } };
uint16_t RailForRequestIndex(uint32_t, const Params &) { return 0; }
'''
        source += body(pattern, "inline uint64_t PatternWord(")
        source += body(data, "void FillBlock(") + body(data, "bool VerifyBlock(")
        source += r'''
class SparseCopyBenchmark {
public:
    struct { std::string sourceUpdate = "markers"; } mOptions;
    Params mParams;
    uint64_t mCaseFirstGeneration = 41;
    uint32_t mBlocksPerRail = 3;
    struct { Buffer buffer; } mRails[1];
    std::vector<CopyEntry> mCopyEntries{{0, 0}, {4096, 4096}, {8192, 8192}};
    void FillRemoteSourceRail(uint16_t rail, uint64_t generation);
    void VerifyLocalMarkers(uint64_t generation);
'''
        source += body(header, "uint64_t BodySeed(") + body(header, "uint64_t MarkerSeed(") + "};\n"
        source += body(remote, "void SparseCopyBenchmark::FillRemoteSourceRail(")
        source += body(local, "void SparseCopyBenchmark::VerifyLocalMarkers(")
        source += r'''
int main() {
    for (auto mode : {"static", "markers"}) for (uint32_t size : {656, 1024}) {
        SparseCopyBenchmark b;
        b.mOptions.sourceUpdate = mode;
        b.mParams.blockBytes = size;
        memset(b.mRails[0].buffer.Data(), 0x5a, 3 * kStrideBytes);
        for (uint64_t gen : {41, 42}) {
            b.FillRemoteSourceRail(0, gen);
            b.VerifyLocalMarkers(gen);
        }
        Buffer initialized = b.mRails[0].buffer;
        for (uint64_t gen : {43, 44, 45}) {
            b.FillRemoteSourceRail(0, gen);
            b.VerifyLocalMarkers(gen);
            for (uint32_t i = 0; i < 3; ++i) {
                auto *p = b.mRails[0].buffer.Data() + i * kStrideBytes;
                auto *original = initialized.Data() + i * kStrideBytes;
                std::string error;
                assert(VerifyBlock(p, b.MarkerSeed(gen), i, size, b.BodySeed(gen), error));
                assert(memcmp(p + 8, original + 8, size - 16) == 0);
                assert(memcmp(p + size, original + size, kStrideBytes - size) == 0);
                if (b.mOptions.sourceUpdate == "static") assert(memcmp(p, original, size) == 0);
                else assert(!VerifyBlock(p, gen - 1, i, size, b.BodySeed(gen), error));
            }
        }
        auto *p = b.mRails[0].buffer.Data();
        p[0] ^= 1;
        bool rejected = false;
        try { b.VerifyLocalMarkers(45); } catch (const std::runtime_error &) { rejected = true; }
        assert(rejected);
        p[0] ^= 1;
        p[100] ^= 1;
        std::string error;
        assert(!VerifyBlock(p, b.MarkerSeed(45), 0, size, b.BodySeed(45), error));
    }
}
'''
        with tempfile.TemporaryDirectory() as directory:
            cpp = Path(directory) / "source.cpp"
            exe = Path(directory) / "source.exe"
            cpp.write_text(source)
            subprocess.run([os.environ.get("CXX", "g++"), "-std=c++17", "-Wall", "-Wextra", "-Werror",
                            str(cpp), "-o", str(exe)], check=True, capture_output=True)
            subprocess.run([str(exe)], check=True, timeout=10)


if __name__ == "__main__":
    unittest.main()
