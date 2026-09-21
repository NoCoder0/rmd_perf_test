#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Compile actual parser/window methods with CPU-only fixtures; no HCOM or device required."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def body(source, marker):
    start = source.index(marker)
    brace = source.index("{", start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


class InflightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compiler = os.environ.get("CXX", "g++")
        if not shutil.which(compiler):
            raise RuntimeError("C++ compiler required; set CXX or install g++")
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.build = Path(cls.temp.name)
        common = (ROOT / "src/common.h").read_text()
        declarations = "\n".join(
            line for line in common.splitlines() if line.startswith("enum class ") and line.endswith("};")
        )
        names = [
            "kMaxLinks",
            "kMaxBlocks",
            "kMaxTraceRounds",
            "kModeDirect",
            "kModeSgl",
            "kPipelineOff",
            "kPipelineOn",
            "kDefaultSglItems",
            "kDesignMaxSglItems",
        ]
        declarations += "\n" + "\n".join(
            line
            for line in common.splitlines()
            if any(line.startswith("constexpr uint") and " " + n + " =" in line for n in names)
        )
        # Constants must precede enum definitions, just as in common.h.
        declarations = "\n".join(sorted(declarations.splitlines(), key=lambda line: line.startswith("enum")))
        declarations += "\nconstexpr uint32_t kCompiledSgeMax = 30;\n"  # Mock provider capability only.
        declarations += body(common, "struct Options {") + ";\n"
        headers = """
#include "aligned_buffer.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>
"""
        config_header = (
            (ROOT / "src/config.h").read_text().replace('#include "common.h"', "").replace("#pragma once", "")
        )
        parser = (ROOT / "src/config.cpp").read_text().replace('#include "config.h"', "")
        parser = headers + "namespace rdma_bench {\n" + declarations + "}\n" + config_header + parser
        parser += """
int main(int argc, char **argv) {
    try { std::cout << rdma_bench::ParseOptions(argc, argv).maxInflight; return 0; }
    catch (const std::exception &e) { std::cerr << e.what(); return 1; }
}
"""
        remote = (ROOT / "src/remote.cpp").read_text()
        benchmark = (ROOT / "src/benchmark.h").read_text()
        methods = body(remote, "void SparseCopyBenchmark::WaitForSglWindow(")
        wait = body(benchmark, "template <typename Predicate> void WaitDataUntil(")
        harness = (
            headers
            + '#include "inflight_window.h"\nnamespace rdma_bench {\n'
            + r"""
constexpr uint32_t kDataDeadlineCheckInterval = 256;
uint64_t ticks = 0;
std::function<void()> onSpin;
uint64_t NowNs() { return ++ticks; }
void CpuRelax() { if (onSpin) onSpin(); }
struct RailState {
    struct { uint64_t attemptedDataCallbacks = 0; } appCounters;
    struct { std::atomic<uint64_t> dataDoneCallbacks{0}; } callbackCounters;
};
class SparseCopyBenchmark {
public:
    struct { uint32_t maxInflight = 0; } mOptions;
    std::array<RailState, 2> mRails;
    bool failed = false;
    void CheckFatal(const char *) { if (failed) throw std::runtime_error("callback failed"); }
    void WaitForSglWindow(uint16_t rail, uint64_t deadlineNs);
"""
            + wait
            + "\n};\n"
            + methods
            + r"""
void RunWindow(uint32_t limit) {
    SparseCopyBenchmark bench;
    bench.mOptions.maxInflight = limit;
    for (uint16_t rail = 0; rail < 2; ++rail) {
        auto &state = bench.mRails[rail];
        state.appCounters.attemptedDataCallbacks = 1000000;
        state.callbackCounters.dataDoneCallbacks.store(1000000);
        for (unsigned round = 0; round < 3; ++round) {
            unsigned peak = 0, notifications = 0, completionsBeforeFirstNotify = 0;
            onSpin = [&] {
                state.callbackCounters.dataDoneCallbacks.fetch_add(1);
                if (!notifications) ++completionsBeforeFirstNotify;
            };
            for (unsigned chunk = 0; chunk < 54; ++chunk) {
                if (limit) bench.WaitForSglWindow(rail, UINT64_MAX);
                ++state.appCounters.attemptedDataCallbacks;
                auto outstanding = state.appCounters.attemptedDataCallbacks -
                    state.callbackCounters.dataDoneCallbacks.load();
                peak = std::max(peak, static_cast<unsigned>(outstanding));
                assert(!limit || outstanding <= limit);
                if ((chunk + 1) % 32 == 0 || chunk + 1 == 54) ++notifications;
            }
            assert(peak == (limit ? std::min(limit, 54U) : 54U));
            assert(notifications == 2);
            if (limit && limit < 32) assert(completionsBeforeFirstNotify > 0);
            state.callbackCounters.dataDoneCallbacks.store(state.appCounters.attemptedDataCallbacks);
        }
    }
    onSpin = nullptr;
}
} // namespace rdma_bench
int main() {
    using namespace rdma_bench;
    for (auto limit : {0U, 1U, 4U, 8U, 54U, 9600U}) RunWindow(limit);
    assert(HasSglWindowCredit(UINT64_MAX, UINT64_MAX - 3, 4));
    assert(!HasSglWindowCredit(UINT64_MAX, UINT64_MAX - 4, 4));
    SparseCopyBenchmark bench;
    bench.mOptions.maxInflight = 4;
    bench.mRails[0].appCounters.attemptedDataCallbacks = 4;
    onSpin = [&] { bench.failed = true; };
    bool caught = false;
    try { bench.WaitForSglWindow(0, UINT64_MAX); } catch (const std::runtime_error &) { caught = true; }
    assert(caught);
    bench.failed = false; onSpin = nullptr; ticks = 0; caught = false;
    try { bench.WaitForSglWindow(0, 1); } catch (const std::runtime_error &) { caught = true; }
    assert(caught);
    // A completed callback frees one credit without waiting for a full group.
    ticks = 0; unsigned spins = 0;
    onSpin = [&] { ++spins; bench.mRails[0].callbackCounters.dataDoneCallbacks.store(1); };
    bench.WaitForSglWindow(0, UINT64_MAX);
    assert(spins == 1);
    std::cout << "PASS: 0/1/4/8/full windows, cumulative rounds, independent rails, G32, errors and deadline\n";
}
"""
        )
        for name, code in (("parser", parser), ("window", harness)):
            path = cls.build / (name + ".cpp")
            path.write_text(code, encoding="utf-8")
            executable = cls.build / (name + (".exe" if os.name == "nt" else ""))
            result = subprocess.run(
                [
                    compiler,
                    "-std=c++17",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-I" + str(ROOT / "src"),
                    str(path),
                    "-o",
                    str(executable),
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                raise RuntimeError(result.stderr)
            setattr(cls, name, executable)

    def parse(self, role="remote", mode="sgl", extra=()):
        args = [
            str(self.parser),
            "--role",
            role,
            "--mode",
            mode,
            "--kind",
            "trace",
            "--trace-rounds",
            "2",
            "--rdma-ip",
            "127.0.0.1",
            "--listen" if role == "remote" else "--peer",
            "127.0.0.1:19000",
        ]
        env = dict(os.environ, RDMA_600_SGL_ITEMS="30")
        env.pop("RDMA_600_QP_MAX_SEND_SGE", None)
        return subprocess.run(args + list(extra), capture_output=True, text=True, env=env)

    def test_window_flow_and_wait_failure_paths(self):
        result = subprocess.run([str(self.window)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_default_and_valid_limits(self):
        self.assertEqual(self.parse().stdout, "0")
        for value in ("0", "1", "4", "8", "9600"):
            result = self.parse(extra=("--max-inflight", value))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, value)

    def test_invalid_limits(self):
        for value in ("-1", "+4", "4x", "", "9601", "18446744073709551616"):
            result = self.parse(extra=("--max-inflight", value))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--max-inflight", result.stderr)

    def test_memory_options_are_process_local_and_strict(self):
        for role in ("local", "remote"):
            for extra in (("--memory-backend", "aligned"), ("--memory-backend", "hugetlb"),
                          ("--memory-backend", "hugetlb", "--hugepage-kb", "32768")):
                result = self.parse(role=role, extra=extra)
                self.assertEqual(result.returncode, 0, result.stderr)
        for extra in (("--memory-backend", "auto"), ("--hugepage-kb", "2048"),
                      ("--memory-backend", "hugetlb", "--hugepage-kb", "0"),
                      ("--memory-backend", "hugetlb", "--hugepage-kb", "3072"),
                      ("--memory-backend", "hugetlb", "--hugepage-kb", "18446744073709551615")):
            self.assertNotEqual(self.parse(extra=extra).returncode, 0)

    def test_reject_wrong_role_mode_and_duplicate(self):
        for role, mode in (("local", "sgl"), ("remote", "direct")):
            result = self.parse(role, mode, ("--max-inflight", "0"))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("only applicable", result.stderr)
        result = self.parse(extra=("--max-inflight", "4", "--max-inflight", "8"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate", result.stderr)


if __name__ == "__main__":
    unittest.main()
