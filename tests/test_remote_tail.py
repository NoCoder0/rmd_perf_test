#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Tail-analysis boundary and attribution checks, using existing trace fixtures."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze_remote_tail as tail
from test_compact_trace import ev, fixture


class RemoteTailTests(unittest.TestCase):
    def test_reused_wr_and_completion_before_post_return(self):
        result = tail.analyze(fixture(), wr_details=True)
        self.assertEqual(result["status"], "ok")
        report = result["rounds"][0]["tail"]
        self.assertEqual(report["duration_us"], 0.7)
        self.assertEqual(report["pending_wr_at_start"], 1)
        self.assertEqual(report["completed_before_or_at_start"], 1)
        self.assertEqual(report["bins"], [[0.0, 0.7, 1, 656, 0]])
        self.assertEqual(len(report["wrs"]), 2)
        # Same pointer-valued WR ID is reused; the two occurrences stay separate.
        self.assertEqual(report["wrs"][0][2], report["wrs"][1][2])
        self.assertEqual(report["wrs"][0][5], 0.5)
        self.assertEqual(report["wrs"][1][5], 2.0)
        # Poll-window counters cross the tail start and cannot be split exactly.
        gap = report["largest_gaps"][0]
        self.assertEqual(gap["empty_polls_full_windows"], 0)
        self.assertEqual(gap["straddling_or_unknown_windows"], 1)

    def test_incomplete_dropped_and_compacted_inputs(self):
        records = fixture()
        next(r for r in records if r.get("record_type") == "hcom_trace_summary")["dropped"] = 1
        result = tail.analyze(records)
        self.assertEqual(result["status"], "incomplete")
        self.assertIsNone(result["rounds"][0]["tail"])
        with self.assertRaisesRegex(ValueError, "already compressed"):
            tail.analyze([{"format": "sgl-compact-v1"}])
        with self.assertRaisesRegex(ValueError, "remote log"):
            tail.analyze(fixture("local"))

    def test_all_completions_before_post_return_has_no_tail(self):
        records = fixture()
        posts = [r for r in records if r.get("event") == "verbs_post_end"]
        posts[-1]["timestamp_ns"] = 3100
        report = tail.analyze(records)["rounds"][0]["tail"]
        self.assertEqual(report["duration_us"], 0)
        self.assertEqual(report["signed_last_post_end_to_last_cqe_us"], -0.1)
        self.assertEqual(report["pending_wr_at_start"], 0)
        self.assertEqual(report["bins"], [])

    def test_missing_cqe_suppresses_tail_and_long_trace_keeps_bounded_bins(self):
        records = fixture()
        removed = next(r for r in records if r.get("event") == "cqe_observed")
        records.remove(removed)
        result = tail.analyze(records)
        self.assertEqual(result["status"], "incomplete")
        self.assertIsNone(result["rounds"][0]["tail"])
        records = fixture()
        for r in records:
            if r.get("record_type") == "hcom_trace" and r.get("batch_id") == 2 and not r["event"].startswith("verbs_post"):
                for field in ("timestamp_ns", "poll_begin_ns", "previous_poll_end_ns"):
                    r[field] += 10_000_000
        report = tail.analyze(records)["rounds"][0]["tail"]
        self.assertLessEqual(len(report["bins"]), tail.MAX_BINS)
        self.assertEqual(sum(b[2] for b in report["bins"]), 1)
        self.assertEqual(sum(b[3] for b in report["bins"]), 656)
        self.assertEqual(report["bins"][-1][4], 0)

    def test_poll_windows_are_per_thread_not_per_cq(self):
        a = ev("cq_poll_batch", 100)
        b = ev("cq_poll_batch", 200)
        b.update(cq_id="0x3", batch_id=2, empty_polls=7, max_poll_gap_ns=50)
        c = ev("cq_poll_batch", 300)
        c.update(batch_id=3, empty_polls=11, max_poll_gap_ns=60)
        windows = tail.poll_windows([a, b, c])
        report = tail.gap_report(100, 300, (0, "0x2"), [], windows, {}, 100)
        self.assertEqual(report["empty_polls_full_windows"], 18)
        self.assertEqual(report["full_poll_windows"], 2)
        self.assertEqual(report["straddling_or_unknown_windows"], 0)
        clipped = tail.gap_report(150, 300, (0, "0x2"), [], windows, {}, 150)
        self.assertEqual(clipped["empty_polls_full_windows"], 11)
        self.assertEqual(clipped["straddling_or_unknown_windows"], 1)

    def test_dispatch_union_and_thread_filter(self):
        intervals = {1: (0, 50, 150), 2: (0, 100, 170), 3: (1, 0, 200), 4: (0, 190, 220)}
        self.assertEqual(tail.covered_ns(intervals, 0, 100, 200), 80)

    def test_same_batch_cqes_not_mistaken_for_multiple_poll_windows(self):
        records = fixture()
        events = tail.base.normalize_events(records, "remote")
        rows, errors = tail.base.match(events)
        self.assertFalse(errors)
        p, e, c = copy.deepcopy(rows[-1])
        p["wr_id"] = e["wr_id"] = c["wr_id"] = "0x9"
        report = tail.round_tail(rows + [(p, e, c)], events, tail.poll_windows(events), 100, True)
        self.assertEqual(report["pending_wr_at_start"], 2)
        self.assertEqual(report["gap_count"], 1)
        self.assertEqual(report["largest_gaps"][0]["data_cqes_at_end"], 2)
        self.assertEqual(report["data_cqes_per_tail_batch"], {2: 1})
        self.assertEqual(sum(b[2] for b in report["bins"]), 2)

    def test_cli_roundtrip_and_invalid_width(self):
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "remote.log"
            log.write_text("normal console line\n" + "\n".join(json.dumps(r) for r in fixture()), encoding="utf-8")
            proc = subprocess.run([sys.executable, str(Path(tail.__file__)), str(log), "--wr-details"],
                                  capture_output=True, text=True, check=False)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(json.loads(proc.stdout)["status"], "ok")
        with self.assertRaisesRegex(ValueError, "positive"):
            tail.analyze(fixture(), bin_us=0)


if __name__ == "__main__":
    unittest.main()
