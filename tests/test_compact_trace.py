#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Synthetic trace regression tests; no hardware or MF checkout required."""

import copy
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compact_trace import compact, read_records  # noqa: E402


def ev(name, time, wr="0x1", **kw):
    return dict(
        record_type="hcom_trace",
        host_role="remote",
        event=name,
        timestamp_ns=time,
        qp_num=1,
        wr_id=wr,
        capture_round=101,
        transfer_kind="data",
        bytes=656,
        sge_count=1,
        send_flags=2,
        post_call_status=0,
        status=0,
        opcode=0,
        thread_slot=0,
        cq_id="0x2",
        batch_id=1,
        count=1,
        poll_begin_ns=time - 20,
        previous_poll_end_ns=time - 30,
        empty_polls=3,
        max_poll_gap_ns=10,
        max_poll_call_ns=20,
        **kw,
    )


def remote(app="mf"):
    records = []
    names = [("request_observed", 100), ("request_decoded", 200), ("copy_batch_begin", 300), ("copy_batch_end", 4000)]
    if app == "sgl":
        names = [
            ("remote_request_received", 90),
            ("remote_request_observed", 100),
            ("remote_request_decoded", 200),
            ("remote_source_prepared", 250),
            ("remote_requests_prepared", 300),
        ]
    for name, time in names:
        records.append(
            dict(
                record_type="mf_app_trace" if app == "mf" else "trace",
                host_role="remote",
                event=name,
                timestamp_ns=time,
                capture_round=101,
                generation=121,
                blocks=2,
                block_bytes=656,
                notify_every_wrs=32,
                sgl_items=30,
            )
        )
    for i, (start, end, complete) in enumerate([(1000, 2000, 1500), (2200, 2300, 3000)]):
        for name, time in [
            ("verbs_post_begin", start),
            ("verbs_post_end", end),
            ("cq_poll_batch", complete),
            ("cqe_observed", complete),
            ("cq_dispatch_begin", complete + 20),
            ("cq_dispatch_end", complete + 40),
        ]:
            e = ev(name, time)
            e.update(batch_id=i + 1, generation=121 if name.startswith("verbs_post") else None)
            if not name.startswith("verbs_post"):
                e["opcode"] = 1
            records.append(e)
    records += (
        [dict(record_type="mf_trace_config", host_role="remote", rounds=1, count=2, size=656)] if app == "mf" else []
    )
    records.append(
        dict(
            record_type="mf_trace_summary" if app == "mf" else "hcom_trace_summary",
            status="ok",
            dropped=0,
            post_records=2,
            cqe_records=2,
            records=12,
            app_records=len(names),
        )
    )
    return records


def local(app):
    records = [e for e in remote(app) if e.get("record_type") not in ("mf_app_trace", "trace")]
    for e in records:
        e["host_role"] = "local"
        if e.get("record_type") == "hcom_trace":
            e["transfer_kind"] = "request"
            e["opcode"] = 2 if e["event"].startswith("verbs_post") else 0
    names = [("local_round_begin", 100), ("request_submit_begin", 300), ("request_end", 450), ("local_round_end", 3500)]
    if app == "sgl":
        names = [
            ("local_begin", 100),
            ("local_request_submit_begin", 300),
            ("local_request_posted", 450),
            ("local_end", 3500),
        ]

    def point(name, time, **fields):
        return dict(
            record_type="mf_app_trace" if app == "mf" else "trace",
            host_role="local",
            event=name,
            timestamp_ns=time,
            capture_round=101,
            generation=121,
            blocks=2,
            block_bytes=656,
            notify_every_wrs=32,
            sgl_items=1,
            **fields,
        )

    points = [point(n, t) for n, t in names]
    for i in range(2):
        for name, time in [("watermark_observed", 1500), ("scatter_begin", 1510), ("scatter_end", 1600)]:
            if app == "sgl":
                name = {
                    "watermark_observed": "local_ready_observed",
                    "scatter_begin": "local_scatter_begin",
                    "scatter_end": "local_scatter_end",
                }[name]
            points.append(point(name, time + i * 1500, rail=0, chunk_id=i, **{"from": i, "upto": i + 1}))
    records[-1]["app_records"] = len(points)
    return records + points


def fixture(role="remote"):
    records = local("sgl") if role == "local" else remote("sgl")
    for record in records:
        if record.get("record_type") == "hcom_trace_summary":
            record.update(host_role=role, data_callback_records=0)
    records.append(
        dict(
            schema_version=8,
            role=role,
            status="ok",
            mode="sgl",
            kind="trace",
            links=1,
            blocks=2,
            block_bytes=656,
            sgl_items=1 if role == "local" else 30,
            notify_every_wrs=32,
            trace_rounds=1,
        )
    )
    return records


class CompactTests(unittest.TestCase):
    def test_complete_remote_early_completion_reused_wr_and_tail(self):
        result = compact(fixture())
        self.assertEqual(result["status"], "ok", result)
        row = result["rounds"][0]
        self.assertEqual(row["data_completed_bytes"], 1312)
        self.assertEqual(row["first_data_post_to_last_cqe_us"], 2)
        self.assertEqual(row["last_data_post_end_to_last_cqe_us"], 0.7)
        self.assertEqual(row["inflight_at_last_post_end"], 1)
        self.assertEqual(row["max_inflight_wr_observed"], 1)

    def test_local_scatter_and_group_summary(self):
        result = compact(fixture("local"))
        self.assertEqual(result["status"], "ok", result)
        row = result["rounds"][0]
        self.assertEqual(row["e2e_us"], 3.4)
        self.assertEqual(row["scatter_sum_us"], 0.18)
        self.assertEqual(len(row["ready_groups"]), 1)
        self.assertEqual(row["ready_groups"][0]["chunks"], 2)

    def test_missing_labels_inferred_from_application_events(self):
        records = fixture()
        for record in records:
            record.pop("host_role", None)
        self.assertEqual(compact(records)["status"], "ok")

    def test_real_mixed_hosts_and_conflicting_labels_rejected(self):
        with self.assertRaisesRegex(ValueError, "roles=.*local.*remote"):
            compact(fixture() + fixture("local"))
        records = fixture()
        records[0]["role"] = "local"
        with self.assertRaisesRegex(ValueError, "roles=.*local.*remote"):
            compact(records)

    def test_missing_cqe_never_reports_fast_tail(self):
        records = [r for r in fixture() if not (r.get("event") == "cqe_observed" and r["batch_id"] == 2)]
        result = compact(records)
        self.assertEqual(result["status"], "incomplete")
        self.assertNotIn("first_data_post_to_last_cqe_us", result["rounds"][0])
        self.assertEqual(result["rounds"][0]["data_completed_bytes"], 656)

    def test_duplicate_cqe_dropped_and_failed_post(self):
        for problem in ("duplicate", "dropped", "post"):
            records = fixture()
            if problem == "duplicate":
                records.append(copy.deepcopy(next(r for r in records if r.get("event") == "cqe_observed")))
            elif problem == "dropped":
                records[-2]["dropped"] = 1
            else:
                next(r for r in records if r.get("event") == "verbs_post_begin")["post_call_status"] = 12
            result = compact(records)
            self.assertEqual(result["status"], "incomplete", problem)
            self.assertFalse(result["rounds"][0]["completion_metrics_valid"])

    def test_missing_summary_result_or_dispatch(self):
        for field, value in (
            ("record_type", "hcom_trace_summary"),
            ("schema_version", 8),
            ("event", "cq_dispatch_end"),
            ("event", "cq_poll_batch"),
        ):
            result = compact([r for r in fixture() if r.get(field) != value])
            self.assertEqual(result["status"], "incomplete", value)

    def test_legacy_metadata_is_not_fabricated(self):
        records = fixture()
        for record in records:
            record.pop("send_flags", None)
            record.pop("post_call_status", None)
        records = [r for r in records if r.get("event") != "remote_request_observed"]
        result = compact(records)
        self.assertEqual(result["status"], "ok", result)
        self.assertTrue(result["warnings"])
        self.assertNotIn("request_to_first_data_post_us", result["rounds"][0])

    def test_wrong_generation_or_round_count(self):
        records = fixture()
        records[-1]["trace_rounds"] = 2
        self.assertEqual(compact(records)["status"], "incomplete")
        records = fixture()
        next(r for r in records if r.get("event") == "verbs_post_begin")["generation"] = 122
        self.assertEqual(compact(records)["status"], "incomplete")

    def test_round_count_absent_is_explicit(self):
        records = fixture()
        records[-1].pop("trace_rounds")
        result = compact(records)
        self.assertEqual(result["status"], "ok")
        self.assertTrue(any("round count absent" in w for w in result["warnings"]))
        self.assertNotIn("identity", result)

    def test_remote_window_setting_preserved_without_inventing_local_setting(self):
        for window in (0, 4, 8):
            records = fixture()
            records[-1].update(max_inflight=window, inflight_limit_unit="data-PutV-per-rail")
            self.assertEqual(compact(records)["config"]["max_inflight"], window)
        self.assertNotIn("max_inflight", compact(fixture("local"))["config"])

    def test_poll_histograms_absent_vs_recorded(self):
        records = fixture()
        self.assertNotIn("poll_call_bins", compact(records)["rounds"][0])
        for record in records:
            if record.get("event") == "cq_poll_batch":
                record["poll_call_bins"] = [2, 1, 0, 0, 0, 0, 0, 0]
                record["poll_gap_bins"] = [1, 2, 0, 0, 0, 0, 0, 0]
        self.assertEqual(compact(records)["rounds"][0]["poll_call_bins"], [4, 2, 0, 0, 0, 0, 0, 0])

    def test_data_callback_durations_and_missing_begin(self):
        records = fixture()
        for cqe in [r for r in records if r.get("event") == "cqe_observed"]:
            records += [
                dict(cqe, event="data_callback_begin", timestamp_ns=cqe["timestamp_ns"] + 25),
                dict(cqe, event="data_callback_end", timestamp_ns=cqe["timestamp_ns"] + 35),
            ]
        summary = next(r for r in records if r.get("record_type") == "hcom_trace_summary")
        summary.update(records=16, data_callback_records=2, expected_sgl_operations=2)
        result = compact(records)
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["rounds"][0]["application_callback_us"]["p50"], 0.01)
        result = compact([r for r in records if r.get("event") != "data_callback_begin"])
        self.assertEqual(result["status"], "incomplete")

    def test_incomplete_scatter_coverage(self):
        records = [
            r for r in fixture("local") if not (r.get("event") == "local_scatter_end" and r.get("chunk_id") == 1)
        ]
        self.assertEqual(compact(records)["status"], "incomplete")

    def test_two_rails_use_distinct_chunk_keys(self):
        records = fixture("local")
        records[-1]["links"] = 2
        for r in records:
            if r.get("chunk_id") == 1:
                r.update(rail=1, chunk_id=0)
        result = compact(records)
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(len(result["rounds"][0]["ready_groups"]), 2)

    def test_linked_post_call_not_counted_per_wr(self):
        records = fixture()
        for r in records:
            if r.get("record_type") == "hcom_trace" and r["batch_id"] == 2:
                r["wr_id"] = "0x3"
                if r["event"] == "verbs_post_begin":
                    r["timestamp_ns"] = 1000
                elif r["event"] == "verbs_post_end":
                    r["timestamp_ns"] = 2000
        result = compact(records)
        self.assertEqual(result["status"], "ok", result)
        self.assertEqual(result["rounds"][0]["data_post_call_us"]["n"], 1)

    def test_multiple_runs_or_cases_rejected(self):
        self.assertEqual(compact(fixture() + fixture())["status"], "incomplete")
        records = fixture()
        records[0]["case_index"] = 2
        with self.assertRaisesRegex(ValueError, "multiple cases"):
            compact(records)

    def test_compact_omits_raw_records(self):
        data = json.dumps(compact(fixture()))
        self.assertNotIn("wr_id", data)
        self.assertNotIn("timestamp_ns", data)
        self.assertLess(len(data), 6000)

    def with_memory(self, role="remote", links=1, backend="hugetlb"):
        records = fixture(role)
        records[-1].update(memory_backend=backend, hugepage_kb_requested=0, links=links)
        if links == 2 and role == "local":
            for r in records:
                if r.get("chunk_id") == 1:
                    r.update(rail=1, chunk_id=0)
        for rail in range(links):
            for purpose in (("source",) if role == "remote" else ("destination", "staging")):
                for phase in ("init", "exit"):
                    records.append(dict(record_type="memory_identity", role=role, rail=rail, purpose=purpose,
                                        phase=phase, requested=backend, actual=backend, fallback=False,
                                        logical_bytes=656, mapping_bytes=2097152 if backend == "hugetlb" else None,
                                        hugetlb_page_bytes=2097152 if backend == "hugetlb" else None,
                                        evidence="MAP_HUGETLB-success" if backend == "hugetlb" else "allocator-only",
                                        mapping={"status": "unknown"}))
        return records

    def test_memory_preserved_for_roles_rails_and_backends(self):
        for role in ("local", "remote"):
            for links in (1, 2):
                for backend in ("aligned", "hugetlb"):
                    result = compact(self.with_memory(role, links, backend))
                    self.assertEqual(result["status"], "ok", result)
                    self.assertEqual(len(result["memory"]), links * (4 if role == "local" else 2))
                    self.assertEqual(result["memory"][0]["actual"], backend)
        self.assertEqual(compact(fixture())["memory"], [])

    def test_missing_or_false_memory_evidence(self):
        for mutation in ({"actual": "aligned"}, {"fallback": True}, {"hugetlb_page_bytes": 3},
                         {"mapping_bytes": 656}, {"role": "local"}):
            records = self.with_memory()
            records[-1].update(mutation)
            self.assertEqual(compact(records)["status"], "incomplete")
        self.assertEqual(compact(self.with_memory()[:-1])["status"], "incomplete")

    def test_standalone_cli_auto_role_noise_whitespace_and_bom(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(Path(__file__).resolve().parents[1] / "compact_trace.py", root)
            # Deliberately misleading filename; role comes from the content.
            path = root / "sgl-local.log"
            path.write_text("noise\n" + "\n".join("  " + json.dumps(r) for r in fixture()), encoding="utf-8-sig")
            run = subprocess.run(
                [sys.executable, str(root / "compact_trace.py"), "--compact", str(path)],
                capture_output=True,
                text=True,
                cwd=root,
            )
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(json.loads(run.stdout)["role"], "remote")
            path.write_text('{"record_type":', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "line 1"):
                read_records(path)
            run = subprocess.run(
                [sys.executable, str(root / "compact_trace.py"), str(path)], capture_output=True, text=True, cwd=root
            )
            self.assertEqual(run.returncode, 2)
            self.assertEqual(run.stdout, "")


if __name__ == "__main__":
    unittest.main()
