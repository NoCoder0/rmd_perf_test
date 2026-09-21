#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Compact one SGL host's full trace log. Standard library only; no MF checkout required.

POST/CQE matching and timing statistics follow MF's trace analyzers.
"""

import argparse
import collections as C
import json
import math
import sys


def stats_us(values):
    """Nanoseconds in; nearest-rank percentiles in microseconds out."""
    values = sorted(values)
    if not values:
        return None
    return {
        "n": len(values),
        "min": round(values[0] / 1000, 3),
        "p50": round(values[math.ceil(len(values) * 0.5) - 1] / 1000, 3),
        "p95": round(values[math.ceil(len(values) * 0.95) - 1] / 1000, 3),
        "max": round(values[-1] / 1000, 3),
    }


def us(a, b):
    return round((a - b) / 1000, 3)


def histogram(values):
    return dict(sorted(C.Counter(values).items(), key=lambda p: str(p[0])))


def wr_key(e):
    return e["qp_num"], e["wr_id"]


def cq_key(e):
    return e["thread_slot"], e["cq_id"], e["batch_id"], e["qp_num"], e["wr_id"]


def match(events):
    """Match each occurrence, using BEGIN (CQE may precede END). Fail ambiguous reuse."""
    groups = C.defaultdict(lambda: C.defaultdict(list))
    for e in events:
        groups[wr_key(e)][e["event"]].append(e)
    rows, errors, consumed = [], [], set()
    for key, g in groups.items():
        starts = sorted(g["verbs_post_begin"], key=lambda e: e["timestamp_ns"])
        ends = sorted(g["verbs_post_end"], key=lambda e: e["timestamp_ns"])
        cqes = sorted(g["cqe_observed"], key=lambda e: e["timestamp_ns"])
        if len(starts) != len(ends):
            errors.append("post_begin_end_count")
        for i, p in enumerate(starts):
            end = ends[i] if i < len(ends) else None
            upper = starts[i + 1]["timestamp_ns"] if i + 1 < len(starts) else math.inf
            candidates = [c for c in cqes if p["timestamp_ns"] <= c["timestamp_ns"] < upper]
            c = candidates[0] if len(candidates) == 1 and p["status"] == 0 else None
            if end is None or end["timestamp_ns"] < p["timestamp_ns"] or end["status"] != p["status"]:
                errors.append("invalid_post_end")
            if p["status"] == 0 and (c is None or c["status"] != 0):
                errors.append("missing_ambiguous_or_failed_cqe")
            expected_opcode = {0: 1, 4: 2, 2: 0, 3: 0}.get(p["opcode"])
            if c is not None and expected_opcode is not None and c["opcode"] != expected_opcode:
                errors.append("wr_wc_opcode_mismatch")
            if p["status"] or p.get("post_call_status", 0) or (end and end.get("post_call_status", 0)):
                errors.append("failed_post_call")
            if c is not None:
                consumed.add(id(c))
            rows.append((p, end, c))
    # Receive WCs have no send-side POST; unmatched send WCs indicate lost posts.
    errors.extend(
        "unmatched_send_cqe"
        for e in events
        if e["event"] == "cqe_observed" and e["opcode"] < 128 and id(e) not in consumed
    )
    return rows, errors


def completion_metrics(rows, events, expected, errors):
    data = [(p, e, c) for p, e, c in rows if p["transfer_kind"] == "data" and p["status"] == 0]
    row = {
        "data_wr": len(data),
        "data_bytes": sum(p["bytes"] for p, _, _ in data),
        "data_completed_bytes": sum(p["bytes"] for p, _, c in data if c and c["status"] == 0),
        "data_cqes": sum(c is not None for _, _, c in data),
    }
    if not data or row["data_bytes"] != expected or row["data_completed_bytes"] != expected:
        errors.append("data_byte_coverage")
    row["completion_metrics_valid"] = not errors and all(e and c for _, e, c in data)
    if not row["completion_metrics_valid"]:
        return row
    first = min(p["timestamp_ns"] for p, _, _ in data)
    last_end = max(e["timestamp_ns"] for _, e, _ in data)
    last_cqe = max(c["timestamp_ns"] for _, _, c in data)
    row.update(
        first_data_post_to_last_post_end_us=us(last_end, first),
        last_data_post_end_to_last_cqe_us=us(last_cqe, last_end),
        first_data_post_to_last_cqe_us=us(last_cqe, first),
    )
    row["data_post_to_cqe_us"] = stats_us([c["timestamp_ns"] - p["timestamp_ns"] for p, _, c in data])
    changes = C.defaultdict(lambda: [0, 0])
    for p, _, c in data:
        changes[p["timestamp_ns"]][0] += 1
        changes[c["timestamp_ns"]][0] -= 1
        changes[c["timestamp_ns"]][1] += p["bytes"]
    live = maximum = completed = 0
    milestones = {}
    for t, (delta, nbytes) in sorted(changes.items()):
        live += delta
        maximum = max(maximum, live)
        completed += nbytes
        for pct in (25, 50, 75, 100):
            if pct not in milestones and completed * 100 >= expected * pct:
                milestones[pct] = us(t, first)
    row["max_inflight_wr_observed"] = maximum
    row["inflight_at_last_post_end"] = sum(p["timestamp_ns"] <= last_end < c["timestamp_ns"] for p, _, c in data)
    row["completed_pct_us"] = milestones
    row["notification_boundaries"] = [
        {
            "kind": p["transfer_kind"],
            "since_first_post_us": us(p["timestamp_ns"], first),
            "inflight_wr": sum(a["timestamp_ns"] <= p["timestamp_ns"] < c["timestamp_ns"] for a, _, c in data),
            "completed_bytes": sum(a["bytes"] for a, _, c in data if c["timestamp_ns"] <= p["timestamp_ns"]),
        }
        for p, _, _ in rows
        if p["transfer_kind"] == "notify"
    ]
    row.update(poll_metrics(data, events))
    calls = sorted({(p["thread_slot"], p["timestamp_ns"], e["timestamp_ns"]) for p, e, _ in data})
    gaps = []
    for thread in {t for t, _, _ in calls}:
        ordered = sorted((a, b) for t, a, b in calls if t == thread)
        gaps.extend(a2 - b1 for (_, b1), (a2, _) in zip(ordered, ordered[1:]))
    row["between_data_post_calls_us"] = stats_us(gaps)
    return row


def poll_metrics(data, events):
    keys = {cq_key(c)[:3] for _, _, c in data}
    polls = [e for e in events if e["event"] == "cq_poll_batch" and cq_key(e)[:3] in keys]
    selected = {cq_key(c): c for _, _, c in data}
    dispatch, callbacks, durations, dispatch_durations = [], [], [], []
    begins, dispatch_begins = {}, {}
    for e in sorted(events, key=lambda e: e["timestamp_ns"]):
        key = cq_key(e)
        if key not in selected:
            continue
        if e["event"] == "cq_dispatch_begin":
            dispatch.append(e["timestamp_ns"] - selected[key]["timestamp_ns"])
            dispatch_begins[key] = e["timestamp_ns"]
        if e["event"] == "cq_dispatch_end" and key in dispatch_begins:
            dispatch_durations.append(e["timestamp_ns"] - dispatch_begins[key])
        if e["event"] == "data_callback_begin":
            callbacks.append(e["timestamp_ns"] - selected[key]["timestamp_ns"])
            begins[key] = e["timestamp_ns"]
        if e["event"] == "data_callback_end" and key in begins:
            durations.append(e["timestamp_ns"] - begins[key])
    result = {
        "data_poll_call_us": stats_us([e["timestamp_ns"] - e["poll_begin_ns"] for e in polls]),
        "immediate_previous_poll_gap_us": stats_us(
            [e["poll_begin_ns"] - e["previous_poll_end_ns"] for e in polls if e["previous_poll_end_ns"]]
        ),
        "max_gap_in_poll_windows_us": max((e["max_poll_gap_ns"] for e in polls), default=0) / 1000,
        "max_call_in_poll_windows_us": max((e["max_poll_call_ns"] for e in polls), default=0) / 1000,
        "empty_polls_in_reported_windows": sum(e["empty_polls"] for e in polls),
        "cqes_per_data_poll": histogram(e["count"] for e in polls),
        "cqe_to_dispatch_us": stats_us(dispatch),
        "cqe_to_application_callback_us": stats_us(callbacks),
        "dispatch_us": stats_us(dispatch_durations),
        "application_callback_us": stats_us(durations),
    }
    if polls and all(len(e.get("poll_call_bins", [])) == len(e.get("poll_gap_bins", [])) == 8 for e in polls):
        result["poll_window_histogram_upper_ns"] = [125, 250, 500, 1000, 2000, 4000, 8000, "inf"]
        for field in ("poll_call_bins", "poll_gap_bins"):
            result[field] = [sum(e[field][i] for e in polls) for i in range(8)]
    return result


def read_records(path):
    records = []
    with open(path, encoding="utf-8-sig") as source:
        for number, line in enumerate(source, 1):
            if not line.lstrip().startswith("{"):
                continue
            try:
                record = json.loads(line)
            except ValueError as error:
                raise ValueError(f"line {number}: truncated/invalid JSON: {error}") from error
            if isinstance(record, dict):
                records.append(record)
    return records


def detect_role(records):
    """Infer missing labels, but never ignore conflicting roles or use a filename as evidence."""
    roles = set()
    for r in records:
        if r.get("record_type") not in ("trace", "hcom_trace", "hcom_trace_summary") and r.get("schema_version") != 8:
            continue
        roles.update(r[k] for k in ("host_role", "role") if r.get(k))
        if r.get("record_type") == "trace":
            prefix = r.get("event", "").split("_", 1)[0]
            if prefix in ("local", "remote"):
                roles.add(prefix)
    if len(roles) != 1 or not roles <= {"local", "remote"}:
        raise ValueError(
            f"expected one host per log; detected roles={sorted(roles, key=str)}. "
            "Keep local/remote logs separate; filenames do not determine the role."
        )
    return roles.pop()


def check_integrity(records, events, errors):
    summaries = [r for r in records if r.get("record_type") == "hcom_trace_summary"]
    if len(summaries) != 1:
        errors.append("missing_or_multiple_collector_summary")
    summary = summaries[0] if len(summaries) == 1 else {}
    if summary.get("status") != "ok" or any(summary.get(k, 0) for k in ("dropped", "clock_errors")):
        errors.append("collector_incomplete")
    counts = C.Counter(e["event"] for e in events)
    for field, event in (
        ("post_records", "verbs_post_end"),
        ("cqe_records", "cqe_observed"),
        ("data_callback_records", "data_callback_end"),
    ):
        if field not in summary or summary[field] != counts[event]:
            errors.append("export_" + field)
    if summary.get("records") != len(events):
        errors.append("export_record_count")
    if summary.get("expected_sgl_operations", 0) and summary["expected_sgl_operations"] != counts["data_callback_end"]:
        errors.append("data_callback_operation_count")
    begins = {cq_key(e): e["timestamp_ns"] for e in events if e["event"] == "data_callback_begin"}
    callbacks = [e for e in events if e["event"] == "data_callback_end"]
    if C.Counter(cq_key(e) for e in callbacks) != C.Counter(
        cq_key(e) for e in events if e["event"] == "data_callback_begin"
    ) or any(e["timestamp_ns"] < begins.get(cq_key(e), 0) for e in callbacks):
        errors.append("data_callback_coverage_or_order")
    cqes = [e for e in events if e["event"] == "cqe_observed"]
    polls = [e for e in events if e["event"] == "cq_poll_batch"]
    batches = C.Counter(cq_key(e)[:3] for e in cqes)
    if C.Counter(cq_key(e)[:3] for e in polls) != C.Counter({k: 1 for k in batches}) or any(
        e["count"] != batches[cq_key(e)[:3]] for e in polls
    ):
        errors.append("poll_cqe_coverage")
    observed = {cq_key(e): e["timestamp_ns"] for e in cqes}
    for name in ("cq_dispatch_begin", "cq_dispatch_end"):
        selected = [e for e in events if e["event"] == name]
        if C.Counter(cq_key(e) for e in selected) != C.Counter(cq_key(e) for e in cqes):
            errors.append(name + "_coverage")
        if any(e["timestamp_ns"] < observed.get(cq_key(e), 0) for e in selected):
            errors.append("dispatch_before_cqe")
    if any(e["poll_begin_ns"] > e["timestamp_ns"] for e in polls):
        errors.append("poll_clock_order")
    if any(e.get("status", 0) or e["timestamp_ns"] <= 0 for e in events):
        errors.append("event_error_or_clock")
    return {
        k: summary[k]
        for k in (
            "status",
            "records",
            "dropped",
            "clock_errors",
            "post_records",
            "cqe_records",
            "data_callback_records",
        )
        if k in summary
    }


def configuration(records, apps, role, errors, warnings):
    results = [r for r in records if r.get("schema_version") == 8 and r.get("role") == role]
    if len(results) != 1 or results[0].get("status") != "ok":
        errors.append("missing_or_multiple_successful_result")
    config = dict(results[0]) if len(results) == 1 else {}
    for field in ("blocks", "block_bytes", "sgl_items", "notify_every_wrs"):
        values = {e[field] for e in apps if field in e}
        if field in config:
            values.add(config[field])
        if len(values) != 1:
            raise ValueError(f"missing or inconsistent {field}; use one workload per log")
        config[field] = values.pop()
        if not isinstance(config[field], int) or config[field] <= 0:
            raise ValueError(f"invalid {field}={config[field]!r}; expected a positive integer")
    if "links" not in config:
        config["links"] = 1 + max((e.get("rail") or 0 for e in apps), default=0)
        warnings.add("links inferred from application trace")
    if config["links"] not in (1, 2) or config.get("mode", "sgl") != "sgl":
        raise ValueError("this script supports one SGL case, with links=1 or 2")
    return {
        k: config[k]
        for k in (
            "commit",
            "kind",
            "mode",
            "links",
            "blocks",
            "block_bytes",
            "sgl_items",
            "notify_every_wrs",
            "max_inflight",
            "inflight_limit_unit",
            "pipeline",
            "warmup_rounds",
            "trace_rounds",
            "source_order",
        )
        if k in config
    }


def stage_metrics(apps, role, errors, warnings):
    times = C.defaultdict(list)
    for event in apps:
        times[event["event"]].append(event["timestamp_ns"])
    pairs = {
        "local": [
            ("e2e_us", "local_begin", "local_end"),
            ("request_prepare_us", "local_begin", "local_request_submit_begin"),
            ("request_submit_us", "local_request_submit_begin", "local_request_posted"),
        ],
        "remote": [
            ("callback_to_request_observed_us", "remote_request_received", "remote_request_observed"),
            ("request_copy_us", "remote_request_observed", "remote_request_copied"),
            ("request_parse_us", "remote_request_copied", "remote_request_decoded"),
            ("source_prepare_us", "remote_request_decoded", "remote_source_prepared"),
            ("requests_prepare_us", "remote_source_prepared", "remote_requests_prepared"),
        ],
    }
    row = {}
    for metric, begin, end in pairs[role]:
        if not times[begin] or not times[end]:
            warnings.add("stage unavailable: " + metric)
            continue
        # Avoid mixing per-rail preparation marks into a single duration.
        if len(times[begin]) != 1 or len(times[end]) != 1:
            warnings.add("multiple stage marks; duration omitted: " + metric)
            continue
        row[metric] = us(times[end][0], times[begin][0])
        if row[metric] < 0:
            errors.append("negative_" + metric)
    if any(e["timestamp_ns"] <= 0 for e in apps):
        errors.append("app_clock_error")
    return row, times


def scatter_metrics(apps, config, times, errors):
    required = ("local_ready_observed", "local_scatter_begin", "local_scatter_end")
    if len(times["local_begin"]) != 1 or len(times["local_end"]) != 1:
        errors.append("missing_or_duplicate_local_boundary")
        return {}
    origin, end = times["local_begin"][0], times["local_end"][0]
    chunks = C.defaultdict(dict)
    for event in apps:
        if event["event"] in required:
            chunk = chunks[(event["rail"], event["chunk_id"])]
            if event["event"] in chunk:
                errors.append("duplicate_scatter_or_ready")
            chunk[event["event"]] = event["timestamp_ns"]
    links, count, k = config["links"], config["blocks"], config["sgl_items"]
    capacity = math.ceil(count / links)
    expected = {
        (rail, chunk)
        for rail in range(links)
        for chunk in range(math.ceil(max(0, min(capacity, count - rail * capacity)) / k))
    }
    if set(chunks) != expected:
        errors.append("local_scatter_chunk_coverage")
    intervals, readiness, groups = [], [], C.defaultdict(list)
    for (rail, chunk), marks in sorted(chunks.items()):
        if not all(name in marks for name in required):
            errors.append("scatter_or_ready_coverage")
            continue
        ready, begin, finish = (marks[name] for name in required)
        if not origin <= ready <= begin <= finish <= end:
            errors.append("scatter_order")
        intervals.append(finish - begin)
        readiness.append(ready)
        groups[(rail, chunk // config["notify_every_wrs"])].append((ready, begin, finish))
    return {
        "scatter_sum_us": us(sum(intervals), 0),
        "scatter_call_us": stats_us(intervals),
        "scatter_interval_count": len(intervals),
        "last_ready_to_end_us": us(end, max(readiness)) if readiness else None,
        "ready_groups": [
            {
                "rail": rail,
                "group": group,
                "chunks": len(items),
                "first_ready_us": us(min(x[0] for x in items), origin),
                "last_ready_us": us(max(x[0] for x in items), origin),
                "scatter_end_us": us(max(x[2] for x in items), origin),
                "scatter_sum_us": us(sum(x[2] - x[1] for x in items), 0),
            }
            for (rail, group), items in sorted(groups.items())
        ],
    }


def normalize_events(records, role):
    events = []
    for original in records:
        if original.get("record_type") != "hcom_trace":
            continue
        event = dict(original, transfer_kind="other")
        if event.get("generation"):
            if event["opcode"] == 0 and role == "remote":
                event["transfer_kind"] = "data"
            elif event["opcode"] in (2, 3):
                event["transfer_kind"] = "request" if role == "local" else "notify"
        events.append(event)
    return events


def compact(records):
    role = detect_role(records)
    events = normalize_events(records, role)
    apps = [r for r in records if r.get("record_type") == "trace"]
    if not apps or not events:
        raise ValueError("missing application/HCOM trace records; use --kind trace and save full stdout")
    if len({r.get("case_index", 1) for r in apps}) != 1:
        raise ValueError("multiple cases in one log; run trace with one --blocks and one --block-bytes value")
    errors, warnings = [], set()
    collector = check_integrity(records, events, errors)
    rows, match_errors = match(events)
    errors += match_errors
    config = configuration(records, apps, role, errors, warnings)
    generations = {r["generation"] for r in apps}
    if any(not isinstance(n, int) or n <= 0 for n in generations):
        raise ValueError("invalid application generation")
    rounds = sorted(generations)
    if "trace_rounds" in config:
        if config["trace_rounds"] != len(rounds):
            errors.append("round_count_mismatch")
    else:
        warnings.add("expected round count absent; entirely missing rounds cannot be detected")
    if any(p.get("generation") not in generations for p, _, _ in rows if p["transfer_kind"] != "other"):
        errors.append("post_without_app_round")
    if any("send_flags" not in p or "post_call_status" not in p for p, _, _ in rows):
        warnings.add("legacy log lacks send_flags/post_call_status")
    output = {
        "format": "sgl-compact-v1",
        "role": role,
        "config": config,
        "hcom_build": next((r.get("id") for r in records if r.get("record_type") == "hcom_build_identity"), None),
        "collector": collector,
        "errors": histogram(errors),
        "rounds": [],
    }
    for number in rounds:
        selected = sorted((e for e in apps if e["generation"] == number), key=lambda e: e["timestamp_ns"])
        posts = [(p, e, c) for p, e, c in rows if p.get("generation") == number]
        problems = list(errors)
        row, times = stage_metrics(selected, role, problems, warnings)
        if role == "local":
            row.update(scatter_metrics(selected, config, times, problems))
        else:
            row.update(completion_metrics(posts, events, config["blocks"] * config["block_bytes"], problems))
            first = min((p["timestamp_ns"] for p, _, _ in posts if p["transfer_kind"] == "data"), default=None)
            for name, metric in (
                ("remote_request_observed", "request_to_first_data_post_us"),
                ("remote_request_received", "request_callback_to_first_data_post_us"),
            ):
                if first is not None and len(times[name]) == 1:
                    row[metric] = us(first, times[name][0])
        row["data_post_call_us"] = stats_us(
            [
                b - a
                for _, a, b in {
                    (p["thread_slot"], p["timestamp_ns"], e["timestamp_ns"])
                    for p, e, _ in posts
                    if e and p["transfer_kind"] == "data"
                }
            ]
        )
        row["wr_counts"] = histogram(p["transfer_kind"] for p, _, _ in posts)
        row["wr_shapes"] = histogram(
            f"{p['transfer_kind']}:{p['sge_count']}SGE/{p['bytes']}B/flags={p.get('send_flags', '?')}"
            for p, _, _ in posts
        )
        row.update(round=number, errors=histogram(problems))
        output["rounds"].append(row)
    output["status"] = "ok" if not errors and all(not r["errors"] for r in output["rounds"]) else "incomplete"
    output["warnings"] = sorted(warnings)
    output["basis"] = "log-only; same-host us; CQE=software observation; exit code/artifact identity not verified"
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", help="one host's complete trace stdout/stderr log")
    parser.add_argument("--compact", action="store_true", help="accepted for MF-style usage; already the default")
    args = parser.parse_args()
    try:
        result = compact(read_records(args.log))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"ERROR: invalid/incomplete SGL trace: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    if result["status"] != "ok":
        print("ERROR: incomplete trace; inspect errors in the exported JSON", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
