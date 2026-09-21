#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Compress an existing SGL remote trace into a bounded CQE-tail report.

No new runtime hooks: run trace manually, then run this script on the full log.
All times are software observations on the remote host, in microseconds.
"""

import argparse
import collections as C
import json
import math
import sys

import compact_trace as base


TOP = 8
MAX_BINS = 64


def stream_key(e):
    return e["thread_slot"], e["cq_id"]


def batch_key(e):
    return base.cq_key(e)[:3]


def durations(events, name):
    """Use the full completion key; wr_id alone may be reused."""
    begin = {base.cq_key(e): e["timestamp_ns"] for e in events if e["event"] == name + "_begin"}
    return {
        base.cq_key(e): (e["thread_slot"], begin[base.cq_key(e)], e["timestamp_ns"])
        for e in events
        if e["event"] == name + "_end" and base.cq_key(e) in begin
    }


def covered_ns(intervals, thread, lo, hi):
    """Union of observed dispatch intervals, clipped to one polling thread."""
    segments = sorted((max(a, lo), min(b, hi)) for t, a, b in intervals.values()
                      if t == thread and a < hi and b > lo)
    total, previous = 0, lo
    for a, b in segments:
        total += max(0, b - max(a, previous))
        previous = max(previous, b)
    return total


def poll_windows(events):
    # The producer accumulates empty polls/maxima PER THREAD, not per CQ.
    previous = {}
    windows = []
    for e in sorted((e for e in events if e["event"] == "cq_poll_batch"),
                    key=lambda e: (e["timestamp_ns"], e["batch_id"])):
        key = e.get("case_index", 1), e["thread_slot"]
        windows.append((previous.get(key), e))
        previous[key] = e["timestamp_ns"]
    return windows


def gap_report(lo, hi, stream, completed, windows, dispatch, origin):
    selected = [(start, e) for start, e in windows
                if e["thread_slot"] == stream[0] and lo < e["timestamp_ns"] <= hi]
    full = [e for start, e in selected if start is not None and start >= lo]
    return {
        "thread_slot": stream[0], "cq_id": stream[1],
        "from_tail_us": base.us(lo, origin), "to_tail_us": base.us(hi, origin),
        "gap_us": base.us(hi, lo), "data_cqes_at_end": len(completed),
        "end_batch_ids": sorted({c["batch_id"] for _, _, c in completed}),
        "empty_polls_full_windows": sum(e["empty_polls"] for e in full),
        "full_poll_windows": len(full), "straddling_or_unknown_windows": len(selected) - len(full),
        "max_poll_gap_reported_us": max((e["max_poll_gap_ns"] for _, e in selected), default=0) / 1000,
        "max_poll_call_reported_us": max((e["max_poll_call_ns"] for _, e in selected), default=0) / 1000,
        "dispatch_covered_us": covered_ns(dispatch, stream[0], lo, hi) / 1000,
    }


def round_tail(data, events, windows, bin_ns, wr_details):
    data = sorted(data, key=lambda r: (r[0]["timestamp_ns"], r[0]["qp_num"], r[0]["wr_id"]))
    first = min(p["timestamp_ns"] for p, _, _ in data)
    t0 = max(e["timestamp_ns"] for _, e, _ in data)
    end = max(c["timestamp_ns"] for _, _, c in data)
    tail = [(p, e, c) for p, e, c in data if c["timestamp_ns"] > t0]
    span = max(0, end - t0)
    width = max(bin_ns, math.ceil(span / MAX_BINS / 1000) * 1000)
    dispatch = durations(events, "cq_dispatch")
    callbacks = durations(events, "data_callback")
    bins = []
    for lo in range(t0, end, width):
        hi = min(end, lo + width)
        done = [p for p, _, c in tail if lo < c["timestamp_ns"] <= hi]
        bins.append([base.us(lo, t0), base.us(hi, t0), len(done), sum(p["bytes"] for p in done),
                     sum(c["timestamp_ns"] > hi for _, _, c in tail)])

    streams = C.defaultdict(lambda: C.defaultdict(list))
    for p, e, c in data:
        streams[stream_key(c)][c["timestamp_ns"]].append((p, e, c))
    gaps = []
    for stream, observations in streams.items():
        previous = t0
        for timestamp, completed in sorted(observations.items()):
            if timestamp <= t0:
                continue
            gaps.append(gap_report(previous, timestamp, stream, completed, windows, dispatch, t0))
            previous = timestamp

    by_qp = []
    for qp in sorted({p["qp_num"] for p, _, _ in data}):
        selected = [(p, e, c) for p, e, c in data if p["qp_num"] == qp]
        pending = [(p, e, c) for p, e, c in selected if c["timestamp_ns"] > t0]
        by_qp.append({
            "qp_num": qp, "data_wr": len(selected), "bytes": sum(p["bytes"] for p, _, _ in selected),
            "pending_at_tail_start": len(pending), "pending_bytes": sum(p["bytes"] for p, _, _ in pending),
            "last_cqe_from_tail_start_us": base.us(max(c["timestamp_ns"] for _, _, c in selected), t0),
        })

    chosen = data if wr_details else sorted(tail, key=lambda r: r[2]["timestamp_ns"] - r[0]["timestamp_ns"], reverse=True)[:TOP]
    indices = {id(p): i + 1 for i, (p, _, _) in enumerate(data)}
    wr_rows = []
    for p, e, c in chosen:
        callback = callbacks.get(base.cq_key(c))
        wr_rows.append([
            indices[id(p)], p["qp_num"], p["wr_id"], base.us(p["timestamp_ns"], first),
            base.us(e["timestamp_ns"], first), base.us(c["timestamp_ns"], first),
            base.us(c["timestamp_ns"], p["timestamp_ns"]), max(0, base.us(c["timestamp_ns"], t0)),
            c["thread_slot"], c["cq_id"], c["batch_id"],
            base.us(callback[1], c["timestamp_ns"]) if callback else None,
            base.us(callback[2], callback[1]) if callback else None,
        ])
    result = {
        "start_from_first_post_us": base.us(t0, first), "duration_us": span / 1000,
        "signed_last_post_end_to_last_cqe_us": base.us(end, t0),
        "data_wr": len(data), "completed_before_or_at_start": len(data) - len(tail),
        "pending_wr_at_start": len(tail), "pending_bytes_at_start": sum(p["bytes"] for p, _, _ in tail),
        "tail_software_completion_GBps": round(sum(p["bytes"] for p, _, _ in tail) / span, 3) if span else None,
        "effective_bin_us": width / 1000,
        "bin_columns": ["from_tail_us", "to_tail_us", "data_cqes", "completed_bytes", "pending_wr_at_end"],
        "bins": bins, "by_qp": by_qp,
        "gap_scope": "between distinct data-CQE observations on each thread/CQ; first gap clipped to tail start",
        "gap_count": len(gaps),
        "gap_us": base.stats_us([round(g["gap_us"] * 1000) for g in gaps]),
        "largest_gaps": sorted(gaps, key=lambda g: g["gap_us"], reverse=True)[:TOP],
        "data_cqes_per_tail_batch": base.histogram(C.Counter(batch_key(c) for _, _, c in tail).values()),
        "wr_selection": "all_data_wrs" if wr_details else "top8_tail_wrs_by_post_to_cqe",
        "wr_columns": ["post_ordinal", "qp_num", "wr_id", "post_begin_us", "post_end_us", "cqe_us",
                       "post_to_cqe_us", "residual_after_tail_start_us", "thread_slot", "cq_id", "batch_id",
                       "cqe_to_callback_begin_us", "callback_us"],
        "wrs": wr_rows,
    }
    result["poll_and_callback_metrics_for_tail_wrs"] = base.poll_metrics(tail, events) if tail else None
    return result


def analyze(records, bin_us=25, wr_details=False):
    if not isinstance(bin_us, int) or bin_us <= 0:
        raise ValueError("--bin-us must be a positive integer")
    if any(r.get("format") for r in records):
        raise ValueError("input is already compressed; use the original remote trace log to recover per-WR events")
    compact = base.compact(records)
    if compact["role"] != "remote":
        raise ValueError("this report requires a remote log; filename is not used to infer the role")
    output = {k: compact[k] for k in ("status", "role", "config", "hcom_build", "collector", "errors", "warnings")}
    output.update(format="sgl-remote-tail-v1", rounds=[], basis={
        "times": "same remote host; CQE is poll observation, not a hardware timestamp",
        "tail": "last successful data POST_END to final data CQE; payload/notify callbacks may overlap this interval",
        "poll_window": "per polling thread since previous nonempty poll, possibly including other CQs",
        "gap_maxima": "whole reported poll-window maxima; may extend outside the selected gap when straddling_or_unknown_windows>0",
        "empty_poll_counts": "sum only fully contained poll windows; boundary-crossing counts excluded, not treated as zero",
        "dispatch": "union of recorded dispatch intervals on that thread; includes nested callback time; not proof of CPU residency",
        "rate": "completed WR bytes divided by software observation time; not NIC wire throughput",
    })
    events = base.normalize_events(records, "remote")
    rows, _ = base.match(events)
    windows = poll_windows(events)
    for r in compact["rounds"]:
        row = {k: r[k] for k in ("round", "errors", "completed_pct_us", "first_data_post_to_last_cqe_us",
                                "data_post_to_cqe_us", "max_inflight_wr_observed") if k in r}
        if compact["status"] == "ok" and r.get("completion_metrics_valid"):
            data = [(p, e, c) for p, e, c in rows if p.get("generation") == r["round"] and p["transfer_kind"] == "data"]
            row["tail"] = round_tail(data, events, windows, bin_us * 1000, wr_details)
        else:
            row["tail"] = None
        output["rounds"].append(row)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", help="full, uncompressed SGL remote trace log, one case per file")
    parser.add_argument("--bin-us", type=int, default=25, help="completion bin width (default: 25 us; at most 64 bins)")
    parser.add_argument("--wr-details", action="store_true", help="retain every data WR instead of the eight slowest tail WRs")
    args = parser.parse_args()
    try:
        result = analyze(base.read_records(args.log), args.bin_us, args.wr_details)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"ERROR: invalid remote trace: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    if result["status"] != "ok":
        print("ERROR: incomplete trace; tail details withheld; inspect errors", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
