#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Read-only Linux mapping evidence for a known live payload address (MF or SGL)."""

import argparse
import json
import re
import sys
from pathlib import Path

HEADER = re.compile(r"^([0-9a-f]+)-([0-9a-f]+)\s")
FIELDS = ("KernelPageSize", "MMUPageSize", "AnonHugePages", "Private_Hugetlb", "Shared_Hugetlb")


def snapshot(smaps, numa_maps, address, length):
    if address < 0 or length <= 0:
        raise ValueError("address must be nonnegative and length positive")
    selected, current = [], None
    for line in smaps.splitlines():
        match = HEADER.match(line)
        if match:
            start, end = (int(s, 16) for s in match.groups())
            current = None
            if start < address + length and end > address:
                current = dict(
                    start=start,
                    end=end,
                    vma_bytes=end - start,
                    hugetlb_flag=None,
                    numa_page_kb=None,
                    numa_pages=None,
                    **{k + "_kb": None for k in FIELDS},
                )
                selected.append(current)
        elif current is not None and ":" in line:
            key, value = line.split(":", 1)
            if key in FIELDS:
                current[key + "_kb"] = int(value.split()[0])
            elif key == "VmFlags":
                current["hugetlb_flag"] = "ht" in value.split()
    by_start = {m["start"]: m for m in selected}
    for line in numa_maps.splitlines():
        tokens = line.split()
        if not tokens:
            continue
        mapping = by_start.get(int(tokens[0], 16))
        if mapping is None:
            continue
        mapping["numa_pages"] = {}
        for token in tokens[1:]:
            key, sep, value = token.partition("=")
            if sep and key == "kernelpagesize_kB":
                mapping["numa_page_kb"] = int(value)
            elif sep and re.fullmatch(r"N\d+", key):
                mapping["numa_pages"][key] = int(value)
    covered = sum(min(m["end"], address + length) - max(m["start"], address) for m in selected)
    for mapping in selected:
        mapping["start"] = hex(mapping["start"])
        mapping["end"] = hex(mapping["end"])
    return dict(
        status="covered" if covered == length else "partial-or-unknown",
        scope="whole-intersecting-vmas",
        address=hex(address),
        logical_bytes=length,
        covered_bytes=covered,
        vmas=selected,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pid", type=int)
    parser.add_argument("address", type=lambda s: int(s, 0), help="actual allocated payload VA; usually 0x...")
    parser.add_argument("bytes", type=lambda s: int(s, 0), help="payload buffer length")
    args = parser.parse_args()
    try:
        if args.pid <= 0:
            raise ValueError("PID must be positive")
        root = Path("/proc") / str(args.pid)
        smaps = (root / "smaps").read_text()
        try:
            numa = (root / "numa_maps").read_text()
        except OSError:
            numa = ""  # Missing NUMA evidence remains null.
        result = snapshot(smaps, numa, args.address, args.bytes)
        result["pid"] = args.pid
        print(json.dumps(result, separators=(",", ":")))
        return 0 if result["status"] == "covered" else 1
    except (OSError, ValueError) as error:
        print(f"ERROR: mapping evidence unavailable: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
