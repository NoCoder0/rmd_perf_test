#!/usr/bin/env bash
# CPU-only tests. No RDMA service, device or performance measurement is used.
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
ubs="${1:?usage: bash tests/run_trace_tests.sh /path/to/updated/ubs-comm}"
out="${TRACE_TEST_BUILD_DIR:-${root}/build/trace-tests}"
mkdir -p "$out"
cxx="${CXX:-g++}"
"$cxx" -std=c++11 -Wall -Wextra -Werror -pthread -I"${ubs}/src" \
    "${root}/tests/rdma_trace_contract.cpp" "${ubs}/src/hcom/hcom_rdma_trace.cpp" -o "${out}/contract"
"${out}/contract"
"$cxx" -std=c++17 -Wall -Wextra -Werror -pthread -I"${ubs}/src" -I"${root}/src" \
    "${root}/tests/detailed_trace_buffer.cpp" "${root}/src/detailed_trace.cpp" \
    "${ubs}/src/hcom/hcom_rdma_trace.cpp" -o "${out}/buffer"
"${out}/buffer" > "${out}/normal.jsonl"
"${out}/buffer" overflow > "${out}/overflow.jsonl"
"${out}/buffer" threads > "${out}/threads.jsonl"
"${out}/buffer" missing-posts > "${out}/missing-posts.jsonl"
python3 - "$out" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
for name in ('normal', 'overflow', 'threads', 'missing-posts'):
    records = [json.loads(line) for line in (root / (name + '.jsonl')).read_text().splitlines()]
    summary = records[-1]
    assert summary['status'] == ('ok' if name == 'normal' else 'incomplete'), summary
    assert summary['records'] == len(records) - 1
    assert (summary['dropped'] == 0) == (name in ('normal', 'missing-posts'))
    cqes = [e for e in records[:-1] if e['event'] == 'cqe_observed']
    assert all(e['wr_id'].startswith('0xffff') and e['generation'] is None for e in cqes)
    assert len({e['thread_slot'] for e in records[:-1]}) == (8 if name == 'threads' else 4)
print('PASS: JSON records, per-thread buffers, overflow detection and lossless WR identifiers')
PY
