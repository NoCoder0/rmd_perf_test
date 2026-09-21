#!/usr/bin/env bash
# SPDX-License-Identifier: MulanPSL-2.0
# One paired scenario per invocation, run remote first and local second.
set -euo pipefail
role=${1:?usage: bash run_memory_compare.sh remote|local baseline|remote-huge|both-huge}
scenario=${2:?missing scenario}
case "$role" in remote|local) ;; *) echo 'invalid role' >&2; exit 2;; esac
case "$scenario" in baseline|remote-huge|both-huge) ;; *) echo 'invalid scenario' >&2; exit 2;; esac
: "${APP_CPU:?retain the previous role-specific app CPU}"
: "${WORKER_CPU:?retain the previous role-specific worker CPU}"
: "${RDMA_600_QP_MAX_SEND_SGE:?retain the previously verified QP cap declaration}"
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
cd "$root"
export RDMA_600_SGL_ITEMS=30
export RDMA_600_SOURCE_SEQUENTIAL=1
backend=aligned
if [[ "$scenario" == both-huge || ( "$scenario" == remote-huge && "$role" == remote ) ]]; then
    backend=hugetlb
fi
memory=(--memory-backend "$backend")
if [[ "$backend" == hugetlb && -n "${HUGEPAGE_KB:-}" ]]; then memory+=(--hugepage-kb "$HUGEPAGE_KB"); fi
if [[ "$role" == remote ]]; then
    network=(--rdma-ip "${RDMA_IP:-192.168.75.87}" --listen "${REMOTE_ENDPOINT:-192.168.75.87:19000}" --max-inflight 0)
else
    network=(--rdma-ip "${RDMA_IP:-192.168.75.86}" --peer "${REMOTE_ENDPOINT:-192.168.75.87:19000}")
fi
common=(--role "$role" --links 1 --mode sgl --pipeline on --blocks 1600 --block-bytes 656
        --notify-every-wrs 32 --warmup 100 --verify-rounds 20 --timeout-sec 120
        --app-cpu "$APP_CPU" --worker-cpu "$WORKER_CPU")
prefix="sgl-${role}-${scenario}"
for suffix in measure.log trace.log compact.json; do
    if [[ -e "${prefix}-${suffix}" ]]; then
        echo "Refusing to overwrite ${prefix}-${suffix}; retain/move previous run logs first." >&2; exit 2
    fi
done
# Both processes must finish each phase successfully. All verify rounds remain enabled.
./build/rdma_600 "${common[@]}" "${network[@]}" "${memory[@]}" --kind measure --rounds 1000 \
    > "${prefix}-measure.log" 2>&1
./build/rdma_600 "${common[@]}" "${network[@]}" "${memory[@]}" --kind trace --trace-rounds 2 \
    > "${prefix}-trace.log" 2>&1
python3 compact_trace.py "${prefix}-trace.log" --measure-log "${prefix}-measure.log" > "${prefix}-compact.json"
echo "Saved ${prefix}-compact.json (full logs retained)"
