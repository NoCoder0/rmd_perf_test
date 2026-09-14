#!/usr/bin/env python3
"""Run and archive one local role of the stage-1 SC-B1 sparse_copy experiment.

The data plane is always the C++ binary.  This wrapper deliberately uses only
the Python standard library for local process orchestration, log collection,
result validation, and report generation.  It never builds, installs, or
copies software. Run it separately with --role remote and --role local on
the corresponding Linux RDMA hosts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


class RunFailure(RuntimeError):
    """A failed or malformed local experiment."""


@dataclass(frozen=True)
class Case:
    name: str
    links: int = 1
    mode: str = "direct"


SC_B1 = Case("SC-B1")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_dump(path: pathlib.Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: pathlib.Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RunFailure(f"cannot parse config {path}: {error}") from error
    if not isinstance(value, dict):
        raise RunFailure("top-level config must be a JSON object")
    return value


def require_string(data: Dict[str, Any], key: str, owner: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RunFailure(f"{owner}.{key} must be a non-empty string")
    return value


def require_absolute_posix_path(value: str, name: str) -> str:
    if not value.startswith("/"):
        raise RunFailure(f"{name} must be an absolute Linux path")
    return value


def require_int(data: Dict[str, Any], key: str, owner: str, *, minimum: int = 0) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise RunFailure(f"{owner}.{key} must be an integer >= {minimum}")
    return value


def require_string_list(data: Dict[str, Any], key: str, owner: str) -> List[str]:
    value = data.get(key)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise RunFailure(f"{owner}.{key} must be a non-empty string array")
    return value


def require_absolute_posix_path_list(data: Dict[str, Any], key: str, owner: str) -> List[str]:
    return [
        require_absolute_posix_path(value, f"{owner}.{key}[{index}]")
        for index, value in enumerate(require_string_list(data, key, owner))
    ]


def require_library_dirs(data: Dict[str, Any], owner: str) -> List[str]:
    if "library_dirs" in data:
        return require_absolute_posix_path_list(data, "library_dirs", owner)
    # Keep a one-entry legacy configuration usable. New configs should list
    # every required dynamic-library directory explicitly.
    return [
        require_absolute_posix_path(require_string(data, "library_dir", owner), f"{owner}.library_dir")
    ]


def validate_host(name: str, host: Any, listener: bool) -> Dict[str, Any]:
    if not isinstance(host, dict):
        raise RunFailure(f"{name} must be an object")
    checked = dict(host)
    checked["binary"] = require_absolute_posix_path(require_string(checked, "binary", name), f"{name}.binary")
    checked["library_dirs"] = require_library_dirs(checked, name)
    checked["rdma_ips"] = require_string_list(checked, "rdma_ips", name)
    checked["app_cpu"] = require_int(checked, "app_cpu", name)
    checked["worker_cpus"] = require_string_list(
        {"worker_cpus": [str(item) for item in checked.get("worker_cpus", [])]}, "worker_cpus", name
    )
    try:
        checked["worker_cpus"] = [int(item) for item in checked["worker_cpus"]]
    except ValueError as error:
        raise RunFailure(f"{name}.worker_cpus must contain integer CPU IDs") from error
    if any(cpu < 0 for cpu in checked["worker_cpus"]):
        raise RunFailure(f"{name}.worker_cpus must contain non-negative CPU IDs")
    if listener:
        checked["oob_ip"] = require_string(checked, "oob_ip", name)
        ports = checked.get("oob_ports")
        if not isinstance(ports, list) or not ports or not all(isinstance(port, int) and 1 <= port <= 65535 for port in ports):
            raise RunFailure(f"{name}.oob_ports must be a non-empty array of TCP port integers")
        checked["oob_ports"] = ports
    return checked


def validate_config(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, int]]:
    local = validate_host("local", raw.get("local"), listener=False)
    remote = validate_host("remote", raw.get("remote"), listener=True)
    if len(local["rdma_ips"]) < 1 or len(remote["rdma_ips"]) < 1:
        raise RunFailure("stage 1 requires one RDMA IP on each host")
    if len(local["worker_cpus"]) < 1 or len(remote["worker_cpus"]) < 1:
        raise RunFailure("stage 1 requires one worker CPU on each host")
    if local["app_cpu"] == local["worker_cpus"][0] or remote["app_cpu"] == remote["worker_cpus"][0]:
        raise RunFailure("stage 1 requires distinct app_cpu and worker_cpus[0] on each host")

    raw_stage = raw.get("stage1", {})
    if not isinstance(raw_stage, dict):
        raise RunFailure("stage1 must be an object when present")
    stage = {
        "verify_rounds": raw_stage.get("verify_rounds", 20),
        "warmup_rounds": raw_stage.get("warmup_rounds", 1000),
        "measure_rounds": raw_stage.get("measure_rounds", 10000),
        "timeout_sec": raw_stage.get("timeout_sec", 10),
        "process_timeout_sec": raw_stage.get("process_timeout_sec", 600),
    }
    for key, value in stage.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RunFailure(f"stage1.{key} must be a non-negative integer")
    if stage["verify_rounds"] == 0:
        raise RunFailure("stage1.verify_rounds must be greater than zero")
    if stage["measure_rounds"] == 0:
        raise RunFailure("stage1.measure_rounds must be greater than zero")
    if not 1 <= stage["timeout_sec"] <= 32767:
        raise RunFailure("stage1.timeout_sec must be in [1, 32767]")
    if stage["process_timeout_sec"] < stage["timeout_sec"]:
        raise RunFailure("stage1.process_timeout_sec must be at least stage1.timeout_sec")
    return {"local": local, "remote": remote}, stage


def library_search_path(host: Dict[str, Any]) -> str:
    return ":".join(host["library_dirs"])


def local_environment(host: Dict[str, Any]) -> Dict[str, str]:
    environment = os.environ.copy()
    inherited = environment.get("LD_LIBRARY_PATH")
    library_path = library_search_path(host)
    environment["LD_LIBRARY_PATH"] = f"{library_path}:{inherited}" if inherited else library_path
    return environment


def stage1_argv(role: str, config: Dict[str, Any], stage: Dict[str, int], kind: str) -> List[str]:
    local = config["local"]
    remote = config["remote"]
    if role == "remote":
        host = remote
        endpoint = f"{remote['oob_ip']}:{remote['oob_ports'][0]}"
        role_args = ["--listen", endpoint]
    elif role == "local":
        host = local
        endpoint = f"{remote['oob_ip']}:{remote['oob_ports'][0]}"
        role_args = ["--peer", endpoint]
    else:
        raise AssertionError(role)

    warmup = 0 if kind == "verify" else stage["warmup_rounds"]
    rounds = 0 if kind == "verify" else stage["measure_rounds"]
    return [
        host["binary"],
        "--role",
        role,
        "--rdma-ip",
        host["rdma_ips"][0],
        *role_args,
        "--kind",
        kind,
        "--verify-rounds",
        str(stage["verify_rounds"]),
        "--warmup",
        str(warmup),
        "--rounds",
        str(rounds),
        "--timeout-sec",
        str(stage["timeout_sec"]),
        "--app-cpu",
        str(host["app_cpu"]),
        "--worker-cpu",
        str(host["worker_cpus"][0]),
        "--links",
        "1",
        "--mode",
        "direct",
    ]


class LinePump(threading.Thread):
    def __init__(self, stream: Any, path: pathlib.Path, console: Optional[Any] = None) -> None:
        super().__init__(daemon=True)
        self._stream = stream
        self._path = path
        self._console = console

    def run(self) -> None:
        with self._path.open("w", encoding="utf-8", errors="replace", newline="") as output:
            for line in iter(self._stream.readline, ""):
                output.write(line)
                output.flush()
                if self._console is not None:
                    print(line, end="", file=self._console, flush=True)
        self._stream.close()


@dataclass
class LocalProcess:
    role: str
    process: subprocess.Popen[str]
    stdout_pump: LinePump
    stderr_pump: LinePump

    def poll(self) -> Optional[int]:
        return self.process.poll()

    def join_pumps(self) -> None:
        self.stdout_pump.join(timeout=5)
        self.stderr_pump.join(timeout=5)


def start_local(host: Dict[str, Any], role: str, argv: List[str], work_dir: pathlib.Path) -> LocalProcess:
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE
    if stdout is None or stderr is None:
        raise AssertionError("subprocess pipes were not created")
    process = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=local_environment(host),
        start_new_session=(os.name == "posix"),
    )
    stdout_pump = LinePump(process.stdout, work_dir / f"{role}.stdout.log", sys.stdout)
    stderr_pump = LinePump(process.stderr, work_dir / f"{role}.stderr.log", sys.stderr)
    stdout_pump.start()
    stderr_pump.start()
    print(f"STARTED role={role} pid={process.pid}", flush=True)
    return LocalProcess(role, process, stdout_pump, stderr_pump)


def wait_for_exit(local: LocalProcess, timeout_sec: int, description: str) -> int:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        code = local.poll()
        if code is not None:
            local.join_pumps()
            return code
        time.sleep(0.1)
    raise RunFailure(f"{description} exceeded its deadline")


def stop_local(local: Optional[LocalProcess]) -> None:
    if local is None:
        return
    if local.poll() is not None:
        local.join_pumps()
        return
    try:
        if os.name == "posix":
            os.killpg(local.process.pid, signal.SIGTERM)
        else:
            local.process.terminate()
        local.process.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        try:
            local.process.kill()
        except OSError:
            pass
    local.join_pumps()


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def local_identity(host: Dict[str, Any]) -> Dict[str, Any]:
    binary = pathlib.Path(host["binary"])
    if not binary.is_file():
        raise RunFailure(f"local binary does not exist: {binary}")
    if not os.access(binary, os.X_OK):
        raise RunFailure(f"local binary is not executable: {binary}")
    library_dirs = [pathlib.Path(directory) for directory in host["library_dirs"]]
    for directory in library_dirs:
        if not directory.is_dir():
            raise RunFailure(f"local library directory does not exist: {directory}")
    try:
        completed = subprocess.run(
            ["ldd", str(binary)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=local_environment(host),
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RunFailure(f"cannot inspect local binary {binary}: {error}") from error
    if completed.returncode != 0:
        raise RunFailure(f"ldd failed for {binary}: {completed.stderr.strip()}")
    if "not found" in completed.stdout:
        raise RunFailure(f"ldd found missing shared libraries for {binary}: {completed.stdout.strip()}")
    return {
        "binary_realpath": str(binary.resolve()),
        "binary_sha256": file_sha256(binary),
        "library_dirs": [str(directory.resolve()) for directory in library_dirs],
        "ldd": completed.stdout.strip(),
        "ldd_stderr": completed.stderr.strip(),
    }


def parse_role_record(path: pathlib.Path, case: Case, role: str) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("case") == case.name and value.get("role") == role:
            records.append(value)
    if len(records) != 1:
        raise RunFailure(
            f"expected exactly one {case.name} role={role} JSON record in {path}, found {len(records)}"
        )
    return records[0]


def validate_result(result: Dict[str, Any], case: Case, kind: str, stage: Dict[str, int]) -> None:
    expected = {
        "schema_version": 3,
        "protocol": "sparse-copy-v3",
        "measurement": "local-sparse-copy",
        "result_role": "local",
        "case": case.name,
        "status": "ok",
        "role": "local",
        "kind": kind,
        "optimization": "stage1.5-AB",
        "data_wait": "busy-poll-relax",
        "deadline_check_interval": 256,
        "callback_allocation": "per-request",
        "links": case.links,
        "blocks": 600,
        "block_bytes": 1024,
        "mode": case.mode,
        "remote_layout": "direct-stride-4096",
        "tls_enabled": False,
        "rounds_in_flight": 1,
        "source_format": "direct-pairs",
        "source_address_count": 600,
        "destination_address_count": 600,
        "request_descriptor_bytes": 9600,
        "request_bytes": 9664,
        "request_send_wr_per_call": 1,
        "data_wr_per_call_expected": 600,
        "completion_send_wr_per_call_expected": 1,
        "imm_events_per_call_expected": 0,
        "ack_wr_per_call": 0,
        "payload_bytes_per_call": 614400,
        "verify_passed": True,
    }
    for key, value in expected.items():
        if result.get(key) != value:
            raise RunFailure(f"result field {key!r} is {result.get(key)!r}, expected {value!r}")
    counter_alignment = result.get("counter_alignment_bytes")
    if not isinstance(counter_alignment, int) or isinstance(counter_alignment, bool) or counter_alignment <= 0:
        raise RunFailure("result field 'counter_alignment_bytes' must be a positive integer")
    metrics = [
        "sparse_copy_avg_us", "sparse_copy_p50_us", "sparse_copy_p95_us", "sparse_copy_p99_us",
        "effective_GBps", "block_Mops", "request_GBps", "measured_wall_seconds",
    ]
    if kind == "verify":
        if result.get("measure_rounds") != 0 or any(result.get(metric) is not None for metric in metrics):
            raise RunFailure("verify run emitted formal performance metrics")
        return
    if result.get("measure_rounds") != stage["measure_rounds"]:
        raise RunFailure("measure result has an unexpected round count")
    for metric in metrics:
        value = result.get(metric)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or value <= 0
        ):
            raise RunFailure(f"measure result {metric} must be a positive number")


def validate_remote_status(result: Dict[str, Any], case: Case, kind: str, stage: Dict[str, int]) -> None:
    expected_calls = stage["verify_rounds"]
    if kind == "measure":
        expected_calls += stage["warmup_rounds"] + stage["measure_rounds"]
    expected = {
        "schema_version": 3,
        "protocol": "sparse-copy-v3",
        "case": case.name,
        "role": "remote",
        "status": "ok",
        "processed_calls": expected_calls,
    }
    for key, value in expected.items():
        if result.get(key) != value:
            raise RunFailure(f"remote status field {key!r} is {result.get(key)!r}, expected {value!r}")


def write_local_report(output: pathlib.Path, kind: str, outcome: Dict[str, Any]) -> None:
    lines = ["# Stage 1 sparse_copy local report", "", f"- Generated: {utc_now()}", f"- Requested kind: `{kind}`", ""]
    result = outcome.get("result")
    if outcome.get("status") == "ok" and isinstance(result, dict):
        lines += [
            f"- Optimization: `{result['optimization']}`",
            f"- Data wait: `{result['data_wait']}`; deadline checked every {result['deadline_check_interval']} spins",
            f"- Callback allocation: `{result['callback_allocation']}`",
            "- Protocol: `sparse-copy-v3`; request: 600 source/destination pairs (9664 bytes); success ACK: 0",
            "",
        ]
    if outcome.get("status") == "ok" and isinstance(result, dict) and kind == "measure":
        lines += [
            "## Measurement",
            "",
            "| Case | sparse_copy avg (us) | p50/p95/p99 (us) | effective GB/s | request GB/s |",
            "|---|---:|---:|---:|---:|",
            f"| SC-B1 | {float(result['sparse_copy_avg_us']):.3f} | "
            f"{float(result['sparse_copy_p50_us']):.3f}/{float(result['sparse_copy_p95_us']):.3f}/"
            f"{float(result['sparse_copy_p99_us']):.3f} | {float(result['effective_GBps']):.3f} | "
            f"{float(result['request_GBps']):.6f} |",
            "",
            "This is one validated local synchronous sparse_copy result. It includes request encoding/transfer/parse, remote submission, data completion, and local handoff; it is not a NIC peak claim.",
            "",
        ]
    elif outcome.get("status") == "ok" and isinstance(result, dict) and kind == "verify":
        lines += ["## Verification", "", "- SC-B1 verification succeeded.", "- No formal bandwidth values are reported for `--kind verify`.", ""]
    else:
        lines += ["## Failure", "", f"- {outcome.get('error', 'No validated local result was produced.')}", ""]
    lines += [
        "## Evidence",
        "",
        "This local invocation has immutable command metadata, local identity output, stdout/stderr, and `result.jsonl` when a result was valid.",
        "",
    ]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run_role(
    config: Dict[str, Any],
    stage: Dict[str, int],
    case: Case,
    role: str,
    kind: str,
    output: pathlib.Path,
    config_path: pathlib.Path,
) -> Dict[str, Any]:
    argv = stage1_argv(role, config, stage, kind)
    manifest: Dict[str, Any] = {
        "schema_version": 3,
        "protocol": "sparse-copy-v3",
        "created_at": utc_now(),
        "case": case.__dict__,
        "role": role,
        "kind": kind,
        "argv": argv,
        "config_path": str(config_path.resolve()),
        "stage1": stage,
    }
    json_dump(output / "manifest.json", manifest)

    local: Optional[LocalProcess] = None
    try:
        manifest["local_identity"] = local_identity(config[role])
        json_dump(output / "manifest.json", manifest)
        local = start_local(config[role], role, argv, output)
        exit_code = wait_for_exit(local, stage["process_timeout_sec"], role)
        manifest["exit_code"] = exit_code
        if exit_code != 0:
            raise RunFailure(f"{role} exited with {exit_code}")
        outcome: Dict[str, Any] = {"repeat": 1, "case": case.name, "role": role, "status": "ok", "path": str(output)}
        if role == "local":
            result = parse_role_record(output / "local.stdout.log", case, role)
            validate_result(result, case, kind, stage)
            (output / "result.jsonl").write_text(
                json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
            )
            outcome["result"] = result
        else:
            result = parse_role_record(output / "remote.stdout.log", case, role)
            validate_remote_status(result, case, kind, stage)
            json_dump(output / "status.json", result)
            outcome["remote_status"] = result
        manifest["finished_at"] = utc_now()
        manifest["status"] = "ok"
        json_dump(output / "manifest.json", manifest)
        return outcome
    except Exception as error:
        manifest["finished_at"] = utc_now()
        manifest["status"] = "failed"
        manifest["error"] = str(error)
        json_dump(output / "manifest.json", manifest)
        return {"repeat": 1, "case": case.name, "role": role, "status": "failed", "path": str(output), "error": str(error)}
    finally:
        stop_local(local)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=pathlib.Path, required=True, help="shared local/remote JSON configuration")
    parser.add_argument("--role", choices=["local", "remote"], required=True, help="role to run on this host")
    parser.add_argument("--suite", choices=["stage1"], required=True)
    parser.add_argument("--case", choices=["SC-B1"], required=True)
    parser.add_argument("--kind", choices=["verify", "measure"], required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True, help="new local result directory")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if sys.platform != "linux":
        raise RunFailure("run.py starts one local RDMA role and must run on a Linux RDMA host")
    raw = read_json(args.config)
    config, stage = validate_config(raw)
    if args.output.exists() and any(args.output.iterdir()):
        raise RunFailure(f"--output already exists and is not empty: {args.output}")
    args.output.mkdir(parents=True, exist_ok=True)
    outcome = run_role(config, stage, SC_B1, args.role, args.kind, args.output, args.config)
    if args.role == "local":
        write_local_report(args.output, args.kind, outcome)
    print(json.dumps({"output": str(args.output), "role": args.role, "status": outcome["status"]}, ensure_ascii=False))
    return 0 if outcome["status"] == "ok" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RunFailure as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2)
