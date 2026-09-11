#!/usr/bin/env python3
"""Run and archive the stage-1 B1 RDMA experiment over SSH.

The data plane is always the C++ binary.  This wrapper deliberately uses only
the Python standard library for process orchestration, log collection, result
validation, and report generation.  It never builds, installs, or copies
software on either host.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import queue
import shlex
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


class RunFailure(RuntimeError):
    """A failed or malformed remote experiment."""


@dataclass(frozen=True)
class Case:
    name: str
    links: int = 1
    mode: str = "plain"
    chunk_items: int = 30
    scatter: str = "pipeline"
    notify: str = "send"


B1 = Case("B1")


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


def validate_host(name: str, host: Any, receiver: bool) -> Dict[str, Any]:
    if not isinstance(host, dict):
        raise RunFailure(f"{name} must be an object")
    checked = dict(host)
    checked["ssh"] = require_string(checked, "ssh", name)
    checked["binary"] = require_absolute_posix_path(require_string(checked, "binary", name), f"{name}.binary")
    checked["library_dir"] = require_absolute_posix_path(
        require_string(checked, "library_dir", name), f"{name}.library_dir"
    )
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
    if receiver:
        checked["oob_ip"] = require_string(checked, "oob_ip", name)
        ports = checked.get("oob_ports")
        if not isinstance(ports, list) or not ports or not all(isinstance(port, int) and 1 <= port <= 65535 for port in ports):
            raise RunFailure(f"{name}.oob_ports must be a non-empty array of TCP port integers")
        checked["oob_ports"] = ports
    return checked


def validate_config(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, int]]:
    sender = validate_host("sender", raw.get("sender"), receiver=False)
    receiver = validate_host("receiver", raw.get("receiver"), receiver=True)
    if len(sender["rdma_ips"]) < 1 or len(receiver["rdma_ips"]) < 1:
        raise RunFailure("stage 1 requires one RDMA IP on each host")
    if len(sender["worker_cpus"]) < 1 or len(receiver["worker_cpus"]) < 1:
        raise RunFailure("stage 1 requires one worker CPU on each host")
    if sender["app_cpu"] == sender["worker_cpus"][0] or receiver["app_cpu"] == receiver["worker_cpus"][0]:
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
    return {"sender": sender, "receiver": receiver}, stage


def quote_command(arguments: Iterable[str]) -> str:
    return " ".join(shlex.quote(str(argument)) for argument in arguments)


def ssh_base(host: Dict[str, Any]) -> List[str]:
    return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host["ssh"]]


def stage1_argv(role: str, config: Dict[str, Any], stage: Dict[str, int], kind: str) -> List[str]:
    sender = config["sender"]
    receiver = config["receiver"]
    if role == "receiver":
        host = receiver
        endpoint = f"{receiver['oob_ip']}:{receiver['oob_ports'][0]}"
        role_args = ["--listen", endpoint]
    elif role == "sender":
        host = sender
        endpoint = f"{receiver['oob_ip']}:{receiver['oob_ports'][0]}"
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
        "plain",
        "--chunk-items",
        "30",
        "--scatter",
        "pipeline",
        "--notify",
        "send",
    ]


def remote_launch_script(host: Dict[str, Any], argv: List[str], pid_file: str) -> str:
    # The shell that owns the ssh command execs the benchmark.  Its PID is saved
    # under a unique run directory so cleanup can target only this invocation.
    binary_command = quote_command(argv)
    library_dir = shlex.quote(host["library_dir"])
    quoted_pid = shlex.quote(pid_file)
    return (
        "umask 077; "
        "mkdir -p /tmp; "
        f"rm -f {quoted_pid}; "
        f"echo $$ > {quoted_pid}; "
        f"export LD_LIBRARY_PATH={library_dir}${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}}; "
        f"exec {binary_command}"
    )


class LinePump(threading.Thread):
    def __init__(self, stream: Any, path: pathlib.Path, notify: Optional[queue.Queue[str]] = None) -> None:
        super().__init__(daemon=True)
        self._stream = stream
        self._path = path
        self._notify = notify

    def run(self) -> None:
        with self._path.open("w", encoding="utf-8", errors="replace", newline="") as output:
            for line in iter(self._stream.readline, ""):
                output.write(line)
                output.flush()
                if self._notify is not None:
                    self._notify.put(line)
        self._stream.close()


@dataclass
class RemoteProcess:
    host: Dict[str, Any]
    role: str
    process: subprocess.Popen[str]
    pid_file: str
    stdout_queue: queue.Queue[str]
    stdout_pump: LinePump
    stderr_pump: LinePump

    def poll(self) -> Optional[int]:
        return self.process.poll()

    def join_pumps(self) -> None:
        self.stdout_pump.join(timeout=5)
        self.stderr_pump.join(timeout=5)


def start_remote(
    host: Dict[str, Any], role: str, argv: List[str], work_dir: pathlib.Path, token: str
) -> RemoteProcess:
    pid_file = f"/tmp/rdma_600_{token}_{role}.pid"
    remote_script = remote_launch_script(host, argv, pid_file)
    command = [*ssh_base(host), remote_script]
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE
    if stdout is None or stderr is None:
        raise AssertionError("subprocess pipes were not created")
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        start_new_session=(os.name == "posix"),
    )
    lines: queue.Queue[str] = queue.Queue()
    stdout_pump = LinePump(process.stdout, work_dir / f"{role}.stdout.log", lines)
    stderr_pump = LinePump(process.stderr, work_dir / f"{role}.stderr.log")
    stdout_pump.start()
    stderr_pump.start()
    return RemoteProcess(host, role, process, pid_file, lines, stdout_pump, stderr_pump)


def wait_for_listening(receiver: RemoteProcess, timeout_sec: int) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if receiver.poll() is not None:
            raise RunFailure(f"receiver exited before LISTENING (exit={receiver.poll()})")
        try:
            line = receiver.stdout_queue.get(timeout=0.2)
        except queue.Empty:
            continue
        if line.startswith("LISTENING "):
            return
    raise RunFailure("receiver did not emit a flushed LISTENING line before startup deadline")


def wait_for_exit(remote: RemoteProcess, timeout_sec: int, description: str) -> int:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        code = remote.poll()
        if code is not None:
            remote.join_pumps()
            return code
        time.sleep(0.1)
    raise RunFailure(f"{description} exceeded its deadline")


def stop_remote(remote: Optional[RemoteProcess]) -> None:
    if remote is None:
        return
    if remote.poll() is not None:
        remote.join_pumps()
        return
    # This command reads only the current run's unpredictable PID file.  It
    # rejects non-numeric content and never kills by program name.
    quoted_pid = shlex.quote(remote.pid_file)
    cleanup = (
        f"if test -r {quoted_pid}; then "
        f"pid=$(cat {quoted_pid}); "
        "case $pid in ''|*[!0-9]*) exit 0;; esac; "
        "kill -TERM \"$pid\" 2>/dev/null || true; "
        "fi"
    )
    try:
        subprocess.run([*ssh_base(remote.host), cleanup], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=15, check=False)
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        if os.name == "posix":
            os.killpg(remote.process.pid, signal.SIGTERM)
        else:
            remote.process.terminate()
        remote.process.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        try:
            remote.process.kill()
        except OSError:
            pass
    remote.join_pumps()


def remote_identity(host: Dict[str, Any]) -> Dict[str, Any]:
    binary = shlex.quote(host["binary"])
    library = shlex.quote(host["library_dir"])
    script = (
        "set -eu; "
        f"printf 'binary_realpath='; readlink -f {binary}; "
        f"printf 'binary_sha256='; sha256sum {binary} | awk '{{print $1}}'; "
        f"printf 'library_dir='; readlink -f {library}; "
        f"printf 'ldd='; LD_LIBRARY_PATH={library}${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}} ldd {binary} | tr '\\n' '|'; echo"
    )
    try:
        completed = subprocess.run(
            [*ssh_base(host), script],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RunFailure(f"cannot inspect {host['ssh']}: {error}") from error
    if completed.returncode != 0:
        raise RunFailure(f"identity inspection failed on {host['ssh']}: {completed.stderr.strip()}")
    result: Dict[str, Any] = {"ssh": host["ssh"], "raw": completed.stdout.strip()}
    for line in completed.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def parse_sender_result(path: pathlib.Path, case: Case) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("case") == case.name:
            records.append(value)
    if len(records) != 1:
        raise RunFailure(f"expected exactly one {case.name} JSON result in {path}, found {len(records)}")
    return records[0]


def validate_result(result: Dict[str, Any], case: Case, kind: str, stage: Dict[str, int]) -> None:
    expected = {
        "case": case.name,
        "status": "ok",
        "role": "sender",
        "kind": kind,
        "links": case.links,
        "blocks": 600,
        "block_bytes": 1024,
        "payload_bytes": 614400,
        "mode": case.mode,
        "chunk_items": case.chunk_items,
        "scatter": case.scatter,
        "notify": case.notify,
        "rounds_in_flight": 1,
        "data_wr_per_round": 600,
        "notify_wr_per_round": 20,
        "ack_wr_per_round": 1,
        "verify_passed": True,
    }
    for key, value in expected.items():
        if result.get(key) != value:
            raise RunFailure(f"result field {key!r} is {result.get(key)!r}, expected {value!r}")
    metrics = ["submit_p50_us", "e2e_p50_us", "e2e_p95_us", "e2e_p99_us", "effective_GBps", "block_Mops"]
    if kind == "verify":
        if result.get("measure_rounds") != 0 or any(result.get(metric) is not None for metric in metrics):
            raise RunFailure("verify run emitted formal performance metrics")
        return
    if result.get("measure_rounds") != stage["measure_rounds"]:
        raise RunFailure("measure result has an unexpected round count")
    for metric in metrics:
        value = result.get(metric)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise RunFailure(f"measure result {metric} must be a non-negative number")


def write_report(output: pathlib.Path, kind: str, runs: List[Dict[str, Any]]) -> None:
    lines = ["# Stage 1 run report", "", f"- Generated: {utc_now()}", f"- Requested kind: `{kind}`", ""]
    successful = [entry["result"] for entry in runs if entry.get("status") == "ok" and "result" in entry]
    failures = [entry for entry in runs if entry.get("status") != "ok"]
    if kind == "measure" and successful:
        e2e = [float(item["e2e_p50_us"]) for item in successful]
        submit = [float(item["submit_p50_us"]) for item in successful]
        bandwidth = [float(item["effective_GBps"]) for item in successful]
        lines += [
            "## Successful measurements",
            "",
            "| Case | Successful repeats | Median e2e p50 (us) | Median submit p50 (us) | Median effective GB/s |",
            "|---|---:|---:|---:|---:|",
            f"| B1 | {len(successful)} | {statistics.median(e2e):.3f} | {statistics.median(submit):.3f} | {statistics.median(bandwidth):.3f} |",
            "",
            "The table aggregates only sender JSON records validated by this wrapper. It is application effective throughput with one round in flight, including scatter and ACK; it is not a NIC peak claim.",
            "",
        ]
    elif kind == "verify" and successful:
        lines += ["## Verification", "", f"- Successful B1 verification runs: {len(successful)}", "- No formal bandwidth values are reported for `--kind verify`.", ""]
    else:
        lines += ["## Result", "", "No successful formal measurement was produced.", ""]
    if failures:
        lines += ["## Failures", ""]
        for failure in failures:
            lines.append(f"- repeat {failure.get('repeat')}: {failure.get('error', 'unknown failure')}")
        lines.append("")
    lines += [
        "## Evidence",
        "",
        "Each repeat has its own directory with immutable command metadata, host identity output, sender/receiver stdout and stderr, and `result.jsonl` when a result was valid.",
        "",
    ]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run_case(
    config: Dict[str, Any], stage: Dict[str, int], case: Case, kind: str, repeat: int, output: pathlib.Path
) -> Dict[str, Any]:
    run_dir = output / f"repeat-{repeat:03d}" / case.name
    run_dir.mkdir(parents=True, exist_ok=False)
    token = uuid.uuid4().hex
    sender_argv = stage1_argv("sender", config, stage, kind)
    receiver_argv = stage1_argv("receiver", config, stage, kind)
    manifest: Dict[str, Any] = {
        "schema_version": 1,
        "created_at": utc_now(),
        "case": case.__dict__,
        "kind": kind,
        "repeat": repeat,
        "sender_argv": sender_argv,
        "receiver_argv": receiver_argv,
        "stage1": stage,
    }
    json_dump(run_dir / "manifest.json", manifest)

    receiver: Optional[RemoteProcess] = None
    sender: Optional[RemoteProcess] = None
    try:
        manifest["sender_identity"] = remote_identity(config["sender"])
        manifest["receiver_identity"] = remote_identity(config["receiver"])
        json_dump(run_dir / "manifest.json", manifest)
        receiver = start_remote(config["receiver"], "receiver", receiver_argv, run_dir, token)
        wait_for_listening(receiver, max(stage["timeout_sec"], 30))
        sender = start_remote(config["sender"], "sender", sender_argv, run_dir, token)
        sender_code = wait_for_exit(sender, stage["process_timeout_sec"], "sender")
        receiver_code = wait_for_exit(receiver, stage["process_timeout_sec"], "receiver")
        manifest["sender_exit_code"] = sender_code
        manifest["receiver_exit_code"] = receiver_code
        if sender_code != 0 or receiver_code != 0:
            raise RunFailure(f"non-zero exit: sender={sender_code}, receiver={receiver_code}")
        result = parse_sender_result(run_dir / "sender.stdout.log", case)
        validate_result(result, case, kind, stage)
        (run_dir / "result.jsonl").write_text(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
        manifest["finished_at"] = utc_now()
        manifest["status"] = "ok"
        json_dump(run_dir / "manifest.json", manifest)
        return {"repeat": repeat, "case": case.name, "status": "ok", "path": str(run_dir), "result": result}
    except Exception as error:
        manifest["finished_at"] = utc_now()
        manifest["status"] = "failed"
        manifest["error"] = str(error)
        json_dump(run_dir / "manifest.json", manifest)
        return {"repeat": repeat, "case": case.name, "status": "failed", "path": str(run_dir), "error": str(error)}
    finally:
        stop_remote(sender)
        stop_remote(receiver)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=pathlib.Path, required=True, help="hosts JSON; passwords are never stored here")
    parser.add_argument("--suite", choices=["stage1"], required=True)
    parser.add_argument("--kind", choices=["verify", "measure"], required=True)
    parser.add_argument("--repeat", type=int, default=1, help="independent fresh-process repeats")
    parser.add_argument("--output", type=pathlib.Path, required=True, help="new result directory")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.repeat < 1:
        raise RunFailure("--repeat must be greater than zero")
    if args.output.exists() and any(args.output.iterdir()):
        raise RunFailure(f"--output already exists and is not empty: {args.output}")
    args.output.mkdir(parents=True, exist_ok=True)
    raw = read_json(args.config)
    config, stage = validate_config(raw)
    root_manifest = {
        "schema_version": 1,
        "created_at": utc_now(),
        "suite": args.suite,
        "kind": args.kind,
        "requested_repeats": args.repeat,
        "config_path": str(args.config.resolve()),
        "stage1": stage,
        "cases": [B1.__dict__],
    }
    json_dump(args.output / "manifest.json", root_manifest)

    runs: List[Dict[str, Any]] = []
    # There is one case in stage 1.  Keeping the loop makes subsequent suites
    # use the same archival contract without duplicating launcher code.
    for repeat in range(1, args.repeat + 1):
        runs.append(run_case(config, stage, B1, args.kind, repeat, args.output))
    root_manifest["finished_at"] = utc_now()
    root_manifest["runs"] = runs
    root_manifest["successful_runs"] = sum(1 for run in runs if run["status"] == "ok")
    root_manifest["failed_runs"] = sum(1 for run in runs if run["status"] != "ok")
    json_dump(args.output / "manifest.json", root_manifest)
    write_report(args.output, args.kind, runs)
    print(json.dumps({"output": str(args.output), "successful_runs": root_manifest["successful_runs"],
                      "failed_runs": root_manifest["failed_runs"]}, ensure_ascii=False))
    return 0 if root_manifest["failed_runs"] == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RunFailure as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2)
