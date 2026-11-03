#!/usr/bin/env python3
"""Public orchestration only; this module contains no Minecraft action driver.

A trusted runner-local config supplies schema_version=1, scenario_command and
cleanup_command (argv lists beginning with installed absolute executables), and
an absolute work_dir outside the checkout and upload directory. The scenario
command must launch real Fabric+Krypton/Paper/FRP, collect evidence, and write
MZSTD_REPORT_PATH. No Python fixture or successful process exit certifies a real
client. The independent workflow validator remains the evidence acceptance gate.

All protocol environment names have the MZSTD_ prefix; unprefixed aliases (except
MODE) are also supplied for runner commands. Raw logs/reports remain private in
work_dir. summary.json preserves evidence and metrics for the next validation
step; it is NOT a claim that endpoint/session data has been sanitized.
"""

import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager

try:
    import fcntl
except ImportError:  # Importable for mocked unit tests on non-Linux hosts.
    fcntl = None


class HarnessError(Exception):
    """An orchestration or report-contract failure (never contains command argv)."""


class Cancelled(HarnessError):
    pass


def require(condition, message):
    if not condition:
        raise HarnessError(message)


class NoopLifecycleLog:
    """Disabled logging does not touch files or read the monotonic clock."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def emit(self, event, *, round_id=None, variant=None):
        pass


class LifecycleLog:
    """Allowlisted orchestration phases, never game/mod-load or performance proof.

    Elapsed milliseconds describe this orchestrator, not a measured game interval.
    No exception, command, path, configuration or driver text enters this stream.
    """

    EVENTS = frozenset(("orchestration_started", "orchestration_finished",
                        "orchestration_failed", "orchestration_cancelled",
                        "lock_acquired", "variant_started", "report_received",
                        "cleanup_started", "cleanup_finished", "cleanup_failed"))
    MODES = frozenset(("smoke", "benchmark-pair"))
    VARIANTS = frozenset(("baseline", "candidate"))

    def __init__(self, path, result_dir, mode):
        require(type(mode) is str and mode in self.MODES, "invalid lifecycle mode")
        self.path = Path(path)
        self.result_dir = Path(result_dir)
        self.mode = mode
        self.stream = None
        self.seq = 0
        self.elapsed = 0

    def __enter__(self):
        path = self.path
        require(path.is_absolute() and path.name == "lifecycle.jsonl",
                "invalid lifecycle path")
        # Check lexical ancestors before resolve: resolving first hides symlinks.
        require(all(not parent.is_symlink() for parent in (path.parent, *path.parent.parents))
                and not path.is_symlink(), "lifecycle path must not use symlinks")
        expected = (self.result_dir.parent / "sanitized").resolve(strict=True)
        require(path.parent.is_dir() and path.parent.resolve(strict=True) == expected,
                "lifecycle parent must be the existing sanitized directory")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        directory_fd = None
        fd = None
        try:
            # Pin the checked directory on platforms supporting openat. O_EXCL
            # also rejects existing regular files and dangling symlinks.
            if os.open in os.supports_dir_fd:
                directory_fd = os.open(str(path.parent), os.O_RDONLY
                                       | getattr(os, "O_DIRECTORY", 0)
                                       | getattr(os, "O_NOFOLLOW", 0))
                fd = os.open(path.name, flags, 0o600, dir_fd=directory_fd)
            else:
                fd = os.open(str(path), flags, 0o600)
            require(stat.S_ISREG(os.fstat(fd).st_mode), "lifecycle must be a regular file")
            self.stream = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
            fd = None
            self.started = time.monotonic()
            self.emit("orchestration_started")
        except BaseException:
            if self.stream is not None:
                self.stream.close()
            raise
        finally:
            if fd is not None:
                os.close(fd)
            if directory_fd is not None:
                os.close(directory_fd)
        return self

    def emit(self, event, *, round_id=None, variant=None):
        # Explicit parameters only: no arbitrary dictionary or **extra payload.
        require(type(event) is str and event in self.EVENTS, "invalid lifecycle event")
        require(round_id is None or (type(round_id) is int and round_id > 0),
                "invalid lifecycle round")
        require(variant is None or (type(variant) is str and variant in self.VARIANTS),
                "invalid lifecycle variant")
        require(self.stream is not None and not self.stream.closed, "lifecycle is not open")
        elapsed = max(self.elapsed, int(max(0, time.monotonic() - self.started) * 1000))
        row = {"schema_version": 1, "type": "orchestration_event", "event": event,
               "seq": self.seq + 1, "monotonic_elapsed_ms": elapsed, "mode": self.mode,
               "measurement_produced": False}
        if round_id is not None:
            row["round_id"] = round_id
        if variant is not None:
            row["variant"] = variant
        self.stream.write(json.dumps(row, allow_nan=False) + "\n")
        self.stream.flush()
        self.seq += 1
        self.elapsed = elapsed

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            event = "orchestration_finished"
        elif issubclass(exc_type, (Cancelled, KeyboardInterrupt)):
            event = "orchestration_cancelled"
        else:
            event = "orchestration_failed"
        try:
            self.emit(event)
        except Exception:
            if exc_type is None:
                raise
        finally:
            try:
                self.stream.close()
            except Exception:
                if exc_type is None:
                    raise
        return False


def lifecycle(path, result_dir, mode):
    return NoopLifecycleLog() if path is None else LifecycleLog(path, result_dir, mode)


def numeric(value, positive=False):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and (value > 0 if positive else value >= 0))


def _under(path, parent):
    return path == parent or parent in path.parents


def _command(value):
    require(isinstance(value, list) and bool(value)
            and all(isinstance(v, str) and v and "\0" not in v for v in value),
            "command must be a nonempty argv list")
    exe = Path(value[0])
    require(exe.is_absolute() and exe.is_file() and os.access(exe, os.X_OK),
            "command must start with an installed absolute executable")
    return value


def load_config(path, result_dir):
    with Path(path).open(encoding="utf-8") as stream:
        data = json.load(stream)
    require(isinstance(data, dict) and type(data.get("schema_version")) is int
            and data["schema_version"] == 1, "unsupported config schema")
    for key in ("scenario_command", "cleanup_command"):
        _command(data.get(key))
    raw = data.get("work_dir")
    require(isinstance(raw, str) and Path(raw).is_absolute(),
            "work_dir must be absolute")
    work = Path(raw).resolve()
    checkout = Path(__file__).resolve().parents[2]
    result = Path(result_dir).resolve()
    require(not _under(work, checkout) and not _under(checkout, work),
            "work_dir must be external to the checkout")
    require(not _under(work, result) and not _under(result, work),
            "work_dir and result_dir must be disjoint")
    data["work_dir"] = work
    for key, default in (("startup_timeout_seconds", 120),
                         ("cleanup_timeout_seconds", 30),
                         ("terminate_grace_seconds", 5)):
        value = data.get(key, default)
        require(numeric(value, positive=True), "invalid timeout")
        data[key] = value
    return data


@contextmanager
def work_lock(work):
    """All modes share the same nonblocking Linux flock for this lab."""
    require(fcntl is not None and sys.platform.startswith("linux"),
            "execution requires a Linux runner with fcntl")
    work.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(str(work / ".mzstd-lab.lock"), flags, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise HarnessError("lab work_dir is already locked") from None
        yield
    finally:
        os.close(fd)


@contextmanager
def cancellation_signals():
    previous = {}
    cancelled = False

    def cancel(signum, frame):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            raise Cancelled("run cancelled")

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, cancel)
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _terminate_group(process, grace):
    """The session leader may have exited while descendants are still alive."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run_process(command, env, cwd, log_path, timeout, grace):
    """Never print commands or subprocess output, including on spawn failures."""
    process = None
    with log_path.open("xb") as output:
        os.chmod(log_path, 0o600)
        try:
            process = subprocess.Popen(command, cwd=str(cwd), env=env,
                                       stdin=subprocess.DEVNULL, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise HarnessError("runner command timed out") from None
            require(code == 0, "runner command failed")
        finally:
            if process is not None:
                _terminate_group(process, grace)


def artifact(path):
    path = Path(path).resolve(strict=True)
    require(path.is_file() and path.stat().st_size > 0, "JAR must be nonempty")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return path, digest.hexdigest()


def read_report(path, started_ns, expected, clients, duration, benchmark):
    """Check transport/binding shape only; do not import the validator API."""
    require(stat.S_ISREG(path.lstat().st_mode), "report must be a regular nonsymlink file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(str(path), flags), "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        # Some filesystems expose coarser timestamps than time.time_ns(). The
        # fresh, exclusively created session directory plus session/nonce/hash
        # binding is the primary replay barrier; allow two seconds of rounding.
        require(stat.S_ISREG(info.st_mode) and info.st_mtime_ns >= started_ns - 2_000_000_000,
                "report must be a fresh regular file")
        require(info.st_size <= 16 * 1024 * 1024, "report is too large")
        data = json.load(stream)
    require(isinstance(data, dict) and type(data.get("schema_version")) is int
            and data["schema_version"] == 1 and data.get("status") == "passed",
            "scenario did not return a passed schema-1 report")
    require(data.get("client_runtime") == "minecraft_client",
            "scenario must identify the Minecraft client runtime")
    for key, value in expected.items():
        require(data.get(key) == value, "report binding mismatch: " + key)
    rows = data.get("clients")
    require(isinstance(rows, list) and len(rows) == clients
            and all(isinstance(row, dict) for row in rows), "client list mismatch")
    metrics = data.get("metrics")
    require(isinstance(metrics, dict), "metrics must be an object")
    keys = ["duration_seconds", "raw_bytes", "compressed_wire_bytes",
            "nic_rx_bytes", "nic_tx_bytes", "server_cpu_seconds", "server_rss_bytes"]
    if benchmark:
        keys += ["rtt_sample_count", "rtt_p50_ms", "rtt_p95_ms", "rtt_p99_ms",
                 "rtt_p999_ms", "pool_queue_wait_p99_ms", "event_loop_wait_p99_ms"]
    for key in keys:
        require(numeric(metrics.get(key)), "missing or invalid metric: " + key)
    require(metrics["duration_seconds"] >= duration, "short measured interval")
    require(metrics["raw_bytes"] > 0 and metrics["compressed_wire_bytes"] > 0,
            "traffic measurements must be positive")
    if benchmark:
        require(metrics["rtt_sample_count"] > 0, "RTT samples missing")
    return data


def scenario(config, private, mode, variant, round_id, jars, sha, nonce, args, events=None):
    events = events if events is not None else NoopLifecycleLog()
    session = secrets.token_hex(16)
    folder = private / (str(round_id) + "-" + variant + "-" + session)
    folder.mkdir(mode=0o700)
    report_path = folder / "report.json"
    expected_hashes = {name: row[1] for name, row in jars.items()}
    for name, (path, digest) in jars.items():
        require(artifact(path)[1] == digest, "JAR changed before scenario")
    values = {"MODE": mode, "CLIENTS": str(args.clients),
              "DURATION_SECONDS": str(args.duration_seconds),
              "WARMUP_SECONDS": str(args.warmup_seconds), "COMMIT_SHA": sha,
              "FRP_COMPRESSION": "true", "RUN_NONCE": nonce,
              "FABRIC_JAR": str(jars["fabric"][0]), "PAPER_JAR": str(jars["paper"][0]),
              "FABRIC_SHA256": expected_hashes["fabric"],
              "PAPER_SHA256": expected_hashes["paper"],
              "REPORT_PATH": str(report_path), "SESSION_ID": session}
    env = os.environ.copy()
    for name, value in values.items():
        env["MZSTD_" + name] = value
        if name != "MODE":
            env[name] = value
    timeout = args.duration_seconds + args.warmup_seconds + config["startup_timeout_seconds"]
    started = time.time_ns()
    monotonic_start = time.monotonic()
    try:
        events.emit("variant_started", round_id=round_id, variant=variant)
        run_process(config["scenario_command"], env, config["work_dir"],
                    folder / "scenario.log", timeout, config["terminate_grace_seconds"])
        require(time.monotonic() - monotonic_start >= args.duration_seconds + args.warmup_seconds,
                "scenario returned before the required observation interval")
        data = read_report(report_path, started, {"run_nonce": nonce, "commit_sha": sha,
                           "artifact_sha256": expected_hashes, "session_id": session},
                           args.clients, args.duration_seconds, mode == "benchmark-pair")
        events.emit("report_received", round_id=round_id, variant=variant)
        for name, (path, digest) in jars.items():
            require(artifact(path)[1] == digest, "JAR changed during scenario")
        return data
    finally:
        # Runs for failed launch, malformed/missing evidence, timeout and INT/TERM.
        # A cleanup failure propagates and prevents summary publication.
        active_failure = sys.exc_info()[0] is not None
        try:
            try:
                events.emit("cleanup_started", round_id=round_id, variant=variant)
            except Exception:
                if not active_failure:
                    raise
        finally:
            # Logging errors must not prevent the existing cleanup command.
            try:
                run_process(config["cleanup_command"], env, config["work_dir"],
                            folder / "cleanup.log", config["cleanup_timeout_seconds"],
                            config["terminate_grace_seconds"])
            except BaseException:
                try:
                    events.emit("cleanup_failed", round_id=round_id, variant=variant)
                except Exception:
                    pass  # Preserve the original cleanup failure.
                raise
            else:
                try:
                    events.emit("cleanup_finished", round_id=round_id, variant=variant)
                except Exception:
                    if not active_failure:
                        raise


def summarize(args, rounds, nonce):
    candidate = rounds[-1]["candidate"]
    result = copy.deepcopy(candidate)
    result.update(test_type=args.mode, run_nonce=nonce)
    if args.mode == "benchmark-pair":
        result["paired_rounds"] = rounds
        result["baseline_sha"] = args.baseline_sha
        result["metric_scope"] = "last_candidate"
        result["duration_scope"] = "all_variants_all_rounds"
        # Percentiles and gauges are NOT merged or averaged. All full per-round
        # reports remain the analysis source, including unknown metric fields.
        result["metrics"]["duration_seconds"] = sum(
            row[v]["metrics"]["duration_seconds"]
            for row in rounds for v in ("baseline", "candidate"))
        result["duration_seconds"] = result["metrics"]["duration_seconds"]
    return result


def execute(args):
    with lifecycle(getattr(args, "event_log", None), args.result_dir, args.mode) as events:
        return _execute(args, events)


def _execute(args, events):
    config = load_config(args.config, args.result_dir)
    variants = ("baseline", "candidate") if args.mode == "benchmark-pair" else ("candidate",)
    builds = {}
    for variant in variants:
        prefix = variant + "_" if args.mode == "benchmark-pair" else ""
        sha = (getattr(args, variant + "_sha") if args.mode == "benchmark-pair"
               else args.candidate_sha or os.environ.get("GITHUB_SHA"))
        require(isinstance(sha, str) and bool(sha.strip()), "commit SHA is required")
        builds[variant] = ({name: artifact(getattr(args, prefix + name + "_jar"))
                            for name in ("fabric", "paper")}, sha)
    nonce = os.environ.get("MZSTD_RUN_NONCE")
    if nonce is None:
        nonce = secrets.token_hex(32)
    require(isinstance(nonce, str) and bool(nonce.strip()), "run nonce is empty")
    result_dir = Path(args.result_dir).resolve()
    with work_lock(config["work_dir"]), cancellation_signals():
        events.emit("lock_acquired")
        result_dir.mkdir(parents=True, exist_ok=True)
        require(all(p.name == "summary.json" and not p.is_dir()
                    for p in result_dir.iterdir()), "result_dir must contain only summary.json")
        summary_path = result_dir / "summary.json"
        summary_path.unlink(missing_ok=True)
        private = Path(tempfile.mkdtemp(prefix=".mzstd-private-", dir=config["work_dir"]))
        os.chmod(private, 0o700)
        order = list(variants)
        if args.mode == "benchmark-pair" and args.randomize_order and secrets.randbelow(2):
            order.reverse()
        rounds = []
        for round_id in range(1, args.rounds + 1):
            row = {"round_id": round_id, "order": order[:]}
            for variant in order:
                jars, sha = builds[variant]
                data = scenario(config, private, args.mode, variant, round_id,
                                jars, sha, nonce, args, events=events)
                # Flat metric aliases retain compatibility with the current
                # workflow validator, without dropping the original report.
                entry = copy.deepcopy(data)
                alias_keys = ("duration_seconds", "raw_bytes", "compressed_wire_bytes",
                              "nic_rx_bytes", "nic_tx_bytes", "server_cpu_seconds",
                              "server_rss_bytes", "rtt_sample_count", "rtt_p50_ms",
                              "rtt_p95_ms", "rtt_p99_ms", "rtt_p999_ms",
                              "pool_queue_wait_p99_ms", "event_loop_wait_p99_ms")
                for key in alias_keys:
                    if key in data["metrics"]:
                        entry[key] = data["metrics"][key]
                entry["actual_clients"] = len(data["clients"])
                frp = data.get("frp", {})
                entry["frp_use_compression"] = frp.get("use_compression") if isinstance(frp, dict) else None
                row[variant] = entry
            rounds.append(row)
            order.reverse()
        summary = summarize(args, rounds, nonce)
        # Serialize before creating the public file: malformed/non-finite values
        # fail closed, and no raw log or report is copied into the upload dir.
        payload = json.dumps(summary, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
        try:
            with summary_path.open("x", encoding="utf-8") as stream:
                stream.write(payload)
        except BaseException:
            summary_path.unlink(missing_ok=True)
            raise
    return summary


def positive(value):
    try:
        result = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive finite number") from None
    if not numeric(result, positive=True):
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return result


def nonnegative(value):
    result = float(value)
    if not numeric(result):
        raise argparse.ArgumentTypeError("must be a nonnegative finite number")
    return result


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    modes = root.add_subparsers(dest="mode", required=True)
    for mode in ("smoke", "benchmark-pair"):
        sub = modes.add_parser(mode)
        sub.add_argument("--config", required=True)
        sub.add_argument("--result-dir", required=True)
        sub.add_argument("--event-log",
                         help="new sanitized lifecycle.jsonl; orchestration only, not game/mod-load proof")
        sub.add_argument("--frp-compression", choices=("true",), required=True)
        sub.add_argument("--clients", type=positive_int, default=1 if mode == "smoke" else None,
                         required=mode == "benchmark-pair")
        if mode == "smoke":
            sub.add_argument("--fabric-jar", required=True)
            sub.add_argument("--paper-jar", required=True)
            sub.add_argument("--candidate-sha", "--commit-sha", dest="candidate_sha")
            sub.add_argument("--duration-seconds", type=positive, required=True)
            # 冒烟默认不预热；配置扫描需要先让首批区块流完，否则测的是登录瞬间。
            sub.add_argument("--warmup-seconds", type=nonnegative, default=0)
            sub.set_defaults(rounds=1, warmup_seconds=0, randomize_order=False)
        else:
            for variant in ("baseline", "candidate"):
                for name in ("fabric", "paper"):
                    sub.add_argument("--" + variant + "-" + name + "-jar",
                                     "--" + variant + "-" + name,
                                     dest=variant + "_" + name + "_jar", required=True)
                sub.add_argument("--" + variant + "-sha", required=True)
            sub.add_argument("--duration-minutes", type=positive, required=True)
            sub.add_argument("--rounds", type=positive_int, required=True)
            sub.add_argument("--warmup-minutes", type=nonnegative, default=0)
            sub.add_argument("--randomize-order", action="store_true")
    return root


def parse_args(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.mode == "benchmark-pair":
        if args.rounds < 2:
            cli.error("benchmark-pair requires at least two rounds")
        args.duration_seconds = args.duration_minutes * 60
        args.warmup_seconds = args.warmup_minutes * 60
        if not numeric(args.duration_seconds, positive=True) or not numeric(args.warmup_seconds):
            cli.error("duration and warmup overflow")
    return args


def main(argv=None):
    args = parse_args(argv)
    try:
        execute(args)
    except (Cancelled, KeyboardInterrupt):
        print("lab harness cancelled; no summary accepted", file=sys.stderr)
        return 130
    except Exception as exc:
        # Exception strings from Popen/OSError can contain command arguments.
        print("lab harness failed: " + type(exc).__name__, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
