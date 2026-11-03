#!/usr/bin/env python3
"""Stop everything one scenario run started, and prove nothing survived.

The orchestrator runs this command unconditionally after the scenario, including
after a timeout, a crash or a cancellation. It must therefore be idempotent and
must fail when it cannot prove the state is clean: a leftover client, server or
tunnel process would poison the next variant and the next measurement.

Processes are located twice: through the PID/port state the driver records as it
starts each component, and through a per-scenario environment token stamped on
every child. The token sweep also catches descendants that escaped their process
group after a kill.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import sys
import time
from pathlib import Path

TOKEN_VARIABLE = b"MZSTD_SCENARIO_TOKEN="


class CleanupError(Exception):
    """The environment could not be returned to a known-clean state."""


def scenario_paths() -> tuple[Path, Path]:
    report = Path(os.environ["MZSTD_REPORT_PATH"]).resolve()
    return report.parent / "driver" / "state.json", report.parent / "driver"


def token_pids(token: bytes) -> list[int]:
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            environment = (entry / "environ").read_bytes()
        except OSError:
            continue
        if TOKEN_VARIABLE + token in environment:
            found.append(int(entry.name))
    return found


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def terminate(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return


def force_kill(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            return


def port_listening(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def main() -> int:
    state_path, artifacts = scenario_paths()
    if not state_path.is_file():
        print("cleanup: no scenario state recorded; nothing to stop")
        return 0
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as failure:
        raise CleanupError("scenario state is unreadable: " + type(failure).__name__) from None
    token = str(state.get("token", ""))
    recorded = [pid for pid in state.get("pids", {}).values() if isinstance(pid, int)]
    ports = [port for port in state.get("ports", {}).values() if isinstance(port, int)]

    targets = list(dict.fromkeys(recorded))
    if token:
        targets.extend(pid for pid in token_pids(token.encode("ascii")) if pid not in targets)
    for pid in targets:
        terminate(pid)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and any(pid_alive(pid) for pid in targets):
        time.sleep(0.2)
    for pid in targets:
        if pid_alive(pid):
            force_kill(pid)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and any(pid_alive(pid) for pid in targets):
        time.sleep(0.2)

    survivors = [pid for pid in targets if pid_alive(pid)]
    if survivors:
        raise CleanupError("processes survived cleanup: " + str(survivors))
    if token and token_pids(token.encode("ascii")):
        raise CleanupError("token sweep still finds live scenario processes")
    lingering = [port for port in ports if port_listening(port)]
    if lingering:
        raise CleanupError("ports still accepting connections: " + str(lingering))
    print("cleanup: stopped %d processes, %d ports free" % (len(targets), len(ports)))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CleanupError as failure:
        print("cleanup failed: " + str(failure), file=sys.stderr)
        raise SystemExit(1)
    except KeyError:
        print("cleanup failed: MZSTD_REPORT_PATH is not set", file=sys.stderr)
        raise SystemExit(1)
