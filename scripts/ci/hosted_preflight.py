#!/usr/bin/env python3
"""Read-only check of a GitHub-hosted runner before a real-client run.

This exists because the hosted path must not require any private runner
variables: the job itself has to prove the environment is usable and say so in
the evidence directory. The report is diagnostic only. It never starts a game,
never touches an account and never claims a measurement: both markers are fixed
to false so nothing here can be mistaken for client or performance evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REQUIRED_EXECUTABLES = ("xvfb-run", "java", "python3", "frps", "frpc")
MIN_FREE_DISK_BYTES = 6 * 1024 ** 3


def java_major(java: str) -> int | None:
    try:
        completed = subprocess.run([java, "-version"], capture_output=True, text=True)
    except OSError:
        return None
    text = (completed.stderr or "") + (completed.stdout or "")
    for token in text.replace('"', " ").split():
        if token[:2].isdigit() and "." in token:
            return int(token.split(".")[0])
        if token.isdigit():
            return int(token)
    return None


def collect() -> tuple[dict, list[str]]:
    problems = []
    executables = {}
    for name in REQUIRED_EXECUTABLES:
        path = shutil.which(name)
        executables[name] = path
        if path is None:
            problems.append("missing_executable:" + name)
    major = java_major(executables.get("java") or "java")
    if major != 25:
        problems.append("java_major_not_25")
    free = shutil.disk_usage(os.environ.get("RUNNER_TEMP", "/tmp")).free
    if free < MIN_FREE_DISK_BYTES:
        problems.append("insufficient_disk")
    report = {
        "schema_version": 1,
        "runner_preflight_only": True,
        "measurement_produced": False,
        "platform": sys.platform,
        "python_version": "%d.%d" % sys.version_info[:2],
        "java_major": major,
        "executables": executables,
        "free_disk_bytes": free,
        "cpu_count": os.cpu_count(),
        "environment": "github-hosted",
        "problems": problems,
    }
    return report, problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-json", required=True)
    args = parser.parse_args(argv)
    report, problems = collect()
    target = Path(args.report_json)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ok" if not problems else "unusable",
                      "problems": problems}, sort_keys=True))
    return 0 if not problems else 1


if __name__ == "__main__":
    raise SystemExit(main())
