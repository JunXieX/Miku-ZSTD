#!/usr/bin/env python3
"""Read-only runner-local schema-1 preflight; standard library only.

Usage: python runner_doctor.py --config ABS --report-json ABS
The existing scenario_command, cleanup_command and external work_dir remain
required, following lab_harness.load_config. runtime contains java_executable,
frps_executable, frpc_executable, minecraft_jar, fabric_api_jar, krypton_jar,
optional network_interfaces (simple Linux names), and optional display_mode
(existing or xvfb); xvfb additionally requires runtime.xvfb_executable.

Only version commands and file metadata are checked. No scenario, cleanup, game,
profile, Xvfb service or sampling driver is started or created. Mod metadata and
hashes establish neither loading nor compatibility. ready is NOT acceptance of
real client startup, game actions, mod loading or a measurement driver. Reports
are private, atomic 0600 files outside the configuration directory.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys
import tempfile
import zipfile


ISSUE_CODES = frozenset({
    "PLATFORM_UNSUPPORTED", "PYTHON_UNSUPPORTED", "HOST_FACTS_UNAVAILABLE",
    "CONFIG_PATH_INVALID", "CONFIG_PERMISSIONS_INVALID", "CONFIG_INVALID",
    "SCENARIO_COMMAND_INVALID", "CLEANUP_COMMAND_INVALID", "WORK_DIR_INVALID",
    "RUNTIME_INVALID", "JAVA_EXECUTABLE_INVALID", "FRPS_EXECUTABLE_INVALID",
    "FRPC_EXECUTABLE_INVALID", "JAVA_PROBE_FAILED", "JAVA_VERSION_INVALID",
    "FRPS_PROBE_FAILED", "FRPC_PROBE_FAILED", "FRP_VERSION_INVALID",
    "FRP_VERSION_MISMATCH", "MINECRAFT_JAR_INVALID", "FABRIC_API_JAR_INVALID",
    "KRYPTON_JAR_INVALID", "DISPLAY_MODE_INVALID", "DISPLAY_MISSING",
    "XVFB_EXECUTABLE_INVALID", "NETWORK_INTERFACES_INVALID",
    "NETWORK_COUNTER_UNAVAILABLE", "REPORT_PATH_INVALID",
    "REPORT_WRITE_FAILED", "ARGUMENTS_INVALID", "INTERNAL_CHECK_FAILED",
})
FACT_KEYS = frozenset({
    "arch", "kernel", "python", "cpu_count", "physical_memory_bytes",
    "java_major", "frp_version", "mod_versions", "artifact_sha256",
})
# Deliberately exclude arbitrary metadata text, paths, endpoints and banners.
VERSION = re.compile(r"[0-9]+(?:\.[0-9]+){0,3}(?:-(?:alpha|beta|rc|snapshot)(?:[.-]?[0-9]+)?)?(?:\+[0-9]+(?:\.[0-9]+){0,3})?", re.ASCII | re.IGNORECASE)
# Mod releases may use bounded alphanumeric SemVer build metadata (for example +mc26.2).
MOD_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+){0,3}(?:-[0-9A-Za-z][0-9A-Za-z.-]{0,63})?(?:\+[0-9A-Za-z][0-9A-Za-z.-]{0,63})?", re.ASCII)
SIMPLE_INTERFACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,14}", re.ASCII)
MAX_JSON_BYTES = 1024 * 1024


class CheckFailure(Exception):
    """Carries only a fixed, public issue code."""


def require(condition, code):
    if not condition:
        raise CheckFailure(code)


def absolute_path(value, code):
    require(isinstance(value, str) and bool(value) and "\0" not in value, code)
    path = Path(value)
    require(path.is_absolute(), code)
    return path


def installed_executable(value, code):
    path = absolute_path(value, code)
    require(path.is_file() and os.access(path, os.X_OK), code)
    return path


def _config_permissions(info):
    if sys.platform.startswith("linux"):
        require(info.st_uid in (0, os.getuid()) and not info.st_mode & 0o077,
                "CONFIG_PERMISSIONS_INVALID")


def read_config(path):
    """Validate the same base contract as load_config without reopening paths."""
    path = absolute_path(path, "CONFIG_PATH_INVALID")
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode), "CONFIG_PATH_INVALID")
    _config_permissions(info)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(path, flags), "r", encoding="utf-8") as stream:
        opened = os.fstat(stream.fileno())
        require(stat.S_ISREG(opened.st_mode) and (opened.st_dev, opened.st_ino) ==
                (info.st_dev, info.st_ino), "CONFIG_PATH_INVALID")
        _config_permissions(opened)
        require(opened.st_size <= MAX_JSON_BYTES, "CONFIG_INVALID")
        data = json.loads(stream.read(MAX_JSON_BYTES + 1))
    require(isinstance(data, dict) and type(data.get("schema_version")) is int
            and data["schema_version"] == 1, "CONFIG_INVALID")
    return data


def _under(path, parent):
    return path == parent or parent in path.parents


def validate_base(data, report_parent, issues):
    for key, code in (("scenario_command", "SCENARIO_COMMAND_INVALID"),
                      ("cleanup_command", "CLEANUP_COMMAND_INVALID")):
        try:
            command = data.get(key)
            require(isinstance(command, list) and bool(command) and
                    all(isinstance(arg, str) and arg and "\0" not in arg
                        for arg in command), code)
            installed_executable(command[0], code)
        except Exception:
            issues.append(code)
    try:
        work = absolute_path(data.get("work_dir"), "WORK_DIR_INVALID").resolve()
        checkout = Path(__file__).resolve().parents[2]
        require(not _under(work, checkout) and not _under(checkout, work) and
                not _under(work, report_parent) and not _under(report_parent, work),
                "WORK_DIR_INVALID")
    except Exception:
        issues.append("WORK_DIR_INVALID")
    for key in ("startup_timeout_seconds", "cleanup_timeout_seconds",
                "terminate_grace_seconds"):
        value = data.get(key, 1)
        if not (type(value) in (int, float) and math.isfinite(value) and value > 0):
            issues.append("CONFIG_INVALID")


def host_facts(report):
    facts = report["facts"]
    machine = platform.machine().lower()
    facts["arch"] = "x86_64" if machine in ("x86_64", "amd64") else "unsupported"
    if not sys.platform.startswith("linux") or facts["arch"] != "x86_64":
        report["issues"].append("PLATFORM_UNSUPPORTED")
    facts["python"] = ".".join(str(part) for part in sys.version_info[:3])
    if sys.version_info[:2] < (3, 10):
        report["issues"].append("PYTHON_UNSUPPORTED")
    # Kernel numeric version only: distro release suffixes may contain hostnames.
    kernel = re.match(r"[0-9]+(?:\.[0-9]+){0,3}", platform.release(), re.ASCII)
    if kernel:
        facts["kernel"] = kernel.group()
    count = os.cpu_count()
    if type(count) is int and count > 0:
        facts["cpu_count"] = count
    try:
        memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        require(type(memory) is int and memory > 0, "HOST_FACTS_UNAVAILABLE")
        facts["physical_memory_bytes"] = memory
    except Exception:
        report["issues"].append("HOST_FACTS_UNAVAILABLE")


def probe(path, argument):
    """Only -version/-v; captured bytes are never printed or added to reports."""
    result = subprocess.run([str(path), argument], shell=False, timeout=10,
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, check=False)
    require(result.returncode == 0, "INTERNAL_CHECK_FAILED")
    parts = []
    for value in (result.stdout, result.stderr):
        if isinstance(value, bytes):
            parts.append(value[:65536].decode("utf-8", errors="replace"))
        elif isinstance(value, str):
            parts.append(value[:65536])
    return "\n".join(parts)


def command_facts(runtime, report):
    issues, facts = report["issues"], report["facts"]
    paths = {}
    for name in ("java", "frps", "frpc"):
        try:
            paths[name] = installed_executable(runtime.get(name + "_executable"),
                                                name.upper() + "_EXECUTABLE_INVALID")
        except Exception:
            issues.append(name.upper() + "_EXECUTABLE_INVALID")
    if "java" in paths:
        try:
            output = probe(paths["java"], "-version")
        except Exception:
            issues.append("JAVA_PROBE_FAILED")
        else:
            match = re.search(r'(?m)^(?:openjdk|java) (?:version )?"([0-9]+)(?:[.\-+"]|$)', output)
            if match:
                major = int(match.group(1))
                facts["java_major"] = major
            if not match or facts.get("java_major") != 25:
                issues.append("JAVA_VERSION_INVALID")
    versions = {}
    for name in ("frps", "frpc"):
        if name not in paths:
            continue
        try:
            output = probe(paths[name], "-v").strip()
        except Exception:
            issues.append(name.upper() + "_PROBE_FAILED")
        else:
            value = output.removeprefix("v")
            if VERSION.fullmatch(value):
                versions[name] = value
            else:
                issues.append("FRP_VERSION_INVALID")
    if len(versions) == 2:
        if versions["frps"] != versions["frpc"]:
            issues.append("FRP_VERSION_MISMATCH")
        else:
            facts["frp_version"] = versions["frps"]


def artifact_metadata(value, member, expected_id, mod=False):
    path = absolute_path(value, "INTERNAL_CHECK_FAILED")
    require(path.is_file(), "INTERNAL_CHECK_FAILED")
    with path.open("rb") as stream:
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            matches = [entry for entry in archive.infolist() if entry.filename == member]
            require(len(matches) == 1 and matches[0].file_size <= MAX_JSON_BYTES,
                    "INTERNAL_CHECK_FAILED")
            metadata = json.loads(archive.read(matches[0]))
    require(isinstance(metadata, dict) and metadata.get("id") == expected_id,
            "INTERNAL_CHECK_FAILED")
    version = metadata.get("version") if mod else None
    if mod:
        require(isinstance(version, str) and len(version) <= 128 and MOD_VERSION.fullmatch(version),
                "INTERNAL_CHECK_FAILED")
    return version, digest.hexdigest()


def artifact_facts(runtime, report):
    hashes, versions = {}, {}
    for key, member, identifier, code in (
            ("minecraft", "version.json", "26.2", "MINECRAFT_JAR_INVALID"),
            ("fabric_api", "fabric.mod.json", "fabric-api", "FABRIC_API_JAR_INVALID"),
            ("krypton", "fabric.mod.json", "krypton", "KRYPTON_JAR_INVALID")):
        try:
            version, digest = artifact_metadata(runtime.get(key + "_jar"), member,
                                                identifier, mod=key != "minecraft")
            hashes[key] = digest
            if version is not None:
                versions[identifier] = version
        except Exception:
            report["issues"].append(code)
    if hashes:
        report["facts"]["artifact_sha256"] = hashes
    if versions:
        report["facts"]["mod_versions"] = versions


def display_check(runtime, issues):
    mode = runtime.get("display_mode")
    if mode is None and "display_mode" not in runtime:
        return
    if mode == "existing":
        if not os.environ.get("DISPLAY", "").strip():
            issues.append("DISPLAY_MISSING")
    elif mode == "xvfb":
        try:
            installed_executable(runtime.get("xvfb_executable"), "XVFB_EXECUTABLE_INVALID")
        except Exception:
            issues.append("XVFB_EXECUTABLE_INVALID")
    else:
        issues.append("DISPLAY_MODE_INVALID")


def network_facts(runtime, report):
    names = runtime.get("network_interfaces", [])
    if not (isinstance(names, list) and len(names) <= 64 and
            all(isinstance(name, str) and SIMPLE_INTERFACE.fullmatch(name) and
                name not in (".", "..") for name in names) and
            len(set(names)) == len(names)):
        report["issues"].append("NETWORK_INTERFACES_INVALID")
        return
    rows = []
    for index, name in enumerate(names):
        try:
            base = Path("/sys/class/net") / name / "statistics"
            values = [(base / (direction + "_bytes")).read_text(encoding="ascii").strip()
                      for direction in ("rx", "tx")]
            require(all(re.fullmatch(r"[0-9]{1,30}", value) for value in values),
                    "NETWORK_COUNTER_UNAVAILABLE")
            rows.append({"label": "nic-" + str(index + 1),
                         "rx_bytes": int(values[0]), "tx_bytes": int(values[1])})
        except Exception:
            report["issues"].append("NETWORK_COUNTER_UNAVAILABLE")
    if names:
        report["network"] = rows


def collect(config, report_parent):
    report = {"schema_version": 1, "type": "runner_preflight_only",
              "measurement_produced": False, "ready": False, "issues": [], "facts": {}}
    try:
        host_facts(report)
        data = read_config(config)
        validate_base(data, report_parent, report["issues"])
        runtime = data.get("runtime")
        if not isinstance(runtime, dict):
            report["issues"].append("RUNTIME_INVALID")
        else:
            artifact_facts(runtime, report)
            display_check(runtime, report["issues"])
            # Do not execute anything or read Linux counters on a wrong host.
            if not {"PLATFORM_UNSUPPORTED", "PYTHON_UNSUPPORTED"}.intersection(report["issues"]):
                command_facts(runtime, report)
                network_facts(runtime, report)
    except CheckFailure as error:
        report["issues"].append(str(error) if str(error) in ISSUE_CODES else "INTERNAL_CHECK_FAILED")
    except Exception:
        report["issues"].append("CONFIG_INVALID")
    report["issues"] = sorted(set(report["issues"]))
    report["ready"] = not report["issues"]
    return report


def report_target(config, output):
    config = absolute_path(config, "CONFIG_PATH_INVALID")
    output = absolute_path(output, "REPORT_PATH_INVALID")
    parent = output.parent.resolve(strict=True)
    require(parent.is_dir(), "REPORT_PATH_INVALID")
    target = parent / output.name
    require(not _under(target, config.parent.resolve()), "REPORT_PATH_INVALID")
    try:
        info = target.lstat()
    except FileNotFoundError:
        pass
    else:
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "REPORT_PATH_INVALID")
        require(not config.exists() or not os.path.samefile(config, target), "REPORT_PATH_INVALID")
    require(target != config.resolve(), "REPORT_PATH_INVALID")
    return target


def write_report(config, target, report):
    temporary = None
    try:
        target = report_target(config, str(target))
        fd, temporary = tempfile.mkstemp(prefix=".runner-doctor-", dir=target.parent)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            os.chmod(temporary, 0o600)
            json.dump(report, stream, ensure_ascii=True, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        report_target(config, str(target))
        os.replace(temporary, target)
        temporary = None
    finally:
        if temporary is not None:
            os.unlink(temporary)


class PrivateParser(argparse.ArgumentParser):
    def error(self, message):
        raise CheckFailure("ARGUMENTS_INVALID")


def main(argv=None):
    parser = PrivateParser(prog="runner_doctor.py", description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", required=True)
    parser.add_argument("--report-json", required=True)
    try:
        args = parser.parse_args(argv)
        target = report_target(args.config, args.report_json)
    except CheckFailure as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception:
        print("REPORT_PATH_INVALID", file=sys.stderr)
        return 1
    report = collect(args.config, target.parent)
    try:
        write_report(args.config, target, report)
    except Exception:
        print("REPORT_WRITE_FAILED", file=sys.stderr)
        return 1
    print("RUNNER_PREFLIGHT_READY" if report["ready"] else "RUNNER_PREFLIGHT_FAILED")
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
