#!/usr/bin/env python3
"""Drive one real Minecraft client against a real Paper server through FRP.

The public orchestrator (lab_harness.py) treats this script as the trusted
scenario command. It is handed an isolated, freshly created session directory
through MZSTD_REPORT_PATH and must, in one run:

1. prepare an isolated Paper server with the plugin whose sha256 it was given;
2. prepare an FRP tunnel that really has transport.useCompression enabled;
3. download the pinned Minecraft client runtime and launch one real client per
   requested instance, each with Krypton and the tested Miku-ZSTD build;
4. wait until every client is really in PLAY, hold that state for the requested
   warmup plus measurement interval, and sample traffic/CPU/RSS throughout;
5. write MZSTD_REPORT_PATH with only measurements it actually observed.

Anything missing (no PLAY, no zstd activation, a crash, a short window) fails
the run. Nothing is filled with zeros, no old report is reused, and no protocol
robot stands in for the game client.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import build_testmod
import mc_runtime

DEFAULT_VERSION = "26.2"
DEFAULT_LOADER = "0.19.5"
# 被测模组声明 depends fabric-api，Loader 会强制校验，所以必须同时装入这一份固定版本。
FABRIC_API_VERSION = "0.160.0+26.2"
FABRIC_API_URL = ("https://maven.fabricmc.net/net/fabricmc/fabric-api/fabric-api/"
                  "0.160.0%2B26.2/fabric-api-0.160.0%2B26.2.jar")
FABRIC_API_SHA1 = "58e82722dee4a78235c4b5c06100ae433973f62e"
KRYPTON_PINNED_SHA512 = os.environ.get("MZSTD_KRYPTON_SHA512", "")


class ScenarioError(Exception):
    """A precondition, launch or observation step failed; no report is written."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise ScenarioError(message)


def env(name, default=None):
    value = os.environ.get("MZSTD_" + name, default)
    require(value is not None, "missing environment variable MZSTD_" + name)
    return value


def env_int(name, default):
    raw = os.environ.get("MZSTD_" + name)
    if raw is None or raw == "":
        return default
    return int(raw)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def port_open(host: str, port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_port(host: str, port: int, deadline: float, label: str) -> None:
    while time.monotonic() < deadline:
        if port_open(host, port):
            return
        time.sleep(0.2)
    raise ScenarioError(label + " did not listen on port %d" % port)


def strip_colors(line: str) -> str:
    return re.sub("\u00a7.", "", line)


BYTE_UNITS = {"B": 1, "KB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}


def parse_bytes(text: str) -> int:
    """Parse the plugin's decimal byte formatting back into a count."""
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(B|KB|MB|GB)", text.strip())
    require(match is not None, "unparsable byte value: " + text)
    return int(round(float(match.group(1)) * BYTE_UNITS[match.group(2)]))


def read_netdev(interface: str) -> tuple[int, int]:
    for line in Path("/proc/net/dev").read_text(encoding="ascii").splitlines():
        if ":" not in line:
            continue
        name, _, rest = line.partition(":")
        if name.strip() != interface:
            continue
        fields = rest.split()
        return int(fields[0]), int(fields[8])
    raise ScenarioError("network interface not found: " + interface)


def read_cpu_seconds(pid: int) -> float:
    fields = Path("/proc/%d/stat" % pid).read_text(encoding="ascii").rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return (int(fields[11]) + int(fields[12])) / float(ticks)


def read_peak_rss(pid: int) -> int:
    for line in Path("/proc/%d/status" % pid).read_text(encoding="ascii").splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    raise ScenarioError("peak RSS not reported for pid %d" % pid)


class LogFile:
    """Whole-file reader; these logs stay small enough to re-read on demand."""

    def __init__(self, path: Path):
        self.path = path

    def text(self) -> str:
        if not self.path.is_file():
            return ""
        return self.path.read_text(encoding="utf-8", errors="replace")

    def wait_for(self, pattern: str, deadline: float, since: int = 0) -> str | None:
        compiled = re.compile(pattern)
        while time.monotonic() < deadline:
            body = self.text()
            for line in body[since:].splitlines():
                plain = strip_colors(line)
                if compiled.search(plain):
                    return plain
            time.sleep(0.25)
        return None

    def tail(self, count: int = 40) -> str:
        return "\n".join(self.text().splitlines()[-count:])

class Scenario:
    def __init__(self):
        self.report_path = Path(env("REPORT_PATH")).resolve()
        self.session_dir = self.report_path.parent
        self.artifacts = self.session_dir / "driver"
        self.artifacts.mkdir(mode=0o700, exist_ok=True)
        self.mode = env("MODE", "smoke")
        self.clients_requested = env_int("CLIENTS", 1)
        self.duration = float(env("DURATION_SECONDS", "120"))
        self.warmup = float(env("WARMUP_SECONDS", "0"))
        self.commit_sha = env("COMMIT_SHA")
        self.run_nonce = env("RUN_NONCE")
        self.session_id = env("SESSION_ID")
        self.fabric_jar = Path(env("FABRIC_JAR")).resolve()
        self.paper_jar = Path(env("PAPER_JAR")).resolve()
        self.fabric_sha = env("FABRIC_SHA256")
        self.paper_sha = env("PAPER_SHA256")
        self.threshold = env_int("COMPRESSION_THRESHOLD", 256)
        self.startup_timeout = float(os.environ.get("MZSTD_STARTUP_TIMEOUT_SECONDS", "900"))
        self.java = os.environ.get("MZSTD_JAVA") or shutil.which("java") or "java"
        self.token = secrets.token_hex(16)
        self.processes: dict[str, subprocess.Popen] = {}
        self.state_path = self.artifacts / "state.json"
        self.ports: dict[str, int] = {}
        self.paper_log = LogFile(self.artifacts / "paper.log")

    # -- process bookkeeping -------------------------------------------------

    def write_state(self) -> None:
        payload = {"token": self.token,
                   "pids": {name: process.pid for name, process in self.processes.items()},
                   "ports": self.ports}
        temporary = self.state_path.with_name(self.state_path.name + ".tmp")
        temporary.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(temporary, self.state_path)

    def spawn(self, name: str, argv: list[str], cwd: Path, log: Path,
              extra_env: dict | None = None, stdin=None) -> subprocess.Popen:
        log.parent.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        environment["MZSTD_SCENARIO_TOKEN"] = self.token
        for key, value in (extra_env or {}).items():
            environment[key] = value
        with log.open("wb") as output:
            process = subprocess.Popen(argv, cwd=str(cwd), env=environment,
                                       stdin=stdin if stdin is not None else subprocess.DEVNULL,
                                       stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True)
        self.processes[name] = process
        self.write_state()
        return process

    def stop(self, name: str) -> None:
        process = self.processes.get(name)
        if process is None or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)

    def stop_all(self) -> None:
        for name in reversed(list(self.processes)):
            self.stop(name)

    # -- Paper ---------------------------------------------------------------

    def prepare_paper(self) -> Path:
        server = self.artifacts / "paper"
        server.mkdir(parents=True, exist_ok=True)
        (server / "plugins").mkdir(exist_ok=True)
        shutil.copy2(self.paper_jar, server / "plugins" / self.paper_jar.name)
        (server / "eula.txt").write_text("eula=true\n", encoding="ascii")
        (server / "server.properties").write_text(
            "online-mode=false\n"
            "network-compression-threshold=%d\n"
            "server-port=%d\n"
            "server-ip=127.0.0.1\n"
            "level-type=minecraft\\:flat\n"
            "level-seed=1\n"
            "spawn-protection=0\n"
            "max-players=16\n"
            "view-distance=4\n"
            "simulation-distance=4\n"
            "sync-chunk-writes=false\n"
            "enable-command-block=false\n"
            "motd=MikuZSTD-RealClient\n"
            % (self.threshold, self.ports["paper"]),
            encoding="ascii")
        return server

    def start_paper(self, server: Path) -> None:
        self.spawn("paper",
                   [self.java, "-Xms512M", "-Xmx1536M",
                    "-Dterminal.jline=false", "-Dterminal.ansi=false",
                    "-jar", str(self.paper_jar), "--nogui"],
                   cwd=server, log=self.paper_log.path, stdin=subprocess.PIPE)
        deadline = time.monotonic() + self.startup_timeout
        line = self.paper_log.wait_for(r"Done \([0-9.]+s\)!", deadline)
        if line is not None:
            return
        require(self.processes["paper"].poll() is None,
                "Paper exited during startup:\n" + self.paper_log.tail())
        raise ScenarioError("Paper did not finish startup:\n" + self.paper_log.tail())

    def console(self, command: str, pattern: str, timeout: float = 30.0) -> str | None:
        """Send one console line and return the first matching output line."""
        stream = self.processes["paper"].stdin
        require(stream is not None, "Paper console is not writable")
        since = len(self.paper_log.text())
        stream.write((command + "\n").encode("utf-8"))
        stream.flush()
        return self.paper_log.wait_for(pattern, time.monotonic() + timeout, since=since)

    def traffic_snapshot(self) -> tuple[int, int]:
        """Read the production plugin's cumulative (raw, wire) totals."""
        since = len(self.paper_log.text())
        stream = self.processes["paper"].stdin
        require(stream is not None, "Paper console is not writable")
        stream.write(b"mikuzstd top\n")
        stream.flush()
        deadline = time.monotonic() + 30.0
        summary = self.paper_log.wait_for(r"合计原始 \S+ → \S+ \(", deadline, since=since)
        if summary is None:
            self.paper_log.wait_for(r"没有走 zstd 的连接", time.monotonic() + 5.0, since=since)
            require(summary is not None, "plugin did not report any zstd connection")
            return 0, 0
        match = re.search(r"合计原始 (\S+) → (\S+) \(", summary)
        require(match is not None, "unexpected traffic summary: " + summary)
        return parse_bytes(match.group(1)), parse_bytes(match.group(2))

    # -- FRP -----------------------------------------------------------------

    def start_frp(self) -> None:
        frp = self.artifacts / "frp"
        frp.mkdir(parents=True, exist_ok=True)
        frps = os.environ.get("MZSTD_FRPS_EXECUTABLE") or shutil.which("frps")
        frpc = os.environ.get("MZSTD_FRPC_EXECUTABLE") or shutil.which("frpc")
        require(frps is not None and frpc is not None, "frps/frpc executables are required")
        control = self.ports["frps_control"]
        remote = self.ports["frp_remote"]
        (frp / "frps.toml").write_text(
            'bindPort = %d\n'
            'auth.method = "token"\n'
            'auth.token = "%s"\n'
            'log.level = "info"\n' % (control, self.token), encoding="ascii")
        (frp / "frpc.toml").write_text(
            'serverAddr = "127.0.0.1"\n'
            'serverPort = %d\n'
            'auth.method = "token"\n'
            'auth.token = "%s"\n'
            'log.level = "info"\n'
            '[[proxies]]\n'
            'name = "mikuzstd-real-client"\n'
            'type = "tcp"\n'
            'localIP = "127.0.0.1"\n'
            'localPort = %d\n'
            'remotePort = %d\n'
            'transport.useCompression = true\n'
            % (control, self.token, self.ports["paper"], remote), encoding="ascii")
        self.spawn("frps", [frps, "-c", str(frp / "frps.toml")], cwd=frp,
                   log=self.artifacts / "frps.log")
        deadline = time.monotonic() + 30
        wait_port("127.0.0.1", control, deadline, "frps")
        self.spawn("frpc", [frpc, "-c", str(frp / "frpc.toml")], cwd=frp,
                   log=self.artifacts / "frpc.log")
        wait_port("127.0.0.1", remote, deadline + 30, "frp tunnel")

    # -- client --------------------------------------------------------------

    def prepare_client_runtime(self) -> dict:
        cache = Path(os.environ.get("MZSTD_MC_CACHE",
                                    str(self.artifacts / "mc-cache"))).resolve()
        loader = cache / ("fabric-loader-%s.jar" % DEFAULT_LOADER)
        if not loader.is_file():
            url = ("https://maven.fabricmc.net/net/fabricmc/fabric-loader/%s/"
                   "fabric-loader-%s.jar" % (DEFAULT_LOADER, DEFAULT_LOADER))
            expected = urllib.request.urlopen(url + ".sha1", timeout=60).read().decode("ascii").split()[0]
            mc_runtime.download(url, loader, expected)
        runtime = mc_runtime.prepare(DEFAULT_VERSION, cache, extra_classpath=[loader],
                                     log=lambda message: print(message, flush=True))
        (self.artifacts / "runtime.json").write_text(json.dumps(runtime, indent=2), encoding="utf-8")
        return runtime

    def prepare_mods(self, runtime: dict) -> list[Path]:
        mods = [self.fabric_jar]
        fabric_api = self.artifacts / "fabric-api.jar"
        if not fabric_api.is_file():
            mc_runtime.download(FABRIC_API_URL, fabric_api, FABRIC_API_SHA1)
        mods.append(fabric_api)
        krypton = Path(env("KRYPTON_JAR")).resolve()
        require(krypton.is_file(), "Krypton JAR is missing")
        if KRYPTON_PINNED_SHA512:
            digest = hashlib.sha512(krypton.read_bytes()).hexdigest()
            require(digest == KRYPTON_PINNED_SHA512, "Krypton JAR does not match the pinned sha512")
        mods.append(krypton)
        testmod = build_testmod.build(runtime["classpath"],
                                      self.artifacts / "testmod" / "Miku-ZSTD-CI-Observer.jar")
        mods.append(testmod)
        return mods

    def launch_client(self, index: int, runtime: dict, mods: list[Path]) -> dict:
        game_dir = self.artifacts / ("client-%d" % index)
        game_dir.mkdir(parents=True, exist_ok=True)
        (game_dir / "options.txt").write_text(
            "version:%s\n"
            "onboardAccessibility:false\n"
            "skipMultiplayerWarning:true\n"
            "pauseOnLostFocus:false\n"
            "tutorialStep:none\n"
            "renderDistance:4\n"
            "simulationDistance:4\n"
            "guiScale:2\n"
            "autoJump:false\n"
            "soundCategory_master:0.0\n" % DEFAULT_VERSION, encoding="utf-8")
        evidence = self.artifacts / ("evidence-%d.jsonl" % index)
        evidence.unlink(missing_ok=True)
        username = os.environ.get("MZSTD_CLIENT_USERNAME_PREFIX", "MikuCI") + str(index)
        address = "127.0.0.1:%d" % self.ports["frp_remote"]
        jvm_args = mc_runtime.build_jvm_args(runtime)
        jvm_args = ["-Xms512M", "-Xmx1536M", *jvm_args,
                    "-Dfabric.addMods=" + os.pathsep.join(str(path) for path in mods),
                    "-Dmiku.zstd.ci.evidence=" + str(evidence),
                    "-Dmiku.zstd.ci.interval-ms=250",
                    "-Dfile.encoding=UTF-8"]
        program_args = mc_runtime.build_game_args(runtime, game_dir, username,
                                                 server_address=address, width=1280, height=720)
        argv = ["xvfb-run", "-a", "-s", "-screen 0 1280x720x24", self.java,
                *jvm_args, runtime["main_class"], *program_args]
        log = self.artifacts / ("client-%d.log" % index)
        self.spawn("client-%d" % index, argv, cwd=game_dir, log=log,
                   extra_env={"LIBGL_ALWAYS_SOFTWARE": "1",
                              "MESA_LOADER_DRIVER_OVERRIDE": "llvmpipe"})
        return {"index": index, "username": username, "game_dir": game_dir,
                "evidence": evidence, "log": LogFile(log),
                "client_log": LogFile(game_dir / "logs" / "latest.log"),
                "address": address, "joined_at": None}

    def wait_ready(self, clients: list[dict], started: float) -> None:
        deadline = started + self.startup_timeout
        while time.monotonic() < deadline:
            for client in clients:
                process = self.processes["client-%d" % client["index"]]
                if process.poll() is not None:
                    raise ScenarioError("client %d exited during startup:\n%s"
                                        % (client["index"], client["log"].tail()))
                if client["joined_at"] is not None:
                    continue
                if not client["client_log"].path.is_file():
                    continue
                if (client["username"] + " joined the game") in self.paper_log.text():
                    client["joined_at"] = time.monotonic()
            if all(client["joined_at"] is not None for client in clients):
                return
            time.sleep(0.5)
        detail = "\n".join("client %d log tail:\n%s" % (client["index"], client["log"].tail(25))
                            for client in clients)
        raise ScenarioError("clients did not reach PLAY in time\n" + detail)

    def install_play_watch(self, clients: list[dict]) -> None:
        deadline = time.monotonic() + min(self.startup_timeout, 300.0)
        while time.monotonic() < deadline:
            if all(self.play_state(client)["playing"] for client in clients):
                return
            for client in clients:
                process = self.processes["client-%d" % client["index"]]
                require(process.poll() is None,
                        "client %d exited before PLAY:\n%s" % (client["index"], client["log"].tail()))
            time.sleep(0.5)
        raise ScenarioError("clients never reported a PLAY state")

    def play_state(self, client: dict) -> dict:
        state = {"playing": False, "samples": 0, "ticks": 0, "inbound_bytes": 0,
                 "inbound_messages": 0, "play_ms": 0, "position": None}
        evidence = client["evidence"]
        if not evidence.is_file():
            return state
        for line in evidence.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("type") != "sample":
                continue
            state["samples"] += 1
            if record.get("playing"):
                state["playing"] = True
                state["ticks"] = max(state["ticks"], record.get("ticks", 0))
                state["position"] = [record.get("x"), record.get("y"), record.get("z")]
            state["inbound_bytes"] = max(state["inbound_bytes"], record.get("inbound_bytes", 0))
            state["inbound_messages"] = max(state["inbound_messages"],
                                            record.get("inbound_messages", 0))
            state["play_ms"] = max(state["play_ms"], record.get("play_ms", 0))
        return state

    def boot_record(self, client: dict) -> dict:
        evidence = client["evidence"]
        require(evidence.is_file(), "client evidence file is missing")
        for line in evidence.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("type") == "boot":
                return record
        raise ScenarioError("client evidence has no boot record")

    def loaded_jar_digest(self, boot: dict, mod_id: str) -> str | None:
        for mod in boot.get("mods", []):
            if mod.get("id") != mod_id:
                continue
            for path in mod.get("origin_paths", []):
                candidate = Path(path)
                if candidate.is_file():
                    return sha256_file(candidate)
        return None

    def client_zstd_activated(self, clients: list[dict]) -> bool:
        marker = "Client downstream zstd activated"
        for client in clients:
            body = client["client_log"].text() + "\n" + client["log"].text()
            if marker in body:
                return True
        return False

    # -- measurement ---------------------------------------------------------

    def measure(self, clients: list[dict]) -> dict:
        interface = os.environ.get("MZSTD_NIC", "lo")
        paper_pid = self.processes["paper"].pid
        raw_before, wire_before = self.traffic_snapshot()
        rx_before, tx_before = read_netdev(interface)
        cpu_before = read_cpu_seconds(paper_pid)
        observer_before = sum(self.play_state(client)["inbound_bytes"] for client in clients)
        started = time.monotonic()
        while time.monotonic() - started < self.duration:
            for client in clients:
                process = self.processes["client-%d" % client["index"]]
                require(process.poll() is None,
                        "client %d exited during the measured interval" % client["index"])
                require((client["username"] + " lost connection") not in self.paper_log.text(),
                        "client %d disconnected during the measured interval" % client["index"])
            time.sleep(1.0)
        elapsed = time.monotonic() - started
        raw_after, wire_after = self.traffic_snapshot()
        rx_after, tx_after = read_netdev(interface)
        cpu_after = read_cpu_seconds(paper_pid)
        states = [self.play_state(client) for client in clients]
        observer_after = sum(state["inbound_bytes"] for state in states)
        return {
            "duration_seconds": round(elapsed, 3),
            "raw_bytes": max(0, raw_after - raw_before),
            "compressed_wire_bytes": max(0, wire_after - wire_before),
            "nic_rx_bytes": max(0, rx_after - rx_before),
            "nic_tx_bytes": max(0, tx_after - tx_before),
            "server_cpu_seconds": round(max(0.0, cpu_after - cpu_before), 3),
            "server_rss_bytes": read_peak_rss(paper_pid),
            "observed_client_inbound_bytes": max(0, observer_after - observer_before),
            "states": states,
        }

    # -- report --------------------------------------------------------------

    def build_report(self, clients: list[dict], measurement: dict,
                     boots: list[dict]) -> dict:
        duration = measurement["duration_seconds"]
        states = measurement["states"]
        rows = []
        for client, state in zip(clients, states):
            rows.append({
                "id": "client-%d" % client["index"],
                "joined": client["joined_at"] is not None,
                "play_traffic_received": bool(state["playing"] and state["inbound_messages"] > 0),
                "disconnected": False,
                "active_seconds": round(min(duration, state["play_ms"] / 1000.0), 3),
            })
        boot_mods = {mod["id"]: mod.get("version") for mod in boots[0].get("mods", [])}
        loader_digest = self.loaded_jar_digest(boots[0], "miku_zstd")
        flags = {
            "actual_minecraft_client": "minecraft" in boot_mods,
            "fabric_loaded": "fabricloader" in boot_mods,
            "krypton_loaded": "krypton" in boot_mods,
            "miku_zstd_loaded": "miku_zstd" in boot_mods,
            "login_success": self.client_zstd_activated(clients),
        }
        for name, value in flags.items():
            require(value, "client evidence missing: " + name)
        for row in rows:
            require(row["joined"] and row["play_traffic_received"],
                    "client %s did not join or receive PLAY traffic" % row["id"])
            require(row["active_seconds"] >= duration,
                    "client %s PLAY interval shorter than the measured window" % row["id"])
        require(measurement["raw_bytes"] > 0 and measurement["compressed_wire_bytes"] > 0,
                "no compressed traffic was observed")
        report = {
            "schema_version": 1,
            "status": "passed",
            "test_type": self.mode,
            "client_runtime": "minecraft_client",
            "commit_sha": self.commit_sha,
            "run_nonce": self.run_nonce,
            "session_id": self.session_id,
            "artifact_sha256": {"fabric": self.fabric_sha, "paper": self.paper_sha},
            "client": dict(flags,
                           clients=rows,
                           mod_versions=boot_mods,
                           loaded_miku_zstd_sha256=loader_digest,
                           loaded_miku_zstd_matches_artifact=loader_digest == self.fabric_sha),
            "clients": rows,
            "frp": {"use_compression": True,
                    "entry_port": self.ports["frp_remote"],
                    "entry": "127.0.0.1:%d" % self.ports["frp_remote"]},
            "metrics": {key: value for key, value in measurement.items() if key != "states"},
            "measurement": {
                "compression_threshold": self.threshold,
                "warmup_seconds": self.warmup,
                "sampling_interval_ms": 250,
                "interface": os.environ.get("MZSTD_NIC", "lo"),
                "traffic_source": ("production plugin counters read through /mikuzstd top before "
                                   "and after the window; delta only"),
                "traffic_granularity_bytes": 50000,
                "rss_statistic": "peak VmHWM of the Paper JVM process",
                "client_inbound_bytes_source": ("CI observation mod Netty inbound counter; "
                                                "reported separately from plugin counters"),
            },
        }
        require(report["client"]["loaded_miku_zstd_matches_artifact"],
                "the loaded Miku-ZSTD jar is not the tested artifact")
        return report

    def write_report(self, report: dict) -> None:
        payload = json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        temporary = self.report_path.with_name(self.report_path.name + ".tmp")
        temporary.write_text(payload, encoding="utf-8")
        os.replace(temporary, self.report_path)

    # -- entry ---------------------------------------------------------------

    def run(self) -> int:
        require(sha256_file(self.fabric_jar) == self.fabric_sha, "Fabric JAR digest changed")
        require(sha256_file(self.paper_jar) == self.paper_sha, "Paper JAR digest changed")
        require(self.clients_requested >= 1, "at least one client is required")
        self.ports = {"paper": free_port(),
                      "frps_control": free_port(),
                      "frp_remote": free_port()}
        self.write_state()
        try:
            server = self.prepare_paper()
            self.start_paper(server)
            self.start_frp()
            runtime = self.prepare_client_runtime()
            mods = self.prepare_mods(runtime)
            started = time.monotonic()
            clients = [self.launch_client(index, runtime, mods)
                       for index in range(self.clients_requested)]
            self.wait_ready(clients, started)
            self.install_play_watch(clients)
            if self.warmup > 0:
                time.sleep(self.warmup)
            boots = [self.boot_record(client) for client in clients]
            measurement = self.measure(clients)
            report = self.build_report(clients, measurement, boots)
            self.write_report(report)
            print(json.dumps({"status": "report_written",
                              "duration_seconds": report["metrics"]["duration_seconds"],
                              "clients": len(report["clients"])}, sort_keys=True))
            return 0
        finally:
            self.stop_all()


def main() -> int:
    try:
        return Scenario().run()
    except ScenarioError as failure:
        print("scenario failed: " + str(failure), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError) as failure:
        print("scenario error: " + type(failure).__name__ + ": " + str(failure), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
