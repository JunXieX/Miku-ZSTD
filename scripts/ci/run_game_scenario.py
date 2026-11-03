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
import math
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

DEFAULT_VERSION = os.environ.get("MZSTD_MC_VERSION", "26.2")
DEFAULT_LOADER = os.environ.get("MZSTD_LOADER_VERSION", "0.19.5")
# 被测模组声明 depends fabric-api，Loader 会强制校验，所以必须同时装入这一份固定版本。
FABRIC_API_VERSION = "0.160.0+26.2"
FABRIC_API_URL = ("https://maven.fabricmc.net/net/fabricmc/fabric-api/fabric-api/"
                  "0.160.0%2B26.2/fabric-api-0.160.0%2B26.2.jar")
FABRIC_API_SHA1 = "58e82722dee4a78235c4b5c06100ae433973f62e"
# Paper 服务端与"被测插件"是两件不同的东西：报告里的 paper 摘要指的是插件成品，
# 服务端本体固定到具体构建号与内容摘要，避免每次跑到不同构建。
PAPER_SERVER_BUILD = 135
PAPER_SERVER_URL = ("https://fill-data.papermc.io/v1/objects/"
                    "ef3dbc68b013deafdbbee3bebf1ba351671ebcda94a8f136809b7532d5ba2fe1/"
                    "paper-26.2-135.jar")
PAPER_SERVER_SHA256 = "ef3dbc68b013deafdbbee3bebf1ba351671ebcda94a8f136809b7532d5ba2fe1"
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


def percentile(samples: list[float], quantile: float) -> float:
    """Nearest-rank 分位点；空样本返回 0。"""
    if not samples:
        return 0.0
    ordered = sorted(samples)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


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


def status_bytes(pid: int, field: str) -> int:
    """One Vm* field from /proc/<pid>/status, in bytes."""
    for line in Path("/proc/%d/status" % pid).read_text(encoding="ascii").splitlines():
        if line.startswith(field + ":"):
            return int(line.split()[1]) * 1024
    return 0


def process_tree(root_pid: int) -> list[int]:
    """root_pid plus every descendant, resolved from /proc.

    A client instance is not one process: xvfb-run spawns Xvfb and the game JVM.
    Per-instance capacity numbers must therefore cover the whole tree, otherwise
    the display server's memory is silently left out.
    """
    children: dict[int, list[int]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text(encoding="ascii").rsplit(")", 1)[1].split()
            parent = int(fields[1])
        except (OSError, IndexError, ValueError):
            continue
        children.setdefault(parent, []).append(int(entry.name))
    tree = [root_pid]
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        for child in children.get(pid, ()):
            tree.append(child)
            stack.append(child)
    return tree


def tree_cpu_seconds(root_pid: int) -> float:
    ticks = os.sysconf("SC_CLK_TCK")
    total = 0.0
    for pid in process_tree(root_pid):
        try:
            fields = Path("/proc/%d/stat" % pid).read_text(encoding="ascii").rsplit(")", 1)[1].split()
            total += (int(fields[11]) + int(fields[12])) / float(ticks)
        except (OSError, IndexError, ValueError):
            continue
    return total


def tree_rss_bytes(root_pid: int) -> int:
    total = 0
    for pid in process_tree(root_pid):
        try:
            total += status_bytes(pid, "VmRSS")
        except OSError:
            continue
    return total


def total_memory_bytes() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    return 0


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

    def find(self, pattern: str) -> bool:
        compiled = re.compile(pattern)
        return any(compiled.search(strip_colors(line)) for line in self.text().splitlines())

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
        self.last_active_connections = 0
        self.traffic_source = "unknown"

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

    def paper_server_jar(self) -> Path:
        configured = os.environ.get("MZSTD_PAPER_SERVER_JAR")
        if configured:
            candidate = Path(configured).resolve()
            require(candidate.is_file(), "pre-fetched Paper server JAR is missing")
            require(sha256_file(candidate) == PAPER_SERVER_SHA256,
                    "pre-fetched Paper server JAR does not match the pinned sha256")
            return candidate
        cache = Path(os.environ.get("MZSTD_MC_CACHE", str(self.artifacts))).resolve()
        target = cache / ("paper-26.2-%d.jar" % PAPER_SERVER_BUILD)
        if not (target.is_file() and sha256_file(target) == PAPER_SERVER_SHA256):
            print("[paper] fetching server build %d" % PAPER_SERVER_BUILD, flush=True)
            mc_runtime.download(PAPER_SERVER_URL, target, None)
        digest = sha256_file(target)
        require(digest == PAPER_SERVER_SHA256,
                "Paper server JAR does not match the pinned sha256")
        return target

    STABILISING_COMMANDS = (
        ("gamerule doDaylightCycle false", r"Gamerule doDaylightCycle is now set to: false"),
        ("gamerule doWeatherCycle false", r"Gamerule doWeatherCycle is now set to: false"),
        ("gamerule randomTickSpeed 0", r"Gamerule randomTickSpeed is now set to: 0"),
        ("gamerule doMobSpawning false", r"Gamerule doMobSpawning is now set to: false"),
        ("gamerule doFireTick false", r"Gamerule doFireTick is now set to: false"),
        ("gamerule doEntityDrops false", r"Gamerule doEntityDrops is now set to: false"),
        ("difficulty peaceful", r"[Dd]ifficulty has been set to [Pp]eaceful"),
        ("time set 6000", r"Set the time to 6000"),
        ("weather clear", r"(Changing to clear weather|Set the weather to clear)"),
        ("forceload add -128 -128 128 128", r"[Aa]dded \d+ chunk"),
        ("kill @e[type=!player]", r"(Killed|No entity was found)"),
    )

    def stabilise_world(self) -> None:
        """Make the world deterministic before any client joins.

        Byte volume in the first empty control swung 36%, and the cause was the
        world itself: chunk generation, random ticks, mob spawning and the time of
        day all change how much traffic the server sends. Freezing them (and force
        loading the visible area) is what makes an A/B comparison meaningful.
        """
        for command, pattern in self.STABILISING_COMMANDS:
            line = self.console(command, pattern, timeout=30.0)
            require(line is not None,
                    "world stabilisation command had no effect: " + command)
        print("[lab] world stabilised for measurement", flush=True)

    def fix_player_orientation(self, client: dict) -> None:
        """Teleport the test client in place with a fixed yaw/pitch.

        Position comes from the deterministic world spawn; the rotation must be
        pinned because a different view angle changes how many chunks and entities
        end up being sent.
        """
        command = "execute at %s run tp %s ~ ~ ~ 90 0" % (client["username"], client["username"])
        self.console(command, r"Teleported", timeout=30.0)

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
                    "-jar", str(self.paper_server_jar()), "--nogui"],
                   cwd=server, log=self.paper_log.path, stdin=subprocess.PIPE)
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if self.paper_log.find(r"Done \([0-9.]+s\)!"):
                return
            if self.processes["paper"].poll() is not None:
                raise ScenarioError("Paper exited during startup:\n" + self.paper_log.tail())
            time.sleep(0.5)
        raise ScenarioError("Paper did not finish startup:\n" + self.paper_log.tail())

    def console(self, command: str, pattern: str, timeout: float = 30.0) -> str | None:
        """Send one console line and return the first matching output line."""
        stream = self.processes["paper"].stdin
        require(stream is not None, "Paper console is not writable")
        since = len(self.paper_log.text())
        stream.write((command + "\n").encode("utf-8"))
        stream.flush()
        return self.paper_log.wait_for(pattern, time.monotonic() + timeout, since=since)

    def latency_snapshot(self, reset: bool = False) -> dict:
        """Read the plugin's per-stage latency histograms (percentiles over the window).

        The histograms are cumulative, so the driver clears them once before the
        measured interval: otherwise the report would silently mix startup traffic
        with the steady-state window.
        """
        since = len(self.paper_log.text())
        stream = self.processes["paper"].stdin
        require(stream is not None, "Paper console is not writable")
        command = "mikuzstd latency reset" if reset else "mikuzstd latency"
        stream.write((command + "\n").encode("utf-8"))
        stream.flush()
        line = self.paper_log.wait_for(r"lat: queue_p50=", time.monotonic() + 30.0, since=since)
        require(line is not None, "plugin did not report latency histograms")
        fields = {}
        for key, value in re.findall(r"([a-z0-9_]+)=([0-9.]+)", line):
            fields[key] = float(value)
        return fields

    def traffic_snapshot(self) -> tuple[int, int]:
        """Read the production plugin's cumulative (raw, wire) totals.

        The exact integer line is preferred: the human-facing formatting has a
        100 KB step in the MB range, which is coarser than the deltas an A/B run
        is trying to resolve. Plugins without the exact line fall back to parsing
        the formatted summary, and the report records which source was used.
        """
        since = len(self.paper_log.text())
        stream = self.processes["paper"].stdin
        require(stream is not None, "Paper console is not writable")
        stream.write(b"mikuzstd traffic\n")
        stream.flush()
        exact = self.paper_log.wait_for(r"traffic: raw=\d+ wire=\d+ connections=\d+",
                                        time.monotonic() + 10.0, since=since)
        if exact is not None:
            match = re.search(r"traffic: raw=(\d+) wire=(\d+) connections=(\d+)", exact)
            self.last_active_connections = int(match.group(3))
            self.traffic_source = "exact"
            return int(match.group(1)), int(match.group(2))
        self.traffic_source = "formatted"
        since = len(self.paper_log.text())
        stream.write(b"mikuzstd top\n")
        stream.flush()
        deadline = time.monotonic() + 30.0
        summary = self.paper_log.wait_for(r"合计原始 \S+ → \S+ \(", deadline, since=since)
        require(summary is not None, "plugin did not report its traffic summary")
        match = re.search(r"活跃 (\d+) 条 \| 合计原始 (\S+) → (\S+) \(", summary)
        require(match is not None, "unexpected traffic summary: " + summary)
        self.last_active_connections = int(match.group(1))
        return parse_bytes(match.group(2)), parse_bytes(match.group(3))

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
            request = urllib.request.Request(url + ".sha1",
                                             headers={"User-Agent": mc_runtime.USER_AGENT})
            expected = urllib.request.urlopen(request, timeout=60).read().decode("ascii").split()[0]
            mc_runtime.download(url, loader, expected)
        runtime = mc_runtime.prepare(DEFAULT_VERSION, cache, loader_jar=loader,
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
        testmod = build_testmod.build(
            runtime["classpath"], self.artifacts / "testmod" / "Miku-ZSTD-CI-Observer.jar",
            extra_classpath=build_testmod.fabric_api_module_jars(fabric_api))
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
                    # 服务端的聊天/命令节流按"每条约 +20、每 tick −1"计，
                    # 持续速率超过约 1 条/秒就会在十几秒内被判 Kicked for spamming。
                    # 0.5 条/秒既留足余量，又能在 60 秒窗口里拿到约 30 个真实往返样本。
                    "-Dmiku.zstd.ci.rtt=true",
                    "-Dmiku.zstd.ci.rtt-interval-ms=2000",
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
                    continue
                if (client["username"] + " lost connection") in self.paper_log.text():
                    raise ScenarioError("client %d was disconnected by the server:\n%s"
                                        % (client["index"], client["log"].tail(30)))
                body = client["client_log"].text()
                for marker in ("Disconnected", "Connection refused", "Failed to connect",
                               "Internal Exception", "Failed to check session lock"):
                    if marker in body:
                        raise ScenarioError("client %d reported %r while joining:\n%s"
                                            % (client["index"], marker, client["log"].tail(30)))
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

    def fps_samples(self, client: dict, wall_from_ms: int, wall_to_ms: int) -> list[float]:
        """Client-reported frame rate inside the measured window."""
        samples = []
        evidence = client["evidence"]
        if not evidence.is_file():
            return samples
        for line in evidence.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("type") != "sample" or not record.get("playing"):
                continue
            wall = record.get("wall_ms")
            fps = record.get("fps")
            if not isinstance(wall, int) or not isinstance(fps, (int, float)):
                continue
            if wall_from_ms <= wall <= wall_to_ms and fps >= 0:
                samples.append(float(fps))
        return samples

    def rtt_samples(self, clients: list[dict], wall_from_ms: int, wall_to_ms: int) -> list[float]:
        """Client-measured round trips that completed inside the measured window.

        The client stamps each sample with wall-clock milliseconds as well as its
        own monotonic clock, so the driver can scope samples to the measurement
        window without relying on shared clock origins.
        """
        samples = []
        for client in clients:
            evidence = client["evidence"]
            if not evidence.is_file():
                continue
            for line in evidence.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("type") != "rtt":
                    continue
                wall = record.get("wall_ms")
                nanos = record.get("rtt_ns")
                if not isinstance(wall, int) or not isinstance(nanos, (int, float)):
                    continue
                if wall_from_ms <= wall <= wall_to_ms:
                    samples.append(float(nanos) / 1_000_000.0)
        return samples

    def ping_diagnostics(self, clients: list[dict]) -> dict:
        """How many pings the clients sent versus how many matched a pong."""
        sent = 0
        unmatched = 0
        for client in clients:
            evidence = client["evidence"]
            if not evidence.is_file():
                continue
            final = None
            for line in evidence.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("type") == "final":
                    final = record
            if final:
                sent += int(final.get("pings_sent", 0))
                unmatched += int(final.get("pings_unmatched", 0))
        return {"pings_sent": sent, "pings_unmatched": unmatched}

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
        # 预热期可能发生掉线/重连：进入测量前必须重新确认所有客户端仍在 PLAY，
        # 否则会拿"没有 zstd 连接"的空快照去算出一组零字节，掩盖真正的原因。
        for client in clients:
            state = self.play_state(client)
            require(state["playing"],
                    "client %s left PLAY before the measured interval%s"
                    % (client["index"], self.disconnect_reason(client)))
        # 先清零分段直方图，再开始计时：这样分位点覆盖的就是本次测量窗口。
        self.latency_snapshot(reset=True)
        raw_before, wire_before = self.traffic_snapshot()
        require(self.last_active_connections >= len(clients),
                "plugin reports %d zstd connection(s) but %d client(s) were requested%s"
                % (self.last_active_connections, len(clients),
                   self.disconnect_reason(clients[0])))
        rx_before, tx_before = read_netdev(interface)
        cpu_before = read_cpu_seconds(paper_pid)
        observer_before = sum(self.play_state(client)["inbound_bytes"] for client in clients)
        client_cpu_before = {client["index"]: tree_cpu_seconds(self.processes["client-%d" % client["index"]].pid)
                             for client in clients}
        client_rss_peak = {client["index"]: 0 for client in clients}
        lag_before = self.paper_log.text().count("Can't keep up")
        wall_started_ms = int(time.time() * 1000)
        started = time.monotonic()
        while time.monotonic() - started < self.duration:
            for client in clients:
                process = self.processes["client-%d" % client["index"]]
                require(process.poll() is None,
                        "client %d exited during the measured interval" % client["index"])
                lost = re.search(re.escape(client["username"]) + r" lost connection: (.+)",
                                 self.paper_log.text())
                if lost is not None:
                    raise ScenarioError("client %d was disconnected during the measured interval: %s"
                                        % (client["index"], lost.group(1).strip()))
                client_rss_peak[client["index"]] = max(
                    client_rss_peak[client["index"]],
                    tree_rss_bytes(process.pid))
            time.sleep(1.0)
        elapsed = time.monotonic() - started
        wall_finished_ms = int(time.time() * 1000)
        round_trips = self.rtt_samples(clients, wall_started_ms, wall_finished_ms)
        latency = self.latency_snapshot()
        raw_after, wire_after = self.traffic_snapshot()
        rx_after, tx_after = read_netdev(interface)
        cpu_after = read_cpu_seconds(paper_pid)
        states = [self.play_state(client) for client in clients]
        observer_after = sum(state["inbound_bytes"] for state in states)
        capacity = self.capacity_block(clients, client_cpu_before, client_rss_peak,
                                       elapsed, lag_before, wall_started_ms, wall_finished_ms)
        return {
            "duration_seconds": round(elapsed, 3),
            "raw_bytes": max(0, raw_after - raw_before),
            "compressed_wire_bytes": max(0, wire_after - wire_before),
            "nic_rx_bytes": max(0, rx_after - rx_before),
            "nic_tx_bytes": max(0, tx_after - tx_before),
            "server_cpu_seconds": round(max(0.0, cpu_after - cpu_before), 3),
            "server_rss_bytes": read_peak_rss(paper_pid),
            "pool_queue_wait_p99_ms": round(latency.get("queue_p99", 0.0), 3),
            "pool_service_p99_ms": round(latency.get("service_p99", 0.0), 3),
            "event_loop_wait_p99_ms": round(latency.get("eventloop_p99", 0.0), 3),
            "write_p99_ms": round(latency.get("write_p99", 0.0), 3),
            "latency_sample_count": int(latency.get("samples", 0.0)),
            "rtt_sample_count": len(round_trips),
            "rtt_pings_sent": self.ping_diagnostics(clients)["pings_sent"],
            "rtt_p50_ms": round(percentile(round_trips, 0.50), 3),
            "rtt_p95_ms": round(percentile(round_trips, 0.95), 3),
            "rtt_p99_ms": round(percentile(round_trips, 0.99), 3),
            "rtt_p999_ms": round(percentile(round_trips, 0.999), 3),
            "rtt_samples_ms": [round(value, 3) for value in round_trips[:2000]],
            "observed_client_inbound_bytes": max(0, observer_after - observer_before),
            "capacity": capacity,
            "states": states,
        }

    def disconnect_reason(self, client: dict) -> str:
        """Best-effort reason string from the server log, for actionable failures."""
        lost = re.search(re.escape(client["username"]) + r" lost connection: (.+)",
                         self.paper_log.text())
        return (" (server said: %s)" % lost.group(1).strip()) if lost else ""

    def capacity_block(self, clients: list[dict], cpu_before: dict, rss_peak: dict,
                       elapsed: float, lag_before: int,
                       wall_from_ms: int, wall_to_ms: int) -> dict:
        """Per-instance resource use on this runner, plus an explicit verdict.

        The point of T6 is to find the real capacity boundary instead of guessing:
        each client is a full game JVM (plus its Xvfb), so the numbers include the
        whole process tree. A run that cannot hold the requested instances must say
        so — silently passing a starved run would be worse than failing.
        """
        min_fps = float(os.environ.get("MZSTD_MIN_CLIENT_FPS", "20"))
        rows = []
        starved = []
        for client in clients:
            index = client["index"]
            after = tree_cpu_seconds(self.processes["client-%d" % index].pid)
            frames = self.fps_samples(client, wall_from_ms, wall_to_ms)
            fps_p50 = percentile(frames, 0.50)
            fps_p05 = percentile(frames, 0.05)
            rows.append({
                "id": "client-%d" % index,
                "cpu_seconds": round(max(0.0, after - cpu_before[index]), 3),
                "rss_peak_bytes": rss_peak[index],
                "fps_p50": round(fps_p50, 1),
                "fps_p05": round(fps_p05, 1),
                "fps_samples": len(frames),
            })
            if frames and fps_p50 < min_fps:
                starved.append("client-%d median %.0f fps below the %.0f fps floor"
                               % (index, fps_p50, min_fps))
        cores = os.cpu_count() or 1
        total_cpu = sum(row["cpu_seconds"] for row in rows)
        total_rss = sum(row["rss_peak_bytes"] for row in rows)
        utilisation = total_cpu / (elapsed * cores) if elapsed > 0 else 0.0
        lag_events = self.paper_log.text().count("Can't keep up") - lag_before
        reasons = []
        if lag_events > 0:
            reasons.append("server logged %d tick-lag warnings during the window" % lag_events)
        if utilisation > 0.9:
            reasons.append("client JVMs consumed %.0f%% of the %d available cores"
                           % (utilisation * 100, cores))
        reasons.extend(starved)
        return {
            "runner_cpu_count": cores,
            "runner_memory_bytes": total_memory_bytes(),
            "clients": rows,
            "total_client_cpu_seconds": round(total_cpu, 3),
            "total_client_rss_bytes": total_rss,
            "client_cpu_utilisation": round(utilisation, 3),
            "paper_rss_bytes": read_peak_rss(self.processes["paper"].pid),
            "server_tick_lag_warnings": lag_events,
            "min_client_fps": min_fps,
            "verdict": "insufficient" if reasons else "sufficient",
            "reasons": reasons,
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
                "disconnected": not state["playing"],
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
            "metrics": {key: value for key, value in measurement.items()
                        if key not in ("states", "rtt_samples_ms", "capacity")},
            "samples": {"rtt_ms": measurement.get("rtt_samples_ms", [])},
            "capacity": measurement.get("capacity", {}),
            "measurement": {
                "minecraft_version": DEFAULT_VERSION,
                "loader_version": DEFAULT_LOADER,
                "compression_threshold": self.threshold,
                "paper_server_build": PAPER_SERVER_BUILD,
                "paper_server_sha256": PAPER_SERVER_SHA256,
                "fabric_api_version": FABRIC_API_VERSION,
                "warmup_seconds": self.warmup,
                "sampling_interval_ms": 250,
                "interface": os.environ.get("MZSTD_NIC", "lo"),
                "traffic_source": ("production plugin counters read through /mikuzstd top before "
                                   "and after the window; delta only"),
                "traffic_granularity_bytes": 0 if self.traffic_source == "exact" else 50000,
                "traffic_counter_source": self.traffic_source,
                "rtt_source": ("game client sends /mikuzstd ping <token> on its own monotonic clock "
                               "and stops the timer when the pong system chat arrives; this includes "
                               "server command handling, not raw TCP RTT"),
                "rtt_floor": ("the server executes commands on a 20 tick/s loop and the client handles "
                              "the reply on its render thread, so the floor is one server tick plus one "
                              "client frame; compare variants under the same settings only"),
                "wire_vs_client_bytes": {
                    "plugin_wire_bytes": measurement.get("compressed_wire_bytes", 0),
                    "client_inbound_bytes": measurement.get("observed_client_inbound_bytes", 0),
                },
                "latency_scope": ("plugin histograms cleared once before the measured interval, "
                                  "so percentiles describe this window only"),
                "latency_stage_semantics": {
                    "queue": "batch submitted -> worker thread started",
                    "service": "worker compression itself",
                    "eventloop": "worker finished -> event-loop callback ran",
                    "write": "first frame write -> all real write promises resolved",
                },
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
            self.stabilise_world()
            self.start_frp()
            runtime = self.prepare_client_runtime()
            mods = self.prepare_mods(runtime)
            started = time.monotonic()
            clients = [self.launch_client(index, runtime, mods)
                       for index in range(self.clients_requested)]
            self.wait_ready(clients, started)
            for client in clients:
                self.fix_player_orientation(client)
            self.install_play_watch(clients)
            if self.warmup > 0:
                time.sleep(self.warmup)
            boots = [self.boot_record(client) for client in clients]
            measurement = self.measure(clients)
            report = self.build_report(clients, measurement, boots)
            capacity = report.get("capacity", {})
            if capacity.get("verdict") != "sufficient":
                # 容量不足时仍然落盘，这样数字（每实例 CPU/RSS）能被诊断步骤读到；
                # 但状态不是 passed，编排器与校验器都会拒收，不会冒充通过。
                report["status"] = "capacity_exceeded"
                self.write_report(report)
                raise ScenarioError(
                    "runner cannot hold %d real client(s): %s"
                    % (len(clients), "; ".join(capacity.get("reasons", [])) or "unknown"))
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
    except BaseException as failure:
        # 任何未预期异常都必须带上类型与信息: 编排器只会转述"命令失败",
        # 真正的定位线索只有这一行 (驱动日志)。
        print("scenario error: " + type(failure).__name__ + ": " + str(failure), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
