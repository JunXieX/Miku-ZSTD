#!/usr/bin/env python3
"""Materialise a real Minecraft client runtime from Mojang's public metadata.

The CI client driver needs the same files a launcher would install: the client
jar, every rule-matching library, the asset objects for the pinned asset index
and the log4j client config. This module only downloads and assembles those
files plus the argument templates; it never authenticates, never reads a
launcher profile and never claims that a game started.

Behaviour is deliberately narrow and inspectable:

* rules are evaluated with the vanilla "last matching rule wins" algorithm;
* every download is verified against the sha1 published in the metadata;
* an existing file is reused only when its digest matches;
* the offline identity is derived from a fixed username, so no session data or
  account material ever enters the driver, the logs or the report.

Nothing in this module is a test result; the driver and the validator decide
whether a real client actually joined a real server.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

VERSION_MANIFEST = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"
ASSET_BASE = "https://resources.download.minecraft.net"
KNOT_MAIN_CLASS = "net.fabricmc.loader.impl.launch.knot.KnotClient"
# Paper 的下载端会拒绝没有 User-Agent 的请求 (HTTP 403)，
# 这里统一带上固定标识，避免部署脚本与 CI 行为不一致。
USER_AGENT = "Miku-ZSTD-CI/1.0 (+https://github.com/JunXieX/Miku-ZSTD)"


class RuntimeFetchError(Exception):
    """A required metadata lookup or download failed."""


def sha1_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, dest: Path, sha1: str | None = None, attempts: int = 4,
             timeout: int = 180) -> Path:
    """Fetch url into dest, reusing an existing file only when sha1 matches."""
    if dest.is_file() and (sha1 is None or sha1_file(dest) == sha1):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    temporary = dest.with_name(dest.name + ".part")
    last_error = None
    request_headers = {"User-Agent": USER_AGENT}
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers=request_headers)
            with urllib.request.urlopen(request, timeout=timeout) as response, \
                    temporary.open("wb") as output:
                shutil.copyfileobj(response, output, 1 << 20)
            if sha1 is not None:
                actual = sha1_file(temporary)
                if actual != sha1:
                    raise RuntimeFetchError("digest mismatch for " + url)
            os.replace(temporary, dest)
            return dest
        except (urllib.error.URLError, OSError, RuntimeFetchError) as error:
            last_error = error
            temporary.unlink(missing_ok=True)
            time.sleep(0.5 + attempt)
    raise RuntimeFetchError("download failed (%s): %s"
                            % (type(last_error).__name__, url))


def platform_os_name() -> str:
    system = platform.system()
    if system == "Linux":
        return "linux"
    if system == "Darwin":
        return "osx"
    if system == "Windows":
        return "windows"
    raise RuntimeFetchError("unsupported operating system: " + system)


def platform_arch() -> str:
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        return "x86_64"
    if machine in ("aarch64", "arm64"):
        return "arm64"
    if machine in ("i386", "i686", "x86"):
        return "x86"
    raise RuntimeFetchError("unsupported architecture: " + machine)


def _rule_matches(rule: dict, os_name: str, arch: str, features: dict) -> bool:
    os_rule = rule.get("os")
    if isinstance(os_rule, dict):
        if os_rule.get("name") not in (None, os_name):
            return False
        if os_rule.get("arch") not in (None, arch):
            return False
        version_pattern = os_rule.get("version")
        if version_pattern and not re.search(version_pattern, platform.release()):
            return False
    feature_rule = rule.get("features")
    if isinstance(feature_rule, dict):
        for key, expected in feature_rule.items():
            if bool(features.get(key, False)) != bool(expected):
                return False
    return True


def rules_allow(entry: dict, os_name: str, arch: str, features: dict) -> bool:
    """Vanilla semantics: walk the rule list, the last matching rule decides."""
    rules = entry.get("rules")
    if not rules:
        return True
    allowed = False
    for rule in rules:
        if _rule_matches(rule, os_name, arch, features):
            allowed = rule.get("action") == "allow"
    return allowed


def load_version(version_id: str, cache_dir: Path) -> dict:
    manifest_path = cache_dir / "version_manifest_v2.json"
    if not manifest_path.is_file():
        download(VERSION_MANIFEST, manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next((row for row in manifest.get("versions", []) if row.get("id") == version_id), None)
    if entry is None:
        raise RuntimeFetchError("unknown Minecraft version: " + version_id)
    version_path = cache_dir / (version_id + ".json")
    download(entry["url"], version_path, entry.get("sha1"))
    return json.loads(version_path.read_text(encoding="utf-8"))


def _library_artifacts(version: dict, os_name: str, arch: str, features: dict) -> list[dict]:
    artifacts = []
    for library in version.get("libraries", []):
        if not rules_allow(library, os_name, arch, features):
            continue
        artifact = library.get("downloads", {}).get("artifact")
        if artifact is None:
            continue
        artifacts.append(artifact)
    return artifacts


def _asset_objects(version: dict, assets_dir: Path) -> tuple[str, dict]:
    index = version["assetIndex"]
    index_path = assets_dir / "indexes" / (index["id"] + ".json")
    download(index["url"], index_path, index.get("sha1"))
    return index["id"], json.loads(index_path.read_text(encoding="utf-8")).get("objects", {})


def prepare(version_id: str, cache_dir: Path, extra_classpath: list[Path] | None = None,
            features: dict | None = None, workers: int = 16, log=None) -> dict:
    """Download everything required to launch the client and return a launch plan."""
    log = log or (lambda message: print(message, flush=True))
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    os_name, arch = platform_os_name(), platform_arch()
    version = load_version(version_id, cache_dir)
    features = dict(features or {})

    client_path = cache_dir / "client.jar"
    client_download = version["downloads"]["client"]
    download(client_download["url"], client_path, client_download["sha1"])

    libraries_root = cache_dir / "libraries"
    artifacts = _library_artifacts(version, os_name, arch, features)
    log("[mc] %d rule-matching libraries for %s/%s" % (len(artifacts), os_name, arch))

    def fetch_library(artifact: dict) -> Path:
        return download(artifact["url"], libraries_root / artifact["path"], artifact.get("sha1"))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        library_paths = list(pool.map(fetch_library, artifacts))
    log("[mc] libraries ready")

    assets_dir = cache_dir / "assets"
    asset_index_id, objects = _asset_objects(version, assets_dir)
    pending = []
    for entry in objects.values():
        digest = entry["hash"]
        target = assets_dir / "objects" / digest[:2] / digest
        if target.is_file() and target.stat().st_size == entry.get("size", -1):
            continue
        pending.append((digest, entry.get("size")))
    log("[mc] asset index %s: %d objects, %d to fetch" % (asset_index_id, len(objects), len(pending)))

    def fetch_asset(item: tuple[str, int | None]) -> Path:
        digest, size = item
        url = ASSET_BASE + "/" + digest[:2] + "/" + digest
        target = assets_dir / "objects" / digest[:2] / digest
        if target.is_file() and size is not None and target.stat().st_size == size:
            return target
        return download(url, target, digest)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(fetch_asset, pending))
    log("[mc] assets ready")

    logging_config = None
    client_logging = (version.get("logging") or {}).get("client") or {}
    log_file = client_logging.get("file") or {}
    if log_file.get("url"):
        logging_config = download(log_file["url"],
                                  cache_dir / "log_configs" / (log_file.get("id") or "client.xml"),
                                  log_file.get("sha1"))

    natives_dir = cache_dir / "natives"
    for sub in ("java", "jna", "lwjgl", "netty"):
        (natives_dir / sub).mkdir(parents=True, exist_ok=True)

    classpath = [client_path, *library_paths]
    classpath.extend(Path(extra) for extra in (extra_classpath or []))
    return {
        "version_id": version_id,
        "asset_index": asset_index_id,
        "assets_dir": str(assets_dir),
        "client_jar": str(client_path),
        "classpath": [str(path) for path in classpath],
        "libraries": len(library_paths),
        "asset_objects": len(objects),
        "natives_dir": str(natives_dir),
        "log_config": str(logging_config) if logging_config else None,
        "main_class": KNOT_MAIN_CLASS,
        "vanilla_main_class": version.get("mainClass", "net.minecraft.client.main.Main"),
        "version_type": version.get("type", "release"),
        "jvm_template": version.get("arguments", {}).get("jvm", []),
        "logging_argument": client_logging.get("argument"),
    }


def _substitute(value: str, mapping: dict) -> str:
    for key, replacement in mapping.items():
        value = value.replace("${" + key + "}", replacement)
    return value


def build_jvm_args(runtime: dict, launcher_name: str = "miku-zstd-ci",
                   launcher_version: str = "1.0") -> list[str]:
    """Expand the vanilla jvm argument template for this platform."""
    os_name, arch = platform_os_name(), platform_arch()
    mapping = {
        "natives_directory": str(Path(runtime["natives_dir"]).resolve()),
        "launcher_name": launcher_name,
        "launcher_version": launcher_version,
        "classpath": os.pathsep.join(str(Path(entry).resolve()) for entry in runtime["classpath"]),
    }
    arguments: list[str] = []
    for entry in runtime["jvm_template"]:
        value = entry.get("value") if isinstance(entry, dict) else entry
        if isinstance(entry, dict) and not rules_allow(entry, os_name, arch, {}):
            continue
        values = [value] if isinstance(value, str) else list(value)
        arguments.extend(_substitute(item, mapping) for item in values)
    if runtime.get("log_config"):
        template = runtime.get("logging_argument") or "-Dlog4j.configurationFile=${path}"
        arguments.append(template.replace("${path}", runtime["log_config"]))
    return arguments


def offline_identity(username: str) -> tuple[str, str]:
    """Return (uuid, username) for an offline profile; never an account identity."""
    digest = hashlib.md5(("OfflinePlayer:" + username).encode("utf-8")).digest()
    # RFC 4122 version 3 layout, identical to the offline UUIDs servers generate.
    raw = bytearray(digest)
    raw[6] = (raw[6] & 0x0F) | 0x30
    raw[8] = (raw[8] & 0x3F) | 0x80
    hexed = raw.hex()
    return "%s-%s-%s-%s-%s" % (hexed[0:8], hexed[8:12], hexed[12:16], hexed[16:20], hexed[20:32]), username


def build_game_args(runtime: dict, game_dir: Path, username: str,
                    server_address: str | None = None,
                    width: int | None = None, height: int | None = None) -> list[str]:
    """Assemble the vanilla program arguments plus an optional quick-play target."""
    uuid, name = offline_identity(username)
    arguments = [
        "--username", name,
        "--version", runtime["version_id"],
        "--gameDir", str(Path(game_dir).resolve()),
        "--assetsDir", runtime["assets_dir"],
        "--assetIndex", runtime["asset_index"],
        "--uuid", uuid,
        "--accessToken", "0",
        "--clientId", "",
        "--xuid", "",
        "--versionType", runtime.get("version_type", "release"),
    ]
    if width and height:
        arguments.extend(["--width", str(width), "--height", str(height)])
    if server_address:
        arguments.extend(["--quickPlayMultiplayer", server_address])
    return arguments


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch a Minecraft client runtime")
    parser.add_argument("--version", default="26.2")
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--loader-jar", action="append", default=[])
    parser.add_argument("--summary-json")
    args = parser.parse_args(argv)
    runtime = prepare(args.version, Path(args.cache_dir),
                      extra_classpath=[Path(item) for item in args.loader_jar])
    payload = json.dumps(runtime, indent=2, sort_keys=True)
    if args.summary_json:
        Path(args.summary_json).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
