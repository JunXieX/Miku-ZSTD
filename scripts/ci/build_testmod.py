#!/usr/bin/env python3
"""Compile the CI observation mod against the real Minecraft client classes.

The observation mod is a test component: it is built here, in the CI job, from
sources kept next to this script, and it is never part of a release artifact.
Building it needs nothing but the JDK and the client classpath that
:mod:`mc_runtime` already materialised, so there is no extra Gradle project and
no dependency on the production mod's build.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

MOD_ID = "miku_zstd_ci"
TESTMOD_ROOT = Path(__file__).resolve().parent / "testmod"
SOURCE_ROOT = TESTMOD_ROOT / "src"
DESCRIPTOR = TESTMOD_ROOT / "fabric.mod.json"


class TestModBuildError(Exception):
    """The observation mod could not be compiled or packaged."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fabric_api_module_jars(bundle: Path) -> list[Path]:
    """Unpack the fabric-api bundle's nested module jars for compilation.

    fabric-api ships as a shell jar whose real API lives in META-INF/jars/*.jar.
    At runtime the loader wires those nested modules; javac needs them unpacked.
    """
    bundle = Path(bundle)
    target_dir = bundle.parent / (bundle.stem + "-modules")
    target_dir.mkdir(parents=True, exist_ok=True)
    jars = []
    with zipfile.ZipFile(bundle) as archive:
        for name in archive.namelist():
            if not (name.startswith("META-INF/jars/") and name.endswith(".jar")):
                continue
            target = target_dir / Path(name).name
            if not target.is_file():
                target.write_bytes(archive.read(name))
            jars.append(target)
    return jars


def build(classpath: list[str], output: Path, javac: str = "javac",
          extra_classpath: list[Path] | None = None) -> Path:
    sources = sorted(SOURCE_ROOT.rglob("*.java"))
    if not sources:
        raise TestModBuildError("no observation mod sources under " + str(SOURCE_ROOT))
    if not DESCRIPTOR.is_file():
        raise TestModBuildError("missing fabric.mod.json for the observation mod")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()
    with tempfile.TemporaryDirectory(prefix="mikuzstdci-") as temporary:
        classes = Path(temporary) / "classes"
        classes.mkdir()
        full_classpath = list(classpath) + [str(entry) for entry in (extra_classpath or [])]
        command = [javac, "--release", "25", "-encoding", "UTF-8", "-Xlint:all,-classfile",
                   "-classpath", os.pathsep.join(full_classpath), "-d", str(classes)]
        command.extend(str(source) for source in sources)
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            raise TestModBuildError("javac failed:\n" + (completed.stderr or completed.stdout))
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(DESCRIPTOR, "fabric.mod.json")
            for compiled in sorted(classes.rglob("*")):
                if compiled.is_file():
                    archive.write(compiled, compiled.relative_to(classes).as_posix())
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-json", required=True,
                        help="JSON produced by mc_runtime.py (contains the launch classpath)")
    parser.add_argument("--loader-jar", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fabric-api-jar")
    parser.add_argument("--javac", default="javac")
    args = parser.parse_args(argv)
    runtime = json.loads(Path(args.runtime_json).read_text(encoding="utf-8"))
    classpath = [str(Path(entry)) for entry in runtime["classpath"]]
    classpath.append(str(Path(args.loader_jar).resolve()))
    missing = [entry for entry in classpath if not Path(entry).is_file()]
    if missing:
        raise TestModBuildError("classpath entries missing: " + str(missing[:3]))
    extra: list[Path] = []
    if args.fabric_api_jar:
        extra = fabric_api_module_jars(Path(args.fabric_api_jar))
    jar = build(classpath, Path(args.output), args.javac, extra)
    summary = {"testmod": str(jar), "sha256": sha256_file(jar),
               "size": jar.stat().st_size, "mod_id": MOD_ID}
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TestModBuildError as failure:
        print("observation mod build failed: " + str(failure), file=sys.stderr)
        raise SystemExit(1)
