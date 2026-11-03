#!/usr/bin/env python3
"""Validate lab reports and export only a numeric/identity evidence allow-list.

This verifies a trusted scenario driver's report contract, not independent proof
of a game process. Keep runner opt-in disabled until that driver is accepted.
"""
import argparse
import hashlib
import json
import math
import re
import sys
from pathlib import Path

COMMON = ("duration_seconds", "raw_bytes", "compressed_wire_bytes", "nic_rx_bytes",
          "nic_tx_bytes", "server_cpu_seconds", "server_rss_bytes")
RTT = ("rtt_p50_ms", "rtt_p95_ms", "rtt_p99_ms", "rtt_p999_ms")
BENCH = ("rtt_sample_count", *RTT, "pool_queue_wait_p99_ms", "event_loop_wait_p99_ms")
FLAGS = ("actual_minecraft_client", "fabric_loaded", "krypton_loaded", "miku_zstd_loaded", "login_success")
SENSITIVE = re.compile(r"access.?token|refresh.?token|password|secret|authorization|cookie|session.?token", re.I)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(mapping, key, label):
    value = mapping.get(key)
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{label}.{key} must be numeric")
    require(math.isfinite(value) and value >= 0, f"{label}.{key} must be finite and non-negative")
    return value


def reject_credentials(value):
    if isinstance(value, dict):
        require(not any(SENSITIVE.search(str(key)) for key in value), "credential field found in result; no evidence exported")
        for row in value.values():
            reject_credentials(row)
    elif isinstance(value, list):
        for row in value:
            reject_credentials(row)


def check_clients(rows, count, duration):
    require(isinstance(rows, list) and len(rows) == count, "actual client list does not match requested count")
    ids = set()
    clean = []
    for row in rows:
        require(isinstance(row, dict), "client row must be an object")
        ident = row.get("id")
        require(isinstance(ident, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", ident), "client id must be a nonempty opaque label")
        require(ident not in ids, "client id values must be unique")
        ids.add(ident)
        require(row.get("joined") is True and row.get("play_traffic_received") is True, "client did not join or receive PLAY traffic")
        require(row.get("disconnected") is False, "client disconnect status must explicitly be false")
        active = number(row, "active_seconds", "client")
        require(active >= duration, "client did not remain active for the measured interval")
        clean.append({"id": ident, "joined": True, "play_traffic_received": True,
                      "disconnected": False, "active_seconds": active})
    return clean


def check_runtime(data, count, duration):
    require(data.get("client_runtime") == "minecraft_client", "client_runtime must identify the real Minecraft client")
    client = data.get("client", {})
    require(isinstance(client, dict), "client evidence must be an object")
    require(client.get("actual_minecraft_client") is True, "result does not certify actual Minecraft client processes")
    require(all(client.get(flag) is True for flag in FLAGS), "client Fabric/Krypton/Miku/login evidence missing")
    rows = data.get("clients", client.get("clients"))
    clean = check_clients(rows, count, duration)
    if "clients" in data and "clients" in client:
        require(data["clients"] == client["clients"], "root and nested client evidence disagree")
    return clean


SAMPLE_LIMIT = 5000


def check_samples(data, metrics):
    """Validate the optional bounded raw sample block and return it for export.

    Raw numbers are the only reason percentiles can be recomputed later, so they
    are exported as-is — but only as a flat list of finite non-negative numbers,
    with a hard length limit, and only when the count agrees with the metrics.
    """
    samples = data.get("samples")
    if samples is None:
        return None
    require(isinstance(samples, dict), "samples must be an object")
    values = samples.get("rtt_ms")
    require(isinstance(values, list), "samples.rtt_ms must be a list")
    require(len(values) <= SAMPLE_LIMIT, "sample list exceeds the exported limit")
    count = metrics.get("rtt_sample_count") if isinstance(metrics, dict) else None
    require(isinstance(count, int) and count == len(values),
            "rtt sample count disagrees with metrics")
    for value in values:
        require(isinstance(value, (int, float)) and not isinstance(value, bool),
                "rtt sample must be numeric")
        require(math.isfinite(value) and value >= 0, "rtt sample must be finite and non-negative")
    return {"rtt_ms": values}


def check_metrics(metrics, minimum, benchmark, label):
    require(isinstance(metrics, dict), f"{label} must be an object")
    for key in COMMON + (BENCH if benchmark else ()):
        number(metrics, key, label)
    require(metrics["duration_seconds"] >= minimum, f"{label} measured interval shorter than requested")
    for key in ("raw_bytes", "compressed_wire_bytes", "server_rss_bytes"):
        require(metrics[key] > 0, f"{label}.{key} must be positive")
    require(metrics["nic_rx_bytes"] + metrics["nic_tx_bytes"] > 0, f"{label} NIC traffic must be positive")
    if benchmark:
        samples = metrics["rtt_sample_count"]
        require(isinstance(samples, int) and samples > 0, f"{label} RTT sample count must be a positive integer")
        require(all(metrics[a] <= metrics[b] for a, b in zip(RTT, RTT[1:])), f"{label} RTT percentiles are not monotonic")
    return {key: metrics[key] for key in COMMON + (BENCH if benchmark else ())}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate(mode, path, expected_clients, expected_sha, expected_baseline_sha, min_duration,
             required_rounds, expected_nonce=None, expected_hashes=None, sanitized_output=None):
    require(expected_clients > 0 and math.isfinite(min_duration) and min_duration > 0, "count and duration must be positive")
    pair = mode == "benchmark-pair"
    benchmark = mode in ("benchmark", "benchmark-pair")
    require(not pair or (required_rounds >= 2 and expected_baseline_sha and expected_baseline_sha != expected_sha), "paired benchmark requires different SHAs and at least two rounds")
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    require(isinstance(data, dict), "result must be an object")
    reject_credentials(data)
    require(data.get("fixture_only") is not True, "synthetic fixture is not real-client evidence")
    require(type(data.get("schema_version")) is int and data["schema_version"] == 1, "schema_version must be 1")
    require(data.get("test_type") == mode and data.get("status") == "passed", "test_type or status does not match successful requested test")
    require(data.get("commit_sha") == expected_sha, "result commit_sha differs from tested commit")
    if expected_nonce is not None:
        require(data.get("run_nonce") == expected_nonce, "result run_nonce differs from this invocation")
    require(data.get("frp", {}).get("use_compression") is True, "FRP transport.useCompression was not confirmed")
    interval = min_duration / (2 * required_rounds) if pair else min_duration
    metrics = check_metrics(data.get("metrics", {}), min_duration, benchmark, "metrics")
    clean_clients = check_runtime(data, expected_clients, interval if pair else metrics["duration_seconds"])
    clean = {"schema_version": 1, "test_type": mode, "status": "passed", "commit_sha": expected_sha,
             "client_runtime": "minecraft_client", "frp": {"use_compression": True}, "client": {**{flag: True for flag in FLAGS}, "clients": clean_clients},
             "metrics": metrics}
    if expected_nonce is not None:
        clean["run_nonce"] = expected_nonce
    if expected_hashes:
        require(data.get("artifact_sha256") == expected_hashes["candidate"], "candidate artifact SHA256 mismatch")
        clean["artifact_sha256"] = expected_hashes["candidate"]
    samples = check_samples(data, data.get("metrics", {}))
    if samples is not None:
        clean["samples"] = samples
    if pair:
        rounds = data.get("paired_rounds")
        require(isinstance(rounds, list) and len(rounds) == required_rounds, "paired_rounds count differs from request")
        ids, orders, clean_rounds = set(), [], []
        total_duration = 0
        for row in rounds:
            rid = row.get("round_id")
            require(type(rid) is int and rid > 0 and rid not in ids, "paired round_id values must be positive and unique")
            ids.add(rid)
            order = row.get("order")
            require(order in (["baseline", "candidate"], ["candidate", "baseline"]), "invalid variant order")
            require(not orders or order != orders[-1], "variant order must alternate every round to limit drift")
            orders.append(order)
            out = {"round_id": rid, "order": order}
            for variant in ("baseline", "candidate"):
                vm = row.get(variant, {})
                require(vm.get("fixture_only") is not True, "synthetic variant is not real-client evidence")
                sha = expected_baseline_sha if variant == "baseline" else expected_sha
                require(vm.get("commit_sha") == sha, f"{variant} commit SHA does not match the selected build")
                require(type(vm.get("actual_clients")) is int and vm["actual_clients"] == expected_clients, f"{variant} actual client count mismatch")
                require(vm.get("frp_use_compression") is True, f"{variant} FRP compression not confirmed")
                if expected_nonce is not None:
                    require(vm.get("run_nonce") == expected_nonce, f"{variant} run_nonce mismatch")
                cm = check_metrics(vm, interval, True, variant)
                cc = check_runtime(vm, expected_clients, cm["duration_seconds"])
                total_duration += cm["duration_seconds"]
                out[variant] = {**cm, "commit_sha": sha, "actual_clients": expected_clients,
                                "frp_use_compression": True, "clients": cc, "client_runtime": "minecraft_client",
                                "client": {**{flag: True for flag in FLAGS}, "clients": cc}}
                if expected_nonce is not None:
                    out[variant]["run_nonce"] = expected_nonce
                if expected_hashes:
                    require(vm.get("artifact_sha256") == expected_hashes[variant], f"{variant} artifact SHA256 mismatch")
                    out[variant]["artifact_sha256"] = expected_hashes[variant]
                vm_samples = check_samples(vm, vm)
                if vm_samples is not None:
                    out[variant]["samples"] = vm_samples
            clean_rounds.append(out)
        require(total_duration >= min_duration, "paired durations below requested total")
        require(math.isclose(metrics["duration_seconds"], total_duration, rel_tol=1e-6, abs_tol=0.001), "aggregate duration differs from paired intervals")
        clean["paired_rounds"] = clean_rounds
        clean["metric_scope"] = "last_candidate_with_total_measured_duration"
    if sanitized_output:
        output = Path(sanitized_output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(clean, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "validated", "test_type": mode, "commit_sha": expected_sha,
                      "actual_clients": expected_clients, "duration_seconds": metrics["duration_seconds"]}, sort_keys=True))
    return clean


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("smoke", "benchmark", "benchmark-pair"))
    parser.add_argument("result_json")
    parser.add_argument("--expected-clients", type=int, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-baseline-sha")
    parser.add_argument("--min-duration-seconds", type=float, default=1.0)
    parser.add_argument("--required-rounds", type=int, default=0)
    parser.add_argument("--expected-nonce", required=True)
    parser.add_argument("--expected-fabric-jar", required=True)
    parser.add_argument("--expected-paper-jar", required=True)
    parser.add_argument("--expected-baseline-fabric-jar")
    parser.add_argument("--expected-baseline-paper-jar")
    parser.add_argument("--sanitized-output", required=True)
    args = parser.parse_args()
    hashes = {"candidate": {"fabric": digest(args.expected_fabric_jar), "paper": digest(args.expected_paper_jar)}}
    if args.mode == "benchmark-pair":
        require(args.expected_baseline_fabric_jar and args.expected_baseline_paper_jar, "baseline jars required")
        hashes["baseline"] = {"fabric": digest(args.expected_baseline_fabric_jar), "paper": digest(args.expected_baseline_paper_jar)}
    validate(args.mode, args.result_json, args.expected_clients, args.expected_sha, args.expected_baseline_sha,
             args.min_duration_seconds, args.required_rounds, args.expected_nonce, hashes, args.sanitized_output)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("result validation failed; inspect the private runner report (not uploaded)", file=sys.stderr)
        raise SystemExit(1)
