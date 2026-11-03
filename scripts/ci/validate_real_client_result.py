#!/usr/bin/env python3
"""Validate evidence emitted by the private real-Minecraft lab harness."""
import argparse
import json
import math
import sys
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(mapping, key, label):
    value = mapping.get(key)
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{label}.{key} must be numeric")
    require(math.isfinite(value) and value >= 0, f"{label}.{key} must be finite and non-negative")
    return value


def validate(mode, path, expected_clients, expected_sha, expected_baseline_sha, min_duration, required_rounds):
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    require(data.get("schema_version") == 1, "schema_version must be 1")
    require(data.get("test_type") == mode, f"test_type must be {mode}")
    require(data.get("status") == "passed", "harness did not report status=passed")
    require(data.get("commit_sha") == expected_sha, "result commit_sha differs from tested commit")
    require(data.get("frp", {}).get("use_compression") is True, "FRP transport.useCompression was not confirmed")
    client = data.get("client", {})
    require(client.get("actual_minecraft_client") is True, "result does not certify actual Minecraft client processes")
    require(client.get("fabric_loaded") is True, "Fabric did not load")
    require(client.get("krypton_loaded") is True, "Krypton did not load")
    require(client.get("miku_zstd_loaded") is True, "Miku-ZSTD did not load")
    require(client.get("login_success") is True, "client login was not confirmed")
    clients = client.get("clients")
    require(isinstance(clients, list) and len(clients) == expected_clients, "actual client list does not match requested count")
    for row in clients:
        require(row.get("joined") is True, f"client {row.get('id')} did not join")
        require(row.get("play_traffic_received") is True, f"client {row.get('id')} did not receive play-state traffic")
    metrics = data.get("metrics", {})
    require(number(metrics, "duration_seconds", "metrics") >= min_duration, "measured duration is below requested minimum")
    require(number(metrics, "raw_bytes", "metrics") > 0, "raw_bytes must be positive")
    require(number(metrics, "compressed_wire_bytes", "metrics") > 0, "compressed_wire_bytes must be positive")
    require(number(metrics, "nic_rx_bytes", "metrics") >= 0, "nic_rx_bytes missing")
    require(number(metrics, "nic_tx_bytes", "metrics") >= 0, "nic_tx_bytes missing")
    require(number(metrics, "server_cpu_seconds", "metrics") >= 0, "server_cpu_seconds missing")
    require(number(metrics, "server_rss_bytes", "metrics") >= 0, "server_rss_bytes missing")
    if mode in ("benchmark", "benchmark-pair"):
        require(number(metrics, "rtt_sample_count", "metrics") > 0, "benchmark must include client RTT samples")
        for percentile in ("rtt_p50_ms", "rtt_p95_ms", "rtt_p99_ms", "rtt_p999_ms"):
            number(metrics, percentile, "metrics")
        require(metrics["rtt_p50_ms"] <= metrics["rtt_p95_ms"] <= metrics["rtt_p99_ms"] <= metrics["rtt_p999_ms"], "RTT percentiles are not monotonic")
        require(number(metrics, "pool_queue_wait_p99_ms", "metrics") >= 0, "pool queue wait p99 missing")
        require(number(metrics, "event_loop_wait_p99_ms", "metrics") >= 0, "event-loop wait p99 missing")
    if mode == "benchmark-pair":
        rounds = data.get("paired_rounds")
        require(isinstance(rounds, list) and len(rounds) == required_rounds, "paired_rounds count differs from request")
        ids = []
        for row in rounds:
            ids.append(row.get("round_id"))
            require(row.get("order") in (["baseline", "candidate"], ["candidate", "baseline"]), "each round must alternate variant order")
            for variant in ("baseline", "candidate"):
                vm = row.get(variant, {})
                expected_variant_sha = expected_baseline_sha if variant == "baseline" else expected_sha
                require(expected_variant_sha and vm.get("commit_sha") == expected_variant_sha, f"{variant} commit SHA does not match the selected build")
                require(vm.get("actual_clients") == expected_clients, f"{variant} actual client count mismatch")
                require(vm.get("frp_use_compression") is True, f"{variant} FRP compression not confirmed")
                for key in ("duration_seconds", "rtt_p50_ms", "rtt_p95_ms", "rtt_p99_ms", "rtt_p999_ms", "raw_bytes", "compressed_wire_bytes", "nic_rx_bytes", "nic_tx_bytes", "server_cpu_seconds", "server_rss_bytes", "pool_queue_wait_p99_ms", "event_loop_wait_p99_ms"):
                    number(vm, key, variant)
                require(vm["duration_seconds"] >= min_duration / (2 * required_rounds), f"{variant} measured interval shorter than requested")
        require(len(set(ids)) == required_rounds, "paired round_id values must be unique")
        orders = [row["order"] for row in rounds]
        require(any(orders[i] != orders[i - 1] for i in range(1, len(orders))), "variant order must alternate at least once to limit drift")
    print(json.dumps({"status": "validated", "test_type": mode, "commit_sha": expected_sha, "actual_clients": expected_clients, "metrics": metrics}, ensure_ascii=False, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("smoke", "benchmark", "benchmark-pair"))
    parser.add_argument("result_json")
    parser.add_argument("--expected-clients", type=int, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-baseline-sha")
    parser.add_argument("--min-duration-seconds", type=float, default=1.0)
    parser.add_argument("--required-rounds", type=int, default=0)
    args = parser.parse_args()
    require(args.expected_clients > 0, "expected client count must be positive")
    if args.mode == "benchmark-pair":
        require(args.required_rounds >= 2, "paired benchmark requires at least two rounds")
        require(args.expected_baseline_sha is not None, "paired benchmark requires the baseline build SHA")
    validate(args.mode, args.result_json, args.expected_clients, args.expected_sha, args.expected_baseline_sha, args.min_duration_seconds, args.required_rounds)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"result validation failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
