import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from validate_real_client_result import validate


class RealClientResultTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "summary.json"

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, result):
        self.path.write_text(json.dumps(result), encoding="utf-8")

    @staticmethod
    def client(count=1):
        return {
            "actual_minecraft_client": True,
            "fabric_loaded": True,
            "krypton_loaded": True,
            "miku_zstd_loaded": True,
            "login_success": True,
            "clients": [{"id": f"c{i}", "joined": True, "play_traffic_received": True} for i in range(count)],
        }

    @staticmethod
    def metrics(duration=10):
        return {
            "duration_seconds": duration,
            "raw_bytes": 1000,
            "compressed_wire_bytes": 400,
            "nic_rx_bytes": 500,
            "nic_tx_bytes": 600,
            "server_cpu_seconds": 1,
            "server_rss_bytes": 100000,
            "rtt_sample_count": 10,
            "rtt_p50_ms": 10,
            "rtt_p95_ms": 20,
            "rtt_p99_ms": 30,
            "rtt_p999_ms": 40,
            "pool_queue_wait_p99_ms": 2,
            "event_loop_wait_p99_ms": 1,
        }

    def smoke(self):
        return {
            "schema_version": 1,
            "test_type": "smoke",
            "status": "passed",
            "commit_sha": "candidate",
            "frp": {"use_compression": True},
            "client": self.client(),
            "metrics": self.metrics(),
        }

    @staticmethod
    def variant(sha):
        result = {
            "commit_sha": sha,
            "actual_clients": 2,
            "frp_use_compression": True,
            "duration_seconds": 5,
        }
        result.update({
            "rtt_p50_ms": 10,
            "rtt_p95_ms": 20,
            "rtt_p99_ms": 30,
            "rtt_p999_ms": 40,
            "raw_bytes": 1000,
            "compressed_wire_bytes": 400,
            "nic_rx_bytes": 500,
            "nic_tx_bytes": 600,
            "server_cpu_seconds": 1,
            "server_rss_bytes": 100000,
            "pool_queue_wait_p99_ms": 2,
            "event_loop_wait_p99_ms": 1,
        })
        return result

    def pair(self):
        return {
            "schema_version": 1,
            "test_type": "benchmark-pair",
            "status": "passed",
            "commit_sha": "candidate",
            "frp": {"use_compression": True},
            "client": self.client(2),
            "metrics": self.metrics(10),
            "paired_rounds": [
                {"round_id": 1, "order": ["baseline", "candidate"], "baseline": self.variant("baseline"), "candidate": self.variant("candidate")},
                {"round_id": 2, "order": ["candidate", "baseline"], "baseline": self.variant("baseline"), "candidate": self.variant("candidate")},
            ],
        }

    def call_validate(self, mode, rounds=0):
        with contextlib.redirect_stdout(io.StringIO()):
            validate(mode, self.path, 2 if mode == "benchmark-pair" else 1,
                     "candidate", "baseline" if mode == "benchmark-pair" else None,
                     10, rounds)

    def test_smoke_requires_real_client_and_frp_evidence(self):
        self.write(self.smoke())
        with contextlib.redirect_stdout(io.StringIO()):
            validate("smoke", self.path, 1, "candidate", None, 10, 0)

    def test_smoke_rejects_protocol_only_client(self):
        data = self.smoke()
        data["client"]["actual_minecraft_client"] = False
        self.write(data)
        with self.assertRaisesRegex(ValueError, "actual Minecraft client"):
            self.call_validate("smoke")

    def test_pair_requires_both_shas_and_reversed_round_order(self):
        self.write(self.pair())
        with contextlib.redirect_stdout(io.StringIO()):
            validate("benchmark-pair", self.path, 2, "candidate", "baseline", 10, 2)

    def test_pair_rejects_wrong_baseline_sha(self):
        data = self.pair()
        data["paired_rounds"][0]["baseline"]["commit_sha"] = "other"
        self.write(data)
        with self.assertRaisesRegex(ValueError, "baseline commit SHA"):
            self.call_validate("benchmark-pair", 2)

    def test_pair_rejects_unalternated_order(self):
        data = self.pair()
        data["paired_rounds"][1]["order"] = ["baseline", "candidate"]
        self.write(data)
        with self.assertRaisesRegex(ValueError, "variant order"):
            self.call_validate("benchmark-pair", 2)

    def test_pair_rejects_short_variant_duration(self):
        data = self.pair()
        data["paired_rounds"][1]["candidate"]["duration_seconds"] = 1
        self.write(data)
        with self.assertRaisesRegex(ValueError, "measured interval"):
            self.call_validate("benchmark-pair", 2)


if __name__ == "__main__":
    unittest.main()
