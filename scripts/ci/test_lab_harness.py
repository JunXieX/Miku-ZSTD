"""Mock-only orchestration tests: no game, runner command or real measurement.

All synthetic reports stay in TemporaryDirectory and are explicitly marked as
fixtures. These tests establish scheduling behavior, not Minecraft evidence.
"""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import lab_harness as lab


class LabHarnessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get("TEMP"))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.work = self.root / "private-work"
        self.work.mkdir()
        self.output = self.root / "upload"
        self.config = self.root / "runner-local.json"
        self.configuration = {"schema_version": 1,
                              "scenario_command": [sys.executable, "fixture-command-never-run"],
                              "cleanup_command": [sys.executable, "fixture-cleanup-never-run"],
                              "work_dir": str(self.work), "startup_timeout_seconds": 3,
                              "cleanup_timeout_seconds": 2, "terminate_grace_seconds": 1}
        self.save_config()
        self.fabric = self.root / "fixture-fabric.jar"
        self.paper = self.root / "fixture-paper.jar"
        self.fabric.write_bytes(b"mock artifact, not a Minecraft JAR")
        self.paper.write_bytes(b"another mock artifact")
        self.calls = []
        self.addCleanup(mock.patch.stopall)

    def save_config(self):
        self.config.write_text(json.dumps(self.configuration), encoding="utf-8")

    def args(self, mode="smoke", rounds=3):
        common = [mode, "--config", str(self.config), "--result-dir", str(self.output),
                  "--frp-compression", "true"]
        if mode == "smoke":
            common += ["--fabric-jar", str(self.fabric), "--paper-jar", str(self.paper),
                       "--candidate-sha", "fixture-candidate", "--duration-seconds", "2"]
        else:
            for variant in ("baseline", "candidate"):
                common += ["--" + variant + "-fabric-jar", str(self.fabric),
                           "--" + variant + "-paper-jar", str(self.paper),
                           "--" + variant + "-sha", "fixture-" + variant]
            common += ["--clients", "2", "--duration-minutes", "0.05",
                       "--rounds", str(rounds), "--warmup-minutes", "0.1", "--randomize-order"]
        return lab.parse_args(common)

    @staticmethod
    def fixture(env):
        # Intentionally synthetic contract data, never an execution result.
        return {"fixture_only": True, "schema_version": 1, "status": "passed",
                "client_runtime": "minecraft_client",
                "run_nonce": env["MZSTD_RUN_NONCE"], "commit_sha": env["MZSTD_COMMIT_SHA"],
                "session_id": env["MZSTD_SESSION_ID"],
                "artifact_sha256": {"fabric": env["MZSTD_FABRIC_SHA256"],
                                    "paper": env["MZSTD_PAPER_SHA256"]},
                "clients": [{"id": "mock-only-" + str(i)}
                            for i in range(int(env["MZSTD_CLIENTS"]))],
                "frp": {"use_compression": True},
                "metrics": {"duration_seconds": float(env["MZSTD_DURATION_SECONDS"]),
                            "raw_bytes": 10, "compressed_wire_bytes": 5,
                            "nic_rx_bytes": 3, "nic_tx_bytes": 4,
                            "server_cpu_seconds": 1, "server_rss_bytes": 20,
                            "rtt_sample_count": 2, "rtt_p50_ms": 1, "rtt_p95_ms": 2,
                            "rtt_p99_ms": 3, "rtt_p999_ms": 4,
                            "pool_queue_wait_p99_ms": 1, "event_loop_wait_p99_ms": 1,
                            "unknown_native_metric": {"must_survive": 123}}}

    def install_runner(self, mutate=None, failure=None, cleanup_failure=False):
        mock.patch.object(lab, "work_lock", side_effect=lambda _: contextlib.nullcontext()).start()
        mock.patch.object(lab, "cancellation_signals", side_effect=contextlib.nullcontext).start()
        # Advance a mock-only clock, never sleep or claim a real observation.
        clock = iter(range(0, 1000000, 10000))
        mock.patch.object(lab.time, "monotonic", side_effect=lambda: next(clock)).start()

        def runner(command, env, cwd, log, timeout, grace):
            self.calls.append({"cleanup": command == self.configuration["cleanup_command"],
                               "env": env.copy(), "timeout": timeout, "log": log})
            log.write_text("mock-only private output", encoding="utf-8")
            if command == self.configuration["cleanup_command"]:
                if cleanup_failure:
                    raise lab.HarnessError("mock cleanup failed")
                return
            if failure:
                raise failure
            data = self.fixture(env)
            if mutate:
                mutate(data)
            Path(env["MZSTD_REPORT_PATH"]).write_text(json.dumps(data), encoding="utf-8")
        return mock.patch.object(lab, "run_process", side_effect=runner).start()

    def test_workflow_smoke_arguments_and_environment(self):
        self.install_runner()
        with mock.patch.dict(os.environ, {"GITHUB_SHA": "fixture-workflow",
                                          "MZSTD_RUN_NONCE": "fixture-nonce",
                                          "INHERITED_FIXTURE": "kept"}):
            args = self.args()
            args.candidate_sha = None
            result = lab.execute(args)
        env = self.calls[0]["env"]
        self.assertEqual(env["MZSTD_COMMIT_SHA"], "fixture-workflow")
        self.assertEqual(env["INHERITED_FIXTURE"], "kept")
        self.assertEqual(env["MZSTD_RUN_NONCE"], "fixture-nonce")
        self.assertEqual(env["MZSTD_FRP_COMPRESSION"], "true")
        self.assertEqual(env["MZSTD_WARMUP_SECONDS"], "0")
        self.assertEqual(self.calls[0]["timeout"], 5)
        self.assertTrue(result["fixture_only"])
        self.assertEqual([p.name for p in self.output.iterdir()], ["summary.json"])
        self.assertTrue(self.calls[0]["log"].is_relative_to(self.work))
        self.assertFalse(self.calls[0]["log"].is_relative_to(self.output))
        self.assertEqual(result["metrics"]["unknown_native_metric"], {"must_survive": 123})

    def test_pair_randomizes_once_then_strictly_alternates(self):
        self.install_runner()
        with mock.patch.object(lab.secrets, "randbelow", return_value=1) as randomize:
            result = lab.execute(self.args("benchmark-pair"))
        randomize.assert_called_once_with(2)
        self.assertEqual([row["order"] for row in result["paired_rounds"]],
                         [["candidate", "baseline"], ["baseline", "candidate"],
                          ["candidate", "baseline"]])
        self.assertEqual(len(self.calls), 12)
        self.assertTrue(all(self.calls[i]["cleanup"] for i in range(1, 12, 2)))
        self.assertEqual(result["metrics"]["duration_seconds"], 18)
        self.assertEqual(result["metric_scope"], "last_candidate")
        self.assertEqual(result["duration_scope"], "all_variants_all_rounds")
        sessions = [self.calls[i]["env"]["MZSTD_SESSION_ID"] for i in range(0, 12, 2)]
        self.assertEqual(len(set(sessions)), 6)
        for row in result["paired_rounds"]:
            for variant in ("baseline", "candidate"):
                self.assertEqual(row[variant]["metrics"]["duration_seconds"], 3)
                self.assertIn("unknown_native_metric", row[variant]["metrics"])

    def test_last_candidate_percentiles_are_not_merged(self):
        self.install_runner()
        counter = iter(range(1, 5))
        original = self.fixture

        def fixture(env):
            data = original(env)
            data["metrics"]["rtt_p99_ms"] = next(counter) * 10
            return data
        with mock.patch.object(self, "fixture", side_effect=fixture), \
                mock.patch.object(lab.secrets, "randbelow", return_value=0):
            result = lab.execute(self.args("benchmark-pair", rounds=2))
        self.assertEqual(result["metrics"]["rtt_p99_ms"], 30)
        self.assertEqual(result["metrics"]["duration_seconds"], 12)

    def test_cleanup_runs_after_timeout_and_cancellation(self):
        for failure in (lab.HarnessError("timeout"), lab.Cancelled("cancelled")):
            with self.subTest(failure=type(failure).__name__):
                self.calls.clear()
                self.install_runner(failure=failure)
                with self.assertRaises(type(failure)):
                    lab.execute(self.args())
                self.assertEqual(len(self.calls), 2)
                self.assertTrue(self.calls[-1]["cleanup"])
                self.assertFalse((self.output / "summary.json").exists())

    def test_cleanup_failure_blocks_summary(self):
        self.install_runner(cleanup_failure=True)
        with self.assertRaisesRegex(lab.HarnessError, "cleanup"):
            lab.execute(self.args())
        self.assertFalse((self.output / "summary.json").exists())

    def test_nonce_hash_sha_and_session_mismatches_fail_closed(self):
        for field in ("run_nonce", "commit_sha", "artifact_sha256", "session_id"):
            with self.subTest(field=field):
                self.calls.clear()
                self.install_runner(mutate=lambda d, key=field: d.update({key: "wrong"}))
                with self.assertRaisesRegex(lab.HarnessError, "binding"):
                    lab.execute(self.args())
                self.assertTrue(self.calls[-1]["cleanup"])
                self.assertFalse((self.output / "summary.json").exists())

    def test_missing_metrics_not_filled_with_zero(self):
        self.install_runner(mutate=lambda d: d["metrics"].pop("nic_rx_bytes"))
        with self.assertRaisesRegex(lab.HarnessError, "nic_rx_bytes"):
            lab.execute(self.args())
        self.assertFalse((self.output / "summary.json").exists())

    def test_short_duration_is_rejected_per_variant(self):
        self.install_runner(mutate=lambda d: d["metrics"].update(duration_seconds=1))
        with self.assertRaisesRegex(lab.HarnessError, "short measured"):
            lab.execute(self.args("benchmark-pair"))
        self.assertFalse((self.output / "summary.json").exists())

    def test_stale_report_rejected(self):
        path = self.root / "stale.json"
        path.write_text("{}", encoding="utf-8")
        os.utime(path, ns=(1, 1))
        with self.assertRaisesRegex(lab.HarnessError, "fresh"):
            lab.read_report(path, time.time_ns(), {}, 1, 1, False)

    def test_config_rejects_shell_string_and_external_path_overlap(self):
        self.configuration["scenario_command"] = "python secret-argv"
        self.save_config()
        with self.assertRaises(lab.HarnessError):
            lab.load_config(self.config, self.output)
        self.configuration["scenario_command"] = [sys.executable]
        self.configuration["work_dir"] = str(self.output)
        self.save_config()
        with self.assertRaisesRegex(lab.HarnessError, "disjoint"):
            lab.load_config(self.config, self.output)

    def test_process_timeout_stops_group_without_printing_argv(self):
        process = mock.Mock(pid=123)
        process.wait.side_effect = [subprocess.TimeoutExpired("private-argv", 1), 0, 0]
        with mock.patch.object(lab.subprocess, "Popen", return_value=process) as popen, \
                mock.patch.object(lab.os, "killpg", create=True) as killpg, \
                mock.patch.object(lab.signal, "SIGKILL", 9, create=True):
            with self.assertRaisesRegex(lab.HarnessError, "timed out"):
                lab.run_process(["private-argv"], {}, self.work,
                                self.work / "mock-process.log", 1, 1)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(killpg.call_args_list, [mock.call(123, lab.signal.SIGTERM),
                                               mock.call(123, 9)])

    def test_shared_lock_is_nonblocking_and_closes_on_contention(self):
        fake = mock.Mock(LOCK_EX=2, LOCK_NB=4)
        fake.flock.side_effect = BlockingIOError()
        with mock.patch.object(lab, "fcntl", fake), \
                mock.patch.object(lab.sys, "platform", "linux"), \
                mock.patch.object(lab.os, "open", return_value=88) as opened, \
                mock.patch.object(lab.os, "close") as closed:
            with self.assertRaisesRegex(lab.HarnessError, "locked"):
                with lab.work_lock(self.work):
                    self.fail("lock unexpectedly acquired")
        self.assertEqual(Path(opened.call_args.args[0]).name, ".mzstd-lab.lock")
        fake.flock.assert_called_once_with(88, 6)
        closed.assert_called_once_with(88)

    def test_cli_failure_does_not_expose_command_arguments(self):
        args = self.args()
        stderr = io.StringIO()
        with mock.patch.object(lab, "parse_args", return_value=args), \
                mock.patch.object(lab, "execute", side_effect=OSError("private-secret-argv")), \
                contextlib.redirect_stderr(stderr):
            self.assertEqual(lab.main([]), 1)
        self.assertNotIn("private-secret-argv", stderr.getvalue())

    def test_missing_report_still_runs_cleanup(self):
        self.install_runner()
        with mock.patch.object(lab, "read_report", side_effect=FileNotFoundError("mock missing")):
            with self.assertRaises(FileNotFoundError):
                lab.execute(self.args())
        self.assertTrue(self.calls[-1]["cleanup"])
        self.assertFalse((self.output / "summary.json").exists())

    def test_artifact_modified_during_scenario_is_rejected(self):
        def tamper(data):
            self.fabric.write_bytes(b"tampered mock artifact")
        self.install_runner(mutate=tamper)
        with self.assertRaisesRegex(lab.HarnessError, "JAR changed"):
            lab.execute(self.args())
        self.assertTrue(self.calls[-1]["cleanup"])
        self.assertFalse((self.output / "summary.json").exists())

    def test_nonce_is_generated_only_when_workflow_value_is_missing(self):
        self.install_runner()
        with mock.patch.dict(os.environ):
            os.environ.pop("MZSTD_RUN_NONCE", None)
            result = lab.execute(self.args())
        self.assertEqual(len(result["run_nonce"]), 64)
        self.assertEqual(result["run_nonce"], self.calls[0]["env"]["MZSTD_RUN_NONCE"])

    def test_signal_cancellation_keeps_cleanup_interrupt_resistant(self):
        registered = {}
        restored = []

        def signal_handler(sig, handler):
            if callable(handler):
                registered[sig] = handler
            else:
                restored.append(sig)
            return "previous-handler"
        with mock.patch.object(lab.signal, "signal", side_effect=signal_handler):
            with lab.cancellation_signals():
                with self.assertRaises(lab.Cancelled):
                    registered[lab.signal.SIGTERM](lab.signal.SIGTERM, None)
                registered[lab.signal.SIGINT](lab.signal.SIGINT, None)
        self.assertEqual(set(restored), {lab.signal.SIGTERM, lab.signal.SIGINT})

    def test_report_directory_rejected_before_open(self):
        with self.assertRaisesRegex(lab.HarnessError, "regular nonsymlink"):
            lab.read_report(self.work, time.time_ns(), {}, 1, 1, False)

    def test_workflow_long_jar_options_and_short_aliases(self):
        args = self.args("benchmark-pair")
        self.assertEqual(args.duration_seconds, 3)
        self.assertEqual(args.warmup_seconds, 6)
        self.assertEqual(args.baseline_fabric_jar, str(self.fabric))
        cli = lab.parser()
        for mode in cli._subparsers._group_actions[0].choices.values():
            if mode.prog.endswith("benchmark-pair"):
                options = mode._option_string_actions
                for variant in ("baseline", "candidate"):
                    for kind in ("fabric", "paper"):
                        self.assertIs(options["--" + variant + "-" + kind],
                                      options["--" + variant + "-" + kind + "-jar"])

    def test_immediate_report_does_not_satisfy_observation_interval(self):
        self.install_runner()
        with mock.patch.object(lab.time, "monotonic", return_value=10):
            with self.assertRaisesRegex(lab.HarnessError, "observation interval"):
                lab.execute(self.args())
        self.assertTrue(self.calls[-1]["cleanup"])
        self.assertFalse((self.output / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
