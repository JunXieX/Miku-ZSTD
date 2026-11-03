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

    def test_preflight_artifact_is_isolated_from_fresh_scenario_directory(self):
        self.output.mkdir()
        exported = self.output / "sanitized"
        exported.mkdir()
        diagnostic = exported / "runner-preflight.json"
        diagnostic.write_text('{"measurement_produced": false}', encoding="utf-8")
        args = self.args()
        args.result_dir = str(self.output / "scenario")
        self.install_runner()
        lab.execute(args)
        self.assertTrue((self.output / "scenario" / "summary.json").is_file())
        self.assertEqual(json.loads(diagnostic.read_text(encoding="utf-8")),
                         {"measurement_produced": False})


    def lifecycle_args(self, mode="smoke", rounds=3):
        args = self.args(mode, rounds)
        sanitized = self.output / "sanitized"
        sanitized.mkdir(parents=True, exist_ok=True)
        args.result_dir = str(self.output / "scenario")
        args.event_log = str(sanitized / "lifecycle.jsonl")
        return args

    @staticmethod
    def event_rows(args):
        return [json.loads(line) for line in Path(args.event_log).read_text(
            encoding="utf-8").splitlines()]

    def test_lifecycle_smoke_success_has_fixed_sequence_and_schema(self):
        self.install_runner()
        args = self.lifecycle_args()
        result = lab.execute(args)
        rows = self.event_rows(args)
        self.assertEqual([row["event"] for row in rows], [
            "orchestration_started", "lock_acquired", "variant_started",
            "report_received", "cleanup_started", "cleanup_finished",
            "orchestration_finished"])
        self.assertEqual([row["seq"] for row in rows], list(range(1, 8)))
        base = {"schema_version", "type", "event", "seq", "monotonic_elapsed_ms",
                "mode", "measurement_produced"}
        for row in rows:
            scoped = row["event"] in ("variant_started", "report_received",
                                      "cleanup_started", "cleanup_finished")
            self.assertEqual(set(row), base | ({"round_id", "variant"} if scoped else set()))
            self.assertEqual(row["schema_version"], 1)
            self.assertEqual(row["type"], "orchestration_event")
            self.assertEqual(row["mode"], "smoke")
            self.assertIs(row["measurement_produced"], False)
            self.assertIs(type(row["seq"]), int)
            self.assertIs(type(row["monotonic_elapsed_ms"]), int)
            self.assertGreaterEqual(row["monotonic_elapsed_ms"], 0)
            if scoped:
                self.assertEqual((row["round_id"], row["variant"]), (1, "candidate"))
        self.assertEqual(result["metrics"]["duration_seconds"], 2)
        self.assertEqual(result["metrics"]["unknown_native_metric"], {"must_survive": 123})
        self.assertEqual([p.name for p in Path(args.result_dir).iterdir()], ["summary.json"])

    def test_lifecycle_pair_records_actual_round_and_variant_order(self):
        self.install_runner()
        args = self.lifecycle_args("benchmark-pair", rounds=2)
        with mock.patch.object(lab.secrets, "randbelow", return_value=0):
            result = lab.execute(args)
        rows = self.event_rows(args)
        starts = [row for row in rows if row["event"] == "variant_started"]
        self.assertEqual([(row["round_id"], row["variant"]) for row in starts],
                         [(1, "baseline"), (1, "candidate"),
                          (2, "candidate"), (2, "baseline")])
        self.assertTrue(all(row["mode"] == "benchmark-pair" for row in rows))
        self.assertEqual(len(rows), 19)
        self.assertEqual(result["metrics"]["duration_seconds"], 12)
        self.assertEqual(rows[-1]["event"], "orchestration_finished")

    def test_lifecycle_failure_preserves_exception_and_excludes_sensitive_text(self):
        secret = "private-driver-argv-stdout-config-path-exception"
        failure = RuntimeError(secret)
        self.install_runner(failure=failure)
        args = self.lifecycle_args()
        with self.assertRaises(RuntimeError) as raised:
            lab.execute(args)
        self.assertIs(raised.exception, failure)
        rows = self.event_rows(args)
        self.assertEqual([row["event"] for row in rows], [
            "orchestration_started", "lock_acquired", "variant_started",
            "cleanup_started", "cleanup_finished", "orchestration_failed"])
        text = Path(args.event_log).read_text(encoding="utf-8")
        for value in (secret, str(self.config), str(self.work), str(self.fabric),
                      "fixture-command-never-run", "mock-only private output"):
            self.assertNotIn(value, text)
        self.assertFalse((Path(args.result_dir) / "summary.json").exists())
        self.assertTrue(self.calls[-1]["cleanup"])

    def test_lifecycle_cancelled_still_cleans_up_and_closes(self):
        failure = lab.Cancelled("private cancellation details")
        self.install_runner(failure=failure)
        args = self.lifecycle_args()
        events = lab.LifecycleLog(args.event_log, args.result_dir, args.mode)
        with mock.patch.object(lab, "lifecycle", return_value=events):
            with self.assertRaises(lab.Cancelled) as raised:
                lab.execute(args)
        self.assertIs(raised.exception, failure)
        self.assertTrue(events.stream.closed)
        self.assertEqual([row["event"] for row in self.event_rows(args)][-3:],
                         ["cleanup_started", "cleanup_finished", "orchestration_cancelled"])
        self.assertTrue(self.calls[-1]["cleanup"])
        self.assertNotIn("private cancellation", Path(args.event_log).read_text(encoding="utf-8"))

    def test_lifecycle_keyboard_interrupt_is_cancelled_after_cleanup(self):
        self.install_runner(failure=KeyboardInterrupt("private interrupt"))
        args = self.lifecycle_args()
        with self.assertRaises(KeyboardInterrupt):
            lab.execute(args)
        self.assertTrue(self.calls[-1]["cleanup"])
        self.assertEqual(self.event_rows(args)[-1]["event"], "orchestration_cancelled")

    def test_lifecycle_cleanup_failure_blocks_summary_and_records_failure(self):
        self.install_runner(cleanup_failure=True)
        args = self.lifecycle_args()
        with self.assertRaisesRegex(lab.HarnessError, "cleanup"):
            lab.execute(args)
        self.assertEqual([row["event"] for row in self.event_rows(args)], [
            "orchestration_started", "lock_acquired", "variant_started",
            "report_received", "cleanup_started", "cleanup_failed", "orchestration_failed"])
        self.assertFalse((Path(args.result_dir) / "summary.json").exists())

    def test_lifecycle_cleanup_failure_retains_precedence_over_cancellation(self):
        self.install_runner(failure=lab.Cancelled("private cancellation"), cleanup_failure=True)
        args = self.lifecycle_args()
        with self.assertRaisesRegex(lab.HarnessError, "cleanup") as raised:
            lab.execute(args)
        self.assertNotIsInstance(raised.exception, lab.Cancelled)
        self.assertEqual([row["event"] for row in self.event_rows(args)][-2:],
                         ["cleanup_failed", "orchestration_failed"])

    def test_lifecycle_lock_conflict_records_failure_and_closes(self):
        runner = self.install_runner()
        args = self.lifecycle_args()
        events = lab.LifecycleLog(args.event_log, args.result_dir, args.mode)
        with mock.patch.object(lab, "work_lock", side_effect=lab.HarnessError("locked")), \
                mock.patch.object(lab, "lifecycle", return_value=events):
            with self.assertRaisesRegex(lab.HarnessError, "locked"):
                lab.execute(args)
        runner.assert_not_called()
        self.assertTrue(events.stream.closed)
        self.assertEqual([row["event"] for row in self.event_rows(args)],
                         ["orchestration_started", "orchestration_failed"])

    def test_lifecycle_config_failure_is_fixed_event_without_raw_details(self):
        args = self.lifecycle_args()
        with mock.patch.object(lab, "load_config", side_effect=OSError("private-config-error")):
            with self.assertRaises(OSError):
                lab.execute(args)
        self.assertEqual([row["event"] for row in self.event_rows(args)],
                         ["orchestration_started", "orchestration_failed"])
        self.assertNotIn("private-config-error", Path(args.event_log).read_text(encoding="utf-8"))

    def test_lifecycle_rejects_relative_wrong_name_and_wrong_parent(self):
        args = self.lifecycle_args()
        other = self.root / "other"
        other.mkdir()
        paths = [Path("sanitized/lifecycle.jsonl"), self.config,
                 Path(args.result_dir) / "summary.json", self.work / "scenario.log",
                 other / "lifecycle.jsonl"]
        before = self.config.read_bytes()
        for path in paths:
            with self.subTest(path=path.name):
                with self.assertRaises((lab.HarnessError, OSError)):
                    with lab.LifecycleLog(path, args.result_dir, args.mode):
                        self.fail("invalid lifecycle path accepted")
        self.assertEqual(self.config.read_bytes(), before)
        self.assertFalse((other / "lifecycle.jsonl").exists())

    def test_lifecycle_rejects_missing_parent_without_creating_directories(self):
        args = self.args()
        args.result_dir = str(self.output / "scenario")
        path = self.output / "sanitized" / "lifecycle.jsonl"
        with self.assertRaises((lab.HarnessError, OSError)):
            with lab.LifecycleLog(path, args.result_dir, args.mode):
                self.fail("missing parent accepted")
        self.assertFalse(self.output.exists())

    def test_lifecycle_rejects_symlink_file_parent_and_ancestor_before_open(self):
        args = self.lifecycle_args()
        path = Path(args.event_log)
        original = Path.is_symlink
        for symlink in (path, path.parent, path.parent.parent):
            with self.subTest(component=symlink.name):
                with mock.patch.object(Path, "is_symlink", autospec=True,
                                       side_effect=lambda p: p == symlink or original(p)), \
                        mock.patch.object(lab.os, "open") as opened:
                    with self.assertRaisesRegex(lab.HarnessError, "symlink"):
                        with lab.LifecycleLog(path, args.result_dir, args.mode):
                            self.fail("symlink accepted")
                opened.assert_not_called()
        self.assertFalse(path.exists())

    def test_lifecycle_exclusive_creation_preserves_existing_files(self):
        args = self.lifecycle_args()
        path = Path(args.event_log)
        path.write_text("private existing log", encoding="utf-8")
        before = path.read_bytes()
        with self.assertRaises(FileExistsError):
            with lab.LifecycleLog(path, args.result_dir, args.mode):
                self.fail("existing file overwritten")
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.config.read_text(encoding="utf-8"), json.dumps(self.configuration))

    def test_lifecycle_existing_hardlink_does_not_overwrite_config(self):
        args = self.lifecycle_args()
        path = Path(args.event_log)
        os.link(self.config, path)
        before = self.config.read_bytes()
        with self.assertRaises(FileExistsError):
            with lab.LifecycleLog(path, args.result_dir, args.mode):
                self.fail("existing hardlink accepted")
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.config.read_bytes(), before)

    def test_lifecycle_creation_uses_exclusive_nofollow_and_private_mode(self):
        args = self.lifecycle_args()
        original = os.open
        with mock.patch.object(lab.os, "open", side_effect=original) as opened:
            with lab.LifecycleLog(args.event_log, args.result_dir, args.mode) as events:
                self.assertFalse(events.stream.closed)
        call = next(call for call in opened.call_args_list if len(call.args) == 3)
        self.assertEqual(call.args[2], 0o600)
        self.assertTrue(call.args[1] & os.O_CREAT)
        self.assertTrue(call.args[1] & os.O_EXCL)
        self.assertTrue(call.args[1] & os.O_WRONLY)
        if hasattr(os, "O_NOFOLLOW"):
            self.assertTrue(call.args[1] & os.O_NOFOLLOW)
        if os.name == "posix":
            self.assertEqual(Path(args.event_log).stat().st_mode & 0o777, 0o600)
        self.assertTrue(events.stream.closed)

    def test_lifecycle_writer_rejects_unknown_events_variants_and_extra_fields(self):
        args = self.lifecycle_args()
        with lab.LifecycleLog(args.event_log, args.result_dir, args.mode) as events:
            for value in ("private unknown driver output", {}, [], True, None):
                with self.subTest(event=value):
                    with self.assertRaises(lab.HarnessError):
                        events.emit(value)
            for value in ("private-variant", {}, True, 1):
                with self.subTest(variant=value):
                    with self.assertRaises(lab.HarnessError):
                        events.emit("variant_started", variant=value)
            for value in (0, -1, True, 1.5, "1"):
                with self.subTest(round_id=value):
                    with self.assertRaises(lab.HarnessError):
                        events.emit("variant_started", round_id=value)
            with self.assertRaises(TypeError):
                events.emit("lock_acquired", arbitrary={"secret": "private"})
            with self.assertRaises(TypeError):
                events.emit("lock_acquired", **{"stdout": "private"})
        self.assertEqual([row["event"] for row in self.event_rows(args)],
                         ["orchestration_started", "orchestration_finished"])
        self.assertNotIn("private", Path(args.event_log).read_text(encoding="utf-8"))
        for mode in ("unknown", {}, True):
            with self.assertRaises(lab.HarnessError):
                lab.LifecycleLog(args.event_log, args.result_dir, mode)

    def test_lifecycle_elapsed_is_relative_integer_and_never_decreases(self):
        args = self.lifecycle_args()
        with mock.patch.object(lab.time, "monotonic", side_effect=[10, 10, 11, 9, 12]):
            with lab.LifecycleLog(args.event_log, args.result_dir, args.mode) as events:
                events.emit("lock_acquired")
                events.emit("cleanup_started")
        rows = self.event_rows(args)
        self.assertEqual([row["monotonic_elapsed_ms"] for row in rows], [0, 1000, 1000, 2000])
        self.assertEqual([row["seq"] for row in rows], [1, 2, 3, 4])

    def test_lifecycle_zero_elapsed_does_not_replace_required_measurement(self):
        self.install_runner()
        args = self.lifecycle_args()
        with mock.patch.object(lab.time, "monotonic", return_value=10):
            with self.assertRaisesRegex(lab.HarnessError, "observation interval"):
                lab.execute(args)
        rows = self.event_rows(args)
        self.assertTrue(all(row["monotonic_elapsed_ms"] == 0 for row in rows))
        self.assertTrue(all(row["measurement_produced"] is False for row in rows))
        self.assertTrue(all("metrics" not in row and "duration_seconds" not in row for row in rows))
        self.assertNotIn("report_received", [row["event"] for row in rows])
        self.assertEqual(rows[-1]["event"], "orchestration_failed")
        self.assertFalse((Path(args.result_dir) / "summary.json").exists())

    def test_legacy_cli_without_event_log_uses_only_existing_clock_reads(self):
        self.install_runner()
        args = self.args()
        self.assertIsNone(args.event_log)
        del args.event_log  # Older direct callers may not have the new attribute.
        with mock.patch.object(lab.time, "monotonic", side_effect=[0, 2]) as clock:
            lab.execute(args)
        self.assertEqual(clock.call_count, 2)
        self.assertEqual(list(self.root.rglob("lifecycle.jsonl")), [])
        self.assertEqual([p.name for p in self.output.iterdir()], ["summary.json"])

    def test_lifecycle_optional_cli_is_present_for_both_modes(self):
        for mode in ("smoke", "benchmark-pair"):
            cli = lab.parser()._subparsers._group_actions[0].choices[mode]
            options = cli._option_string_actions
            self.assertIn("--event-log", options)
            self.assertFalse(options["--event-log"].required)
            self.assertIn("orchestration only", options["--event-log"].help)


    def test_lifecycle_openat_pins_parent_and_requires_nofollow(self):
        args = self.lifecycle_args()
        opened = mock.Mock(side_effect=[88, 89])
        stream = io.StringIO()
        with mock.patch.object(lab.os, "open", opened), \
                mock.patch.object(lab.os, "supports_dir_fd", {opened}), \
                mock.patch.object(lab.os, "O_NOFOLLOW", 0x100000, create=True), \
                mock.patch.object(lab.os, "O_DIRECTORY", 0x200000, create=True), \
                mock.patch.object(lab.os, "fstat", return_value=mock.Mock(st_mode=0o100600)), \
                mock.patch.object(lab.os, "fdopen", return_value=stream), \
                mock.patch.object(lab.os, "close") as closed:
            with lab.LifecycleLog(args.event_log, args.result_dir, args.mode):
                pass
        parent_call, file_call = opened.call_args_list
        self.assertEqual(parent_call.args[0], str(Path(args.event_log).parent))
        self.assertTrue(parent_call.args[1] & 0x100000)
        self.assertTrue(parent_call.args[1] & 0x200000)
        self.assertEqual(file_call.args[0], "lifecycle.jsonl")
        self.assertEqual(file_call.args[2], 0o600)
        self.assertTrue(file_call.args[1] & 0x100000)
        self.assertTrue(file_call.args[1] & os.O_EXCL)
        self.assertEqual(file_call.kwargs, {"dir_fd": 88})
        closed.assert_called_once_with(88)
        self.assertTrue(stream.closed)

    def test_lifecycle_logging_failure_never_skips_cleanup_or_masks_driver_failure(self):
        failure = RuntimeError("private driver failure")
        self.install_runner(failure=failure)
        args = self.lifecycle_args()
        events = lab.LifecycleLog(args.event_log, args.result_dir, args.mode)
        original = events.emit

        def emit(event, **fields):
            if event in ("cleanup_started", "cleanup_finished"):
                raise OSError("private logging failure")
            original(event, **fields)

        with mock.patch.object(lab, "lifecycle", return_value=events), \
                mock.patch.object(events, "emit", side_effect=emit):
            with self.assertRaises(RuntimeError) as raised:
                lab.execute(args)
        self.assertIs(raised.exception, failure)
        self.assertTrue(self.calls[-1]["cleanup"])
        self.assertTrue(events.stream.closed)
        self.assertEqual(self.event_rows(args)[-1]["event"], "orchestration_failed")
        self.assertFalse((Path(args.result_dir) / "summary.json").exists())

    def test_lifecycle_enter_write_failure_closes_stream(self):
        args = self.lifecycle_args()
        events = lab.LifecycleLog(args.event_log, args.result_dir, args.mode)
        with mock.patch.object(events, "emit", side_effect=OSError("private write failure")):
            with self.assertRaises(OSError):
                with events:
                    self.fail("write failure ignored")
        self.assertTrue(events.stream.closed)
        self.assertEqual(Path(args.event_log).read_bytes(), b"")


if __name__ == "__main__":
    unittest.main()
