"""Mock-only preflight tests; never run a game, FRP, Java or a real driver."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import zipfile

import runner_doctor as doctor


class RunnerDoctorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get("TEMP"))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config_dir = self.root / "config"
        self.config_dir.mkdir()
        self.out_dir = self.root / "reports"
        self.out_dir.mkdir()
        self.config = self.config_dir / "private.json"
        self.output = self.out_dir / "doctor.json"
        self.work = self.root / "not-created-work"
        self.runtime = {}
        for name in ("java", "frps", "frpc", "xvfb"):
            path = self.root / ("fixture-" + name)
            path.write_bytes(b"mock only: never executable game code")
            path.chmod(0o700)
            self.runtime[name + "_executable"] = str(path)
        for name, member, metadata in (
                ("minecraft", "version.json", {"id": "26.2"}),
                ("fabric_api", "fabric.mod.json", {"id": "fabric-api", "version": "0.160.0+26.2"}),
                ("krypton", "fabric.mod.json", {"id": "krypton", "version": "0.2.11"})):
            path = self.root / ("mock-" + name + ".jar")
            self.jar(path, member, metadata)
            self.runtime[name + "_jar"] = str(path)
        self.data = {"schema_version": 1,
                     "scenario_command": [sys.executable, "never-run-scenario"],
                     "cleanup_command": [sys.executable, "never-run-cleanup"],
                     "work_dir": str(self.work), "runtime": self.runtime}
        self.save()
        self.permission_check = doctor._config_permissions
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(doctor.sys, "platform", "linux"))
        self.stack.enter_context(mock.patch.object(doctor.platform, "machine", return_value="x86_64"))
        self.stack.enter_context(mock.patch.object(doctor.platform, "release", return_value="6.8.12-private-host"))
        self.stack.enter_context(mock.patch.object(doctor.os, "cpu_count", return_value=8))
        self.stack.enter_context(mock.patch.object(doctor.os, "sysconf", create=True,
                                                side_effect=lambda key: 4096 if key == "SC_PAGE_SIZE" else 1000))
        # Windows does not expose POSIX uid/mode; cover these separately below.
        self.stack.enter_context(mock.patch.object(doctor, "_config_permissions", return_value=None))
        self.runner = self.stack.enter_context(mock.patch.object(doctor.subprocess, "run", side_effect=self.version_probe))

    @staticmethod
    def jar(path, member, metadata):
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(member, json.dumps(metadata))

    def save(self):
        self.config.write_text(json.dumps(self.data), encoding="utf-8")
        self.config.chmod(0o600)

    def version_probe(self, argv, **kwargs):
        if argv[1] == "-version":
            return subprocess.CompletedProcess(argv, 0, b"", b'openjdk version "25.0.1" 2025-10-21\nprivate-host token-secret')
        return subprocess.CompletedProcess(argv, 0, b"0.65.0\n", b"")

    def report(self):
        self.save()
        return doctor.collect(str(self.config), self.out_dir)

    def cli(self, config=None, output=None, extra=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        args = ["--config", str(config or self.config),
                "--report-json", str(output or self.output)] + (extra or [])
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = doctor.main(args)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_ready_mock_report_has_whitelist_and_no_measurement(self):
        report = self.report()
        self.assertTrue(report["ready"], report)
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["type"], "runner_preflight_only")
        self.assertIs(report["measurement_produced"], False)
        self.assertEqual(set(report["facts"]), doctor.FACT_KEYS)
        self.assertEqual(report["facts"]["java_major"], 25)
        self.assertEqual(report["facts"]["kernel"], "6.8.12")
        self.assertEqual(report["facts"]["physical_memory_bytes"], 4096000)
        self.assertEqual(report["facts"]["mod_versions"]["fabric-api"], "0.160.0+26.2")
        path = Path(self.runtime["minecraft_jar"])
        self.assertEqual(report["facts"]["artifact_sha256"]["minecraft"], hashlib.sha256(path.read_bytes()).hexdigest())

    def test_wrong_platform_does_not_run_any_commands(self):
        with mock.patch.object(doctor.sys, "platform", "win32"):
            report = self.report()
        self.assertIn("PLATFORM_UNSUPPORTED", report["issues"])
        self.runner.assert_not_called()

    def test_wrong_architecture_does_not_run_commands(self):
        with mock.patch.object(doctor.platform, "machine", return_value="aarch64-private-host"):
            report = self.report()
        self.assertIn("PLATFORM_UNSUPPORTED", report["issues"])
        self.assertEqual(report["facts"]["arch"], "unsupported")
        self.runner.assert_not_called()

    def test_old_python_does_not_run_commands(self):
        with mock.patch.object(doctor.sys, "version_info", (3, 9, 19)):
            report = self.report()
        self.assertIn("PYTHON_UNSUPPORTED", report["issues"])
        self.runner.assert_not_called()

    def test_java_wrong_major(self):
        original = self.version_probe
        self.runner.side_effect = lambda argv, **kw: (subprocess.CompletedProcess(argv, 0, b"", b'java version "21.0.3"')
                                                     if argv[1] == "-version" else original(argv, **kw))
        self.assertIn("JAVA_VERSION_INVALID", self.report()["issues"])

    def test_java_first_version_is_required(self):
        original = self.version_probe
        self.runner.side_effect = lambda argv, **kw: (subprocess.CompletedProcess(argv, 0, b"", b'openjdk version "21"\nopenjdk version "25"')
                                                     if argv[1] == "-version" else original(argv, **kw))
        report = self.report()
        self.assertEqual(report["facts"]["java_major"], 21)
        self.assertIn("JAVA_VERSION_INVALID", report["issues"])

    def test_frp_version_mismatch(self):
        original = self.version_probe
        self.runner.side_effect = lambda argv, **kw: (subprocess.CompletedProcess(argv, 0, b"0.66.0", b"")
                                                     if argv[0] == self.runtime["frpc_executable"] else original(argv, **kw))
        report = self.report()
        self.assertIn("FRP_VERSION_MISMATCH", report["issues"])
        self.assertNotIn("frp_version", report["facts"])

    def test_frp_empty_or_sensitive_output_is_not_recorded(self):
        original = self.version_probe
        for text in (b"", b"private-host /private/account token-secret"):
            with self.subTest(text=text):
                self.runner.side_effect = lambda argv, **kw: (subprocess.CompletedProcess(argv, 0, text, b"")
                                                             if argv[1] == "-v" else original(argv, **kw))
                report = self.report()
                self.assertIn("FRP_VERSION_INVALID", report["issues"])
                self.assertNotIn("private-host", json.dumps(report))

    def test_wrong_game_jar_id(self):
        self.jar(Path(self.runtime["minecraft_jar"]), "version.json", {"id": "1.21"})
        self.assertIn("MINECRAFT_JAR_INVALID", self.report()["issues"])

    def test_fabric_loader_is_not_a_game_jar(self):
        self.runtime["minecraft_jar"] = self.runtime["fabric_api_jar"]
        self.assertIn("MINECRAFT_JAR_INVALID", self.report()["issues"])

    def test_wrong_mod_ids_and_empty_versions(self):
        for key, code in (("fabric_api", "FABRIC_API_JAR_INVALID"), ("krypton", "KRYPTON_JAR_INVALID")):
            for metadata in ({"id": "wrong-mod", "version": "1.0"},
                             {"id": "fabric-api" if key == "fabric_api" else key, "version": ""}):
                with self.subTest(key=key, metadata=metadata):
                    self.jar(Path(self.runtime[key + "_jar"]), "fabric.mod.json", metadata)
                    self.assertIn(code, self.report()["issues"])

    def test_malicious_metadata_not_copied_to_report(self):
        self.jar(Path(self.runtime["krypton_jar"]), "fabric.mod.json",
                 {"id": "krypton", "version": "account/private-host token-secret", "token": "token-secret"})
        report = self.report()
        self.assertIn("KRYPTON_JAR_INVALID", report["issues"])
        self.assertNotIn("token-secret", json.dumps(report))

    def test_corrupt_jar_and_duplicate_metadata_rejected(self):
        path = Path(self.runtime["minecraft_jar"])
        path.write_bytes(b"not a zip")
        self.assertIn("MINECRAFT_JAR_INVALID", self.report()["issues"])
        self.jar(path, "version.json", {"id": "26.2"})
        with zipfile.ZipFile(path, "a") as archive:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr("version.json", '{"id":"26.2"}')
        self.assertIn("MINECRAFT_JAR_INVALID", self.report()["issues"])

    def test_missing_scenario_cleanup_and_work_still_fail(self):
        for key, code in (("scenario_command", "SCENARIO_COMMAND_INVALID"),
                          ("cleanup_command", "CLEANUP_COMMAND_INVALID"),
                          ("work_dir", "WORK_DIR_INVALID")):
            with self.subTest(key=key):
                old = self.data.pop(key)
                self.assertIn(code, self.report()["issues"])
                self.data[key] = old

    def test_missing_runtime_or_executables(self):
        for name in ("java", "frps", "frpc"):
            with self.subTest(name=name):
                old = self.runtime.pop(name + "_executable")
                self.assertIn(name.upper() + "_EXECUTABLE_INVALID", self.report()["issues"])
                self.runtime[name + "_executable"] = old
        self.data.pop("runtime")
        self.assertIn("RUNTIME_INVALID", self.report()["issues"])

    def test_relative_and_nonexecutable_runtime_paths(self):
        self.runtime["java_executable"] = "java"
        self.assertIn("JAVA_EXECUTABLE_INVALID", self.report()["issues"])
        self.runtime["java_executable"] = str(self.root / "missing-java")
        self.assertIn("JAVA_EXECUTABLE_INVALID", self.report()["issues"])
        self.runtime["java_executable"] = sys.executable
        with mock.patch.object(doctor.os, "access", return_value=False):
            self.assertIn("JAVA_EXECUTABLE_INVALID", self.report()["issues"])

    def test_linux_config_permissions_and_owner(self):
        with mock.patch.object(doctor.os, "getuid", create=True, return_value=1000):
            for uid, mode in ((1000, 0o600), (0, 0o400)):
                self.permission_check(SimpleNamespace(st_uid=uid, st_mode=stat.S_IFREG | mode))
            for uid, mode in ((1001, 0o600), (1000, 0o640), (1000, 0o607), (0, 0o644)):
                with self.subTest(uid=uid, mode=mode), self.assertRaises(doctor.CheckFailure) as error:
                    self.permission_check(SimpleNamespace(st_uid=uid, st_mode=stat.S_IFREG | mode))
                self.assertEqual(str(error.exception), "CONFIG_PERMISSIONS_INVALID")

    def test_permissions_failure_produces_diagnostic(self):
        with mock.patch.object(doctor, "_config_permissions", side_effect=doctor.CheckFailure("CONFIG_PERMISSIONS_INVALID")):
            code, _, _ = self.cli()
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(self.output.read_text())["issues"], ["CONFIG_PERMISSIONS_INVALID"])
        self.runner.assert_not_called()

    def test_config_symlink_or_nonregular_rejected(self):
        original = Path.lstat
        def fake(path, *args, **kwargs):
            if path == self.config:
                return SimpleNamespace(st_mode=stat.S_IFLNK | 0o777)
            return original(path, *args, **kwargs)
        with mock.patch.object(Path, "lstat", fake):
            self.assertIn("CONFIG_PATH_INVALID", doctor.collect(str(self.config), self.out_dir)["issues"])

    def test_config_open_inode_race_rejected(self):
        original = doctor.os.fstat
        def changed(fd):
            result = original(fd)
            return SimpleNamespace(st_mode=result.st_mode, st_dev=result.st_dev, st_ino=result.st_ino + 1)
        with mock.patch.object(doctor.os, "fstat", side_effect=changed):
            self.assertIn("CONFIG_PATH_INVALID", doctor.collect(str(self.config), self.out_dir)["issues"])

    def test_display_existing_requires_display(self):
        self.runtime["display_mode"] = "existing"
        with mock.patch.dict(doctor.os.environ, {}, clear=True):
            self.assertIn("DISPLAY_MISSING", self.report()["issues"])
        with mock.patch.dict(doctor.os.environ, {"DISPLAY": ":99-private-host"}):
            report = self.report()
        self.assertTrue(report["ready"])
        self.assertNotIn("private-host", json.dumps(report))

    def test_xvfb_checks_only_file_and_does_not_launch_it(self):
        self.runtime["display_mode"] = "xvfb"
        self.assertTrue(self.report()["ready"])
        self.assertTrue(all(call.args[0][0] != self.runtime["xvfb_executable"] for call in self.runner.call_args_list))
        self.runtime.pop("xvfb_executable")
        self.assertIn("XVFB_EXECUTABLE_INVALID", self.report()["issues"])

    def test_invalid_display_mode(self):
        self.runtime["display_mode"] = "auto"
        self.assertIn("DISPLAY_MODE_INVALID", self.report()["issues"])

    def test_network_only_labels_and_counts(self):
        self.runtime["network_interfaces"] = ["eth0", "lo"]
        def counter(path, **kwargs):
            self.assertTrue(str(path).replace("\\", "/").startswith("/sys/class/net/"))
            return "123\n" if path.name == "rx_bytes" else "456\n"
        with mock.patch.object(Path, "read_text", counter):
            report = self.report()
        self.assertEqual(report["network"], [
            {"label": "nic-1", "rx_bytes": 123, "tx_bytes": 456},
            {"label": "nic-2", "rx_bytes": 123, "tx_bytes": 456}])
        self.assertNotIn("eth0", json.dumps(report))

    def test_network_names_reject_traversal_duplicates_and_wrong_types(self):
        for names in (["../private"], ["eth0/../../x"], ["eth0", "eth0"], [".."], "eth0", [7]):
            with self.subTest(names=names):
                self.runtime["network_interfaces"] = names
                self.assertIn("NETWORK_INTERFACES_INVALID", self.report()["issues"])

    def test_unreadable_network_counter_is_fixed_issue(self):
        self.runtime["network_interfaces"] = ["eth0"]
        with mock.patch.object(Path, "read_text", side_effect=OSError("private-host token-secret")):
            report = self.report()
        self.assertIn("NETWORK_COUNTER_UNAVAILABLE", report["issues"])
        self.assertNotIn("token-secret", json.dumps(report))

    def test_no_scenario_cleanup_game_profile_or_real_mc(self):
        self.assertTrue(self.report()["ready"])
        self.assertEqual(self.runner.call_count, 3)
        for call in self.runner.call_args_list:
            argv = call.args[0]
            self.assertIn(argv[1], ("-version", "-v"))
            self.assertEqual(len(argv), 2)
            self.assertFalse(call.kwargs["shell"])
            self.assertEqual(call.kwargs["timeout"], 10)
            self.assertEqual(call.kwargs["stdin"], subprocess.DEVNULL)
            self.assertNotIn("cwd", call.kwargs)
        self.assertFalse(self.work.exists())
        self.assertFalse((self.root / "game").exists())
        self.assertFalse((self.root / "profile").exists())

    def test_timeout_spawn_and_nonzero_outputs_are_private(self):
        for failure in (subprocess.TimeoutExpired(["token-secret"], 10, output=b"private-host"),
                        OSError("account/private-host token-secret")):
            with self.subTest(failure=type(failure).__name__):
                self.runner.side_effect = failure
                report = self.report()
                self.assertIn("JAVA_PROBE_FAILED", report["issues"])
                self.assertNotIn("token-secret", json.dumps(report))
        self.runner.side_effect = None
        self.runner.return_value = subprocess.CompletedProcess(["secret"], 1, b"token-secret", b"private-host")
        self.assertIn("FRPS_PROBE_FAILED", self.report()["issues"])

    def test_cli_failure_writes_diagnostic_and_fixed_console(self):
        self.runtime["java_executable"] = "sensitive-relative-path"
        self.save()
        code, stdout, stderr = self.cli()
        report = json.loads(self.output.read_text())
        self.assertEqual(code, 1)
        self.assertFalse(report["ready"])
        self.assertEqual(stdout, "RUNNER_PREFLIGHT_FAILED\n")
        self.assertEqual(stderr, "")
        self.assertIn("JAVA_EXECUTABLE_INVALID", report["issues"])
        encoded = json.dumps(report) + stdout + stderr
        for private in (str(self.root), str(self.config), "private-host", "token-secret",
                        "sensitive-relative-path", "never-run-scenario"):
            self.assertNotIn(private, encoded)

    def test_cli_malformed_config_writes_fixed_diagnostic(self):
        self.config.write_text("private-host /account token-secret not json")
        code, stdout, stderr = self.cli()
        self.assertEqual(code, 1)
        report = json.loads(self.output.read_text())
        self.assertEqual(report["issues"], ["CONFIG_INVALID"])
        self.assertNotIn("token-secret", json.dumps(report) + stdout + stderr)

    def test_cli_success_atomic_0600_report(self):
        original = doctor.os.replace
        replacements = []
        def replace(source, target):
            replacements.append((Path(source), Path(target)))
            self.assertEqual(Path(source).parent, self.out_dir)
            self.assertTrue(json.loads(Path(source).read_text())["ready"])
            original(source, target)
        with mock.patch.object(doctor.os, "replace", side_effect=replace), \
                mock.patch.object(doctor.os, "chmod", wraps=doctor.os.chmod) as chmod:
            code, stdout, stderr = self.cli()
        self.assertEqual((code, stdout, stderr), (0, "RUNNER_PREFLIGHT_READY\n", ""))
        self.assertEqual(len(replacements), 1)
        self.assertTrue(any(call.args[1] == 0o600 for call in chmod.call_args_list))
        if os.name == "posix":
            self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o600)
        self.assertEqual(list(self.out_dir.iterdir()), [self.output])

    def test_reject_config_overwrite_and_inside_config_directory(self):
        before = self.config.read_bytes()
        for target in (self.config, self.config_dir / "output.json", self.config_dir / "nested" / "output.json"):
            with self.subTest(target=target):
                code, stdout, stderr = self.cli(output=target)
                self.assertEqual(code, 1)
                self.assertEqual(stdout, "")
                self.assertEqual(stderr, "REPORT_PATH_INVALID\n")
                self.assertEqual(self.config.read_bytes(), before)
        self.runner.assert_not_called()

    def test_reject_report_symlink_without_following_it(self):
        original = Path.lstat
        def fake(path, *args, **kwargs):
            if path == self.output:
                return SimpleNamespace(st_mode=stat.S_IFLNK | 0o777)
            return original(path, *args, **kwargs)
        with mock.patch.object(Path, "lstat", fake):
            self.assertEqual(self.cli()[0], 1)
        self.assertFalse(self.output.exists())

    def test_reject_report_hardlink_or_directory(self):
        self.output.mkdir()
        self.assertEqual(self.cli()[0], 1)
        self.output.rmdir()
        os.link(self.config, self.output)
        before = self.config.read_bytes()
        self.assertEqual(self.cli()[0], 1)
        self.assertEqual(self.config.read_bytes(), before)

    def test_atomic_write_failure_cleans_temp_and_keeps_config(self):
        before = self.config.read_bytes()
        with mock.patch.object(doctor.os, "replace", side_effect=OSError("private-host token-secret")):
            code, stdout, stderr = self.cli()
        self.assertEqual(code, 1)
        self.assertEqual(stderr, "REPORT_WRITE_FAILED\n")
        self.assertNotIn("token-secret", stdout + stderr)
        self.assertEqual(list(self.out_dir.iterdir()), [])
        self.assertEqual(self.config.read_bytes(), before)

    def test_cli_argument_errors_do_not_echo_argv(self):
        code, stdout, stderr = self.cli(extra=["--private-token=token-secret"])
        self.assertEqual((code, stdout, stderr), (1, "", "ARGUMENTS_INVALID\n"))

    def test_help_never_discloses_invocation_path(self):
        stdout = io.StringIO()
        with mock.patch.object(doctor.sys, "argv", [str(self.root / "private-account" / "runner_doctor.py")]), \
                contextlib.redirect_stdout(stdout), self.assertRaises(SystemExit) as exit_code:
            doctor.main(["--help"])
        self.assertEqual(exit_code.exception.code, 0)
        self.assertNotIn(str(self.root), stdout.getvalue())
        self.assertIn("runner_doctor.py", stdout.getvalue())

    def test_wrong_platform_cli_still_writes_diagnostic(self):
        with mock.patch.object(doctor.sys, "platform", "win32"):
            code, _, _ = self.cli()
        self.assertEqual(code, 1)
        self.assertIn("PLATFORM_UNSUPPORTED", json.loads(self.output.read_text())["issues"])
        self.runner.assert_not_called()

    def test_missing_config_cli_still_writes_diagnostic(self):
        code, stdout, stderr = self.cli(config=self.config_dir / "nonexistent-private.json")
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(self.output.read_text())["ready"])
        self.assertEqual((stdout, stderr), ("RUNNER_PREFLIGHT_FAILED\n", ""))

    def test_existing_report_replaced_not_appended(self):
        self.output.write_text("old-private-host token-secret")
        self.assertEqual(self.cli()[0], 0)
        self.assertNotIn("token-secret", self.output.read_text())
        self.assertTrue(json.loads(self.output.read_text())["ready"])

    def test_report_failure_cannot_replace_config_via_late_symlink(self):
        before = self.config.read_bytes()
        original = doctor.report_target
        checks = []
        def target(config, output):
            checks.append(output)
            if len(checks) == 3:
                raise doctor.CheckFailure("REPORT_PATH_INVALID")
            return original(config, output)
        with mock.patch.object(doctor, "report_target", side_effect=target):
            self.assertEqual(self.cli()[0], 1)
        self.assertEqual(self.config.read_bytes(), before)
        self.assertEqual(list(self.out_dir.iterdir()), [])

    def test_bad_schema_and_timeout_types(self):
        self.data["schema_version"] = True
        self.assertIn("CONFIG_INVALID", self.report()["issues"])
        self.data["schema_version"] = 1
        self.data["startup_timeout_seconds"] = False
        self.assertIn("CONFIG_INVALID", self.report()["issues"])

    def test_mod_versions_accept_bounded_build_metadata(self):
        for version in ("0.3.0+mc26.2", "0.160.0-rc.1+abcdef26"):
            with self.subTest(version=version):
                self.jar(Path(self.runtime["krypton_jar"]), "fabric.mod.json",
                         {"id": "krypton", "version": version})
                report = self.report()
                self.assertTrue(report["ready"], report["issues"])
                self.assertEqual(report["facts"]["mod_versions"]["krypton"], version)


if __name__ == "__main__":
    unittest.main()
