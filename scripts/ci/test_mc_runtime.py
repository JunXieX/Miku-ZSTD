#!/usr/bin/env python3
"""Unit tests for the real-client launcher plumbing (no game, no network)."""

import unittest

import mc_runtime
import run_game_scenario


class RulesTest(unittest.TestCase):
    def test_no_rules_allows(self):
        self.assertTrue(mc_runtime.rules_allow({}, "linux", "x86_64", {}))

    def test_explicit_allow_for_other_os_denies(self):
        entry = {"rules": [{"action": "allow", "os": {"name": "windows"}}]}
        self.assertFalse(mc_runtime.rules_allow(entry, "linux", "x86_64", {}))

    def test_last_matching_rule_wins(self):
        entry = {"rules": [{"action": "allow"},
                           {"action": "disallow", "os": {"name": "linux"}}]}
        self.assertFalse(mc_runtime.rules_allow(entry, "linux", "x86_64", {}))

    def test_arch_mismatch_denies(self):
        entry = {"rules": [{"action": "allow", "os": {"arch": "x86"}}]}
        self.assertFalse(mc_runtime.rules_allow(entry, "linux", "x86_64", {}))

    def test_feature_rule_requires_matching_feature(self):
        entry = {"rules": [{"action": "allow", "features": {"is_demo_user": True}}]}
        self.assertFalse(mc_runtime.rules_allow(entry, "linux", "x86_64", {}))
        self.assertTrue(mc_runtime.rules_allow(entry, "linux", "x86_64",
                                                {"is_demo_user": True}))


class IdentityTest(unittest.TestCase):
    def test_offline_identity_is_a_version_three_uuid(self):
        uuid, name = mc_runtime.offline_identity("MikuCI0")
        self.assertEqual(name, "MikuCI0")
        self.assertEqual(len(uuid), 36)
        self.assertEqual(uuid[14], "3")

    def test_offline_identity_is_stable(self):
        self.assertEqual(mc_runtime.offline_identity("MikuCI0"),
                         mc_runtime.offline_identity("MikuCI0"))


class ArgumentTest(unittest.TestCase):
    def runtime(self):
        return {"version_id": "26.2", "asset_index": "32", "assets_dir": "/assets",
                "classpath": ["/client.jar", "/libs/netty.jar"], "natives_dir": "/natives",
                "version_type": "release",
                "jvm_template": ["-Djava.library.path=${natives_directory}/java",
                                 {"rules": [{"action": "allow", "os": {"name": "osx"}}],
                                  "value": "-XstartOnFirstThread"},
                                 "-cp", "${classpath}"],
                "log_config": "/assets/log_configs/client.xml",
                "logging_argument": "-Dlog4j.configurationFile=${path}"}

    def test_jvm_args_substitute_and_skip_other_platforms(self):
        args = mc_runtime.build_jvm_args(self.runtime())
        self.assertIn("-Djava.library.path=" + str(run_game_scenario.Path("/natives").resolve())
                      + "/java", args)
        self.assertNotIn("-XstartOnFirstThread", args)
        self.assertIn("-cp", args)
        self.assertTrue(any(argument.startswith("-Dlog4j.configurationFile=") for argument in args))

    def test_game_args_carry_offline_identity_and_quick_play(self):
        args = mc_runtime.build_game_args(self.runtime(), run_game_scenario.Path("/game"),
                                          "MikuCI0", server_address="127.0.0.1:25566")
        self.assertIn("--username", args)
        self.assertEqual(args[args.index("--username") + 1], "MikuCI0")
        self.assertEqual(args[args.index("--quickPlayMultiplayer") + 1], "127.0.0.1:25566")
        self.assertEqual(args[args.index("--accessToken") + 1], "0")
        self.assertEqual(args[args.index("--assetIndex") + 1], "32")

    def test_game_args_without_quick_play(self):
        args = mc_runtime.build_game_args(self.runtime(), run_game_scenario.Path("/game"),
                                          "MikuCI0")
        self.assertNotIn("--quickPlayMultiplayer", args)


class ByteFormatTest(unittest.TestCase):
    def test_parses_decimal_units(self):
        self.assertEqual(run_game_scenario.parse_bytes("512B"), 512)
        self.assertEqual(run_game_scenario.parse_bytes("1.5KB"), 1500)
        self.assertEqual(run_game_scenario.parse_bytes("2.0MB"), 2000000)
        self.assertEqual(run_game_scenario.parse_bytes("1.25GB"), 1250000000)

    def test_rejects_unknown_unit(self):
        with self.assertRaises(run_game_scenario.ScenarioError):
            run_game_scenario.parse_bytes("12MiB")


if __name__ == "__main__":
    unittest.main()
