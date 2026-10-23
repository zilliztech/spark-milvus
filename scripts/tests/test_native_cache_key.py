"""Native artifact cache identity changes only with native build inputs."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("native_cache_key", Path(__file__).resolve().parents[1] / "native-cache-key.py")
cache = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cache)


class NativeCacheKeyTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.git(self.root, "init", "-q")
        self.plan = {
            "sourceSha": "a" * 40, "version": "2.0.0-ci.1", "pins": {},
            "native": [{"platform": "linux-x86_64", "runner": "ubuntu-22.04", "build_jobs": 4,
                        "libc": "glibc", "glibc": "2.35", "glibcxx": "3.4.30", "cxxabi": "1.3.13"}],
            "jvm": [{"line": "4.0", "spark": "4.0.1"}],
        }
        for engine in cache.ENGINES:
            directory = self.root / engine
            directory.mkdir()
            self.git(directory, "init", "-q")
            (directory / "engine.cc").write_text("int engine;\n")
            self.commit(directory)
            self.plan["pins"][engine] = self.git(directory, "rev-parse", "HEAD")
        paths = set(cache.EXPLICIT_INPUTS) | {
            "native/build/build.py", "native/build/CMakeLists.txt", "native/build/dependencies.json",
            "native/build/profiles/linux-x86_64", "build.sbt", "core/src/main/scala/Example.scala",
        }
        for name in paths:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture for " + name + "\n")
        self.commit(self.root)
        self.identity = {"os": {"id": "ubuntu", "versionId": "22.04"}, "glibc": "glibc 2.35",
                         "tools": {"gcc": "gcc 12.3.0", "cmake": "cmake 3.27.5", "conan": "Conan 2.25.1",
                                   "rustc": "rustc 1.90.0", "java": "openjdk 21.0.8",
                                   "compilerTarget": "x86_64-linux-gnu"}}

    @staticmethod
    def git(directory, *arguments):
        return subprocess.check_output(["git", "-c", "user.name=Cache Test", "-c", "user.email=cache@example.invalid",
                                        *arguments], cwd=directory, stderr=subprocess.PIPE, text=True).strip()

    def commit(self, directory):
        self.git(directory, "add", ".")
        self.git(directory, "commit", "-qm", "fixture")

    def fingerprint(self):
        return cache.fingerprint(self.root, self.plan, "linux-x86_64", self.identity)

    def test_connector_commit_release_version_and_jvm_changes_reuse_native(self):
        before = self.fingerprint()
        (self.root / "build.sbt").write_text("new Scala settings")
        (self.root / "core/src/main/scala/Example.scala").write_text("new JVM implementation")
        self.commit(self.root)
        self.plan.update(sourceSha=self.git(self.root, "rev-parse", "HEAD"), version="2.0.0")
        self.plan["jvm"][0]["spark"] = "4.2.0"
        self.plan["pins"]["milvus-proto"] = "b" * 40
        self.plan["native"][0]["build_jobs"] = 8
        self.assertEqual(before, self.fingerprint())

    def test_all_native_entry_scripts_and_tracked_build_inputs_invalidate_key(self):
        before = self.fingerprint()
        for name in before["inputs"]["files"]:
            with self.subTest(input=name):
                path = self.root / name
                original = path.read_bytes()
                path.write_bytes(original + b"modified\n")
                changed = self.fingerprint()
                self.assertNotEqual(before["key"], changed["key"])
                self.assertEqual(before["dependencyKeyPrefix"], changed["dependencyKeyPrefix"])
                path.write_bytes(original)
        self.assertIn("scripts/check-native-abi.py", before["inputs"]["files"])
        self.assertIn("scripts/ci/prepare_native.sh", before["inputs"]["files"])

    def test_new_tracked_native_input_invalidates_but_build_noise_does_not(self):
        before = self.fingerprint()
        generated = self.root / "native/build/untracked.o"
        generated.write_bytes(b"compiler output")
        self.assertEqual(before, self.fingerprint())
        added = self.root / "native/build/new-build-input.cmake"
        added.write_text("new build logic")
        self.git(self.root, "add", str(added))
        self.assertNotEqual(before["key"], self.fingerprint()["key"])

    def test_engine_pin_changes_invalidate_key(self):
        before = self.fingerprint()
        engine = self.root / "knowhere"
        (engine / "engine.cc").write_text("int upgraded_engine;\n")
        self.commit(engine)
        self.commit(self.root)
        self.plan["pins"]["knowhere"] = self.git(engine, "rev-parse", "HEAD")
        self.assertNotEqual(before["key"], self.fingerprint()["key"])

    def test_plan_index_and_worktree_pins_must_agree(self):
        self.plan["pins"]["knowhere"] = "f" * 40
        with self.assertRaisesRegex(ValueError, "recorded gitlink"):
            self.fingerprint()
        self.plan["pins"]["knowhere"] = self.git(self.root / "knowhere", "rev-parse", "HEAD")
        self.git(self.root, "update-index", "--cacheinfo", "160000," + "f" * 40 + ",knowhere")
        with self.assertRaisesRegex(ValueError, "recorded gitlink"):
            self.fingerprint()
        self.git(self.root, "reset", "-q", "HEAD", "--", "knowhere")
        (self.root / "knowhere/engine.cc").write_text("int other_revision;")
        self.commit(self.root / "knowhere")
        with self.assertRaisesRegex(ValueError, "checkout differs"):
            self.fingerprint()

    def test_dirty_or_untracked_engine_sources_cannot_reuse_a_pin(self):
        path = self.root / "milvus-storage/engine.cc"
        original = path.read_text()
        path.write_text("locally modified source")
        with self.assertRaisesRegex(ValueError, "must be clean"):
            self.fingerprint()
        path.write_text(original)
        (path.parent / "new-source.cc").write_text("untracked source")
        with self.assertRaisesRegex(ValueError, "must be clean"):
            self.fingerprint()

    def test_toolchain_versions_and_declared_compiler_target_isolate_dependency_cache(self):
        before = self.fingerprint()
        for name in self.identity["tools"]:
            with self.subTest(tool=name):
                original = self.identity["tools"][name]
                self.identity["tools"][name] += " changed"
                after = self.fingerprint()
                self.assertNotEqual(before["key"], after["key"])
                self.assertNotEqual(before["dependencyKeyPrefix"], after["dependencyKeyPrefix"])
                self.identity["tools"][name] = original

    def test_random_runner_cpu_does_not_change_bundle_or_dependency_identity(self):
        java = self.root / "jdk/bin/java"
        java.parent.mkdir(parents=True)
        java.touch()
        for platform, machine, runner in (("linux-x86_64", "x86_64", "ubuntu-22.04"),
                                          ("linux-aarch64", "aarch64", "ubuntu-22.04-arm")):
            self.plan["native"][0].update(platform=platform, runner=runner)
            identities = []
            for host_cpu in ("icelake-client", "znver3", "sapphirerapids"):
                def probe(arguments):
                    if any(argument in ("-march=native", "-mcpu=native") for argument in arguments):
                        return "compiler feature choices for " + host_cpu
                    if arguments == ["getconf", "GNU_LIBC_VERSION"]:
                        return "glibc 2.35"
                    return "same prepared tool identity"

                with mock.patch.dict(os.environ, {"JAVA_HOME": str(java.parent.parent)}, clear=True), \
                        mock.patch.object(cache.host_platform, "system", return_value="Linux"), \
                        mock.patch.object(cache.host_platform, "machine", return_value=machine), \
                        mock.patch.object(cache, "os_release", return_value=self.identity["os"]), \
                        mock.patch.object(cache, "command", side_effect=probe):
                    identity = cache.toolchain_identity(platform)
                identities.append(cache.fingerprint(self.root, self.plan, platform, identity))
            with self.subTest(platform=platform):
                self.assertEqual(len({value["key"] for value in identities}), 1)
                self.assertEqual(len({value["dependencyKeyPrefix"] for value in identities}), 1)
                self.assertNotIn("compilerNativeTarget", identities[0]["inputs"]["toolchain"]["tools"])

    def test_declared_profile_changes_rebuild_bundle_and_reuse_package_id_aware_dependencies(self):
        profile = self.root / "native/build/profiles/linux-x86_64"
        profile.write_text("openblas/*:target=NEHALEM\nopenblas/*:NUM_THREADS=64\n")
        before = self.fingerprint()
        profile.write_text("openblas/*:target=HASWELL\nopenblas/*:NUM_THREADS=32\n")
        after = self.fingerprint()
        self.assertNotEqual(before["key"], after["key"])
        self.assertEqual(before["dependencyKeyPrefix"], after["dependencyKeyPrefix"])

    def test_abi_baseline_changes_isolate_cache_and_other_os_is_rejected(self):
        before = self.fingerprint()
        self.plan["native"][0]["glibcxx"] = "3.4.31"
        after = self.fingerprint()
        self.assertNotEqual(before["key"], after["key"])
        self.assertNotEqual(before["dependencyKeyPrefix"], after["dependencyKeyPrefix"])
        self.identity["os"]["versionId"] = "24.04"
        with self.assertRaisesRegex(ValueError, "build baseline"):
            self.fingerprint()

    def test_other_platform_and_runner_use_separate_keys(self):
        before = self.fingerprint()
        row = self.plan["native"][0]
        row.update(platform="linux-aarch64", runner="ubuntu-22.04-arm")
        after = cache.fingerprint(self.root, self.plan, "linux-aarch64", self.identity)
        self.assertNotEqual(before["key"], after["key"])
        self.assertNotEqual(before["dependencyKeyPrefix"], after["dependencyKeyPrefix"])

    def test_no_absolute_checkout_paths_or_release_identity_enter_fingerprint(self):
        serialized = json.dumps(self.fingerprint())
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn(self.plan["version"], serialized)
        self.assertNotIn(self.plan["sourceSha"], serialized)

    def test_toolchain_probes_use_the_prepared_java_and_reject_environment_overrides(self):
        java = self.root / "jdk/bin/java"
        java.parent.mkdir(parents=True)
        java.touch()
        with mock.patch.dict(os.environ, {"JAVA_HOME": str(java.parent.parent)}, clear=True), \
                mock.patch.object(cache.host_platform, "system", return_value="Linux"), \
                mock.patch.object(cache.host_platform, "machine", return_value="aarch64"), \
                mock.patch.object(cache, "os_release", return_value=self.identity["os"]), \
                mock.patch.object(cache, "command", return_value="tool identity") as run:
            tools = cache.toolchain_identity("linux-aarch64")["tools"]
            self.assertIn("rustc", tools)
            self.assertIn("systemPackages", tools)
            self.assertIn(mock.call([str(java), "-version"]), run.call_args_list)
            self.assertIn(mock.call(["gcc-12", "-dumpmachine"]), run.call_args_list)
            self.assertNotIn("compilerNativeTarget", tools)
            os.environ["RUSTFLAGS"] = "-C target-cpu=native"
            with self.assertRaisesRegex(ValueError, "environment overrides"):
                cache.toolchain_identity("linux-aarch64")

    def test_macos_fingerprint_holds_the_deployment_target_and_its_own_baseline(self):
        self.plan["native"].append({"platform": "darwin-aarch64", "runner": "macos-15", "build_jobs": 3,
                                    "libc": "libc++", "macos": "14.0"})
        (self.root / "native/build/profiles/darwin-aarch64").write_text("profile\n")
        self.commit(self.root)
        identity = {"os": {"id": "macos", "versionId": "15.7.9"}, "libcxx": "system",
                    "tools": {"clang": "Apple clang version 17.0.0 (clang-1700.6.4.2)",
                              "developerDir": "/Applications/Xcode_16.4.app/Contents/Developer",
                              "sdk": "15.5", "cmake": "cmake version 3.31.10", "conan": "Conan version 2.25.1",
                              "libomp": "libomp 21.1.2", "deploymentTarget": "14.0"}}
        result = cache.fingerprint(self.root, self.plan, "darwin-aarch64", identity)
        self.assertTrue(result["key"].startswith("native-bundle-v2-darwin-aarch64-"))
        self.assertEqual(result["inputs"]["baseline"], {"libc": "libc++", "macos": "14.0"})
        self.assertNotEqual(result["key"], self.fingerprint()["key"])
        # Another Xcode or deployment target is another bundle.
        other = copy.deepcopy(identity)
        other["tools"]["clang"] = "Apple clang version 16.0.0 (clang-1600.0.26.3)"
        self.assertNotEqual(result["key"], cache.fingerprint(self.root, self.plan, "darwin-aarch64", other)["key"])
        wrong = copy.deepcopy(identity)
        wrong["tools"]["deploymentTarget"] = "15.0"
        with self.assertRaisesRegex(ValueError, "macOS 14.0 build baseline"):
            cache.fingerprint(self.root, self.plan, "darwin-aarch64", wrong)
        # A Linux identity cannot fingerprint the macOS row, and the macOS row
        # cannot claim the Linux platform.
        with self.assertRaisesRegex(ValueError, "macOS 14.0 build baseline"):
            cache.fingerprint(self.root, self.plan, "darwin-aarch64", self.identity)
        self.assertEqual(cache.PLATFORMS["darwin-aarch64"], ("Darwin", "arm64"))

    def test_macos_toolchain_identity_requires_the_release_deployment_target(self):
        with mock.patch.object(cache.host_platform, "system", return_value="Darwin"), \
                mock.patch.object(cache.host_platform, "machine", return_value="arm64"), \
                mock.patch.dict(os.environ, {"JAVA_HOME": str(self.root), "MACOSX_DEPLOYMENT_TARGET": "15.0"},
                                clear=True), \
                mock.patch.object(cache, "command", side_effect=AssertionError("no probe before the target check")):
            (self.root / "bin").mkdir(exist_ok=True)
            (self.root / "bin/java").write_text("")
            with self.assertRaisesRegex(ValueError, "MACOSX_DEPLOYMENT_TARGET must be 14.0"):
                cache.toolchain_identity("darwin-aarch64")

    def test_cli_emits_fingerprint_and_github_outputs(self):
        plan = self.root / "plan.json"
        plan.write_text(json.dumps(self.plan))
        output = self.root / "output/cache-inputs.json"
        github = self.root / "github-output"
        arguments = ["native-cache-key.py", "--plan", str(plan), "--platform", "linux-x86_64",
                     "--output", str(output), "--github-output", str(github)]
        with mock.patch.object(cache, "ROOT", self.root), \
                mock.patch.object(cache, "toolchain_identity", return_value=copy.deepcopy(self.identity)), \
                mock.patch.object(cache.sys, "argv", arguments), mock.patch("builtins.print"):
            cache.main()
        report = json.loads(output.read_text())
        self.assertEqual(report["inputs"]["cacheFormat"], 2)
        self.assertTrue(report["key"].startswith("native-bundle-v2-"))
        self.assertTrue(report["dependencyKeyPrefix"].startswith("native-deps-v2-"))
        outputs = dict(line.split("=", 1) for line in github.read_text().splitlines())
        self.assertEqual(outputs["key"], report["key"])
        self.assertEqual(outputs["key_digest"], report["keyDigest"])
        self.assertEqual(outputs["dependency_key_prefix"], report["dependencyKeyPrefix"])
        self.assertEqual(outputs["toolchain_digest"], report["toolchainDigest"])


if __name__ == "__main__":
    unittest.main()
