"""Consumer gates reject missing, substituted and excess native artifacts."""

import importlib.util
import json
import os
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest import mock
import zipfile


SPEC = importlib.util.spec_from_file_location("consumer_smoke", Path(__file__).with_name("consumer_smoke.py"))
consumer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(consumer)


class ConsumerTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.ivy = self.root / "ivy"
        (self.ivy / "jars").mkdir(parents=True)

    def jar(self, artifact, platform=None):
        path = self.ivy / "jars" / f"com.zilliz_{artifact}-2.0.0.jar"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("META-INF/identity", artifact)
            if platform:
                archive.writestr(f"native/milvus/1/{platform}/manifest.properties", "format.version=1")
        return {"artifactId": artifact, "extension": "jar", "classifier": None,
                "sha256": consumer.digest(path), "path": path.name}

    def manifest(self):
        return {
            "plan": {"native": [{"platform": "linux-x86_64"}, {"platform": "linux-aarch64"}]},
            "artifacts": [
                self.jar("spark-milvus-4.0_2.13"),
                self.jar("spark-milvus-4.0-linux-x86_64_2.13"),
                self.jar("spark-milvus-4.0-jvm_2.13"),
                self.jar("spark-milvus-native-linux-x86_64", "linux-x86_64"),
                self.jar("spark-milvus-native-linux-aarch64", "linux-aarch64"),
            ],
        }

    def keep(self, expected):
        for path in (self.ivy / "jars").glob("*.jar"):
            if not any(path.name == f"com.zilliz_{name}-2.0.0.jar" for name in expected):
                path.unlink()

    def test_universal_and_platform_have_distinct_native_closures(self):
        manifest = self.manifest()
        _, universal = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "universal")
        self.assertEqual(len(universal), 4)
        _, platform = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.assertEqual(len(platform), 3)
        self.keep(platform)
        self.assertEqual(len(consumer.check_resolved(self.ivy, platform)), 3)

    def test_substituted_candidate_jar_fails(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        target = self.ivy / "jars/com.zilliz_spark-milvus-4.0-jvm_2.13-2.0.0.jar"
        with zipfile.ZipFile(target, "a") as archive:
            archive.writestr("unverified-production.class", b"different")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_platform_entry_must_not_pull_other_architecture(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        self.jar("spark-milvus-native-linux-aarch64", "linux-aarch64")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_missing_matrix_platform_fails_before_resolution(self):
        manifest = self.manifest()
        manifest["artifacts"].pop()
        with self.assertRaisesRegex(ValueError, "missing artifacts"):
            consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "universal")

    def test_native_metadata_must_match_the_resolved_coordinate(self):
        manifest = self.manifest()
        manifest["artifacts"][3] = self.jar("spark-milvus-native-linux-x86_64", "linux-aarch64")
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)

    def test_other_groups_cannot_introduce_an_extra_native_platform(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        with zipfile.ZipFile(self.ivy / "jars/other.group_bad.jar", "w") as archive:
            archive.writestr("native/milvus/1/linux-aarch64/manifest.properties", "format.version=1")
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)

    def thin_manifest(self):
        names = ("native-runtime", "native-vector", "native-storage_2.13",
                 "core_2.13", "4.0-thin_2.13")
        return {"plan": {"distribution": "thin", "native": []},
                "artifacts": [self.jar("spark-milvus-" + name) for name in names]}

    def test_thin_resolves_modules_without_a_platform_or_fat_jar(self):
        manifest = self.thin_manifest()
        entry, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "thin")
        self.assertEqual(entry, "spark-milvus-4.0-thin_2.13")
        self.assertEqual(len(consumer.check_resolved(self.ivy, expected)), 5)
        self.jar("spark-milvus-4.0-jvm_2.13")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_thin_rejects_native_resources_in_transitive_dependencies(self):
        manifest = self.thin_manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "thin")
        with zipfile.ZipFile(self.ivy / "jars/other.group_bad.jar", "w") as archive:
            archive.writestr("native/milvus/1/linux-x86_64/manifest.properties", "format.version=1")
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)

    def test_thin_requires_complete_candidate_and_separate_native(self):
        manifest = self.thin_manifest()
        with self.assertRaisesRegex(ValueError, "needs --native-bundle"):
            consumer.thin_native_bundle(self.root, manifest, "linux-x86_64")
        manifest["artifacts"].pop()
        with self.assertRaisesRegex(ValueError, "missing thin artifacts"):
            consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "thin")
        with self.assertRaisesRegex(ValueError, "only Spark 4.0"):
            consumer.expected_artifacts(manifest, "4.2", "linux-x86_64", "thin")

    def test_ivy_resolvers_do_not_use_maven_local(self):
        path = self.root / "ivysettings.xml"
        consumer.ivy_settings(path, "file:///candidate with spaces/repository")
        settings = consumer.ET.parse(path).getroot()
        resolvers = settings.findall("resolvers/chain/ibiblio")
        self.assertEqual([item.get("root") for item in resolvers],
                         ["file:///candidate with spaces/repository", consumer.CENTRAL])
        self.assertEqual(len(settings.findall("resolvers/chain/*")), 2)

    def test_fixture_compilation_uses_candidate_and_canonical_central_only(self):
        repository = "file:///candidate%20with%20spaces/repository"
        row = {"scala": "2.13.16", "spark": "4.0.1"}
        with mock.patch.object(consumer, "fixture_resources", return_value={}), \
             mock.patch.object(consumer, "run") as run, \
             mock.patch.object(consumer, "verify_fixture_jar"):
            consumer.compile_fixture(self.root, repository, "2.0.0", row,
                                     "spark-milvus-4.0-thin_2.13", {})
        build = (self.root / "fixture/build.sbt").read_text()
        resolvers = build.split("externalResolvers := Seq(\n", 1)[1].split(")\n", 1)[0]
        self.assertEqual(resolvers,
                         '  "candidate" at "file:///candidate%20with%20spaces/repository", '
                         '"central" at "https://repo.maven.apache.org/maven2"')
        self.assertNotIn("Resolver.", build)
        self.assertNotIn("repo1.maven.org", build)
        self.assertEqual(run.call_args.kwargs["env"]["COURSIER_CACHE"],
                         str(self.root / "compile-coursier"))


class ConsumerDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / "persistent results"
        self.candidate = self.root / "candidate"
        self.candidate.mkdir()
        (self.candidate / "release-manifest.json").write_text(json.dumps({
            "version": "2.0.0", "plan": {"jvm": [{"line": "4.0", "spark": "4.0.1"}]},
            "artifacts": [],
        }))
        java_home = self.root / "java"
        (java_home / "lib").mkdir(parents=True)
        (java_home / "lib/libjsig.so").touch()
        self.environment = mock.patch.dict(os.environ, {"JAVA_HOME": str(java_home)})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def simulate(self, *, fail=False):
        commands = []
        spark_home = None

        def spark_installation(*args, **kwargs):
            nonlocal spark_home
            # This is inside smoke's actual TemporaryDirectory, so assertions
            # after smoke returns also prove diagnostics survive its cleanup.
            spark_home = Path(args[0][0]).parents[2] / "spark"
            spark_home.mkdir()
            return str(spark_home) + "\n"

        def run(command, *, cwd, env, log=None):
            if log is None:
                return
            commands.append(command)
            entry = cwd.name
            executor = spark_home / "work" / f"app-{entry}" / "0"
            executor.mkdir(parents=True)
            (executor / "stdout").write_text(entry + " stdout\n")
            (executor / "stderr").write_text(entry + " stderr\n")
            (executor / "core.123").write_bytes(b"do not collect a core dump")
            (executor / "dependency.jar").write_bytes(b"do not collect dependencies")
            (executor / "stdout.1").symlink_to(executor / "core.123")
            (executor / "hs_err_pid123.log").write_text("executor fallback report\n")
            driver_options = shlex.split(command[command.index("--driver-java-options") + 1])
            error_option = next(value for value in driver_options if value.startswith("-XX:ErrorFile="))
            error_path = Path(error_option.removeprefix("-XX:ErrorFile=").replace("%p", "124"))
            error_path.write_text("explicit JVM error report\n")
            (cwd / "hs_err_pid125.log").write_text("driver fallback report\n")
            Path(log).write_text("driver log\n" if fail else "ALL PASS\n" + "".join(
                f"MILVUS_CHECK\t{name}\tpassed\n" for name in consumer.REQUIRED_CHECKS))
            if fail:
                raise RuntimeError("original consumer failure")

        with mock.patch.object(consumer, "run", side_effect=run), \
             mock.patch.object(consumer.subprocess, "check_output", side_effect=spark_installation), \
             mock.patch.object(consumer, "compile_fixture", return_value=self.root / "smoke.jar"), \
             mock.patch.object(consumer, "fixture_resources", return_value={"read-fixtures/test": "a" * 64}), \
             mock.patch.object(consumer, "expected_artifacts", side_effect=lambda *args: (args[-1], {})), \
             mock.patch.object(consumer, "check_resolved", return_value={}), \
             mock.patch.object(consumer.sys, "platform", "linux"), \
             mock.patch.object(consumer.os, "uname", return_value=mock.Mock(machine="x86_64")):
            if fail:
                with self.assertRaisesRegex(RuntimeError, "^original consumer failure$"):
                    consumer.smoke(self.candidate, "4.0", "linux-x86_64", self.output)
            else:
                consumer.smoke(self.candidate, "4.0", "linux-x86_64", self.output)
        self.assertFalse(spark_home.exists(), "temporary Spark installation must be removed")
        return commands

    def assert_diagnostics(self, entry):
        directory = self.output / f"{entry}-diagnostics"
        files = {str(path.relative_to(directory)) for path in directory.rglob("*") if path.is_file()}
        self.assertEqual(files, {
            f"worker/app-{entry}/0/stdout", f"worker/app-{entry}/0/stderr",
            f"worker/app-{entry}/0/hs_err_pid123.log", "jvm/hs_err_pid124.log",
            "driver/hs_err_pid125.log", "diagnostics.json",
        })
        self.assertEqual((directory / f"worker/app-{entry}/0/stderr").read_text(), entry + " stderr\n")

    def test_failure_retains_diagnostics_before_temporary_cleanup_and_still_fails(self):
        commands = self.simulate(fail=True)
        self.assertEqual(len(commands), 1, "failed universal entry must not continue to platform")
        self.assert_diagnostics("universal")
        self.assertEqual((self.output / "universal.log").read_text(), "driver log\n")
        self.assertFalse((self.output / "consumer-result.json").exists())

    def test_both_entries_keep_separate_diagnostics_and_worker_jvm_options(self):
        commands = self.simulate()
        self.assertEqual(len(commands), 2)
        for entry, command in zip(("universal", "platform"), commands):
            self.assert_diagnostics(entry)
            self.assertEqual(command[command.index("--master") + 1], "local-cluster[1,2,1536]")
            driver = shlex.split(command[command.index("--driver-java-options") + 1])
            self.assertIn("-Dmilvus.smoke.master=local-cluster[1,2,1536]", driver)
            executor = next(value for value in command if value.startswith("spark.executor.extraJavaOptions="))
            self.assertEqual(shlex.split(executor.split("=", 1)[1]), driver[1:])
        report = json.loads((self.output / "consumer-result.json").read_text())
        self.assertEqual([entry["entry"] for entry in report["entries"]], ["universal", "platform"])
        self.assertEqual(report["read_fixture_sha256"], {"read-fixtures/test": "a" * 64})
        for entry in report["entries"]:
            self.assertEqual(entry["executed"], len(consumer.REQUIRED_CHECKS))
            self.assertEqual(entry["passed"], entry["executed"])
            self.assertEqual((entry["failed"], entry["canceled"]), (0, 0))

    def test_required_check_evidence_rejects_missing_duplicate_and_canceled_work(self):
        lines = [f"MILVUS_CHECK\t{name}\tpassed" for name in consumer.REQUIRED_CHECKS]
        for incomplete in (lines[:-1], lines + lines[:1],
                           [line.replace("passed", "canceled") for line in lines]):
            with self.subTest(lines=incomplete), self.assertRaises(RuntimeError):
                consumer.completed_checks("\n".join(incomplete + ["ALL PASS"]))
        with self.assertRaisesRegex(RuntimeError, "did not finish"):
            consumer.completed_checks("\n".join(lines))


class ReadFixtureTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.group = self.root / "sample"
        self.group.mkdir()
        self.data = self.group / "segment.parquet"
        self.data.write_bytes(b"fixed independent sample")
        self.manifest = self.group / "SHA256SUMS"
        self.manifest.write_text(consumer.digest(self.data) + "  segment.parquet\n")

    def resources(self):
        return consumer.fixture_resources(self.root, ("sample",))

    def jar(self, *, additional=None, substituted=False):
        path = self.root / "smoke.jar"
        with zipfile.ZipFile(path, "w") as archive:
            for name in consumer.FIXTURE_CLASSES:
                archive.writestr(name + ".class", b"fixture class")
            for name in self.resources():
                relative = name.removeprefix("read-fixtures/")
                archive.writestr(name, b"substituted" if substituted else (self.root / relative).read_bytes())
            if additional:
                archive.writestr(*additional)
        return path

    def test_only_checksummed_data_is_packaged(self):
        (self.group / "README.md").write_text("maintenance instructions, not runtime input")
        resources = self.resources()
        self.assertEqual(set(resources), {
            "read-fixtures/sample/SHA256SUMS", "read-fixtures/sample/segment.parquet"})
        consumer.verify_fixture_jar(self.jar(), resources)

    def test_changed_or_missing_fixed_data_fails(self):
        self.data.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "missing or changed"):
            self.resources()
        self.data.unlink()
        with self.assertRaisesRegex(ValueError, "missing or changed"):
            self.resources()

    def test_unsafe_and_duplicate_checksum_paths_fail(self):
        for name in ("../outside", "/absolute", "sub/../segment.parquet", "./segment.parquet"):
            self.manifest.write_text("a" * 64 + "  " + name + "\n")
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Unsafe fixture path"):
                self.resources()
        entry = consumer.digest(self.data) + "  segment.parquet\n"
        self.manifest.write_text(entry + entry)
        with self.assertRaisesRegex(ValueError, "Duplicate or recursive"):
            self.resources()

    def test_absent_or_empty_fixture_manifest_fails(self):
        self.manifest.write_text("")
        with self.assertRaisesRegex(ValueError, "Empty fixture checksums"):
            self.resources()
        self.manifest.unlink()
        with self.assertRaisesRegex(ValueError, "Missing fixed read fixture"):
            self.resources()

    def test_consumer_jar_rejects_production_classes(self):
        for name in ("com/zilliz/milvus/storage/read/SegmentReader.class",
                     consumer.FIXTURE_CLASSES[0] + "Production.class"):
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, "production classes"):
                consumer.verify_fixture_jar(self.jar(additional=(name, b"production")), self.resources())

    def test_consumer_jar_rejects_changed_resource_bytes(self):
        with self.assertRaisesRegex(RuntimeError, "fixtures differ"):
            consumer.verify_fixture_jar(self.jar(substituted=True), self.resources())

    def test_public_consumer_only_compiles_the_read_entry_and_integrity_helper(self):
        self.assertEqual([Path(path).stem for path in consumer.FIXTURES],
                         ["ReadOnlyConsumerSmoke", "ReadFixtures"])
        self.assertTrue(consumer.MAIN.endswith(".ReadOnlyConsumerSmoke"))


if __name__ == "__main__":
    unittest.main()
