"""Consumer gates reject missing, substituted and excess native artifacts."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import tarfile
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
        path = self.ivy / "jars" / f"com.zilliz_{artifact}-3.0.0.jar"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("META-INF/identity", artifact)
            if platform:
                archive.writestr(f"native/milvus/2/{platform}/manifest.properties", "format.version=2")
        return {"artifactId": artifact, "extension": "jar", "classifier": None,
                "sha256": consumer.digest(path), "path": path.name}

    def manifest(self, line="4.0", binary="2.13"):
        return {
            "plan": {"native": [{"platform": "linux-x86_64"}, {"platform": "linux-aarch64"},
                                {"platform": "darwin-aarch64"}]},
            "artifacts": [
                self.jar(f"spark-milvus-{line}_{binary}"),
                self.jar(f"spark-milvus-{line}-linux-x86_64_{binary}"),
                self.jar(f"spark-milvus-{line}-jvm_{binary}"),
                self.jar("spark-milvus-native-linux-x86_64", "linux-x86_64"),
                self.jar("spark-milvus-native-linux-aarch64", "linux-aarch64"),
                self.jar("spark-milvus-native-darwin-aarch64", "darwin-aarch64"),
            ],
        }

    def keep(self, expected):
        for path in (self.ivy / "jars").glob("*.jar"):
            if not any(path.name == f"com.zilliz_{name}-3.0.0.jar" for name in expected):
                path.unlink()

    def test_every_release_native_platform_has_a_consumer_host(self):
        spec = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[1] / "release.py")
        release = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(release)
        self.assertTrue(set(release.NATIVE_PLATFORMS) <= set(consumer.HOSTS))

    def test_jvm_launcher_preloads_libjsig_into_the_jvm_alone(self):
        java_home = self.root / "jdk"
        (java_home / "bin").mkdir(parents=True)
        signal = java_home / "lib/libjsig.dylib"
        launcher = consumer.jvm_launcher(self.root, java_home, signal) / "bin/java"
        self.assertTrue(os.access(launcher, os.X_OK))
        text = launcher.read_text()
        # Not a system shell: the driver's children inherit the inserted library.
        self.assertTrue(text.startswith("#!" + consumer.sys.executable + "\n"))
        self.assertIn("os.environ['DYLD_INSERT_LIBRARIES'] = " + repr(str(signal)), text)
        self.assertIn("os.execv(" + repr(str(java_home / "bin/java")), text)

    def test_universal_and_platform_have_distinct_native_closures(self):
        manifest = self.manifest()
        _, universal = consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "universal")
        self.assertEqual(len(universal), 5)
        _, platform = consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "platform")
        self.assertEqual(len(platform), 3)
        self.keep(platform)
        self.assertEqual(len(consumer.check_resolved(self.ivy, platform)), 3)
        # The universal entry carries every platform bundle of the plan, macOS included.
        self.manifest()
        self.keep(universal)
        self.assertEqual(len(consumer.check_resolved(self.ivy, universal)), 5)

    def test_substituted_candidate_jar_fails(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "platform")
        self.keep(expected)
        target = self.ivy / "jars/com.zilliz_spark-milvus-4.0-jvm_2.13-3.0.0.jar"
        with zipfile.ZipFile(target, "a") as archive:
            archive.writestr("unverified-production.class", b"different")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_platform_entry_must_not_pull_other_architecture(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "platform")
        self.keep(expected)
        self.jar("spark-milvus-native-linux-aarch64", "linux-aarch64")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_missing_matrix_platform_fails_before_resolution(self):
        manifest = self.manifest()
        manifest["artifacts"].pop()
        with self.assertRaisesRegex(ValueError, "missing artifacts"):
            consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "universal")

    def test_native_metadata_must_match_the_resolved_coordinate(self):
        manifest = self.manifest()
        manifest["artifacts"][3] = self.jar("spark-milvus-native-linux-x86_64", "linux-aarch64")
        _, expected = consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "platform")
        self.keep(expected)
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)

    def test_other_groups_cannot_introduce_an_extra_native_platform(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "2.13", "linux-x86_64", "platform")
        self.keep(expected)
        with zipfile.ZipFile(self.ivy / "jars/other.group_bad.jar", "w") as archive:
            archive.writestr("native/milvus/2/linux-aarch64/manifest.properties", "format.version=2")
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)

    def test_every_release_combination_resolves_both_entry_types(self):
        for line, binary in (("3.5", "2.12"), ("3.5", "2.13"), ("4.0", "2.13"),
                             ("4.1", "2.13"), ("4.2", "2.13")):
            for kind in ("universal", "platform"):
                with self.subTest(line=line, binary=binary, entry=kind):
                    manifest = self.manifest(line, binary)
                    entry, expected = consumer.expected_artifacts(
                        manifest, line, binary, "linux-x86_64", kind)
                    suffix = "" if kind == "universal" else "-linux-x86_64"
                    self.assertEqual(entry, f"spark-milvus-{line}{suffix}_{binary}")
                    self.assertIn(f"spark-milvus-{line}-jvm_{binary}", expected)
                    self.keep(expected)
                    consumer.check_resolved(self.ivy, expected)

    def test_spark_35_scala_binary_variants_are_not_interchangeable(self):
        manifest = self.manifest("3.5", "2.12")
        with self.assertRaisesRegex(ValueError, "missing artifacts"):
            consumer.expected_artifacts(manifest, "3.5", "2.13", "linux-x86_64", "universal")
        with self.assertRaisesRegex(ValueError, "Unsupported consumer entry"):
            consumer.expected_artifacts(manifest, "3.5", "2.12", "linux-x86_64", "thin")

    def test_release_row_requires_an_unambiguous_matching_scala_version(self):
        rows = [{"line": "3.5", "scala": scala, "scalaBinary": binary}
                for scala, binary in (("2.12.20", "2.12"), ("2.13.16", "2.13"))]
        manifest = {"plan": {"jvm": rows}}
        self.assertEqual(consumer.consumer_row(manifest, "3.5", "2.12.20"), rows[0])
        self.assertEqual(consumer.consumer_row(manifest, "3.5", "2.13.16"), rows[1])
        for line, scala in (("4.0", "2.13.16"), ("3.5", "2.13.15")):
            with self.assertRaisesRegex(ValueError, "exactly one"):
                consumer.consumer_row(manifest, line, scala)
        rows.append(dict(rows[0]))
        with self.assertRaisesRegex(ValueError, "exactly one"):
            consumer.consumer_row(manifest, "3.5", "2.12.20")
        rows.pop()
        rows[0]["scalaBinary"] = "2.13"
        with self.assertRaisesRegex(ValueError, "binary version differ"):
            consumer.consumer_row(manifest, "3.5", "2.12.20")

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
        row = {"scala": "2.13.16", "scalaBinary": "2.13", "spark": "4.0.1"}
        with mock.patch.object(consumer, "fixture_resources", return_value={}), \
             mock.patch.object(consumer, "run") as run, \
             mock.patch.object(consumer, "verify_fixture_jar"):
            consumer.compile_fixture(self.root, repository, "3.0.0", row,
                                     "spark-milvus-4.0_2.13", {})
        build = (self.root / "fixture/build.sbt").read_text()
        self.assertEqual((self.root / "fixture/project/build.properties").read_text(),
                         (consumer.ROOT / "project/build.properties").read_text())
        resolvers = build.split("externalResolvers := Seq(\n", 1)[1].split(")\n", 1)[0]
        self.assertEqual(resolvers,
                         '  "candidate" at "file:///candidate%20with%20spaces/repository", '
                         '"central" at "https://repo.maven.apache.org/maven2"')
        self.assertNotIn("Resolver.", build)
        self.assertNotIn("repo1.maven.org", build)
        self.assertEqual(run.call_args.kwargs["env"]["COURSIER_CACHE"],
                         str(self.root / "compile-coursier"))

    def test_scala212_fixture_uses_the_selected_compiler_and_output(self):
        row = {"scala": "2.12.20", "scalaBinary": "2.12", "spark": "3.5.5"}
        with mock.patch.object(consumer, "fixture_resources", return_value={}), \
             mock.patch.object(consumer, "run"), \
             mock.patch.object(consumer, "verify_fixture_jar") as verify:
            jar = consumer.compile_fixture(self.root, "file:///candidate", "3.0.1", row,
                                           "spark-milvus-3.5_2.12", {})
        self.assertEqual(jar, self.root / "fixture/target/scala-2.12/smoke.jar")
        self.assertEqual(verify.call_args.args[0], jar)
        build = (self.root / "fixture/build.sbt").read_text()
        self.assertIn('scalaVersion := "2.12.20"', build)
        self.assertIn('"org.apache.spark" %% "spark-sql" % "3.5.5" % "provided"', build)
        self.assertIn('"com.zilliz" % "spark-milvus-3.5_2.12" % "3.0.1"', build)


class SparkInstallationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def distribution(self, *, binary="2.13", spark="3.5.5"):
        home = self.root / "distribution"
        (home / "jars").mkdir(parents=True, exist_ok=True)
        (home / "bin").mkdir(exist_ok=True)
        (home / "bin/spark-submit").touch()
        for jar in (f"scala-library-{binary}.16.jar", f"spark-core_{binary}-{spark}.jar",
                    f"spark-sql_{binary}-{spark}.jar"):
            (home / "jars" / jar).touch()
        return home

    def test_runtime_rejects_wrong_scala_binary_and_spark_version(self):
        home = self.distribution(binary="2.12")
        row = {"spark": "3.5.5", "scalaBinary": "2.13"}
        with self.assertRaisesRegex(ValueError, "wrong Scala binary"):
            consumer.verify_spark_installation(home, row)
        row["scalaBinary"] = "2.12"
        consumer.verify_spark_installation(home, row)
        row["spark"] = "3.5.6"
        with self.assertRaisesRegex(ValueError, "selected runtime"):
            consumer.verify_spark_installation(home, row)
        row["spark"] = "3.5.5"
        (home / "jars/scala-library-2.13.16.jar").touch()
        with self.assertRaisesRegex(ValueError, "wrong Scala binary"):
            consumer.verify_spark_installation(home, row)

    def apache_archive(self):
        name = "spark-3.5.5-bin-hadoop3-scala2.13"
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            for path in ("bin/spark-submit", "jars/scala-library-2.13.8.jar",
                         "jars/spark-core_2.13-3.5.5.jar", "jars/spark-sql_2.13-3.5.5.jar"):
                item = tarfile.TarInfo(name + "/" + path)
                archive.addfile(item, io.BytesIO())
        return buffer.getvalue()

    def test_scala213_spark35_downloads_and_verifies_official_distribution(self):
        archive = self.apache_archive()
        checksum = hashlib.sha512(archive).hexdigest()
        row = {"line": "3.5", "spark": "3.5.5", "scalaBinary": "2.13"}
        with mock.patch.object(consumer.urllib.request, "urlopen", side_effect=[
                io.BytesIO((checksum + "  distribution.tgz\n").encode()), io.BytesIO(archive)]) as fetch, \
             mock.patch.object(consumer, "run") as run:
            home, python = consumer.install_spark(self.root, row, {})
        self.assertEqual(home, self.root / "spark-3.5.5-bin-hadoop3-scala2.13")
        self.assertEqual(python, consumer.sys.executable)
        self.assertTrue((home / "bin/spark-submit").is_file())
        url = "https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3-scala2.13.tgz"
        self.assertEqual(fetch.call_args_list, [mock.call(url + ".sha512", timeout=60),
                                                mock.call(url, timeout=60)])
        run.assert_not_called()

    def test_corrupt_spark_archive_is_never_extracted(self):
        row = {"line": "3.5", "spark": "3.5.5", "scalaBinary": "2.13"}
        with mock.patch.object(consumer.urllib.request, "urlopen", side_effect=[
                io.BytesIO(b"0" * 128), io.BytesIO(self.apache_archive())]), \
             mock.patch.object(consumer.tarfile, "open") as extract, \
             self.assertRaisesRegex(ValueError, "checksum mismatch"):
            consumer.install_spark(self.root, row, {})
        extract.assert_not_called()

    def test_other_profiles_use_pyspark_but_still_verify_runtime_jars(self):
        for line, spark, binary in (("3.5", "3.5.5", "2.12"), ("4.0", "4.0.1", "2.13"),
                                    ("4.1", "4.1.1", "2.13"), ("4.2", "4.2.0", "2.13")):
            with self.subTest(line=line):
                home = self.distribution(binary=binary, spark=spark)
                row = {"line": line, "spark": spark, "scalaBinary": binary}
                with mock.patch.object(consumer, "run") as run, \
                     mock.patch.object(consumer.subprocess, "check_output", return_value=str(home)), \
                     mock.patch.object(consumer.urllib.request, "urlopen") as fetch:
                    actual, python = consumer.install_spark(self.root, row, {})
                self.assertEqual(actual, home)
                self.assertEqual(python, str(self.root / "python/bin/python"))
                self.assertIn("pyspark==" + spark, run.call_args.args[0])
                fetch.assert_not_called()
                for jar in (home / "jars").iterdir():
                    jar.unlink()

    def test_java_version_comes_from_the_selected_jdk(self):
        home = self.root / "java17"
        with mock.patch.object(consumer.subprocess, "check_output", return_value=(
                "Property settings:\n    java.specification.version = 17\n")) as run:
            self.assertEqual(consumer.java_major(home, {}), "17")
        self.assertEqual(run.call_args.args[0][0], str(home / "bin/java"))
        with mock.patch.object(consumer.subprocess, "check_output", return_value="unknown"), \
             self.assertRaisesRegex(ValueError, "Cannot determine Java version"):
            consumer.java_major(home, {})


class ConsumerDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / "persistent results"
        self.candidate = self.root / "candidate"
        self.candidate.mkdir()
        (self.candidate / "release-manifest.json").write_text(json.dumps({
            "version": "3.0.0", "plan": {"jvm": [{"line": "4.0", "spark": "4.0.1",
                "scala": "2.13.16", "scalaBinary": "2.13", "java": "21", "buildJava": "21"}]},
            "artifacts": [],
        }))
        java_home = self.root / "java"
        (java_home / "lib").mkdir(parents=True)
        (java_home / "lib/libjsig.so").touch()
        self.environment = mock.patch.dict(os.environ, {"JAVA_HOME": str(java_home)})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def simulate(self, *, fail=False, line="4.0", scala="2.13.16", runtime_java=None):
        commands = []
        spark_home = None

        def spark_installation(*args, **kwargs):
            nonlocal spark_home
            # This is inside smoke's actual TemporaryDirectory, so assertions
            # after smoke returns also prove diagnostics survive its cleanup.
            spark_home = args[0] / "spark"
            spark_home.mkdir()
            return spark_home, consumer.sys.executable

        def run(command, *, cwd, env, log=None):
            if log is None:
                return
            self.assertEqual(env["JAVA_HOME"], str(runtime_java or self.root / "java"))
            self.assertEqual(env["LD_PRELOAD"], str(Path(env["JAVA_HOME"]) / "lib/libjsig.so"))
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
             mock.patch.object(consumer, "install_spark", side_effect=spark_installation), \
             mock.patch.object(consumer, "compile_fixture", return_value=self.root / "smoke.jar") as compile_fixture, \
             mock.patch.object(consumer, "fixture_resources", return_value={"read-fixtures/test": "a" * 64}), \
             mock.patch.object(consumer, "expected_artifacts", side_effect=lambda *args: (args[-1], {})), \
             mock.patch.object(consumer, "check_resolved", return_value={}), \
             mock.patch.object(consumer, "java_major", side_effect=lambda home, env: "17" if home == runtime_java else "21"), \
             mock.patch.object(consumer.sys, "platform", "linux"), \
             mock.patch.object(consumer.os, "uname", return_value=mock.Mock(machine="x86_64")):
            if fail:
                with self.assertRaisesRegex(RuntimeError, "^original consumer failure$"):
                    consumer.smoke(self.candidate, line, scala, "linux-x86_64", self.output)
            else:
                consumer.smoke(self.candidate, line, scala, "linux-x86_64", self.output)
            for call in compile_fixture.call_args_list:
                self.assertEqual(call.args[-1]["JAVA_HOME"], str(self.root / "java"))
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

    def test_spark_35_runs_both_entries_with_java17_after_compiling_on_java21(self):
        runtime = self.root / "runtime-java17"
        (runtime / "lib").mkdir(parents=True)
        (runtime / "lib/libjsig.so").touch()
        for scala, binary in (("2.12.20", "2.12"), ("2.13.16", "2.13")):
            with self.subTest(scala=scala):
                (self.candidate / "release-manifest.json").write_text(json.dumps({
                    "version": "3.0.0", "plan": {"jvm": [{"line": "3.5", "spark": "3.5.5",
                        "scala": scala, "scalaBinary": binary, "java": "17", "buildJava": "21"}]},
                    "artifacts": [],
                }))
                with mock.patch.dict(os.environ, {"CONSUMER_JAVA_HOME": str(runtime)}):
                    commands = self.simulate(line="3.5", scala=scala, runtime_java=runtime)
                self.assertEqual(len(commands), 2)
                self.assertNotIn("--jars", commands[0] + commands[1])
                report = json.loads((self.output / "consumer-result.json").read_text())
                self.assertEqual((report["line"], report["scala"]), ("3.5", scala))
                self.assertEqual((report["java"], report["buildJava"]), ("17", "21"))
                self.assertEqual([entry["entry"] for entry in report["entries"]],
                                 ["universal", "platform"])

    def test_wrong_jdk_is_rejected_before_compilation_or_runtime_installation(self):
        with mock.patch.object(consumer, "fixture_resources", return_value={}), \
             mock.patch.object(consumer, "expected_artifacts", return_value=("entry", {})), \
             mock.patch.object(consumer, "java_major", return_value="17"), \
             mock.patch.object(consumer, "install_spark") as install, \
             mock.patch.object(consumer, "compile_fixture") as compile_fixture, \
             mock.patch.object(consumer.sys, "platform", "linux"), \
             mock.patch.object(consumer.os, "uname", return_value=mock.Mock(machine="x86_64")), \
             self.assertRaisesRegex(ValueError, "build requires Java 21"):
            consumer.smoke(self.candidate, "4.0", "2.13.16", "linux-x86_64", self.output)
        install.assert_not_called()
        compile_fixture.assert_not_called()

    def test_required_check_evidence_rejects_missing_duplicate_and_canceled_work(self):
        lines = [f"MILVUS_CHECK\t{name}\tpassed" for name in consumer.REQUIRED_CHECKS]
        for incomplete in (lines[:-1], lines + lines[:1],
                           [line.replace("passed", "canceled") for line in lines]):
            with self.subTest(lines=incomplete), self.assertRaises(RuntimeError):
                consumer.completed_checks("\n".join(incomplete + ["ALL PASS"]))
        with self.assertRaisesRegex(RuntimeError, "did not finish"):
            consumer.completed_checks("\n".join(lines))

    def test_read_only_success_cannot_satisfy_column_replacement_acceptance(self):
        read_checks = ("independent-executor", "v3-snapshot", "v3-advanced",
                       "v3-empty", "repeated-actions")
        read_only = "\n".join(f"MILVUS_CHECK\t{name}\tpassed" for name in read_checks)
        with self.assertRaisesRegex(RuntimeError, "omitted"):
            consumer.completed_checks(read_only + "\nALL PASS\n")
        for missing in ("physical-read", "column-replacement-receipt",
                        "column-replacement-readback", "source-snapshot-unchanged"):
            incomplete = "\n".join(f"MILVUS_CHECK\t{name}\tpassed"
                                   for name in consumer.REQUIRED_CHECKS if name != missing)
            with self.subTest(missing=missing), self.assertRaisesRegex(RuntimeError, "omitted"):
                consumer.completed_checks(incomplete + "\nALL PASS\n")


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

    def test_public_consumer_only_compiles_the_artifact_entry_and_integrity_helper(self):
        self.assertEqual([Path(path).stem for path in consumer.FIXTURES],
                         ["ArtifactConsumerSmoke", "ReadFixtures"])
        self.assertTrue(consumer.MAIN.endswith(".ArtifactConsumerSmoke"))


if __name__ == "__main__":
    unittest.main()
