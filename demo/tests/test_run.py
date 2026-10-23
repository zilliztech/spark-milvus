"""Exercise demo setup and actual spark-submit arguments without starting Spark."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / "demo/run.sh"
EXAMPLES = (
    "01_read_snapshot.py", "02_filter_project.py", "03_sql_analytics.py", "04_exact_search.py",
    "05_filtered_search.py", "06_index_search.py", "07_vector_functions.py", "08_partition_selection.py",
)
JAVA_EXAMPLES = (
    "ReadSnapshot", "FilterProject", "SqlAnalytics", "ExactSearch",
    "FilteredSearch", "IndexSearch", "VectorFunctions", "PartitionSelection",
)


class RunExamplesTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="spark-demo-launcher-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.capture = self.root / "arguments"
        self.environment_capture = self.root / "environment"
        self.javac_capture = self.root / "javac-arguments"
        self.jar_capture = self.root / "jar-arguments"
        self.java_capture = self.root / "java-arguments"
        self.python_capture = self.root / "python-arguments"
        self.helper_environment_capture = self.root / "helper-environment"
        self.spark = self.root / "spark"
        self.java = self.root / "jdk"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.executable(self.bin / "uname", '#!/bin/bash\n'
                        'if [[ "$1" == -s ]]; then printf "%s\\n" "$DEMO_TEST_SYSTEM"; '
                        'else printf "%s\\n" "$DEMO_TEST_MACHINE"; fi\n')
        self.executable(self.spark / "bin/spark-submit", '#!/bin/bash\n'
                        'printf "%s\\0" "$@" > "$DEMO_TEST_ARGUMENTS"\n'
                        'printf "%s\\0" "${LD_PRELOAD-}" "${DYLD_INSERT_LIBRARIES-}" '
                        '"${LD_BIND_NOW-}" "${MILVUS_DEMO_ROOT-}" > "$DEMO_TEST_ENVIRONMENT"\n'
                        'exit "${DEMO_TEST_SUBMIT_EXIT:-0}"\n')
        self.executable(self.java / "bin/java", '#!/bin/bash\n'
                        'if [[ "$1" == -version ]]; then\n'
                        '  printf \'openjdk version "%s.0.1"\\n\' "${DEMO_TEST_JAVA_VERSION:-21}" >&2\n'
                        'else\n'
                        '  printf "%s\\0" "$@" > "$DEMO_TEST_JAVA_ARGUMENTS"\n'
                        '  printf "%s\\0" "${MILVUS_URI-}" "${MILVUS_TOKEN-}" > "$DEMO_TEST_HELPER_ENVIRONMENT"\n'
                        '  exit "${DEMO_TEST_JAVA_EXIT:-0}"\n'
                        'fi\n')
        self.fake_python = self.bin / "helper-python"
        self.executable(self.fake_python, '#!/bin/bash\n'
                        'printf "%s\\0" "$@" > "$DEMO_TEST_PYTHON_ARGUMENTS"\n'
                        'printf "%s\\0" "${MILVUS_URI-}" "${MILVUS_TOKEN-}" > "$DEMO_TEST_HELPER_ENVIRONMENT"\n'
                        'exit "${DEMO_TEST_PYTHON_EXIT:-0}"\n')
        self.executable(self.java / "bin/javac", '#!/bin/bash\n'
                        'printf "%s\\0" "$@" > "$DEMO_TEST_JAVAC_ARGUMENTS"\n'
                        'exit "${DEMO_TEST_JAVAC_EXIT:-0}"\n')
        self.executable(self.java / "bin/jar", '#!/bin/bash\n'
                        'printf "%s\\0" "$@" > "$DEMO_TEST_JAR_ARGUMENTS"\n'
                        '[[ "$1" == --create && "$2" == --file ]] || exit 2\n'
                        ': > "$3"\n')
        (self.spark / "jars").mkdir()
        (self.java / "lib").mkdir()
        (self.java / "lib/libjsig.so").touch()
        self.assembly = self.root / "chosen jars" / "connector.jar"
        self.jar(self.assembly)
        self.env = dict(os.environ)
        for name in ("CANDIDATE", "CONNECTOR_JAR", "SPARK_HOME", "JAVA_HOME", "MASTER",
                     "LD_PRELOAD", "LD_BIND_NOW", "DYLD_INSERT_LIBRARIES", "MILVUS_DEMO_ROOT",
                     "MILVUS_URI", "MILVUS_TOKEN"):
            self.env.pop(name, None)
        self.env.update(
            SPARK_HOME=str(self.spark), JAVA_HOME=str(self.java),
            CONNECTOR_JAR=str(self.assembly), PYTHON=sys.executable,
            PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
            DEMO_TEST_SYSTEM="Linux", DEMO_TEST_MACHINE="x86_64",
            DEMO_TEST_ARGUMENTS=str(self.capture),
            DEMO_TEST_ENVIRONMENT=str(self.environment_capture),
            DEMO_TEST_JAVAC_ARGUMENTS=str(self.javac_capture),
            DEMO_TEST_JAR_ARGUMENTS=str(self.jar_capture),
            DEMO_TEST_JAVA_ARGUMENTS=str(self.java_capture),
            DEMO_TEST_PYTHON_ARGUMENTS=str(self.python_capture),
            DEMO_TEST_HELPER_ENVIRONMENT=str(self.helper_environment_capture),
        )

    @staticmethod
    def executable(path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        path.chmod(0o755)

    @staticmethod
    def jar(path, native=True):
        path.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(path, "w") as archive:
            archive.writestr("com/zilliz/spark/connector/extensions/MilvusSparkPlugin.class", b"fixture")
            if native:
                for architecture in ("x86_64", "aarch64"):
                    archive.writestr(f"native/milvus/1/linux-{architecture}/manifest.properties", "")

    def run_demo(self, *arguments, **environment):
        env = dict(self.env)
        for key, value in environment.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        self.capture.unlink(missing_ok=True)
        return subprocess.run(["bash", str(LAUNCHER), *arguments], cwd=self.root,
                              env=env, text=True, capture_output=True)

    def run_python(self, *arguments, **environment):
        return self.run_demo("python", *arguments, **environment)

    def arguments(self):
        return self.capture.read_bytes().decode().split("\0")[:-1]

    def assert_failed(self, result, message):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)
        self.assertFalse(self.capture.exists(), "spark-submit ran after invalid setup")

    def test_menu_needs_no_runtime(self):
        for arguments in ([], ["--help"], ["-h"]):
            with self.subTest(arguments=arguments):
                result = self.run_demo(*arguments, SPARK_HOME=None, JAVA_HOME=None, CONNECTOR_JAR=None,
                                       PYTHON="/does/not/exist")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("CONNECTOR_JAR", result.stdout)
                for example in EXAMPLES:
                    self.assertIn(example, result.stdout)
                for example in JAVA_EXAMPLES:
                    self.assertIn(example, result.stdout)
                self.assertIn("00_snapshot_path.py", result.stdout)
                self.assertIn("SnapshotPath", result.stdout)
                self.assertFalse(self.capture.exists())

    def test_language_and_example_must_be_explicit(self):
        for arguments in (["04_exact_search.py"], ["scala", "ExactSearch"], ["python"], ["java"]):
            with self.subTest(arguments=arguments):
                result = self.run_demo(*arguments)
                self.assert_failed(result, "Choose")

    def test_example_help_needs_only_python(self):
        for example in ("00_snapshot_path.py", *EXAMPLES):
            with self.subTest(example=example):
                result = self.run_python(example, "--help", SPARK_HOME=None, JAVA_HOME=None, CONNECTOR_JAR=None)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout)
                if example == "04_exact_search.py":
                    self.assertIn("--queries", result.stdout)
                self.assertFalse(self.capture.exists())

    def test_every_allowlisted_example_is_selected_explicitly(self):
        for example in EXAMPLES:
            for selection in (example, example.removesuffix(".py")):
                with self.subTest(selection=selection):
                    result = self.run_python(selection)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(self.arguments()[-1], str(ROOT / "demo/python" / example))

    def test_external_or_unknown_scripts_are_rejected(self):
        for example in ("missing.py", "../outside.py", "./04_exact_search.py",
                        str(ROOT / "demo/python/04_exact_search.py"), "04_exact_search.py;echo injected"):
            with self.subTest(example=example):
                self.assert_failed(self.run_python(example), "Unknown Python example")

    def test_assembly_paths_and_application_arguments_preserve_spaces(self):
        forwarded = ["--where", "category = 1", "--explain"]
        result = self.run_python("05_filtered_search.py", *forwarded)
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.arguments()
        self.assertEqual(args[:4], ["--master", "local[2]", "--jars", str(self.assembly)])
        self.assertEqual(args[-len(forwarded) - 1:], [str(ROOT / "demo/python/05_filtered_search.py"), *forwarded])
        self.assertEqual(args[args.index("--py-files") + 1], str(ROOT / "demo/python/common.py"))
        self.assertIn("spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions", args)
        self.assertIn("spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin", args)
        signal = str(self.java / "lib/libjsig.so")
        self.assertIn("spark.executorEnv.LD_PRELOAD=" + signal, args)
        self.assertIn("spark.executorEnv.LD_BIND_NOW=1", args)
        self.assertEqual(self.environment_capture.read_bytes().decode().split("\0")[:-1], [signal, "", "1", ""])

    def test_custom_snapshot_can_use_a_cluster_master(self):
        for options in (["--options", "/private/read options.json"], ["--options=/private/read options.json"]):
            with self.subTest(options=options):
                result = self.run_python("04_exact_search.py", *options, "--queries", "/data/query.json",
                                       MASTER="spark://example.invalid:7077")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.arguments()[:2], ["--master", "spark://example.invalid:7077"])

    def test_default_snapshot_requires_a_local_master(self):
        for master in ("spark://example.invalid:7077", "local-cluster[2,1,1024]", "yarn"):
            with self.subTest(master=master):
                self.assert_failed(self.run_python("01_read_snapshot.py", MASTER=master),
                                   "default snapshot requires MASTER=local")
        for master in ("local", "local[*]", "local[2,3]"):
            with self.subTest(master=master):
                result = self.run_python("01_read_snapshot.py", MASTER=master)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_vector_functions_can_use_a_cluster_without_snapshot_options(self):
        result = self.run_python("07_vector_functions.py", MASTER="spark://example.invalid:7077")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.arguments()[:2], ["--master", "spark://example.invalid:7077"])
        self.assertEqual(self.arguments()[-1], str(ROOT / "demo/python/07_vector_functions.py"))

    def test_exactly_one_connector_source_is_required(self):
        self.assert_failed(self.run_python("01_read_snapshot.py", CONNECTOR_JAR=None), "exactly one")
        self.assert_failed(self.run_python("01_read_snapshot.py", CANDIDATE="/candidate"), "exactly one")

    def test_missing_executables_fail_before_submission(self):
        self.assert_failed(self.run_python("01_read_snapshot.py", SPARK_HOME=str(self.root / "absent")),
                           "spark-submit must be executable")
        self.assert_failed(self.run_python("01_read_snapshot.py", JAVA_HOME=str(self.root / "absent")),
                           "java must be executable")

    def test_wrong_java_and_missing_signal_library_fail_before_submission(self):
        self.assert_failed(self.run_python("01_read_snapshot.py", DEMO_TEST_JAVA_VERSION="17"), "Java 21")
        (self.java / "lib/libjsig.so").unlink()
        self.assert_failed(self.run_python("01_read_snapshot.py"), "Missing Java signal-chaining library")

    def test_invalid_assembly_paths_and_thin_jars_fail_before_submission(self):
        self.assert_failed(self.run_python("01_read_snapshot.py", CONNECTOR_JAR=str(self.root / "missing.jar")),
                           "Demo setup failed")
        for suffix in (",bad", "\nbad", "\rbad"):
            path = self.root / ("connector" + suffix + ".jar")
            self.jar(path)
            with self.subTest(suffix=suffix):
                self.assert_failed(self.run_python("01_read_snapshot.py", CONNECTOR_JAR=str(path)),
                                   "cannot contain commas or newlines")
        self.jar(self.assembly, native=False)
        self.assert_failed(self.run_python("01_read_snapshot.py"), "spark/assembly JAR with native libraries")

    def test_linux_aarch64_assembly_uses_signal_chaining(self):
        result = self.run_python("01_read_snapshot.py", DEMO_TEST_MACHINE="aarch64")
        self.assertEqual(result.returncode, 0, result.stderr)
        signal = str(self.java / "lib/libjsig.so")
        args = self.arguments()
        self.assertIn("spark.executorEnv.LD_PRELOAD=" + signal, args)
        self.assertIn("spark.executorEnv.LD_BIND_NOW=1", args)
        self.assertEqual(self.environment_capture.read_bytes().decode().split("\0")[:-1], [signal, "", "1", ""])

    def test_java_help_needs_no_runtime(self):
        for example in JAVA_EXAMPLES:
            with self.subTest(example=example):
                result = self.run_demo("java", example, "--help", SPARK_HOME=None, JAVA_HOME=None,
                                       CONNECTOR_JAR=None, PYTHON="/does/not/exist")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Usage: ./demo/run.sh java " + example, result.stdout)
                self.assertIn("--explain", result.stdout)
                if example == "VectorFunctions":
                    self.assertNotIn("--options", result.stdout)
                if example == "ExactSearch":
                    self.assertIn("--queries", result.stdout)
                self.assertFalse(self.capture.exists())

    def test_snapshot_helper_help_needs_no_java_or_spark(self):
        result = self.run_demo("java", "SnapshotPath", "--help", SPARK_HOME=None, JAVA_HOME=None,
                               CONNECTOR_JAR=None, PYTHON="/does/not/exist")
        self.assertEqual(result.returncode, 0, result.stderr)
        for option in ("--uri", "--database", "--collection", "--snapshot", "--timeout",
                       "--storage-options", "--output"):
            self.assertIn(option, result.stdout)
        self.assertIn("MILVUS_TOKEN", result.stdout)
        self.assertNotIn("--explain", result.stdout)
        self.assertFalse(self.capture.exists())
        self.assertFalse(self.javac_capture.exists())

    def test_python_snapshot_helper_bypasses_spark_and_connector_setup(self):
        arguments = ["--uri", "http://example.invalid:19530", "--database", "default",
                     "--collection", "documents", "--snapshot", "fixed", "--timeout", "30",
                     "--storage-options", "/private/storage options.json", "--output", "/private/read options.json"]
        for selection in ("00_snapshot_path.py", "00_snapshot_path"):
            with self.subTest(selection=selection):
                result = self.run_python(selection, *arguments, SPARK_HOME=None, JAVA_HOME=None,
                                         CONNECTOR_JAR=None, CANDIDATE=None, MASTER="yarn",
                                         PYTHON=str(self.fake_python), MILVUS_URI="http://environment.invalid",
                                         MILVUS_TOKEN="test-token", DEMO_TEST_PYTHON_EXIT="31")
                self.assertEqual(result.returncode, 31, result.stderr)
                self.assertEqual(self.python_capture.read_bytes().decode().split("\0")[:-1],
                                 [str(ROOT / "demo/python/00_snapshot_path.py"), *arguments])
                self.assertEqual(self.helper_environment_capture.read_bytes().decode().split("\0")[:-1],
                                 ["http://environment.invalid", "test-token"])
                self.assertFalse(self.capture.exists())
                self.assertFalse(self.javac_capture.exists())

    def test_java_snapshot_helper_compiles_only_itself_and_runs_without_spark_or_connector(self):
        (self.spark / "bin/spark-submit").unlink()
        (self.java / "bin/jar").unlink()
        (self.java / "lib/libjsig.so").unlink()
        arguments = ["--collection", "documents", "--snapshot", "fixed", "--output", "/private/read options.json"]
        for exit_code in (0, 37):
            with self.subTest(exit_code=exit_code):
                result = self.run_demo("java", "SnapshotPath", *arguments,
                                       CONNECTOR_JAR=None, CANDIDATE=None, PYTHON="/does/not/exist", MASTER="yarn",
                                       MILVUS_URI="http://example.invalid:19530", MILVUS_TOKEN="test-token",
                                       DEMO_TEST_JAVA_EXIT=str(exit_code))
                self.assertEqual(result.returncode, exit_code, result.stderr)
                compiler = self.javac_capture.read_bytes().decode().split("\0")[:-1]
                self.assertEqual(compiler[:4], ["--release", "17", "-encoding", "UTF-8"])
                self.assertEqual(compiler[compiler.index("-classpath") + 1], str(self.spark / "jars/*"))
                self.assertEqual(compiler[compiler.index("-d") + 2:],
                                 [str(ROOT / "demo/java/src/main/java/com/zilliz/spark/connector/demo/SnapshotPath.java")])
                classes = Path(compiler[compiler.index("-d") + 1])
                self.assertFalse(classes.parent.exists(), "snapshot helper build was not cleaned")
                self.assertEqual(self.java_capture.read_bytes().decode().split("\0")[:-1],
                                 ["-cp", str(classes) + ":" + str(self.spark / "jars/*"),
                                  "com.zilliz.spark.connector.demo.SnapshotPath", *arguments])
                self.assertEqual(self.helper_environment_capture.read_bytes().decode().split("\0")[:-1],
                                 ["http://example.invalid:19530", "test-token"])
                self.assertFalse(self.capture.exists())
                self.assertFalse(self.jar_capture.exists())

    def test_java_snapshot_helper_validates_its_jdk(self):
        self.assert_failed(self.run_demo("java", "SnapshotPath", CONNECTOR_JAR=None,
                                         DEMO_TEST_JAVA_VERSION="17"), "Java 21")
        self.assertFalse(self.javac_capture.exists())

    def test_unknown_or_qualified_java_classes_are_rejected(self):
        for example in ("Missing", "ExactSearch.java", "../ExactSearch", "com.zilliz.spark.connector.demo.ExactSearch",
                        "ExactSearch;echo injected"):
            with self.subTest(example=example):
                self.assert_failed(self.run_demo("java", example), "Unknown Java example")

    def test_every_java_example_compiles_and_selects_its_main_class(self):
        for example in JAVA_EXAMPLES:
            with self.subTest(example=example):
                result = self.run_demo("java", example, "--explain")
                self.assertEqual(result.returncode, 0, result.stderr)
                args = self.arguments()
                at = args.index("--class")
                self.assertEqual(args[at + 1], "com.zilliz.spark.connector.demo." + example)
                app = Path(args[at + 2])
                self.assertEqual(app.name, "app.jar")
                self.assertEqual(app.parent.parent, ROOT / "demo/target")
                self.assertFalse(app.parent.exists(), "owned build directory was not cleaned")
                self.assertEqual(args[-1], "--explain")
                self.assertNotIn("--py-files", args)
                self.assertEqual(args[args.index("--deploy-mode") + 1], "client")
                compiler = self.javac_capture.read_bytes().decode().split("\0")[:-1]
                self.assertEqual(compiler[:4], ["--release", "17", "-encoding", "UTF-8"])
                self.assertEqual(compiler[compiler.index("-classpath") + 1],
                                 str(self.spark / "jars/*") + ":" + str(self.assembly))
                sources = set(compiler[compiler.index("-d") + 2:])
                expected_sources = {str(path) for path in (ROOT / "demo/java/src/main/java").rglob("*.java")}
                self.assertEqual(sources, expected_sources)
                self.assertTrue(sources)
                self.assertEqual(self.jar_capture.read_bytes().decode().split("\0")[:-1],
                                 ["--create", "--file", str(app), "-C", str(app.parent / "classes"), "."])
                self.assertEqual(self.environment_capture.read_bytes().decode().split("\0")[:-1],
                                 [str(self.java / "lib/libjsig.so"), "", "1", str(ROOT)])

    def test_java_exit_codes_are_preserved_and_only_owned_build_output_is_removed(self):
        (ROOT / "demo/target").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="keep-test-", dir=ROOT / "demo/target") as unrelated:
            marker = Path(unrelated) / "keep.txt"
            marker.write_text("another build")
            result = self.run_demo("java", "ExactSearch", DEMO_TEST_SUBMIT_EXIT="42")
            self.assertEqual(result.returncode, 42, result.stderr)
            app = Path(self.arguments()[self.arguments().index("--class") + 2])
            self.assertFalse(app.parent.exists())
            self.assertEqual(marker.read_text(), "another build")
            result = self.run_demo("java", "ExactSearch", DEMO_TEST_JAVAC_EXIT="23")
            self.assertEqual(result.returncode, 23, result.stderr)
            self.assertFalse(self.capture.exists())
            compiler = self.javac_capture.read_bytes().decode().split("\0")[:-1]
            self.assertFalse(Path(compiler[compiler.index("-d") + 1]).parent.exists())
            self.assertEqual(marker.read_text(), "another build")

    def test_java_custom_inputs_and_vector_functions_can_run_on_a_cluster(self):
        self.assert_failed(self.run_demo("java", "ReadSnapshot", MASTER="yarn"),
                           "default snapshot requires MASTER=local")
        for arguments in (["ReadSnapshot", "--options", "/private/read options.json"], ["VectorFunctions"]):
            with self.subTest(arguments=arguments):
                result = self.run_demo("java", *arguments, MASTER="yarn")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.arguments()[:2], ["--master", "yarn"])

    def test_java_requires_compiler_and_packaging_tools(self):
        for name in ("javac", "jar"):
            path = self.java / "bin" / name
            path.chmod(0o644)
            self.assert_failed(self.run_demo("java", "ReadSnapshot"), f"JAVA_HOME/bin/{name} must be executable")
            path.chmod(0o755)

    def test_unsupported_platforms_fail_before_submission(self):
        for system, architecture in (("Darwin", "arm64"), ("Windows", "x86_64"), ("Linux", "ppc64le")):
            for source in ({}, {"CONNECTOR_JAR": None, "CANDIDATE": "/candidate"}):
                with self.subTest(system=system, architecture=architecture, source=source):
                    self.assert_failed(self.run_python("01_read_snapshot.py", DEMO_TEST_SYSTEM=system,
                                                     DEMO_TEST_MACHINE=architecture, **source),
                                       "supports Linux on x86_64 or aarch64")

    def test_candidate_verification_selects_exact_jars_and_rejects_tampering(self):
        spec = importlib.util.spec_from_file_location("demo_release_fixtures", ROOT / "scripts/tests/test_release.py")
        fixtures = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixtures)
        fixture = fixtures.ReleaseTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        manifest = fixture.stage()
        candidate = fixture.output
        result = self.run_python("01_read_snapshot.py", CONNECTOR_JAR=None, CANDIDATE=str(candidate))
        self.assertEqual(result.returncode, 0, result.stderr)
        wanted = []
        for kind in ("jvm", "native"):
            entry = next(entry for entry in manifest["artifacts"]
                         if entry["kind"] == kind and not entry["classifier"] and entry["extension"] == "jar"
                         and (entry["line"] == "4.0" if kind == "jvm" else entry["platform"] == "linux-x86_64"))
            wanted.append(str(candidate / "repository" / entry["path"]))
        self.assertEqual(self.arguments()[3], ",".join(wanted))
        Path(wanted[0]).write_bytes(b"changed candidate")
        self.assert_failed(self.run_python("01_read_snapshot.py", CONNECTOR_JAR=None, CANDIDATE=str(candidate)),
                           "Changed candidate")


if __name__ == "__main__":
    unittest.main()
