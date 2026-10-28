"""The connector image build validates its input, isolates its context and publishes explicitly."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = "core/src/test/resources/read-fixtures/collections/nearest"


class DockerBuildTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="spark-milvus-image-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = self.root / "checkout"
        for directory in ("demo/python", "demo/java", "demo/config", FIXTURE):
            shutil.copytree(ROOT / directory, self.repo / directory,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "target"))
        for relative in ("LICENSE", "demo/run.sh", "docker/build.sh", "docker/Dockerfile",
                         "scripts/ci/check_read_fixtures.py"):
            target = self.repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)
        self.script = self.repo / "docker/build.sh"
        self.assembly = self.root / "assembly with spaces.jar"
        self.jar()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.capture = self.root / "docker.jsonl"
        self.context_capture = self.root / "context.json"
        self.temporary_contexts = self.root / "contexts"
        self.temporary_contexts.mkdir()
        docker = self.bin / "docker"
        docker.write_text("#!" + sys.executable + "\n" + '''
import hashlib
import json
import os
from pathlib import Path
import sys

with open(os.environ["DEMO_TEST_DOCKER_CALLS"], "a") as output:
    output.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[1] == "build":
    context = Path(sys.argv[-1])
    files = {}
    for path in sorted(context.rglob("*")):
        if path.is_file():
            data = path.read_bytes()
            files[str(path.relative_to(context))] = {
                "sha256": hashlib.sha256(data).hexdigest(),
                "has_private_sentinel": b"must-not-enter-image" in data,
                "symlink": path.is_symlink(),
            }
    Path(os.environ["DEMO_TEST_CONTEXT"]).write_text(json.dumps({
        "path": str(context), "files": files,
    }))
    sys.exit(int(os.environ.get("DEMO_TEST_BUILD_EXIT", "0")))
if sys.argv[1] == "push":
    sys.exit(int(os.environ.get("DEMO_TEST_PUSH_EXIT", "0")))
sys.exit("Unexpected Docker operation")
''')
        docker.chmod(0o755)
        uname = self.bin / "uname"
        uname.write_text('#!/bin/bash\nprintf "%s\\n" "$DEMO_TEST_MACHINE"\n')
        uname.chmod(0o755)
        self.env = dict(os.environ)
        self.env.update(
            PATH=str(self.bin) + os.pathsep + os.environ["PATH"], PYTHON=sys.executable,
            TMPDIR=str(self.temporary_contexts), DEMO_TEST_MACHINE="x86_64",
            DEMO_TEST_DOCKER_CALLS=str(self.capture), DEMO_TEST_CONTEXT=str(self.context_capture),
        )

    def jar(self, platform="linux-x86_64", plugin=True, aliases=False, missing_library=False):
        prefix = "native/milvus/1/" + platform + "/"
        properties = "format.version=1\nplatform=" + platform + "\n"
        with ZipFile(self.assembly, "w") as archive:
            if plugin:
                archive.writestr("com/zilliz/spark/connector/extensions/MilvusSparkPlugin.class", b"plugin")
            archive.writestr(prefix + "libmilvus-storage-jni.so", b"storage-native")
            if not missing_library:
                name = "libknowhere_jni-abcd.so" if aliases else "libknowhere_jni.so"
                archive.writestr(prefix + name, b"vector-native")
                if aliases:
                    properties += "alias.libknowhere_jni.so=" + name + "\n"
            archive.writestr(prefix + "manifest.properties", properties)

    def run_script(self, *arguments, **environment):
        return subprocess.run(["bash", str(self.script), *arguments], cwd=self.root,
                              env={**self.env, **environment}, text=True, capture_output=True)

    def build(self, *arguments, **environment):
        return self.run_script("--assembly", str(self.assembly), "--image", "demo:test",
                               *arguments, **environment)

    def calls(self):
        return [json.loads(line) for line in self.capture.read_text().splitlines()]

    def assert_rejected(self, result, message):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)
        self.assertFalse(self.capture.exists(), "Docker ran after invalid setup")
        self.assertEqual(list(self.temporary_contexts.iterdir()), [], "Temporary context leaked")

    def test_help_and_missing_arguments_need_no_toolchain(self):
        result = self.run_script("--help", PYTHON="/missing/python")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--push", result.stdout)
        self.assertFalse(self.capture.exists())
        for arguments in ([], ["--assembly"], ["--image", "demo:test"], ["--unknown"]):
            with self.subTest(arguments=arguments):
                result = self.run_script(*arguments)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.capture.exists())

    def test_missing_or_invalid_assembly_never_reaches_docker(self):
        self.assembly.unlink()
        self.assert_rejected(self.build(), "Image setup failed")
        self.assembly.write_text("This is not a JAR")
        self.assert_rejected(self.build(), "File is not a zip file")
        self.jar(plugin=False)
        self.assert_rejected(self.build(), "does not contain MilvusSparkPlugin")
        self.jar(missing_library=True)
        self.assert_rejected(self.build(), "lacks native library libknowhere_jni.so")

    def test_native_platform_must_match_requested_image(self):
        self.assert_rejected(self.build("--platform", "linux/arm64"),
                             "lacks native resources for linux-aarch64")
        self.jar(platform="linux-aarch64")
        result = self.build("--platform", "linux/arm64")
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.calls()[0]
        self.assertEqual(args[args.index("--platform") + 1], "linux/arm64")

    def test_platform_default_and_native_manifest_aliases(self):
        self.jar(platform="linux-aarch64", aliases=True)
        result = self.build(DEMO_TEST_MACHINE="aarch64")
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.calls()[0]
        self.assertEqual(args[args.index("--platform") + 1], "linux/arm64")

    def test_context_excludes_private_files_and_build_is_local_by_default(self):
        for relative in (".git/config", ".env", "target/private-options.json",
                         "demo/config/private-options.json", "demo/python/secret.py",
                         "demo/python/__pycache__/common.pyc", "demo/target/private.jar",
                         "demo/java/src/main/java/com/zilliz/spark/connector/demo/Private.java"):
            path = self.repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("must-not-enter-image")
        result = self.build("--base-image", "mirror.example/spark:4.0.1")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual(len(calls), 1, "A local build must not push")
        self.assertEqual(calls[0][0], "build")
        self.assertIn("BASE_IMAGE=mirror.example/spark:4.0.1", calls[0])
        digest = hashlib.sha256(self.assembly.read_bytes()).hexdigest()
        self.assertIn("Assembly SHA256: " + digest, result.stdout)
        self.assertIn("ASSEMBLY_SHA256=" + digest, calls[0])
        context = json.loads(self.context_capture.read_text())
        files = context["files"]
        self.assertEqual(files["connector.jar"]["sha256"], digest)
        for required in ("Dockerfile", "LICENSE", "demo/run.sh", "demo/python/common.py",
                         "demo/config/queries.json", "scripts/ci/check_read_fixtures.py",
                         "demo/java/src/main/java/com/zilliz/spark/connector/demo/ExactSearch.java",
                         FIXTURE + "/snapshot.json", FIXTURE + "/SHA256SUMS"):
            self.assertIn(required, files)
        self.assertFalse(any(data["has_private_sentinel"] or data["symlink"] for data in files.values()))
        self.assertFalse(any(part in {".git", "__pycache__", "target"}
                             for relative in files for part in Path(relative).parts))
        self.assertFalse(Path(context["path"]).exists(), "Build context was not removed")
        self.assertEqual(list(self.temporary_contexts.iterdir()), [])

    def test_public_source_symlink_and_tampered_fixture_are_rejected(self):
        path = self.repo / "demo/config/queries.json"
        original = path.read_bytes()
        path.unlink()
        private = self.root / "private-options.json"
        private.write_text("must-not-enter-image")
        path.symlink_to(private)
        self.assert_rejected(self.build(), "Missing public file or symlink")
        path.unlink()
        path.write_bytes(original)
        (self.repo / FIXTURE / "expected.json").write_text("tampered")
        self.assert_rejected(self.build(), "Fixture SHA256 mismatch")

    def test_push_is_explicit_and_occurs_only_after_successful_build(self):
        result = self.build("--push")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual([call[0] for call in calls], ["build", "push"])
        self.assertEqual(calls[1], ["push", "demo:test"])
        self.capture.unlink()
        result = self.build("--push", DEMO_TEST_BUILD_EXIT="42")
        self.assertEqual(result.returncode, 42)
        self.assertEqual([call[0] for call in self.calls()], ["build"])
        self.assertEqual(list(self.temporary_contexts.iterdir()), [])

    def test_push_failure_is_reported_and_context_is_removed(self):
        result = self.build("--push", DEMO_TEST_PUSH_EXIT="17")
        self.assertEqual(result.returncode, 17)
        self.assertEqual([call[0] for call in self.calls()], ["build", "push"])
        self.assertEqual(list(self.temporary_contexts.iterdir()), [])

    def test_invalid_platform_and_image_reference_are_rejected(self):
        self.assert_rejected(self.build("--platform", "darwin/arm64"), "--platform must be")
        self.assert_rejected(self.build("--image", "demo tag"), "cannot start with '-' or contain whitespace")


if __name__ == "__main__":
    unittest.main()
