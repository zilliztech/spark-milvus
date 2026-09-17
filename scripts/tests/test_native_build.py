"""Source snapshots and build isolation using temporary trees and real processes.

No Conan command, compiler invocation, or native build is executed. Snapshot
tests stop at the compiler-version probe; lock tests replace the build body.
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[2] / "native-build" / "build.py"
SPEC = importlib.util.spec_from_file_location("native_build", SCRIPT)
NATIVE_BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NATIVE_BUILD)
SUBPROCESS_RUN = subprocess.run


class SnapshotComplete(Exception):
    """Stop before compiler probes, dependency resolution, and native builds."""


LOCK_WORKER = """
import importlib.util
from pathlib import Path
import sys

driver, directory, mode = sys.argv[1:]
spec = importlib.util.spec_from_file_location("native_build_lock_worker", driver)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def controlled_build(args, repository, java_home, work):
    if mode == "contend":
        (work / "status.json").write_text("unexpected second build")
        (work / "provenance" / "source.json").write_text("unexpected provenance")
    print("BUILD_ENTERED", flush=True)
    if mode == "hold":
        action = sys.stdin.readline().strip()
        if action == "raise":
            raise RuntimeError("Simulated build failure")

module.build = controlled_build
sys.argv = [driver, "--work-dir", directory]
module.main()
"""


@unittest.skipUnless(
    sys.platform == "linux" and os.uname().machine == "x86_64",
    "The native build script currently supports Linux x86_64",
)
class NativeBuildLockTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="native-build-lock-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work = self.root / "work"
        (self.work / "provenance").mkdir(parents=True)
        self.sentinels = {
            "status.json": b'{"phase":"existing-build"}\n',
            "provenance/source.json": b'{"source":"existing-provenance"}\n',
        }
        for name, contents in self.sentinels.items():
            (self.work / name).write_bytes(contents)
        java = self.root / "java"
        (java / "bin").mkdir(parents=True)
        (java / "bin" / "javac").touch()
        self.environment = dict(os.environ, JAVA_HOME=str(java))

    def command(self, mode):
        return [sys.executable, "-c", LOCK_WORKER, str(SCRIPT), str(self.work), mode]

    @staticmethod
    def stop_process(process):
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)

    def start_holder(self):
        process = subprocess.Popen(
            self.command("hold"), env=self.environment, text=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.addCleanup(self.stop_process, process)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            self.assertTrue(selector.select(timeout=5), "Lock holder did not reach the build body")
        self.assertEqual("BUILD_ENTERED\n", process.stdout.readline())
        return process

    def assert_sentinels_unchanged(self):
        for name, contents in self.sentinels.items():
            self.assertEqual(contents, (self.work / name).read_bytes(), name)
        self.assertEqual(["source.json"], sorted(path.name for path in (self.work / "provenance").iterdir()))

    def assert_lock_available(self):
        result = subprocess.run(
            self.command("probe"), env=self.environment, text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("BUILD_ENTERED\n", result.stdout)
        self.assert_sentinels_unchanged()

    def test_competing_build_is_rejected_before_writes_and_normal_exit_releases_lock(self):
        holder = self.start_holder()

        # A blocking lock would hang here while the first process waits on stdin.
        rejected = subprocess.run(
            self.command("contend"), env=self.environment, text=True, capture_output=True, timeout=5,
        )

        self.assertEqual(2, rejected.returncode, rejected.stderr)
        self.assertIn("Another native build is already using this work directory", rejected.stderr)
        self.assertNotIn("BUILD_ENTERED", rejected.stdout)
        self.assertIsNone(holder.poll())
        self.assert_sentinels_unchanged()
        _, error = holder.communicate(input="release\n", timeout=5)
        self.assertEqual(0, holder.returncode, error)
        self.assert_lock_available()

    def test_failed_or_killed_build_releases_work_directory(self):
        for termination in ("exception", "kill"):
            with self.subTest(termination=termination):
                holder = self.start_holder()
                if termination == "exception":
                    _, error = holder.communicate(input="raise\n", timeout=5)
                    self.assertEqual(1, holder.returncode, error)
                    self.assertIn("Simulated build failure", error)
                else:
                    holder.kill()
                    holder.communicate(timeout=5)
                    self.assertEqual(-signal.SIGKILL, holder.returncode)
                self.assert_lock_available()


@unittest.skipUnless(
    sys.platform == "linux" and os.uname().machine == "x86_64",
    "The native build script currently supports Linux x86_64",
)
class NativeBuildSourceTest(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(shutil.which("git"), "Git is required by source snapshot tests")
        self.temporary = tempfile.TemporaryDirectory(prefix="native-build-source-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "connector"
        self.repository.mkdir()
        self.git(self.repository, "init", "--quiet")
        (self.repository / "native-build").mkdir()
        self.storage = self.repository / "milvus-storage"
        self.knowhere = self.repository / "knowhere"
        self.storage_revision = self.create_repository(self.storage, "cpp/source.cc", "int storage = 1;\n")
        self.knowhere_revision = self.create_repository(self.knowhere, "source.cc", "int knowhere = 2;\n")
        self.git(self.repository, "add", "milvus-storage", "knowhere")
        (self.repository / "native-build" / "dependencies.json").write_text(
            json.dumps({"references": {}, "cardinal": {}}), encoding="ascii",
        )
        # The historical bug discarded every source whenever an ancestor of
        # the snapshot directory had this perfectly valid name.
        self.work = self.root / "build"
        self.java = self.root / "java"
        (self.java / "bin").mkdir(parents=True)
        (self.java / "bin" / "javac").touch()

    def git(self, directory, *arguments):
        result = SUBPROCESS_RUN(
            ["git", "-C", str(directory), *arguments], capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()

    def create_repository(self, directory, filename, content):
        directory.mkdir()
        self.git(directory, "init", "--quiet")
        self.git(directory, "config", "user.name", "Native Build Test")
        self.git(directory, "config", "user.email", "native-build-test@example.invalid")
        source = directory / filename
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(content, encoding="ascii")
        self.git(directory, "add", ".")
        self.git(directory, "commit", "--quiet", "-m", "Initial test source")
        return self.git(directory, "rev-parse", "HEAD")

    def invoke_snapshot(self, knowhere_source=None):
        arguments = [
            str(SCRIPT), "--work-dir", str(self.work), "--storage-source", str(self.storage),
            "--jobs", "2",
        ]
        if knowhere_source is not None:
            arguments.extend(("--knowhere-source", str(knowhere_source)))

        def guard(command, *args, **kwargs):
            executable = str(command[0])
            if executable == "gcc-12":
                raise SnapshotComplete()
            if executable == "conan" or executable == "cmake":
                raise AssertionError("Snapshot tests must never resolve or compile dependencies")
            if executable == "git" and "stdout" not in kwargs:
                kwargs["stdout"] = subprocess.PIPE
                kwargs["stderr"] = subprocess.PIPE
            return SUBPROCESS_RUN(command, *args, **kwargs)

        with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(
            NATIVE_BUILD, "__file__", str(self.repository / "native-build" / "build.py"),
        ), mock.patch.object(sys, "argv", arguments), mock.patch.dict(
            os.environ, {"JAVA_HOME": str(self.java)},
        ), mock.patch.object(NATIVE_BUILD.subprocess, "run", side_effect=guard):
            NATIVE_BUILD.main()

    def initial_snapshot(self):
        with self.assertRaises(SnapshotComplete):
            self.invoke_snapshot()

    def test_manifest_includes_sources_beneath_work_directory_named_build(self):
        self.initial_snapshot()

        recorded = json.loads((self.work / "provenance" / "storage-source-files.json").read_text())
        self.assertEqual(
            {"cpp/source.cc": hashlib.sha256(b"int storage = 1;\n").hexdigest()}, recorded,
        )
        identity = json.loads((self.work / "provenance" / "storage-source-identity.json").read_text())
        self.assertEqual(self.storage_revision, identity["revision"])
        self.assertEqual(recorded, identity["files"])
        knowhere_identity = json.loads((self.work / "provenance" / "knowhere-source-identity.json").read_text())
        self.assertEqual(self.knowhere_revision, knowhere_identity["revision"])
        self.assertEqual(
            {"source.cc": hashlib.sha256(b"int knowhere = 2;\n").hexdigest()}, knowhere_identity["files"],
        )

    def test_matching_alternate_knowhere_object_source_is_accepted(self):
        alternate = self.root / "knowhere-cache"
        SUBPROCESS_RUN(["git", "clone", "--quiet", str(self.knowhere), str(alternate)], check=True)

        with self.assertRaises(SnapshotComplete):
            self.invoke_snapshot(alternate)

        self.assertEqual(self.knowhere_revision, self.git(self.work / "sources" / "knowhere", "rev-parse", "HEAD"))

    def test_alternate_knowhere_object_source_must_match_recorded_gitlink(self):
        alternate = self.root / "wrong-knowhere"
        self.create_repository(alternate, "source.cc", "int knowhere = 9;\n")

        with self.assertRaisesRegex(ValueError, "object source HEAD differs"):
            self.invoke_snapshot(alternate)

    def test_knowhere_submodule_head_must_match_recorded_gitlink(self):
        (self.knowhere / "source.cc").write_text("int knowhere = 10;\n", encoding="ascii")
        self.git(self.knowhere, "add", ".")
        self.git(self.knowhere, "commit", "--quiet", "-m", "Move submodule HEAD")

        with self.assertRaisesRegex(ValueError, "submodule HEAD differs from the recorded gitlink"):
            self.invoke_snapshot()

    def test_storage_submodule_head_must_match_recorded_gitlink(self):
        (self.storage / "cpp" / "source.cc").write_text("int storage = 10;\n", encoding="ascii")
        self.git(self.storage, "add", ".")
        self.git(self.storage, "commit", "--quiet", "-m", "Move storage submodule HEAD")

        with self.assertRaisesRegex(ValueError, "Storage submodule HEAD differs from the recorded gitlink"):
            self.invoke_snapshot()

    def test_unchanged_sources_allow_resume(self):
        self.initial_snapshot()
        manifest = self.work / "provenance" / "storage-source-files.json"
        before = manifest.read_bytes()

        self.initial_snapshot()

        self.assertEqual(before, manifest.read_bytes())

    def test_changed_storage_revision_cannot_reuse_snapshot(self):
        self.initial_snapshot()
        (self.storage / "cpp" / "source.cc").write_text("int storage = 3;\n", encoding="ascii")
        self.git(self.storage, "add", ".")
        self.git(self.storage, "commit", "--quiet", "-m", "Change source pin")
        self.git(self.repository, "add", "milvus-storage")

        with self.assertRaisesRegex(RuntimeError, "Storage source changed"):
            self.invoke_snapshot()
        self.assertEqual(
            "int storage = 1;\n", (self.work / "sources" / "milvus-storage" / "cpp" / "source.cc").read_text(),
        )

    def test_uncommitted_storage_source_changes_cannot_reuse_snapshot(self):
        self.initial_snapshot()
        (self.storage / "cpp" / "source.cc").write_text("int storage = 4;\n", encoding="ascii")
        self.assertEqual(self.storage_revision, self.git(self.storage, "rev-parse", "HEAD"))

        with self.assertRaisesRegex(RuntimeError, "Storage source changed"):
            self.invoke_snapshot()

    def test_untracked_storage_source_is_bound_to_snapshot(self):
        addition = self.storage / "cpp" / "new-jni.cc"
        addition.write_text("int new_jni = 1;\n", encoding="ascii")
        self.initial_snapshot()
        recorded = json.loads((self.work / "provenance" / "storage-source-files.json").read_text())
        self.assertIn("cpp/new-jni.cc", recorded)
        addition.write_text("int new_jni = 2;\n", encoding="ascii")

        with self.assertRaisesRegex(RuntimeError, "Storage source changed"):
            self.invoke_snapshot()

    def test_mutated_storage_snapshot_is_rejected(self):
        self.initial_snapshot()
        (self.work / "sources" / "milvus-storage" / "cpp" / "source.cc").write_text(
            "int stale_snapshot = 5;\n", encoding="ascii",
        )

        with self.assertRaisesRegex(RuntimeError, "Storage snapshot was modified"):
            self.invoke_snapshot()

    def test_snapshot_without_identity_is_not_trusted(self):
        snapshot = self.work / "sources" / "milvus-storage" / "cpp"
        snapshot.mkdir(parents=True)
        (snapshot / "source.cc").write_text("int storage = 1;\n", encoding="ascii")

        with self.assertRaisesRegex(RuntimeError, "snapshot is unverified"):
            self.invoke_snapshot()

    def test_tracked_knowhere_modification_is_rejected_on_resume(self):
        self.initial_snapshot()
        snapshot = self.work / "sources" / "knowhere"
        (snapshot / "source.cc").write_text("int modified_knowhere = 6;\n", encoding="ascii")

        with self.assertRaisesRegex(RuntimeError, "Recorded source checkout was modified"):
            self.invoke_snapshot()

    def test_untracked_knowhere_modification_is_rejected_on_resume(self):
        self.initial_snapshot()
        snapshot = self.work / "sources" / "knowhere"
        (snapshot / "injected.cc").write_text("int added_knowhere = 7;\n", encoding="ascii")

        with self.assertRaisesRegex(RuntimeError, "Recorded source checkout was modified"):
            self.invoke_snapshot()

    def test_knowhere_snapshot_remains_pristine_across_resume(self):
        self.initial_snapshot()
        snapshot = self.work / "sources" / "knowhere"
        source = snapshot / "source.cc"
        manifest = self.work / "provenance" / "knowhere-source-files.json"
        recorded = manifest.read_bytes()
        self.assertEqual("int knowhere = 2;\n", source.read_text())
        self.assertEqual(self.knowhere_revision, self.git(snapshot, "rev-parse", "HEAD"))
        self.assertEqual("", self.git(snapshot, "status", "--porcelain"))
        self.assertFalse((self.work / "provenance" / "knowhere-working-tree.patch").exists())

        self.initial_snapshot()

        self.assertEqual(recorded, manifest.read_bytes())
        self.assertEqual("int knowhere = 2;\n", source.read_text())
        source.write_text("int unreviewed_knowhere = 10;\n", encoding="ascii")
        with self.assertRaisesRegex(RuntimeError, "Recorded source checkout was modified"):
            self.invoke_snapshot()

    def test_hashes_ignore_only_relative_build_artifacts(self):
        source = self.root / "build" / "sources"
        source.mkdir(parents=True)
        (source / "keep.cc").write_text("int keep = 8;\n", encoding="ascii")
        for directory in ("build", "target", "__pycache__", ".git"):
            (source / directory).mkdir()
            (source / directory / "generated").write_text("ignored", encoding="ascii")
        (source / "libnative.so").write_text("ignored", encoding="ascii")
        (source / "libnative.so.1").write_text("ignored", encoding="ascii")

        self.assertEqual({"keep.cc": hashlib.sha256(b"int keep = 8;\n").hexdigest()}, NATIVE_BUILD.tree_hashes(source))

    def test_empty_source_manifest_is_rejected(self):
        empty = self.root / "empty"
        empty.mkdir()

        with self.assertRaisesRegex(RuntimeError, "Source manifest is empty"):
            NATIVE_BUILD.tree_hashes(empty)

    def test_explicit_storage_pin_cannot_mislabel_a_git_working_tree(self):
        with self.assertRaisesRegex(ValueError, "override differs from the recorded gitlink"):
            NATIVE_BUILD.storage_revision(self.repository, self.storage, "f" * 40)

        self.assertEqual(
            self.storage_revision,
            NATIVE_BUILD.storage_revision(self.repository, self.storage, self.storage_revision),
        )
        self.assertEqual(
            self.storage_revision,
            NATIVE_BUILD.storage_revision(self.repository, self.storage, None),
        )

    def test_exported_storage_source_requires_an_explicit_pin(self):
        exported = self.root / "exported"
        exported.mkdir()
        with self.assertRaisesRegex(ValueError, "exported storage source requires --storage-revision"):
            NATIVE_BUILD.storage_revision(self.repository, exported, None)

        self.assertEqual(
            self.storage_revision,
            NATIVE_BUILD.storage_revision(self.repository, exported, self.storage_revision),
        )
        with self.assertRaisesRegex(ValueError, "matching the recorded gitlink"):
            NATIVE_BUILD.storage_revision(self.repository, exported, "f" * 40)


if __name__ == "__main__":
    unittest.main()
