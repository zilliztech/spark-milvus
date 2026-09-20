"""Check repeated staging and reusable dependency/tool source inputs."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import jvm_load
import platforms
from build import (knowhere_c_api_tests, cardinal_versions, digest, platform_tool_requirements, prepare_conan_lock,
                   promote_bundle, replace_requires_section, snapshot_corrosion, validate_conan_lock,
                   validate_locked_graph)


class CardinalVersionTest(unittest.TestCase):
    def write(self, root, generation, body):
        path = root / "cmake" / "libs" / "cardinal" / ("v" + generation) / "CMakeLists.txt"
        path.parent.mkdir(parents=True)
        path.write_text(body)

    def test_tags_come_from_the_knowhere_cmake_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(root, "1", "# comment\nset(CARDINAL_VERSION v2.5.112)\nset(CARDINAL_REPO_URL \"x\")\n")
            self.write(root, "2", "set(CARDINAL_VERSION v3.0.8)\n")
            self.assertEqual(cardinal_versions(root), {"1": "v2.5.112", "2": "v3.0.8"})

    def test_a_missing_or_ambiguous_version_file_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(root, "1", "set(CARDINAL_VERSION v2.5.112)\n")
            with self.assertRaisesRegex(RuntimeError, "no Cardinal version file"):
                cardinal_versions(root)
            self.write(root, "2", "set(CARDINAL_VERSION a)\nset(CARDINAL_VERSION b)\n")
            with self.assertRaisesRegex(RuntimeError, "exactly one"):
                cardinal_versions(root)


class BuildContextPinTest(unittest.TestCase):
    def test_every_selected_reference_replaces_its_recipe_in_both_contexts(self):
        references = {"zlib": "zlib/1.3.1#8045", "grpc": "grpc/1.67.1@milvus/dev#efea"}
        section = replace_requires_section(references)
        self.assertEqual(section, "[replace_requires]\ngrpc/*: grpc/1.67.1@milvus/dev#efea\nzlib/*: zlib/1.3.1#8045\n")

    def test_replacements_keep_the_exact_recipe_revision(self):
        section = replace_requires_section({"openssl": "openssl/3.3.2#9f9f"})
        self.assertIn("openssl/*: openssl/3.3.2#9f9f", section)
        self.assertNotIn("openssl/*: openssl/3.3.2\n", section)

#: The JNI entries this platform names, so fixtures match what promotion checks.
ENTRIES = jvm_load.JVM_LOAD_ENTRIES


class BundlePromotionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.work = Path(self.temporary.name)

    def candidate(self, name, **changes):
        path = self.work / "bundle-candidates" / name
        path.mkdir(parents=True)
        record = {"audit": "passed", "knowhereCApiTestsExit": 0,
                  "knowhereCApiTests": knowhere_c_api_tests(platforms.host()), "auditPolicy": "jvm-load",
                  "jvmLoadTests": [
                      {"entries": list(ENTRIES), "exit": 0},
                      {"entries": list(ENTRIES[::-1]), "exit": 0},
                  ]}
        record.update(changes)
        (path / "provenance.json").write_text(json.dumps(record))
        (path / "library.so").write_text(name)
        return path

    def test_second_success_preserves_the_first_bundle(self):
        promote_bundle(self.candidate("first"), self.work)
        promote_bundle(self.candidate("second"), self.work)
        self.assertEqual("second", (self.work / "bundle/library.so").read_text())
        self.assertEqual("first", (self.work / "bundle-history/second/library.so").read_text())

    def test_failed_tests_or_audit_preserve_previous_bundle_and_candidate(self):
        promote_bundle(self.candidate("good"), self.work)
        for name, changes in (("failed-tests", {"knowhereCApiTestsExit": 1}),
                              ("failed-audit", {"audit": "pending"}),
                              ("old-audit-policy", {"auditPolicy": "standalone"}),
                              ("missing-jvm-loads", {"jvmLoadTests": []}),
                              ("failed-jvm-load", {"jvmLoadTests": [
                                  {"entries": list(ENTRIES), "exit": 1},
                                  {"entries": list(ENTRIES[::-1]), "exit": 0},
                              ]}),
                              ("missing-test", {"knowhereCApiTests": ["knowhere_c_api"]})):
            with self.subTest(name=name):
                candidate = self.candidate(name, **changes)
                with self.assertRaisesRegex(ValueError, "not passed"):
                    promote_bundle(candidate, self.work)
                self.assertEqual("good", (self.work / "bundle/library.so").read_text())
                self.assertEqual(name, (candidate / "library.so").read_text())

    def test_failed_promotion_restores_previous_bundle(self):
        promote_bundle(self.candidate("good"), self.work)
        candidate = self.candidate("replacement")
        rename = Path.rename

        def failing_rename(path, destination):
            if path == candidate:
                raise OSError("simulated filesystem failure")
            return rename(path, destination)

        with patch.object(Path, "rename", failing_rename):
            with self.assertRaisesRegex(OSError, "filesystem failure"):
                promote_bundle(candidate, self.work)
        self.assertEqual("good", (self.work / "bundle/library.so").read_text())
        self.assertTrue(candidate.is_dir())


class ConanLockTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "reviewed.lock"
        self.destination = self.root / "conan.lock"
        self.identity = self.root / "conan-lock.sha256"
        self.references = {"grpc": "grpc/1.0#direct"}
        self.record = {"version": "0.5", "requires": ["grpc/1.0#direct%123", "c-ares/1.0#transitive%456"],
                       "build_requires": ["protoc/1.0#tool"], "python_requires": [], "config_requires": []}
        self.source.write_text(json.dumps(self.record))

    def test_prior_lock_is_copied_verbatim_and_reused_on_resume(self):
        prepare_conan_lock(self.source, self.destination, self.identity, self.references)
        prepare_conan_lock(None, self.destination, self.identity, self.references)
        self.assertEqual(self.source.read_bytes(), self.destination.read_bytes())
        self.assertEqual(digest(self.source), self.identity.read_text().strip())

    def test_changed_input_is_rejected_without_replacing_recorded_lock(self):
        prepare_conan_lock(self.source, self.destination, self.identity, self.references)
        original = self.destination.read_bytes()
        self.record["requires"][1] = "c-ares/2.0#different"
        self.source.write_text(json.dumps(self.record))
        with self.assertRaisesRegex(ValueError, "lock changed"):
            prepare_conan_lock(self.source, self.destination, self.identity, self.references)
        self.assertEqual(original, self.destination.read_bytes())

    def test_modified_recorded_lock_is_rejected(self):
        prepare_conan_lock(self.source, self.destination, self.identity, self.references)
        self.destination.write_text(json.dumps(self.record, indent=2))
        with self.assertRaisesRegex(ValueError, "Recorded Conan lock was modified"):
            prepare_conan_lock(None, self.destination, self.identity, self.references)

    def test_lock_must_match_pinned_direct_recipe_revisions(self):
        with self.assertRaisesRegex(ValueError, "selected direct recipes"):
            validate_conan_lock(self.source, {"grpc": "grpc/1.0#new-patch"})

    def test_lock_must_pin_transitive_recipe_revisions(self):
        self.record["requires"][1] = "c-ares/1.0"
        self.source.write_text(json.dumps(self.record))
        with self.assertRaisesRegex(ValueError, "pin every recipe revision"):
            validate_conan_lock(self.source, self.references)

    def test_only_profile_declared_platform_tools_may_omit_recipe_revision(self):
        profile = self.root / "profile"
        profile.write_text("[settings]\nos=Linux\n[platform_tool_requires]\ncmake/3.27.5\n[options]\nfmt/*:shared=True\n")
        self.record["build_requires"].append("cmake/3.27.5")
        self.source.write_text(json.dumps(self.record))
        validate_conan_lock(self.source, self.references, platform_tool_requirements(profile))
        with self.assertRaisesRegex(ValueError, "pin every recipe revision"):
            validate_conan_lock(self.source, self.references)

    def test_graph_cannot_resolve_an_unlocked_transitive_or_build_dependency(self):
        graph = {"0": {"ref": "consumer/1", "context": "host", "recipe": "Consumer"},
                 "1": {"ref": "grpc/1.0#direct", "context": "host", "recipe": "Cache"},
                 "2": {"ref": "c-ares/1.0#transitive", "context": "host", "recipe": "Cache"},
                 "3": {"ref": "protoc/1.0#tool", "context": "build", "recipe": "Cache"}}
        validate_locked_graph(graph, self.record)
        for context in ("host", "build"):
            with self.subTest(context=context):
                graph["4"] = {"ref": "another/2.0#unreviewed", "context": context,
                              "recipe": "Cache"}
                with self.assertRaisesRegex(ValueError, "absent from the Conan lock"):
                    validate_locked_graph(graph, self.record)

    def test_platform_tool_without_revision_must_still_be_in_the_lock(self):
        self.record["build_requires"].append("cmake/3.27.5")
        graph = {"0": {"ref": "consumer/1", "context": "host", "recipe": "Consumer"},
                 "1": {"ref": "cmake/3.27.5", "context": "build", "recipe": "Platform"}}
        validate_locked_graph(graph, self.record)
        self.record["build_requires"].remove("cmake/3.27.5")
        with self.assertRaisesRegex(ValueError, "absent from the Conan lock"):
            validate_locked_graph(graph, self.record)

    def test_non_consumer_graph_node_requires_a_reference(self):
        graph = {"1": {"context": "host", "recipe": "Cache"}}
        with self.assertRaisesRegex(ValueError, "has no reference"):
            validate_locked_graph(graph, self.record)


class CorrosionSourceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "upstream"
        self.source.mkdir()
        self.git("init", "-q")
        self.git("config", "user.name", "Native Build Test")
        self.git("config", "user.email", "native-build@example.invalid")
        (self.source / "CMakeLists.txt").write_text("project(fixture NONE)\n")
        self.git("add", ".")
        self.git("commit", "-qm", "Pinned fixture")
        self.revision = self.git("rev-parse", "HEAD").strip()
        self.specification = {"repository": str(self.source), "revision": self.revision}

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.source), *arguments], text=True)

    def test_pinned_tool_is_snapshotted_without_changing_local_checkout(self):
        destination = self.root / "snapshot"
        files = snapshot_corrosion(self.specification, self.source, destination)
        self.assertEqual({"CMakeLists.txt": digest(self.source / "CMakeLists.txt")}, files)
        self.assertEqual(self.revision, self.git("rev-parse", "HEAD").strip())
        self.assertEqual(files, snapshot_corrosion(self.specification, self.source, destination))

    def test_wrong_revision_and_dirty_source_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "pinned revision"):
            snapshot_corrosion({**self.specification, "revision": "0" * 40}, self.source, self.root / "wrong")
        (self.source / "untracked.cmake").write_text("unreviewed")
        with self.assertRaisesRegex(ValueError, "clean pinned checkout"):
            snapshot_corrosion(self.specification, self.source, self.root / "dirty")


if __name__ == "__main__":
    unittest.main()
