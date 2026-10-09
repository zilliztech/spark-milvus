"""Checks for direct recipe pins and the newer-version selection contract."""
import json
from pathlib import Path
import re
import unittest

from dependency_versions import validate_upstream_versions


class VersionSelectionTest(unittest.TestCase):
    def setUp(self):
        self.constraints = json.loads((Path(__file__).parent / "dependencies.json").read_text())

    def test_top_level_references_are_exact_upstream_recipe_revisions(self):
        self.assertNotIn("custom_recipes", self.constraints)
        self.assertTrue(self.constraints["references"])
        for name, reference in self.constraints["references"].items():
            with self.subTest(name=name):
                self.assertRegex(reference, r"^[^/]+/[^#]+#[0-9a-f]{32}$")

    def test_selects_newer_numeric_and_reviewed_commit_versions(self):
        storage = '\n'.join(['self.requires("fmt/11.2.0#old")',
                             'self.requires("lz4/1.9.4#old")',
                             'self.requires("milvus-common/1.0.0-60a563c@milvus/dev#old")'])
        knowhere = '\n'.join(['self.requires("fmt/12.1.0#new")',
                              'self.requires("lz4/1.10.0#new")',
                              'self.requires("milvus-common/1.0.0-b589c5a@milvus/dev#new")'])
        report = validate_upstream_versions(self.constraints, storage, knowhere)
        self.assertEqual(set(report["conflicts"]), {"fmt", "lz4", "milvus-common"})
        self.assertEqual(report["conflicts"]["lz4"]["selected"], "1.10.0")

    def test_rejects_selecting_older_upstream_release(self):
        self.constraints["references"]["lz4"] = "lz4/1.9.4#old"
        with self.assertRaisesRegex(ValueError, "newer upstream version for lz4"):
            validate_upstream_versions(self.constraints, 'self.requires("lz4/1.9.4")',
                                       'self.requires("lz4/1.10.0")')

    def test_does_not_silently_upgrade_beyond_upstream(self):
        self.constraints["references"]["fmt"] = "fmt/13.0.0#unreviewed"
        with self.assertRaisesRegex(ValueError, "newer upstream version for fmt"):
            validate_upstream_versions(self.constraints, 'self.requires("fmt/11.2.0")',
                                       'self.requires("fmt/12.1.0")')

    def test_rejects_unreviewed_commit_ordering(self):
        with self.assertRaisesRegex(ValueError, "commit version ordering"):
            validate_upstream_versions(self.constraints,
                                       'self.requires("milvus-common/1.0.0-ffffff0")', "")

    def test_rejects_missing_runtime_dependency(self):
        with self.assertRaisesRegex(ValueError, "missing from the unified graph: new-dependency"):
            validate_upstream_versions(self.constraints, 'self.requires("new-dependency/1.0")', "")

    def test_omits_disabled_test_requirements(self):
        report = validate_upstream_versions(self.constraints,
                                            'self.requires("gtest/1.15.0")',
                                            'self.requires("catch2/3.7.1")')
        self.assertEqual(report["conflicts"], {})


if __name__ == "__main__":
    unittest.main()
