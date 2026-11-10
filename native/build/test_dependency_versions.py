"""Checks for direct recipe pins and the newer-version selection contract."""
import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest

from dependency_versions import selected_references, selected_transitive_references, validate_upstream_versions


class VersionSelectionTest(unittest.TestCase):
    def setUp(self):
        self.constraints = json.loads((Path(__file__).parent / "dependencies.json").read_text())

    def validate(self, storage, component, *, conan_os="Linux", with_extension=False):
        return validate_upstream_versions(self.constraints, {
            "storage": (storage, {"with_crt": False, "with_ut": False, "with_benchmark": False}),
            "component": (component, {"with_extension": with_extension, "with_light": False,
                                      "with_ut": False, "with_benchmark": False})
        }, conan_os=conan_os)

    def test_top_level_references_are_exact_upstream_recipe_revisions(self):
        self.assertNotIn("custom_recipes", self.constraints)
        self.assertTrue(self.constraints["references"])
        for name, reference in self.constraints["references"].items():
            with self.subTest(name=name):
                self.assertRegex(reference, r"^[^/]+/[^#]+#[0-9a-f]{32}$")

    def test_transitive_recipes_are_exact_and_do_not_become_direct_requirements(self):
        direct = selected_references(self.constraints, conan_os="Linux")
        transitive = selected_transitive_references(self.constraints, conan_os="Linux")
        self.assertFalse(set(direct) & set(transitive))
        for name, reference in transitive.items():
            with self.subTest(name=name):
                self.assertRegex(reference, r"^" + name + r"/[^#]+#[0-9a-f]{32}$")
        self.assertIn("s2n", transitive)
        self.assertNotIn("s2n", selected_transitive_references(self.constraints, conan_os="Macos"))
        self.assertIn("aws-c-event-stream", transitive)

    def test_selects_newer_numeric_and_reviewed_commit_versions(self):
        storage = '\n'.join(['self.requires("fmt/11.2.0#old")',
                             'self.requires("lz4/1.9.4#old")',
                             'self.requires("milvus-common/1.0.0-60a563c@milvus/dev#old")'])
        component = '\n'.join(['self.requires("fmt/12.1.0#new")',
                              'self.requires("lz4/1.10.0#new")',
                              'self.requires("milvus-common/1.0.0-78f2c29@milvus/dev#new")'])
        self.constraints["references"].update({"fmt": "fmt/12.1.0#new", "lz4": "lz4/1.10.0#new",
                                              "milvus-common": "milvus-common/1.0.0-78f2c29#new"})
        report = self.validate(storage, component)
        self.assertEqual(set(report["conflicts"]), {"fmt", "lz4", "milvus-common"})
        self.assertEqual(report["conflicts"]["lz4"]["selected"], "1.10.0")

    def test_rejects_selecting_older_upstream_release(self):
        self.constraints["references"]["lz4"] = "lz4/1.9.4#old"
        with self.assertRaisesRegex(ValueError, "newer upstream version for lz4"):
            self.validate('self.requires("lz4/1.9.4")', 'self.requires("lz4/1.10.0")')

    def test_does_not_silently_upgrade_beyond_upstream(self):
        self.constraints["references"]["fmt"] = "fmt/13.0.0#unreviewed"
        with self.assertRaisesRegex(ValueError, "newer upstream version for fmt"):
            self.validate('self.requires("fmt/11.2.0")', 'self.requires("fmt/12.1.0")')

    def test_rejects_unreviewed_commit_ordering(self):
        with self.assertRaisesRegex(ValueError, "commit version ordering"):
            self.validate('self.requires("milvus-common/1.0.0-ffffff0")', "")

    def test_rejects_missing_runtime_dependency(self):
        with self.assertRaisesRegex(ValueError, "missing from the unified graph: new-dependency"):
            self.validate('self.requires("new-dependency/1.0")', "")

    def test_omits_disabled_test_requirements(self):
        report = self.validate('self.requires("gtest/1.15.0")', 'self.requires("catch2/3.7.1")')
        self.assertEqual(report["conflicts"], {})

    def test_known_conditions_choose_one_version_and_keep_platform_requirements(self):
        recipe = '''if self.options.with_extension:
    self.requires("fmt/12.1.0")
else:
    self.requires("fmt/11.2.0")
if self.settings.os == "Linux":
    self.requires("liburing/2.8")
if self.settings.os != "Macos":
    self.requires("libunwind/1.8.1")
if not self.options.with_light:
    self.requires("opentelemetry-cpp/1.23.0")
'''
        self.constraints["references"].update({"fmt": "fmt/12.1.0#new", "liburing": "liburing/2.8#new"})
        for conan_os in ("Linux", "Macos"):
            report = self.validate("", recipe, conan_os=conan_os, with_extension=True)
            requirements = report["requirements"]["component"]
            self.assertEqual("12.1.0", requirements["fmt"])
            self.assertEqual(conan_os == "Linux", "liburing" in requirements)
            self.assertEqual(conan_os == "Linux", "libunwind" in requirements)
            self.assertIn("opentelemetry-cpp", requirements)
        with self.assertRaisesRegex(ValueError, "newer upstream version for fmt: 11.2.0"):
            self.validate("", recipe, with_extension=False)

    def test_unknown_conditions_or_dynamic_requirements_fail_even_in_disabled_branch(self):
        for statement in ('if self.options.unknown:\n    self.requires("fmt/12.1.0")',
                          'if detect_feature():\n    self.requires("fmt/12.1.0")',
                          'self.requires(compute_reference())',
                          'self.add_requirements()',
                          'for ref in refs:\n    self.requires(ref)',
                          'if self.options.with_extension:\n    self.requires(compute_reference())'):
            with self.subTest(statement=statement), self.assertRaisesRegex(ValueError, "Unsupported upstream"):
                self.validate("", statement)

    def test_real_conan_recipe_selects_the_same_graph_as_driver_and_validator(self):
        # Exercise our actual recipe body without requiring Conan in the test interpreter.
        source = ast.parse((Path(__file__).parent / "conanfile.py").read_text())
        recipe = next(node for node in source.body if isinstance(node, ast.ClassDef))
        method = next(node for node in recipe.body if isinstance(node, ast.FunctionDef)
                      and node.name == "requirements")
        namespace = {"json": json, "os": os}
        exec(compile(ast.Module(body=[method], type_ignores=[]), "conanfile.py", "exec"), namespace)
        for conan_os in ("Linux", "Macos"):
            for enabled in (False, True):
                with self.subTest(conan_os=conan_os, with_extension=enabled):
                    observed = {}
                    def requires(reference, **keywords):
                        self.assertEqual({"force": True}, keywords)
                        observed[reference.split("/", 1)[0]] = reference
                    consumer = SimpleNamespace(recipe_folder=str(Path(__file__).parent),
                                               settings=SimpleNamespace(os=conan_os),
                                               options=SimpleNamespace(with_extension=enabled), requires=requires)
                    namespace["requirements"](consumer)
                    expected = selected_references(self.constraints, conan_os=conan_os)
                    self.assertEqual(expected, observed)

    def test_initialized_storage_recipe_fits_the_public_graph(self):
        repository = Path(__file__).resolve().parents[2]
        storage = repository / "milvus-storage/cpp/conanfile.py"
        if not storage.is_file():
            self.skipTest("Initialize milvus-storage to validate its recipe pins")
        for conan_os in ("Linux", "Macos"):
            report = self.validate(storage.read_text(), "", conan_os=conan_os)
            self.assertEqual({}, report["conflicts"])


if __name__ == "__main__":
    unittest.main()
