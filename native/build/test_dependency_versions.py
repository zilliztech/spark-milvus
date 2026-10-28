"""Checks for direct recipe pins and the newer-version selection contract."""
import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest

from dependency_versions import selected_references, validate_upstream_versions


class VersionSelectionTest(unittest.TestCase):
    def setUp(self):
        self.constraints = json.loads((Path(__file__).parent / "dependencies.json").read_text())

    def validate(self, storage, knowhere, *, conan_os="Linux", with_cardinal=False):
        return validate_upstream_versions(self.constraints, storage, knowhere,
                                          conan_os=conan_os, with_cardinal=with_cardinal)

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
                              'self.requires("milvus-common/1.0.0-78f2c29@milvus/dev#new")'])
        report = self.validate(storage, knowhere)
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

    def test_cardinal_requirement_is_selected_only_when_enabled(self):
        recipe = 'if self.options.with_cardinal:\n    self.requires("metis/5.2.1")\n'
        for enabled in (False, True):
            with self.subTest(with_cardinal=enabled):
                report = self.validate("", recipe, with_cardinal=enabled)
                references = selected_references(self.constraints, conan_os="Linux", with_cardinal=enabled)
                self.assertEqual(enabled, "metis" in report["requirements"]["knowhere"])
                self.assertEqual(enabled, "metis" in references)
                self.assertEqual(enabled, report["options"]["knowhere"]["with_cardinal"])
        del self.constraints["references"]["metis"]
        del self.constraints["feature_scope"]["metis"]
        self.validate("", recipe, with_cardinal=False)
        with self.assertRaisesRegex(ValueError, "missing from the unified graph: metis"):
            self.validate("", recipe, with_cardinal=True)

    def test_known_conditions_choose_one_version_and_keep_platform_requirements(self):
        recipe = '''if self.options.with_cardinal:
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
        for conan_os in ("Linux", "Macos"):
            report = self.validate("", recipe, conan_os=conan_os, with_cardinal=True)
            requirements = report["requirements"]["knowhere"]
            self.assertEqual("12.1.0", requirements["fmt"])
            self.assertEqual(conan_os == "Linux", "liburing" in requirements)
            self.assertEqual(conan_os == "Linux", "libunwind" in requirements)
            self.assertIn("opentelemetry-cpp", requirements)
        with self.assertRaisesRegex(ValueError, "newer upstream version for fmt: 11.2.0"):
            self.validate("", recipe, with_cardinal=False)

    def test_unknown_conditions_or_dynamic_requirements_fail_even_in_disabled_branch(self):
        for statement in ('if self.options.unknown:\n    self.requires("fmt/12.1.0")',
                          'if detect_feature():\n    self.requires("fmt/12.1.0")',
                          'self.requires(compute_reference())',
                          'self.add_requirements()',
                          'for ref in refs:\n    self.requires(ref)',
                          'if self.options.with_cardinal:\n    self.requires(compute_reference())'):
            with self.subTest(statement=statement), self.assertRaisesRegex(ValueError, "Unsupported upstream"):
                self.validate("", statement)

    def test_unknown_graph_feature_cannot_be_ignored(self):
        self.constraints["feature_scope"]["metis"] = "unknown"
        with self.assertRaisesRegex(ValueError, "Unknown dependency feature scope"):
            self.validate("", "")

    def test_cardinal_metis_and_its_gklib_dependency_use_shared_host_libraries(self):
        # Both upstream recipes default to static libraries; the host graph requires shared.
        profiles = Path(__file__).parent / "profiles"
        for platform in ("linux-x86_64", "linux-aarch64", "darwin-x86_64", "darwin-aarch64"):
            with self.subTest(platform=platform):
                section = ""
                options = {}
                for line in (profiles / platform).read_text().splitlines():
                    line = line.split("#", 1)[0].strip()
                    if line.startswith("["):
                        section = line
                    elif section == "[options]" and "=" in line:
                        key, value = line.split("=", 1)
                        options[key.strip()] = value.strip()
                self.assertEqual("True", options["metis/*:shared"])
                self.assertEqual("True", options["gklib/*:shared"])

    def test_cardinal_metis_uses_the_upstream_64bit_abi(self):
        def defaults(path):
            tree = ast.parse(path.read_text())
            recipe = next(node for node in tree.body if isinstance(node, ast.ClassDef))
            declaration = next(node for node in recipe.body if isinstance(node, ast.Assign)
                               and any(isinstance(target, ast.Name) and target.id == "default_options"
                                       for target in node.targets))
            return ast.literal_eval(declaration.value)

        native_build = Path(__file__).parent
        self.assertIs(True, defaults(native_build / "conanfile.py")["metis/*:with_64bit_types"])
        upstream = native_build.parents[1] / "knowhere/conanfile.py"
        if upstream.is_file():
            self.assertIs(True, defaults(upstream)["metis/*:with_64bit_types"])

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
                with self.subTest(conan_os=conan_os, with_cardinal=enabled):
                    observed = {}
                    def requires(reference, **keywords):
                        self.assertEqual({"force": True}, keywords)
                        observed[reference.split("/", 1)[0]] = reference
                    consumer = SimpleNamespace(recipe_folder=str(Path(__file__).parent),
                                               settings=SimpleNamespace(os=conan_os),
                                               options=SimpleNamespace(with_cardinal=enabled), requires=requires)
                    namespace["requirements"](consumer)
                    expected = selected_references(self.constraints, conan_os=conan_os, with_cardinal=enabled)
                    self.assertEqual(expected, observed)

    def test_initialized_engine_recipes_fit_the_unified_graph(self):
        repository = Path(__file__).resolve().parents[2]
        storage = repository / "milvus-storage/cpp/conanfile.py"
        knowhere = repository / "knowhere/conanfile.py"
        if not storage.is_file() or not knowhere.is_file():
            self.skipTest("Initialize milvus-storage and knowhere to validate their recipe pins")
        for conan_os in ("Linux", "Macos"):
            for enabled in (False, True):
                with self.subTest(conan_os=conan_os, with_cardinal=enabled):
                    report = self.validate(storage.read_text(), knowhere.read_text(),
                                           conan_os=conan_os, with_cardinal=enabled)
                    self.assertEqual("1.0.0-78f2c29", report["conflicts"]["milvus-common"]["selected"])
                    self.assertEqual(enabled, "metis" in report["requirements"]["knowhere"])


if __name__ == "__main__":
    unittest.main()
