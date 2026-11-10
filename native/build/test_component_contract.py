"""Keep external components additive while allowing reviewed shared versions."""
import copy
import json
from pathlib import Path
import unittest

from component_contract import merge

ROOT = Path(__file__).resolve().parent


class ComponentContractTest(unittest.TestCase):
    def setUp(self):
        self.constraints = json.loads((ROOT / "dependencies.json").read_text())
        self.profile = (ROOT / "profiles/linux-x86_64").read_text()
        self.component = {"name": "compute", "sourceDirectory": "/sources/compute",
                          "cmakeVariables": {"COMPUTE_SOURCE_DIR": "/sources/compute"},
                          "sourceLicenseDirectories": {"compute": {
                              "directory": "/sources/compute", "files": ["LICENSE"]}}}
        self.extension = {"components": [self.component]}

    def test_shared_upstream_version_merge_preserves_storage_inputs(self):
        self.extension["references"] = {"fmt": "fmt/12.1.0#" + "b" * 32}
        self.extension["conanOptions"] = {"new-package/*:shared": True}
        merged = merge(self.constraints, self.extension, self.profile)
        self.assertEqual(self.extension["references"]["fmt"], merged["references"]["fmt"])
        self.assertEqual(self.constraints["references"]["arrow"], merged["references"]["arrow"])
        self.assertEqual(self.constraints["transitive_references"], merged["transitive_references"])
        self.assertNotEqual(merged["references"]["fmt"], self.constraints["references"]["fmt"])

    def test_extension_cannot_replace_storage_source_or_toolchain(self):
        for variable in ("MILVUS_STORAGE_SOURCE_DIR", "CMAKE_CXX_COMPILER", "CMAKE_PROJECT_INCLUDE"):
            with self.subTest(variable=variable):
                self.component["cmakeVariables"] = {variable: "/other"}
                with self.assertRaisesRegex(ValueError, "driver CMake"):
                    merge(self.constraints, self.extension, self.profile)

    def test_extension_cannot_replace_storage_license_or_empty_its_own(self):
        for licenses in ({"storage": {"directory": "/sources/compute", "files": ["LICENSE"]}}, {}, []):
            with self.subTest(licenses=licenses):
                self.component["sourceLicenseDirectories"] = licenses
                with self.assertRaisesRegex(ValueError, "licenses"):
                    merge(self.constraints, self.extension, self.profile)

    def test_extension_cannot_change_storage_dependency_options_or_conf(self):
        for field, key in (("conanOptions", "arrow/*:shared"), ("conanConf", "folly/*:tools.build:cxxflags"),
                           ("conanOptions", "bzip2/*:shared"), ("conanOptions", "*/*:shared")):
            with self.subTest(key=key):
                extension = copy.deepcopy(self.extension)
                extension[field] = {key: False}
                with self.assertRaisesRegex(ValueError, "Storage dependency|exact package owner"):
                    merge(self.constraints, extension, self.profile)

    def test_same_version_recipe_replacement_and_storage_policy_are_rejected(self):
        for field, value in (("references", {"folly": self.constraints["references"]["folly"].split("#")[0] + "#" + "c" * 32}),
                             ("platformScope", {"arrow": ["Macos"]}),
                             ("versionPolicy", {"excluded_requirements": {"arrow": "extension disabled it"}})):
            with self.subTest(field=field):
                extension = copy.deepcopy(self.extension)
                extension[field] = value
                with self.assertRaises(ValueError):
                    merge(self.constraints, extension, self.profile)


if __name__ == "__main__":
    unittest.main()
