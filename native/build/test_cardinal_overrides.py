"""The --cardinal-version override replaces one generation's Knowhere pin."""
import importlib.util
from pathlib import Path
import unittest


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


build = module("build")


class CardinalOverridesTest(unittest.TestCase):
    def test_tag_and_commit_refs_are_accepted_per_generation(self):
        self.assertEqual(build.cardinal_overrides(["v1=v2.5.113", "v2=" + "a" * 40]),
                         {"v1": "v2.5.113", "v2": "a" * 40})
        self.assertEqual(build.cardinal_overrides(None), {})

    def test_unknown_generation_or_malformed_value_fails(self):
        for value in ("v3=v1.0.0", "v1", "v1=", "v1=has space", "=v1.0.0"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "cardinal-version"):
                build.cardinal_overrides([value])

    def test_duplicate_generation_fails(self):
        with self.assertRaisesRegex(ValueError, "twice"):
            build.cardinal_overrides(["v1=a", "v1=b"])

    def test_override_replaces_only_the_named_generation(self):
        versions = {"v1": "v2.5.112", "v2": "fd184100"}
        versions.update(build.cardinal_overrides(["v2=" + "b" * 40]))
        self.assertEqual(versions, {"v1": "v2.5.112", "v2": "b" * 40})


if __name__ == "__main__":
    unittest.main()
