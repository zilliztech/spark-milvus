"""Exercise component test selection through CTest's actual regex dialect."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import build


class ComponentTests(unittest.TestCase):
    def test_ctest_discovers_exact_component_test_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = ["engine.c_api", "engine+scan"]
            source = "cmake_minimum_required(VERSION 3.16)\nproject(ComponentTests NONE)\nenable_testing()\n"
            for name in names + ["engineXc_api", "other_engine.c_api"]:
                source += 'add_test(NAME "' + name + '" COMMAND "${CMAKE_COMMAND}" -E true)\n'
            (root / "CMakeLists.txt").write_text(source)
            subprocess.run(["cmake", "-S", str(root), "-B", str(root / "build")],
                           check=True, capture_output=True, text=True)
            discovery = subprocess.check_output(
                ["ctest", "--test-dir", str(root / "build"), "--show-only=json-v1",
                 "-R", build.ctest_pattern(names)], text=True)
            self.assertEqual(sorted(test["name"] for test in json.loads(discovery)["tests"]), sorted(names))


if __name__ == "__main__":
    unittest.main()
