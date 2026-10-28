"""Every consumer of the pinned native toolchain reads or repeats the same versions."""

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")


class ToolchainPinTest(unittest.TestCase):
    def setUp(self):
        self.pins = json.loads((ROOT / "native/build/toolchain.json").read_text(encoding="utf-8"))

    def test_pins_are_exact_patch_versions(self):
        self.assertEqual(self.pins["format"], 1)
        for name in ("rust", "java"):
            self.assertIsNotNone(VERSION.fullmatch(self.pins[name]), name)

    def test_dockerfile_defaults_to_the_pinned_rust(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("ARG RUST_TOOLCHAIN=" + self.pins["rust"] + "\n", dockerfile)
        self.assertIn('--default-toolchain "${RUST_TOOLCHAIN}"', dockerfile)
        self.assertNotIn("--default-toolchain stable", dockerfile)

    def test_runner_preparation_reads_the_pin_file(self):
        script = (ROOT / "scripts/ci/prepare_native.sh").read_text(encoding="utf-8")
        self.assertIn("native/build/toolchain.json", script)
        self.assertIn('rustup toolchain install "$rust_version" --profile minimal --no-self-update', script)
        self.assertIn('rustup default "$rust_version"', script)
        self.assertIn('"${JAVA_HOME:?}/bin/java" -version', script)
        # No literal version may bypass the pin file.
        for name in ("rust", "java"):
            self.assertNotIn(self.pins[name], script, name)


if __name__ == "__main__":
    unittest.main()
