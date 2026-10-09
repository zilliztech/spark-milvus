"""Exercise public Make targets without invoking native builds or sbt."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class NativePlatformMakeTest(unittest.TestCase):
    def run_make(self, system, architecture, bundle="", target="package"):
        with tempfile.TemporaryDirectory(prefix="native-make-test-") as temporary:
            directory = Path(temporary)
            override = directory / "override.mk"
            # Replace only external work. Make still resolves the actual public
            # prerequisite graph and constructs the actual sbt command.
            override.write_text("\n".join(
                name + ":\n\t@echo selected:" + name
                for name in ("init-missing-submodules", "native-build", "native-bundle")
            ) + "\n")
            sbt = directory / "sbt"
            sbt.write_text('#!/bin/sh\nprintf "sbt-arg:%s\\n" "$@"\n')
            sbt.chmod(0o755)
            return subprocess.run(
                ["make", "--no-print-directory", "-f", str(ROOT / "Makefile"),
                 "-f", str(override), target, "UNAME_S=" + system,
                 "UNAME_M=" + architecture, "NATIVE_BUNDLE=" + bundle,
                 "NATIVE_WORK_DIR=" + str(directory / "work"),
                 "SBT=" + str(sbt), "JAVA_HOME=" + os.environ.get("JAVA_HOME", "/unused/jdk")],
                cwd=ROOT, text=True, capture_output=True, check=True,
            ).stdout

    #: A platform builds both engines when native-build carries its profile.
    PROFILED = sorted(path.name for path in (ROOT / "native-build" / "profiles").iterdir())

    def test_a_profiled_platform_builds_unified_resources_and_passes_the_bundle_to_sbt(self):
        for platform in self.PROFILED:
            system, architecture = platform.split("-", 1)
            with self.subTest(platform=platform):
                output = self.run_make(system.capitalize(), architecture)
                self.assertIn("selected:native-build", output)
                self.assertIn("selected:native-bundle", output)
                self.assertIn("sbt-arg:-Dmilvus.native.bundle=", output)
                self.assertIn("sbt-arg:package", output)

    def test_every_platform_has_a_profile(self):
        # The selection rule is the profile directory, so a platform is added by
        # adding its profile and its platforms.py adapter, not by editing Make.
        for platform in ("linux-x86_64", "linux-aarch64", "darwin-x86_64", "darwin-aarch64"):
            self.assertIn(platform, self.PROFILED)

    def test_an_explicit_bundle_skips_the_source_build_on_every_platform(self):
        for system, architecture in (("Linux", "x86_64"), ("Linux", "aarch64"),
                                     ("Darwin", "arm64"), ("Darwin", "x86_64")):
            with self.subTest(system=system, architecture=architecture):
                output = self.run_make(system, architecture, "/chosen/bundle.jar")
                self.assertIn("selected:native-bundle", output)
                self.assertNotIn("selected:native-build", output)
                self.assertIn("sbt-arg:-Dmilvus.native.bundle=/chosen/bundle.jar", output)

    def test_docker_resource_target_builds_the_unified_bundle(self):
        for architecture in ("x86_64", "aarch64"):
            with self.subTest(architecture=architecture):
                output = self.run_make("Linux", architecture, target="native-resources")
                self.assertIn("selected:native-bundle", output)
                self.assertNotIn("sbt-arg:", output)

    def test_bundle_paths_with_spaces_remain_one_sbt_argument(self):
        for bundle in ("/tmp/chosen bundles/bundle.jar", "chosen bundles/bundle.jar"):
            with self.subTest(bundle=bundle):
                output = self.run_make("Linux", "x86_64", bundle)
                expected = bundle if bundle.startswith("/") else str(ROOT / bundle)
                arguments = [line for line in output.splitlines() if line.startswith("sbt-arg:")]
                self.assertEqual(["sbt-arg:-Dmilvus.native.bundle=" + expected, "sbt-arg:package"], arguments)



if __name__ == "__main__":
    unittest.main()
