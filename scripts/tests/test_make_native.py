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
                for name in ("check-deps", "init-missing-submodules", "build-milvus-storage",
                             "native-build", "native-bundle", "copy-native-libs")
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
                self.assertNotIn("selected:copy-native-libs", output)
                self.assertIn("sbt-arg:-Dmilvus.native.bundle=", output)
                self.assertIn("sbt-arg:package", output)

    def test_both_operating_systems_have_a_profiled_platform(self):
        # The selection rule is the profile directory, so a platform is added by
        # adding its profile and its platforms.py adapter, not by editing Make.
        self.assertIn("linux-x86_64", self.PROFILED)
        self.assertIn("darwin-aarch64", self.PROFILED)

    def test_a_platform_without_a_profile_keeps_the_storage_build(self):
        for system, architecture in (("Linux", "aarch64"), ("Darwin", "x86_64")):
            with self.subTest(system=system, architecture=architecture):
                self.assertNotIn(system.lower() + "-" + architecture, self.PROFILED)
                output = self.run_make(system, architecture)
                self.assertLess(output.index("selected:build-milvus-storage"),
                                output.index("selected:copy-native-libs"))
                self.assertIn("selected:copy-native-libs", output)
                self.assertNotIn("selected:native-build", output)
                self.assertNotIn("selected:native-bundle", output)
                self.assertNotIn("-Dmilvus.native.bundle=", output)
                self.assertIn("sbt-arg:package", output)

    def test_an_explicit_bundle_skips_the_source_build_on_every_platform(self):
        for system, architecture in (("Linux", "x86_64"), ("Linux", "aarch64"),
                                     ("Darwin", "arm64"), ("Darwin", "x86_64")):
            with self.subTest(system=system, architecture=architecture):
                output = self.run_make(system, architecture, "/chosen/bundle.jar")
                self.assertIn("selected:native-bundle", output)
                self.assertNotIn("selected:native-build", output)
                self.assertNotIn("selected:copy-native-libs", output)
                self.assertNotIn("selected:build-milvus-storage", output)
                self.assertIn("sbt-arg:-Dmilvus.native.bundle=/chosen/bundle.jar", output)

    def test_docker_resource_target_uses_same_platform_selection(self):
        for architecture, expected in (("x86_64", "native-bundle"), ("aarch64", "copy-native-libs")):
            with self.subTest(architecture=architecture):
                output = self.run_make("Linux", architecture, target="native-resources")
                self.assertIn("selected:" + expected, output)
                self.assertNotIn("sbt-arg:", output)

    def test_bundle_paths_with_spaces_remain_one_sbt_argument(self):
        for bundle in ("/tmp/chosen bundles/bundle.jar", "chosen bundles/bundle.jar"):
            with self.subTest(bundle=bundle):
                output = self.run_make("Linux", "x86_64", bundle)
                expected = bundle if bundle.startswith("/") else str(ROOT / bundle)
                arguments = [line for line in output.splitlines() if line.startswith("sbt-arg:")]
                self.assertEqual(["sbt-arg:-Dmilvus.native.bundle=" + expected, "sbt-arg:package"], arguments)

    def test_legacy_resources_rebuild_existing_libraries_before_copying(self):
        # Mach-O without a build profile: the dylib names and the Darwin branch
        # of the resource copy, on the platform that still takes that path.
        with tempfile.TemporaryDirectory(prefix="native-make-freshness-") as temporary:
            directory = Path(temporary)
            engine = directory / "libmilvus-storage.dylib"
            jni = directory / "libmilvus-storage-jni.dylib"
            for library in (engine, jni):
                library.write_text("old library")
            dependencies = directory / "dependencies"
            dependencies.mkdir()
            resources = directory / "resources"
            override = directory / "override.mk"
            override.write_text(
                "check-deps init-missing-submodules:\n\t@true\n"
                "build-milvus-storage:\n"
                '\t@printf "fresh engine" > "$(STORAGE_LIB)"\n'
                '\t@printf "fresh JNI" > "$(STORAGE_JNI_LIB)"\n'
            )
            subprocess.run(
                ["make", "--no-print-directory", "-j4", "-f", str(ROOT / "Makefile"),
                 "-f", str(override), "native-resources", "UNAME_S=Darwin", "UNAME_M=x86_64",
                 "NATIVE_BUNDLE=", "STORAGE_LIB=" + str(engine), "STORAGE_JNI_LIB=" + str(jni),
                 "MILVUS_STORAGE_DEPS=" + str(dependencies), "NATIVE_DIR=" + str(resources)],
                cwd=ROOT, text=True, capture_output=True, check=True,
            )
            self.assertEqual("fresh engine", (resources / engine.name).read_text())
            self.assertEqual("fresh JNI", (resources / jni.name).read_text())


if __name__ == "__main__":
    unittest.main()
