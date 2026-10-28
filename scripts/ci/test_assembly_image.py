"""The release image packages only the assembly built for the tagged release."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from zipfile import ZipFile


SCRIPT = Path(__file__).with_name("assembly_image.py")
SPEC = importlib.util.spec_from_file_location("assembly_image", SCRIPT)
image = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(image)

SOURCE = "0123456789abcdef0123456789abcdef01234567"


class AssemblyImageTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.version = "2.0.0-rc.3"
        self.platform = "linux-x86_64"
        self.plan = self.root / "plan.json"
        self.plan.write_text(json.dumps({"version": self.version, "sourceSha": SOURCE}))
        self.native = self.root / "milvus-native-linux-x86_64.jar"
        self.native.write_bytes(b"native")
        self.assembly = self.root / image.jar_name(self.version)
        self.write_assembly(self.assembly, self.platform)
        self.output = self.root / "staged"

    def write_assembly(self, path, platform, alias=True, plugin=True):
        prefix = f"native/milvus/1/{platform}/"
        with ZipFile(path, "w") as archive:
            if plugin:
                archive.writestr(image.PLUGIN, b"class")
            manifest = f"format.version=1\nplatform={platform}\n"
            if alias:
                manifest += "alias.libknowhere_jni.so=libknowhere_jni-avx2.so\n"
                archive.writestr(prefix + "libknowhere_jni-avx2.so", b"knowhere")
            else:
                archive.writestr(prefix + "libknowhere_jni.so", b"knowhere")
            archive.writestr(prefix + "manifest.properties", manifest)
            archive.writestr(prefix + "libmilvus-storage-jni.so", b"storage")

    def run_main(self, *argv):
        stderr = io.StringIO()
        with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
            code = image.main(list(argv))
        return code, stderr.getvalue()

    def stage_arguments(self, **overrides):
        values = {"--assembly": self.assembly, "--plan": self.plan, "--platform": self.platform,
                  "--native-bundle": self.native, "--output": self.output}
        values.update(overrides)
        return ["stage", *(str(part) for pair in values.items() for part in pair)]

    def test_stage_records_plan_identity_and_checksums(self):
        code, errors = self.run_main(*self.stage_arguments())
        self.assertEqual((code, errors), (0, ""))
        manifest = json.loads((self.output / image.MANIFEST).read_text())
        self.assertEqual(manifest, {
            "schemaVersion": 1, "version": self.version, "sourceSha": SOURCE, "sparkLine": "4.0",
            "platform": self.platform, "dockerPlatform": "linux/amd64", "architecture": "amd64",
            "jar": self.assembly.name,
            "sha256": hashlib.sha256(self.assembly.read_bytes()).hexdigest(),
            "nativeBundleSha256": hashlib.sha256(b"native").hexdigest(),
        })
        self.assertEqual((self.output / self.assembly.name).read_bytes(), self.assembly.read_bytes())

    def test_stage_rejects_an_assembly_of_another_version_or_platform(self):
        other = self.root / image.jar_name("2.0.0")
        self.write_assembly(other, self.platform)
        code, errors = self.run_main(*self.stage_arguments(**{"--assembly": other}))
        self.assertEqual(code, 1)
        self.assertIn("does not carry release version", errors)
        code, errors = self.run_main(*self.stage_arguments(**{"--platform": "linux-aarch64"}))
        self.assertEqual(code, 1)
        self.assertIn("lacks native resources for linux-aarch64", errors)
        code, errors = self.run_main(*self.stage_arguments(**{"--platform": "darwin-aarch64"}))
        self.assertEqual(code, 1)
        self.assertIn("Image platform must be one of", errors)
        self.assertFalse(self.output.exists())

    def test_stage_requires_plugin_and_both_native_libraries(self):
        self.write_assembly(self.assembly, self.platform, plugin=False)
        code, errors = self.run_main(*self.stage_arguments())
        self.assertEqual(code, 1)
        self.assertIn("MilvusSparkPlugin", errors)
        with ZipFile(self.assembly, "w") as archive:
            archive.writestr(image.PLUGIN, b"class")
            archive.writestr(f"native/milvus/1/{self.platform}/manifest.properties",
                             f"format.version=1\nplatform={self.platform}\n")
            archive.writestr(f"native/milvus/1/{self.platform}/libknowhere_jni.so", b"knowhere")
        code, errors = self.run_main(*self.stage_arguments())
        self.assertEqual(code, 1)
        self.assertIn("lacks native library libmilvus-storage-jni.so", errors)

    def test_stage_rejects_snapshot_plans_and_missing_sources(self):
        self.plan.write_text(json.dumps({"version": "2.0.0-SNAPSHOT", "sourceSha": SOURCE}))
        code, errors = self.run_main(*self.stage_arguments())
        self.assertEqual(code, 1)
        self.assertIn("explicit release version", errors)
        self.plan.write_text(json.dumps({"version": self.version, "sourceSha": "abc"}))
        code, errors = self.run_main(*self.stage_arguments())
        self.assertEqual(code, 1)
        self.assertIn("source commit", errors)

    def test_verify_exports_tags_only_for_the_tagged_release(self):
        self.assertEqual(self.run_main(*self.stage_arguments())[0], 0)
        github_output = self.root / "output.txt"
        code, errors = self.run_main("verify", "--input", str(self.output), "--platform", self.platform,
                                     "--version", self.version, "--source-sha", SOURCE,
                                     "--github-output", str(github_output))
        self.assertEqual((code, errors), (0, ""))
        outputs = dict(line.split("=", 1) for line in github_output.read_text().splitlines())
        self.assertEqual(outputs, {
            "assembly": str((self.output / self.assembly.name).resolve()),
            "docker_platform": "linux/amd64",
            "image_tag": "2.0.0-rc.3-spark4.0-amd64",
            "manifest_tag": "2.0.0-rc.3-spark4.0",
        })
        for version, source, message in (
            ("2.0.0", SOURCE, "version differs"),
            (self.version, "f" * 40, "sourceSha differs"),
        ):
            code, errors = self.run_main("verify", "--input", str(self.output), "--platform",
                                         self.platform, "--version", version, "--source-sha", source)
            self.assertEqual(code, 1, (version, source))
            self.assertIn(message, errors)
        code, errors = self.run_main("verify", "--input", str(self.output), "--platform",
                                     "linux-aarch64", "--version", self.version, "--source-sha", SOURCE)
        self.assertEqual(code, 1)
        self.assertIn("platform differs", errors)

    def test_verify_rejects_a_replaced_assembly(self):
        self.assertEqual(self.run_main(*self.stage_arguments())[0], 0)
        staged = self.output / self.assembly.name
        self.write_assembly(staged, self.platform, alias=False)
        code, errors = self.run_main("verify", "--input", str(self.output), "--platform", self.platform,
                                     "--version", self.version, "--source-sha", SOURCE)
        self.assertEqual(code, 1)
        self.assertIn("checksum differs", errors)
        manifest = json.loads((self.output / image.MANIFEST).read_text())
        manifest["sha256"] = hashlib.sha256(staged.read_bytes()).hexdigest()
        manifest["jar"] = "other.jar"
        (self.output / image.MANIFEST).write_text(json.dumps(manifest))
        code, errors = self.run_main("verify", "--input", str(self.output), "--platform", self.platform,
                                     "--version", self.version, "--source-sha", SOURCE)
        self.assertEqual(code, 1)
        self.assertIn("jar differs", errors)
        (self.output / image.MANIFEST).write_text("[]")
        code, errors = self.run_main("verify", "--input", str(self.output), "--platform", self.platform,
                                     "--version", self.version, "--source-sha", SOURCE)
        self.assertEqual(code, 1)
        self.assertIn("must be a JSON object", errors)

    def test_aarch64_maps_to_arm64_docker_platform(self):
        platform = "linux-aarch64"
        native = self.root / "milvus-native-linux-aarch64.jar"
        native.write_bytes(b"arm native")
        self.write_assembly(self.assembly, platform)
        code, errors = self.run_main(*self.stage_arguments(**{"--platform": platform,
                                                              "--native-bundle": native}))
        self.assertEqual((code, errors), (0, ""))
        values = image.verify(self.output, platform, self.version, SOURCE)
        self.assertEqual(values["docker_platform"], "linux/arm64")
        self.assertEqual(values["image_tag"], "2.0.0-rc.3-spark4.0-arm64")


if __name__ == "__main__":
    unittest.main()
