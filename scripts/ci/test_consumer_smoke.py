"""Consumer gates reject missing, substituted and excess native artifacts."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile


SPEC = importlib.util.spec_from_file_location("consumer_smoke", Path(__file__).with_name("consumer_smoke.py"))
consumer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(consumer)


class ConsumerTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.ivy = self.root / "ivy"
        (self.ivy / "jars").mkdir(parents=True)

    def jar(self, artifact, platform=None):
        path = self.ivy / "jars" / f"com.zilliz_{artifact}-2.0.0.jar"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("META-INF/identity", artifact)
            if platform:
                archive.writestr(f"native/milvus/1/{platform}/manifest.properties", "format.version=1")
        return {"artifactId": artifact, "extension": "jar", "classifier": None,
                "sha256": consumer.digest(path), "path": path.name}

    def manifest(self):
        return {
            "plan": {"native": [{"platform": "linux-x86_64"}, {"platform": "linux-aarch64"}]},
            "artifacts": [
                self.jar("spark-milvus-4.0_2.13"),
                self.jar("spark-milvus-4.0-linux-x86_64_2.13"),
                self.jar("spark-milvus-4.0-jvm_2.13"),
                self.jar("spark-milvus-native-linux-x86_64", "linux-x86_64"),
                self.jar("spark-milvus-native-linux-aarch64", "linux-aarch64"),
            ],
        }

    def keep(self, expected):
        for path in (self.ivy / "jars").glob("*.jar"):
            if not any(path.name == f"com.zilliz_{name}-2.0.0.jar" for name in expected):
                path.unlink()

    def test_universal_and_platform_have_distinct_native_closures(self):
        manifest = self.manifest()
        _, universal = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "universal")
        self.assertEqual(len(universal), 4)
        _, platform = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.assertEqual(len(platform), 3)
        self.keep(platform)
        self.assertEqual(len(consumer.check_resolved(self.ivy, platform)), 3)

    def test_substituted_candidate_jar_fails(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        target = self.ivy / "jars/com.zilliz_spark-milvus-4.0-jvm_2.13-2.0.0.jar"
        with zipfile.ZipFile(target, "a") as archive:
            archive.writestr("unverified-production.class", b"different")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_platform_entry_must_not_pull_other_architecture(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        self.jar("spark-milvus-native-linux-aarch64", "linux-aarch64")
        with self.assertRaisesRegex(RuntimeError, "different connector artifacts"):
            consumer.check_resolved(self.ivy, expected)

    def test_missing_matrix_platform_fails_before_resolution(self):
        manifest = self.manifest()
        manifest["artifacts"].pop()
        with self.assertRaisesRegex(ValueError, "missing artifacts"):
            consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "universal")

    def test_native_metadata_must_match_the_resolved_coordinate(self):
        manifest = self.manifest()
        manifest["artifacts"][3] = self.jar("spark-milvus-native-linux-x86_64", "linux-aarch64")
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)

    def test_other_groups_cannot_introduce_an_extra_native_platform(self):
        manifest = self.manifest()
        _, expected = consumer.expected_artifacts(manifest, "4.0", "linux-x86_64", "platform")
        self.keep(expected)
        with zipfile.ZipFile(self.ivy / "jars/other.group_bad.jar", "w") as archive:
            archive.writestr("native/milvus/1/linux-aarch64/manifest.properties", "format.version=1")
        with self.assertRaisesRegex(RuntimeError, "Resolved native platforms"):
            consumer.check_resolved(self.ivy, expected)


if __name__ == "__main__":
    unittest.main()
