"""Exercise the consumer-visible Maven graph and immutable release boundary."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import zipfile


SPEC = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[1] / "release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.inputs = self.root / "inputs"
        self.inputs.mkdir()
        self.output = self.root / "candidate"
        self.plan = json.loads(release.MATRIX.read_text())
        self.plan.update(version="2.0.0-rc.1", sourceSha="a" * 40,
                         pins={"milvus-storage": "b" * 40, "knowhere": "c" * 40, "milvus-proto": "d" * 40})
        self.plan_path = self.root / "plan.json"
        release.write_json(self.plan_path, self.plan)
        for native in self.plan["native"]:
            self.native(native["platform"])
        for jvm in self.plan["jvm"]:
            self.jvm(jvm["line"])

    @staticmethod
    def jar(path, entries):
        path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path, "w") as archive:
            for name, contents in entries.items():
                archive.writestr(name, contents)

    def native(self, platform, storage=None, library="libtest.so"):
        path = self.inputs / platform / f"milvus-native-{platform}.jar"
        provenance = json.dumps({"audit": "passed", "auditPolicy": "jvm-load"}).encode()
        manifest = {
            "format.version": "1", "platform": platform, "with_cardinal": "false",
            "storage.revision": storage or self.plan["pins"]["milvus-storage"],
            "knowhere.revision": self.plan["pins"]["knowhere"], "libraries": library,
            "sha256." + library: hashlib.sha256(b"native test payload").hexdigest(),
            "provenance.sha256": hashlib.sha256(provenance).hexdigest(),
        }
        prefix = f"native/milvus/1/{platform}/"
        self.jar(path, {prefix + library: b"native test payload",
                        prefix + "manifest.properties": "".join(f"{key}={value}\n" for key, value in manifest.items()),
                        "META-INF/milvus-native/provenance.json": provenance,
                        "META-INF/milvus-native/licenses/LICENSE": "fixture license"})
        Path(str(path) + ".properties").write_text(f"platform={platform}\njar.sha256={release.digest(path)}\n")
        release.write_json(path.parent / "abi.json", {
            "platform": platform, "jar.sha256": release.digest(path), "passed": True,
            "baseline": {"GLIBC": "2.35", "GLIBCXX": "3.4.30", "CXXABI": "1.3.13"}, "violations": []})
        return path

    def jvm(self, line, omit_api=False):
        artifact = f"spark-milvus-{line}-jvm_2.13"
        directory = self.inputs / line
        records = []
        for classifier in ("", "sources", "javadoc"):
            suffix = "-" + classifier if classifier else ""
            path = directory / f"{artifact}-{self.plan['version']}{suffix}.jar"
            contents = {"README.md": "fixture"}
            if not classifier:
                contents.update({
                    "io/milvus/storage/Api.class": b"class fixture",
                    "com/zilliz/spark/connector/Api.class": b"class fixture",
                    release.COMPATIBILITY: "format.version=1\nstorage.revision=" + "b" * 40 + "\nknowhere.revision=" + "c" * 40 + "\n",
                })
                if not omit_api:
                    contents["io/knowhere/Api.class"] = b"class fixture"
            self.jar(path, contents)
            records.append(dict(classifier=classifier, path=path.name, extension="jar", sha256=release.digest(path)))
        release.write_json(directory / "artifact.json", dict(
            artifactId=artifact, groupId="com.zilliz", version=self.plan["version"], sourceSha=self.plan["sourceSha"],
            sparkLine=line, files=records, providedDependencies=[
                dict(groupId="org.apache.spark", artifactId="spark-sql_2.13", version="4.0.0")], runtimeDependencies=[]))

    def stage(self):
        release.stage(self.plan_path, self.inputs, self.output)
        return release.verify(self.output)

    def rewrite_file(self, manifest, record, contents):
        path = self.output / "repository" / record["path"]
        path.write_bytes(contents)
        record.update(sha256=release.digest(path), size=path.stat().st_size)
        for algorithm in ("md5", "sha1", "sha256", "sha512"):
            Path(str(path) + "." + algorithm).write_text(release.digest(path, algorithm) + "\n")
        release.write_json(self.output / "release-manifest.json", manifest)

    def test_both_entries_share_exact_bottom_level_artifacts(self):
        manifest = self.stage()
        self.assertEqual(len(manifest["artifacts"]), 40)
        entries = [item for item in manifest["artifacts"] if item["kind"] == "entry" and item["extension"] == "pom"]
        self.assertEqual(len(entries), 6)
        self.assertEqual(len(release.entry_dependencies(self.plan, "4.0", None)), 3)
        self.assertEqual(len(release.entry_dependencies(self.plan, "4.0", "linux-x86_64")), 2)
        with self.assertRaisesRegex(ValueError, "never overwrite"):
            self.stage()

    def test_failed_platform_cannot_produce_partial_universal_entry(self):
        (self.inputs / "linux-aarch64" / "milvus-native-linux-aarch64.jar").unlink()
        with self.assertRaisesRegex(ValueError, "exactly one native bundle"):
            self.stage()
        self.assertFalse(self.output.exists())

    def test_nested_openssl_provider_is_a_valid_library(self):
        self.native("linux-x86_64", library="ossl-modules/legacy.so")
        self.stage()

    def test_source_pin_mismatch_is_rejected(self):
        self.native("linux-x86_64", storage="f" * 40)
        with self.assertRaisesRegex(ValueError, "source pin mismatch"):
            self.stage()

    def test_missing_unmanaged_api_is_rejected(self):
        self.jvm("4.0", omit_api=True)
        with self.assertRaisesRegex(ValueError, "missing API classes"):
            self.stage()

    def test_jvm_from_another_commit_is_rejected(self):
        path = self.inputs / "4.0" / "artifact.json"
        metadata = json.loads(path.read_text())
        metadata["sourceSha"] = "f" * 40
        release.write_json(path, metadata)
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.stage()

    def test_abi_report_cannot_be_reused_for_another_bundle(self):
        path = self.inputs / "linux-x86_64" / "abi.json"
        report = json.loads(path.read_text())
        report["jar.sha256"] = "0" * 64
        release.write_json(path, report)
        with self.assertRaisesRegex(ValueError, "ABI evidence"):
            self.stage()

    def test_abi_report_must_use_declared_baseline(self):
        path = self.inputs / "linux-x86_64" / "abi.json"
        report = json.loads(path.read_text())
        report["baseline"]["GLIBC"] = "2.39"
        release.write_json(path, report)
        with self.assertRaisesRegex(ValueError, "different baseline"):
            self.stage()

    def test_changed_candidate_bytes_are_rejected(self):
        manifest = self.stage()
        path = self.output / "repository" / manifest["artifacts"][0]["path"]
        path.write_bytes(b"replaced artifact")
        with self.assertRaisesRegex(ValueError, "Changed candidate"):
            release.verify(self.output)

    def test_platform_entry_cannot_transitively_pull_universal_entry(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["kind"] == "entry" and item["platform"] and item["extension"] == "pom")
        wrong = release.pom_bytes(record["artifactId"], self.plan, [dict(
            groupId="com.zilliz", artifactId=f"spark-milvus-{record['line']}_2.13", version=self.plan["version"])])
        self.rewrite_file(manifest, record, wrong)
        with self.assertRaisesRegex(ValueError, "Wrong entry dependency graph"):
            release.verify(self.output)

    def test_unpublished_internal_dependency_is_rejected(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"] if item["kind"] == "jvm" and item["extension"] == "pom")
        wrong = release.pom_bytes(record["artifactId"], self.plan, [dict(
            groupId="com.zilliz", artifactId="native-vector_2.13", version=self.plan["version"])])
        self.rewrite_file(manifest, record, wrong)
        with self.assertRaisesRegex(ValueError, "unpublished internal"):
            release.verify(self.output)

    def test_untracked_candidate_file_is_rejected(self):
        self.stage()
        (self.output / "repository" / "extra.jar").write_bytes(b"surprise")
        with self.assertRaisesRegex(ValueError, "untracked files"):
            release.verify(self.output)

    def test_snapshot_and_malformed_versions_are_rejected(self):
        for version in ("2.0.0-SNAPSHOT", "2.0.0-snapshot", "2.0.0/escape", "1.6.1", "2.0.0-"):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "immutable 2.x"):
                release.validate_plan(dict(self.plan, version=version))

    def test_central_failure_does_not_proceed(self):
        with mock.patch.object(release, "central_request", return_value=b'{"deploymentState":"FAILED","errors":{"pom":["bad"]}}'):
            with self.assertRaisesRegex(ValueError, "validation failed"):
                release.await_central("id", "not-a-token", "VALIDATED")

    def test_candidate_identity_uses_checked_in_gitlinks(self):
        output = self.root / "actual-plan.json"
        release.create_plan("2.0.0-rc.1", output)
        plan = json.loads(output.read_text())
        self.assertEqual(len(plan["consumer"]), 8)
        self.assertEqual({row["os_version"] for row in plan["consumer"]}, {"ubuntu-22.04", "ubuntu-24.04"})
        self.assertEqual(set(plan["pins"]), {"milvus-proto", "milvus-storage", "knowhere"})


class SigningKeyTest(unittest.TestCase):
    def test_armored_and_base64_keys_are_sent_only_through_stdin(self):
        armored = b"-----BEGIN PGP PRIVATE KEY BLOCK-----\nfixture\n-----END PGP PRIVATE KEY BLOCK-----"
        for value in (armored.decode(), release.base64.b64encode(armored).decode()):
            with self.subTest(encoded=value != armored.decode()):
                with mock.patch.dict(release.os.environ, {"GPG_PRIVATE_KEY": value}):
                    with mock.patch.object(release.subprocess, "run") as run:
                        release.import_key()
                        run.assert_called_once_with(["gpg", "--batch", "--import"], input=armored, check=True)

    def test_invalid_or_public_keys_are_rejected_before_import(self):
        for value in ("", "not-base64!", release.base64.b64encode(b"public key").decode()):
            with self.subTest(value=value), mock.patch.dict(release.os.environ, {"GPG_PRIVATE_KEY": value}):
                with mock.patch.object(release.subprocess, "run") as run:
                    with self.assertRaises(ValueError):
                        release.import_key()
                    run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
