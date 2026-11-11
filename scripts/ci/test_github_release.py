"""A release exposes the accepted bytes, including after an interrupted upload."""

import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from contextlib import redirect_stdout
from unittest import mock
from zipfile import ZipFile

import assembly_image
import github_release


SPEC = importlib.util.spec_from_file_location("release_fixtures",
                                            Path(__file__).resolve().parents[1] / "tests/test_release.py")
fixtures = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixtures)


class FakeGitHub:
    def __init__(self, source, annotated=False):
        self.source, self.annotated = source, annotated
        self.remote = None
        self.other_releases = []
        self.files = {}
        self.starters = {}
        self.mutations = []
        self.fail_after = None

    def api(self, path, method="GET", data=None):
        if path.startswith("git/ref/tags/"):
            return {"object": {"type": "tag" if self.annotated else "commit", "sha": self.source}}
        if path.startswith("git/tags/"):
            return {"object": {"type": "commit", "sha": self.source}}
        if path.startswith("releases/tags/"):
            return copy.deepcopy(self.remote) if self.remote and not self.remote["draft"] else None
        if path.startswith("releases?per_page=100&page="):
            page = int(path.rsplit("=", 1)[1])
            rows = self.other_releases + ([self.remote] if self.remote else [])
            return copy.deepcopy(rows[(page - 1) * 100:page * 100])
        self.mutations.append((method, path))
        if path == "releases" and method == "POST":
            if self.remote is not None:
                raise AssertionError("must resume the existing draft")
            self.remote = dict(data, id=7, html_url="https://github.com/zilliztech/spark-milvus/releases/tag/" + data["tag_name"])
        elif path == "releases/7" and method == "PATCH":
            self.remote.update(data)
        elif path.startswith("releases/assets/") and method == "DELETE":
            asset_id = int(path.rsplit("/", 1)[1])
            name = next(name for name, item in self.starters.items() if item["id"] == asset_id)
            del self.starters[name]
            return None
        else:
            raise AssertionError((path, method, data))
        return copy.deepcopy(self.remote)

    def assets(self, release_id):
        return ([{"id": index, "name": name, "state": "uploaded", "size": len(self.files[name])}
                 for index, name in enumerate(sorted(self.files))] + list(self.starters.values()))

    def upload(self, tag, path):
        if self.fail_after is not None and len(self.files) >= self.fail_after:
            raise RuntimeError("interrupted upload")
        if path.name in self.files or path.name in self.starters:
            raise AssertionError("must never overwrite an asset")
        self.mutations.append(("UPLOAD", path.name))
        self.files[path.name] = path.read_bytes()

    def digest(self, asset):
        return hashlib.sha256(self.files[asset["name"]]).hexdigest()


class GitHubReleaseTest(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.ReleaseTest("runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.version, self.source = fixture.plan["version"], fixture.plan["sourceSha"]
        self.assemblies = fixture.root / "assemblies"
        for platform in github_release.PLATFORMS:
            native = fixture.native(platform, library="libmilvus-storage-jni.so")
            jar = fixture.root / assembly_image.jar_name(self.version)
            with ZipFile(native) as source, ZipFile(jar, "w") as target:
                for name in source.namelist():
                    target.writestr(name, source.read(name))
                target.writestr(assembly_image.PROVIDER, b"provider")
            assembly_image.stage(jar, fixture.plan_path, platform, native,
                                 self.assemblies / ("assembly-" + platform))
        fixture.stage()
        self.output = fixture.root / "downloads"
        self.notes = fixture.root / "notes.md"
        self.notes.write_text("# Spark-Milvus {{VERSION}}\n\nRelease notes.\n")
        self.client = FakeGitHub(self.source)

    def prepare(self):
        return github_release.prepare(self.fixture.output, self.assemblies, self.output, self.version, self.source)

    def publish(self):
        with redirect_stdout(io.StringIO()):
            github_release.publish(self.output, self.version, self.source, self.notes, self.client)

    def test_preparation_preserves_candidate_and_both_assembly_bytes(self):
        paths = self.prepare()
        self.assertEqual(set(paths), github_release.asset_names(self.version))
        self.assertEqual(paths["release-manifest.json"].read_bytes(),
                         (self.fixture.output / "release-manifest.json").read_bytes())
        for platform in github_release.PLATFORMS:
            original = self.assemblies / ("assembly-" + platform)
            self.assertEqual(paths[github_release.asset_name(self.version, platform)].read_bytes(),
                             (original / assembly_image.jar_name(self.version)).read_bytes())
            metadata = json.loads(paths[f"assembly-manifest-{platform}.json"].read_text())
            original_metadata = json.loads((original / assembly_image.MANIFEST).read_text())
            self.assertEqual(metadata, dict(original_metadata, jar=github_release.asset_name(self.version, platform)))
            self.assertEqual(github_release.release.digest(self.output / metadata["jar"]), metadata["sha256"])
        self.assertEqual(len(paths["SHA256SUMS"].read_text().splitlines()), 5)

    def test_preparation_rejects_candidate_of_another_commit(self):
        with self.assertRaisesRegex(ValueError, "candidate differs"):
            github_release.prepare(self.fixture.output, self.assemblies, self.output, self.version, "f" * 40)
        self.assertFalse(self.output.exists())

    def test_preparation_rejects_modified_candidate_files(self):
        artifact = next((self.fixture.output / "repository").rglob("*.jar"))
        artifact.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_preparation_rejects_assembly_using_a_different_native_candidate(self):
        path = self.assemblies / "assembly-linux-x86_64" / assembly_image.MANIFEST
        metadata = json.loads(path.read_text())
        metadata["nativeBundleSha256"] = "f" * 64
        path.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(ValueError, "native bundle differs"):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_preparation_rejects_missing_or_modified_assemblies(self):
        path = self.assemblies / "assembly-linux-aarch64" / assembly_image.jar_name(self.version)
        path.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            self.prepare()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "assembly is missing"):
            self.prepare()

    def test_publication_verifies_tag_and_uploads_before_exposing_release(self):
        self.prepare()
        self.client.annotated = True
        self.publish()
        self.assertFalse(self.client.remote["draft"])
        self.assertTrue(self.client.remote["prerelease"])
        self.assertEqual(self.client.remote["body"], self.notes.read_text().replace("{{VERSION}}", self.version))
        self.assertEqual(self.client.mutations[0], ("POST", "releases"))
        self.assertEqual(self.client.mutations[-1], ("PATCH", "releases/7"))
        self.assertEqual(set(self.client.files), github_release.asset_names(self.version))

    def test_interrupted_draft_resumes_without_reuploading_existing_files(self):
        self.prepare()
        self.client.fail_after = 2
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.publish()
        self.assertTrue(self.client.remote["draft"])
        self.assertIsNone(self.client.api("releases/tags/v" + self.version))
        # An older draft must be found beyond the first REST page.
        self.client.other_releases = [{"tag_name": "v1.0." + str(index)} for index in range(100)]
        previous = dict(self.client.files)
        self.client.fail_after = None
        self.publish()
        self.assertEqual({name: self.client.files[name] for name in previous}, previous)
        uploads = [name for method, name in self.client.mutations if method == "UPLOAD"]
        self.assertEqual(len(uploads), len(set(uploads)))
        self.assertFalse(self.client.remote["draft"])

    def test_completed_release_retry_is_read_only(self):
        self.prepare()
        self.publish()
        previous = list(self.client.mutations)
        self.publish()
        self.assertEqual(self.client.mutations, previous)

    def test_empty_starter_upload_in_a_draft_is_removed_and_retried(self):
        self.prepare()
        self.client.fail_after = 2
        with self.assertRaises(RuntimeError):
            self.publish()
        name = sorted(github_release.asset_names(self.version) - self.client.files.keys())[0]
        self.client.starters[name] = {"id": 99, "name": name, "state": "starter", "size": 0}
        self.client.fail_after = None
        self.publish()
        self.assertFalse(self.client.remote["draft"])
        self.assertEqual(self.client.starters, {})
        self.assertEqual(self.client.mutations.count(("DELETE", "releases/assets/99")), 1)

    def test_starter_cleanup_does_not_hide_a_changed_uploaded_asset(self):
        self.prepare()
        self.client.fail_after = 2
        with self.assertRaises(RuntimeError):
            self.publish()
        name = sorted(github_release.asset_names(self.version) - self.client.files.keys())[0]
        self.client.starters[name] = {"id": 99, "name": name, "state": "starter", "size": 0}
        self.client.files[next(iter(self.client.files))] = b"changed"
        previous = list(self.client.mutations)
        self.client.fail_after = None
        with self.assertRaisesRegex(ValueError, "different bytes"):
            self.publish()
        self.assertEqual(self.client.mutations, previous)

    def test_nonempty_or_published_starter_is_never_deleted(self):
        self.prepare()
        self.publish()
        name = "SHA256SUMS"
        del self.client.files[name]
        for draft, size in ((True, 1), (False, 0)):
            with self.subTest(draft=draft, size=size):
                self.client.remote["draft"] = draft
                self.client.starters[name] = {"id": 99, "name": name, "state": "starter", "size": size}
                previous = list(self.client.mutations)
                with self.assertRaisesRegex(ValueError, "different bytes"):
                    self.publish()
                self.assertEqual(self.client.mutations, previous)

    def test_final_version_is_not_marked_as_a_prerelease(self):
        paths = self.prepare()
        # Asset identity verification has separate byte-level tests; exercise
        # the final version's GitHub metadata through the same publication path.
        with mock.patch.object(github_release, "verify_assets", return_value=paths), redirect_stdout(io.StringIO()):
            github_release.publish(self.output, "3.0.0", self.source, self.notes, self.client)
        self.assertFalse(self.client.remote["prerelease"])
        self.assertEqual(self.client.remote["tag_name"], "v3.0.0")

    def test_remote_tag_moving_during_upload_keeps_the_release_draft(self):
        self.prepare()
        original = self.client.upload

        def upload_and_move(tag, path):
            original(tag, path)
            self.client.source = "f" * 40

        self.client.upload = upload_and_move
        with self.assertRaisesRegex(ValueError, "tag differs"):
            self.publish()
        self.assertTrue(self.client.remote["draft"])
        self.assertFalse(any(method == "PATCH" for method, _ in self.client.mutations))

    def test_incomplete_published_release_is_not_modified(self):
        self.prepare()
        self.publish()
        del self.client.files["SHA256SUMS"]
        previous = list(self.client.mutations)
        with self.assertRaisesRegex(ValueError, "Published GitHub release is incomplete"):
            self.publish()
        self.assertEqual(self.client.mutations, previous)

    def test_changed_release_notes_are_not_overwritten(self):
        self.prepare()
        self.publish()
        self.client.remote["body"] = "Edited release notes"
        previous = list(self.client.mutations)
        with self.assertRaisesRegex(ValueError, "metadata differs"):
            self.publish()
        self.assertEqual(self.client.mutations, previous)

    def test_existing_changed_asset_is_not_replaced_or_published(self):
        self.prepare()
        self.client.fail_after = 2
        with self.assertRaises(RuntimeError):
            self.publish()
        self.client.files[next(iter(self.client.files))] = b"different bytes"
        previous = list(self.client.mutations)
        self.client.fail_after = None
        with self.assertRaisesRegex(ValueError, "different bytes"):
            self.publish()
        self.assertEqual(self.client.mutations, previous)
        self.assertTrue(self.client.remote["draft"])

    def test_wrong_tag_and_tampered_local_assets_never_create_a_release(self):
        self.prepare()
        self.client.source = "f" * 40
        with self.assertRaisesRegex(ValueError, "tag differs"):
            self.publish()
        self.client.source = self.source
        (self.output / "release-manifest.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.publish()
        self.assertEqual(self.client.mutations, [])

    def test_unexpected_remote_assets_fail_without_mutation(self):
        self.prepare()
        self.publish()
        self.client.files["other.jar"] = b"other"
        previous = list(self.client.mutations)
        with self.assertRaisesRegex(ValueError, "unexpected"):
            self.publish()
        self.assertEqual(self.client.mutations, previous)

    def test_duplicate_release_tag_is_not_selected_arbitrarily(self):
        self.prepare()
        self.publish()
        self.client.other_releases = [dict(self.client.remote, id=8, draft=True)]
        previous = list(self.client.mutations)
        with self.assertRaisesRegex(ValueError, "Multiple GitHub releases"):
            self.publish()
        self.assertEqual(self.client.mutations, previous)

    def test_download_manifest_must_name_the_downloaded_jar(self):
        self.prepare()
        path = self.output / "assembly-manifest-linux-x86_64.json"
        metadata = json.loads(path.read_text())
        metadata["jar"] = assembly_image.jar_name(self.version)
        path.write_text(json.dumps(metadata))
        checksums = self.output / "SHA256SUMS"
        checksums.write_text("".join(
            f"{github_release.release.digest(file)}  {file.name}\n"
            for file in sorted(self.output.iterdir()) if file.name != "SHA256SUMS"))
        with self.assertRaisesRegex(ValueError, "assembly differs"):
            self.publish()
        self.assertEqual(self.client.mutations, [])

    def test_central_test_version_cannot_be_published(self):
        with self.assertRaisesRegex(ValueError, "final or alpha/beta/rc"):
            github_release.check_version("3.0.0-central-test.1", self.source)

    def test_api_errors_are_not_treated_as_an_empty_release_list(self):
        client = github_release.GitHub()
        for code in (403, 404, 500):
            with self.subTest(code=code), mock.patch.object(github_release.subprocess, "run") as run:
                run.return_value = mock.Mock(returncode=1, stdout=f"HTTP/2.0 {code} Error\n\n{{}}\n")
                with self.assertRaisesRegex(ValueError, f"HTTP {code}"):
                    client.api("releases?per_page=100&page=1")


if __name__ == "__main__":
    unittest.main()
