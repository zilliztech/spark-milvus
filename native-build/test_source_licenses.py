"""Source notices must survive replacement of upstream engine CMake rules."""
import importlib.util
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("stage", Path(__file__).with_name("stage.py"))
stage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage)


class SourceLicensesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.sources = {name: self.root / name for name in ("storage", "knowhere")}
        self.documents = {
            "storage/LICENSE": b"storage license\n",
            "knowhere/LICENSE": b"knowhere license\n",
            "knowhere/thirdparty/faiss/LICENSE": b"faiss license\n",
            "knowhere/thirdparty/faiss/THIRD_PARTY_NOTICES": b"faiss embedded notices\n",
            "knowhere/thirdparty/hnswlib/LICENSE": b"hnsw license\n",
            "knowhere/thirdparty/DiskANN/LICENSE": b"diskann license\n",
            "knowhere/thirdparty/DiskANN/NOTICE.txt": b"diskann notice\n",
        }
        for name, content in self.documents.items():
            self.write(name, content)

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    def test_oss_embedded_notices_are_preserved_without_cardinal(self):
        output = self.root / "licenses"
        stage.copy_source_licenses({"sourceDirectories": self.sources, "with_cardinal": False}, output)
        copied = {str(path.relative_to(output)): path.read_bytes()
                  for path in output.rglob("*") if path.is_file()}
        self.assertEqual(self.documents, copied)

    def test_cardinal_embedded_notices_are_preserved_for_both_generations(self):
        for generation in ("v1", "v2"):
            path = "knowhere/thirdparty/cardinal" + generation + "/third_party/smalltopk/LICENSE"
            self.documents[path] = ("smalltopk " + generation + "\n").encode()
            self.write(path, self.documents[path])
        output = self.root / "licenses"
        stage.copy_source_licenses({"sourceDirectories": self.sources, "with_cardinal": True}, output)
        copied = {str(path.relative_to(output)): path.read_bytes()
                  for path in output.rglob("*") if path.is_file()}
        self.assertEqual(self.documents, copied)

    def test_missing_notice_fails_delivery(self):
        missing = self.sources["knowhere"] / "thirdparty/DiskANN/NOTICE.txt"
        missing.unlink()
        with self.assertRaisesRegex(ValueError, "Required source license document is missing"):
            stage.copy_source_licenses({"sourceDirectories": self.sources}, self.root / "licenses")

    def test_cardinal_cannot_silently_omit_embedded_license(self):
        with self.assertRaisesRegex(ValueError, "cardinalv1/third_party/smalltopk/LICENSE"):
            stage.copy_source_licenses({"sourceDirectories": self.sources, "with_cardinal": True},
                                       self.root / "licenses")


class DeliveryProvenanceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_public_metadata_removes_local_source_paths(self):
        metadata = {
            "storage.revision": "a" * 40,
            "sourceDirectories": {"storage": "/work/sources/storage"},
            "sourceObjectDirectories": {"storage": "/root/storage"},
        }

        public = stage.public_metadata(metadata)

        self.assertEqual({"storage.revision": "a" * 40}, public)
        self.assertIn("sourceDirectories", metadata)

    def test_delivery_evidence_excludes_path_dependent_build_records(self):
        source = self.root / "source"
        source.mkdir()
        (source / "conan.lock").write_text("reviewed lock\n")
        (source / "conan-graph.json").write_text('{"package_folder":"/root/.conan2"}\n')
        (source / "compile_commands.json").write_text('[{"directory":"/work/build"}]\n')

        output = self.root / "bundle" / "provenance" / "build"
        stage.copy_delivery_evidence(source, output)

        self.assertEqual(["conan.lock"], sorted(path.name for path in output.iterdir()))
        self.assertEqual("reviewed lock\n", (output / "conan.lock").read_text())

    def test_public_library_and_package_origins_drop_cache_directories(self):
        origin = stage.public_origin({
            "type": "system-runtime",
            "package": "libaio1 0.3",
            "packageVersion": "libaio1 0.3",
            "copyrightFile": "/usr/share/doc/libaio1/copyright",
        })
        package = stage.public_package({
            "reference": "folly/1#recipe",
            "packageId": "package-id",
            "packageRevision": "package-revision",
            "directory": "/root/.conan2/p/folly",
            "options": {"shared": True},
        })

        self.assertNotIn("copyrightFile", origin)
        self.assertNotIn("directory", package)


if __name__ == "__main__":
    unittest.main()
