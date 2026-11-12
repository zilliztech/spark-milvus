"""Arrow's supplemental resource stays bound to its source and audited ARM bytes."""

import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock
import warnings
import zipfile

import arrow_cdata


class ArrowCDataSupplementTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.sources = self.root / "source"
        for name in arrow_cdata.SOURCE_FILES:
            path = self.sources / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("source: " + name)
        source_pins = mock.patch.object(arrow_cdata, "SOURCE_SHA256", {
            name: arrow_cdata.sha256((self.sources / name).read_bytes())
            for name in arrow_cdata.SOURCE_FILES})
        source_pins.start()
        self.addCleanup(source_pins.stop)
        self.library = self.root / "libarrow_cdata_jni.so"
        contents = bytearray(64)
        contents[:7] = b"\x7fELF\x02\x01\x01"
        struct.pack_into("<HH", contents, 16, 3, 183)
        self.library.write_bytes(contents)
        self.abi = {
            "elfMachine": 183, "soname": self.library.name,
            "needed": ["libc.so.6"], "versionNeeds": {"libc.so.6": ["GLIBC_2.17"]},
            "maxRequired": {"GLIBC": "2.17", "GLIBCXX": None, "CXXABI": None},
            "violations": [],
        }
        # These tests exercise packaging/verification failures independently of
        # the build host. The production builder and Linux verifier run readelf.
        inspector = mock.patch.object(arrow_cdata, "inspect_library", return_value=self.abi)
        self.inspector = inspector.start()
        self.addCleanup(inspector.stop)
        self.evidence = {"compiler": "g++ 12", "compilerTarget": "aarch64-linux-gnu",
                         "cmake": "cmake 3.22", "javac": "javac 21"}
        self.jar = self.root / "supplement.jar"
        arrow_cdata.write_jar(self.jar, self.sources, self.library, self.evidence)

    def rewrite(self, change):
        with zipfile.ZipFile(self.jar) as archive:
            entries = {name: archive.read(name) for name in archive.namelist()}
        change(entries)
        with zipfile.ZipFile(self.jar, "w") as archive:
            for name, data in entries.items():
                archive.writestr(name, data)

    def change_record(self, change):
        def edit(entries):
            record = json.loads(entries[arrow_cdata.PROVENANCE])
            change(record)
            entries[arrow_cdata.PROVENANCE] = json.dumps(record).encode()
        self.rewrite(edit)

    def test_repeat_packaging_preserves_bytes_and_embedded_resources(self):
        again = self.root / "again.jar"
        arrow_cdata.write_jar(again, self.sources, self.library, self.evidence)
        self.assertEqual(self.jar.read_bytes(), again.read_bytes())
        self.rewrite(lambda entries: entries.update({"com/example/Application.class": b"class"}))
        with self.assertRaisesRegex(ValueError, "unrelated"):
            arrow_cdata.verify_jar(self.jar)
        self.assertEqual(arrow_cdata.VERSION, arrow_cdata.verify_jar(self.jar, embedded=True)["source"]["version"])

    def test_changed_library_cannot_reuse_provenance(self):
        self.rewrite(lambda entries: entries.update({arrow_cdata.RESOURCE: b"different ELF"}))
        with self.assertRaisesRegex(ValueError, "library checksum"):
            arrow_cdata.verify_jar(self.jar)

    def test_changed_source_cannot_reuse_source_inventory(self):
        name = arrow_cdata.PREFIX + "source/java/c/src/main/cpp/jni_wrapper.cc"
        self.rewrite(lambda entries: entries.update({name: b"edited source"}))
        with self.assertRaisesRegex(ValueError, "source checksum"):
            arrow_cdata.verify_jar(self.jar)

    def test_different_upstream_identity_is_rejected(self):
        self.change_record(lambda record: record["source"].update({"revision": "0" * 40}))
        with self.assertRaisesRegex(ValueError, "source identity"):
            arrow_cdata.verify_jar(self.jar)

    def test_rehashed_source_cannot_claim_the_original_apache_release(self):
        name = "java/c/src/main/cpp/jni_wrapper.cc"
        changed = b"edited source with an updated inventory checksum"
        self.rewrite(lambda entries: entries.update({arrow_cdata.PREFIX + "source/" + name: changed}))
        self.change_record(lambda record: record["sourceFiles"].update({name: arrow_cdata.sha256(changed)}))
        with self.assertRaisesRegex(ValueError, "source inventory differs"):
            arrow_cdata.verify_jar(self.jar)

    def test_missing_loader_path_is_rejected(self):
        self.rewrite(lambda entries: entries.pop(arrow_cdata.RESOURCE))
        with self.assertRaisesRegex(ValueError, "resources"):
            arrow_cdata.verify_jar(self.jar)

    def test_duplicate_resources_are_rejected(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(self.jar, "a") as archive:
                archive.writestr(arrow_cdata.RESOURCE, self.library.read_bytes())
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            arrow_cdata.verify_jar(self.jar)

    def test_unexpected_native_payload_is_rejected(self):
        self.rewrite(lambda entries: entries.update({"native/libunexpected.so": b"ELF"}))
        with self.assertRaisesRegex(ValueError, "unrelated"):
            arrow_cdata.verify_jar(self.jar)

    def test_linux_rechecks_actual_abi(self):
        with mock.patch.object(arrow_cdata.platform, "system", return_value="Linux"):
            self.inspector.return_value = dict(self.abi, needed=["libunexpected.so"])
            with self.assertRaisesRegex(ValueError, "ABI evidence mismatch"):
                arrow_cdata.verify_jar(self.jar)

    def test_macos_checks_bound_elf_identity_without_linux_tools(self):
        with mock.patch.object(arrow_cdata.platform, "system", return_value="Darwin"):
            self.inspector.side_effect = AssertionError("macOS must not invoke Linux readelf")
            arrow_cdata.verify_jar(self.jar)
            contents = bytearray(self.library.read_bytes())
            struct.pack_into("<H", contents, 18, 62)
            self.rewrite(lambda entries: entries.update({arrow_cdata.RESOURCE: contents}))
            self.change_record(lambda record: record["library"].update({"sha256": arrow_cdata.sha256(contents)}))
            with self.assertRaisesRegex(ValueError, "ARM64 ELF"):
                arrow_cdata.verify_jar(self.jar)

    def test_newer_abi_cannot_be_declared_compatible(self):
        self.change_record(lambda record: record["abi"].update({
            "versionNeeds": {"libc.so.6": ["GLIBC_2.36"]},
            "maxRequired": {"GLIBC": "2.36", "GLIBCXX": None, "CXXABI": None}}))
        with self.assertRaisesRegex(ValueError, "exceeds the baseline"):
            arrow_cdata.verify_jar(self.jar)

    def test_wrong_source_archive_fails_before_extraction(self):
        archive = self.root / "source.tar.gz"
        archive.write_bytes(b"not the pinned Apache release")
        with self.assertRaisesRegex(ValueError, "source archive checksum"):
            arrow_cdata.extract_sources(archive, self.root / "extracted")

    def test_cross_compilation_requires_an_explicit_compiler(self):
        with mock.patch.object(arrow_cdata.platform, "system", return_value="Linux"), \
                mock.patch.object(arrow_cdata.platform, "machine", return_value="x86_64"):
            with self.assertRaisesRegex(ValueError, "cross compiler"):
                arrow_cdata.build(self.root / "work", self.root / "output.jar")


if __name__ == "__main__":
    unittest.main()
