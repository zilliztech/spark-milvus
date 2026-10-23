"""Test ABI parsing and audit real, locally compiled ELF fixtures without loading them."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
import unittest
import warnings
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "check-native-abi.py"
SPEC = importlib.util.spec_from_file_location("check_native_abi", SCRIPT)
ABI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ABI)


class ReadelfParsingTest(unittest.TestCase):
    OUTPUT = """
Dynamic section at offset 0x2dc0 contains 24 entries:
 0x0000000000000001 (NEEDED) Shared library: [libc.so.6]
 0x0000000000000001 (NEEDED) Shared library: [libstdc++.so.6]
 0x000000000000000e (SONAME) Library soname: [libfixture.so]
 0x000000006ffffffe (VERNEED) 0x3f0
Version symbols section '.gnu.version' contains 4 entries:
  000: 2 (GLIBC_99.0) 3 (GLIBCXX_99.0)
Version definition section '.gnu.version_d' contains 1 entry:
  000000: Rev: 1 Flags: none Index: 2 Cnt: 1 Name: GLIBC_99.0
Version needs section '.gnu.version_r' contains 2 entries:
 Addr: 0x00000000000003f0 Offset: 0x3f0 Link: 4 (.dynstr)
  000000: Version: 1 File: libc.so.6 Cnt: 2
  0x0010: Name: GLIBC_2.9 Flags: none Version: 3
  0x0020: Name: GLIBC_2.35 Flags: none Version: 2
  0x0030: Version: 1 File: libstdc++.so.6 Cnt: 2
  0x0040: Name: GLIBCXX_3.4.30 Flags: none Version: 5
  0x0050: Name: CXXABI_1.3.13 Flags: none Version: 4
"""

    def test_only_version_needs_contribute_to_requirements(self):
        record = ABI.parse_readelf(self.OUTPUT)
        self.assertEqual("libfixture.so", record["soname"])
        self.assertEqual(["GLIBC_2.35", "GLIBC_2.9"], record["versionNeeds"]["libc.so.6"])
        maximum, violations = ABI.requirement_summary(record["versionNeeds"])
        self.assertEqual({"GLIBC": "2.35", "GLIBCXX": "3.4.30", "CXXABI": "1.3.13"}, maximum)
        self.assertEqual([], violations)

    def test_all_three_baseline_limits_are_enforced(self):
        maximum, violations = ABI.requirement_summary({"provider": [
            "GLIBC_2.36", "GLIBCXX_3.4.31", "CXXABI_1.3.14"]})
        self.assertEqual("2.36", maximum["GLIBC"])
        self.assertEqual(3, len(violations))
        self.assertTrue(all("exceeds" in violation for violation in violations))

    def test_non_numeric_capabilities_are_explicit(self):
        _, violations = ABI.requirement_summary({"provider": [
            "CXXABI_TM_1", "CXXABI_FLOAT128", "GLIBC_PRIVATE", "GLIBC_ABI_DT_RELR"]})
        self.assertEqual(2, len(violations))
        self.assertTrue(all(violation.startswith("GLIBC_") for violation in violations))

    def test_truncated_or_inconsistent_needs_fail(self):
        for text in (
                self.OUTPUT.replace("File: libc.so.6 Cnt: 2", "File: libc.so.6 Cnt: 3"),
                self.OUTPUT.replace("File: libc.so.6", "File: missing.so"),
                self.OUTPUT.replace("contains 2 entries:", "contains 3 entries:"),
                self.OUTPUT[:self.OUTPUT.index("Version needs section")]):
            with self.subTest(text=text), self.assertRaises(ValueError):
                ABI.parse_readelf(text)

    def test_unversioned_library_has_no_requirements(self):
        record = ABI.parse_readelf("There is no dynamic section in this file.\n"
                                  "No version information found in this file.\n")
        self.assertEqual({}, record["versionNeeds"])
        self.assertEqual([], record["needed"])


class OtoolParsingTest(unittest.TestCase):
    """Mach-O load commands as `otool -l` prints them; no macOS host is needed."""

    OUTPUT = """
/tmp/libfixture.dylib:
Load command 0
      cmd LC_SEGMENT_64
  cmdsize 72
  segname __TEXT
Load command 1
          cmd LC_ID_DYLIB
      cmdsize 48
         name @rpath/libfixture.dylib (offset 24)
   time stamp 1 Thu Jan  1 00:00:01 1970
      current version 0.0.0
compatibility version 0.0.0
Load command 2
      cmd LC_BUILD_VERSION
  cmdsize 32
  platform 1
    minos 14.0
      sdk 15.5
   ntools 1
     tool 3
  version 1167.5
Load command 3
          cmd LC_LOAD_DYLIB
      cmdsize 56
         name @rpath/libknowhere.dylib (offset 24)
   time stamp 2 Thu Jan  1 00:00:02 1970
      current version 0.0.0
compatibility version 0.0.0
Load command 4
          cmd LC_LOAD_DYLIB
      cmdsize 56
         name /usr/lib/libc++.1.dylib (offset 24)
   time stamp 2 Thu Jan  1 00:00:02 1970
      current version 1800.105.0
compatibility version 1.0.0
Load command 5
          cmd LC_LOAD_DYLIB
      cmdsize 56
         name /usr/lib/libSystem.B.dylib (offset 24)
   time stamp 2 Thu Jan  1 00:00:02 1970
      current version 1351.0.0
compatibility version 1.0.0
Load command 6
          cmd LC_RPATH
      cmdsize 32
         path @loader_path (offset 12)
"""

    def test_install_name_target_and_dependencies_are_read(self):
        record = ABI.parse_otool(self.OUTPUT)
        self.assertEqual("@rpath/libfixture.dylib", record["installName"])
        self.assertEqual("14.0", record["minos"])
        self.assertEqual("15.5", record["sdk"])
        self.assertEqual(["@rpath/libknowhere.dylib", "/usr/lib/libc++.1.dylib", "/usr/lib/libSystem.B.dylib"],
                         record["needPaths"])
        self.assertEqual(["@loader_path"], record["rpaths"])
        maximum, violations = ABI.macos_requirements(record)
        self.assertEqual({"MACOS": "14.0"}, maximum)
        self.assertEqual([], violations)

    def test_legacy_version_min_command_is_a_deployment_target(self):
        text = self.OUTPUT.replace("""      cmd LC_BUILD_VERSION
  cmdsize 32
  platform 1
    minos 14.0
      sdk 15.5
   ntools 1
     tool 3
  version 1167.5
""", """      cmd LC_VERSION_MIN_MACOSX
  cmdsize 16
  version 13.0
      sdk 14.5
""")
        self.assertEqual("13.0", ABI.parse_otool(text)["minos"])

    def test_deployment_target_above_the_baseline_is_a_violation(self):
        record = ABI.parse_otool(self.OUTPUT.replace("minos 14.0", "minos 15.0"))
        maximum, violations = ABI.macos_requirements(record)
        self.assertEqual({"MACOS": "15.0"}, maximum)
        self.assertEqual(1, len(violations))
        self.assertIn("exceeds macOS 14.0", violations[0])

    def test_dependency_outside_system_and_bundle_is_a_violation(self):
        record = ABI.parse_otool(self.OUTPUT.replace("@rpath/libknowhere.dylib", "/opt/homebrew/lib/libomp.dylib"))
        _, violations = ABI.macos_requirements(record)
        self.assertEqual(1, len(violations))
        self.assertIn("/opt/homebrew/lib/libomp.dylib", violations[0])

    def test_missing_duplicate_or_foreign_platform_targets_fail(self):
        for text in (
                self.OUTPUT.replace("platform 1", "platform 2"),
                self.OUTPUT.replace("    minos 14.0\n", ""),
                self.OUTPUT + self.OUTPUT[self.OUTPUT.index("Load command 2"):self.OUTPUT.index("Load command 3")],
                self.OUTPUT.replace("Load command 4", "Load command 7").replace(
                    "/usr/lib/libc++.1.dylib", "@rpath/libknowhere.dylib"),
                self.OUTPUT[:self.OUTPUT.index("Load command 2")]):
            with self.subTest(text=text[-120:]), self.assertRaises(ValueError):
                ABI.parse_otool(text)

    def test_platform_table_names_both_families(self):
        self.assertEqual("linux", ABI.PLATFORMS["linux-x86_64"][0])
        self.assertEqual(("darwin", 0x0100000C), ABI.PLATFORMS["darwin-aarch64"])
        self.assertEqual({"linux-x86_64": 62, "linux-aarch64": 183}, ABI.MACHINES)
        self.assertEqual("14.0", ABI.BASELINES["darwin"]["MACOS"])
        self.assertTrue(ABI.is_native_binary(b"\xcf\xfa\xed\xfe" + b"\0" * 28))
        self.assertTrue(ABI.is_native_binary(b"\x7fELF" + b"\0" * 28))
        self.assertFalse(ABI.is_native_binary(b"PK\x03\x04" + b"\0" * 28))


@unittest.skipUnless(sys.platform.startswith("linux"), "ELF fixture compilation requires Linux")
class NativeAbiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for executable in ("gcc", "readelf"):
            if shutil.which(executable) is None:
                raise AssertionError("Required ABI test tool: " + executable)
        cls.temporary = tempfile.TemporaryDirectory(prefix="native-abi-test-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.platform = "linux-x86_64" if os.uname().machine == "x86_64" else "linux-aarch64"
        cls.prefix = f"native/milvus/1/{cls.platform}/"
        cls.library = cls.compile("libfixture.so", """
#include <stdio.h>
#include <stdlib.h>
__attribute__((constructor)) static void never_execute(void) { abort(); }
int fixture(const char *message) { return puts(message); }
""")
        version_map = cls.root / "future.map"
        version_map.write_text("GLIBC_2.99 { global: future_api; local: *; };\n", encoding="ascii")
        cls.provider = cls.compile("libfuture.so", "int future_api(void) { return 7; }",
                                   ["-Wl,--version-script=" + str(version_map)])
        cls.consumer = cls.compile("libconsumer.so", "extern int future_api(void);\n"
                                   "int consumer(void) { return future_api(); }\n",
                                   ["-L" + str(cls.root), "-l:libfuture.so"])

    @classmethod
    def compile(cls, name, contents, options=()):
        source = cls.root / (name + ".c")
        source.write_text(contents, encoding="ascii")
        output = cls.root / name
        environment = os.environ.copy()
        for key in ("LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT"):
            environment.pop(key, None)
        subprocess.run(["gcc", "-shared", "-fPIC", str(source), "-Wl,-soname," + name,
                        *options, "-o", str(output)], check=True, capture_output=True, env=environment)
        return output.read_bytes()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=self.root)
        self.addCleanup(self.directory.cleanup)
        self.work = Path(self.directory.name)

    def bundle(self, libraries=None, extras=(), manifest_edit=None):
        libraries = libraries or {"libfixture.so": self.library}
        manifest = f"format.version=1\nplatform={self.platform}\nlibraries={','.join(libraries)}\n"
        for name, contents in libraries.items():
            manifest += f"sha256.{name}={hashlib.sha256(contents).hexdigest()}\n"
        if manifest_edit:
            manifest = manifest_edit(manifest)
        path = self.work / "bundle.jar"
        with warnings.catch_warnings(), zipfile.ZipFile(path, "w") as archive:
            warnings.simplefilter("ignore", UserWarning)
            archive.writestr(self.prefix + "manifest.properties", manifest)
            for name, contents in libraries.items():
                archive.writestr(self.prefix + name, contents)
            for name, contents in extras:
                archive.writestr(name, contents)
        return path

    def test_real_elf_records_actual_imports_and_system_dependencies(self):
        bundle = self.bundle()
        report = ABI.audit_bundle(bundle, self.platform)
        self.assertEqual("passed", report["status"])
        self.assertEqual(hashlib.sha256(bundle.read_bytes()).hexdigest(), report["jar.sha256"])
        library = report["libraries"]["libfixture.so"]
        self.assertIn("libc.so.6", library["needed"])
        self.assertTrue(library["versionNeeds"]["libc.so.6"])
        self.assertIsNotNone(report["maxRequired"]["GLIBC"])
        self.assertEqual(["libfixture.so"], report["systemDependencies"]["libc.so.6"]["consumers"])

    def test_exported_future_version_is_ignored_but_import_is_rejected(self):
        report = ABI.audit_bundle(self.bundle({"libfuture.so": self.provider}), self.platform)
        self.assertEqual("passed", report["status"])
        self.assertIsNone(report["maxRequired"]["GLIBC"])
        bundle = self.bundle({"libfuture.so": self.provider, "libconsumer.so": self.consumer})
        report = ABI.audit_bundle(bundle, self.platform)
        self.assertEqual("failed", report["status"])
        self.assertEqual("2.99", report["maxRequired"]["GLIBC"])
        self.assertNotIn("libfuture.so", report["systemDependencies"])
        self.assertIn("libconsumer.so: GLIBC_2.99 exceeds GLIBC_2.35", report["violations"])

    def test_cli_writes_failure_evidence_and_returns_nonzero(self):
        bundle = self.bundle({"libconsumer.so": self.consumer})
        output = self.work / "abi.json"
        result = subprocess.run([sys.executable, str(SCRIPT), "--bundle", str(bundle),
                                 "--platform", self.platform, "--output", str(output)],
                                capture_output=True, text=True)
        self.assertEqual(1, result.returncode, result.stderr)
        report = json.loads(output.read_text())
        self.assertEqual("failed", report["status"])
        self.assertEqual(["GLIBC_2.99"], report["systemDependencies"]["libfuture.so"]["versions"])

    def test_wrong_architecture_and_non_elf_are_rejected(self):
        wrong = bytearray(self.library)
        struct.pack_into("<H", wrong, 18, 183 if self.platform == "linux-x86_64" else 62)
        for contents, message in ((wrong, "architecture"), (b"not ELF", "not ELF")):
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                ABI.audit_bundle(self.bundle({"libfixture.so": contents}), self.platform)

    def test_unsafe_and_duplicate_zip_paths_are_rejected(self):
        for name in ("../escape.so", "/absolute.so", "C:/absolute.so", "a\\b", "a//b",
                     self.prefix + "../escape.so", self.prefix + "libfixture.so"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Unsafe|Duplicate"):
                ABI.audit_bundle(self.bundle(extras=[(name, self.library)]), self.platform)
        self.assertFalse((self.root / "escape.so").exists())

    def test_zip_symlink_is_rejected(self):
        link = zipfile.ZipInfo("link.so")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        with self.assertRaisesRegex(ValueError, "not a regular file"):
            ABI.audit_bundle(self.bundle(extras=[(link, "../escape")]), self.platform)

    def test_corrupt_zip_payload_is_rejected(self):
        bundle = self.bundle()
        with zipfile.ZipFile(bundle) as archive:
            entry = archive.getinfo(self.prefix + "libfixture.so")
        contents = bytearray(bundle.read_bytes())
        name_length, extra_length = struct.unpack_from("<HH", contents, entry.header_offset + 26)
        payload = entry.header_offset + 30 + name_length + extra_length
        contents[payload + entry.file_size - 1] ^= 1
        bundle.write_bytes(contents)
        with self.assertRaisesRegex(zipfile.BadZipFile, "CRC"):
            ABI.audit_bundle(bundle, self.platform)

    def test_undeclared_resources_and_checksum_mismatch_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "library set"):
            ABI.audit_bundle(self.bundle(extras=[(self.prefix + "extra.so", self.library)]), self.platform)
        with self.assertRaisesRegex(ValueError, "outside the declared"):
            ABI.audit_bundle(self.bundle(extras=[("META-INF/hidden", self.library)]), self.platform)
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            ABI.audit_bundle(self.bundle(manifest_edit=lambda text: text.replace("sha256.", "ignored.")),
                             self.platform)


if __name__ == "__main__":
    unittest.main()
