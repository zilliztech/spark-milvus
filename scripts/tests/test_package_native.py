"""Exercise native packaging against real ELF dependencies, without native engines.

Run on Linux with GCC, readelf and ldd installed:
    python3 -m unittest discover -s scripts/tests -p 'test_package_native.py' -v
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "package-native.py"
SPEC = importlib.util.spec_from_file_location("package_native", SCRIPT)
PACKAGE_NATIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGE_NATIVE)


@unittest.skipUnless(sys.platform.startswith("linux"), "Native packaging validates Linux ELF files")
class PackageNativeTest(unittest.TestCase):
    def setUp(self):
        for executable in ("gcc", "readelf", "ldd", "nm"):
            self.assertIsNotNone(shutil.which(executable), f"Required test tool: {executable}")
        self.temporary = tempfile.TemporaryDirectory(prefix="package-native-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.libraries = self.root / "lib"
        self.libraries.mkdir()
        self.source_count = 0
        self.compile("libdependency.so.1", "int dependency(void) { return 7; }")
        for number, name in enumerate(PACKAGE_NATIVE.REQUIRED_ENTRIES):
            self.compile(
                name,
                f"extern int dependency(void); int entry_{number}(void) {{ return dependency(); }}",
                dependencies=("libdependency.so.1",),
            )
        self.provenance = self.root / "provenance.json"
        self.metadata = {
            "storage.revision": "a" * 40,
            "knowhere.revision": "b" * 40,
            "platform": "linux-x86_64" if os.uname().machine == "x86_64" else "linux-aarch64",
            "with_cardinal": False,
            "dependency.mode": "shared",
            "audit": "passed",
            "knowhereCApiTestsExit": 0,
            "knowhereCApiTests": [
                "knowhere_c_api",
                "knowhere_c_api_concurrency",
                "knowhere_c_api_diskann_acceptance",
            ],
            "relocationRoots": [],
            "dlopenEntries": list(PACKAGE_NATIVE.AUDIT_DLOPEN_ENTRIES),
        }
        self.record_provenance()
        self.prefix = f"native/milvus/1/{self.metadata['platform']}/"
        self.output = self.root / "bundle.jar"

    def compile(self, name, source, dependencies=(), soname=None, rpath="$ORIGIN"):
        self.source_count += 1
        source_file = self.root / f"library-{self.source_count}.c"
        source_file.write_text(source, encoding="ascii")
        output = self.libraries / name
        output.parent.mkdir(parents=True, exist_ok=True)
        arguments = [
            "gcc", "-shared", "-fPIC",
            f"-Wl,-rpath,{rpath}", str(source_file), f"-L{self.libraries}",
        ]
        if soname is not False:
            arguments.append(f"-Wl,-soname,{soname or name}")
        arguments.extend(f"-l:{dependency}" for dependency in dependencies)
        arguments.extend(("-o", str(output)))
        environment = os.environ.copy()
        for key in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT"):
            environment.pop(key, None)
        subprocess.run(arguments, env=environment, capture_output=True, text=True, check=True)
        return output

    def record_provenance(self):
        libraries, aliases = PACKAGE_NATIVE.inventory(self.libraries, self.metadata["platform"])
        self.metadata["relocationRoots"] = sorted(libraries)
        self.metadata["dlopenEntries"] = list(PACKAGE_NATIVE.AUDIT_DLOPEN_ENTRIES)
        self.metadata["libraries"] = {
            name: {key: record[key] for key in ("sha256", "soname", "needed")}
            for name, record in libraries.items()
        }
        self.metadata["aliases"] = aliases
        self.write_provenance()

    def write_provenance(self):
        self.provenance.write_text(json.dumps(self.metadata, sort_keys=True), encoding="utf-8")

    def package(self, output=None, evidence=None):
        with contextlib.redirect_stdout(io.StringIO()):
            PACKAGE_NATIVE.package(self.libraries, self.provenance, output or self.output, evidence=evidence)

    def manifest(self, archive):
        return dict(
            line.split("=", 1)
            for line in archive.read(self.prefix + "manifest.properties").decode("ascii").splitlines()
        )

    def test_real_soname_aliases_share_one_archived_binary(self):
        dependency = self.libraries / "libdependency.so.1"
        (self.libraries / "libdependency.so").symlink_to(dependency.name)
        shutil.copy2(dependency, self.libraries / "libdependency.so.1.0")
        self.record_provenance()

        self.package()

        with zipfile.ZipFile(self.output) as archive:
            manifest = self.manifest(archive)
            self.assertEqual("libdependency.so.1", manifest["alias.libdependency.so"])
            self.assertEqual("libdependency.so.1", manifest["alias.libdependency.so.1.0"])
            self.assertNotIn(self.prefix + "libdependency.so", archive.namelist())
            self.assertNotIn(self.prefix + "libdependency.so.1.0", archive.namelist())
            self.assertEqual(dependency.read_bytes(), archive.read(self.prefix + dependency.name))
            elf = json.loads(archive.read("META-INF/milvus-native/elf.json"))
            self.assertEqual(len(PACKAGE_NATIVE.REQUIRED_ENTRIES) + 1, len(elf))
            self.assertEqual("libdependency.so.1", elf[dependency.name]["soname"])

    def test_manifest_declares_exact_jvm_load_entries(self):
        self.package()

        with zipfile.ZipFile(self.output) as archive:
            self.assertEqual(",".join(PACKAGE_NATIVE.LOAD_ENTRIES),
                             self.manifest(archive)["load.entries"])

    def test_entry_library_may_be_a_real_soname_alias(self):
        self.compile(
            "libknowhere_jni.so", "extern int dependency(void); int known(void) { return dependency(); }",
            dependencies=("libdependency.so.1",), soname="libknowhere_jni.so.1",
        )
        (self.libraries / "libknowhere_jni.so.1").symlink_to("libknowhere_jni.so")
        self.record_provenance()

        self.package()

        with zipfile.ZipFile(self.output) as archive:
            manifest = self.manifest(archive)
            self.assertEqual("libknowhere_jni.so.1", manifest["alias.libknowhere_jni.so"])
            self.assertIn("libknowhere_jni.so.1", manifest["libraries"].split(","))

    def test_packaged_libraries_load_after_moving_away_from_build_directory(self):
        (self.libraries / "libdependency.so").symlink_to("libdependency.so.1")
        self.record_provenance()
        self.package()
        extracted = self.root / "runtime"
        extracted.mkdir()
        with zipfile.ZipFile(self.output) as archive:
            manifest = self.manifest(archive)
            for name in manifest["libraries"].split(","):
                (extracted / name).write_bytes(archive.read(self.prefix + name))
            for name in manifest["aliases"].split(","):
                (extracted / name).hardlink_to(extracted / manifest[f"alias.{name}"])
        # The child must resolve the packaged copy, with no surviving library
        # at any absolute path that was passed to the original link command.
        self.libraries.rename(self.root / "unused-build-directory")
        environment = os.environ.copy()
        for key in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT"):
            environment.pop(key, None)
        environment["LD_BIND_NOW"] = "1"
        probe = "\n".join((
            "import ctypes, json, pathlib, sys",
            "directory = pathlib.Path(sys.argv[1])",
            "for number, name in json.loads(sys.argv[2]):",
            "    library = ctypes.CDLL(str(directory / name))",
            "    assert getattr(library, 'entry_' + str(number))() == 7",
        ))
        entries = [
            (PACKAGE_NATIVE.REQUIRED_ENTRIES.index(name), name)
            for name in PACKAGE_NATIVE.LOAD_ENTRIES
        ]
        result = subprocess.run(
            [sys.executable, "-c", probe, str(extracted), json.dumps(entries)],
            env=environment, capture_output=True, text=True, check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_different_bytes_with_same_soname_are_rejected(self):
        self.compile(
            "libdifferent.so", "int dependency(void) { return 99; }", soname="libdependency.so.1",
        )

        with self.assertRaisesRegex(ValueError, "SONAME.*libdependency.so.1"):
            self.package()
        self.assertFalse(self.output.exists())

    def test_soname_filename_cannot_belong_to_another_binary(self):
        # Both files have different SONAMEs, but libowner advertises the path
        # occupied by liboccupied. A loader resolves filenames, not this intent.
        self.compile("liboccupied.so", "int occupied(void) { return 1; }", soname="libdifferent.so")
        (self.libraries / "libdifferent.so").symlink_to("liboccupied.so")
        self.compile("libowner.so", "int owner(void) { return 2; }", soname="liboccupied.so")

        with self.assertRaisesRegex(ValueError, "SONAME.*liboccupied.so"):
            self.package()

    def test_missing_non_system_dependency_is_rejected(self):
        (self.libraries / "libdependency.so.1").rename(self.root / "outside-dependency.so")

        with self.assertRaisesRegex(ValueError, "Missing non-system dependency libdependency.so.1"):
            self.package()

    def test_unresolved_symbol_is_rejected_even_when_ldd_exits_successfully(self):
        self.compile(
            "libknowhere_jni.so",
            "extern int missing_native_symbol(void); int known(void) { return missing_native_symbol(); }",
        )
        # GNU ldd -r normally exits zero for undefined symbols; checking only
        # its exit code would publish a JAR that fails at native initialization.
        diagnostic = PACKAGE_NATIVE.command("ldd", "-r", str(self.libraries / "libknowhere_jni.so"))
        self.assertIn("undefined symbol: missing_native_symbol", diagnostic)
        self.record_provenance()

        with self.assertRaisesRegex(ValueError, "Unresolved native relocations.*libknowhere_jni.so"):
            self.package()

    def test_missing_soname_file_is_rejected_before_relocation(self):
        self.compile("libunaliased.so.1.0", "int unaliased(void) { return 3; }", soname="libunaliased.so.1")

        with self.assertRaisesRegex(ValueError, "SONAME.*libunaliased.so.1"):
            self.package()

    def test_cardinal_callbacks_resolve_only_in_the_declared_knowhere_parent(self):
        self.compile("libcardinalv1.so", "extern int host_callback(void); int plugin(void) { return host_callback(); }")
        self.compile("libcardinalv2.so", "int second_plugin(void) { return 5; }")
        self.compile("libknowhere.so", "extern int plugin(void); int host_callback(void) { return 9; } int engine(void) { return plugin(); }",
                     dependencies=("libcardinalv1.so",))
        self.metadata["with_cardinal"] = True
        self.record_provenance()

        self.package()

        self.assertTrue(self.output.is_file())

    def test_zlib_is_a_system_abi_dependency_not_a_second_bundled_copy(self):
        self.compile("libz.so.1", "int test_zlib(void) { return 1; }")

        with self.assertRaisesRegex(ValueError, "System ABI library must not be bundled: libz.so.1"):
            self.package()

    def test_cardinal_missing_callback_is_not_hidden_by_its_parent(self):
        self.compile("libcardinalv1.so", "extern int missing_callback(void); int plugin(void) { return missing_callback(); }")
        self.compile("libcardinalv2.so", "int second_plugin(void) { return 5; }")
        self.compile("libknowhere.so", "int unrelated(void) { return 9; }")
        self.metadata["with_cardinal"] = True
        self.record_provenance()

        with self.assertRaisesRegex(ValueError, "symbols outside.*libknowhere.so"):
            self.package()

    def test_ordinary_library_cannot_depend_on_symbols_from_its_consumer(self):
        self.compile("libother-plugin.so", "extern int host_callback(void); int plugin(void) { return host_callback(); }")
        self.compile("libknowhere.so", "int host_callback(void) { return 9; }")
        self.compile(
            "libknowhere_jni.so",
            "extern int plugin(void); extern int host_callback(void); "
            "int entry_2(void) { return plugin() + host_callback(); }",
            dependencies=("libother-plugin.so", "libknowhere.so"),
        )
        self.record_provenance()

        standalone = PACKAGE_NATIVE.command("ldd", "-r", str(self.libraries / "libother-plugin.so"))
        self.assertIn("undefined symbol: host_callback", standalone)
        with self.assertRaisesRegex(ValueError, "Unresolved native relocations in libother-plugin.so"):
            self.package()
        self.assertFalse(self.output.exists())

    def test_absolute_dependency_search_path_is_rejected(self):
        self.compile(
            "libknowhere_jni.so", "extern int dependency(void); int known(void) { return dependency(); }",
            dependencies=("libdependency.so.1",), rpath=str(self.libraries),
        )

        with self.assertRaisesRegex(ValueError, "dependencies from the bundle.*libknowhere_jni.so"):
            self.package()

    def test_source_symlink_must_not_escape_library_directory(self):
        outside = self.root / "outside.so"
        shutil.copy2(self.libraries / "libdependency.so.1", outside)
        (self.libraries / "libescape.so").symlink_to("../outside.so")

        with self.assertRaisesRegex(ValueError, "Invalid native path: libescape.so"):
            self.package()

    def test_resource_path_cannot_inject_manifest_properties(self):
        shutil.copy2(self.libraries / "libdependency.so.1", self.libraries / "bad\nwith_cardinal=true")

        with self.assertRaisesRegex(ValueError, "Invalid native path"):
            self.package()

    def test_required_entry_cannot_be_omitted(self):
        (self.libraries / "libknowhere_jni.so").rename(self.root / "outside-jni.so")

        with self.assertRaisesRegex(ValueError, "Missing JNI/engine entry: libknowhere_jni.so"):
            self.package()

    def test_provenance_requires_exact_pins_and_shared_build(self):
        for key, invalid in (
            ("storage.revision", "main"),
            ("knowhere.revision", "b" * 39),
            ("platform", "unknown"),
            ("with_cardinal", "false"),
            ("dependency.mode", "static"),
        ):
            with self.subTest(key=key):
                invalid_metadata = dict(self.metadata, **{key: invalid})
                self.provenance.write_text(json.dumps(invalid_metadata), encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.package()
                self.assertFalse(self.output.exists())

    def test_elf_architecture_must_match_declared_platform(self):
        wrong_platform = "linux-aarch64" if self.metadata["platform"] == "linux-x86_64" else "linux-x86_64"
        self.metadata["platform"] = wrong_platform
        self.provenance.write_text(json.dumps(self.metadata), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, f"ELF architecture does not match {wrong_platform}"):
            self.package()
        self.assertFalse(self.output.exists())

    def test_stale_binary_provenance_does_not_replace_existing_artifact(self):
        self.package()
        before = self.output.read_bytes()
        sidecar = Path(str(self.output) + ".properties")
        before_sidecar = sidecar.read_bytes()
        self.compile("libdependency.so.1", "int dependency(void) { return 99; }")

        with self.assertRaisesRegex(ValueError, "sha256 differs.*libdependency.so.1"):
            self.package()

        self.assertEqual(before, self.output.read_bytes())
        self.assertEqual(before_sidecar, sidecar.read_bytes())

    def test_library_set_must_match_the_audited_directory(self):
        extra = self.compile("libextra.so", "int extra(void) { return 1; }")
        with self.assertRaisesRegex(ValueError, "library file set differs"):
            self.package()
        self.record_provenance()
        extra.unlink()
        with self.assertRaisesRegex(ValueError, "library file set differs"):
            self.package()

    def test_aliases_must_match_the_audited_directory(self):
        alias = self.libraries / "libdependency.so"
        alias.symlink_to("libdependency.so.1")
        with self.assertRaisesRegex(ValueError, "aliases differ"):
            self.package()
        self.record_provenance()
        self.metadata["aliases"][alias.name] = "libmilvus-storage.so"
        self.write_provenance()
        with self.assertRaisesRegex(ValueError, "aliases differ"):
            self.package()
        self.record_provenance()
        alias.unlink()
        with self.assertRaisesRegex(ValueError, "aliases differ"):
            self.package()

    def test_unrecorded_dangling_alias_cannot_be_silently_ignored(self):
        (self.libraries / "libdangling.so").symlink_to("libmissing.so")
        with self.assertRaisesRegex(ValueError, "alias does not resolve.*libdangling.so"):
            self.package()

    def test_provenance_requires_complete_successful_validation(self):
        valid = dict(self.metadata)
        cases = (
            ("audit", None), ("audit", "pending"), ("audit", "failed"),
            ("knowhereCApiTestsExit", None), ("knowhereCApiTestsExit", 1),
            ("knowhereCApiTestsExit", False),
            ("knowhereCApiTests", None),
            ("knowhereCApiTests", valid["knowhereCApiTests"][:-1]),
            ("knowhereCApiTests", valid["knowhereCApiTests"] + ["unexpected_test"]),
            ("relocationRoots", None),
            ("relocationRoots", valid["relocationRoots"][:-1]),
            ("relocationRoots", ["libmilvus-storage-jni.so", "libmilvus-storage.so"]),
            ("relocationRoots", valid["relocationRoots"] + ["libmilvus-storage.so"]),
            ("dlopenEntries", None),
            ("dlopenEntries", list(reversed(valid["dlopenEntries"]))),
            ("dlopenEntries", ["libmilvus-storage-jni.so", "libmilvus-storage.so"]),
            ("dlopenEntries", valid["dlopenEntries"] + ["libmilvus-storage.so"]),
            ("libraries", None), ("libraries", []), ("aliases", None),
        )
        for key, invalid in cases:
            with self.subTest(key=key, value=invalid):
                self.metadata = dict(valid)
                if invalid is None:
                    del self.metadata[key]
                else:
                    self.metadata[key] = invalid
                self.write_provenance()
                with self.assertRaises(ValueError):
                    self.package()
                self.assertFalse(self.output.exists())

    def test_nested_provider_without_soname_matches_staging_provenance(self):
        name = "ossl-modules/provider.so"
        self.compile(name, "extern int dependency(void); int provider(void) { return dependency(); }",
                     dependencies=("libdependency.so.1",), soname=False, rpath="$ORIGIN:$ORIGIN/..")
        self.record_provenance()
        self.assertEqual("provider.so", self.metadata["libraries"][name]["soname"])

        self.package()

        with zipfile.ZipFile(self.output) as archive:
            self.assertIn(name, self.manifest(archive)["libraries"].split(","))
            self.assertEqual((self.libraries / name).read_bytes(), archive.read(self.prefix + name))

    def test_provenance_cannot_misrepresent_elf_identity_or_dependencies(self):
        original = dict(self.metadata["libraries"]["libdependency.so.1"])
        for key, invalid in (("sha256", "a" * 64), ("soname", "libother.so"), ("needed", ["libother.so"])):
            with self.subTest(key=key):
                self.metadata["libraries"]["libdependency.so.1"] = dict(original, **{key: invalid})
                self.write_provenance()
                with self.assertRaisesRegex(ValueError, f"{key} differs.*libdependency.so.1"):
                    self.package()
        self.metadata["libraries"]["libdependency.so.1"] = None
        self.write_provenance()
        with self.assertRaisesRegex(ValueError, "Invalid provenance library record"):
            self.package()

    def test_cardinal_flag_must_match_both_compiled_plugins(self):
        self.metadata["with_cardinal"] = True
        self.write_provenance()
        with self.assertRaisesRegex(ValueError, "Cardinal libraries do not match"):
            self.package()
        self.compile("libcardinalv1.so", "int first_plugin(void) { return 1; }")
        self.record_provenance()
        with self.assertRaisesRegex(ValueError, "Cardinal libraries do not match"):
            self.package()
        self.compile("libcardinalv2.so", "int second_plugin(void) { return 2; }")
        self.metadata["with_cardinal"] = False
        self.record_provenance()
        with self.assertRaisesRegex(ValueError, "Cardinal libraries do not match"):
            self.package()

    def test_library_changed_during_packaging_does_not_replace_existing_artifact(self):
        self.package()
        before = self.output.read_bytes()
        sidecar = Path(str(self.output) + ".properties")
        before_sidecar = sidecar.read_bytes()
        original_put = PACKAGE_NATIVE.put

        def put_after_rebuild(archive, name, contents):
            if name.endswith("manifest.properties"):
                self.compile("libdependency.so.1", "int dependency(void) { return 11; }")
            original_put(archive, name, contents)

        with mock.patch.object(PACKAGE_NATIVE, "put", side_effect=put_after_rebuild):
            with self.assertRaisesRegex(ValueError, "changed while packaging: libdependency.so.1"):
                self.package()

        self.assertEqual(before, self.output.read_bytes())
        self.assertEqual(before_sidecar, sidecar.read_bytes())
        self.assertEqual({"bundle.jar"}, {path.name for path in self.root.glob("*.jar")})

    def test_archive_bytes_and_metadata_are_deterministic(self):
        self.package()
        second = self.root / "second.jar"
        for path in self.libraries.iterdir():
            os.utime(path, (1_900_000_000, 1_900_000_000))

        self.package(second)

        self.assertEqual(self.output.read_bytes(), second.read_bytes())
        self.assertEqual(0o644, stat.S_IMODE(self.output.stat().st_mode))
        self.assertEqual(0o644, stat.S_IMODE(Path(str(self.output) + ".properties").stat().st_mode))
        sidecar = dict(
            line.split("=", 1)
            for line in Path(str(self.output) + ".properties").read_text(encoding="ascii").splitlines()
        )
        self.assertEqual(hashlib.sha256(self.output.read_bytes()).hexdigest(), sidecar["jar.sha256"])
        self.assertEqual(self.metadata["platform"], sidecar["platform"])
        with zipfile.ZipFile(self.output) as archive:
            for entry in archive.infolist():
                self.assertEqual((1980, 1, 1, 0, 0, 0), entry.date_time)
                self.assertEqual(0o100644, entry.external_attr >> 16)
                self.assertEqual(zipfile.ZIP_DEFLATED, entry.compress_type)
            manifest = self.manifest(archive)
            self.assertEqual(self.metadata["storage.revision"], manifest["storage.revision"])
            self.assertEqual(
                hashlib.sha256(self.provenance.read_bytes()).hexdigest(), manifest["provenance.sha256"],
            )
            for name in manifest["libraries"].split(","):
                self.assertEqual(
                    hashlib.sha256(archive.read(self.prefix + name)).hexdigest(), manifest[f"sha256.{name}"],
                )

    def test_failed_validation_does_not_replace_existing_archive(self):
        self.package()
        before = self.output.read_bytes()
        sidecar = Path(str(self.output) + ".properties")
        before_sidecar = sidecar.read_bytes()
        self.compile("libdifferent.so", "int dependency(void) { return 0; }", soname="libdependency.so.1")

        with self.assertRaises(ValueError):
            self.package()

        self.assertEqual(before, self.output.read_bytes())
        self.assertEqual(before_sidecar, sidecar.read_bytes())

    def test_build_locks_profiles_dependency_pins_and_source_patches_are_embedded_verbatim(self):
        evidence = self.root / "evidence"
        records = {
            "build/conan.lock": b'{"version":"0.5","requires":[]}\n',
            "build/host-profile": b"[settings]\nos=Linux\narch=x86_64\n",
            "build/storage-source-files.json": b'{"cpp/source.cc":"sample-digest"}\n',
            "build/storage-working-tree.patch": b"--- a/file.cc\n+++ b/file.cc\n@@ -1 +1 @@\n-old\n+new\n",
            "build/direct-references.json": b'{"grpc":"grpc/1#fixed"}\n',
            "build/dependency-input.json": b'{"references":{"grpc":"grpc/1#fixed"}}\n',
        }
        for name, content in records.items():
            path = evidence / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)

        self.package(evidence=evidence)

        with zipfile.ZipFile(self.output) as archive:
            prefix = "META-INF/milvus-native/provenance/"
            embedded = {name[len(prefix):] for name in archive.namelist() if name.startswith(prefix)}
            self.assertEqual(set(records), embedded)
            for name, content in records.items():
                self.assertEqual(content, archive.read(prefix + name))

    def test_absolute_build_paths_are_rejected_from_delivered_provenance(self):
        self.metadata["sourceDirectories"] = {"storage": "/work/sources/milvus-storage"}
        self.write_provenance()

        with self.assertRaisesRegex(ValueError, "absolute build path.*sourceDirectories.storage"):
            self.package()

        self.assertFalse(self.output.exists())

    def test_provenance_url_cannot_contain_user_information(self):
        self.metadata["source.repository"] = "https://builder:secret@example.invalid/source.git"
        self.write_provenance()

        with self.assertRaisesRegex(ValueError, "URL user information.*source.repository"):
            self.package()

        self.assertFalse(self.output.exists())

    def test_path_dependent_build_evidence_is_not_embedded(self):
        evidence = self.root / "evidence/build"
        evidence.mkdir(parents=True)
        (evidence / "conan-graph.json").write_text(
            '{"package_folder":"/home/builder/.conan2/p/example"}\n', encoding="ascii",
        )

        with self.assertRaisesRegex(ValueError, "Unexpected delivery evidence: build/conan-graph.json"):
            self.package(evidence=evidence.parent)

        self.assertFalse(self.output.exists())

    def test_evidence_symlink_escape_does_not_replace_existing_artifact(self):
        self.package()
        before = self.output.read_bytes()
        sidecar = Path(str(self.output) + ".properties")
        before_sidecar = sidecar.read_bytes()
        evidence = self.root / "evidence"
        evidence.mkdir()
        outside = self.root / "outside.patch"
        outside.write_text("not part of the reviewed build evidence", encoding="ascii")
        (evidence / "escaped.patch").symlink_to("../outside.patch")

        with self.assertRaisesRegex(ValueError, "Invalid evidence path: escaped.patch"):
            self.package(evidence=evidence)

        self.assertEqual(before, self.output.read_bytes())
        self.assertEqual(before_sidecar, sidecar.read_bytes())
        self.assertEqual({"bundle.jar"}, {path.name for path in self.root.glob("*.jar")})


if __name__ == "__main__":
    unittest.main()
