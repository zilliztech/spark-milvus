"""Regression tests for source identity and native dependency staging."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


build = module("build")
stage = module("stage")


class SourceIdentityTest(unittest.TestCase):
    def test_parent_build_directory_does_not_empty_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "build/sources/project"
            source.mkdir(parents=True)
            (source / "source.cpp").write_text("source")
            (source / "build").mkdir()
            (source / "build/generated.o").write_text("generated")
            self.assertEqual({"source.cpp": build.digest(source / "source.cpp")}, build.tree_hashes(source))

    def test_empty_manifest_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(RuntimeError, "empty"):
                build.tree_hashes(Path(temporary))

    def test_cmake_trace_rejects_nested_upstream_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace = root / "trace.jsonl"
            trace.write_text(json.dumps({"file": str(root / "knowhere/thirdparty/faiss/CMakeLists.txt"),
                                         "cmd": "add_library"}) + "\n")
            with self.assertRaisesRegex(RuntimeError, "executed upstream CMake"):
                build.check_cmake_trace(trace, [root / "knowhere"])

    def test_cmake_trace_allows_external_dependency_toolkit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace = root / "trace.jsonl"
            trace.write_text(json.dumps({"version": {"major": 1, "minor": 2}}) + "\n" +
                             json.dumps({"file": str(root / "cmake/Corrosion.cmake"),
                                         "cmd": "add_custom_command"}) + "\n")
            self.assertEqual(1, build.check_cmake_trace(trace, [root / "knowhere", root / "storage"]))

    def test_empty_cmake_trace_does_not_prove_independence(self):
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / "trace.jsonl"
            trace.write_text(json.dumps({"version": {"major": 1, "minor": 2}}) + "\n")
            with self.assertRaisesRegex(RuntimeError, "no executed commands"):
                build.check_cmake_trace(trace, [])


@unittest.skipUnless(shutil.which("gcc") and shutil.which("patchelf"), "native compiler and patchelf required")
class NativeStageTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def library(self, directory, filename, soname, body, dependencies=()):
        directory.mkdir(parents=True, exist_ok=True)
        source = directory / (filename + ".c")
        source.write_text(body)
        path = directory / filename
        subprocess.run(["gcc", "-shared", "-fPIC", str(source), "-o", str(path),
                        "-Wl,-soname," + soname, "-Wl,-rpath,/unusable/conan/cache", *dependencies], check=True)
        return path

    def test_aliases_preserved_and_only_staged_rpath_changed(self):
        library = self.library(self.root / "source", "libfixture.so.1.2", "libfixture.so.1",
                               "int fixture(void) { return 7; }")
        alias = library.parent / "libfixture.so"
        alias.symlink_to(library.name)
        original = stage.digest(library)
        providers, _ = stage.inventory({"graph": {"nodes": {}}}, [library, alias])
        selected, aliases = stage.stage(providers, ["libfixture.so.1"], self.root / "lib")
        self.assertEqual(original, stage.digest(library))
        self.assertEqual("libfixture.so.1", aliases["libfixture.so"])
        self.assertEqual("$ORIGIN", subprocess.check_output(
            ["patchelf", "--print-rpath", str(self.root / "lib/libfixture.so.1")], text=True).strip())
        self.assertEqual({"libfixture.so.1"}, set(selected))

    def test_same_soname_with_different_binaries_rejected(self):
        first = self.library(self.root / "first", "libfixture.so", "libfixture.so.1",
                             "int fixture(void) { return 1; }")
        second = self.library(self.root / "second", "libfixture.so", "libfixture.so.1",
                              "int fixture(void) { return 2; }")
        with self.assertRaisesRegex(ValueError, "Conflicting implementations"):
            stage.inventory({"graph": {"nodes": {}}}, [first, second])

    def test_entry_filename_alias_to_versioned_soname_is_preserved(self):
        entry = self.library(
            self.root / "source", "libentry.so", "libentry.so.1",
            "int entry(void) { return 7; }",
        )
        providers, _ = stage.inventory({"graph": {"nodes": {}}}, [entry])

        selected, aliases = stage.stage(providers, ["libentry.so"], self.root / "lib")

        self.assertEqual({"libentry.so"}, set(selected))
        self.assertEqual("libentry.so", aliases["libentry.so.1"])
        self.assertEqual("libentry.so", (self.root / "lib/libentry.so.1").readlink().as_posix())

    def test_unselected_dependency_rejected(self):
        dependency = self.library(self.root / "source", "libdependency.so", "libdependency.so.1",
                                  "int dependency(void) { return 7; }")
        entry = self.library(self.root / "source", "libentry.so", "libentry.so",
                             "extern int dependency(void); int entry(void) { return dependency(); }",
                             [str(dependency)])
        providers, _ = stage.inventory({"graph": {"nodes": {}}}, [entry])
        with self.assertRaisesRegex(ValueError, "outside the unified Conan graph"):
            stage.stage(providers, ["libentry.so"], self.root / "lib")


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ("gcc", "readelf", "ldd", "nm")),
    "native compiler and ELF inspection tools required",
)
class NativeAuditTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.libraries = self.root / "lib"
        self.libraries.mkdir()
        self.source_count = 0

    def library(self, filename, body, dependencies=()):
        self.source_count += 1
        source = self.root / ("source-" + str(self.source_count) + ".c")
        source.write_text(body)
        path = self.libraries / filename
        subprocess.run(
            ["gcc", "-shared", "-fPIC", str(source), f"-L{self.libraries}",
             "-Wl,-rpath,$ORIGIN", "-Wl,-soname," + filename,
             *("-l:" + dependency for dependency in dependencies), "-o", str(path)],
            check=True,
        )
        return path

    def complete_roots(self):
        self.library("libhost.so", "int host_callback(void) { return 9; }")
        plugin = self.library(
            "libplugin.so",
            "extern int host_callback(void); int plugin(void) { return host_callback(); }",
            ("libhost.so",),
        )
        self.library("libknowhere.so", "int knowhere_engine(void) { return 3; }")
        self.library(
            "libknowhere_c.so.1",
            "extern int knowhere_engine(void); int knowhere_c(void) { return knowhere_engine(); }",
            ("libknowhere.so",),
        )
        self.library(
            "libknowhere_jni.so",
            "extern int plugin(void); extern int host_callback(void); "
            "int vector_entry(void) { return plugin() + host_callback(); }",
            ("libplugin.so", "libhost.so"),
        )
        self.library("libmilvus-storage.so", "int storage_engine(void) { return 7; }")
        self.library(
            "libmilvus-storage-jni.so",
            "extern int storage_engine(void); int storage_entry(void) { return storage_engine(); }",
            ("libmilvus-storage.so",),
        )
        return plugin

    def test_every_library_is_relocated_and_all_native_entries_are_loaded(self):
        plugin = self.complete_roots()
        standalone = subprocess.run(
            ["ldd", "-r", str(plugin)], text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, check=False,
        )
        self.assertNotIn("undefined symbol:", standalone.stdout)
        output = self.root / "audit"

        stage.audit(self.libraries, sorted(path.name for path in self.libraries.iterdir()),
                    stage.JVM_LOAD_ENTRIES, stage.AUDIT_DLOPEN_ENTRIES, output)

        results = json.loads((output / "results.json").read_text())
        self.assertEqual(set(path.name for path in self.libraries.iterdir()), set(results))
        for name in stage.AUDIT_DLOPEN_ENTRIES:
            expected_context = "jvm-load-entry" if name in stage.JVM_LOAD_ENTRIES else "native-load-entry"
            self.assertEqual(expected_context, results[name]["context"])
            self.assertTrue(results[name]["relocationsPassed"])
            self.assertEqual(0, results[name]["dlopenExit"])
            self.assertTrue(results[name]["passed"])
        orders = json.loads((output / "load-orders.json").read_text())
        self.assertEqual(2, len(orders))
        self.assertTrue(all(record["exit"] == 0 for record in orders))

    def test_missing_load_entry_is_rejected(self):
        self.library("libmilvus-storage-jni.so", "int storage_entry(void) { return 7; }")
        with self.assertRaisesRegex(ValueError, "missing load entries: libknowhere"):
            stage.audit(self.libraries, ["libmilvus-storage-jni.so"],
                        stage.JVM_LOAD_ENTRIES, stage.AUDIT_DLOPEN_ENTRIES, self.root / "audit")

    def test_dlopen_failure_is_recorded_as_failed(self):
        self.complete_roots()
        self.library(
            "libknowhere_jni.so",
            "#include <stdlib.h>\n"
            "__attribute__((constructor)) static void fail_load(void) { exit(17); }\n"
            "int vector_entry(void) { return 1; }",
        )
        output = self.root / "audit"

        with self.assertRaisesRegex(ValueError, "Strict native audit failed: libknowhere_jni.so"):
            stage.audit(self.libraries, sorted(path.name for path in self.libraries.iterdir()),
                        stage.JVM_LOAD_ENTRIES, stage.AUDIT_DLOPEN_ENTRIES, output)

        result = json.loads((output / "results.json").read_text())["libknowhere_jni.so"]
        self.assertTrue(result["relocationsPassed"])
        self.assertEqual(17, result["dlopenExit"])
        self.assertFalse(result["passed"])

    def test_unresolved_ordinary_dependency_fails_the_audit(self):
        self.complete_roots()
        self.library(
            "libbroken.so",
            "extern int missing(void); int broken(void) { return missing(); }",
        )

        with self.assertRaisesRegex(ValueError, "Strict native audit failed: libbroken.so"):
            stage.audit(self.libraries, sorted(path.name for path in self.libraries.iterdir()),
                        stage.JVM_LOAD_ENTRIES, stage.AUDIT_DLOPEN_ENTRIES, self.root / "audit")

        result = json.loads((self.root / "audit/results.json").read_text())["libbroken.so"]
        self.assertFalse(result["relocationsPassed"])


if __name__ == "__main__":
    unittest.main()
