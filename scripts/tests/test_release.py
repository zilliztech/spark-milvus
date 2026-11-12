"""Exercise the consumer-visible Maven graph and immutable release boundary."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import struct
import tempfile
import unittest
from unittest import mock
import zipfile


SPEC = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[1] / "release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

RELEASE_ROWS = [
    dict(line=line, project="spark", spark=consumer, compileSpark=compile_version,
         scala=scala, scalaBinary=".".join(scala.split(".")[:2]),
         java="17" if line == "3.5" else "21", buildJava="21")
    for line, consumer, compile_version, scala in (
        ("3.5", "3.5.5", "3.5.5", "2.12.20"),
        ("3.5", "3.5.5", "3.5.5", "2.13.16"),
        ("4.0", "4.0.1", "4.0.0", "2.13.16"),
        ("4.1", "4.1.1", "4.1.1", "2.13.16"),
        ("4.2", "4.2.0", "4.2.0", "2.13.16"),
    )
]


class ReleaseTest(unittest.TestCase):
    DEPLOYMENT = "12345678-1234-4234-9234-123456789abc"
    SIGNING_KEY = release.SIGNING_FINGERPRINT

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.inputs = self.root / "inputs"
        self.inputs.mkdir()
        self.output = self.root / "candidate"
        self.plan = json.loads(release.MATRIX.read_text())
        self.plan["jvm"] = copy.deepcopy(RELEASE_ROWS)
        self.build_matrix = self.root / "build-matrix.json"
        release.write_json(self.build_matrix, {"schemaVersion": 1, "release": RELEASE_ROWS,
            "spark": [dict(row, spark=row["compileSpark"]) for row in RELEASE_ROWS]})
        self.plan.update(version="3.0.0-rc.1", sourceSha="a" * 40,
                         pins={"milvus-storage": "b" * 40, "milvus-proto": "d" * 40})
        self.plan_path = self.root / "plan.json"
        release.write_json(self.plan_path, self.plan)
        self.arrow_supplement()
        for native in self.plan["native"]:
            self.native(native["platform"])
        for jvm in self.plan["jvm"]:
            self.jvm(jvm["line"], jvm["scalaBinary"])

    def arrow_supplement(self):
        arrow = release.arrow_cdata
        sources = self.root / "arrow-sources"
        for name in arrow.SOURCE_FILES:
            path = sources / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("source: " + name)
        source_pins = mock.patch.object(arrow, "SOURCE_SHA256", {
            name: arrow.sha256((sources / name).read_bytes()) for name in arrow.SOURCE_FILES})
        source_pins.start()
        self.addCleanup(source_pins.stop)
        library = self.root / "libarrow_cdata_jni.so"
        payload = bytearray(64)
        payload[:7] = b"\x7fELF\x02\x01\x01"
        struct.pack_into("<HH", payload, 16, 3, 183)
        library.write_bytes(payload)
        self.arrow_abi = {
            "elfMachine": 183, "soname": library.name,
            "needed": ["libc.so.6"], "versionNeeds": {"libc.so.6": ["GLIBC_2.17"]},
            "maxRequired": {"GLIBC": "2.17", "GLIBCXX": None, "CXXABI": None}, "violations": [],
        }
        # Pin the deterministic fixture sources above; keep resource hashes,
        # ELF identity and the evidence contract real. Only readelf is replaced
        # for this deliberately minimal ELF.
        inspector = mock.patch.object(arrow, "inspect_library", return_value=self.arrow_abi)
        self.arrow_inspector = inspector.start()
        self.addCleanup(inspector.stop)
        supplement = self.root / "arrow-supplement.jar"
        arrow.write_jar(supplement, sources, library, {
            "compiler": "g++ 12", "compilerTarget": "aarch64-linux-gnu",
            "cmake": "cmake 3.22", "javac": "javac 21"})
        with zipfile.ZipFile(supplement) as archive:
            self.arrow_entries = {name: archive.read(name) for name in archive.namelist()}

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
            "format.version": "2", "platform": platform, "components": "storage",
            "storage.revision": storage or self.plan["pins"]["milvus-storage"],
            "libraries": library,
            "sha256." + library: hashlib.sha256(b"native test payload").hexdigest(),
            "provenance.sha256": hashlib.sha256(provenance).hexdigest(),
        }
        prefix = f"native/milvus/2/{platform}/"
        self.jar(path, {prefix + library: b"native test payload",
                        prefix + "manifest.properties": "".join(f"{key}={value}\n" for key, value in manifest.items()),
                        "META-INF/milvus-native/provenance.json": provenance,
                        "META-INF/milvus-native/licenses/LICENSE": "fixture license"})
        Path(str(path) + ".properties").write_text(f"platform={platform}\njar.sha256={release.digest(path)}\n")
        row = next(item for item in self.plan["native"] if item["platform"] == platform)
        baseline = {key: row[field] for key, field in release.ABI_BASELINE_KEYS[release.platform_family(platform)]}
        release.write_json(path.parent / "abi.json", {
            "platform": platform, "jar.sha256": release.digest(path), "passed": True,
            "baseline": baseline, "violations": []})
        return path

    def jvm(self, line, binary="2.13", omit_api=False):
        artifact = f"spark-milvus-{line}-jvm_{binary}"
        row = next(item for item in self.plan["jvm"] if release.jvm_selection(item) == (line, binary))
        directory = self.inputs / line / binary
        records = []
        for classifier in ("", "sources", "javadoc"):
            suffix = "-" + classifier if classifier else ""
            path = directory / f"{artifact}-{self.plan['version']}{suffix}.jar"
            contents = {"README.md": "fixture"}
            if classifier == "sources":
                contents.update({name: "source fixture" for names in release.API_SOURCES.values() for name in names})
            if not classifier:
                contents.update({
                    release.REQUIREMENTS: "format.version=2\nrevision=" + "b" * 40 + "\nentry=milvus-storage-jni\n",
                    release.REQUIREMENTS_LIST: "storage\n",
                })
                contents.update({name: b"class fixture"
                                 for names in release.JVM_CONTRACT["requiredEntries"].values() for name in names
                                 if name.endswith(".class")
                                 and not (omit_api and name.startswith("io/milvus/storage/"))})
                if line == "3.5":
                    contents.update(self.arrow_entries)
            self.jar(path, contents)
            records.append(dict(classifier=classifier, path=path.name, extension="jar", sha256=release.digest(path),
                                size=path.stat().st_size))
        release.write_json(directory / "artifact.json", dict(
            formatVersion=1, artifactId=artifact, groupId="com.zilliz", version=self.plan["version"],
            sourceSha=self.plan["sourceSha"], sparkLine=line, sparkVersion=row["compileSpark"],
            scalaVersion=row["scala"], scalaBinaryVersion=binary, files=records,
            providedDependencies=[dict(groupId="org.apache.spark", artifactId=f"spark-{name}_{binary}",
                                       version=row["compileSpark"]) for name in ("core", "sql", "catalyst")],
            bundledComponents=(release.bundled_components(json.loads(self.arrow_entries[release.arrow_cdata.PROVENANCE]))
                               if line == "3.5" else []),
            runtimeDependencies=[]))

    def rewrite_jvm_export(self, line, binary, transform, classifier=""):
        metadata_path = self.inputs / line / binary / "artifact.json"
        metadata = json.loads(metadata_path.read_text())
        record = next(item for item in metadata["files"] if item["classifier"] == classifier)
        path = metadata_path.parent / record["path"]
        transform(path)
        record.update(sha256=release.digest(path), size=path.stat().st_size)
        release.write_json(metadata_path, metadata)

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

    def test_all_entries_share_the_exact_matching_jvm_and_native_artifacts(self):
        manifest = self.stage()
        # Five profiles each publish a JVM bundle, universal entry and three
        # platform entries. All profiles share the three native artifacts.
        self.assertEqual(len(manifest["artifacts"]), 28 * 4)
        self.assertEqual(len(release.expected_artifacts(self.plan)), 28)
        self.assertEqual({item["kind"] for item in manifest["artifacts"]}, {"entry", "jvm", "native"})
        entries = [item for item in manifest["artifacts"] if item["kind"] == "entry" and item["extension"] == "pom"]
        self.assertEqual(len(entries), 5 * (1 + 3))
        for row in RELEASE_ROWS:
            line, binary = release.jvm_selection(row)
            for platform, count in ((None, 4), ("linux-x86_64", 2)):
                deps = release.entry_dependencies(self.plan, line, binary, platform)
                self.assertEqual(len(deps), count)
                self.assertEqual(deps[0]["artifactId"], f"spark-milvus-{line}-jvm_{binary}")
        self.assertNotIn("thin", manifest)
        self.assertFalse(any("thin" in item["artifactId"] or item["artifactId"].startswith((
            "spark-milvus-core_", "spark-milvus-native-storage_", "spark-milvus-native-runtime"))
            for item in manifest["artifacts"]))
        with self.assertRaisesRegex(ValueError, "never overwrite"):
            self.stage()

    def test_spark_35_scala_variants_do_not_collide_in_staging_or_dependencies(self):
        manifest = self.stage()
        for binary in ("2.12", "2.13"):
            artifact = f"spark-milvus-3.5-jvm_{binary}"
            record = next(item for item in manifest["artifacts"]
                          if item["artifactId"] == artifact and item["extension"] == "pom")
            self.assertEqual(record["scalaBinary"], binary)
            _, deps = release.pom_dependencies(self.output / "repository" / record["path"])
            self.assertEqual({dep["artifactId"] for dep in deps},
                             {f"spark-{name}_{binary}" for name in ("core", "sql", "catalyst")})
            self.assertTrue(all(dep["version"] == "3.5.5" and dep["scope"] == "provided" for dep in deps))

    def test_duplicate_jvm_metadata_cannot_replace_a_scala_variant(self):
        source = self.inputs / "3.5" / "2.12" / "artifact.json"
        destination = self.inputs / "3.5" / "2.13" / "artifact.json"
        shutil.copyfile(source, destination)
        with self.assertRaisesRegex(ValueError, "exactly one JVM build"):
            self.stage()

    def test_wrong_scala_artifact_and_build_version_are_rejected(self):
        path = self.inputs / "3.5" / "2.12" / "artifact.json"
        original = json.loads(path.read_text())
        for field, value in (("artifactId", "spark-milvus-3.5-jvm_2.13"),
                             ("scalaVersion", "2.13.16"), ("sparkVersion", "3.5.6"),
                             ("formatVersion", 2)):
            with self.subTest(field=field):
                release.write_json(path, dict(original, **{field: value}))
                with self.assertRaisesRegex(ValueError, "identity mismatch"):
                    self.stage()

    def test_malformed_jvm_file_membership_and_size_are_rejected(self):
        path = self.inputs / "3.5" / "2.12" / "artifact.json"
        original = json.loads(path.read_text())
        for mutate, message in (
            (lambda files: files.pop(), "Incomplete JVM artifact set"),
            (lambda files: files[0].update(extension="pom"), "Incomplete JVM artifact set"),
            (lambda files: files[0].update(size=0), "JVM build checksum mismatch"),
        ):
            metadata = copy.deepcopy(original)
            mutate(metadata["files"])
            release.write_json(path, metadata)
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.stage()

    def test_old_thin_exports_and_candidate_metadata_are_rejected(self):
        for name in ("thin-artifacts.json", "spark-milvus-4.0-thin_2.13-3.0.0.jar",
                     "spark-milvus-core_2.13-3.0.0.jar", "spark-milvus-native-storage_2.12-3.0.0.jar",
                     "spark-milvus-native-runtime-3.0.0.jar"):
            path = self.inputs / "legacy" / name
            release.write_json(path, {"kind": "thin"})
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Unexpected legacy Thin artifact input"):
                self.stage()
            path.unlink()
        manifest = self.stage()
        manifest["thin"] = []
        release.write_json(self.output / "release-manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "Unexpected legacy Thin candidate metadata"):
            release.verify(self.output)

    def test_candidate_rechecks_jvm_spark_version_and_binary_after_rehash(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-3.5-jvm_2.12" and item["extension"] == "pom")
        for binary, version in (("2.13", "3.5.5"), ("2.12", "3.5.6")):
            deps = [dict(groupId="org.apache.spark", artifactId=f"spark-{name}_{binary}",
                         version=version, scope="provided") for name in ("core", "sql", "catalyst")]
            self.rewrite_file(manifest, record, release.pom_bytes(record["artifactId"], self.plan, deps))
            with self.subTest(binary=binary, version=version), self.assertRaisesRegex(ValueError, "Spark"):
                release.verify(self.output)

    def test_jvm_spark_dependencies_must_remain_provided(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"] if item["kind"] == "jvm" and item["extension"] == "pom")
        deps = [dict(groupId="org.apache.spark", artifactId="spark-sql_2.12", version="3.5.5")]
        self.rewrite_file(manifest, record, release.pom_bytes(record["artifactId"], self.plan, deps))
        with self.assertRaisesRegex(ValueError, "provided by the Spark runtime"):
            release.verify(self.output)

    def test_jvm_jar_must_not_embed_spark_or_native_classes(self):
        for name in ("org/apache/spark/sql/Dataset.class", "org/apache/hadoop/conf/Configuration.class",
                     "native/milvus/2/linux-x86_64/libtest.so", "META-INF/milvus-native/provenance.json"):
            self.jvm("3.5", "2.12")
            def mutate(path):
                with zipfile.ZipFile(path) as archive:
                    contents = {entry: archive.read(entry) for entry in archive.namelist()}
                self.jar(path, contents | {name: b"unexpected"})
            self.rewrite_jvm_export("3.5", "2.12", mutate)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "forbidden resources"):
                self.stage()

    def test_every_release_profile_requires_all_module_apis_not_only_package_prefixes(self):
        for row in self.plan["jvm"]:
            line, binary = release.jvm_selection(row)
            path = self.inputs / line / binary / f"spark-milvus-{line}-jvm_{binary}-{self.plan['version']}.jar"
            with zipfile.ZipFile(path) as archive:
                original = {name: archive.read(name) for name in archive.namelist()}
            for module, names in release.JVM_CONTRACT["requiredEntries"].items():
                for missing in (name for name in names if name.endswith(".class")):
                    entries = dict(original)
                    entries.pop(missing)
                    # A placeholder in the right package must not satisfy a
                    # concrete API requirement, unlike the old prefix check.
                    entries[missing.rpartition("/")[0] + "/Placeholder.class"] = b"unrelated class"
                    self.jar(path, entries)
                    with self.subTest(line=line, binary=binary, missing=missing), \
                            self.assertRaisesRegex(ValueError, "missing " + module + " API"):
                        release.verify_jvm(path, self.plan, row)
            self.jar(path, original)

    def test_candidate_rejects_missing_modules_after_all_checksums_are_rewritten(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-4.1-jvm_2.13"
                      and item["extension"] == "jar" and not item["classifier"])
        path = self.output / "repository" / record["path"]
        with zipfile.ZipFile(path) as archive:
            original = {name: archive.read(name) for name in archive.namelist()}
        for module, packages in release.PUBLIC_PACKAGES.items():
            entries = {name: content for name, content in original.items()
                       if not (name.endswith(".class") and name.rpartition("/")[0] in packages)}
            self.jar(path, entries)
            self.rewrite_file(manifest, record, path.read_bytes())
            with self.subTest(module=module), self.assertRaisesRegex(ValueError, "missing " + module + " API"):
                release.verify(self.output)

    def test_candidate_rejects_unapproved_native_binaries_after_all_checksums_are_rewritten(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-4.1-jvm_2.13"
                      and item["extension"] == "jar" and not item["classifier"])
        path = self.output / "repository" / record["path"]
        with zipfile.ZipFile(path) as archive:
            original = {name: archive.read(name) for name in archive.namelist()}
        for name in ("libunexpected.so", "nested/libunexpected.so.1.2", "libunexpected.dylib", "unexpected.dll",
                     "libunexpected.jnilib", "META-INF/native/libunexpected.so",
                     "arrow_cdata_jni/x86_64/libunexpected.so", "unexpected/x86_64/libarrow_cdata_jni.so",
                     "arrow_cdata_jni/x86_64/libarrow_cdata_jni.so.1"):
            self.jar(path, original | {name: b"unexpected native binary"})
            self.rewrite_file(manifest, record, path.read_bytes())
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "forbidden resources"):
                release.verify(self.output)

    def test_upstream_arrow_and_grpc_jni_resources_remain_allowed(self):
        def add_jni(path):
            with zipfile.ZipFile(path) as archive:
                entries = {name: archive.read(name) for name in archive.namelist()}
            names = ("x86_64/libarrow_cdata_jni.so", "x86_64/libarrow_cdata_jni.dylib",
                     "arrow_cdata_jni/aarch_64/libarrow_cdata_jni.so",
                     "arrow_cdata_jni/x86_64/arrow_cdata_jni.dll",
                     "META-INF/native/libio_grpc_netty_shaded_netty_tcnative_linux_x86_64.so",
                     "META-INF/native/libio_grpc_netty_shaded_netty_transport_native_epoll_aarch_64.so")
            entries.update({name: b"upstream JNI fixture" for name in names})
            self.jar(path, entries)
        self.rewrite_jvm_export("4.1", "2.13", add_jni)
        self.stage()

    def test_bundled_source_build_is_discoverable_and_bound_to_embedded_provenance(self):
        manifest = self.stage()
        for record in (item for item in manifest["artifacts"]
                       if item["kind"] == "jvm" and item["extension"] == "jar" and not item["classifier"]):
            components = record["bundledComponents"]
            self.assertEqual(bool(components), record["line"] == "3.5")
            if components:
                self.assertEqual(components[0]["name"], "Apache Arrow C Data JNI")
                self.assertEqual(components[0]["source"], release.arrow_cdata.source_identity())
                self.assertEqual(components[0]["resource"], release.arrow_cdata.RESOURCE)
        record = next(item for item in manifest["artifacts"] if item.get("bundledComponents"))
        original = copy.deepcopy(record["bundledComponents"])
        for change in ([], [dict(original[0], sha256="0" * 64)], [dict(original[0], licenses=[])]):
            record["bundledComponents"] = change
            release.write_json(self.output / "release-manifest.json", manifest)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "bundled component inventory"):
                release.verify(self.output)

    def test_stage_rejects_missing_or_incorrect_bundled_component_inventory(self):
        for line, binary in (("3.5", "2.12"), ("3.5", "2.13"), ("4.1", "2.13")):
            path = self.inputs / line / binary / "artifact.json"
            original = json.loads(path.read_text())
            changes = [None, []] if line == "3.5" else [None, [{"name": "unexpected component"}]]
            for value in changes:
                metadata = dict(original, bundledComponents=value)
                release.write_json(path, metadata)
                with self.subTest(line=line, binary=binary, value=value), \
                        self.assertRaisesRegex(ValueError, "bundled component inventory"):
                    self.stage()
            release.write_json(path, original)

    def test_spark_35_supplement_accepts_assembly_directories_but_rejects_other_version_files(self):
        def add_directories(path):
            with zipfile.ZipFile(path) as archive:
                entries = {name: archive.read(name) for name in archive.namelist()}
            for name in tuple(entries):
                for parent in Path(name).parents:
                    if str(parent) != ".":
                        entries[str(parent) + "/"] = b""
            self.jar(path, entries)
        for binary in ("2.12", "2.13"):
            self.rewrite_jvm_export("3.5", binary, add_directories)
        manifest = self.stage()
        self.assertEqual(len(manifest["artifacts"]), 28 * 4)
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-3.5-jvm_2.12"
                      and item["extension"] == "jar" and not item["classifier"])
        path = self.output / "repository" / record["path"]
        with zipfile.ZipFile(path) as archive:
            entries = {name: archive.read(name) for name in archive.namelist()}
        entries["META-INF/spark-milvus/arrow-cdata/99.0.0/linux-aarch64.json"] = b"unexpected version"
        self.jar(path, entries)
        self.rewrite_file(manifest, record, path.read_bytes())
        with self.assertRaisesRegex(ValueError, "Unexpected Arrow C Data supplement version"):
            release.verify(self.output)

    def test_spark_35_candidate_requires_arm_library_and_provenance_for_both_scala_versions(self):
        for binary in ("2.12", "2.13"):
            for missing in (release.arrow_cdata.RESOURCE, release.arrow_cdata.PROVENANCE):
                self.jvm("3.5", binary)
                def mutate(path):
                    with zipfile.ZipFile(path) as archive:
                        entries = {name: archive.read(name) for name in archive.namelist() if name != missing}
                    self.jar(path, entries)
                self.rewrite_jvm_export("3.5", binary, mutate)
                with self.subTest(binary=binary, missing=missing), self.assertRaisesRegex(ValueError, "supplement resources"):
                    self.stage()
            self.jvm("3.5", binary)

    def test_spark_35_candidate_rechecks_supplement_bytes_and_source_identity_after_rehash(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-3.5-jvm_2.12"
                      and item["extension"] == "jar" and not item["classifier"])
        path = self.output / "repository" / record["path"]
        with zipfile.ZipFile(path) as archive:
            original = {name: archive.read(name) for name in archive.namelist()}
        mutations = (
            (lambda entries: entries.pop(release.arrow_cdata.RESOURCE), "supplement resources"),
            (lambda entries: entries.update({release.arrow_cdata.RESOURCE: b"corrupted ELF"}), "library checksum"),
            (lambda entries: entries.update({release.arrow_cdata.PREFIX + "source/LICENSE.txt": b"wrong source"}),
             "source checksum"),
            (lambda entries: entries.update({release.arrow_cdata.PROVENANCE: json.dumps({
                **json.loads(entries[release.arrow_cdata.PROVENANCE]),
                "source": dict(release.arrow_cdata.source_identity(), revision="0" * 40)}).encode()}),
             "source identity"),
        )
        for mutate, message in mutations:
            entries = dict(original)
            mutate(entries)
            self.jar(path, entries)
            self.rewrite_file(manifest, record, path.read_bytes())
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                release.verify(self.output)

    def test_spark_35_candidate_rejects_modified_source_with_rehashed_provenance(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-3.5-jvm_2.12"
                      and item["extension"] == "jar" and not item["classifier"])
        path = self.output / "repository" / record["path"]
        with zipfile.ZipFile(path) as archive:
            entries = {name: archive.read(name) for name in archive.namelist()}
        arrow = release.arrow_cdata
        source = "java/c/src/main/cpp/jni_wrapper.cc"
        changed = b"modified JNI source with rewritten provenance"
        entries[arrow.PREFIX + "source/" + source] = changed
        provenance = json.loads(entries[arrow.PROVENANCE])
        provenance["sourceFiles"][source] = arrow.sha256(changed)
        entries[arrow.PROVENANCE] = json.dumps(provenance).encode()
        self.jar(path, entries)
        self.rewrite_file(manifest, record, path.read_bytes())
        with self.assertRaisesRegex(ValueError, "source inventory differs from the Apache release"):
            release.verify(self.output)

    def test_spark_35_candidate_repeats_actual_linux_abi_inspection(self):
        manifest = self.stage()
        with mock.patch.object(release.arrow_cdata.platform, "system", return_value="Linux"):
            self.arrow_inspector.return_value = dict(self.arrow_abi, needed=["libunexpected.so"])
            with self.assertRaisesRegex(ValueError, "ABI evidence mismatch"):
                release.verify(self.output)

    def test_other_spark_lines_reject_arrow_12_supplement_even_after_rehash(self):
        manifest = self.stage()
        for line in ("4.0", "4.1", "4.2"):
            record = next(item for item in manifest["artifacts"]
                          if item["artifactId"] == f"spark-milvus-{line}-jvm_2.13"
                          and item["extension"] == "jar" and not item["classifier"])
            path = self.output / "repository" / record["path"]
            with zipfile.ZipFile(path) as archive:
                original = {name: archive.read(name) for name in archive.namelist()}
            for name in (release.arrow_cdata.RESOURCE, release.arrow_cdata.PROVENANCE):
                self.jar(path, original | {name: self.arrow_entries[name]})
                self.rewrite_file(manifest, record, path.read_bytes())
                with self.subTest(line=line, name=name), self.assertRaisesRegex(ValueError, "only supported for Spark 3.5"):
                    release.verify(self.output)
            self.jar(path, original)
            self.rewrite_file(manifest, record, path.read_bytes())

    def test_managed_source_roots_preserve_package_ownership(self):
        release.verify_public_classes(["scalapb/io/milvus/grpc/common/KeyValuePair.scala"], "core")
        release.verify_public_classes(["milvus-storage/scala/io/milvus/storage/MilvusStorageReader.scala"], "native-storage")
        for name in ("scalapb/io/milvus/grpc/common/KeyValuePair.class",
                     "scalapb/org/example/Unknown.scala"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "Unowned public"):
                release.verify_public_classes([name], "core")

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
        with self.assertRaisesRegex(ValueError, "missing native-storage API"):
            self.stage()

    def test_jvm_from_another_commit_is_rejected(self):
        path = self.inputs / "4.0" / "2.13" / "artifact.json"
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

    def test_macos_abi_report_must_use_the_deployment_target_baseline(self):
        path = self.inputs / "darwin-aarch64" / "abi.json"
        report = json.loads(path.read_text())
        self.assertEqual(report["baseline"], {"MACOS": "14.0"})
        report["baseline"]["MACOS"] = "15.0"
        release.write_json(path, report)
        with self.assertRaisesRegex(ValueError, "different baseline"):
            self.stage()

    def test_release_matrix_names_every_family_baseline(self):
        platforms = {item["platform"] for item in self.plan["native"]}
        self.assertEqual(platforms, {"linux-x86_64", "linux-aarch64", "darwin-aarch64"})
        release.validate_plan(self.plan)
        for platform in platforms:
            item = next(row for row in self.plan["native"] if row["platform"] == platform)
            self.assertTrue(all(item[field] == value
                                for field, value in release.system_baseline(platform).items()))
        darwin = next(row for row in self.plan["native"] if row["platform"] == "darwin-aarch64")
        self.assertEqual(darwin["runner"], "macos-15")
        self.assertEqual([host["runner"] for host in darwin["consumers"]], ["macos-15"])
        broken = copy.deepcopy(self.plan)
        next(row for row in broken["native"] if row["platform"] == "darwin-aarch64")["macos"] = "13.0"
        with self.assertRaisesRegex(ValueError, "system ABI baseline"):
            release.validate_plan(broken)
        broken = copy.deepcopy(self.plan)
        broken["native"].append({**darwin, "platform": "windows-x86_64"})
        with self.assertRaisesRegex(ValueError, "Unsupported native release platform"):
            release.validate_plan(broken)

    def test_changed_candidate_bytes_are_rejected(self):
        manifest = self.stage()
        path = self.output / "repository" / manifest["artifacts"][0]["path"]
        path.write_bytes(b"replaced artifact")
        with self.assertRaisesRegex(ValueError, "Changed candidate"):
            release.verify(self.output)

    def test_fat_jvm_sdk_classes_are_rejected_even_after_rehash(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["kind"] == "jvm" and item["line"] == "4.2"
                      and item["extension"] == "jar" and not item["classifier"])
        path = self.output / "repository" / record["path"]
        with zipfile.ZipFile(path) as archive:
            original = {name: archive.read(name) for name in archive.namelist()}
        for name in ("io/milvus/client/MilvusServiceClient.class",
                     "io/milvus/v2/client/MilvusClientV2.class",
                     "io/milvus/param/ConnectParam.class", "io/milvus/response/R.class"):
            with self.subTest(name=name):
                self.jar(path, {**original, name: b"unexpected SDK class"})
                self.rewrite_file(manifest, record, path.read_bytes())
                with self.assertRaisesRegex(ValueError, "forbidden resources"):
                    release.verify(self.output)

    def test_sdk_pom_dependencies_are_rejected_even_when_provided_and_rehashed(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-4.1-jvm_2.13" and item["extension"] == "pom")
        path = self.output / "repository" / record["path"]
        _, original = release.pom_dependencies(path)
        for scope in ("compile", "runtime", "provided"):
            for artifact in ("milvus-sdk-java", "milvus-sdk-java_2.13"):
                dependency = dict(groupId="io.milvus", artifactId=artifact, version="3.0.0", scope=scope)
                self.rewrite_file(manifest, record, release.pom_bytes(record["artifactId"], self.plan,
                                                                     original + [dependency]))
                with self.subTest(scope=scope, artifact=artifact), \
                        self.assertRaisesRegex(ValueError, "depends on the official Milvus SDK"):
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
            groupId="com.zilliz", artifactId="unexpected-module_2.13", version=self.plan["version"])])
        self.rewrite_file(manifest, record, wrong)
        with self.assertRaisesRegex(ValueError, "unpublished internal"):
            release.verify(self.output)

    def test_untracked_candidate_file_is_rejected(self):
        self.stage()
        (self.output / "repository" / "extra.jar").write_bytes(b"surprise")
        with self.assertRaisesRegex(ValueError, "untracked files"):
            release.verify(self.output)

    def test_published_coordinates_and_failed_lookups_block_new_candidates(self):
        missing = release.urllib.error.HTTPError("https://example.test", 404, "Not Found", {}, None)
        with mock.patch.object(release.urllib.request, "urlopen", side_effect=missing) as request:
            release.check_unused(self.plan)
        self.assertEqual(request.call_count, len(release.expected_artifacts(self.plan)))
        with mock.patch.object(release.urllib.request, "urlopen", return_value=mock.MagicMock()):
            with self.assertRaisesRegex(ValueError, "already published"):
                release.check_unused(self.plan)
        unavailable = release.urllib.error.HTTPError("https://example.test", 503, "Unavailable", {}, None)
        with mock.patch.object(release.urllib.request, "urlopen", side_effect=unavailable):
            with self.assertRaises(release.urllib.error.HTTPError):
                release.check_unused(self.plan)

    def test_snapshot_and_malformed_versions_are_rejected(self):
        for version in ("3.0.0-SNAPSHOT", "3.0.0-snapshot", "3.0.0/escape", "1.6.1", "2.1.0", "3.0.0-", "3.0.0-migration.4", "3.0.0-ci.123.1"):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "immutable 3.x"):
                release.validate_plan(dict(self.plan, version=version))

    def test_central_failure_does_not_proceed(self):
        with mock.patch.object(release, "central_request", return_value=b'{"deploymentState":"FAILED","errors":{"pom":["bad"]}}'):
            with self.assertRaisesRegex(ValueError, "validation failed"):
                release.await_central(self.DEPLOYMENT, "not-a-token", "VALIDATED")

    def test_bundle_selects_the_only_secret_primary_key(self):
        manifest = self.stage()
        output = self.root / "central.zip"
        commands = []

        def run(command, **kwargs):
            if "--list-secret-keys" in command:
                return mock.Mock(stdout=(
                    "sec:u:4096:1:0123456789ABCDEF:0:0:::::scESC:::\n"
                    f"fpr:::::::::{self.SIGNING_KEY}:\n"
                    "ssb:u:4096:1:FEDCBA9876543210:0:0:::::e:::\n"
                    f"fpr:::::::::{'B' * 40}:\n"))
            commands.append(command)
            signature = Path(command[command.index("--output") + 1])
            signature.write_bytes(b"signature")
            return mock.Mock(returncode=0)

        with mock.patch.dict(release.os.environ, {"GPG_PASSPHRASE": "test-passphrase"}), \
                mock.patch.object(release.subprocess, "run", side_effect=run):
            release.bundle(self.output, output)

        self.assertEqual(len(commands), len(manifest["artifacts"]))
        for command in commands:
            key = command[command.index("--local-user") + 1]
            self.assertEqual(key, self.SIGNING_KEY)
        with zipfile.ZipFile(output) as archive:
            self.assertEqual(len([name for name in archive.namelist() if name.endswith(".asc")]),
                             len(manifest["artifacts"]))

    def test_download_validated_deployment_reconstructs_exact_candidate(self):
        self.stage()
        downloaded = self.root / "downloaded"
        paths = []

        def download(path, token, output):
            self.assertEqual(token, "token")
            relative = path.split("/download/", 1)[1]
            paths.append(relative)
            shutil.copyfile(self.output / "repository" / relative, output)

        with mock.patch.object(release, "central_download", side_effect=download):
            manifest = release.download_deployment(
                self.output, downloaded, self.DEPLOYMENT, "token")

        # Every artifact with its four checksum files.
        self.assertEqual(len(paths), 28 * 4 * 5)
        self.assertEqual(manifest, release.verify(downloaded))

    def test_upload_is_user_managed_and_keeps_the_token_off_argv(self):
        bundle_path = self.root / "central.zip"
        bundle_path.write_bytes(b"signed bundle")
        completed = mock.Mock(returncode=0, stdout=self.DEPLOYMENT + "\n", stderr="")
        with mock.patch.object(release.subprocess, "run", return_value=completed) as run:
            deployment = release.upload_bundle(bundle_path, "secret-token")

        command = run.call_args.args[0]
        self.assertEqual(deployment, self.DEPLOYMENT)
        self.assertEqual(command[-1], release.CENTRAL + "/upload?publishingType=USER_MANAGED")
        self.assertNotIn("secret-token", " ".join(command))
        self.assertEqual(run.call_args.kwargs["input"],
                         'header = "Authorization: Bearer secret-token"\n')
        self.assertTrue(run.call_args.kwargs["capture_output"])

    def test_changed_deployment_bytes_fail_and_are_still_dropped(self):
        self.stage()
        bundle_path = self.root / "central.zip"
        bundle_path.write_bytes(b"signed bundle")
        downloaded = self.root / "downloaded"
        report_path = self.root / "central-test.json"

        def download(candidate, output, deployment, token):
            raise ValueError("Central deployment bytes differ: changed.jar")

        with mock.patch.dict(release.os.environ, {
                "CENTRAL_USERNAME": "central-user", "CENTRAL_TOKEN": "central-secret"}), \
                mock.patch.object(release, "upload_bundle", return_value=self.DEPLOYMENT), \
                mock.patch.object(release, "await_central", return_value={"deploymentState": "VALIDATED"}), \
                mock.patch.object(release, "download_deployment", side_effect=download), \
                mock.patch.object(release, "drop_deployment", return_value=True) as drop, \
                mock.patch.object(release, "promote_deployment") as promote:
            with self.assertRaisesRegex(ValueError, "bytes differ"):
                release.test_publish(bundle_path, self.output, downloaded, report_path)

        drop.assert_called_once_with(self.DEPLOYMENT, mock.ANY)
        promote.assert_not_called()
        report = json.loads(report_path.read_text())
        self.assertTrue(report["dropped"])
        self.assertFalse(report["downloadVerified"])
        self.assertNotIn("central-user", report_path.read_text())
        self.assertNotIn("central-secret", report_path.read_text())

    def test_central_test_records_id_downloads_and_drops_without_promoting(self):
        self.stage()
        bundle_path = self.root / "central.zip"
        bundle_path.write_bytes(b"signed bundle")
        downloaded = self.root / "downloaded"
        report_path = self.root / "central-test.json"
        github_output = self.root / "github-output"
        github_output.touch()

        def download(candidate, output, deployment, token):
            shutil.copytree(candidate, output)

        with mock.patch.dict(release.os.environ, {
                "CENTRAL_USERNAME": "central-user", "CENTRAL_TOKEN": "central-secret"}), \
                mock.patch.object(release, "upload_bundle", return_value=self.DEPLOYMENT), \
                mock.patch.object(release, "await_central", return_value={"deploymentState": "VALIDATED"}), \
                mock.patch.object(release, "download_deployment", side_effect=download) as fetch, \
                mock.patch.object(release, "drop_deployment", return_value=True) as drop, \
                mock.patch.object(release, "promote_deployment") as promote:
            report = release.test_publish(
                bundle_path, self.output, downloaded, report_path, github_output)

        fetch.assert_called_once_with(self.output, downloaded, self.DEPLOYMENT, mock.ANY)
        drop.assert_called_once_with(self.DEPLOYMENT, mock.ANY)
        promote.assert_not_called()
        self.assertEqual(github_output.read_text(), f"deployment_id={self.DEPLOYMENT}\n")
        self.assertTrue(report["downloadVerified"])
        self.assertTrue(report["dropped"])
        self.assertEqual(release.verify(downloaded)["version"], self.plan["version"])

    def test_failed_central_validation_is_still_dropped(self):
        self.stage()
        bundle_path = self.root / "central.zip"
        bundle_path.write_bytes(b"signed bundle")
        report_path = self.root / "central-test.json"

        with mock.patch.dict(release.os.environ, {
                "CENTRAL_USERNAME": "central-user", "CENTRAL_TOKEN": "central-secret"}), \
                mock.patch.object(release, "upload_bundle", return_value=self.DEPLOYMENT), \
                mock.patch.object(release, "await_central", side_effect=ValueError("Central validation failed")), \
                mock.patch.object(release, "download_deployment") as fetch, \
                mock.patch.object(release, "drop_deployment", return_value=True) as drop:
            with self.assertRaisesRegex(ValueError, "validation failed"):
                release.test_publish(
                    bundle_path, self.output, self.root / "downloaded", report_path)

        fetch.assert_not_called()
        drop.assert_called_once_with(self.DEPLOYMENT, mock.ANY)
        self.assertTrue(json.loads(report_path.read_text())["dropped"])

    def test_cleanup_failure_preserves_the_deployment_id_and_error(self):
        self.stage()
        bundle_path = self.root / "central.zip"
        bundle_path.write_bytes(b"signed bundle")
        downloaded = self.root / "downloaded"
        report_path = self.root / "central-test.json"

        def download(candidate, output, deployment, token):
            shutil.copytree(candidate, output)

        with mock.patch.dict(release.os.environ, {
                "CENTRAL_USERNAME": "central-user", "CENTRAL_TOKEN": "central-secret"}), \
                mock.patch.object(release, "upload_bundle", return_value=self.DEPLOYMENT), \
                mock.patch.object(release, "await_central", return_value={"deploymentState": "VALIDATED"}), \
                mock.patch.object(release, "download_deployment", side_effect=download), \
                mock.patch.object(release, "drop_deployment", side_effect=ValueError("cleanup failed")):
            with self.assertRaisesRegex(ValueError, "cleanup failed"):
                release.test_publish(bundle_path, self.output, downloaded, report_path)

        report = json.loads(report_path.read_text())
        self.assertEqual(report["deploymentId"], self.DEPLOYMENT)
        self.assertTrue(report["downloadVerified"])
        self.assertFalse(report["dropped"])
        self.assertEqual(report["dropError"], "cleanup failed")

    def test_drop_is_idempotent_when_deployment_is_absent(self):
        with mock.patch.object(release, "central_request", return_value=None) as request:
            self.assertFalse(release.drop_deployment(self.DEPLOYMENT, "token"))
        request.assert_called_once_with(
            "/deployment/" + self.DEPLOYMENT, "token", data=None,
            method="DELETE", allow_not_found=True)

    def test_cleanup_retry_updates_the_persisted_report(self):
        report_path = self.root / "central-test.json"
        release.write_json(report_path, {
            "deploymentId": self.DEPLOYMENT, "dropped": False,
            "dropError": "first attempt failed"})
        with mock.patch.object(release, "central_token", return_value="token"), \
                mock.patch.object(release, "drop_deployment", return_value=False) as cleanup:
            result = release.drop(self.DEPLOYMENT, report_path)

        cleanup.assert_called_once_with(self.DEPLOYMENT, "token")
        self.assertEqual(result["dropResult"], "already-absent")
        report = json.loads(report_path.read_text())
        self.assertTrue(report["dropped"])
        self.assertEqual(report["dropResult"], "already-absent")
        self.assertNotIn("dropError", report)

    def test_cleanup_retry_refuses_a_report_for_another_deployment(self):
        report_path = self.root / "central-test.json"
        release.write_json(report_path, {"deploymentId": "87654321-4321-4321-8321-cba987654321"})
        with mock.patch.object(release, "central_token") as token, \
                mock.patch.object(release, "drop_deployment") as cleanup:
            with self.assertRaisesRegex(ValueError, "another deployment"):
                release.drop(self.DEPLOYMENT, report_path)
        token.assert_not_called()
        cleanup.assert_not_called()

    def test_candidate_identity_uses_checked_in_gitlinks(self):
        output = self.root / "actual-plan.json"
        release.create_plan("3.0.0-rc.1", output, self.build_matrix)
        plan = json.loads(output.read_text())
        # Two Linux platforms with two OS versions and one macOS platform with one,
        # each across all five Spark/Scala profiles.
        self.assertEqual(len(plan["consumer"]), 25)
        self.assertEqual({row["os_version"] for row in plan["consumer"]},
                         {"ubuntu-22.04", "ubuntu-24.04", "macos-15"})
        self.assertEqual(set(plan["pins"]), {"milvus-proto", "milvus-storage"})

    def test_release_plan_exports_every_source_combination(self):
        output = self.root / "reader-plan.json"
        release.create_plan("3.0.0-rc.1", output, self.build_matrix)
        plan = json.loads(output.read_text())
        self.assertNotIn("thin", plan)
        self.assertEqual(plan["jvm"], RELEASE_ROWS)
        self.assertEqual(len(release.expected_artifacts(plan)), 28)

    def test_release_plan_requires_all_five_unique_profiles_and_runtime_versions(self):
        for rows in (self.plan["jvm"][:-1], self.plan["jvm"] + [self.plan["jvm"][0]],
                     self.plan["jvm"][:-1] + [self.plan["jvm"][0]]):
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "all five"):
                release.validate_plan(dict(self.plan, jvm=rows))
        for field, value, message in (("java", "21", "Java version"),
                                      ("buildJava", "17", "Java version"),
                                      ("scala", "2.13.16", "source selection"),
                                      ("spark", "3.5.4", "precedes"),
                                      ("compileSpark", "4.0.0", "release line")):
            plan = copy.deepcopy(self.plan)
            plan["jvm"][0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                release.validate_plan(plan)
        for field, value in (("thin", []), ("distribution", "full"), ("distribution", "thin")):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "legacy release distribution"):
                release.validate_plan(dict(self.plan, **{field: value}))

    def test_consumer_matrix_cannot_drop_or_duplicate_a_scala_platform_selection(self):
        output = self.root / "plan-consumers.json"
        release.create_plan("3.0.0-rc.1", output, self.build_matrix)
        plan = json.loads(output.read_text())
        for consumers in (plan["consumer"][:-1], plan["consumer"] + [plan["consumer"][0]]):
            with self.subTest(count=len(consumers)), self.assertRaisesRegex(ValueError, "Consumer matrix"):
                release.validate_plan(dict(plan, consumer=consumers))

    def test_release_plan_takes_versions_from_the_exported_matrix(self):
        rows = [dict(row, spark="4.0.2") if row["line"] == "4.0" else row for row in RELEASE_ROWS]
        matrix = json.loads(self.build_matrix.read_text())
        matrix["release"] = rows
        release.write_json(self.build_matrix, matrix)
        output = self.root / "exported-plan.json"
        release.create_plan("3.0.0-rc.1", output, self.build_matrix)
        self.assertEqual(json.loads(output.read_text())["jvm"], rows)
        matrix["release"] = rows[:-1]
        release.write_json(self.build_matrix, matrix)
        with self.assertRaisesRegex(ValueError, "all five"):
            release.create_plan("3.0.0-rc.1", output, self.build_matrix)

    def test_release_build_matrix_must_match_all_source_profiles(self):
        matrix = json.loads(self.build_matrix.read_text())
        output = self.root / "mismatched-plan.json"
        for field, value in (("scala", "2.13.16"), ("spark", "3.5.6"), ("java", "21")):
            changed = copy.deepcopy(matrix)
            changed["spark"][0][field] = value
            release.write_json(self.build_matrix, changed)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "differs from the source matrix"):
                release.create_plan("3.0.0-rc.1", output, self.build_matrix)
        matrix["spark"][1] = matrix["spark"][0]
        release.write_json(self.build_matrix, matrix)
        with self.assertRaisesRegex(ValueError, "all five"):
            release.create_plan("3.0.0-rc.1", output, self.build_matrix)

    def test_source_jar_requires_nonempty_api_sources_and_no_binary(self):
        path = self.root / "sources.jar"
        sources = {name: "source" for names in release.API_SOURCES.values() for name in names}
        for contents, message in (({"README.md": "placeholder"}, "missing API sources"),
                                  (dict.fromkeys(sources, ""), "empty API sources"),
                                  (sources | {"Bad.class": "binary"}, "binary implementation")):
            with self.subTest(message=message):
                self.jar(path, contents)
                with self.assertRaisesRegex(ValueError, message):
                    release.verify_sources(path, "jvm")
        self.jar(path, sources)
        release.verify_sources(path, "jvm")

    def test_candidate_rejects_placeholder_sources_even_with_updated_hashes(self):
        self.rewrite_jvm_export("3.5", "2.12", lambda path: self.jar(path, {"README.md": "placeholder"}),
                                classifier="sources")
        with self.assertRaisesRegex(ValueError, "missing API sources"):
            self.stage()

    def test_storage_sources_keep_the_generated_upstream_directory(self):
        path = self.root / "storage-sources.jar"
        upstream = "milvus-storage/scala/io/milvus/storage/MilvusStorageReader.scala"
        sources = {name: "source" for names in release.API_SOURCES.values() for name in names}
        self.jar(path, sources)
        release.verify_sources(path, "jvm")
        sources["io/milvus/storage/MilvusStorageReader.scala"] = sources.pop(upstream)
        self.jar(path, sources)
        with self.assertRaisesRegex(ValueError, "missing API sources"):
            release.verify_sources(path, "jvm")


class SigningKeyTest(unittest.TestCase):
    def test_exactly_one_secret_primary_key_is_required(self):
        key_a = "A" * 40
        key_b = "B" * 40
        listings = (
            "",
            f"sec:u:4096:1:A:0:0:::::scESC:::\nfpr:::::::::{key_a}:\n"
            f"sec:u:4096:1:B:0:0:::::scESC:::\nfpr:::::::::{key_b}:\n",
        )
        for listing in listings:
            with self.subTest(keys=listing.count("sec:")), \
                    mock.patch.object(release.subprocess, "run", return_value=mock.Mock(stdout=listing)):
                with self.assertRaisesRegex(ValueError, "exactly one secret primary key"):
                    release.secret_primary_key()

    def test_wrong_single_secret_primary_key_is_rejected(self):
        listing = "sec:u:4096:1:A:0:0:::::scESC:::\n" + "fpr:::::::::" + "A" * 40 + ":\n"
        with mock.patch.object(release.subprocess, "run", return_value=mock.Mock(stdout=listing)):
            with self.assertRaisesRegex(ValueError, "does not match release fingerprint"):
                release.secret_primary_key()

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
