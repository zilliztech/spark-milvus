"""Exercise the consumer-visible Maven graph and immutable release boundary."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET
import zipfile


SPEC = importlib.util.spec_from_file_location("release", Path(__file__).resolve().parents[1] / "release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

RELEASE_ROWS = [
    {"line": "4.0", "project": "spark40", "spark": "4.0.1", "scala": "2.13.16", "java": "21"},
    {"line": "4.2", "project": "spark42", "spark": "4.2.0", "scala": "2.13.16", "java": "21"},
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
        self.plan["jvm"] = RELEASE_ROWS
        self.build_matrix = self.root / "build-matrix.json"
        release.write_json(self.build_matrix, {"schemaVersion": 1, "release": RELEASE_ROWS})
        self.plan.update(version="2.0.0-rc.1", sourceSha="a" * 40,
                         pins={"milvus-storage": "b" * 40, "knowhere": "c" * 40, "milvus-proto": "d" * 40})
        self.plan_path = self.root / "plan.json"
        release.write_json(self.plan_path, self.plan)
        for native in self.plan["native"]:
            self.native(native["platform"])
        for jvm in self.plan["jvm"]:
            self.jvm(jvm["line"])
        self.thin()

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
            if classifier == "sources":
                contents.update({name: "source fixture" for names in release.THIN_SOURCES.values() for name in names})
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

    def thin(self, spark_build_version="4.0.0"):
        directory = self.inputs / "thin-4.0"
        modules = []
        line = next(item for item in self.plan["jvm"] if item["line"] == "4.0")
        for artifact, (project, internal) in release.THIN_MODULES.items():
            runtime = [dict(groupId="com.zilliz", artifactId=name, version=self.plan["version"])
                       for name in internal]
            provided = [dict(groupId="org.apache.spark", artifactId="spark-" + name + "_2.13", version=spark_build_version)
                        for name in ("core", "sql", "catalyst")] if project == "spark40" else []
            files = []
            for classifier, extension in sorted(release.MAVEN_FILES):
                suffix = "-" + classifier if classifier else ""
                path = directory / artifact / f"{artifact}-{self.plan['version']}{suffix}.{extension}"
                path.parent.mkdir(parents=True, exist_ok=True)
                if extension == "pom":
                    path.write_bytes(release.pom_bytes(artifact, self.plan,
                        runtime + [dict(dep, scope="provided") for dep in provided]))
                else:
                    contents = {"README.md": "fixture"} if classifier else {f"fixture/{project}/Api.class": b"fixture"}
                    if classifier == "sources":
                        contents.update({name: "source fixture" for name in release.THIN_SOURCES[project]})
                    if project == "native-runtime" and not classifier:
                        contents[release.COMPATIBILITY] = "format.version=1\nstorage.revision=" + "b" * 40 + \
                            "\nknowhere.revision=" + "c" * 40 + "\n"
                    if project == "native-vector" and not classifier:
                        contents["io/knowhere/Knowhere.class"] = b"fixture"
                    self.jar(path, contents)
                files.append(dict(classifier=classifier, extension=extension,
                    path=str(path.relative_to(directory)), sha256=release.digest(path), size=path.stat().st_size))
            modules.append(dict(artifactId=artifact, projectId=project, javaRelease=17, files=files,
                                runtimeDependencies=runtime, providedDependencies=provided))
        release.write_json(directory / "thin-artifacts.json", dict(
            schemaVersion=1, kind="thin", groupId="com.zilliz", version=self.plan["version"],
            sourceSha=self.plan["sourceSha"], sparkLine="4.0", sparkVersion=spark_build_version,
            scalaVersion=line["scala"], scalaBinaryVersion="2.13", migrationState="transition", modules=modules))

    def thin_only(self):
        self.plan.update(distribution="thin", native=[], jvm=[item for item in self.plan["jvm"] if item["line"] == "4.0"])
        release.write_json(self.plan_path, self.plan)
        for path in self.inputs.iterdir():
            if path.name != "thin-4.0":
                shutil.rmtree(path)

    def rewrite_thin_export(self, artifact, transform, classifier="", extension="pom"):
        metadata_path = self.inputs / "thin-4.0" / "thin-artifacts.json"
        metadata = json.loads(metadata_path.read_text())
        module = next(item for item in metadata["modules"] if item["artifactId"] == artifact)
        record = next(item for item in module["files"]
                      if item["classifier"] == classifier and item["extension"] == extension)
        path = metadata_path.parent / record["path"]
        transform(path)
        record.update(sha256=release.digest(path), size=path.stat().st_size)
        release.write_json(metadata_path, metadata)

    @staticmethod
    def modify_pom(path, mutate):
        tree = ET.parse(path)
        mutate(tree.getroot())
        tree.write(path, encoding="utf-8", xml_declaration=True)

    def rewrite_file(self, manifest, record, contents):
        path = self.output / "repository" / record["path"]
        path.write_bytes(contents)
        record.update(sha256=release.digest(path), size=path.stat().st_size)
        for algorithm in ("md5", "sha1", "sha256", "sha512"):
            Path(str(path) + "." + algorithm).write_text(release.digest(path, algorithm) + "\n")
        release.write_json(self.output / "release-manifest.json", manifest)

    def test_both_entries_share_exact_bottom_level_artifacts(self):
        manifest = self.stage()
        self.assertEqual(len(manifest["artifacts"]), 68)
        entries = [item for item in manifest["artifacts"] if item["kind"] == "entry" and item["extension"] == "pom"]
        self.assertEqual(len(entries), 6)
        self.assertEqual(len(release.entry_dependencies(self.plan, "4.0", None)), 3)
        self.assertEqual(len(release.entry_dependencies(self.plan, "4.0", "linux-x86_64")), 2)
        with self.assertRaisesRegex(ValueError, "never overwrite"):
            self.stage()

    def test_thin_only_uses_the_same_candidate_format_and_preserves_generated_poms(self):
        self.thin_only()
        artifact = "spark-milvus-4.0-thin_2.13"
        namespace = "{http://maven.apache.org/POM/4.0.0}"

        def exclude_arrow(project):
            spark = next(dep for dep in project.find(namespace + "dependencies")
                         if dep.findtext(namespace + "groupId") == "org.apache.spark")
            exclusion = ET.SubElement(ET.SubElement(spark, namespace + "exclusions"), namespace + "exclusion")
            ET.SubElement(exclusion, namespace + "groupId").text = "org.apache.arrow"
            ET.SubElement(exclusion, namespace + "artifactId").text = "*"

        self.rewrite_thin_export(artifact, lambda path: self.modify_pom(path, exclude_arrow))
        manifest = self.stage()
        self.assertEqual(len(manifest["artifacts"]), 28)
        self.assertEqual(manifest["thin"]["migrationState"], "transition")
        self.assertEqual({item["kind"] for item in manifest["artifacts"]}, {"thin"})
        for record in manifest["artifacts"]:
            source = self.inputs / "thin-4.0" / record["artifactId"] / Path(record["path"]).name
            self.assertEqual(source.read_bytes(), (self.output / "repository" / record["path"]).read_bytes())
        pom = next(record for record in manifest["artifacts"]
                   if record["artifactId"] == artifact and record["extension"] == "pom")
        _, dependencies = release.pom_dependencies(self.output / "repository" / pom["path"])
        self.assertEqual({dep["artifactId"] for dep in dependencies if dep["groupId"] == "com.zilliz"},
                         {"spark-milvus-core_2.13", "spark-milvus-compat_2.13", "spark-milvus-client_2.13"})

    def test_full_candidate_requires_thin_modules(self):
        shutil.rmtree(self.inputs / "thin-4.0")
        with self.assertRaisesRegex(ValueError, "exactly one thin module export"):
            self.stage()

    def test_thin_only_plan_cannot_silently_drop_full_native_acceptance(self):
        invalid = dict(self.plan, distribution="thin")
        with self.assertRaisesRegex(ValueError, "Thin-only plan"):
            release.validate_plan(invalid)

    def test_duplicate_thin_gav_is_rejected(self):
        path = self.inputs / "thin-4.0" / "thin-artifacts.json"
        data = json.loads(path.read_text())
        data["modules"][-1] = data["modules"][0]
        release.write_json(path, data)
        with self.assertRaisesRegex(ValueError, "duplicate thin module GAV"):
            self.stage()

    def test_thin_export_cannot_claim_final_readonly_state(self):
        path = self.inputs / "thin-4.0" / "thin-artifacts.json"
        data = json.loads(path.read_text())
        data["migrationState"] = "readonly"
        release.write_json(path, data)
        with self.assertRaisesRegex(ValueError, "must be transition"):
            self.stage()

    def test_thin_build_version_cannot_exceed_or_change_consumer_line(self):
        for version in ("4.0.2", "4.1.0", "4.0.0-SNAPSHOT", "[4.0.0,)", "4.0.01", "4.0"):
            with self.subTest(version=version):
                self.thin(spark_build_version=version)
                with self.assertRaisesRegex(ValueError, "build version must match the line"):
                    self.stage()

    def test_thin_spark_pom_must_match_exact_build_version(self):
        path = self.inputs / "thin-4.0" / "thin-artifacts.json"
        data = json.loads(path.read_text())
        # Both patch releases are valid consumer targets, but the POM must
        # describe the release actually used to compile this candidate.
        data["sparkVersion"] = "4.0.1"
        release.write_json(path, data)
        with self.assertRaisesRegex(ValueError, "provided Spark build version"):
            self.stage()

    def test_missing_thin_sources_is_rejected(self):
        path = self.inputs / "thin-4.0" / "thin-artifacts.json"
        data = json.loads(path.read_text())
        data["modules"][0]["files"] = [file for file in data["modules"][0]["files"]
                                       if file["classifier"] != "sources"]
        release.write_json(path, data)
        with self.assertRaisesRegex(ValueError, "Incomplete thin artifact set"):
            self.stage()

    def test_thin_pom_cannot_drop_internal_dependency(self):
        artifact = "spark-milvus-core_2.13"
        self.rewrite_thin_export(artifact, lambda path: path.write_bytes(release.pom_bytes(artifact, self.plan, [])))
        with self.assertRaisesRegex(ValueError, "Wrong thin internal dependency graph"):
            self.stage()

    def test_thin_pom_cannot_hide_internal_dependency_as_provided(self):
        artifact = "spark-milvus-compat_2.13"
        dependencies = [dict(groupId="com.zilliz", artifactId="spark-milvus-core_2.13",
                             version=self.plan["version"], scope="provided")]
        self.rewrite_thin_export(artifact, lambda path: path.write_bytes(release.pom_bytes(artifact, self.plan, dependencies)))
        with self.assertRaisesRegex(ValueError, "transitive compile"):
            self.stage()

    def test_thin_pom_cannot_drop_provided_dependency(self):
        artifact = "spark-milvus-4.0-thin_2.13"
        dependencies = [dict(groupId="com.zilliz", artifactId=name, version=self.plan["version"])
                        for name in release.THIN_MODULES[artifact][1]]
        self.rewrite_thin_export(artifact, lambda path: path.write_bytes(release.pom_bytes(artifact, self.plan, dependencies)))
        with self.assertRaisesRegex(ValueError, "differ from build metadata"):
            self.stage()

    def test_thin_pom_cannot_add_official_sdk(self):
        artifact = "spark-milvus-native-runtime"
        dependencies = [dict(groupId="io.milvus", artifactId="milvus-sdk-java", version="2.6.1")]
        self.rewrite_thin_export(artifact, lambda path: path.write_bytes(release.pom_bytes(artifact, self.plan, dependencies)))
        with self.assertRaisesRegex(ValueError, "official Milvus SDK"):
            self.stage()

    def test_thin_jar_cannot_embed_native_or_sdk(self):
        artifact = "spark-milvus-core_2.13"
        for name, expected in (("native/milvus/1/linux-x86_64/libtest.so", "platform bundle"),
                               ("io/milvus/v2/client/MilvusClientV2.class", "official Milvus SDK")):
            with self.subTest(name=name):
                self.thin()
                self.rewrite_thin_export(artifact, lambda path: self.jar(path, {
                    "fixture/core/Api.class": "fixture", name: "unexpected"}), extension="jar")
                with self.assertRaisesRegex(ValueError, expected):
                    self.stage()

    def test_thin_modules_cannot_duplicate_shared_proto_classes(self):
        self.rewrite_thin_export("spark-milvus-client_2.13", lambda path: self.jar(path, {
            "fixture/core/Api.class": "duplicate generated schema"}), extension="jar")
        with self.assertRaisesRegex(ValueError, "Duplicate thin class"):
            self.stage()

    def test_thin_vector_requires_upstream_java_api(self):
        self.rewrite_thin_export("spark-milvus-native-vector", lambda path: self.jar(path, {
            "fixture/native-vector/Api.class": "fixture"}), extension="jar")
        with self.assertRaisesRegex(ValueError, "missing the Knowhere Java API"):
            self.stage()

    def test_candidate_rechecks_thin_pom_graph_after_rehash(self):
        manifest = self.stage()
        record = next(item for item in manifest["artifacts"]
                      if item["artifactId"] == "spark-milvus-compat_2.13" and item["extension"] == "pom")
        self.rewrite_file(manifest, record, release.pom_bytes(record["artifactId"], self.plan, []))
        with self.assertRaisesRegex(ValueError, "Wrong thin internal dependency graph"):
            release.verify(self.output)

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
                with self.assertRaisesRegex(ValueError, "JVM JAR embeds the official Milvus SDK"):
                    release.verify(self.output)
        self.jar(path, {**original,
                        "io/milvus/grpc/milvus/MilvusServiceGrpc.class": b"generated protocol",
                        "com/zilliz/milvus/client/api/MilvusClient.class": b"transitional client"})
        self.rewrite_file(manifest, record, path.read_bytes())
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

        self.assertEqual(len(paths), 340)
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
        release.create_plan("2.0.0-rc.1", output, self.build_matrix)
        plan = json.loads(output.read_text())
        self.assertEqual(len(plan["consumer"]), 8)
        self.assertEqual({row["os_version"] for row in plan["consumer"]}, {"ubuntu-22.04", "ubuntu-24.04"})
        self.assertEqual(set(plan["pins"]), {"milvus-proto", "milvus-storage", "knowhere"})

    def test_release_plan_takes_versions_from_the_exported_matrix(self):
        rows = [dict(row, spark="4.0.2") if row["line"] == "4.0" else row for row in RELEASE_ROWS]
        release.write_json(self.build_matrix, {"schemaVersion": 1, "release": rows})
        output = self.root / "exported-plan.json"
        release.create_plan("2.0.0-rc.1", output, self.build_matrix)
        self.assertEqual(json.loads(output.read_text())["jvm"], rows)
        release.write_json(self.build_matrix, {"schemaVersion": 1, "release": rows + [dict(rows[0], line="4.1")]})
        with self.assertRaisesRegex(ValueError, "supported 4.0/4.2"):
            release.create_plan("2.0.0-rc.1", output, self.build_matrix)

    def test_source_jar_requires_nonempty_api_sources_and_no_binary(self):
        path = self.root / "sources.jar"
        artifact = "spark-milvus-4.0-thin_2.13"
        sources = {name: "source" for name in release.THIN_SOURCES["spark40"]}
        for contents, message in (({"README.md": "placeholder"}, "missing API sources"),
                                  (dict.fromkeys(sources, ""), "empty API sources"),
                                  (sources | {"Bad.class": "binary"}, "binary implementation")):
            with self.subTest(message=message):
                self.jar(path, contents)
                with self.assertRaisesRegex(ValueError, message):
                    release.verify_sources(path, "thin", artifact)
        self.jar(path, sources)
        release.verify_sources(path, "thin", artifact)

    def test_candidate_rejects_placeholder_sources_even_with_updated_hashes(self):
        self.rewrite_thin_export("spark-milvus-core_2.13",
                                 lambda path: self.jar(path, {"README.md": "placeholder"}),
                                 classifier="sources", extension="jar")
        with self.assertRaisesRegex(ValueError, "missing API sources"):
            self.stage()

    def test_storage_sources_keep_the_generated_upstream_directory(self):
        path = self.root / "storage-sources.jar"
        own = "com/zilliz/milvus/jni/storage/NativeStorageLibrary.java"
        upstream = "milvus-storage/scala/io/milvus/storage/MilvusStorageReader.scala"
        self.jar(path, {own: "source", upstream: "upstream source"})
        release.verify_sources(path, "thin", "spark-milvus-native-storage_2.13")
        self.jar(path, {own: "source", "io/milvus/storage/MilvusStorageReader.scala": "wrong mapping"})
        with self.assertRaisesRegex(ValueError, "missing API sources"):
            release.verify_sources(path, "thin", "spark-milvus-native-storage_2.13")


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
