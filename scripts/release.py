#!/usr/bin/env python3
"""Stage, verify and publish an immutable Maven release candidate.

``publish`` promotes a deployment; ``test-publish`` creates and drops a private
deployment after verification. Candidate packaging and verification use the
exact bytes tested by consumers.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "scripts/release-matrix.json"
PROJECT_URL = "https://github.com/zilliztech/spark-milvus"
CENTRAL = "https://central.sonatype.com/api/v1/publisher"
CENTRAL_REPOSITORY = "https://repo.maven.apache.org/maven2"
SIGNING_FINGERPRINT = "F0004DED15D9C011F344D6A8526AB23093F3D44A"
COMPATIBILITY = "META-INF/milvus-native/compatibility.properties"
SDK_PACKAGES = ("io/milvus/client/", "io/milvus/v2/", "io/milvus/param/", "io/milvus/response/")
VERSION = re.compile(r"2\.\d+\.\d+(?:-[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)?")
SHA = re.compile(r"[0-9a-f]{40}")
THIN_MODULES = {
    "spark-milvus-native-runtime": ("native-runtime", ()),
    "spark-milvus-native-storage_2.13": ("native-storage", ("spark-milvus-native-runtime",)),
    "spark-milvus-native-vector": ("native-vector", ("spark-milvus-native-runtime",)),
    "spark-milvus-core_2.13": ("core", ("spark-milvus-native-storage_2.13", "spark-milvus-native-vector")),
    "spark-milvus-compat_2.13": ("compat", ("spark-milvus-core_2.13",)),
    "spark-milvus-client_2.13": ("client", ("spark-milvus-core_2.13",)),
    "spark-milvus-4.0-thin_2.13": ("spark40", ("spark-milvus-core_2.13", "spark-milvus-compat_2.13",
                                                 "spark-milvus-client_2.13")),
}
MAVEN_FILES = {(classifier, extension) for classifier, extension in
               (("", "jar"), ("sources", "jar"), ("javadoc", "jar"), ("", "pom"))}
THIN_SOURCES = {
    "native-runtime": {"com/zilliz/milvus/jni/runtime/NativeLibraries.java"},
    "native-storage": {"com/zilliz/milvus/jni/storage/NativeStorageLibrary.java",
                       "milvus-storage/scala/io/milvus/storage/MilvusStorageReader.scala"},
    "native-vector": {"com/zilliz/milvus/jni/vector/NativeVectorLibrary.java", "io/knowhere/Knowhere.java"},
    "core": {"com/zilliz/milvus/storage/snapshot/Snapshot.scala"},
    "compat": {"com/zilliz/milvus/storage/compat/v2/FooterV2SegmentResolver.scala"},
    "client": {"com/zilliz/milvus/client/api/MilvusClient.scala"},
    "spark40": {"com/zilliz/spark/connector/sources/MilvusDataSource.scala",
                "com/zilliz/spark/connector/extensions/MilvusReadSparkSessionExtensions.scala",
                "com/zilliz/spark/connector/extensions/NearestByExtensions.scala"},
}
ET.register_namespace("", "http://maven.apache.org/POM/4.0.0")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path, algorithm="sha256"):
    value = hashlib.new(algorithm)
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def properties(contents):
    result = {}
    for line in contents.decode("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith(("#", "!")):
            continue
        key, sep, value = line.partition("=")
        require(sep and key.strip() not in result, "Malformed or duplicate property")
        result[key.strip()] = value.strip()
    return result


def safe_path(value):
    path = PurePosixPath(value)
    require(value and not path.is_absolute() and ".." not in path.parts
            and "\\" not in value and str(path) == value, f"Unsafe path: {value}")
    return path


def regular_file(root, relative):
    path = root / safe_path(relative)
    require(path.is_file() and not path.is_symlink(), f"Missing regular file: {relative}")
    require(path.resolve().is_relative_to(root.resolve()), f"File escapes repository: {relative}")
    return path


def validate_plan(plan):
    require(plan.get("schemaVersion") == 1, "Unsupported release plan schema")
    version = plan.get("version", "")
    require(VERSION.fullmatch(version) and not version.upper().endswith("SNAPSHOT"),
            "Release version must be an immutable 2.x version, without SNAPSHOT")
    require(SHA.fullmatch(plan.get("sourceSha", "")), "Invalid source SHA")
    for name in ("milvus-proto", "milvus-storage", "knowhere"):
        require(SHA.fullmatch(plan.get("pins", {}).get(name, "")), f"Invalid {name} pin")
    require(plan.get("groupId") == "com.zilliz", "Unexpected Maven group")
    native = [item["platform"] for item in plan["native"]]
    lines = [item["line"] for item in plan["jvm"]]
    distribution = plan.get("distribution", "full")
    require(distribution in {"full", "thin"}, "Unsupported release distribution")
    require(len(set(native)) == len(native) and (native or distribution == "thin"),
            "Empty or duplicate native matrix")
    require(set(native) <= {"linux-x86_64", "linux-aarch64"}, "Unsupported native release platform")
    require(lines and len(set(lines)) == len(lines), "Empty or duplicate JVM matrix")
    require(set(lines) <= {"4.0", "4.2"}, "Unsupported JVM release line")
    if distribution == "thin":
        require(not native and lines == ["4.0"] and not plan.get("consumer"),
                "Thin-only plan must contain Spark 4.0 and no native consumer matrix")
    for item in plan["native"]:
        require(item["libc"] == "glibc" and item["glibc"] == "2.35"
                and item["glibcxx"] == "3.4.30" and item["cxxabi"] == "1.3.13",
                "Unsupported system ABI baseline")
    for item in plan["jvm"]:
        require(item["scala"] == "2.13.16", "Unsupported Scala release version")


def create_plan(version, output, matrix, thin_only=False):
    plan = json.loads(MATRIX.read_text())
    exported = json.loads(Path(matrix).read_text())
    require(exported.get("schemaVersion") == 1, "Unsupported build matrix schema")
    rows = exported.get("release", [])
    require(len(rows) == 2 and {row.get("line") for row in rows} == {"4.0", "4.2"},
            "Build matrix must retain the supported 4.0/4.2 release lines")
    plan["jvm"] = rows
    plan["version"] = version
    if thin_only:
        plan["distribution"] = "thin"
        plan["native"] = []
        plan["jvm"] = [item for item in plan["jvm"] if item["line"] == "4.0"]
    plan["sourceSha"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    plan["pins"] = {}
    for name in ("milvus-proto", "milvus-storage", "knowhere"):
        record = subprocess.check_output(["git", "ls-tree", "HEAD", name], cwd=ROOT, text=True)
        fields = record.split()
        require(len(fields) == 4 and fields[:2] == ["160000", "commit"], f"Missing gitlink: {name}")
        plan["pins"][name] = fields[2]
    plan["consumer"] = [
        dict(jvm, platform=native["platform"], **host)
        for native in plan["native"] for host in native["consumers"] for jvm in plan["jvm"]
    ]
    validate_plan(plan)
    write_json(output, plan)


def archive_names(archive):
    names = archive.namelist()
    require(len(names) == len(set(names)), "Duplicate ZIP entries")
    for name in names:
        safe_path(name.rstrip("/"))
    return names


def verify_native(path, plan, platform):
    prefix = f"native/milvus/1/{platform}/"
    with zipfile.ZipFile(path) as archive:
        names = archive_names(archive)
        require(not any(name.endswith(".class") for name in names), "Native JAR contains JVM classes")
        require(all(name.startswith(prefix) for name in names if name.startswith("native/")),
                "Native JAR contains another platform")
        manifest = properties(archive.read(prefix + "manifest.properties"))
        require(manifest.get("format.version") == "1" and manifest.get("platform") == platform,
                "Native manifest format/platform mismatch")
        require(manifest.get("with_cardinal") == "false", "Public release must use the OSS native variant")
        for key, name in (("storage.revision", "milvus-storage"), ("knowhere.revision", "knowhere")):
            require(manifest.get(key) == plan["pins"][name], f"Native source pin mismatch: {name}")
        libraries = manifest["libraries"].split(",")
        require(libraries and len(set(libraries)) == len(libraries), "Invalid native library inventory")
        for library in libraries:
            safe_path(library)
            hasher = hashlib.sha256()
            with archive.open(prefix + library) as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    hasher.update(block)
            require(hasher.hexdigest() == manifest.get("sha256." + library),
                    f"Corrupt native library: {library}")
        provenance = archive.read("META-INF/milvus-native/provenance.json")
        require(hashlib.sha256(provenance).hexdigest() == manifest.get("provenance.sha256"),
                "Native provenance digest mismatch")
        provenance = json.loads(provenance)
        require(provenance.get("audit") == "passed" and provenance.get("auditPolicy") == "jvm-load",
                "Native JVM loading acceptance is missing")
        require(any(name.startswith("META-INF/milvus-native/licenses/") for name in names),
                "Missing native licenses")
        return manifest


def verify_jvm(path, plan):
    with zipfile.ZipFile(path) as archive:
        names = archive_names(archive)
        require(not any(name.startswith("native/") for name in names), "JVM JAR embeds a platform bundle")
        require(not any(name.startswith(SDK_PACKAGES) for name in names),
                "JVM JAR embeds the official Milvus SDK")
        required = ["io/knowhere/", "io/milvus/storage/", "com/zilliz/spark/connector/"]
        for prefix in required:
            require(any(name.startswith(prefix) and name.endswith(".class") for name in names),
                    f"JVM JAR is missing API classes: {prefix}")
        expected = properties(archive.read(COMPATIBILITY))
        require(expected == {"format.version": "1", "storage.revision": plan["pins"]["milvus-storage"],
                             "knowhere.revision": plan["pins"]["knowhere"]},
                "JVM/native compatibility identity mismatch")


def verify_sources(path, kind, artifact):
    """Validate real sources for the APIs consumers compile against, not a placeholder JAR."""
    with zipfile.ZipFile(path) as archive:
        names = archive_names(archive)
        require(not any(name.endswith((".class", ".so", ".dll", ".dylib")) or name.startswith("native/")
                        for name in names), "Source JAR contains binary implementation")
        if kind == "thin":
            required = THIN_SOURCES[THIN_MODULES[artifact][0]]
        elif kind == "jvm":
            required = set().union(*(THIN_SOURCES[name] for name in
                                   ("native-runtime", "native-storage", "native-vector", "core", "spark40")))
        else:
            return  # Entry/native artifacts document resources; they have no JVM implementation.
        require(required <= set(names), "Source JAR is missing API sources: " + ", ".join(sorted(required - set(names))))
        require(all(archive.read(name).strip() for name in required), "Source JAR contains empty API sources")


def artifact_path(group, artifact, version, classifier="", extension="jar"):
    suffix = "-" + classifier if classifier else ""
    return f"{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}{suffix}.{extension}"


def pom_bytes(artifact, plan, dependencies):
    ns = "{http://maven.apache.org/POM/4.0.0}"
    project = ET.Element(ns + "project")

    def add(parent, name, value):
        ET.SubElement(parent, ns + name).text = value

    for name, value in (("modelVersion", "4.0.0"), ("groupId", plan["groupId"]),
                        ("artifactId", artifact), ("version", plan["version"]),
                        ("packaging", "jar"), ("name", artifact),
                        ("description", "Milvus Spark Connector: " + artifact), ("url", PROJECT_URL)):
        add(project, name, value)
    licenses = ET.SubElement(project, ns + "licenses")
    for name, url in (("Server Side Public License v1", PROJECT_URL + "/blob/" + plan["sourceSha"] + "/LICENSE"),
                      ("GNU Affero General Public License v3 (AGPLv3)", "https://www.gnu.org/licenses/agpl-3.0.txt")):
        license_node = ET.SubElement(licenses, ns + "license")
        add(license_node, "name", name)
        add(license_node, "url", url)
        add(license_node, "distribution", "repo")
    developers = ET.SubElement(project, ns + "developers")
    developer = ET.SubElement(developers, ns + "developer")
    for name, value in (("id", "santiago-wjq"), ("name", "Santiago Wu"),
                        ("email", "santiago.wu@zilliz.com"), ("organization", "Zilliz")):
        add(developer, name, value)
    scm = ET.SubElement(project, ns + "scm")
    for name, value in (("url", PROJECT_URL), ("connection", "scm:git:" + PROJECT_URL + ".git"),
                        ("developerConnection", "scm:git:ssh://git@github.com/zilliztech/spark-milvus.git"),
                        ("tag", plan["sourceSha"])):
        add(scm, name, value)
    if dependencies:
        node = ET.SubElement(project, ns + "dependencies")
        for dependency in dependencies:
            dep = ET.SubElement(node, ns + "dependency")
            for name in ("groupId", "artifactId", "version", "classifier", "scope"):
                if dependency.get(name):
                    add(dep, name, dependency[name])
    ET.indent(project)
    return ET.tostring(project, encoding="utf-8", xml_declaration=True) + b"\n"


def small_jar(path, readme, license_bytes):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in (("META-INF/MANIFEST.MF", b"Manifest-Version: 1.0\r\n\r\n"),
                              ("README.md", readme.encode()), ("META-INF/LICENSE", license_bytes)):
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, content)


def expected_artifacts(plan):
    thin = {artifact: ("thin", "4.0", None) for artifact in THIN_MODULES
            if any(item["line"] == "4.0" for item in plan["jvm"])}
    if plan.get("distribution") == "thin":
        return thin
    result = {f"spark-milvus-native-{item['platform']}": ("native", None, item["platform"])
              for item in plan["native"]}
    for item in plan["jvm"]:
        line = item["line"]
        result[f"spark-milvus-{line}-jvm_2.13"] = ("jvm", line, None)
        result[f"spark-milvus-{line}_2.13"] = ("entry", line, None)
        for native in plan["native"]:
            platform = native["platform"]
            result[f"spark-milvus-{line}-{platform}_2.13"] = ("entry", line, platform)
    require(not set(result) & set(thin), "Thin GAV collides with a deployment artifact")
    return result | thin


def entry_dependencies(plan, line, platform):
    names = [f"spark-milvus-{line}-jvm_2.13"]
    names.extend(f"spark-milvus-native-{native['platform']}" for native in plan["native"]
                 if platform is None or native["platform"] == platform)
    return [{"groupId": plan["groupId"], "artifactId": name, "version": plan["version"]}
            for name in names]


def thin_metadata(data, plan):
    line = next((item for item in plan["jvm"] if item["line"] == "4.0"), None)
    require(line is not None, "Thin modules require Spark 4.0 in the release plan")
    require(data.get("schemaVersion") == 1 and data.get("kind") == "thin",
            "Unsupported thin artifact metadata")
    require(all(data.get(key) == plan[key] for key in ("groupId", "version", "sourceSha"))
            and data.get("sparkLine") == "4.0"
            and data.get("scalaVersion") == line["scala"] and data.get("scalaBinaryVersion") == "2.13",
            "Thin build identity mismatch")
    # The published API compiles against the line's compatibility floor; the
    # consumer matrix may use a newer maintenance release of that same line.
    versions = [data.get("sparkVersion", ""), line["spark"]]
    require(all(re.fullmatch(r"4\.0\.(?:0|[1-9][0-9]*)", version) for version in versions)
            and tuple(map(int, versions[0].split("."))) <= tuple(map(int, versions[1].split("."))),
            "Thin Spark build version must match the line and not exceed the consumer version")
    # The current closure still contains the old client and writes until their
    # migration. This export must not claim to be the final read-only artifact.
    require(data.get("migrationState") == "transition", "Thin migration state must be transition")
    modules = data.get("modules", [])
    require(len(modules) == len(THIN_MODULES)
            and {module["artifactId"] for module in modules} == set(THIN_MODULES),
            "Missing or duplicate thin module GAV")
    for module in modules:
        artifact = module["artifactId"]
        require(module.get("projectId") == THIN_MODULES[artifact][0], "Thin project identity mismatch")
    return {module["artifactId"]: module for module in modules}


def pom_dependencies(path):
    project = ET.parse(path).getroot()
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    require(project.tag == "{" + ns["m"] + "}project", "Invalid Maven POM root")
    require(not project.findall(".//m:repositories", ns), "POM declares an extra repository")
    # Profiles and inherited dependency management could hide dependencies from
    # the consumer-visible direct graph checked below. Export self-contained POMs.
    require(not any(project.find("m:" + name, ns) is not None
                    for name in ("parent", "profiles", "dependencyManagement")),
            "POM must declare its dependencies directly")
    dependencies = [{node.tag.split("}")[-1]: node.text for node in dep
                     if node.tag != "{" + ns["m"] + "}exclusions"}
                    for dep in project.findall("m:dependencies/m:dependency", ns)]
    keys = [(dep.get("groupId"), dep.get("artifactId"), dep.get("classifier", "")) for dep in dependencies]
    require(len(keys) == len(set(keys)), "Duplicate POM dependency")
    for dep in dependencies:
        require(all(dep.get(key) for key in ("groupId", "artifactId", "version")),
                "POM dependency is missing a Maven identity")
        require(dep.get("scope", "compile") in {"compile", "runtime", "provided"}
                and "systemPath" not in dep and "SNAPSHOT" not in dep["version"].upper()
                and not any(value in dep["version"] for value in ("$", "[", "]", "(", ")", ",", "+"))
                and dep["version"] not in {"LATEST", "RELEASE"}, "Non-release POM dependency")
        require(not (dep["groupId"] == "io.milvus" and "sdk" in dep["artifactId"].lower()),
                "Public artifact depends on the official Milvus SDK")
    return project, dependencies


def verify_thin_pom(artifact, dependencies, module, plan, spark_build_version):
    internal = {dep["artifactId"]: dep for dep in dependencies if dep["groupId"] == plan["groupId"]}
    require(set(internal) == set(THIN_MODULES[artifact][1]), "Wrong thin internal dependency graph")
    require(all(dep.get("scope", "compile") == "compile" and dep.get("optional", "false") == "false"
                and dep.get("type", "jar") == "jar" and not dep.get("classifier")
                and dep["version"] == plan["version"] for dep in internal.values()),
            "Thin internal dependencies must remain transitive compile dependencies")
    for scope, key in (("provided", "providedDependencies"), ("runtime", "runtimeDependencies")):
        actual = {(dep["groupId"], dep["artifactId"], dep["version"]) for dep in dependencies
                  if (dep.get("scope", "compile") == "provided") == (scope == "provided")}
        declared = module.get(key, [])
        expected = {(dep["groupId"], dep["artifactId"], dep["version"]) for dep in declared}
        require(actual == expected and len(declared) == len(expected),
                f"Thin POM dependencies differ from build metadata: {artifact} ({scope})")
    if artifact == "spark-milvus-4.0-thin_2.13":
        spark = {dep["artifactId"]: dep for dep in dependencies if dep["groupId"] == "org.apache.spark"}
        require(set(spark) >= {"spark-core_2.13", "spark-sql_2.13", "spark-catalyst_2.13"},
                "Thin Spark POM is missing provided Spark dependencies")
        require(all(dep.get("scope") == "provided" and dep["version"] == spark_build_version
                    for dep in spark.values()), "Thin Spark dependencies must use the provided Spark build version")


def verify_thin_jar(path, artifact, plan, classes):
    with zipfile.ZipFile(path) as archive:
        names = archive_names(archive)
        require(not any(name.startswith("native/") or name.endswith((".so", ".dylib", ".dll")) or ".so." in name
                        or name == "META-INF/milvus-native/provenance.json"
                        for name in names), "Thin JAR embeds a platform bundle")
        require(not any(name.startswith("org/apache/spark/") for name in names),
                "Thin JAR embeds Spark classes")
        require(not any(name.startswith(SDK_PACKAGES) for name in names),
                "Thin JAR embeds the official Milvus SDK")
        own_classes = {name for name in names if name.endswith(".class")}
        require(own_classes, f"Thin JAR is missing implementation classes: {artifact}")
        if artifact == "spark-milvus-native-vector":
            require("io/knowhere/Knowhere.class" in own_classes, "Thin JAR is missing the Knowhere Java API")
        for name in own_classes:
            require(name not in classes, f"Duplicate thin class: {name} in {artifact} and {classes.get(name)}")
            classes[name] = artifact
        if artifact == "spark-milvus-native-runtime":
            expected = properties(archive.read(COMPATIBILITY))
            require(expected == {"format.version": "1", "storage.revision": plan["pins"]["milvus-storage"],
                                 "knowhere.revision": plan["pins"]["knowhere"]},
                    "Thin/native compatibility identity mismatch")


def stage(plan_path, inputs, output):
    plan = json.loads(plan_path.read_text())
    validate_plan(plan)
    require(not output.exists(), "Candidate output already exists; never overwrite a candidate")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="release-stage-", dir=output.parent) as temp:
        candidate = Path(temp) / "candidate"
        repository = candidate / "repository"
        repository.mkdir(parents=True)
        manifest = {"schemaVersion": 1, "version": plan["version"], "plan": plan, "artifacts": [], "nativeAbi": {}}
        license_bytes = (ROOT / "LICENSE").read_bytes()

        def record(artifact, kind, source=None, content=None, classifier="", extension="jar", line=None, platform=None):
            relative = artifact_path(plan["groupId"], artifact, plan["version"], classifier, extension)
            destination = repository / relative
            require(not destination.exists(), f"Duplicate Maven artifact: {relative}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source is not None:
                shutil.copyfile(source, destination)
            else:
                destination.write_bytes(content)
            manifest["artifacts"].append(dict(groupId=plan["groupId"], artifactId=artifact,
                version=plan["version"], classifier=classifier, extension=extension, kind=kind,
                line=line, platform=platform, path=relative, sha256=digest(destination), size=destination.stat().st_size))

        def document_jars(artifact, kind, readme, line=None, platform=None, main=False):
            for classifier in ([""] if main else []) + ["sources", "javadoc"]:
                path = Path(temp) / f"{artifact}-{classifier or 'entry'}.jar"
                small_jar(path, readme, license_bytes)
                record(artifact, kind, source=path, classifier=classifier, line=line, platform=platform)

        for item in plan["native"]:
            platform = item["platform"]
            matches = list(inputs.rglob(f"milvus-native-{platform}.jar"))
            require(len(matches) == 1, f"Expected exactly one native bundle for {platform}")
            source = matches[0]
            sidecar = properties(Path(str(source) + ".properties").read_bytes())
            require(sidecar.get("jar.sha256") == digest(source) and sidecar.get("platform") == platform,
                    "Native checksum sidecar mismatch")
            verify_native(source, plan, platform)
            abi = json.loads((source.parent / "abi.json").read_text())
            require(abi.get("platform") == platform and abi.get("jar.sha256") == digest(source),
                    "Native ABI evidence does not identify this bundle")
            require(abi.get("passed") is True and not abi.get("violations"), "Native ABI audit did not pass")
            require(all(abi.get("baseline", {}).get(key) == item[field]
                        for key, field in (("GLIBC", "glibc"), ("GLIBCXX", "glibcxx"), ("CXXABI", "cxxabi"))),
                    "Native ABI evidence uses a different baseline")
            manifest["nativeAbi"][platform] = abi
            artifact = f"spark-milvus-native-{platform}"
            record(artifact, "native", source=source, platform=platform)
            record(artifact, "native", content=pom_bytes(artifact, plan, []), extension="pom", platform=platform)
            document_jars(artifact, "native", f"# {artifact}\n\nResource-only native artifact.\n"
                f"Storage source: https://github.com/milvus-io/milvus-storage/tree/{plan['pins']['milvus-storage']}\n"
                f"Knowhere source: https://github.com/LawrenceTL92/knowhere-contrib/tree/{plan['pins']['knowhere']}\n"
                "The main JAR contains dependency provenance and third-party licenses.\n"
                "See docs/releasing.md in the connector source for build and system requirements.\n", platform=platform)
        jvm_lines = [] if plan.get("distribution") == "thin" else plan["jvm"]
        metadata = [json.loads(path.read_text()) | {"_directory": path.parent}
                    for path in inputs.rglob("artifact.json")]
        require(len(metadata) == len(jvm_lines), "Unexpected number of JVM metadata files")
        for item in jvm_lines:
            line = item["line"]
            matches = [record for record in metadata if record.get("sparkLine") == line]
            require(len(matches) == 1, f"Expected exactly one JVM build for Spark {line}")
            data = matches[0]
            artifact = f"spark-milvus-{line}-jvm_2.13"
            require(data["artifactId"] == artifact and data["groupId"] == plan["groupId"]
                    and data["version"] == plan["version"] and data["sourceSha"] == plan["sourceSha"],
                    "JVM build identity mismatch")
            require({file["classifier"] for file in data["files"]} == {"", "sources", "javadoc"}
                    and len(data["files"]) == 3, "Incomplete JVM artifact set")
            for file in data["files"]:
                source = regular_file(data["_directory"], file["path"])
                require(digest(source) == file["sha256"], "JVM build checksum mismatch")
                if not file["classifier"]:
                    verify_jvm(source, plan)
                record(artifact, "jvm", source=source, classifier=file["classifier"], line=line)
            dependencies = [dict(dep, scope="provided") for dep in data["providedDependencies"]]
            dependencies.extend(data.get("runtimeDependencies", []))
            record(artifact, "jvm", content=pom_bytes(artifact, plan, dependencies), extension="pom", line=line)
            for platform in [None] + [native["platform"] for native in plan["native"]]:
                suffix = "-" + platform if platform else ""
                artifact = f"spark-milvus-{line}{suffix}_2.13"
                deps = entry_dependencies(plan, line, platform)
                record(artifact, "entry", content=pom_bytes(artifact, plan, deps), extension="pom", line=line, platform=platform)
                document_jars(artifact, "entry", f"# {artifact}\n\nMaven dependency entry; implementation and native "
                    "resources are provided by the dependencies in this artifact's POM.\n", line=line, platform=platform, main=True)
        thin_exports = list(inputs.rglob("thin-artifacts.json"))
        has_thin = any(item["line"] == "4.0" for item in plan["jvm"])
        require(len(thin_exports) == int(has_thin), "Expected exactly one thin module export for Spark 4.0")
        if has_thin:
            metadata_path = thin_exports[0]
            data = json.loads(metadata_path.read_text())
            modules = thin_metadata(data, plan)
            manifest["thin"] = data | {"modules": [{key: value for key, value in module.items() if key != "files"}
                                                  for module in modules.values()]}
            for artifact, module in modules.items():
                files = module.get("files", [])
                require(len(files) == len(MAVEN_FILES)
                        and {(file["classifier"], file["extension"]) for file in files} == MAVEN_FILES,
                        "Incomplete thin artifact set")
                for file in files:
                    source = regular_file(metadata_path.parent, file["path"])
                    require(digest(source) == file["sha256"] and source.stat().st_size == file["size"],
                            "Thin build checksum mismatch")
                    record(artifact, "thin", source=source, classifier=file["classifier"],
                           extension=file["extension"], line="4.0")
        for file in manifest["artifacts"]:
            path = repository / file["path"]
            for algorithm in ("md5", "sha1", "sha256", "sha512"):
                Path(str(path) + "." + algorithm).write_text(digest(path, algorithm) + "\n", encoding="ascii")
        manifest["artifacts"].sort(key=lambda file: file["path"])
        write_json(candidate / "release-manifest.json", manifest)
        verify(candidate)
        candidate.rename(output)


def verify(candidate):
    manifest = json.loads((candidate / "release-manifest.json").read_text())
    require(manifest.get("schemaVersion") == 1, "Unsupported candidate schema")
    plan = manifest["plan"]
    validate_plan(plan)
    require(manifest["version"] == plan["version"], "Candidate version differs from plan")
    repository = candidate / "repository"
    files = manifest["artifacts"]
    require(len({file["path"] for file in files}) == len(files), "Duplicate candidate file")
    expected = expected_artifacts(plan)
    thin_modules = thin_metadata(manifest.get("thin", {}), plan) if any(
        role[0] == "thin" for role in expected.values()) else {}
    thin_classes = {}
    slots = {(artifact, classifier, extension) for artifact in expected
             for classifier, extension in MAVEN_FILES}
    actual = {(file["artifactId"], file["classifier"], file["extension"]) for file in files}
    require(actual == slots and len(files) == len(slots), "Candidate is missing or has unexpected Maven artifacts")
    expected_paths = set()
    for file in files:
        require(file["groupId"] == plan["groupId"] and file["version"] == plan["version"], "Mixed Maven identities")
        require((file["kind"], file["line"], file["platform"]) == expected[file["artifactId"]], "Artifact role mismatch")
        require(file["path"] == artifact_path(file["groupId"], file["artifactId"], file["version"],
                                             file["classifier"], file["extension"]), "Maven layout mismatch")
        path = regular_file(repository, file["path"])
        require(path.stat().st_size == file["size"] and digest(path) == file["sha256"], f"Changed candidate: {file['path']}")
        expected_paths.add(file["path"])
        for algorithm in ("md5", "sha1", "sha256", "sha512"):
            relative = file["path"] + "." + algorithm
            checksum = regular_file(repository, relative).read_text().strip()
            require(checksum == digest(path, algorithm), f"Checksum mismatch: {relative}")
            expected_paths.add(relative)
        if file["extension"] == "pom":
            project, deps = pom_dependencies(path)
            ns = {"m": "http://maven.apache.org/POM/4.0.0"}
            for field in ("groupId", "artifactId", "version"):
                require(project.findtext("m:" + field, namespaces=ns) == file[field], "POM identity mismatch")
            for dep in deps:
                if dep.get("groupId") == plan["groupId"]:
                    require(dep.get("artifactId") in expected and dep["version"] == plan["version"],
                            "POM refers to an unpublished internal artifact")
            if file["kind"] == "entry":
                require(deps == entry_dependencies(plan, file["line"], file["platform"]), "Wrong entry dependency graph")
            elif file["kind"] == "thin":
                verify_thin_pom(file["artifactId"], deps, thin_modules[file["artifactId"]], plan,
                                manifest["thin"]["sparkVersion"])
        elif file["classifier"] == "sources":
            verify_sources(path, file["kind"], file["artifactId"])
        elif not file["classifier"]:
            if file["kind"] == "jvm":
                verify_jvm(path, plan)
            elif file["kind"] == "thin":
                verify_thin_jar(path, file["artifactId"], plan, thin_classes)
            elif file["kind"] == "native":
                verify_native(path, plan, file["platform"])
                abi = manifest["nativeAbi"][file["platform"]]
                require(abi.get("passed") is True and not abi.get("violations")
                        and abi.get("jar.sha256") == file["sha256"]
                        and abi.get("platform") == file["platform"], "ABI evidence mismatch")
                native = next(item for item in plan["native"] if item["platform"] == file["platform"])
                require(all(abi.get("baseline", {}).get(key) == native[field]
                            for key, field in (("GLIBC", "glibc"), ("GLIBCXX", "glibcxx"), ("CXXABI", "cxxabi"))),
                        "ABI evidence baseline mismatch")
            else:
                with zipfile.ZipFile(path) as archive:
                    names = archive_names(archive)
                    require(not any(name.endswith(".class") or name.startswith("native/") for name in names),
                            "Entry JAR contains implementation")
    actual_paths = {str(path.relative_to(repository)) for path in repository.rglob("*") if path.is_file()}
    require(actual_paths == expected_paths, "Candidate repository contains untracked files")
    return manifest


def secret_primary_key():
    result = subprocess.run(
        ["gpg", "--batch", "--with-colons", "--fingerprint", "--list-secret-keys"],
        check=True, capture_output=True, text=True)
    fingerprints = []
    expecting_fingerprint = False
    for line in result.stdout.splitlines():
        fields = line.split(":")
        kind = fields[0] if fields else ""
        if kind == "sec":
            require(not expecting_fingerprint, "Secret primary key is missing its fingerprint")
            expecting_fingerprint = True
        elif kind == "fpr" and expecting_fingerprint:
            require(len(fields) > 9 and re.fullmatch(r"(?:[0-9A-Fa-f]{40}|[0-9A-Fa-f]{64})", fields[9]),
                    "Secret primary key has an invalid fingerprint")
            fingerprints.append(fields[9].upper())
            expecting_fingerprint = False
    require(not expecting_fingerprint, "Secret primary key is missing its fingerprint")
    require(len(fingerprints) == 1,
            f"Expected exactly one secret primary key, found {len(fingerprints)}")
    require(fingerprints[0] == SIGNING_FINGERPRINT,
            f"Secret primary key does not match release fingerprint {SIGNING_FINGERPRINT}")
    return fingerprints[0]


def bundle(candidate, output):
    manifest = verify(candidate)
    require(not output.exists(), "Central bundle already exists")
    passphrase = os.environ.get("GPG_PASSPHRASE")
    require(passphrase is not None, "GPG_PASSPHRASE must be set for signing")
    signing_key = secret_primary_key()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="central-sign-") as directory:
        repository = Path(directory) / "repository"
        shutil.copytree(candidate / "repository", repository)
        for file in manifest["artifacts"]:
            path = repository / file["path"]
            subprocess.run(["gpg", "--batch", "--yes", "--pinentry-mode", "loopback", "--passphrase-fd", "0",
                            "--local-user", signing_key, "--armor", "--detach-sign",
                            "--output", str(path) + ".asc", str(path)],
                           input=passphrase.encode(), check=True)
            require(Path(str(path) + ".asc").is_file(), f"GPG did not create a signature: {file['path']}")
        with zipfile.ZipFile(output, "w", zipfile.ZIP_STORED, allowZip64=True) as archive:
            for path in sorted(repository.rglob("*")):
                if path.is_file():
                    archive.write(path, str(path.relative_to(repository)))
    print(json.dumps({"bundle": str(output), "sha256": digest(output), "version": manifest["version"]}))


def import_key():
    # Accept both common GitHub secret encodings without writing the private
    # key to disk or passing it as a process argument.
    encoded = os.environ.get("GPG_PRIVATE_KEY", "").strip().encode()
    require(encoded, "GPG_PRIVATE_KEY must be set")
    if not encoded.startswith(b"-----BEGIN PGP PRIVATE KEY BLOCK-----"):
        try:
            encoded = base64.b64decode(b"".join(encoded.split()), validate=True)
        except ValueError:
            raise ValueError("GPG_PRIVATE_KEY must be an armored key or its base64 encoding") from None
    require(encoded.startswith(b"-----BEGIN PGP PRIVATE KEY BLOCK-----"),
            "GPG_PRIVATE_KEY must contain an armored private key")
    subprocess.run(["gpg", "--batch", "--import"], input=encoded, check=True)


def central_token():
    username, password = os.environ.get("CENTRAL_USERNAME"), os.environ.get("CENTRAL_TOKEN")
    require(username and password, "CENTRAL_USERNAME and CENTRAL_TOKEN must be set")
    return base64.b64encode(f"{username}:{password}".encode()).decode()


def central_request(path, token, data=b"", headers=None, method="POST", allow_not_found=False):
    request = urllib.request.Request(CENTRAL + path, data=data, method=method,
        headers={"Authorization": "Bearer " + token, **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        if allow_not_found and error.code == 404:
            error.close()
            return None
        # Do not include request headers or credential-bearing objects in logs.
        raise ValueError(f"Central returned HTTP {error.code}: {error.read(4096).decode(errors='replace')}") from None


def central_download(path, token, output):
    request = urllib.request.Request(
        CENTRAL + path, method="GET", headers={"Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(request, timeout=120) as response, output.open("wb") as stream:
            shutil.copyfileobj(response, stream)
    except urllib.error.HTTPError as error:
        # Do not include request headers or credential-bearing objects in logs.
        raise ValueError(f"Central returned HTTP {error.code}: {error.read(4096).decode(errors='replace')}") from None


def checked_deployment_id(value):
    try:
        parsed = str(uuid.UUID(value))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("Invalid Central deployment ID") from None
    require(parsed == value.lower(), "Invalid Central deployment ID")
    return parsed


def await_central(deployment, token, expected, timeout=1800):
    deployment = checked_deployment_id(deployment)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = json.loads(central_request("/status?id=" + deployment, token))
        state = status.get("deploymentState")
        print(f"Central deployment {deployment}: {state}", flush=True)
        if state == expected:
            return status
        require(state != "FAILED", "Central validation failed: " + json.dumps(status.get("errors", {})))
        require(state in {"PENDING", "VALIDATING", "VALIDATED", "PUBLISHING", "PUBLISHED"}, "Unknown Central state")
        time.sleep(10)
    raise TimeoutError(f"Central deployment {deployment} did not reach {expected}")


def upload_bundle(path, token):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "Central bundle must be a regular file")
    # curl streams the potentially multi-gigabyte multipart upload from disk.
    # The credential is supplied through stdin config, never argv or a log.
    config = f'header = "Authorization: Bearer {token}"\n'
    result = subprocess.run(["curl", "--fail-with-body", "--silent", "--show-error", "--config", "-",
        "--max-time", "3600", "--form", f"bundle=@{path};type=application/octet-stream",
        CENTRAL + "/upload?publishingType=USER_MANAGED"], input=config, text=True, capture_output=True)
    require(result.returncode == 0, "Central upload failed: " + result.stderr[:2000] + result.stdout[:2000])
    deployment = checked_deployment_id(result.stdout.strip())
    print(f"Central deployment: {deployment}", flush=True)
    return deployment


def promote_deployment(deployment, token):
    deployment = checked_deployment_id(deployment)
    central_request("/deployment/" + deployment, token)


def drop_deployment(deployment, token):
    deployment = checked_deployment_id(deployment)
    response = central_request(
        "/deployment/" + deployment, token, data=None, method="DELETE", allow_not_found=True)
    if response is None:
        print(f"Central deployment already absent: {deployment}", flush=True)
        return False
    print(f"Dropped Central deployment: {deployment}", flush=True)
    return True


def drop(deployment, report_path=None):
    deployment = checked_deployment_id(deployment)
    report = None
    if report_path is not None:
        require(report_path.is_file(), "Central test report does not exist")
        report = json.loads(report_path.read_text())
        require(report.get("deploymentId") == deployment,
                "Central test report identifies another deployment")
    removed = drop_deployment(deployment, central_token())
    result = {"deploymentId": deployment, "dropped": True,
              "dropResult": "dropped" if removed else "already-absent"}
    if report is not None:
        report.update(dropped=True, dropResult=result["dropResult"])
        report.pop("dropError", None)
        write_json(report_path, report)
    return result


def download_deployment(candidate, output, deployment, token):
    manifest = verify(candidate)
    deployment = checked_deployment_id(deployment)
    require(not output.exists(), "Central test candidate already exists")
    output.mkdir(parents=True)
    write_json(output / "release-manifest.json", manifest)
    source_repository = candidate / "repository"
    for source in sorted(path for path in source_repository.rglob("*") if path.is_file()):
        relative = source.relative_to(source_repository)
        destination = output / "repository" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        encoded = urllib.parse.quote(str(relative), safe="/")
        central_download(f"/deployment/{deployment}/download/{encoded}", token, destination)
        require(destination.stat().st_size == source.stat().st_size
                and digest(destination) == digest(source), f"Central deployment bytes differ: {relative}")
    verify(output)
    return manifest


def append_github_output(path, name, value):
    require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), "Invalid GitHub output name")
    with path.open("a", encoding="utf-8") as stream:
        print(f"{name}={value}", file=stream)


def test_publish(bundle_path, candidate, output, report_path, github_output=None):
    manifest = verify(candidate)
    bundle_path = Path(bundle_path)
    require(bundle_path.is_file() and not bundle_path.is_symlink(), "Central bundle must be a regular file")
    require(not report_path.exists(), "Central test report already exists")
    token = central_token()
    report = {
        "schemaVersion": 1,
        "operation": "central-test",
        "version": manifest["version"],
        "sourceSha": manifest["plan"]["sourceSha"],
        "bundleSha256": digest(bundle_path),
        "manifestSha256": digest(candidate / "release-manifest.json"),
        "deploymentId": None,
        "deploymentState": "PREPARED",
        "downloadVerified": False,
        "dropped": False,
    }
    write_json(report_path, report)
    deployment = None
    failure = None
    cleanup_failure = None
    try:
        deployment = upload_bundle(bundle_path, token)
        report.update(deploymentId=deployment, deploymentState="UPLOADED")
        write_json(report_path, report)
        if github_output is not None:
            append_github_output(github_output, "deployment_id", deployment)
        status = await_central(deployment, token, "VALIDATED")
        report["deploymentState"] = status["deploymentState"]
        write_json(report_path, report)
        download_deployment(candidate, output, deployment, token)
        report["downloadVerified"] = True
        write_json(report_path, report)
    except BaseException as error:
        failure = error
    finally:
        if deployment is not None:
            try:
                removed = drop_deployment(deployment, token)
                report.update(dropped=True, dropResult="dropped" if removed else "already-absent")
            except BaseException as error:
                cleanup_failure = error
                report["dropError"] = str(error)
            write_json(report_path, report)
    if failure is not None:
        if cleanup_failure is not None:
            failure.add_note(f"Central deployment cleanup also failed: {cleanup_failure}")
        raise failure.with_traceback(failure.__traceback__)
    if cleanup_failure is not None:
        raise cleanup_failure.with_traceback(cleanup_failure.__traceback__)
    return report


def publish(path):
    token = central_token()
    deployment = upload_bundle(path, token)
    await_central(deployment, token, "VALIDATED")
    promote_deployment(deployment, token)
    await_central(deployment, token, "PUBLISHED")


def download_public(candidate, output):
    manifest = verify(candidate)
    require(not output.exists(), "Public verification directory already exists")
    output.mkdir(parents=True)
    write_json(output / "release-manifest.json", manifest)
    for path in sorted((candidate / "repository").rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(candidate / "repository")
        destination = output / "repository" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(30):
            try:
                with urllib.request.urlopen(CENTRAL_REPOSITORY + "/" + str(relative), timeout=120) as response:
                    with destination.open("wb") as stream:
                        shutil.copyfileobj(response, stream)
                break
            except urllib.error.HTTPError as error:
                if error.code not in (404, 429, 502, 503) or attempt == 29:
                    raise
                time.sleep(10)
        require(digest(destination) == digest(path), f"Central bytes differ: {relative}")
    verify(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("import-key")
    plan = commands.add_parser("plan")
    plan.add_argument("--version", required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--matrix", type=Path, required=True,
                      help="Versions exportBuildMatrix JSON; release lines have one source of truth")
    plan.add_argument("--thin-only", action="store_true",
                      help="Export a Spark 4.0 thin-only plan for separate module consumption checks")
    staging = commands.add_parser("stage")
    staging.add_argument("--plan", type=Path, required=True)
    staging.add_argument("--inputs", type=Path, required=True)
    staging.add_argument("--output", type=Path, required=True)
    verification = commands.add_parser("verify")
    verification.add_argument("--candidate", type=Path, required=True)
    signing = commands.add_parser("bundle")
    signing.add_argument("--candidate", type=Path, required=True)
    signing.add_argument("--output", type=Path, required=True)
    publishing = commands.add_parser("publish")
    publishing.add_argument("--bundle", type=Path, required=True)
    testing = commands.add_parser("test-publish")
    testing.add_argument("--bundle", type=Path, required=True)
    testing.add_argument("--candidate", type=Path, required=True)
    testing.add_argument("--output", type=Path, required=True)
    testing.add_argument("--report", type=Path, required=True)
    testing.add_argument("--github-output", type=Path)
    dropping = commands.add_parser("drop")
    dropping.add_argument("--deployment-id", required=True)
    dropping.add_argument("--report", type=Path)
    public = commands.add_parser("download-public")
    public.add_argument("--candidate", type=Path, required=True)
    public.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "import-key":
        import_key()
    elif args.command == "plan":
        create_plan(args.version, args.output, args.matrix, thin_only=args.thin_only)
    elif args.command == "stage":
        stage(args.plan, args.inputs, args.output)
    elif args.command == "verify":
        verify(args.candidate)
    elif args.command == "bundle":
        bundle(args.candidate, args.output)
    elif args.command == "publish":
        publish(args.bundle)
    elif args.command == "test-publish":
        test_publish(args.bundle, args.candidate, args.output, args.report, args.github_output)
    elif args.command == "drop":
        print(json.dumps(drop(args.deployment_id, args.report)))
    else:
        download_public(args.candidate, args.output)


if __name__ == "__main__":
    main()
