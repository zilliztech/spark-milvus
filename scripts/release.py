#!/usr/bin/env python3
"""Stage, verify and publish an immutable Maven release candidate.

Only ``publish`` changes Central; ``download-public`` verifies its public files.
Candidate packaging and verification use the exact bytes tested by consumers.
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
COMPATIBILITY = "META-INF/milvus-native/compatibility.properties"
VERSION = re.compile(r"2\.\d+\.\d+(?:-[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)?")
SHA = re.compile(r"[0-9a-f]{40}")
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
    require(native and len(set(native)) == len(native), "Empty or duplicate native matrix")
    require(set(native) <= {"linux-x86_64", "linux-aarch64"}, "Unsupported native release platform")
    require(lines and len(set(lines)) == len(lines), "Empty or duplicate JVM matrix")
    require(set(lines) <= {"4.0", "4.2"}, "Unsupported JVM release line")
    for item in plan["native"]:
        require(item["libc"] == "glibc" and item["glibc"] == "2.35"
                and item["glibcxx"] == "3.4.30" and item["cxxabi"] == "1.3.13",
                "Unsupported system ABI baseline")
    for item in plan["jvm"]:
        require(item["scala"] == "2.13.16", "Unsupported Scala release version")


def create_plan(version, output):
    plan = json.loads(MATRIX.read_text())
    plan["version"] = version
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
        required = ["io/knowhere/", "io/milvus/storage/", "com/zilliz/spark/connector/"]
        for prefix in required:
            require(any(name.startswith(prefix) and name.endswith(".class") for name in names),
                    f"JVM JAR is missing API classes: {prefix}")
        expected = properties(archive.read(COMPATIBILITY))
        require(expected == {"format.version": "1", "storage.revision": plan["pins"]["milvus-storage"],
                             "knowhere.revision": plan["pins"]["knowhere"]},
                "JVM/native compatibility identity mismatch")


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
    result = {f"spark-milvus-native-{item['platform']}": ("native", None, item["platform"])
              for item in plan["native"]}
    for item in plan["jvm"]:
        line = item["line"]
        result[f"spark-milvus-{line}-jvm_2.13"] = ("jvm", line, None)
        result[f"spark-milvus-{line}_2.13"] = ("entry", line, None)
        for native in plan["native"]:
            platform = native["platform"]
            result[f"spark-milvus-{line}-{platform}_2.13"] = ("entry", line, platform)
    return result


def entry_dependencies(plan, line, platform):
    names = [f"spark-milvus-{line}-jvm_2.13"]
    names.extend(f"spark-milvus-native-{native['platform']}" for native in plan["native"]
                 if platform is None or native["platform"] == platform)
    return [{"groupId": plan["groupId"], "artifactId": name, "version": plan["version"]}
            for name in names]


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
        metadata = [json.loads(path.read_text()) | {"_directory": path.parent}
                    for path in inputs.rglob("artifact.json")]
        require(len(metadata) == len(plan["jvm"]), "Unexpected number of JVM metadata files")
        for item in plan["jvm"]:
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
    slots = {(artifact, classifier, extension) for artifact in expected
             for classifier, extension in (("", "jar"), ("sources", "jar"), ("javadoc", "jar"), ("", "pom"))}
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
            project = ET.parse(path).getroot()
            ns = {"m": "http://maven.apache.org/POM/4.0.0"}
            for field in ("groupId", "artifactId", "version"):
                require(project.findtext("m:" + field, namespaces=ns) == file[field], "POM identity mismatch")
            require(not project.findall(".//m:repositories", ns), "POM declares an extra repository")
            deps = [{node.tag.split("}")[-1]: node.text for node in dep}
                    for dep in project.findall("m:dependencies/m:dependency", ns)]
            for dep in deps:
                require(dep.get("scope") != "system" and "systemPath" not in dep
                        and "SNAPSHOT" not in dep["version"].upper(), "Non-release POM dependency")
                if dep.get("groupId") == plan["groupId"]:
                    require(dep.get("artifactId") in expected and dep["version"] == plan["version"],
                            "POM refers to an unpublished internal artifact")
            if file["kind"] == "entry":
                require(deps == entry_dependencies(plan, file["line"], file["platform"]), "Wrong entry dependency graph")
        elif not file["classifier"]:
            if file["kind"] == "jvm":
                verify_jvm(path, plan)
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


def bundle(candidate, output):
    manifest = verify(candidate)
    require(not output.exists(), "Central bundle already exists")
    passphrase = os.environ.get("GPG_PASSPHRASE")
    require(passphrase is not None, "GPG_PASSPHRASE must be set for signing")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="central-sign-") as directory:
        repository = Path(directory) / "repository"
        shutil.copytree(candidate / "repository", repository)
        for file in manifest["artifacts"]:
            path = repository / file["path"]
            subprocess.run(["gpg", "--batch", "--yes", "--pinentry-mode", "loopback", "--passphrase-fd", "0",
                            "--armor", "--detach-sign", "--output", str(path) + ".asc", str(path)],
                           input=passphrase.encode(), check=True)
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


def central_request(path, token, data=b"", headers=None):
    request = urllib.request.Request(CENTRAL + path, data=data, method="POST",
        headers={"Authorization": "Bearer " + token, **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        # Do not include request headers or credential-bearing objects in logs.
        raise ValueError(f"Central returned HTTP {error.code}: {error.read(4096).decode(errors='replace')}") from None


def await_central(deployment, token, expected, timeout=1800):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = json.loads(central_request("/status?id=" + deployment, token))
        state = status.get("deploymentState")
        print(f"Central deployment {deployment}: {state}", flush=True)
        if state == expected:
            return
        require(state != "FAILED", "Central validation failed: " + json.dumps(status.get("errors", {})))
        require(state in {"PENDING", "VALIDATING", "VALIDATED", "PUBLISHING", "PUBLISHED"}, "Unknown Central state")
        time.sleep(10)
    raise TimeoutError(f"Central deployment {deployment} did not reach {expected}")


def publish(path):
    username, password = os.environ.get("CENTRAL_USERNAME"), os.environ.get("CENTRAL_TOKEN")
    require(username and password, "CENTRAL_USERNAME and CENTRAL_TOKEN must be set")
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    # curl streams the potentially multi-gigabyte multipart upload from disk.
    # The credential is supplied through stdin config, never argv or a log.
    config = f'header = "Authorization: Bearer {token}"\n'
    result = subprocess.run(["curl", "--fail-with-body", "--silent", "--show-error", "--config", "-",
        "--max-time", "3600", "--form", f"bundle=@{path};type=application/octet-stream",
        CENTRAL + "/upload?publishingType=USER_MANAGED"], input=config, text=True, capture_output=True)
    require(result.returncode == 0, "Central upload failed: " + result.stderr[:2000] + result.stdout[:2000])
    deployment = result.stdout.strip()
    require(str(uuid.UUID(deployment)) == deployment.lower(), "Invalid Central deployment ID")
    print(f"Central deployment: {deployment}", flush=True)
    await_central(deployment, token, "VALIDATED")
    central_request("/deployment/" + deployment, token)
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
    public = commands.add_parser("download-public")
    public.add_argument("--candidate", type=Path, required=True)
    public.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "import-key":
        import_key()
    elif args.command == "plan":
        create_plan(args.version, args.output)
    elif args.command == "stage":
        stage(args.plan, args.inputs, args.output)
    elif args.command == "verify":
        verify(args.candidate)
    elif args.command == "bundle":
        bundle(args.candidate, args.output)
    elif args.command == "publish":
        publish(args.bundle)
    else:
        download_public(args.candidate, args.output)


if __name__ == "__main__":
    main()
