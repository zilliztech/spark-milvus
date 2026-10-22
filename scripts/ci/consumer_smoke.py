#!/usr/bin/env python3
"""Compile only smoke fixtures, then consume a Maven entry with spark-submit.

Production classes come exclusively from the candidate's published coordinates.
The compiler and spark-submit use separate, fresh dependency caches. Entries
and thin modules read fixed, checksummed data with a worker JVM.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = (
    "spark/src/test/scala/com/zilliz/spark/connector/read/ReadOnlyConsumerSmoke.scala",
    "core/src/test/scala/com/zilliz/milvus/storage/io/ReadFixtures.scala",
)
MAIN = "com.zilliz.spark.connector.read.ReadOnlyConsumerSmoke"
FIXTURE_ROOT = ROOT / "core/src/test/resources/read-fixtures"
FIXTURE_GROUPS = ("collections/nearest",)
REQUIRED_CHECKS = (
    "independent-executor", "v3-snapshot", "v3-advanced", "v3-empty",
    "repeated-actions", "exact-unfiltered", "exact-filtered",
    "approx-unfiltered", "approx-filtered", "sql-search",
)
FIXTURE_CLASSES = (
    MAIN.replace(".", "/"), "com/zilliz/milvus/storage/io/ReadFixtures",
)
GROUP = "com.zilliz"
CENTRAL = "https://repo.maven.apache.org/maven2"


def completed_checks(log):
    """Require each gate exactly once; a final banner cannot hide omitted work."""
    checks = []
    for line in log.splitlines():
        if line.startswith("MILVUS_CHECK\t"):
            fields = line.split("\t")
            if len(fields) != 3 or fields[2] != "passed":
                raise RuntimeError("Consumer check did not pass: " + line)
            checks.append(fields[1])
    if len(checks) != len(set(checks)) or set(checks) != set(REQUIRED_CHECKS):
        raise RuntimeError("Consumer omitted or duplicated required checks: " + repr(checks))
    if "ALL PASS" not in log.splitlines():
        raise RuntimeError("Consumer did not finish")
    return {"executed": len(checks), "passed": len(checks), "failed": 0, "canceled": 0,
            "checks": checks}


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fixture_resources(root=FIXTURE_ROOT, groups=FIXTURE_GROUPS):
    """Use the same SHA256SUMS as the JVM reader, without generating any data."""
    resources = {}
    for group in groups:
        directory = root / group
        manifests = sorted(directory.rglob("SHA256SUMS"))
        if not manifests:
            raise ValueError("Missing fixed read fixture checksums: " + str(directory))
        for manifest in manifests:
            names = set()
            for line in manifest.read_text().splitlines():
                if not line:
                    continue
                match = re.fullmatch(r"([a-f0-9]{64})  (.+)", line)
                if not match:
                    raise ValueError("Invalid fixture checksum line: " + str(manifest))
                checksum, relative = match.groups()
                path = Path(relative)
                if path.is_absolute() or ".." in path.parts or path.as_posix() != relative:
                    raise ValueError("Unsafe fixture path: " + relative)
                if relative in names or relative == "SHA256SUMS":
                    raise ValueError("Duplicate or recursive fixture checksum: " + relative)
                names.add(relative)
                source = manifest.parent / path
                if not source.is_file() or source.is_symlink() or digest(source) != checksum:
                    raise ValueError("Fixed read fixture is missing or changed: " + str(source))
                resources["read-fixtures/" + source.relative_to(root).as_posix()] = checksum
            if not names:
                raise ValueError("Empty fixture checksums: " + str(manifest))
            resources["read-fixtures/" + manifest.relative_to(root).as_posix()] = digest(manifest)
    return resources


def verify_fixture_jar(jar, resources):
    """The consumer contains only its test entry, integrity helper and fixed data."""
    with zipfile.ZipFile(jar) as archive:
        names = archive.namelist()
        if len(set(names)) != len(names):
            raise RuntimeError("Smoke JAR contains duplicate entries")
        classes = [name for name in names if name.endswith(".class")]
        if any(base + ".class" not in classes for base in FIXTURE_CLASSES):
            raise RuntimeError("Smoke JAR is missing its read-only entry or integrity helper")
        for name in classes:
            if not any(name == base + ".class" or name.startswith(base + "$")
                       for base in FIXTURE_CLASSES):
                raise RuntimeError("Smoke JAR contains production classes: " + name)
        actual = {name: hashlib.sha256(archive.read(name)).hexdigest() for name in names
                  if name.startswith("read-fixtures/") and not name.endswith("/")}
        if actual != resources:
            raise RuntimeError("Smoke JAR fixed fixtures differ from their checked input")


def main_jars(manifest):
    return {
        artifact["artifactId"]: artifact
        for artifact in manifest["artifacts"]
        if artifact["extension"] == "jar" and not artifact.get("classifier")
    }


def expected_artifacts(manifest, line, platform, entry):
    if entry == "thin":
        if line != "4.0":
            raise ValueError("Thin consumer currently supports only Spark 4.0")
        names = {
            "spark-milvus-native-runtime", "spark-milvus-native-vector",
            "spark-milvus-native-storage_2.13", "spark-milvus-core_2.13",
            "spark-milvus-4.0-thin_2.13",
        }
        available = main_jars(manifest)
        missing = names - available.keys()
        if missing:
            raise ValueError("Candidate is missing thin artifacts: " + ", ".join(sorted(missing)))
        return "spark-milvus-4.0-thin_2.13", {name: available[name] for name in names}
    suffix = "" if entry == "universal" else "-" + platform
    entry_id = f"spark-milvus-{line}{suffix}_2.13"
    names = {entry_id, f"spark-milvus-{line}-jvm_2.13"}
    platforms = (
        [row["platform"] for row in manifest["plan"]["native"]]
        if entry == "universal"
        else [platform]
    )
    names.update("spark-milvus-native-" + value for value in platforms)
    available = main_jars(manifest)
    missing = names - available.keys()
    if missing:
        raise ValueError("Candidate is missing artifacts: " + ", ".join(sorted(missing)))
    return entry_id, {name: available[name] for name in names}


def check_resolved(ivy, expected):
    """Ivy's jars directory is the actual --packages output used by Spark."""
    jars = sorted((Path(ivy) / "jars").glob("*.jar"))
    found = {}
    native_platforms = set()
    for jar in jars:
        if jar.name.startswith("com.zilliz_"):
            found[jar.name] = digest(jar)
        with zipfile.ZipFile(jar) as archive:
            for name in archive.namelist():
                if name.startswith("native/milvus/1/") and name.endswith("/manifest.properties"):
                    native_platforms.add(name.split("/")[3])
    wanted = {item["sha256"] for item in expected.values()}
    if set(found.values()) != wanted or len(found) != len(expected):
        raise RuntimeError(
            "spark-submit resolved different connector artifacts from the candidate: "
            + json.dumps({"resolved": found, "expected": sorted(wanted)}, sort_keys=True)
        )
    wanted_platforms = {
        name.removeprefix("spark-milvus-native-")
        for name in expected
        if name in {"spark-milvus-native-linux-x86_64", "spark-milvus-native-linux-aarch64"}
    }
    if native_platforms != wanted_platforms:
        raise RuntimeError(f"Resolved native platforms {native_platforms} != {wanted_platforms}")
    return found


def thin_native_bundle(candidate, manifest, platform, requested=None):
    """Choose a separate, validated OSS bundle; it is never a thin dependency."""
    if requested is not None:
        path = requested.resolve()
    else:
        name = "spark-milvus-native-" + platform
        artifact = main_jars(manifest).get(name)
        if artifact is None:
            raise ValueError("Thin-only consumption needs --native-bundle")
        path = candidate / "repository" / artifact["path"]
        if digest(path) != artifact["sha256"]:
            raise ValueError("Native bundle differs from the verified candidate")
    spec = importlib.util.spec_from_file_location("release", ROOT / "scripts/release.py")
    release = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(release)
    release.verify_native(path, manifest["plan"], platform)
    return path


def ivy_settings(path, repository):
    """Do not consult a developer's Maven local repository during consumption."""
    settings = ET.Element("ivysettings")
    ET.SubElement(settings, "settings", defaultResolver="candidate-and-central")
    resolvers = ET.SubElement(settings, "resolvers")
    chain = ET.SubElement(resolvers, "chain", name="candidate-and-central", returnFirst="true")
    for name, root in (("candidate", repository), ("central", CENTRAL)):
        ET.SubElement(chain, "ibiblio", name=name, root=root, m2compatible="true", usepoms="true")
    ET.ElementTree(settings).write(path, encoding="utf-8", xml_declaration=True)


def run(command, *, cwd, env, log=None):
    print("+ " + " ".join(map(str, command)), flush=True)
    if log is None:
        subprocess.run(command, cwd=cwd, env=env, check=True)
        return
    with Path(log).open("w") as stream:
        process = subprocess.Popen(
            command, cwd=cwd, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        for line in process.stdout:
            print(line, end="", flush=True)
            stream.write(line)
        if process.wait():
            raise RuntimeError(f"Consumer failed; see {log}")


def collect_diagnostics(spark_home, work, output, previous_apps):
    """Copy only this entry's logs before its temporary Spark installation is removed."""
    # LocalSparkCluster bypasses WorkerArguments, so SPARK_WORKER_DIR does not
    # apply. Both supported Spark lines use SPARK_HOME/work/<app>/<executor>.
    worker_root = spark_home / "work"
    copied = []
    for app in sorted(worker_root.glob("*")):
        if app in previous_apps or app.is_symlink() or not app.is_dir():
            continue
        for executor in sorted(app.iterdir()):
            if executor.is_symlink() or not executor.is_dir():
                continue
            for source in sorted(executor.iterdir()):
                name = source.name
                is_log = (name in ("stdout", "stderr")
                          or name.startswith(("stdout.", "stderr."))
                          or (name.startswith("hs_err_pid") and name.endswith(".log")))
                if is_log and source.is_file() and not source.is_symlink():
                    target = output / "worker" / source.relative_to(worker_root)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
                    copied.append(str(target.relative_to(output)))
    # Also retain a report written to the driver's working directory if the JVM
    # could not use its explicit ErrorFile path. Do not copy JARs or core dumps.
    for source in sorted(work.glob("hs_err_pid*.log")):
        if source.is_file() and not source.is_symlink():
            target = output / "driver" / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            copied.append(str(target.relative_to(output)))
    output.mkdir(parents=True, exist_ok=True)
    (output / "diagnostics.json").write_text(json.dumps({
        "worker_directory": str(worker_root), "copied_files": copied,
        "jvm_error_files": [str(path.relative_to(output))
                            for path in sorted((output / "jvm").glob("hs_err_pid*.log"))],
    }, indent=2) + "\n")
    print(f"Saved consumer diagnostics to {output}", flush=True)


def compile_fixture(work, repository, version, row, entry_id, env):
    project = work / "fixture"
    (project / "project").mkdir(parents=True)
    sources = project / "src/main/scala"
    sources.mkdir(parents=True)
    for relative in FIXTURES:
        shutil.copyfile(ROOT / relative, sources / Path(relative).name)
    resources = fixture_resources()
    for relative in resources:
        destination = project / "src/main/resources" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(FIXTURE_ROOT.parent / relative, destination)
    (project / "project/build.properties").write_text("sbt.version=1.11.1\n")
    scala = row["scala"]
    # JSON strings are valid Scala string literals; no text is evaluated by a shell.
    quote = json.dumps
    (project / "build.sbt").write_text(
        'name := "release-consumer-smoke"\n'
        f'scalaVersion := {quote(scala)}\n'
        'Compile / packageBin / artifactName := { (_, _, _) => "smoke.jar" }\n'
        'externalResolvers := Seq(\n'
        f'  "candidate" at {quote(repository)}, "central" at {quote(CENTRAL)})\n'
        'libraryDependencies ++= Seq(\n'
        f'  "org.apache.spark" %% "spark-sql" % {quote(row["spark"])} % "provided",\n'
        f'  "com.zilliz" % {quote(entry_id)} % {quote(version)})\n'
    )
    run(
        ["sbt", "-batch", f"-Dsbt.ivy.home={work / 'compile-ivy'}", "package"],
        cwd=project, env={**env, "COURSIER_CACHE": str(work / "compile-coursier")},
    )
    jar = project / "target/scala-2.13/smoke.jar"
    verify_fixture_jar(jar, resources)
    return jar


def smoke(candidate, line, platform, output, *, central=False, entry=None, native_bundle=None):
    manifest = json.loads((candidate / "release-manifest.json").read_text())
    version = manifest["version"]
    row = next(row for row in manifest["plan"]["jvm"] if row["line"] == line)
    repository = CENTRAL if central else (candidate / "repository").as_uri()
    if entry:
        entries = (entry,)
    elif manifest["plan"].get("distribution") == "thin":
        entries = ("thin",)
    else:
        entries = ("universal", "platform")
        if line == "4.0" and "spark-milvus-4.0-thin_2.13" in main_jars(manifest):
            entries += ("thin",)
    if native_bundle is not None and entries != ("thin",):
        raise ValueError("--native-bundle requires --entry thin or a thin-only candidate")
    expected_machine = "x86_64" if platform == "linux-x86_64" else "aarch64"
    if sys.platform != "linux" or os.uname().machine != expected_machine:
        raise ValueError("Consumer runner does not match " + platform)
    output.mkdir(parents=True, exist_ok=True)
    resources = fixture_resources()
    env = os.environ.copy()
    # The native package must work without engine libraries installed on the host.
    for name in ("CLASSPATH", "SPARK_CLASSPATH", "SPARK_DIST_CLASSPATH", "SPARK_SUBMIT_OPTS",
                 "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "LD_LIBRARY_PATH",
                 "LD_PRELOAD", "PYSPARK_SUBMIT_ARGS"):
        env.pop(name, None)
    java_home = Path(env["JAVA_HOME"])
    signal = java_home / "lib/libjsig.so"
    if not signal.is_file():
        raise ValueError("JDK is missing libjsig: " + str(signal))
    env.update(SPARK_LOCAL_IP="127.0.0.1", OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2")
    with tempfile.TemporaryDirectory(prefix="milvus-consumer-") as directory:
        root = Path(directory)
        # pip's PySpark distribution contains the real bin/spark-submit and Spark jars.
        python_env = root / "python"
        run([sys.executable, "-m", "venv", str(python_env)], cwd=root, env=env)
        run([str(python_env / "bin/python"), "-m", "pip", "install", "--disable-pip-version-check",
             f"pyspark=={row['spark']}"], cwd=root, env=env)
        result = subprocess.check_output(
            [str(python_env / "bin/python"), "-c", "import os,pyspark; print(os.path.dirname(pyspark.__file__))"],
            env=env, text=True,
        ).strip()
        spark_home = Path(result)
        env["SPARK_HOME"] = str(spark_home)
        env["PYSPARK_PYTHON"] = str(python_env / "bin/python")
        reports = []
        for entry in entries:
            work = root / entry
            work.mkdir()
            entry_id, expected = expected_artifacts(manifest, line, platform, entry)
            native = thin_native_bundle(candidate, manifest, platform, native_bundle) if entry == "thin" else None
            native_digest = digest(native) if native else None
            fixture = compile_fixture(work, repository, version, row, entry_id, env)
            ivy = work / "runtime-ivy"
            settings = work / "ivysettings.xml"
            ivy_settings(settings, repository)
            log = output / f"{entry}.log"
            diagnostics = output / f"{entry}-diagnostics"
            (diagnostics / "jvm").mkdir(parents=True, exist_ok=True)
            error_file = shlex.quote(f"-XX:ErrorFile={diagnostics / 'jvm/hs_err_pid%p.log'}")
            previous_apps = set((spark_home / "work").glob("*"))
            command = [
                str(spark_home / "bin/spark-submit"),
                "--master", "local-cluster[1,2,1536]",
                "--driver-memory", "2g", "--executor-memory", "1g",
                "--driver-java-options", "-Dmilvus.smoke.master=local-cluster[1,2,1536] " + error_file,
                "--conf", "spark.executor.extraJavaOptions=" + error_file,
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.cores.max=2",
                "--conf", "spark.ui.enabled=false",
                "--conf", "spark.driver.host=127.0.0.1",
                "--conf", "spark.driver.bindAddress=127.0.0.1",
                "--conf", f"spark.jars.ivy={ivy}",
                "--conf", f"spark.jars.ivySettings={settings}",
                "--conf", f"spark.executorEnv.LD_PRELOAD={signal}",
                "--conf", "spark.executorEnv.MALLOC_ARENA_MAX=2",
                "--conf", "spark.executorEnv.MALLOC_MMAP_THRESHOLD_=1048576",
                "--conf", "spark.executorEnv.MALLOC_TRIM_THRESHOLD_=134217728",
                "--repositories", repository,
                "--packages", f"{GROUP}:{entry_id}:{version}",
            ]
            if native:
                command += ["--jars", str(native)]
            command += ["--class", MAIN, str(fixture)]
            try:
                run(command, cwd=work, env={**env, "LD_PRELOAD": str(signal)}, log=log)
            finally:
                collect_diagnostics(spark_home, work, diagnostics, previous_apps)
            text = log.read_text()
            checks = completed_checks(text)
            resolved = check_resolved(ivy, expected)
            report = {"entry": entry_id, "resolved": resolved, "result": "passed", **checks}
            if native:
                if digest(native) != native_digest:
                    raise RuntimeError("Native bundle changed during thin consumption")
                report["nativeBundle"] = {"sha256": native_digest, "platform": platform, "withCardinal": False}
            reports.append(report)
        (output / "consumer-result.json").write_text(json.dumps({
            "version": version, "platform": platform, "line": line,
            "manifest_sha256": digest(candidate / "release-manifest.json"), "entries": reports,
            "read_fixture_sha256": resources,
        }, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--line", choices=("4.0", "4.2"), required=True)
    parser.add_argument("--platform", choices=("linux-x86_64", "linux-aarch64"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--central", action="store_true", help="Resolve from public Maven Central")
    parser.add_argument("--entry", choices=("universal", "platform", "thin"), help="Consume one delivery form")
    parser.add_argument("--native-bundle", type=Path, help="Separate OSS platform JAR for a thin-only candidate")
    args = parser.parse_args()
    smoke(args.candidate.resolve(), args.line, args.platform, args.output.resolve(),
          central=args.central, entry=args.entry, native_bundle=args.native_bundle)


if __name__ == "__main__":
    main()
