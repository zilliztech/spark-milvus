#!/usr/bin/env python3
"""Compile only smoke fixtures, then consume a Maven entry with spark-submit.

Production classes come exclusively from the candidate's published coordinates.
The compiler and spark-submit use separate, fresh dependency caches. Entries
read fixed, checksummed data, replace stored columns and verify
the resulting revisions with a worker JVM.
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
import tarfile
import tempfile
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[2]
_DOWNLOAD_SPEC = importlib.util.spec_from_file_location(
    "consumer_pinned_download", Path(__file__).with_name("pinned_download.py"))
downloads = importlib.util.module_from_spec(_DOWNLOAD_SPEC)
_DOWNLOAD_SPEC.loader.exec_module(downloads)
# Apache's official SHA-512 for this exact Scala 2.13 binary distribution.
# Upgrade this immutable input explicitly when changing the consumer runtime.
SPARK_ARCHIVES = {
    ("3.5.5", "2.13"): (
        "a462656d2a87afcf81f24e4dea9a99e6666ed5c8ff7ba0cc9c936188498c549cf8"
        "52d0ebb475325761d6cf645d1184d527be9a609503d587bb47d5532b533aa1"),
}
FIXTURES = (
    "spark/src/test/scala/com/zilliz/spark/connector/read/ArtifactConsumerSmoke.scala",
    "core/src/test/scala/com/zilliz/milvus/storage/io/ReadFixtures.scala",
)
MAIN = "com.zilliz.spark.connector.read.ArtifactConsumerSmoke"
FIXTURE_ROOT = ROOT / "core/src/test/resources/read-fixtures"
FIXTURE_GROUPS = ("collections/basic",)
REQUIRED_CHECKS = (
    "independent-executor", "v3-snapshot", "v3-advanced", "v3-empty",
    "repeated-actions", "physical-read", "column-replacement-receipt",
    "column-replacement-readback", "source-snapshot-unchanged",
)
FIXTURE_CLASSES = (
    MAIN.replace(".", "/"), "com/zilliz/milvus/storage/io/ReadFixtures",
)
GROUP = "com.zilliz"
# Native platform -> the (sys.platform, uname machine) of the host that consumes
# it; also what tells a platform bundle from the native-runtime, native-storage
# modules, whose names share the prefix.
HOSTS = {"linux-x86_64": ("linux", "x86_64"), "linux-aarch64": ("linux", "aarch64"),
         "darwin-aarch64": ("darwin", "arm64"), "darwin-x86_64": ("darwin", "x86_64")}
CENTRAL = "https://repo.maven.apache.org/maven2"


def completed_checks(log, required=REQUIRED_CHECKS):
    """Require each gate exactly once; a final banner cannot hide omitted work."""
    checks = []
    for line in log.splitlines():
        if line.startswith("MILVUS_CHECK\t"):
            fields = line.split("\t")
            if len(fields) != 3 or fields[2] != "passed":
                raise RuntimeError("Consumer check did not pass: " + line)
            checks.append(fields[1])
    if len(checks) != len(set(checks)) or set(checks) != set(required):
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


def verify_fixture_jar(jar, resources, allowed=FIXTURE_CLASSES):
    """The consumer contains only its test entry, integrity helper and fixed data."""
    with zipfile.ZipFile(jar) as archive:
        names = archive.namelist()
        if len(set(names)) != len(names):
            raise RuntimeError("Smoke JAR contains duplicate entries")
        classes = [name for name in names if name.endswith(".class")]
        if any(base + ".class" not in classes for base in allowed):
            raise RuntimeError("Smoke JAR is missing its artifact consumer entry or integrity helper")
        for name in classes:
            if not any(name == base + ".class" or name.startswith(base + "$")
                       for base in allowed):
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


def expected_artifacts(manifest, line, scala_binary, platform, entry):
    if entry not in ("universal", "platform"):
        raise ValueError("Unsupported consumer entry: " + entry)
    suffix = "" if entry == "universal" else "-" + platform
    entry_id = f"spark-milvus-{line}{suffix}_{scala_binary}"
    names = {entry_id, f"spark-milvus-{line}-jvm_{scala_binary}"}
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
                if name.startswith("native/milvus/2/") and name.endswith("/manifest.properties"):
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
        if name.removeprefix("spark-milvus-native-") in HOSTS
    }
    if native_platforms != wanted_platforms:
        raise RuntimeError(f"Resolved native platforms {native_platforms} != {wanted_platforms}")
    return found


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
    # apply. Supported Spark lines use SPARK_HOME/work/<app>/<executor>.
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
    shutil.copyfile(ROOT / "project/build.properties", project / "project/build.properties")
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
    jar = project / f"target/scala-{row['scalaBinary']}/smoke.jar"
    verify_fixture_jar(jar, resources, FIXTURE_CLASSES)
    return jar


def jvm_launcher(root, java_home, signal):
    """A JAVA_HOME whose bin/java preloads libjsig into the JVM alone.

    On macOS the system shell running spark-submit and spark-class refuses an
    inserted library of another architecture, so DYLD_INSERT_LIBRARIES is set
    by this launcher right before the JVM. The driver's children inherit the
    variable, and executors are launched through this same path, so its
    interpreter is this Python rather than a system shell.
    """
    home = root / "jvm-launcher"
    (home / "bin").mkdir(parents=True)
    java = java_home / "bin/java"
    launcher = home / "bin/java"
    launcher.write_text("#!" + sys.executable + "\n"
                        "import os, sys\n"
                        f"os.environ['DYLD_INSERT_LIBRARIES'] = {str(signal)!r}\n"
                        f"os.execv({str(java)!r}, [{str(java)!r}, *sys.argv[1:]])\n")
    launcher.chmod(0o755)
    return home


def consumer_row(manifest, line, scala):
    rows = [row for row in manifest["plan"]["jvm"]
            if row["line"] == line and row["scala"] == scala]
    if len(rows) != 1:
        raise ValueError(f"Candidate must contain exactly one Spark {line} / Scala {scala} row")
    row = rows[0]
    if row["scalaBinary"] != ".".join(scala.split(".")[:2]):
        raise ValueError("Candidate Scala version and binary version differ")
    return row


def java_major(home, env):
    text = subprocess.check_output(
        [str(home / "bin/java"), "-XshowSettings:properties", "-version"],
        env=env, stderr=subprocess.STDOUT, text=True,
    )
    match = re.search(r"^\s*java\.specification\.version = (\d+)\s*$", text, re.MULTILINE)
    if not match:
        raise ValueError("Cannot determine Java version: " + str(home))
    return match.group(1)


def verify_spark_installation(home, row):
    """Never run a 2.13 consumer against PySpark 3.5's default 2.12 distribution."""
    libraries = list((home / "jars").glob("scala-library-*.jar"))
    if len(libraries) != 1 or not libraries[0].name.startswith(f"scala-library-{row['scalaBinary']}."):
        raise ValueError("Spark distribution has the wrong Scala binary version")
    for module in ("spark-core", "spark-sql"):
        expected = f"{module}_{row['scalaBinary']}-{row['spark']}.jar"
        jars = list((home / "jars").glob(module + "_*.jar"))
        if len(jars) != 1 or jars[0].name != expected:
            raise ValueError("Spark distribution does not match the selected runtime: " + expected)
    if not (home / "bin/spark-submit").is_file():
        raise ValueError("Spark distribution is missing spark-submit")


def install_spark(root, row, env, download_cache=None):
    # Spark 3.5's PyPI distribution uses Scala 2.12. Apache publishes a separate
    # Scala 2.13 distribution; check its SHA-512 before extracting any files.
    if row["line"] == "3.5" and row["scalaBinary"] == "2.13":
        identity = (row["spark"], row["scalaBinary"])
        if identity not in SPARK_ARCHIVES:
            raise ValueError("No pinned Apache Spark archive for " + str(identity) +
                             "; update SPARK_ARCHIVES before changing the consumer runtime")
        name = f"spark-{row['spark']}-bin-hadoop3-scala2.13"
        url = f"https://archive.apache.org/dist/spark/spark-{row['spark']}/{name}.tgz"
        archive = downloads.fetch(url, SPARK_ARCHIVES[identity], cache_dir=download_cache)
        with tarfile.open(archive) as bundle:
            bundle.extractall(root, filter="data")
        home = root / name
        python = sys.executable
    else:
        # pip's PySpark distribution contains the real spark-submit and Spark jars.
        python_env = root / "python"
        python = str(python_env / "bin/python")
        run([sys.executable, "-m", "venv", str(python_env)], cwd=root, env=env)
        run([python, "-m", "pip", "install", "--disable-pip-version-check",
             f"pyspark=={row['spark']}"], cwd=root, env=env)
        home = Path(subprocess.check_output(
            [python, "-c", "import os,pyspark; print(os.path.dirname(pyspark.__file__))"],
            env=env, text=True,
        ).strip())
    verify_spark_installation(home, row)
    return home, python


def smoke(candidate, line, scala, platform, output, *, central=False, entry=None, download_cache=None):
    manifest = json.loads((candidate / "release-manifest.json").read_text())
    version = manifest["version"]
    row = consumer_row(manifest, line, scala)
    repository = CENTRAL if central else (candidate / "repository").as_uri()
    entries = (entry,) if entry else ("universal", "platform")
    for kind in entries:
        expected_artifacts(manifest, line, row["scalaBinary"], platform, kind)
    # Platform -> (sys.platform, uname machine) of a matching consumer host.
    if platform not in HOSTS or (sys.platform, os.uname().machine) != HOSTS[platform]:
        raise ValueError("Consumer runner does not match " + platform)
    darwin = sys.platform == "darwin"
    output.mkdir(parents=True, exist_ok=True)
    resources = fixture_resources()
    env = os.environ.copy()
    # The native package must work without engine libraries installed on the host.
    for name in ("CLASSPATH", "SPARK_CLASSPATH", "SPARK_DIST_CLASSPATH", "SPARK_SUBMIT_OPTS",
                 "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "LD_LIBRARY_PATH",
                 "LD_PRELOAD", "DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES", "DYLD_FALLBACK_LIBRARY_PATH",
                 "PYSPARK_SUBMIT_ARGS"):
        env.pop(name, None)
    build_java = Path(env["JAVA_HOME"])
    java_home = Path(env.get("CONSUMER_JAVA_HOME", str(build_java)))
    for home, wanted, label in ((build_java, row["buildJava"], "build"),
                                (java_home, row["java"], "runtime")):
        if java_major(home, env) != str(wanted):
            raise ValueError(f"Consumer {label} requires Java {wanted}: {home}")
    # The running JDK's signal-chaining library, preloaded the platform's way.
    preload = "DYLD_INSERT_LIBRARIES" if darwin else "LD_PRELOAD"
    signal = java_home / ("lib/libjsig.dylib" if darwin else "lib/libjsig.so")
    if not signal.is_file():
        raise ValueError("JDK is missing libjsig: " + str(signal))
    env.update(SPARK_LOCAL_IP="127.0.0.1", OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2")
    with tempfile.TemporaryDirectory(prefix="milvus-consumer-") as directory:
        root = Path(directory)
        # Linux preloads libjsig into spark-submit and everything below it; macOS
        # only into the JVMs, through a launcher spark-submit selects as JAVA_HOME.
        launch = ({"JAVA_HOME": str(jvm_launcher(root, java_home, signal))} if darwin
                  else {"JAVA_HOME": str(java_home), preload: str(signal)})
        spark_home, python = install_spark(root, row, env, download_cache)
        env["SPARK_HOME"] = str(spark_home)
        env["PYSPARK_PYTHON"] = python
        reports = []
        for entry in entries:
            work = root / entry
            work.mkdir()
            entry_id, expected = expected_artifacts(manifest, line, row["scalaBinary"], platform, entry)
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
                "--conf", f"spark.executorEnv.{preload}={signal}",
                "--conf", "spark.executorEnv.MALLOC_ARENA_MAX=2",
                "--conf", "spark.executorEnv.MALLOC_MMAP_THRESHOLD_=1048576",
                "--conf", "spark.executorEnv.MALLOC_TRIM_THRESHOLD_=134217728",
                "--repositories", repository,
                "--packages", f"{GROUP}:{entry_id}:{version}",
            ]
            command += ["--class", MAIN, str(fixture)]
            try:
                run(command, cwd=work, env={**env, **launch}, log=log)
            finally:
                collect_diagnostics(spark_home, work, diagnostics, previous_apps)
            text = log.read_text()
            checks = completed_checks(text, REQUIRED_CHECKS)
            resolved = check_resolved(ivy, expected)
            report = {"entry": entry_id, "resolved": resolved, "result": "passed", **checks}
            reports.append(report)
        (output / "consumer-result.json").write_text(json.dumps({
            "version": version, "platform": platform, "line": line,
            "scala": scala, "scalaBinary": row["scalaBinary"],
            "java": row["java"], "buildJava": row["buildJava"], "spark": row["spark"],
            "manifest_sha256": digest(candidate / "release-manifest.json"), "entries": reports,
            "read_fixture_sha256": resources,
        }, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--line", choices=("3.5", "4.0", "4.1", "4.2"), required=True)
    parser.add_argument("--scala", required=True, help="Exact Scala version from the candidate's JVM row")
    parser.add_argument("--platform", choices=sorted(HOSTS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--central", action="store_true", help="Resolve from public Maven Central")
    parser.add_argument("--entry", choices=("universal", "platform"), help="Consume one delivery form")
    parser.add_argument("--download-cache", type=Path,
                        help="Verified download cache (default: SPARK_MILVUS_DOWNLOAD_CACHE or ~/.cache/spark-milvus/downloads)")
    args = parser.parse_args()
    smoke(args.candidate.resolve(), args.line, args.scala, args.platform, args.output.resolve(),
          central=args.central, entry=args.entry, download_cache=args.download_cache)


if __name__ == "__main__":
    main()
