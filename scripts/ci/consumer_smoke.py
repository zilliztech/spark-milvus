#!/usr/bin/env python3
"""Compile only smoke fixtures, then consume a Maven entry with spark-submit.

Production classes come exclusively from the candidate's published coordinates.
The compiler and spark-submit use separate, fresh dependency caches. Both entry
types run the same real native read/write/search workload with a worker JVM.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = (
    "spark-4.0/src/test/scala/com/zilliz/spark/connector/read/LocalEndToEndSmoke.scala",
    "core/src/test/scala/com/zilliz/milvus/storage/manifest/SnapshotSegmentFixture.scala",
)
MAIN = "com.zilliz.spark.connector.read.LocalEndToEndSmoke"
GROUP = "com.zilliz"
CENTRAL = "https://repo.maven.apache.org/maven2"


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main_jars(manifest):
    return {
        artifact["artifactId"]: artifact
        for artifact in manifest["artifacts"]
        if artifact["extension"] == "jar" and not artifact.get("classifier")
    }


def expected_artifacts(manifest, line, platform, entry):
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
        if name.startswith("spark-milvus-native-")
    }
    if native_platforms != wanted_platforms:
        raise RuntimeError(f"Resolved native platforms {native_platforms} != {wanted_platforms}")
    return found


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
    (project / "project/build.properties").write_text("sbt.version=1.11.1\n")
    scala = row["scala"]
    # JSON strings are valid Scala string literals; no text is evaluated by a shell.
    quote = json.dumps
    (project / "build.sbt").write_text(
        'name := "release-consumer-smoke"\n'
        f'scalaVersion := {quote(scala)}\n'
        'Compile / packageBin / artifactName := { (_, _, _) => "smoke.jar" }\n'
        'externalResolvers := Seq(\n'
        f'  "candidate" at {quote(repository)}, Resolver.mavenCentral)\n'
        'libraryDependencies ++= Seq(\n'
        f'  "org.apache.spark" %% "spark-sql" % {quote(row["spark"])} % "provided",\n'
        f'  "com.zilliz" % {quote(entry_id)} % {quote(version)})\n'
    )
    run(
        ["sbt", "-batch", f"-Dsbt.ivy.home={work / 'compile-ivy'}", "package"],
        cwd=project, env={**env, "COURSIER_CACHE": str(work / "compile-coursier")},
    )
    jar = project / "target/scala-2.13/smoke.jar"
    with zipfile.ZipFile(jar) as archive:
        unexpected = [
            name for name in archive.namelist()
            if name.endswith(".class")
            and not (name.startswith("com/zilliz/spark/connector/read/LocalEndToEndSmoke")
                     or name.startswith("com/zilliz/milvus/storage/manifest/SnapshotSegmentFixture"))
        ]
        if unexpected:
            raise RuntimeError("Smoke JAR contains production classes: " + str(unexpected))
    return jar


def smoke(candidate, line, platform, output, *, central=False):
    manifest = json.loads((candidate / "release-manifest.json").read_text())
    version = manifest["version"]
    row = next(row for row in manifest["plan"]["jvm"] if row["line"] == line)
    repository = CENTRAL if central else (candidate / "repository").as_uri()
    expected_machine = "x86_64" if platform == "linux-x86_64" else "aarch64"
    if sys.platform != "linux" or os.uname().machine != expected_machine:
        raise ValueError("Consumer runner does not match " + platform)
    output.mkdir(parents=True, exist_ok=True)
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
        for entry in ("universal", "platform"):
            work = root / entry
            work.mkdir()
            entry_id, expected = expected_artifacts(manifest, line, platform, entry)
            fixture = compile_fixture(work, repository, version, row, entry_id, env)
            ivy = work / "runtime-ivy"
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
                "--conf", f"spark.executorEnv.LD_PRELOAD={signal}",
                "--conf", "spark.executorEnv.MALLOC_ARENA_MAX=2",
                "--conf", "spark.executorEnv.MALLOC_MMAP_THRESHOLD_=1048576",
                "--conf", "spark.executorEnv.MALLOC_TRIM_THRESHOLD_=134217728",
                "--repositories", repository,
                "--packages", f"{GROUP}:{entry_id}:{version}",
                "--class", MAIN, str(fixture),
            ]
            try:
                run(command, cwd=work, env={**env, "LD_PRELOAD": str(signal)}, log=log)
            finally:
                collect_diagnostics(spark_home, work, diagnostics, previous_apps)
            text = log.read_text()
            if "ALL PASS" not in text or "PASS independent executor JVM" not in text:
                raise RuntimeError("Smoke did not complete every gate: " + str(log))
            resolved = check_resolved(ivy, expected)
            reports.append({"entry": entry_id, "resolved": resolved, "result": "passed"})
        (output / "consumer-result.json").write_text(json.dumps({
            "version": version, "platform": platform, "line": line,
            "manifest_sha256": digest(candidate / "release-manifest.json"), "entries": reports,
        }, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--line", choices=("4.0", "4.2"), required=True)
    parser.add_argument("--platform", choices=("linux-x86_64", "linux-aarch64"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--central", action="store_true", help="Resolve from public Maven Central")
    args = parser.parse_args()
    smoke(args.candidate.resolve(), args.line, args.platform, args.output.resolve(), central=args.central)


if __name__ == "__main__":
    main()
