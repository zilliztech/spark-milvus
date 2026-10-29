#!/usr/bin/env python3
"""Run the exported source matrix and require actual read/search test reports."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
SPARK_SUITES = (
    "com.zilliz.spark.connector.nearestby.NearestByJoinTest",
    "com.zilliz.spark.connector.nearestby.FrameSearchTest",
    "com.zilliz.spark.connector.nearestby.VectorFunctionsTest",
    "com.zilliz.spark.connector.search.SearchMergeTest",
    "com.zilliz.spark.connector.read.V3ReadContractTest",
    "com.zilliz.spark.connector.read.ArrowReadOwnershipTest",
)
CONNECT_SUITE = "com.zilliz.spark.connector.connect.ConnectReadSessionTest"
CORE_SUITES = (
    "com.zilliz.milvus.storage.io.NativeSegmentReadTest",
    "com.zilliz.milvus.storage.io.NativeStorageFixtureTest",
    "com.zilliz.milvus.storage.io.NativeSegmentFixtureTest",
    "com.zilliz.milvus.storage.read.exec.SegmentReaderTakeTest",
)
RUNTIME_SUITE = "com.zilliz.milvus.jni.runtime.NativeLibrariesTest"
INTEGRATION_SUITE = "com.zilliz.spark.connector.sources.MilvusDataSourceTest"

MODULE_CLEAN = [project + "/clean" for project in
                ("native-runtime", "native-storage", "native-vector", "core", "spark", "integration")]
# The layer-1 project IDs keep their published names; their directories sit under native/.
PROJECT_DIRECTORIES = {"native-runtime": "native/runtime", "native-storage": "native/storage",
                       "native-vector": "native/vector"}


def read_matrix(path):
    matrix = json.loads(Path(path).read_text())
    if matrix.get("schemaVersion") != 1:
        raise ValueError("Unsupported build matrix schema")
    for name in ("shared", "spark", "release"):
        if not isinstance(matrix.get(name), list) or not matrix[name]:
            raise ValueError("Empty build matrix: " + name)
    identities = [(row["line"], row["scala"]) for row in matrix["spark"]]
    if len(set(identities)) != len(identities):
        raise ValueError("Duplicate Spark/Scala combination")
    shared = [row["scala"] for row in matrix["shared"]]
    if len(set(shared)) != len(shared) or set(shared) != {scala for _, scala in identities}:
        raise ValueError("Shared Scala matrix differs from Spark sources")
    for row in matrix["spark"]:
        if row["scalaBinary"] != ".".join(row["scala"].split(".")[:2]):
            raise ValueError("Scala binary version differs from selected Scala")
        if not row["spark"].startswith(row["line"] + "."):
            raise ValueError("Spark compile version differs from its line")
    for row in matrix["release"]:
        match = [source for source in matrix["spark"]
                 if source["line"] == row["line"] and source["scala"] == row["scala"]]
        if len(match) != 1 or match[0]["spark"] != row["compileSpark"]:
            raise ValueError("Release combination is absent from the source matrix")
    return matrix


def check_reports(directory, required, *, allow_skipped=False):
    """Check XML coverage; ScalaTest cancellations also require log summaries."""
    reports = {}
    for path in sorted(Path(directory).glob("*.xml")):
        document = ET.parse(path).getroot()
        suites = [document] if document.tag == "testsuite" else document.findall("testsuite")
        for suite in suites:
            name = suite.get("name")
            if name in reports:
                raise ValueError("Duplicate test report: " + str(name))
            cases = suite.findall("testcase")
            counts = {key: int(suite.get(key, "0"))
                      for key in ("tests", "failures", "errors", "skipped")}
            if counts["tests"] != len(cases):
                raise ValueError("Test report count differs from cases: " + str(name))
            failed = counts["failures"] + counts["errors"]
            failed += sum(len(case.findall("failure")) + len(case.findall("error")) for case in cases)
            skipped = max(counts["skipped"], sum(len(case.findall("skipped")) for case in cases))
            if failed or (skipped and not allow_skipped):
                raise ValueError("Failed or skipped tests in " + str(name))
            reports[name] = {"tests": counts["tests"], "skipped": skipped}
    for name in required:
        if name not in reports or reports[name]["tests"] <= reports[name]["skipped"]:
            raise ValueError("Required suite did not execute a successful test: " + name)
    if not reports:
        raise ValueError("No test reports in " + str(directory))
    if sum(report["tests"] - report["skipped"] for report in reports.values()) <= 0:
        raise ValueError("No successful tests in " + str(directory))
    return reports


def check_execution(log, executions, *, allow_skipped=False):
    """Reconcile every test command with its completed framework summary.

    sbt's XML reporter writes ScalaTest canceled cases as ordinary testcases
    with skipped=0. Only the framework's summary provides their actual status.
    """
    summaries = []
    suites = None
    for line in Path(log).read_text(errors="replace").splitlines():
        match = re.fullmatch(r"\[info\] Suites: completed (\d+), aborted (\d+)", line)
        if match:
            suites = tuple(map(int, match.groups()))
            if suites[1]:
                raise ValueError("Aborted test suites in " + str(log))
            continue
        match = re.fullmatch(
            r"\[info\] Tests: succeeded (\d+), failed (\d+), canceled (\d+), ignored (\d+), pending (\d+)", line)
        if match:
            if suites is None:
                raise ValueError("Missing ScalaTest suite summary in " + str(log))
            counts = dict(zip(("succeeded", "failed", "canceled", "ignored", "pending"),
                              map(int, match.groups())))
            counts.update(tests=sum(counts.values()), errors=0, skipped=0,
                          suites=suites[0], aborted=suites[1])
            summaries.append(counts)
            suites = None
            continue
        match = re.fullmatch(r"\[info\] (?:Passed|Failed): Total (\d+), Failed (\d+), Errors (\d+), Passed (\d+)(?:, (?:Skipped|Ignored) (\d+))?", line)
        if match:
            total, failed, errors, passed = map(int, match.groups()[:4])
            skipped = int(match.group(5) or "0")
            # sbt can omit a skipped suffix, but never count those as successes.
            skipped = max(skipped, total - passed - failed - errors)
            summaries.append(dict(tests=total, succeeded=passed, failed=failed,
                                  errors=errors, skipped=skipped, canceled=0,
                                  ignored=0, pending=0))
    if len(summaries) != len(executions):
        raise ValueError("Missing or unexpected completed test summaries in " + str(log))
    result = {}
    for (project, reports), counts in zip(executions, summaries):
        xml_tests = sum(report["tests"] for report in reports.values())
        xml_skipped = sum(report["skipped"] for report in reports.values())
        unexecuted = sum(counts[key] for key in ("skipped", "canceled", "ignored", "pending"))
        if counts["tests"] != counts["succeeded"] + counts["failed"] + counts["errors"] + unexecuted:
            raise ValueError("Inconsistent framework test totals: " + project)
        if counts["tests"] != xml_tests or xml_skipped > unexecuted:
            raise ValueError("Framework summary differs from XML coverage: " + project)
        if "suites" in counts and counts["suites"] != len(reports):
            raise ValueError("Framework suite count differs from XML coverage: " + project)
        if counts["failed"] or counts["errors"] or counts["succeeded"] <= 0:
            raise ValueError("No successful complete test execution: " + project)
        if unexecuted and not allow_skipped:
            raise ValueError("Canceled, ignored, pending or skipped tests: " + project)
        result[project] = counts
    return result


def xml_evidence(reports):
    return {name: {"reportedTests": counts["tests"], "reportedSkipped": counts["skipped"]}
            for name, counts in reports.items()}


def signal_environment():
    """What the sbt process is given for signal chaining.

    Linux preloads the running JDK's libjsig into sbt and every JVM below it.
    macOS gives sbt nothing: the system shell that launches sbt refuses an
    inserted library of another architecture, and the forked test JVMs preload
    libjsig.dylib through the build's own test settings (Modules.nativeTest).
    """
    darwin = sys.platform == "darwin"
    signal = Path(os.environ["JAVA_HOME"]) / ("lib/libjsig.dylib" if darwin else "lib/libjsig.so")
    if not signal.is_file():
        raise ValueError("JDK is missing libjsig")
    return {} if darwin else {"LD_PRELOAD": str(signal)}


def run_sbt(name, commands, *, bundle=None, environment=None, line="4.0", scala="2.13.16"):
    logs = ROOT / "target/read-ci"
    logs.mkdir(parents=True, exist_ok=True)
    path = logs / (name + ".log")
    command = ["sbt", "-batch", "-Dsbt.log.noformat=true", "-Dsbt.supershell=false"]
    command.extend(("-DsparkVersion=" + line, "-DscalaVersion=" + scala))
    if bundle:
        bundle = Path(bundle).resolve(strict=True)
        command.append("-Dmilvus.native.bundle=" + str(bundle))
    command.extend(commands)
    print("Running " + name + "; log: " + str(path), flush=True)
    with path.open("w") as output:
        result = subprocess.run(command, cwd=ROOT, env={**os.environ, **(environment or {})},
                                stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError("sbt failed: " + str(path) + "\n" +
                           "\n".join(path.read_text(errors="replace").splitlines()[-60:]))
    return path


def report_directory(project):
    return ROOT / PROJECT_DIRECTORIES.get(project, project) / "target/test-reports"


def reset_reports(projects):
    for project in projects:
        shutil.rmtree(report_directory(project), ignore_errors=True)


def save_report(name, report):
    path = ROOT / "target/read-ci" / (name + ".json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("PASS " + name + "; evidence: " + str(path), flush=True)


def selected(matrix, group, scala, line=None):
    rows = [row for row in matrix[group] if row["scala"] == scala
            and (line is None or row["line"] == line)]
    if len(rows) != 1:
        raise ValueError("Unsupported matrix selection")
    return rows[0]


def spark_checks(matrix, line, scala, bundle):
    row = selected(matrix, "spark", scala, line)
    project = row["project"]
    suites = SPARK_SUITES + ((CONNECT_SUITE,) if line != "3.5" else ()) if bundle else (SPARK_SUITES[3],)
    name = ("spark-native-" if bundle else "spark-source-") + line + "-" + scala
    before = None if bundle else source_hashes()
    reset_reports((project,))
    commands = MODULE_CLEAN + [project + "/checkReadBuild", project + "/Test/compile"]
    if not bundle:
        commands.append("integration/Test/compile")
    commands.append(project + "/Test/testOnly " + " ".join(suites))
    log = run_sbt(name, commands, bundle=bundle, line=line, scala=scala)
    reports = check_reports(report_directory(project), suites)
    counts = check_execution(log, [(project, reports)])[project]
    report = {"selection": row, "native": bool(bundle),
              "xmlSuites": xml_evidence(reports), "execution": counts}
    if not bundle:
        integration = ROOT / "integration/target" / ("scala-" + row["scalaBinary"]) / "test-classes"
        report["clean"] = clean_contract(name, [project], [build_record(project, row["scalaBinary"]),
                                                           {"cleanOutputs": [str(integration)]}], before)
    save_report(name, report)


def shared_checks(matrix, scala, bundle):
    row = selected(matrix, "shared", scala)
    modules = row["modules"]
    name = ("shared-native-" if bundle else "shared-source-") + scala
    projects = ["core"] + (["native-runtime"] if "native-runtime" in modules else []) if bundle else modules
    before = None if bundle else source_hashes()
    reset_reports(projects)
    commands = list(MODULE_CLEAN)
    if bundle:
        commands.append("core/Test/testOnly " + " ".join(CORE_SUITES))
        if "native-runtime" in projects:
            commands.append("native-runtime/Test/testOnly " + RUNTIME_SUITE)
    else:
        for project in modules:
            commands.extend((project + "/checkReadBuild", project + "/Test/test"))
    log = run_sbt(name, commands, bundle=bundle,
                  environment=signal_environment() if bundle else None,
                  line="3.5" if scala.startswith("2.12.") else "4.0", scala=scala)
    results = {}
    for project in projects:
        required = CORE_SUITES if bundle and project == "core" else (
            (RUNTIME_SUITE,) if project == "native-runtime" else ())
        results[project] = check_reports(report_directory(project), required, allow_skipped=not bundle)
    counts = check_execution(log, list(results.items()), allow_skipped=not bundle)
    report = {"selection": row, "native": bool(bundle), "modules": {
        project: {"xmlSuites": xml_evidence(reports), "execution": counts[project]}
        for project, reports in results.items()}}
    if not bundle:
        report["clean"] = clean_contract(name, projects, [build_record(project, row["scalaBinary"])
                                                          for project in projects], before)
    save_report(name, report)


def integration_checks(bundle):
    if not bundle:
        raise ValueError("Real storage reads require --native-bundle")
    for name in ("MILVUS_READ_IT_OPTIONS", "MILVUS_READ_IT_OPTIONS_B"):
        if not Path(os.environ.get(name, "")).is_file():
            raise ValueError("Missing input identity options: " + name)
    reset_reports(("integration",))
    log = run_sbt("object-storage", ["integration/Test/testOnly " + INTEGRATION_SUITE],
                  bundle=bundle, environment=signal_environment())
    reports = check_reports(report_directory("integration"), (INTEGRATION_SUITE,))
    counts = check_execution(log, [("integration", reports)])["integration"]
    save_report("object-storage", {"xmlSuites": xml_evidence(reports), "execution": counts})


def source_hashes():
    names = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                                    cwd=ROOT).decode().split("\0")
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in names if name and (ROOT / name).is_file()
            and Path(name).suffix in {".scala", ".java", ".sbt", ".g4", ".proto"}}


def assert_clean_outputs(records):
    remaining = [path for record in records for path in record["cleanOutputs"] if Path(path).exists()]
    if remaining:
        raise ValueError("Clean retained generated products: " + ", ".join(remaining))


def build_record(project, scala_binary):
    return json.loads((ROOT / "target/read-build" / (project + "-" + scala_binary + ".json")).read_text())


def preserve_reports(name, projects):
    """Copy test reports under the root target, which no module clean touches."""
    for project in projects:
        source = report_directory(project)
        if source.is_dir():
            shutil.copytree(source, ROOT / "target/read-ci/test-reports" / name / project,
                            dirs_exist_ok=True)


def clean_contract(name, projects, records, before):
    """Clean from the default selection must remove this combination's products.

    Each source matrix job builds one Spark/Scala combination; switching
    combinations relies on the default build's clean removing what another
    selection produced, and neither cleaning nor cross compiling may touch a
    source file.
    """
    preserve_reports(name, projects)
    run_sbt(name + "-clean", MODULE_CLEAN)
    assert_clean_outputs(records)
    if source_hashes() != before:
        raise ValueError("Clean or cross compilation changed source files")
    return {"records": records, "result": "passed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("matrix", "shared", "spark", "integration"))
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--scala")
    parser.add_argument("--line")
    parser.add_argument("--native-bundle", type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    matrix = read_matrix(args.matrix)
    if args.action == "matrix":
        if not args.github_output:
            raise ValueError("matrix requires --github-output")
        with args.github_output.open("a") as output:
            for key in ("shared", "spark", "release"):
                output.write(key + "=" + json.dumps({"include": matrix[key]}, separators=(",", ":")) + "\n")
    elif args.action == "shared":
        shared_checks(matrix, args.scala, args.native_bundle)
    elif args.action == "spark":
        spark_checks(matrix, args.line, args.scala, args.native_bundle)
    else:
        integration_checks(args.native_bundle)


if __name__ == "__main__":
    main()
