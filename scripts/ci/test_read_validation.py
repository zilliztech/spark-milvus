"""Fail closed when matrix jobs execute no tests or retain generated products."""

import copy
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

import read_validation as validation


class ReadReportTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def report(self, name="reader", cases=(None,), **attributes):
        suite = ET.Element("testsuite", name=name, tests=str(len(cases)),
                           failures="0", errors="0", skipped="0")
        suite.attrib.update({key: str(value) for key, value in attributes.items()})
        for index, status in enumerate(cases):
            case = ET.SubElement(suite, "testcase", name=str(index))
            if status:
                ET.SubElement(case, status)
        path = self.root / (name + ".xml")
        ET.ElementTree(suite).write(path)
        return path

    def test_actual_successful_cases_are_recorded(self):
        self.report(cases=(None, None))
        self.assertEqual(validation.check_reports(self.root, ("reader",)),
                         {"reader": {"tests": 2, "skipped": 0}})

    def test_missing_empty_and_all_canceled_modules_fail(self):
        for cases in (None, (), ("skipped",)):
            with self.subTest(cases=cases):
                for path in self.root.glob("*.xml"):
                    path.unlink()
                if cases is not None:
                    self.report(cases=cases)
                with self.assertRaises(ValueError):
                    validation.check_reports(self.root, (), allow_skipped=True)

    def test_required_suite_cannot_be_replaced_by_an_unrelated_success(self):
        self.report("unrelated")
        self.report("reader", cases=())
        with self.assertRaisesRegex(ValueError, "Required suite"):
            validation.check_reports(self.root, ("reader",))

    def test_failures_errors_and_cancellations_fail_by_case_and_counter(self):
        for status in ("failure", "error", "skipped"):
            with self.subTest(status=status):
                self.report(cases=(status,))
                with self.assertRaisesRegex(ValueError, "Failed or skipped"):
                    validation.check_reports(self.root, ("reader",))
        for counter in ("failures", "errors", "skipped"):
            with self.subTest(counter=counter):
                self.report(**{counter: 1})
                with self.assertRaisesRegex(ValueError, "Failed or skipped"):
                    validation.check_reports(self.root, ("reader",))

    def test_ordinary_native_absence_is_counted_without_hiding_real_failures(self):
        self.report(cases=(None, "skipped"), skipped=1)
        reports = validation.check_reports(self.root, (), allow_skipped=True)
        self.assertEqual(reports["reader"], {"tests": 2, "skipped": 1})
        self.report(cases=("failure", "skipped"), skipped=1)
        with self.assertRaises(ValueError):
            validation.check_reports(self.root, (), allow_skipped=True)

    def test_corrupt_count_and_duplicate_suite_fail(self):
        path = self.report(tests=2)
        with self.assertRaisesRegex(ValueError, "count differs"):
            validation.check_reports(self.root, ("reader",))
        self.report()
        (self.root / "duplicate.xml").write_bytes(path.read_bytes())
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            validation.check_reports(self.root, ("reader",))

    def test_junit_testsuites_container_is_supported(self):
        path = self.report()
        suite = ET.parse(path).getroot()
        container = ET.Element("testsuites")
        container.append(suite)
        ET.ElementTree(container).write(path)
        self.assertEqual(validation.check_reports(self.root, ("reader",))["reader"]["tests"], 1)

    def test_retained_generated_directory_or_jar_fails_clean(self):
        generated = self.root / "src_managed"
        jar = self.root / "reader-sources.jar"
        records = [{"cleanOutputs": [str(generated), str(jar)]}]
        validation.assert_clean_outputs(records)
        generated.mkdir()
        with self.assertRaisesRegex(ValueError, "Clean retained"):
            validation.assert_clean_outputs(records)
        generated.rmdir()
        jar.write_bytes(b"jar")
        with self.assertRaisesRegex(ValueError, "Clean retained"):
            validation.assert_clean_outputs(records)


class ExecutionSummaryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / "run.log"
        self.reports = {"reader": {"tests": 2, "skipped": 0}}

    def scala_log(self, succeeded=2, canceled=0, ignored=0, pending=0, failed=0, aborted=0):
        self.log.write_text(
            f"[info] Suites: completed 1, aborted {aborted}\n"
            f"[info] Tests: succeeded {succeeded}, failed {failed}, canceled {canceled}, "
            f"ignored {ignored}, pending {pending}\n")

    def test_scala_cancellation_hidden_by_sbt_xml_is_rejected(self):
        self.scala_log(succeeded=1, canceled=1)
        with self.assertRaisesRegex(ValueError, "Canceled, ignored"):
            validation.check_execution(self.log, [("core", self.reports)])
        counts = validation.check_execution(self.log, [("core", self.reports)], allow_skipped=True)
        self.assertEqual(counts["core"]["succeeded"], 1)
        self.assertEqual(counts["core"]["canceled"], 1)
        self.assertEqual(validation.xml_evidence(self.reports)["reader"]["reportedSkipped"], 0)

    def test_ignored_pending_aborted_and_failed_are_never_native_success(self):
        for status in ("ignored", "pending", "failed"):
            with self.subTest(status=status):
                self.scala_log(succeeded=1, **{status: 1})
                with self.assertRaises(ValueError):
                    validation.check_execution(self.log, [("core", self.reports)])
        self.scala_log(aborted=1)
        with self.assertRaisesRegex(ValueError, "Aborted"):
            validation.check_execution(self.log, [("core", self.reports)], allow_skipped=True)

    def test_missing_summary_and_xml_coverage_mismatch_fail(self):
        self.log.write_text("[info] All tests passed.\n")
        with self.assertRaisesRegex(ValueError, "Missing or unexpected"):
            validation.check_execution(self.log, [("core", self.reports)])
        self.scala_log(succeeded=1)
        with self.assertRaisesRegex(ValueError, "differs from XML"):
            validation.check_execution(self.log, [("core", self.reports)])
        self.scala_log()
        with self.assertRaisesRegex(ValueError, "Missing or unexpected"):
            validation.check_execution(self.log, [("core", self.reports), ("compat", self.reports)])

    def test_all_canceled_module_still_fails_when_cancellation_is_allowed(self):
        self.scala_log(succeeded=0, canceled=2)
        with self.assertRaisesRegex(ValueError, "No successful"):
            validation.check_execution(self.log, [("core", self.reports)], allow_skipped=True)

    def test_junit_and_scalatest_commands_are_counted_in_execution_order(self):
        self.scala_log()
        self.log.write_text(self.log.read_text() +
                            "[info] Passed: Total 3, Failed 0, Errors 0, Passed 3\n")
        counts = validation.check_execution(self.log, [
            ("core", self.reports), ("native-runtime", {"loader": {"tests": 3, "skipped": 0}})])
        self.assertEqual(counts["core"]["succeeded"], 2)
        self.assertEqual(counts["native-runtime"]["succeeded"], 3)

    def test_junit_assumptions_and_inconsistent_totals_fail(self):
        self.log.write_text("[info] Passed: Total 2, Failed 0, Errors 0, Passed 1\n")
        with self.assertRaisesRegex(ValueError, "Canceled, ignored"):
            validation.check_execution(self.log, [("native-runtime", self.reports)])
        self.log.write_text("[info] Passed: Total 2, Failed 0, Errors 0, Passed 3\n")
        with self.assertRaisesRegex(ValueError, "Inconsistent"):
            validation.check_execution(self.log, [("native-runtime", self.reports)])

    def test_signal_environment_requires_the_selected_jdk_library(self):
        with mock.patch.dict(os.environ, {"JAVA_HOME": str(self.root)}):
            with self.assertRaisesRegex(ValueError, "libjsig"):
                validation.signal_environment()
            library = self.root / "lib/libjsig.so"
            library.parent.mkdir()
            library.touch()
            self.assertEqual(validation.signal_environment(), {"LD_PRELOAD": str(library)})
            # macOS: the shell launching sbt refuses the insertion; the forked JVMs preload it themselves.
            with mock.patch.object(validation.sys, "platform", "darwin"):
                with self.assertRaisesRegex(ValueError, "libjsig"):
                    validation.signal_environment()
                (self.root / "lib/libjsig.dylib").touch()
                self.assertEqual(validation.signal_environment(), {})


class MatrixTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "matrix.json"
        self.matrix = {
            "schemaVersion": 1,
            "shared": [{"scala": "2.13.16"}],
            "spark": [{"line": "4.0", "scala": "2.13.16", "scalaBinary": "2.13",
                       "spark": "4.0.0", "project": "spark"}],
            "release": [{"line": "4.0", "scala": "2.13.16", "compileSpark": "4.0.0"}],
        }

    def read(self, matrix):
        self.path.write_text(json.dumps(matrix))
        return validation.read_matrix(self.path)

    def test_source_and_release_relationship_is_retained(self):
        self.assertEqual(self.read(self.matrix), self.matrix)
        self.assertEqual(validation.selected(self.matrix, "spark", "2.13.16", "4.0")["project"],
                         "spark")
        with self.assertRaises(ValueError):
            validation.selected(self.matrix, "spark", "2.12.20", "4.0")

    def test_inconsistent_shared_release_and_source_coordinates_fail(self):
        broken = []
        matrix = copy.deepcopy(self.matrix)
        matrix["spark"].append(copy.deepcopy(matrix["spark"][0]))
        broken.append(matrix)
        for group, field, value in (("shared", "scala", "2.12.20"),
                                    ("spark", "scalaBinary", "2.12"),
                                    ("spark", "spark", "4.1.1"),
                                    ("release", "compileSpark", "4.0.1")):
            matrix = copy.deepcopy(self.matrix)
            matrix[group][0][field] = value
            broken.append(matrix)
        for matrix in broken:
            with self.subTest(matrix=matrix), self.assertRaises(ValueError):
                self.read(matrix)

    def test_report_locations_match_project_directories(self):
        for project in ("spark", "integration", "core"):
            self.assertEqual(validation.report_directory(project),
                             validation.ROOT / project / "target/test-reports")
        for project, directory in (("native-runtime", "native/runtime"), ("native-storage", "native/storage"),
                                   ("native-vector", "native/vector")):
            self.assertEqual(validation.report_directory(project),
                             validation.ROOT / directory / "target/test-reports")

    def source_matrix(self):
        matrix = copy.deepcopy(self.matrix)
        matrix["shared"] = [
            {"scala": "2.12.20", "scalaBinary": "2.12", "modules": ["native-storage", "core"]},
            {"scala": "2.13.16", "scalaBinary": "2.13", "modules": ["core"]},
        ]
        matrix["spark"].insert(0, {"line": "3.5", "project": "spark", "scala": "2.12.20",
                                   "scalaBinary": "2.12", "spark": "3.5.5"})
        return matrix

    def run_source_check(self, check, *, clean_removes=True, hashes=("unchanged", "unchanged")):
        """Run one source check against an sbt double that builds and cleans products."""
        root = Path(self.temporary.name)
        products = []

        def sbt(name, commands, *, bundle=None, environment=None, line="4.0", scala="2.13.16"):
            if name.endswith("-clean"):
                if clean_removes:
                    for product in products:
                        shutil.rmtree(product)
                    for directory in root.glob("**/target/test-reports"):
                        shutil.rmtree(directory)
                return ""
            binary = ".".join(scala.split(".")[:2])
            for command in commands:
                project = command.split("/")[0]
                if command.endswith("/checkReadBuild"):
                    product = root / project / "target" / ("classes-" + binary)
                    product.mkdir(parents=True)
                    products.append(product)
                    metadata = root / "target/read-build" / (project + "-" + binary + ".json")
                    metadata.parent.mkdir(parents=True, exist_ok=True)
                    metadata.write_text(json.dumps({"cleanOutputs": [str(product)]}))
                    reports = validation.report_directory(project)
                    reports.mkdir(parents=True, exist_ok=True)
                    (reports / "TEST-suite.xml").write_text("<testsuite/>")
                elif command == "integration/Test/compile":
                    product = root / "integration/target" / ("scala-" + binary) / "test-classes"
                    product.mkdir(parents=True)
                    products.append(product)
            return ""

        sources = iter({"source.scala": value} for value in hashes)
        with mock.patch.object(validation, "ROOT", root), \
                mock.patch.object(validation, "source_hashes", side_effect=lambda: next(sources)), \
                mock.patch.object(validation, "check_reports", return_value=[]), \
                mock.patch.object(validation, "xml_evidence", return_value=[]), \
                mock.patch.object(validation, "check_execution",
                                  side_effect=lambda log, items, **_: {name: {} for name, _ in items}), \
                mock.patch.object(validation, "save_report") as saved, \
                mock.patch.object(validation, "run_sbt", side_effect=sbt) as runs:
            check()
        return root, products, runs, saved

    def test_source_checks_clean_their_combination_from_the_default_selection(self):
        matrix = self.source_matrix()
        for label, check, projects in (
                ("spark-source-3.5-2.12.20",
                 lambda: validation.spark_checks(matrix, "3.5", "2.12.20", None), ["spark"]),
                ("shared-source-2.12.20",
                 lambda: validation.shared_checks(matrix, "2.12.20", None), ["native-storage", "core"])):
            with self.subTest(check=label):
                self.temporary.cleanup()
                Path(self.temporary.name).mkdir()
                root, products, runs, saved = self.run_source_check(check)
                build, clean = runs.call_args_list
                self.assertEqual((build.kwargs["line"], build.kwargs["scala"]), ("3.5", "2.12.20"))
                self.assertEqual(build.args[1][:len(validation.MODULE_CLEAN)], validation.MODULE_CLEAN)
                self.assertEqual("integration/Test/compile" in build.args[1], projects == ["spark"])
                self.assertEqual(clean.args, (label + "-clean", validation.MODULE_CLEAN))
                self.assertNotIn("line", clean.kwargs)
                self.assertNotIn("scala", clean.kwargs)
                self.assertTrue(products)
                self.assertTrue(all(not product.exists() for product in products))
                for project in projects:
                    self.assertTrue((root / "target/read-ci/test-reports" / label / project /
                                     "TEST-suite.xml").is_file())
                report = saved.call_args.args[1]
                self.assertEqual(report["clean"]["result"], "passed")
                self.assertEqual(len(report["clean"]["records"]),
                                 len(projects) + (1 if projects == ["spark"] else 0))

    def test_retained_products_and_changed_sources_fail_the_source_check(self):
        matrix = self.source_matrix()
        for options in ({"clean_removes": False}, {"hashes": ("before", "after")}):
            with self.subTest(options=options):
                self.temporary.cleanup()
                Path(self.temporary.name).mkdir()
                with self.assertRaises(ValueError):
                    self.run_source_check(lambda: validation.spark_checks(matrix, "3.5", "2.12.20", None),
                                          **options)

    def test_native_checks_leave_cleaning_to_the_source_jobs(self):
        matrix = self.source_matrix()
        bundle = Path(self.temporary.name) / "bundle.jar"
        _, _, runs, saved = self.run_source_check(
            lambda: validation.spark_checks(matrix, "3.5", "2.12.20", bundle))
        self.assertEqual(runs.call_count, 1)
        self.assertNotIn("integration/Test/compile", runs.call_args.args[1])
        self.assertNotIn("clean", saved.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
