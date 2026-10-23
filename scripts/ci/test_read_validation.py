"""Fail closed when matrix jobs execute no tests or retain generated products."""

import copy
import json
import os
from pathlib import Path
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

    def test_clean_build_reloads_selectors_and_removes_all_products(self):
        root = Path(self.temporary.name)
        matrix = copy.deepcopy(self.matrix)
        matrix["shared"] = [
            {"scala": "2.12.20", "scalaBinary": "2.12", "modules": ["core"]},
            {"scala": "2.13.16", "scalaBinary": "2.13", "modules": ["core"]},
        ]
        matrix["spark"].insert(0, {"line": "3.5", "project": "spark", "scala": "2.12.20",
                                   "scalaBinary": "2.12", "spark": "3.5.5"})
        products = []

        def sbt_outputs(name, commands, *, line="4.0", scala="2.13.16"):
            if name == "clean-restored":
                for product in products:
                    product.unlink()
                return
            for command in commands:
                if command.endswith("/checkReadBuild"):
                    project = command.split("/")[0]
                    binary = ".".join(scala.split(".")[:2])
                    product = root / (project + "-" + binary + ".jar")
                    product.touch()
                    products.append(product)
                    metadata = root / "target/read-build" / (project + "-" + binary + ".json")
                    metadata.parent.mkdir(parents=True, exist_ok=True)
                    metadata.write_text(json.dumps({"cleanOutputs": [str(product)]}))

        with mock.patch.object(validation, "ROOT", root), \
                mock.patch.object(validation, "source_hashes", return_value={"source.scala": "unchanged"}), \
                mock.patch.object(validation, "save_report") as saved, \
                mock.patch.object(validation, "run_sbt", side_effect=sbt_outputs) as sbt:
            validation.clean_checks(matrix)
        self.assertEqual(sbt.call_count, 5)
        selected = [(call.kwargs.get("line"), call.kwargs.get("scala"))
                    for call in sbt.call_args_list[:-1]]
        self.assertEqual(selected, [("3.5", "2.12.20"), ("4.0", "2.13.16"),
                                    ("3.5", "2.12.20"), ("4.0", "2.13.16")])
        self.assertEqual(sbt.call_args_list[-1].args[1], validation.MODULE_CLEAN)
        for call in sbt.call_args_list[:-1]:
            self.assertEqual(call.args[1][:len(validation.MODULE_CLEAN)], validation.MODULE_CLEAN)
        self.assertTrue(all(not product.exists() for product in products))
        self.assertEqual(saved.call_args.args[1]["result"], "passed")


if __name__ == "__main__":
    unittest.main()
