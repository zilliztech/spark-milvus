"""Release must traverse every native and consumer gate, even after PR tuning."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[2]


def workflow(name):
    # BaseLoader preserves the YAML 1.2 key "on" instead of interpreting it as
    # YAML 1.1's Boolean true. Scalar values stay strings for explicit checks.
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


class WorkflowGateTest(unittest.TestCase):
    def release_identity(self, tag):
        script = workflow("release.yml")["jobs"]["identity"]["steps"][-1]["run"]
        code = script.split("python3 - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "github-output"
            with mock.patch.dict(os.environ, {"RELEASE_TAG": tag, "GITHUB_OUTPUT": str(output)}):
                exec(compile(code, "release-identity", "exec"), {})
            return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_release_and_central_test_tags_are_disjoint(self):
        self.assertEqual(self.release_identity("v2.1.2"), {
            "version": "2.1.2", "mode": "publish"})
        self.assertEqual(self.release_identity("v2.1.2-rc.3"), {
            "version": "2.1.2-rc.3", "mode": "publish"})
        self.assertEqual(self.release_identity("v2.1.2-central-test.7"), {
            "version": "2.1.2-central-test.7", "mode": "test"})
        for tag in ("v2.1.2-central-test", "v2.1.2-central-test.preview",
                    "v2.1.2-central-test7", "v2.1.2-central-testing.1",
                    "v2.1.2-CENTRAL-TEST.7", "v2.1.2-central.test.7",
                    "v2.1.2-rc.central-test.7"):
            with self.subTest(tag=tag), self.assertRaises(SystemExit):
                self.release_identity(tag)

    def test_release_requires_full_ci_and_its_candidate(self):
        release = workflow("release.yml")
        self.assertEqual(release["on"]["push"]["tags"], ["v2.*"])
        identity = release["jobs"]["identity"]
        self.assertEqual(set(identity["outputs"]), {"version", "mode"})
        identity_script = identity["steps"][-1]["run"]
        self.assertIn("-central-test\\.\\d+", identity_script)
        self.assertLess(identity_script.index("if central_test"), identity_script.index("elif release"))
        self.assertIn("reserved_test_name", identity_script)
        self.assertIn("re.IGNORECASE", identity_script)
        self.assertIn("version, mode", identity_script)
        validate = release["jobs"]["validate"]
        self.assertEqual(validate["uses"], "./.github/workflows/ci.yml")
        self.assertEqual(validate["with"]["full_validation"], "true")
        self.assertEqual(validate["with"]["version"], "${{ needs.identity.outputs.version }}")
        publish = release["jobs"]["publish"]
        self.assertIn("validate", publish["needs"])
        self.assertEqual(publish["if"], "needs.identity.outputs.mode == 'publish'")
        self.assertEqual(publish["environment"], "maven-central")
        steps = publish["steps"]
        guard = next(i for i, step in enumerate(steps) if step.get("name") == "Require the fully validated candidate")
        download = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith("actions/download-artifact@"))
        self.assertLess(guard, download)
        self.assertEqual(steps[guard]["run"], 'test -n "$CANDIDATE_ARTIFACT"')
        identity_check = next(step for step in steps
                              if step.get("name") == "Verify the tested candidate before accessing credentials")
        self.assertEqual(identity_check["env"]["RELEASE_VERSION"], "${{ needs.identity.outputs.version }}")
        self.assertIn("manifest['plan']['sourceSha']", identity_check["run"])
        self.assertIn("manifest['version']", identity_check["run"])
        signing = next(step for step in steps if step.get("name") == "Sign the tested files")
        self.assertEqual(signing["env"], {
            "GPG_PRIVATE_KEY": "${{ secrets.PGP_SECRET }}",
            "GPG_PASSPHRASE": "${{ secrets.PGP_PASSPHRASE }}",
        })
        publication = next(step for step in steps
                           if step.get("name") == "Validate and publish the signed files")
        self.assertEqual(publication["env"], {
            "CENTRAL_USERNAME": "${{ secrets.SONATYPE_USERNAME }}",
            "CENTRAL_TOKEN": "${{ secrets.SONATYPE_PASSWORD }}",
        })
        self.assertNotIn("CENTRAL_USERNAME", signing["env"])
        self.assertNotIn("GPG_PRIVATE_KEY", publication["env"])

    def test_central_test_validates_downloads_and_drops_without_publishing(self):
        release = workflow("release.yml")
        publish = release["jobs"]["publish"]
        central = release["jobs"]["central-test"]
        self.assertEqual(central["if"], "needs.identity.outputs.mode == 'test'")
        self.assertEqual(central["environment"], "maven-central")
        self.assertEqual(publish["concurrency"], central["concurrency"])
        self.assertEqual(central["concurrency"], {
            "group": "maven-central-${{ github.repository }}",
            "cancel-in-progress": "false",
        })

        steps = central["steps"]
        identity_check = next(step for step in steps
                              if step.get("name") == "Verify the tested candidate before accessing credentials")
        self.assertIn("manifest['plan']['sourceSha']", identity_check["run"])
        self.assertIn("manifest['version']", identity_check["run"])
        test = next(step for step in steps if step.get("id") == "central_test")
        signing = next(step for step in steps if step.get("name") == "Sign the tested files")
        self.assertIn("release.py bundle", signing["run"])
        self.assertIn("release.py test-publish", test["run"])
        self.assertIn('--github-output "$GITHUB_OUTPUT"', test["run"])
        self.assertNotIn("release.py publish --bundle", test["run"])
        self.assertEqual(signing["env"], {
            "GPG_PRIVATE_KEY": "${{ secrets.PGP_SECRET }}",
            "GPG_PASSPHRASE": "${{ secrets.PGP_PASSPHRASE }}",
        })
        self.assertEqual(test["env"], {
            "CENTRAL_USERNAME": "${{ secrets.SONATYPE_USERNAME }}",
            "CENTRAL_TOKEN": "${{ secrets.SONATYPE_PASSWORD }}",
        })
        cleanup = next(step for step in steps
                       if step.get("name") == "Retry Central deployment cleanup")
        self.assertEqual(cleanup["if"],
                         "always() && steps.central_test.outputs.deployment_id != ''")
        self.assertIn("release.py drop", cleanup["run"])
        self.assertEqual(cleanup["env"]["DEPLOYMENT_ID"],
                         "${{ steps.central_test.outputs.deployment_id }}")
        self.assertIn("--report central-test-report.json", cleanup["run"])
        candidate = next(step for step in steps
                         if step.get("name") == "Upload the authenticated Central candidate")
        self.assertGreater(steps.index(candidate), steps.index(test))
        self.assertEqual(candidate["with"]["path"], "central-candidate/")
        self.assertEqual(candidate["with"]["if-no-files-found"], "error")
        report = next(step for step in steps
                      if step.get("name") == "Preserve the Central test report")
        self.assertEqual(report["if"], "always()")
        self.assertEqual(report["with"]["path"], "central-test-report.json")
        self.assertEqual(report["with"]["if-no-files-found"], "warn")

        consumer = release["jobs"]["central-test-consumer"]
        self.assertEqual(set(consumer["needs"]), {"identity", "validate", "central-test"})
        self.assertEqual(consumer["if"], "needs.identity.outputs.mode == 'test'")
        smoke = next(step for step in consumer["steps"]
                     if step.get("name") == "Consume the authenticated Central files with fresh caches")
        verification = next(step for step in consumer["steps"]
                            if step.get("name") == "Verify the authenticated Central candidate")
        self.assertIn("--candidate central-test-input", verification["run"])
        self.assertNotIn("central-test-input/central-candidate", verification["run"])
        self.assertIn("--candidate central-test-input", smoke["run"])
        self.assertNotIn("central-test-input/central-candidate", smoke["run"])
        self.assertNotIn("--central", smoke["run"])
        public = release["jobs"]["public-consumer"]
        self.assertEqual(public["if"], "needs.identity.outputs.mode == 'publish'")

    def test_direct_pr_omits_only_native_build_and_consumer(self):
        ci = workflow("ci.yml")
        self.assertEqual(ci["on"]["workflow_call"]["inputs"]["full_validation"], {
            "type": "boolean", "default": "true",
        })
        self.assertIn("workflow_dispatch", ci["on"])
        condition = "github.event_name != 'pull_request' || inputs.full_validation == true"
        self.assertEqual(ci["jobs"]["artifacts"]["with"]["build_native"], "${{ " + condition + " }}")
        self.assertEqual(ci["jobs"]["consumer"]["if"], condition)
        self.assertEqual(ci["jobs"]["consumer"]["needs"], "artifacts")
        for job in ("artifacts", "build-and-test", "native-build-driver", "integration-helm"):
            self.assertNotIn("if", ci["jobs"][job])

    def test_jvm_exports_run_without_native_and_candidate_requires_both(self):
        build = workflow("build-artifacts.yml")
        self.assertEqual(build["on"]["workflow_call"]["inputs"]["build_native"], {
            "type": "boolean", "default": "true",
        })
        self.assertNotIn("if", build["jobs"]["jvm"])
        for name in ("native", "native-cpu", "candidate"):
            self.assertEqual(build["jobs"][name]["if"], "inputs.build_native")
        self.assertEqual(set(build["jobs"]["candidate"]["needs"]), {"native", "native-cpu", "jvm"})
        self.assertNotIn("secrets", build["on"]["workflow_call"])
        self.assertNotIn("environment", build["jobs"]["native"])
        self.assertNotIn("environment", build["jobs"]["jvm"])

    def test_native_cpu_gate_cannot_be_bypassed_by_a_bundle_cache_hit(self):
        build = workflow("build-artifacts.yml")
        cpu = build["jobs"]["native-cpu"]
        self.assertEqual(cpu["needs"], "native")
        self.assertEqual(cpu["runs-on"], "ubuntu-24.04")
        self.assertEqual(cpu["if"], "inputs.build_native")
        self.assertNotIn("continue-on-error", cpu)
        steps = cpu["steps"]
        download = next(step for step in steps if step.get("uses", "").startswith("actions/download-artifact@"))
        self.assertEqual(download["with"]["name"], "native-linux-x86_64")
        check = next(step for step in steps if "scripts/check-native-cpu.py" in step.get("run", ""))
        self.assertNotIn("if", check)
        self.assertNotIn("continue-on-error", check)
        self.assertIn("--bundle native-input/milvus-native-linux-x86_64.jar", check["run"])
        self.assertLess(steps.index(download), steps.index(check))
        upload = next(step for step in steps if step.get("uses", "").startswith("actions/upload-artifact@"))
        self.assertEqual(upload["if"], "always()")
        self.assertFalse(upload["with"]["name"].startswith("native-linux-"))

    def test_native_cache_hit_still_requires_current_audit(self):
        native = workflow("build-artifacts.yml")["jobs"]["native"]
        steps = native["steps"]
        by_name = {step.get("name"): step for step in steps}
        prepare = by_name["Prepare native toolchain"]
        fingerprint = by_name["Fingerprint native inputs and the prepared toolchain"]
        bundle = by_name["Restore the exact native bundle"]
        dependencies = by_name["Restore compatible native dependencies"]
        build = by_name["Build both native engines on a bundle cache miss"]
        audit = by_name["Audit the native ABI on every build and cache hit"]
        trim = by_name["Trim dependency caches before saving"]
        self.assertLess(steps.index(prepare), steps.index(fingerprint))
        self.assertLess(steps.index(fingerprint), steps.index(bundle))
        self.assertLess(steps.index(bundle), steps.index(dependencies))
        self.assertNotIn("restore-keys", bundle["with"])
        self.assertEqual(bundle["with"]["key"], "${{ steps.fingerprint.outputs.key }}")
        self.assertEqual(len(bundle["with"]["path"].splitlines()), 2)
        self.assertNotIn("abi.json", bundle["with"]["path"])
        self.assertEqual(build["if"], "steps.bundle-cache.outputs.cache-hit != 'true'")
        self.assertEqual(dependencies["if"], build["if"])
        self.assertEqual(dependencies["with"]["restore-keys"], "${{ steps.fingerprint.outputs.dependency_key_prefix }}")
        self.assertNotIn("if", audit)
        self.assertIn("scripts/check-native-abi.py", audit["run"])
        self.assertLess(steps.index(build), steps.index(audit))
        self.assertEqual(trim["if"], build["if"])
        self.assertLess(steps.index(audit), steps.index(trim))
        self.assertIn("conan cache clean '*' --build --source --download --temp", trim["run"])
        self.assertIn("ccache --cleanup", trim["run"])
        self.assertIn('if [[ -d "$cache_dir" ]]', trim["run"])
        locations = by_name["Initialize cache locations"]
        self.assertLess(steps.index(locations), steps.index(prepare))
        for variable in ("CONAN_HOME", "CCACHE_DIR", "CARGO_HOME"):
            self.assertIn(variable + "=", locations["run"])
        self.assertIn('"$GITHUB_ENV"', locations["run"])
        self.assertIn("CCACHE_MAXSIZE=2G", locations["run"])
        self.assertEqual(dependencies["with"]["path"].splitlines(), [
            "${{ env.CONAN_HOME }}/p", "${{ env.CCACHE_DIR }}",
            "${{ env.CARGO_HOME }}/registry", "${{ env.CARGO_HOME }}/git",
        ])


if __name__ == "__main__":
    unittest.main()
