"""Release must traverse every native and consumer gate, even after PR tuning."""

from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]


def workflow(name):
    # BaseLoader preserves the YAML 1.2 key "on" instead of interpreting it as
    # YAML 1.1's Boolean true. Scalar values stay strings for explicit checks.
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


class WorkflowGateTest(unittest.TestCase):
    def test_release_requires_full_ci_and_its_candidate(self):
        release = workflow("release.yml")
        validate = release["jobs"]["validate"]
        self.assertEqual(validate["uses"], "./.github/workflows/ci.yml")
        self.assertEqual(validate["with"]["full_validation"], "true")
        publish = release["jobs"]["publish"]
        self.assertIn("validate", publish["needs"])
        self.assertEqual(publish["environment"], "maven-central")
        self.assertNotIn("if", publish)  # no always()/failure bypass of its needs
        steps = publish["steps"]
        guard = next(i for i, step in enumerate(steps) if step.get("name") == "Require the fully validated candidate")
        download = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith("actions/download-artifact@"))
        self.assertLess(guard, download)
        self.assertEqual(steps[guard]["run"], 'test -n "$CANDIDATE_ARTIFACT"')
        signing = next(step for step in steps if step.get("name") == "Sign, validate and publish the tested files")
        self.assertEqual(signing["env"], {
            "GPG_PRIVATE_KEY": "${{ secrets.PGP_SECRET }}",
            "GPG_PASSPHRASE": "${{ secrets.PGP_PASSPHRASE }}",
            "CENTRAL_USERNAME": "${{ secrets.SONATYPE_USERNAME }}",
            "CENTRAL_TOKEN": "${{ secrets.SONATYPE_PASSWORD }}",
        })

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
        for name in ("native", "candidate"):
            self.assertEqual(build["jobs"][name]["if"], "inputs.build_native")
        self.assertEqual(set(build["jobs"]["candidate"]["needs"]), {"native", "jvm"})
        self.assertNotIn("secrets", build["on"]["workflow_call"])
        self.assertNotIn("environment", build["jobs"]["native"])
        self.assertNotIn("environment", build["jobs"]["jvm"])

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
