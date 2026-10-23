"""Release must traverse every native and consumer gate, even after PR tuning."""

import json
import os
from pathlib import Path
import subprocess
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
    def test_source_and_native_lanes_consume_the_exported_matrix(self):
        jobs = workflow("ci.yml")["jobs"]
        exporter = jobs["read-matrix"]
        self.assertEqual(set(exporter["outputs"]), {"shared", "spark"})
        self.assertTrue(any("exportBuildMatrix" in step.get("run", "")
                            for step in exporter["steps"]))
        for name, group in (("shared-libraries", "shared"), ("spark-sources", "spark"),
                            ("shared-native", "shared"), ("spark-native", "spark")):
            job = jobs[name]
            with self.subTest(job=name):
                self.assertEqual(job["strategy"]["matrix"],
                                 "${{ fromJSON(needs.read-matrix.outputs." + group + ") }}")
                check = next(step for step in job["steps"]
                             if "read_validation.py " + group in step.get("run", ""))
                self.assertIn("--matrix build-matrix.json", check["run"])
                self.assertIn("--scala '${{ matrix.scala }}'", check["run"])
                self.assertNotIn("continue-on-error", check)
                if name.endswith("native"):
                    self.assertIn("--native-bundle", check["run"])
                    self.assertEqual(job["if"],
                                     "github.event_name != 'pull_request' || inputs.full_validation == true")
                else:
                    self.assertNotIn("if", job)
        aggregate = jobs["build-and-test"]
        self.assertTrue(any(step.get("run") == "sbt test" for step in aggregate["steps"]))
        self.assertTrue(any("read_validation.py clean" in step.get("run", "")
                            for step in aggregate["steps"]))

    def test_real_storage_reads_run_with_two_identities_and_always_clean_up(self):
        steps = workflow("ci.yml")["jobs"]["spark-native"]["steps"]
        start = next(step for step in steps if "read_storage.py start" in step.get("run", ""))
        read = next(step for step in steps if "read_validation.py integration" in step.get("run", ""))
        stop = next(step for step in steps if "read_storage.py stop" in step.get("run", ""))
        self.assertEqual(start["if"], "matrix.line == '4.0'")
        self.assertEqual(read["if"], start["if"])
        self.assertEqual(set(read["env"]), {"MILVUS_READ_IT_OPTIONS", "MILVUS_READ_IT_OPTIONS_B"})
        self.assertNotEqual(*read["env"].values())
        self.assertEqual(stop["if"], "always() && matrix.line == '4.0'")
        self.assertLess(steps.index(start), steps.index(read))
        self.assertLess(steps.index(read), steps.index(stop))

    def test_helm_lints_the_service_free_read_mode(self):
        steps = workflow("ci.yml")["jobs"]["integration-helm"]["steps"]
        lint = next(step for step in steps if "helm lint" in step.get("run", ""))
        for values in ("ci-values.read.example.yaml",):
            self.assertIn("-f deploy/helm/spark-milvus-integration/" + values, lint["run"])
        self.assertNotIn("ci-values.managed", lint["run"])
        self.assertNotIn("/ci-values.example", lint["run"])

    def test_release_plan_uses_versions_export_without_an_independent_jvm_matrix(self):
        steps = workflow("build-artifacts.yml")["jobs"]["plan"]["steps"]
        export = next(step for step in steps if "exportBuildMatrix" in step.get("run", ""))
        plan = next(step for step in steps if "release.py plan" in step.get("run", ""))
        self.assertIn("--matrix build-matrix.json", plan["run"])
        self.assertLess(steps.index(export), steps.index(plan))
        self.assertNotIn("jvm", json.loads((ROOT / "scripts/release-matrix.json").read_text()))

    def release_identity(self, tag):
        steps = workflow("release.yml")["jobs"]["identity"]["steps"]
        script = next(step for step in steps if step.get("id") == "identity")["run"]
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
        self.assertEqual(release["permissions"], {"actions": "read", "contents": "read"})
        identity = release["jobs"]["identity"]
        self.assertEqual(set(identity["outputs"]), {"version", "mode", "native_source_run_id"})
        identity_script = next(step for step in identity["steps"]
                               if step.get("id") == "identity")["run"]
        self.assertIn("-central-test\\.\\d+", identity_script)
        self.assertLess(identity_script.index("if central_test"), identity_script.index("elif release"))
        self.assertIn("reserved_test_name", identity_script)
        self.assertIn("re.IGNORECASE", identity_script)
        self.assertIn("version, mode", identity_script)
        validate = release["jobs"]["validate"]
        self.assertEqual(validate["uses"], "./.github/workflows/ci.yml")
        self.assertEqual(validate["with"]["full_validation"], "true")
        self.assertEqual(validate["with"]["version"], "${{ needs.identity.outputs.version }}")
        self.assertEqual(validate["with"]["native_source_run_id"],
                         "${{ needs.identity.outputs.native_source_run_id }}")
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

    def test_release_native_source_is_exact_trusted_successful_push(self):
        release = workflow("release.yml")
        identity = release["jobs"]["identity"]
        source = next(step for step in identity["steps"] if step.get("id") == "native-source")
        self.assertEqual(source["env"], {
            "GH_TOKEN": "${{ github.token }}",
            "RELEASE_REPOSITORY": "${{ github.repository }}",
            "RELEASE_SHA": "${{ github.sha }}",
        })
        script = source["run"]
        for fragment in (
                "actions/workflows/ci.yml/runs", "branch=refactor/v2", "event=push",
                "status=success", 'head_sha="$RELEASE_SHA"',
                "run.get('path') == '.github/workflows/ci.yml'",
                "run.get('head_branch') == 'refactor/v2'",
                "run.get('head_sha') == sha", "run.get('status') == 'completed'",
                "run.get('conclusion') == 'success'", "scripts/release-matrix.json",
                "'native-' + row['platform'] for row in matrix['native']", "artifact.get('expired') is False",
                "artifact.get('workflow_run', {}).get('id') == run_id"):
            self.assertIn(fragment, script)
        self.assertNotIn("secrets.", str(source))

    def test_release_native_source_resolver_accepts_only_matching_artifacts(self):
        source = next(step for step in workflow("release.yml")["jobs"]["identity"]["steps"]
                      if step.get("id") == "native-source")
        sha = "a" * 40
        repository = "example/spark-milvus"
        run_id = 12345
        run = {
            "id": run_id, "created_at": "2026-10-16T00:00:00Z",
            "repository": {"full_name": repository},
            "head_repository": {"full_name": repository},
            "path": ".github/workflows/ci.yml", "event": "push",
            "head_branch": "refactor/v2", "head_sha": sha,
            "status": "completed", "conclusion": "success",
        }
        artifact_run = {"id": run_id, "head_branch": "refactor/v2", "head_sha": sha}
        artifacts = [{
            "name": name, "expired": False, "size_in_bytes": 1,
            "workflow_run": artifact_run,
        } for name in ("native-" + row["platform"] for row in
                       json.loads((ROOT / "scripts/release-matrix.json").read_text())["native"])]
        self.assertIn("native-darwin-aarch64", [artifact["name"] for artifact in artifacts])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs_path = root / "runs.json"
            artifacts_path = root / "artifacts.json"
            output = root / "output"
            runs_path.write_text(json.dumps({"workflow_runs": [run]}))
            artifacts_path.write_text(json.dumps({
                "total_count": len(artifacts), "artifacts": artifacts}))
            fake_gh = root / "gh"
            fake_gh.write_text(
                '#!/bin/sh\ncase "$*" in *"/artifacts"*) cat "$FAKE_ARTIFACTS";; '
                '*) cat "$FAKE_RUNS";; esac\n')
            fake_gh.chmod(0o755)
            environment = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                               GH_TOKEN="token", RELEASE_REPOSITORY=repository,
                               RELEASE_SHA=sha, RUNNER_TEMP=str(root),
                               GITHUB_OUTPUT=str(output), FAKE_RUNS=str(runs_path),
                               FAKE_ARTIFACTS=str(artifacts_path))
            result = subprocess.run(["bash", "-c", source["run"]], cwd=ROOT,
                                    env=environment, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(output.read_text(), f"run_id={run_id}\n")

            run["head_sha"] = "b" * 40
            runs_path.write_text(json.dumps({"workflow_runs": [run]}))
            output.unlink()
            result = subprocess.run(["bash", "-c", source["run"]], cwd=ROOT,
                                    env=environment, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())

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
        self.assertEqual(ci["on"]["workflow_call"]["inputs"]["native_source_run_id"], {
            "type": "string", "default": "",
        })
        self.assertEqual(ci["permissions"], {"actions": "read", "contents": "read"})
        self.assertIn("workflow_dispatch", ci["on"])
        condition = "github.event_name != 'pull_request' || inputs.full_validation == true"
        self.assertEqual(ci["jobs"]["artifacts"]["with"]["build_native"], "${{ " + condition + " }}")
        self.assertEqual(ci["jobs"]["artifacts"]["with"]["native_source_run_id"],
                         "${{ inputs.native_source_run_id }}")
        self.assertEqual(ci["jobs"]["consumer"]["if"], condition)
        self.assertEqual(ci["jobs"]["consumer"]["needs"], "artifacts")
        for job in ("artifacts", "build-and-test", "native-build-driver", "integration-helm"):
            self.assertNotIn("if", ci["jobs"][job])

    def test_jvm_exports_run_without_native_and_candidate_requires_both(self):
        build = workflow("build-artifacts.yml")
        self.assertEqual(build["on"]["workflow_call"]["inputs"]["build_native"], {
            "type": "boolean", "default": "true",
        })
        self.assertEqual(build["on"]["workflow_call"]["inputs"]["native_source_run_id"], {
            "type": "string", "default": "",
        })
        self.assertEqual(build["permissions"], {"actions": "read", "contents": "read"})
        self.assertNotIn("if", build["jobs"]["jvm"])
        for name in ("native", "native-cpu", "darwin-reads", "candidate"):
            self.assertEqual(build["jobs"][name]["if"], "inputs.build_native")
        self.assertEqual(set(build["jobs"]["candidate"]["needs"]), {"native", "native-cpu", "darwin-reads", "jvm"})
        # The macOS read suites run on the darwin bundle before any candidate is assembled.
        darwin = build["jobs"]["darwin-reads"]
        self.assertEqual(darwin["runs-on"], "macos-15")
        self.assertEqual(darwin["needs"], "native")
        downloads = [step["with"]["name"] for step in darwin["steps"]
                     if step.get("uses", "").startswith("actions/download-artifact")]
        self.assertEqual(downloads, ["native-darwin-aarch64"])
        suites = [step["run"] for step in darwin["steps"] if "read_validation.py" in step.get("run", "")]
        self.assertEqual(len(suites), 2)
        # The runner's platform binaries honor DYLD_INSERT_LIBRARIES (SIP is off);
        # a JVM's children need tools that can load the inserted libjsig.
        runs = [step.get("run", "") for step in darwin["steps"]]
        children = next(index for index, run in enumerate(runs) if "prepare_macos_jvm_children.sh" in run)
        self.assertLess(children, min(index for index, run in enumerate(runs) if "read_validation.py" in run))
        self.assertTrue((ROOT / "scripts/ci/prepare_macos_jvm_children.sh").is_file())
        consumer = workflow("integration.yml")["jobs"]["consumer"]["steps"]
        prepare = next(step for step in consumer if "prepare_macos_jvm_children.sh" in step.get("run", ""))
        self.assertEqual(prepare["if"], "runner.os == 'macOS'")
        record = next(step for step in consumer if step.get("name", "").startswith("Record the consumer"))
        self.assertIn("sw_vers", record["run"])
        self.assertTrue(all("--native-bundle native-input/milvus-native-darwin-aarch64.jar" in run for run in suites))
        self.assertTrue(any(" shared " in run for run in suites) and any(" spark " in run for run in suites))
        candidate_inputs = [step["with"]["pattern"] for step in build["jobs"]["candidate"]["steps"]
                            if step.get("uses", "").startswith("actions/download-artifact") and "pattern" in step.get("with", {})]
        self.assertIn("native-linux-*", candidate_inputs)
        self.assertIn("native-darwin-*", candidate_inputs)
        self.assertNotIn("secrets", build["on"]["workflow_call"])
        self.assertNotIn("environment", build["jobs"]["native"])
        self.assertNotIn("environment", build["jobs"]["jvm"])

    def test_thin_modules_are_exported_without_native_and_staged_with_the_same_candidate(self):
        jobs = workflow("build-artifacts.yml")["jobs"]
        steps = jobs["jvm"]["steps"]
        export = next(step for step in steps if "releaseThinArtifacts" in step.get("run", ""))
        self.assertEqual(export["if"], "matrix.line == '4.0'")
        self.assertEqual(export["env"]["RELEASE_VERSION"], "${{ inputs.version }}")
        self.assertIn('"-Drelease.version=$RELEASE_VERSION" "spark/releaseThinArtifacts"', export["run"])
        upload = next(step for step in steps if step.get("with", {}).get("name") == "thin-${{ matrix.line }}")
        self.assertEqual(upload["if"], export["if"])
        self.assertEqual(upload["with"]["path"], "target/release-thin/${{ matrix.line }}/")
        self.assertEqual(upload["with"]["if-no-files-found"], "error")
        self.assertLess(steps.index(export), steps.index(upload))
        candidate = jobs["candidate"]["steps"]
        download = next(step for step in candidate if step.get("with", {}).get("pattern") == "thin-*")
        stage = next(step for step in candidate if "release.py stage" in step.get("run", ""))
        self.assertEqual(download["with"]["path"], "release-inputs")
        self.assertLess(candidate.index(download), candidate.index(stage))
        self.assertNotIn("--thin-only", stage["run"])

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
        branch_bundle = by_name["Restore the exact-commit branch native artifact"]
        branch_verify = by_name["Verify the exact-commit branch native artifact"]
        dependencies = by_name["Restore compatible native dependencies"]
        build = by_name["Build both native engines on a bundle cache miss"]
        audit = by_name["Audit the native ABI on every build and cache hit"]
        trim = by_name["Trim dependency caches before saving"]
        self.assertLess(steps.index(prepare), steps.index(fingerprint))
        self.assertLess(steps.index(fingerprint), steps.index(bundle))
        self.assertLess(steps.index(bundle), steps.index(branch_bundle))
        self.assertLess(steps.index(branch_bundle), steps.index(branch_verify))
        self.assertLess(steps.index(branch_verify), steps.index(dependencies))
        self.assertNotIn("restore-keys", bundle["with"])
        self.assertEqual(bundle["with"]["key"], "${{ steps.fingerprint.outputs.key }}")
        self.assertEqual(len(bundle["with"]["path"].splitlines()), 2)
        self.assertNotIn("abi.json", bundle["with"]["path"])
        available = ("steps.bundle-cache.outputs.cache-hit != 'true' && "
                     "steps.branch_bundle_verify.outcome != 'success'")
        self.assertEqual(build["if"], available)
        self.assertEqual(dependencies["if"], build["if"])
        self.assertEqual(branch_bundle["if"],
                         "steps.bundle-cache.outputs.cache-hit != 'true' && inputs.native_source_run_id != ''")
        self.assertEqual(branch_bundle["uses"], "actions/download-artifact@v4")
        self.assertEqual(branch_bundle["with"], {
            "name": "native-${{ matrix.platform }}",
            "path": "target/native-${{ matrix.platform }}",
            "github-token": "${{ github.token }}",
            "repository": "${{ github.repository }}",
            "run-id": "${{ inputs.native_source_run_id }}",
        })
        self.assertEqual(branch_verify["if"], "steps.branch-bundle.outcome == 'success'")
        self.assertIn("scripts/ci/verify_native_reuse.py", branch_verify["run"])
        self.assertIn("--expected-cache-inputs", branch_verify["run"])
        self.assertEqual(dependencies["with"]["restore-keys"], "${{ steps.fingerprint.outputs.dependency_key_prefix }}")
        self.assertNotIn("if", audit)
        self.assertIn("scripts/check-native-abi.py", audit["run"])
        self.assertLess(steps.index(build), steps.index(audit))
        # The dependency cache is restored and saved explicitly, and saved even
        # after a failed or timed-out build so the next attempt continues.
        self.assertEqual(dependencies["uses"], "actions/cache/restore@v4")
        self.assertEqual(dependencies["id"], "dependency-cache")
        self.assertEqual(trim["if"], "${{ !cancelled() && steps.native_build.outcome != 'skipped' }}")
        save = by_name["Save native dependencies"]
        self.assertEqual(save["uses"], "actions/cache/save@v4")
        # Not always(): a cache saved by a cancelled run would shadow a complete one.
        self.assertEqual(save["if"], "${{ !cancelled() && steps.native_build.outcome != 'skipped' "
                                     "&& steps.dependency-cache.outputs.cache-hit != 'true' }}")
        self.assertEqual(save["with"]["key"], dependencies["with"]["key"])
        self.assertEqual(save["with"]["path"], dependencies["with"]["path"])
        self.assertLess(steps.index(trim), steps.index(save))
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
