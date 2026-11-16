# Reversible Central tests

Use this procedure when the task authorizes testing Central credentials and
delivery without publishing a public version, or cleaning up that test's
interrupted deployment. It creates a private deployment and removes it; it is
not a read-only credentials check.

## Run the test

The reserved tag form `v3.x.y-central-test.<digits>` selects the mutually
exclusive test path in `.github/workflows/release.yml`. For example,
`v3.0.1-central-test.2026111601` is an example test tag, not a public prerelease;
choose an unused test version. Its required branch is derived from the version:
a `v3.0.x-central-test.<digits>` tag requires successful full push CI on `3.0`
for the exact tagged commit, with native artifacts from that same branch,
commit and CI run. Main, PR and manual-dispatch CI runs do not qualify. It has
the same candidate acceptance, protected environment and signing requirements
as a real release. Follow the [release-source checks](../SKILL.md#publish-from-a-protected-tag)
before an authorized tag push; do not push a tag merely to edit documentation
or inspect existing release state.

The workflow uploads a signed `USER_MANAGED` deployment and waits for
`VALIDATED`. It downloads every candidate file through that deployment's
authenticated endpoint, verifies the bytes, then deletes the deployment.
It never calls the promotion endpoint. The Spark 4.0 Linux x86_64 universal
and platform-specific consumers subsequently use the downloaded repository
with fresh caches; no public Maven version is created.

For an authorized local equivalent, first apply the same branch and successful
push-CI requirements, then verify and sign the candidate as described in the
main skill. The low-level `test-publish` command does not enforce branch
provenance itself. Before uploading, prepare a Linux x86_64 host with
Python 3.12, `binutils` (for `readelf`), sbt, and `JAVA_HOME` pointing to JDK 21 containing
`lib/libjsig.so`. The consumer launcher configures signal-library preloading;
it rejects a different host architecture. With Portal credentials securely
supplied, run:

```bash
python3 scripts/release.py test-publish --bundle target/central.zip \
  --candidate target/release-candidate \
  --output target/central-test-candidate \
  --report target/central-test-report.json
consumer_scala=$(python3 scripts/ci/candidate_scala.py \
  --candidate target/central-test-candidate --line 4.0 --scala-binary 2.13)
python3 scripts/ci/consumer_smoke.py --candidate target/central-test-candidate \
  --line 4.0 --scala "$consumer_scala" --platform linux-x86_64 \
  --output target/central-test-consumer
```

The output directory and report must not already exist; do not overwrite a
report identifying an unresolved deployment. The report records the source SHA,
version, bundle and manifest digests, deployment ID, verified download and
cleanup outcome. Preserve it on failure as well as success.

## Recover interrupted cleanup

`test-publish` attempts deletion in its `finally` block. The workflow retries
that idempotent deletion once at the step boundary and preserves the report.
When a runner interruption leaves a deployment, inspect the report and current
Central state. For the test's recorded deployment in `VALIDATED` or `FAILED`,
the authorized test's cleanup can use:

```bash
python3 scripts/release.py drop --deployment-id "$deployment_id" \
  --report target/central-test-report.json
```

Set `deployment_id` to the report's exact deployment ID. The script rejects a
report belonging to a different ID and treats an already absent deployment as
successful cleanup. If the report is unavailable, recover and verify ownership
of the exact ID from that test run before omitting `--report`. Never delete
deployments by a broad name/version search or retry a new upload to repair
cleanup. If deletion still fails, preserve the error and ID and stop mutation;
resolve the cause before retrying. A `PUBLISHING` or `PUBLISHED` state is outside
this test path and must be investigated as a publication event.
