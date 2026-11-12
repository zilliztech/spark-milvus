---
name: spark-milvus-release
description: Prepare, validate, publish or diagnose Spark-Milvus release candidates and Maven Central releases, and render their GitHub release notes. Use for maintainer release operations, not ordinary dependency installation.
---

# Release Spark-Milvus

Keep the operation within the user's requested scope: candidate validation,
reversible Central testing, publication, or release-note maintenance. Reuse
authorization already given in the conversation. Preparing a candidate or
editing this skill does not authorize pushing a release tag or publishing it.

Use the current repository contracts:

- [CI](../../../.github/workflows/ci.yml) and
  [artifact builds](../../../.github/workflows/build-artifacts.yml) own candidate
  production; [consumer checks](../../../.github/workflows/integration.yml)
  own acceptance.
- [The release workflow](../../../.github/workflows/release.yml) owns protected
  publication; [scripts/release.py](../../../scripts/release.py) owns planning,
  verification, signing and Central operations.
- [Release notes](../../../docs/release-notes/3.0.md) own user-facing Maven
  coordinates, installation examples and supported platforms. Keep that material
  there and in the corresponding GitHub release announcement.
- For source changes or commits, use the repository's
  [contributing checks](../../../docs/contributing.md#formatting).

Run the commands below from the repository root, with `python3` using Python
3.12 as in the release workflow. Python 3.10 lacks the exception-reporting API
used when Central testing and its cleanup both fail. Read the applicable
workflow before acting; workflow files cannot configure GitHub protection
rules, environment reviewers or secrets.

## Produce and accept a candidate

Use Java 21 and initialize `milvus-proto` and `milvus-storage`. The sbt
`exportBuildMatrix` task is authoritative for the source profiles, native
platforms and deployment inputs; `scripts/release.py plan` derives the inventory.

```bash
git submodule update --init --recursive milvus-proto milvus-storage
sbt 'exportBuildMatrix target/build-matrix.json'
python3 scripts/release.py plan --version "$release_version" \
  --matrix target/build-matrix.json --output target/release-plan.json
python3 scripts/release.py check-unused --plan target/release-plan.json
```

Set `release_version` to the version being validated before running these
commands. CI candidates use `3.0.0-rc.<run-id>.<producing-attempt>`; dispatch CI
without a version override. Tagged releases use the pushed tag without `v`.
The planner accepts final versions and numeric `alpha`, `beta` or `rc`
qualifiers, plus the reserved Central-test form described below. Only a Central
404 establishes that a coordinate is unused; authentication or network failure
does not. Never reuse a published version for changed artifacts.

Produce replacement CI candidates with a new dispatch or an all-jobs rerun.
Consumers must use the artifact ID and digest from the producing attempt;
do not mix artifacts from a failed-jobs rerun with another attempt's candidate.
All modules in a candidate use one version, and shared coordinates exported by
multiple Spark profiles must have identical POMs and bytes.

For local reproduction, follow `build-artifacts.yml`: build native bundles on
their declared architectures with `native/build/build.py`, package them with
`scripts/package-native.py`, and audit with `scripts/check-native-abi.py`.
The toolchain and lock under `native/build/` preserve the Linux Ubuntu 22.04 ABI
baseline. Reused bundles require the same provenance, checksums and ABI audits.
Public format-2 native bundles contain only `storage`; supply one complete
platform bundle, never overlapping platform JARs.

Export JVM and thin artifacts with the selected Spark/Scala profile and
`-Drelease.version=<version>` using `spark/releaseJvmArtifacts` and
`spark/releaseThinArtifacts`. Restart sbt and clean module outputs between
profiles. Thin exports include
`target/release-thin/<spark-line>/<scala-binary>/<product>/thin-artifacts.json`.
`project/public-packages.txt` defines class/source ownership; update package
responsibilities before changing that contract. Stage collected native, JVM and
thin inputs with `scripts/release.py stage --help`, then verify:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
```

Verification checks the complete inventory, public package boundaries, POMs,
native components, source identity, shared coordinates and file bytes. The root
publishes nothing. Publish the candidate bytes that passed acceptance, without
rebuilding them in the publication job.

Ordinary PR checks do not satisfy release acceptance. Full CI covers all three
native platforms, real V3 reads, Arrow/Connect, and column replacement on every
Spark/Scala combination, both locally and on isolated object storage. Required
runtime tests must run without native/fixture cancellations. Consumers resolve
artifacts in isolated projects with independent executors and must pass all nine
checks: executor isolation, fixed V3 rows, projection/deletes, empty results,
repeated actions, physical row identities, replacement receipt, committed-revision
readback, and an unchanged source snapshot. Readback checks fixed values,
including NULLs, unchanged vectors and deleted rows. Its separate snapshot pins
the returned revisions without registering them with Milvus. Linux assembly
acceptance also runs Python and Java reads on the host Spark runtime.

## Publish from a protected tag

Release runs only upstream. Before an authorized tag push, confirm a successful
**push** run of `.github/workflows/ci.yml` for the exact source commit on `main`
or a numeric `<major>.<minor>` release branch. That run must have one unexpired,
nonempty `native-<platform>` artifact for each platform in
`scripts/release-matrix.json`. A successful PR or manual-dispatch run does not
satisfy this source gate. Recover missing branch CI or artifacts before tagging.

The normal tag form is `v3.x.y`, optionally with the planner's numeric
alpha/beta/rc qualifier. Protect creation, replacement and deletion of `v3.*`
tags and point them at reviewed commits. Never move an existing release tag to
repair a failed run. The release workflow reruns full validation for the tagged
version, using native artifacts from the trusted source run, and checks that the
candidate version and source SHA match the tag.

The `maven-central` GitHub environment must have required reviewers, a deployment
policy allowing `v3.*` tags, and the following **environment** secrets. Retain
protection policies still needed by older release lines.

| Environment secret | Step environment variable | Value |
|---|---|---|
| `PGP_SECRET` | `GPG_PRIVATE_KEY` | ASCII-armored private signing key or its base64 encoding |
| `PGP_PASSPHRASE` | `GPG_PASSPHRASE` | Signing-key passphrase |
| `SONATYPE_USERNAME` | `CENTRAL_USERNAME` | Central Portal user-token username |
| `SONATYPE_PASSWORD` | `CENTRAL_TOKEN` | Central Portal user-token password |

Legacy Sonatype credentials do not prove Central Portal access. The account must
control `com.zilliz`, and Central must be able to retrieve the signing public
key. The release script requires exactly one primary secret key, with fingerprint
`F0004DED15D9C011F344D6A8526AB23093F3D44A`, and selects it explicitly for each
signature. If a credential is supplied as Markdown inline code, remove only the
formatting backticks; preserve the credential's actual characters and never log
or commit it.

The workflow verifies the candidate before accessing credentials. Signing uses
a temporary GPG home removed on exit; the separate Central step receives only
Portal credentials. It uploads a `USER_MANAGED` deployment, waits for
`VALIDATED`, promotes that deployment and waits for `PUBLISHED`. It does not
rebuild the candidate. Equivalent commands, with the expected key already
imported into an isolated GPG home and credentials securely supplied, are:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
python3 scripts/release.py bundle --candidate target/release-candidate --output target/central.zip
python3 scripts/release.py publish --bundle target/central.zip
```

For a reversible credential/delivery test or interrupted-test cleanup, read
[Central test operations](references/central-test.md). A real publication
failure is different: record the deployment ID and inspect its state before
taking another mutating step. Do not blindly rerun `publish` or the release
workflow; `publish` uploads a new deployment and may already have promoted the
first one. Diagnose unknown or partially completed state and report the recovery
needed without changing tags or deleting unrelated deployments.

## Verify delivery and render the announcement

After publication, download and verify every manifest-listed Central file, then
consume universal and platform-specific entries with fresh caches on Ubuntu
22.04 x86_64 / Spark 4.0. The full consumer matrix runs before publication; this
last check verifies the public delivery path. For the local commands below,
use a matching Linux x86_64 host, sbt, and `JAVA_HOME` pointing to JDK 21 with
`lib/libjsig.so`; the consumer launcher configures signal-library preloading.

```bash
python3 scripts/release.py download-public --candidate target/release-candidate \
  --output target/public-candidate
python3 scripts/ci/consumer_smoke.py --candidate target/public-candidate \
  --line 4.0 --platform linux-x86_64 --central --output target/public-consumer
```

The workflow does not create the GitHub release announcement. Once public
delivery succeeds, render `docs/release-notes/3.0.md` into a temporary Markdown
body, replacing every `{{VERSION}}` with the published version without `v`.
Keep Maven examples, artifact choices and platform requirements in that body;
do not replace it with a link-only announcement. Verify no placeholder remains
and all repository links resolve at the intended tag. GitHub release bodies
need absolute repository links; relative document links resolve differently
there. A documentation correction to an existing announcement can use the
approved updated template without moving its release tag.

Create or edit the announcement only within the user's authorized release-note
scope. Pass multiline Markdown using `--notes-file` or a structured API body.
For `gh release create`, use `--verify-tag` so it cannot silently create a tag;
preserve prerelease status and link to the actual source tag. Report candidate
identity, workflow/consumer results and any incomplete acceptance separately
from publication and announcement status.
