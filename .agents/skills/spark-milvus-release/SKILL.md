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
- [Release artifacts](../../../docs/release-artifacts.md) owns current user-facing
  coordinates and platform availability. Keep published and unreleased versions
  distinct. [Release notes](../../../docs/release-notes/3.0.md) is the template
  for the next announcement; render its placeholders before publication.
- [Application development](../../../docs/application-development.md) owns
  application launch commands, including `--packages` and `--jars` usage. Link
  to that guide instead of repeating those instructions in release notes.
- For source changes or commits, use the repository's
  [contributing checks](../../../docs/contributing.md#formatting).

Run the commands below from the repository root, with `python3` using Python
3.12 as in the release workflow. Python 3.10 lacks the exception-reporting API
used when Central testing and its cleanup both fail. Read the applicable
workflow before acting; workflow files cannot configure GitHub protection
rules, environment reviewers or secrets.

## Produce and accept a candidate

The 3.0.0 inventory includes 15 coordinates added on 2026-11-16 for Spark
3.5 / Scala 2.12, Spark 3.5 / Scala 2.13 and Spark 4.1 / Scala 2.13. These
bundled JVM, universal and platform entries were built from main commit
`a64a831c2d511dd32cbeed77804f4192fc04f69c`; the Spark 3.5 bundles include the
Arrow Linux ARM64 supplement. At that publication, the original `v3.0.0`
tag and `3.0` release branch both pointed to
`50ec8905fdf3edfebcf97ab57a0b7009feb3f0c5`; neither was moved. Existing Spark
4.0/4.2, native and Thin artifacts were not replaced. See the
[published inventory](../../../docs/release-artifacts.md#published-300) and
[build provenance](../../../docs/release-artifacts.md#build-provenance).
Before preparing or verifying a candidate, establish its exact coordinate
inventory and source commit from the candidate manifest and POM SCM metadata;
do not infer the incremental packages' source from the original release tag.
Use the build and verification scripts from that candidate's source revision.

Use Java 21 and initialize `milvus-proto` and `milvus-storage`. The sbt
`exportBuildMatrix` task is authoritative for the source profiles, native
platforms and deployment inputs; `scripts/release.py plan` derives the inventory.
On Linux, install `binutils` so `readelf` is available before building the Arrow
supplement, exporting JVM artifacts, staging or verifying a candidate. These
operations repeat the supplement's ELF and ABI audit; candidate consumers also
need this tool when they verify the complete candidate.

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
does not. Never replace bytes at an already published Maven coordinate.

Produce replacement CI candidates with a new dispatch or an all-jobs rerun.
Consumers must use the artifact ID and digest from the producing attempt;
do not mix artifacts from a failed-jobs rerun with another attempt's candidate.
All artifacts in a candidate use one version. Every supported Spark and Scala
combination has its own bundled JVM, universal and platform-specific coordinates;
native platform artifacts are shared. The four production modules remain
internal build units, and this build graph no longer publishes Thin modules.
Existing Thin artifacts remain available with their original bytes.

For local reproduction, follow `build-artifacts.yml`: build native bundles on
their declared architectures with `native/build/build.py`, package them with
`scripts/package-native.py`, and audit with `scripts/check-native-abi.py`.
The toolchain and lock under `native/build/` preserve the Linux Ubuntu 22.04 ABI
baseline. Reused bundles require the same provenance, checksums and ABI audits.
Public format-2 native bundles contain only `storage`; supply one complete
platform bundle, never overlapping platform JARs.

Export JVM artifacts with the selected Spark/Scala profile and
`-Drelease.version=<version>` using `spark/releaseJvmArtifacts`. Each selection
exports its JVM implementation, sources, API documentation and build metadata to
`target/release-jvm/<spark-line>/<scala-binary>/artifact.json`; the Scala binary
version separates Spark 3.5's Scala 2.12 and 2.13 variants. Build with Java 21;
consumer validation uses Java 17 for Spark 3.5 and Java 21 for Spark 4.x.

Spark 3.5's Arrow 12 dependency omits its Linux ARM64 C Data JNI library. Before
exporting either Scala variant, build the pinned upstream library on a Linux
ARM64 runner, then supply the verified supplement to sbt:

```bash
python3 scripts/ci/arrow_cdata.py build \
  --work-dir /tmp/arrow-cdata-build \
  --output target/arrow-cdata-12-linux-aarch64.jar
sbt -DsparkVersion=3.5 -DscalaVersion=2.12.20 \
  "-Drelease.version=$release_version" \
  -Darrow.cdata.supplement=/absolute/path/to/arrow-cdata-12-linux-aarch64.jar \
  spark/releaseJvmArtifacts
```

The build verifies the pinned Apache source archive, architecture and ABI
baseline, and packages the library with source inputs, licenses and provenance.
The JVM exporter verifies and embeds those same bytes. The same supplement is
required by Spark 3.5's local `spark/assembly` task, but ordinary compilation and
tests do not require it. The release workflow builds this supplement separately
from the Milvus native bundle and requires real ARM64 reads and writes.

Arrow sources and the Spark 3.5 / Scala 2.13 consumer distribution have fixed
SHA-512 digests in their download scripts. Downloads retry transient failures
up to three attempts; checksum mismatches fail. Verified archives are cached by
digest and rechecked before reuse. Both `arrow_cdata.py build` and
`consumer_smoke.py` accept `--download-cache PATH`, defaulting to
`SPARK_MILVUS_DOWNLOAD_CACHE` or `~/.cache/spark-milvus/downloads`. A new Spark
consumer runtime requires its official archive digest in `SPARK_ARCHIVES` in
`scripts/ci/consumer_smoke.py`; unpinned versions are rejected.

Export metadata records Maven dependencies under `bundledDependencies` and the
source-built Arrow library under `bundledComponents`. The latter records its
version, platform, binary digest, source archive identity and embedded provenance,
LICENSE and NOTICE paths. Staging preserves it in the candidate manifest;
dependency and license audits must inspect both inventories.

`project/public-packages.txt` defines class/source ownership. The exporter and
candidate verifier share `project/release-jvm-contract.json`, which requires
entry points and binding metadata from all four production modules and permits
native resources only at declared Arrow and gRPC dependency paths. Unexpected
native binaries, including a `.so` at the JAR root, are rejected. Update package
responsibilities before changing that contract.

Restart sbt and clean module outputs between profiles. Stage collected native
and JVM inputs with `scripts/release.py stage --help`, then verify:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
```

Verification checks the complete inventory, required module entries, public
package boundaries, POMs, native components, source identity, shared coordinates
and file bytes. It rejects undeclared native binaries and checks the source-built
component inventory against embedded provenance. The root
publishes nothing. Publish the candidate bytes that passed acceptance, without
rebuilding them in the publication job.

Ordinary PR checks do not satisfy release acceptance. Full CI covers all three
native platforms, real V3 reads, Arrow/Connect, and column replacement on every
Spark/Scala combination, both locally and on isolated object storage. Required
runtime tests must run without native/fixture cancellations. Consumers resolve
artifacts in isolated projects with independent executors. Set `JAVA_HOME` to
JDK 21 for fixture compilation. `CONSUMER_JAVA_HOME` selects the Spark runtime
JDK: Java 17 for Spark 3.5 and Java 21 for Spark 4.x; it defaults to `JAVA_HOME`.
The consumer validates both versions before installing Spark or compiling
fixtures. Select an exact Scala version with `--scala`; derive it from the
candidate using `scripts/ci/candidate_scala.py` instead of assuming a patch
version. Every consumer must pass all nine
checks: executor isolation, fixed V3 rows, projection/deletes, empty results,
repeated actions, physical row identities, replacement receipt, committed-revision
readback, and an unchanged source snapshot. Readback checks fixed values,
including NULLs, unchanged vectors and deleted rows. Its separate snapshot pins
the returned revisions without registering them with Milvus. Linux assembly
acceptance also runs Python and Java reads on the host Spark runtime.

## Publish from a protected tag

Release runs only upstream, from **`main`**. Merge the intended changes through
a PR to `main`, wait for the complete **push** run of `.github/workflows/ci.yml`
to succeed for that exact commit, then tag that tested commit with an unused
release version. Release branches are not part of this publication procedure.
CI from another branch, a PR or a manual dispatch does not satisfy the
release-source requirement, even when the source SHA is identical.

Before tagging, confirm that the release workflow requires successful `main`
push CI. Normal release tags, prereleases and Central-test tags all use this
same source requirement, regardless of their version line.

The source gate must validate both the successful CI run and its native
artifacts. The run must belong to the upstream repository, use
`.github/workflows/ci.yml`, have event `push`, and match `main` and the
exact tagged commit. It must provide exactly one unexpired, nonempty
`native-<platform>` artifact for every platform in `scripts/release-matrix.json`.
Each artifact's workflow metadata must match that same branch, commit and
selected CI run ID. Recover missing `main` CI or artifacts before tagging.

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
rebuild the candidate.

For manual publication, apply the same `main` branch and successful push-CI
requirements before building the candidate. The low-level commands below
verify candidate files and publish them, but do not enforce branch provenance
themselves. Passing `verify` does not establish release-source eligibility.
With the expected key already imported into an isolated GPG home and
credentials securely supplied, run:

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
consumer_scala=$(python3 scripts/ci/candidate_scala.py \
  --candidate target/public-candidate --line 4.0 --scala-binary 2.13)
python3 scripts/ci/consumer_smoke.py --candidate target/public-candidate \
  --line 4.0 --scala "$consumer_scala" --platform linux-x86_64 \
  --central --output target/public-consumer
```

The workflow does not create the GitHub release announcement. Once public
delivery succeeds, render `docs/release-notes/3.0.md` into a temporary Markdown
body, replacing every `{{VERSION}}` with the published version without `v`.
Remove the template notice and ensure the capabilities, artifact inventory
and build provenance match the published candidate. For 3.0.0, distinguish the
original artifacts from the incremental packages built from main; the original
tag alone does not describe the complete published inventory.
Keep Maven coordinates, dependency XML, artifact choices and platform requirements
in that body;
do not replace it with a link-only announcement. Verify no placeholder remains
and all repository links resolve at the intended tag. GitHub release bodies
need absolute repository links; relative document links resolve differently
there. A documentation correction to an existing announcement can use the
approved updated template without moving its release tag. If a linked document
does not exist at that tag, use an immutable commit containing the document.

Create or edit the announcement only within the user's authorized release-note
scope. Pass multiline Markdown using `--notes-file` or a structured API body.
For `gh release create`, use `--verify-tag` so it cannot silently create a tag;
preserve prerelease status and link to the actual source tag. Report candidate
identity, workflow/consumer results and any incomplete acceptance separately
from publication and announcement status.
