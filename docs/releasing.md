# Building and releasing Maven artifacts

GitHub Actions builds the JVM and native artifacts independently, tests the exact
Maven candidate that developers resolve, and publishes those files to Maven
Central. The public workflows need the three public source submodules; they do
not fetch `docs/design` or depend on Jenkins.

## Supported release matrix

| Component | Initial release configuration |
|---|---|
| Spark / Scala / Java | Spark 4.0.1 and 4.2.0, Scala 2.13.16, JDK 21 |
| Native platforms | `linux-x86_64`, `linux-aarch64` |
| Native build hosts | Ubuntu 22.04 on each architecture, GCC 12 including gfortran |
| Consumer hosts | Ubuntu 22.04 and 24.04 on each architecture |
| Entry points | Universal and platform-specific coordinates; Spark 4.0 also has a thin module coordinate |

The eight consumer jobs cover both Spark lines, both architectures and both OS
versions. Each job tests both entry points against the same candidate; Spark
4.0 jobs also test the thin modules with a separate native bundle. Each
platform's native JAR is built once on Ubuntu 22.04 and reused on both OS
versions. A failed platform or consumer blocks publication.

The Spark 4.0 connector compiles against 4.0.0 and is tested with 4.0.1;
the 4.2 connector compiles and runs against 4.2.0. Export metadata records the
actual compile version. Thin POMs must match that version exactly, while the
consumer version must be an equal or newer patch release of the same Spark line.

This release targets glibc. musl/Alpine is outside the matrix. macOS profiles
remain available for local builds; they are not published by this workflow.
All five source combinations receive compile/source/POM checks; full-validation
CI also runs native read/search suites on Linux x86_64. These checks do not add
3.5 or 4.1 release coordinates. Open-source release bundles disable Cardinal.

The ELF audit rejects imported symbol requirements above GLIBC 2.35, GLIBCXX
3.4.30 and CXXABI 1.3.13. Its report records the actual requirements and native
JAR digest. These bounds and consumer tests establish the tested OS baseline;
they do not establish compatibility with every Linux distribution or CPU of
the same architecture. CPU instructions are a separate constraint: inspect the
native provenance and validate on deployment CPUs, especially when using custom
compiler flags or Cardinal's `-march=native` build. System libraries and the
running JRE's signal-chaining library remain runtime prerequisites.

Linux profiles keep OpenBLAS runtime dispatch and fix its common compilation
target to `NEHALEM` on x86 and `ARMV8` on ARM. Its compiled thread capacity is
64, independent of the build host's core count; runtime thread settings still
control concurrency. This CMake setting participates in the Conan package ID.
Leaving the target to host detection can put AVX512 instructions in the ZEN
kernel even though it is selected for AMD CPUs without AVX512.

Every x86 native build and bundle cache hit also runs batched Knowhere L2
search in a separate Ubuntu 24.04 job under QEMU 8 or newer, using Nehalem,
Haswell and EPYC CPU models and comparing with per-query results. The probe
asserts the guest vendor and instruction sets so an emulator cannot silently
fall back to a weaker CPU. Candidate staging requires this job to pass. This catches CPU dispatch regressions without overriding
OpenBLAS or Faiss kernel selection. The real Spark consumer matrix remains
required.

## Maven coordinates and runtime requirements

All artifacts use group `com.zilliz` and the same final version. `<line>` is
`4.0` or `4.2`; `<platform>` is one of the two Linux platforms above.

| Artifact ID | Contents and dependencies |
|---|---|
| `spark-milvus-<line>_2.13` | Small real entry JAR and POM; depends on the JVM artifact and every released native platform |
| `spark-milvus-<line>-<platform>_2.13` | Small real entry JAR and POM; depends on the same JVM artifact and exactly one native platform |
| `spark-milvus-<line>-jvm_2.13` | Connector implementation, upstream Java APIs and bundled dependencies; Spark dependencies remain provided |
| `spark-milvus-native-<platform>` | Both milvus-storage and Knowhere JNI libraries and their shared non-system dependency closure |

Resolve an entry through Maven or `spark-submit --packages`. Supplying its small
JAR alone through `--jars` does not resolve the implementation or native JARs.
Use `sbt -DsparkVersion=<line> spark/assembly` with a selected native bundle
for local fat-JAR workflows. Root assembly/publication is retired.

The JVM release contains no Storage/Knowhere platform bundle. It does retain
the official Arrow C Data JAR's native runtime resources, which Spark does not
provide. JVM compatibility metadata records the expected Storage and Knowhere
source pins. The native loader checks those pins against the selected platform
manifest before initializing JNI; features such as Cardinal support come from
that manifest. Universal entries select the matching platform at runtime.

Sources and Javadoc classifiers accompany every artifact. JVM classifiers
contain the connector sources and generated API documentation. Entry and native
classifiers describe their role and source/provenance locations; native binaries
also carry their provenance and license material.

### Thin modules for downstream libraries

`com.zilliz:spark-milvus-4.0-thin_2.13` contains the Spark connector classes.
Its POM resolves `spark-milvus-core_2.13`,
`spark-milvus-native-storage_2.13`, `spark-milvus-native-runtime` and
`spark-milvus-native-vector`. All five artifacts use the same candidate version. Each has a POM, binary,
sources and Javadoc; `native-vector` includes the pinned Knowhere Java API.
The exported POMs preserve module boundaries and resolve external dependencies.
The Spark module marks dependencies supplied by Spark as `provided`; the four
lower-level modules retain their declared scopes so their compile dependencies
remain available to downstream libraries without Spark.

Thin is a packaging choice, not another implementation or a new sbt project.
Downstream code compiles against these JARs without copying connector sources.
The thin coordinate does not depend on a platform bundle. Applications must
add exactly one matching native bundle when assembling their runtime; the
loader checks its Storage and Knowhere source pins against `native-runtime`.
Do not combine thin modules with the bundled `-jvm` or assembly JAR: their
connector classes overlap.

The candidate is read-only. Verification rejects client, Catalog, CALL, writer,
backup and legacy-format connector classes/sources; retired DataFrame methods
and service/backup options; private dependencies; and the Milvus SDK. Thin
metadata must declare `migrationState=read-only`.

Load the **running JRE's** `libjsig.so` before both the driver and every executor
start. Paths may differ between driver and workers. For example, with an
available release version and Spark 4.0 installed:

```bash
release_version=2.0.0-rc.1  # replace with the released version to consume
worker_jsig=/absolute/path/on/workers/to/jdk21/lib/libjsig.so
export LD_PRELOAD="$JAVA_HOME/lib/libjsig.so"
"$SPARK_HOME/bin/spark-submit" \
  --packages "com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:$release_version" \
  --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions \
  --conf spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin \
  --conf "spark.executorEnv.LD_PRELOAD=$worker_jsig" \
  your_application.py
```

Use the matching Spark-line coordinate and configure object storage as described
in [the reference](reference-en.md). In cluster deploy mode, configure the driver
container or launcher environment too: exporting a variable only on the submit
host does not configure a remote driver.

## Build a candidate

Use the same clean source commit and public submodule pins on every build host.
Choose an unused final version before building; staging and publication never
rewrite it. Python 3.12, JDK 21 and sbt are required. Native hosts also need the
[platform toolchain](../native-build/README.md).

```bash
git submodule update --init --recursive milvus-proto milvus-storage knowhere
release_version=2.0.0-rc.1
sbt "exportBuildMatrix target/build-matrix.json"
python3 scripts/release.py plan --matrix target/build-matrix.json \
  --version "$release_version" --output target/release-plan.json
sbt -DsparkVersion=4.0 -DscalaVersion=2.13.16 "-Drelease.version=$release_version" \
  "spark/releaseJvmArtifacts" "spark/releaseThinArtifacts"
# Restart sbt and clean module products before changing the selected line.
sbt -DsparkVersion=4.2 -DscalaVersion=2.13.16 "-Drelease.version=$release_version" \
  "native-runtime/clean; native-storage/clean; native-vector/clean; core/clean; spark/clean; integration/clean" \
  "spark/releaseJvmArtifacts"
```

Each selected JVM task writes `target/release-jvm/<line>` with binary, sources,
Javadoc and `artifact.json`. Module clean does not remove these release exports;
root `clean` does, so retain/stage completed exports before using root clean.
The tasks neither select nor compile a native bundle.

The 4.0 thin task writes five module exports and `thin-artifacts.json` to
`target/release-thin/4.0`. It does not enable direct sbt publication. The verifier
checks the final read-only surface, POM closure, duplicate coordinates/classes,
upstream APIs, source pins and absence of embedded platform resources.

Build on **each** Ubuntu 22.04 architecture; change `native_platform` to match
the host. Do not use a newer OS to produce the older-baseline binary.

```bash
native_platform=linux-x86_64
native_output="$PWD/target/release-inputs/native-$native_platform"
mkdir -p "$native_output"
make native-bundle \
  NATIVE_WORK_DIR="$PWD/target/release-build/$native_platform" \
  NATIVE_BUNDLE_OUTPUT="$native_output/milvus-native-$native_platform.jar" \
  NATIVE_JOBS=4
python3 scripts/check-native-abi.py \
  --bundle "$native_output/milvus-native-$native_platform.jar" \
  --platform "$native_platform" --output "$native_output/abi.json"
```

Collect both native directories, including each `.jar.properties` sidecar and
`abi.json`, under `target/release-inputs`. Copy the JVM exports into that input
directory once, then stage the complete candidate:

```bash
cp -R target/release-jvm/4.0 target/release-inputs/jvm-4.0
cp -R target/release-jvm/4.2 target/release-inputs/jvm-4.2
cp -R target/release-thin/4.0 target/release-inputs/thin-4.0
python3 scripts/release.py stage --plan target/release-plan.json \
  --inputs target/release-inputs --output target/release-candidate
python3 scripts/release.py verify --candidate target/release-candidate
```

The candidate contains `repository/` in Maven layout and
`release-manifest.json`. The manifest records the final version, source commit,
public submodule pins, matrix, Maven paths and SHA-256 digests. Staging checks the
JVM exports, native provenance, sidecar checksums and matching ABI reports.

For downstream development, a thin-only candidate uses the same staging and
verification code, without rebuilding native artifacts:

```bash
python3 scripts/release.py plan --matrix target/build-matrix.json --version "$release_version" --thin-only \
  --output target/thin-plan.json
python3 scripts/release.py stage --plan target/thin-plan.json \
  --inputs target/release-thin/4.0 --output target/thin-candidate
python3 scripts/release.py verify --candidate target/thin-candidate
```

Point the downstream Maven resolver at `target/thin-candidate/repository`.
This local workflow does not publish artifacts or satisfy the full release
matrix. A thin-only runtime test needs a previously built OSS platform bundle
with the same source pins:

```bash
python3 scripts/ci/consumer_smoke.py --candidate target/thin-candidate \
  --line 4.0 --platform linux-x86_64 --entry thin \
  --native-bundle /absolute/path/to/milvus-native-linux-x86_64.jar \
  --output target/thin-consumer-4.0
```

## Consumer gates and CI triggers

On each consumer host, select the matching platform and run each Spark line:

```bash
python3 scripts/ci/consumer_smoke.py --candidate target/release-candidate \
  --line 4.0 --platform linux-x86_64 --output target/consumer-4.0
```

The script installs the matching PySpark distribution, creates a separate sbt
project containing only the smoke fixtures, and resolves production classes
from the candidate. It runs real `spark-submit --packages` for both entry types
and the Spark 4.0 thin coordinate, with fresh dependency caches and no Maven
local fallback. The thin run adds its matching native bundle via `--jars`.
Resolved JAR digests and native platform sets must
match the manifest. No implementation source is compiled into the fixture JAR.

The workload reads checked-in, SHA-256-verified files through real Storage and
Knowhere code. It covers V3 projection, nullable fields, deletes and exact and
approximate vector search over existing HNSW indexes. Approximate searches must
report index loads, so an exact fallback cannot satisfy that gate. Snapshot and
advanced schema/manifest inputs both use V3; an empty manifest list preserves
the schema. Repeated actions and SQL run against the same fixed relation.
Expected rows and the L2 oracle are independent of the reader under test.
`consumer-result.json` records the hashes of all packaged fixture resources and
the ten required check names with executed/passed/canceled counts. Missing or
duplicate checks fail the consumer.
The consumer compiles only `ReadOnlyConsumerSmoke` and the fixture integrity
helper; its JAR is rejected if it contains production classes or references
writers, procedures, Catalog, management parser or the online client. No normal consumer command generates
segments, builds indexes, publishes snapshots or calls Milvus.

`local-cluster` starts an independent executor JVM, and the smoke checks its PID
is different from the driver's. Both processes preload their JRE's `libjsig`.
Missing native libraries or canceled native tests cannot satisfy this gate.

Each entry keeps its driver log and a `<entry>-diagnostics` directory under
`--output`, including executor stdout/stderr and JVM fatal error reports.
Diagnostics are collected before temporary files are removed, including when
Spark fails, and CI uploads them with the consumer evidence. An executor exit
code alone does not identify the native failure; inspect these reports. CI also
records the consumer CPU, operating system and exact JDK version.

This is an **offline native end-to-end test on local files**, not a live Milvus
service test. The separate `integration/test` suite reads fixed V3 inputs
from live object storage. The public native CI runs the V3 DataSource suite using an isolated MinIO
fixture with two identities. Registration and online service behavior remain
outside the public gate. The public integration Helm chart renders only
a fixed-input read Job with an existing two-identity Secret; service setup and
write integration charts belong to the private repository.

| Event | Required work |
|---|---|
| Pull request to `main` or `refactor/v2` | Formatting, Versions-derived five-combination source/POM/source-JAR checks, shared unit tests, selected-line aggregate coverage, clean checks, scripts and Helm; existing JVM/thin exports |
| Push to those branches or manual CI dispatch | The checks above, native builds, mandatory read/search/Arrow/Connect suites, isolated object-storage reads, candidate verification and all eight consumer jobs |
| `v2.x.y-central-test.<digits>` tag | Full CI, signed `USER_MANAGED` upload, Central validation, authenticated byte verification, one consumer job and deployment deletion; no public release |
| Other accepted `v2.*` release tag | Full CI explicitly enabled, nonempty validated candidate, controlled publication and public Central consumer check |

A successful PR CI result covers the PR checks and JVM export; it does not mean
the full native release matrix has passed. Reusable CI defaults to full
validation. Release explicitly requests it and cannot publish an absent
candidate. Public fork jobs receive no publication credentials and do not fetch
private design documents.

## Native runner capacity and caches

Native compilation needs substantial disk space in addition to its toolchain.
The CI preparation script removes only `/usr/local/lib/android`,
`/usr/share/dotnet` and `/opt/ghc`, and only when both `GITHUB_ACTIONS=true` and
`RUNNER_ENVIRONMENT=github-hosted`. It requires at least 30 GiB free before
restoring dependency caches. Local and self-hosted machines are never cleaned
automatically. Cache growth and build size can require more capacity; a runner
that fails the capacity check must be provisioned with enough space.

Native caching has three levels, all separated by platform, OS baseline and
actual toolchain identity. Cache format v2 uses the declared compilation targets
and fixed OpenBLAS capacity; it does not key on the random runner CPU. The
build provenance still records that CPU. The format change requires an initial
rebuild and does not fall back to older complete bundles:

- The complete native JAR and checksum sidecar use an exact fingerprint of the
  pinned engines, native build inputs and toolchain. Connector-only JVM changes
  and release version changes do not invalidate that fingerprint. No fallback
  cache prefix is permitted for these binaries.
- Conan package contents and their package database are cached under explicit
  `CONAN_HOME/p`; authentication databases and credentials are excluded.
- Explicit `CCACHE_DIR` and Cargo registry/git directories under `CARGO_HOME`
  reuse compiler results and dependency downloads. Dependency caches may use a
  compatible toolchain prefix. Cargo compilation targets are not cached by CI;
  safe reuse of a target across build directories has not been established.

After a successful native build and ABI audit, CI removes Conan build, source,
download and temporary files while preserving installed packages and the package
database. It sets `CCACHE_MAXSIZE=2G`, runs its cleanup, and logs the remaining
Conan, Cargo and ccache sizes before the caches are saved. These measures reduce
cache storage; availability and eviction still depend on the repository's cache
quota.

A complete bundle hit skips native compilation, but always reruns the ELF ABI
audit. Staging still verifies pins and checksums, and the full consumer matrix
still runs. Caches accelerate a gate; they do not replace it.

GitHub scopes caches by ref and otherwise falls back only to the repository's
default branch. This repository's default branch is `main`, so a release tag
cannot directly restore a complete bundle cached by `refactor/v2`. Before a
release runs, it therefore requires a successful `CI` push run on
`refactor/v2` for the exact tagged commit and verifies that run still has one
unexpired native artifact for each Linux platform. On a tag-local cache miss,
the native jobs download those artifacts by exact run ID and name. They accept
them only when the file inventory is regular and complete, the recorded native
inputs exactly equal the inputs recomputed with the current prepared toolchain,
and the platform and JAR SHA-256 agree across the sidecar and ABI report. The
current ABI audit, x86 CPU gate, candidate verification and all eight consumer
jobs still run. JVM artifacts and the versioned candidate are rebuilt by the
tag run. If the exact successful source run is missing or its artifacts no
longer satisfy these checks, the release fails before entering the protected
publication environment.

The small `cache-inputs-<platform>` artifact is uploaded before cache restoration.
Compare its `native-cache-inputs.json` with an earlier run to explain a miss
while a rebuild is still running.

## Configure and publish a release

Repository files do not configure GitHub protection rules or prove that stored
credentials work. The `maven-central` environment has the following secrets;
they are environment secrets rather than repository secrets so another workflow
cannot read them without entering the protected environment:

| GitHub environment secret | Environment variable in the publication step | Required value |
|---|---|---|
| `PGP_SECRET` | `GPG_PRIVATE_KEY` | ASCII-armored private signing key, or its base64 encoding |
| `PGP_PASSPHRASE` | `GPG_PASSPHRASE` | Signing key passphrase |
| `SONATYPE_USERNAME` | `CENTRAL_USERNAME` | Central Portal user-token username |
| `SONATYPE_PASSWORD` | `CENTRAL_TOKEN` | Central Portal user-token password |

The Sonatype names are retained for compatibility with repository configuration;
their values must be valid **Central Portal** token credentials. Existing legacy
Sonatype credentials are not evidence of Portal access. The signing public key
must be available for Central verification and the account must control the
`com.zilliz` namespace. The release script requires signing fingerprint
`F0004DED15D9C011F344D6A8526AB23093F3D44A`.

When a source displays a secret as Markdown inline code, store only the value
inside the formatting backticks; those backticks are not part of the value.
Preserve every character that belongs to the credential itself, and do not log
or commit the value while checking it.

A maintainer must configure the `maven-central` GitHub environment with required
reviewers and a deployment policy limited to `v2.*` tags. The release repository
has those controls and the four environment secrets as of 2026-10-16; forks and
replacement repositories must create them separately because committing the
workflow does not configure GitHub environments. Protect creation, replacement
and deletion of `v2.*` tags and ensure releases point to reviewed commits.
Restrict tag bypass and environment administration appropriately.

Release accepts `v2.x.y` tags and prereleases such as `v2.0.0-rc.1`. It verifies
that the candidate source commit is the tagged commit, then enters the
`maven-central` environment. Build and consumer jobs have no signing or Central
credentials. The publication job imports the key into a temporary GPG home,
requires exactly one primary secret key and explicitly selects its fingerprint
for every signature. A separate step receives only the Central credentials,
uploads a `USER_MANAGED` deployment, waits for Central validation and requests
publication. It does not rebuild the candidate.

A tag such as `v2.0.0-central-test.2026101601` takes the mutually exclusive test
path. It runs the same full validation and signing steps, uploads a
`USER_MANAGED` deployment, waits for `VALIDATED`, downloads every candidate file
through that deployment's authenticated endpoint and checks the bytes. The
workflow then deletes the deployment, retries that idempotent cleanup at the
step boundary, and runs the Spark 4.0 x86_64 universal and platform-specific
consumer checks from the downloaded repository. This path never calls Central's
promotion endpoint, so it does not create a public Maven version. The test
report records the deployment ID for manual cleanup if the runner stops before
deletion.

The equivalent final commands, with the signing key already imported and
credentials provided by a trusted environment, are:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
python3 scripts/release.py bundle --candidate target/release-candidate --output target/central.zip
python3 scripts/release.py publish --bundle target/central.zip
```

The equivalent reversible Central test is:

```bash
python3 scripts/release.py test-publish --bundle target/central.zip \
  --candidate target/release-candidate \
  --output target/central-test-candidate \
  --report target/central-test-report.json
```

If an interrupted run leaves the report's deployment in `VALIDATED` or `FAILED`,
remove it with:

```bash
python3 scripts/release.py drop --deployment-id <id> \
  --report target/central-test-report.json
```

Omit `--report` if that file is unavailable.

After publication, the workflow downloads every manifest-listed file from
Central and checks its digest, then consumes both entries again with fresh
caches on Ubuntu 22.04 x86_64 / Spark 4.0. The full eight-job matrix runs before
publication; this final public check verifies the published delivery path.
Locally, the corresponding commands are:

```bash
python3 scripts/release.py download-public --candidate target/release-candidate \
  --output target/public-candidate
python3 scripts/ci/consumer_smoke.py --candidate target/public-candidate \
  --line 4.0 --platform linux-x86_64 --central --output target/public-consumer
```
