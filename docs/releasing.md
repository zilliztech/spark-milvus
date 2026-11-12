# Building and releasing artifacts

A candidate ties JVM artifacts, source JARs, POMs and native bundles to one
source commit and unused version. Build once, verify and consume those bytes,
then publish the verified files. Fork workflows build and test candidates;
only the upstream repository can enter publication jobs.

## Artifact graph

| Product | Modules | Profiles |
|---|---|---|
| Bundled JVM | native-runtime, native-storage, core, spark and their JVM dependencies, without Milvus platform libraries | Spark 3.5 with Scala 2.12/2.13; 4.0, 4.1, 4.2 with Scala 2.13 |
| Native platform | Storage JNI and its dependency closure | Linux x86_64/aarch64; macOS aarch64 |

All artifacts use one immutable candidate version. Each Spark and Scala
combination has its own JVM, universal and platform-specific coordinates. Native
platform artifacts are shared across those combinations. The four production
modules remain internal build units; the root publishes nothing.

A native bundle has `format.version=2` and `components=storage` under
`native/milvus/2/<platform>/manifest.properties`. Binding modules contribute
component requirements under `META-INF/milvus-native/requirements/`. The
runtime checks each requested component, source revision, platform and file
checksum before loading. Supply exactly one complete platform bundle; do not
combine overlapping platform JARs. Public native artifacts contain only the
Storage component.

## Using release artifacts

The coordinates below describe the next release. The published 3.0.0 inventory
is unchanged; consult its
[artifact guide](https://github.com/zilliztech/spark-milvus/blob/v3.0.0/docs/releasing.md#using-release-artifacts)
when using that version. Installation commands require a version published to
Maven Central. Choose a
published version from
[GitHub Releases](https://github.com/zilliztech/spark-milvus/releases), then use
that same version for every connector artifact. CI candidates are separate from
public releases.

All Maven artifacts use group `com.zilliz`. `<line>` is `3.5`, `4.0`, `4.1`
or `4.2`. `<scala-binary>` is `2.12` or `2.13` for Spark 3.5, and `2.13` for
Spark 4.x. `<platform>` is `linux-x86_64`, `linux-aarch64` or `darwin-aarch64`.

| Artifact ID | Use |
|---|---|
| `spark-milvus-<line>_<scala-binary>` | Universal dependency entry: resolves the JVM artifact and all released native platforms; the loader selects the current platform. |
| `spark-milvus-<line>-<platform>_<scala-binary>` | Platform dependency entry: resolves the JVM artifact and one native platform. |
| `spark-milvus-<line>-jvm_<scala-binary>` | Bundled JVM implementation, without the Milvus platform libraries; Spark dependencies are provided by the application or cluster. |
| `spark-milvus-native-<platform>` | Storage native libraries, dependencies, provenance and licenses for one platform. |

Every supported Spark and Scala combination uses the same installation model.
For example, Spark 3.5 / Scala 2.12 uses `spark-milvus-3.5_2.12`, while
Spark 4.1 / Scala 2.13 uses `spark-milvus-4.1_2.13`. The native loader selects
the matching platform when using a universal entry. Do not combine a bundled
JVM JAR with an assembly JAR, which contains the same connector classes.
Sources and Javadoc accompany the Maven artifacts.

Resolve an entry with Maven, sbt or `spark-submit --packages`, as shown in
[the installation examples](../README.md#installing). Entry JARs contain only
delivery documentation: `--jars` does not resolve their POM dependencies.
For `--jars`, [build an assembly](../README.md#building), or explicitly supply
the bundled JVM artifact and exactly one matching native platform JAR from
[Maven Central](https://repo.maven.apache.org/maven2/com/zilliz/).

Released native platforms target glibc Linux (Ubuntu 22.04 and 24.04 in the
tested matrix, with a glibc 2.35 baseline) and Apple silicon on macOS 14 or later.
Linux also needs a system `libstdc++` providing `GLIBCXX_3.4.30` and
`CXXABI_1.3.13` (GCC 12's runtime or newer); it is not bundled in the native JAR.
They do not cover musl/Alpine or Windows. All driver and executor processes need
a matching platform, a writable native extraction directory, and their own JRE's signal
library. Linux uses `LD_PRELOAD` with `lib/libjsig.so`; macOS uses
`DYLD_INSERT_LIBRARIES` with `lib/libjsig.dylib`, passed to the JVM rather than
system shell utilities. See [cluster setup](cluster-setup.md) for Linux
deployment requirements.

## Build a candidate

CI assigns `3.0.0-rc.<run-id>.<producing-attempt>` and rejects coordinates
belonging to another run or attempt. Dispatch without a version override. An
explicit release uses its pushed tag. Candidate planning checks Central for
existing coordinates before building; failed lookups do not mean a name is
available. Maven orders numeric `alpha`, `beta` and `rc` qualifiers before the
corresponding final version. Arbitrary prerelease labels are rejected.
Rerun all jobs or dispatch a new run when producing a replacement candidate;
consumers bind the exact artifact ID and digest they accepted. Development
snapshots remain isolated by branch and architecture.

`project/public-packages.txt` declares package ownership for the four modules.
JVM bundles may contain the four production modules and their declared
dependencies. The exporter and final candidate verifier share
[`project/release-jvm-contract.json`](../project/release-jvm-contract.json): it
requires named entry points and binding metadata from all four modules, and
permits native resources only at the declared Arrow and gRPC dependency paths.
Unexpected native binaries, including a `.so` at the JAR root, are rejected.
Update a package's responsibility statement before changing this contract.

Use Java 21 and initialize `milvus-proto` and `milvus-storage`. The sbt
`exportBuildMatrix` task is authoritative for source profiles, native platforms
and deployment inputs. `scripts/release.py plan` derives the artifact inventory.
On Linux, install `binutils` so `readelf` is available before building the Arrow
supplement, exporting JVM artifacts, staging or verifying a candidate. These
checks repeat the supplement's ELF and ABI audit; candidate consumers such as
the Demo launcher also need this tool when they verify the complete candidate.

```bash
sbt 'exportBuildMatrix target/build-matrix.json'
python3 scripts/release.py plan --version 3.0.0-rc.1 \
  --matrix target/build-matrix.json --output target/release-plan.json
```

Build native bundles on the platform declared in the plan, using
`native/build/build.py`, `scripts/package-native.py` and
`scripts/check-native-abi.py`. The native toolchain and dependency lock are
pinned under `native/build/`. Linux builds retain the Ubuntu 22.04 ABI baseline.
Native audits execute on each build; cached bundles must pass the same identity
and checksum validation before reuse.

Export JVM artifacts with the selected Spark/Scala profile and
`-Drelease.version=<version>`. `spark/releaseJvmArtifacts` exports the JVM
implementation, sources, API documentation and build metadata. Each selection
emits `target/release-jvm/<spark-line>/<scala-binary>/artifact.json`; the Scala
binary version keeps the two Spark 3.5 variants separate. Build with Java 21;
consumer validation uses Java 17 for Spark 3.5 and Java 21 for Spark 4.x.
Spark 3.5's Arrow 12 dependency omits its Linux ARM64 C Data JNI library. Before
exporting either Scala variant, build the matching upstream library on a Linux
ARM64 runner. Set `RELEASE_VERSION` to an unused candidate version:

```bash
python3 scripts/ci/arrow_cdata.py build \
  --work-dir /tmp/arrow-cdata-build \
  --output target/arrow-cdata-12-linux-aarch64.jar
sbt -DsparkVersion=3.5 -DscalaVersion=2.12.20 \
  "-Drelease.version=$RELEASE_VERSION" \
  -Darrow.cdata.supplement=/absolute/path/to/arrow-cdata-12-linux-aarch64.jar \
  spark/releaseJvmArtifacts
```

The build verifies the pinned Apache source archive, Linux ARM64 architecture
and ABI baseline, and packages the library with its source inputs, licenses and
provenance. The JVM export verifies and embeds those same bytes. The release
workflow builds this supplement independently of the Milvus native bundle and
requires real reads and writes on ARM64 before publication.

Arrow sources and the Spark 3.5 / Scala 2.13 consumer distribution use fixed
SHA-512 digests checked into their download scripts. Downloads retry transient
network failures up to three attempts. Verified archives are cached by digest
and rechecked before reuse; a checksum mismatch fails the download. Both
`arrow_cdata.py build` and `consumer_smoke.py` accept `--download-cache PATH`.
The default is `SPARK_MILVUS_DOWNLOAD_CACHE`, or
`~/.cache/spark-milvus/downloads` when that variable is unset. Updating the Spark
consumer runtime requires adding its official archive digest to `SPARK_ARCHIVES`
in `scripts/ci/consumer_smoke.py`; an unpinned version is rejected.

Export metadata lists Maven dependencies under `bundledDependencies` and the
source-built Arrow JNI library under `bundledComponents`. The latter records
its version, platform, binary digest, source archive identity, and paths to its
embedded provenance, LICENSE and NOTICE; staging also preserves it in the
candidate manifest. Dependency and license audits must inspect both inventories.

Restart sbt and clean module outputs when changing profiles. Pass the collected
native and JVM inputs to `scripts/release.py stage`; its `--help` lists
required paths. Then verify the complete inventory:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
```

The verifier rejects missing required module entries, classes outside the public
packages, undeclared native binaries, wrong native components, incompatible
POMs, changed shared GAVs, source mismatches and changed file bytes. It also
checks the source-built component inventory against the embedded provenance.
Do not reuse a previously published version for the new graph.

## CI and acceptance

Pull requests run formatting, boundary checks, five source/Scala profiles,
unit tests, script tests and Helm contracts. Full validation builds all three
native platforms, executes real V3 reads and Arrow/Connect checks, and replaces
columns on every Spark/Scala combination, locally and on isolated object storage
under a prefix-scoped identity. The write must commit revisions that read back;
a mocked manifest is not runtime evidence.

Candidate consumers resolve the selected artifacts in isolated projects and
run with independent executors. Set `JAVA_HOME` to JDK 21 for compiling their
fixtures. `CONSUMER_JAVA_HOME` selects the Spark runtime JDK: use JDK 17 for
Spark 3.5 and JDK 21 for Spark 4.x. When unset, it defaults to `JAVA_HOME`.
The consumer validates both JDK versions before installing Spark or compiling
fixtures. All nine acceptance checks must pass: executor
isolation, fixed V3 rows, projection and deletes, empty results, repeated actions,
physical row identities, a column-replacement receipt, readback of the committed
revisions, and an unchanged source snapshot. Readback compares fixed expected
values, including NULLs, unchanged vectors and deleted rows. The consumer pins
the returned revisions in a separate snapshot for this check; it does not
register them with Milvus. Linux assembly jobs build and verify a standalone
JAR from the audited native bundle, then run Python and Java reads with that
JAR on the host Spark runtime.

Required GitHub check names must match the workflow after the migration. A
workflow file cannot update repository rules, environment secrets or approval
settings.

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
reviewers, a deployment policy allowing `v3.*` tags and the four secrets
above. Retain any policies still needed for older release lines; committing the
workflow does not configure GitHub environments. Protect
creation, replacement and deletion of `v3.*` tags and ensure releases point to
reviewed commits. Restrict tag bypass and environment administration
appropriately.

Release accepts `v3.x.y` tags and prereleases such as `v3.0.0-rc.1`. It verifies
that the candidate source commit is the tagged commit, then enters the
`maven-central` environment. Build and consumer jobs have no signing or Central
credentials. The publication job imports the key into a temporary GPG home,
requires exactly one primary secret key and explicitly selects its fingerprint
for every signature. A separate step receives only the Central credentials,
uploads a `USER_MANAGED` deployment, waits for Central validation and requests
publication. It does not rebuild the candidate.

A tag such as `v3.0.0-central-test.2026111001` takes the mutually exclusive test
path. It runs the same full validation and signing steps, uploads a
`USER_MANAGED` deployment, waits for `VALIDATED`, downloads every candidate file
through that deployment's authenticated endpoint and checks the bytes. The
workflow then deletes the deployment, retries that idempotent cleanup at the
step boundary, and runs the Spark 4.0 x86_64 universal and platform-specific
consumer checks from the downloaded repository. This path never calls Central's
promotion endpoint, so it does not create a public Maven version. The test
report records the deployment ID for manual cleanup if the runner stops before
deletion.

The equivalent Central commands, with the signing key already imported and
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
caches on Ubuntu 22.04 x86_64 / Spark 4.0. The full consumer matrix runs before
publication; this final public check verifies the published delivery path.
Locally, the corresponding commands are:

```bash
python3 scripts/release.py download-public --candidate target/release-candidate \
  --output target/public-candidate
consumer_scala=$(python3 scripts/ci/candidate_scala.py \
  --candidate target/public-candidate --line 4.0 --scala-binary 2.13)
python3 scripts/ci/consumer_smoke.py --candidate target/public-candidate \
  --line 4.0 --scala "$consumer_scala" --platform linux-x86_64 \
  --central --output target/public-consumer
```

After the public Central consumer succeeds, prepare the
GitHub release announcement from [the 3.0 release notes](release-notes/3.0.md).
Replace `{{VERSION}}` with the published version, remove the unreleased-template
notice and link the release to its source tag. This announcement is a maintainer
step outside the release workflow.
