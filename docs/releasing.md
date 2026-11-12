# Building and releasing artifacts

A candidate ties JVM artifacts, source JARs, POMs and native bundles to one
source commit and unused version. Build once, verify and consume those bytes,
then publish the verified files. Fork workflows build and test candidates;
only the upstream repository can enter publication jobs.

## Artifact graph

| Product | Modules | Profiles |
|---|---|---|
| Reader thin | native-runtime, native-storage, core, spark | Spark 3.5 with Scala 2.12/2.13; 4.0, 4.1, 4.2 with Scala 2.13 |
| Reader bundled JVM | JVM dependency closure, without platform libraries | Spark 4.0 and 4.2 / Scala 2.13 |
| Native platform | Storage JNI and its dependency closure | Linux x86_64/aarch64; macOS aarch64 |

All thin modules use one immutable candidate version. Shared artifacts exported
by several Spark profiles must have identical POM metadata and bytes for a given
Maven coordinate. The root project publishes nothing.

A native bundle has `format.version=2` and `components=storage` under
`native/milvus/2/<platform>/manifest.properties`. Binding modules contribute
component requirements under `META-INF/milvus-native/requirements/`. The
runtime checks each requested component, source revision, platform and file
checksum before loading. Supply exactly one complete platform bundle; do not
combine overlapping platform JARs. Public native artifacts contain only the
Storage component.

## Using release artifacts

The coordinates and URLs below describe the 3.0 delivery contract. Installation
commands require a version published to Maven Central. Choose a
published version from
[GitHub Releases](https://github.com/zilliztech/spark-milvus/releases), then use
that same version for every connector artifact. CI candidates are separate from
public releases.

All Maven artifacts use group `com.zilliz`. For the bundled distribution,
`<line>` is `4.0` or `4.2`, and `<platform>` is `linux-x86_64`,
`linux-aarch64` or `darwin-aarch64`.

| Artifact ID | Use |
|---|---|
| `spark-milvus-<line>_2.13` | Universal dependency entry: resolves the JVM artifact and all released native platforms; the loader selects the current platform. |
| `spark-milvus-<line>-<platform>_2.13` | Platform dependency entry: resolves the JVM artifact and one native platform. |
| `spark-milvus-<line>-jvm_2.13` | Bundled JVM implementation, without the Milvus platform libraries; Spark dependencies are provided by the application or cluster. |
| `spark-milvus-native-<platform>` | Storage native libraries, dependencies, provenance and licenses for one platform. |
| `spark-milvus-<line>-thin_<scala-binary>` | Unbundled connector classes with dependencies on `core`, `native-storage` and `native-runtime`; add a matching native platform separately. |

Thin coordinates cover Spark `3.5` with Scala `2.12` or `2.13`, and Spark
`4.0`, `4.1` and `4.2` with Scala `2.13`. They are useful for downstream
libraries that manage their own dependencies. Do not combine thin modules with
the bundled JVM or assembly JARs, which contain the same connector classes.
Sources and Javadoc accompany the Maven artifacts.

Resolve an entry with Maven, sbt or `spark-submit --packages`, as shown in
[the installation examples](../README.md#installing). Entry JARs contain only
delivery documentation: `--jars` does not resolve their POM dependencies.
For `--jars`, [build an assembly](application-development.md#building-the-connector-from-source), or explicitly supply
the bundled JVM artifact and exactly one matching native platform JAR from
[Maven Central](https://repo.maven.apache.org/maven2/com/zilliz/).
Spark 3.5/4.1 thin deployments also need their declared JVM dependencies.

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
Thin artifacts may contain only their owner's classes and sources; reader JVM
bundles may contain the reader modules and their declared dependencies. Update
a package's responsibility statement before changing this positive contract.

Use Java 21 and initialize `milvus-proto` and `milvus-storage`. The sbt
`exportBuildMatrix` task is authoritative for source profiles, native platforms
and deployment inputs. `scripts/release.py plan` derives the artifact inventory.

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
`-Drelease.version=<version>`. `spark/releaseThinArtifacts` exports the reader
closure.
The build workflow records each export in
`target/release-thin/<spark-line>/<scala-binary>/<product>/thin-artifacts.json`.
Restart sbt and clean module outputs when changing profiles. Pass the collected
native, JVM and thin inputs to `scripts/release.py stage`; its `--help` lists
required paths. Then verify the complete inventory:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
```

The verifier rejects missing or surplus modules, classes outside the public
packages, wrong native components, incompatible POMs, changed shared GAVs, source
mismatches and changed file bytes. Do not reuse a previously published version
for the new graph.

## CI and acceptance

Pull requests run formatting, boundary checks, five source/Scala profiles,
unit tests, script tests and Helm contracts. Full validation builds all three
native platforms, executes real V3 reads and Arrow/Connect checks, and replaces
columns on every Spark/Scala combination, locally and on isolated object storage
under a prefix-scoped identity. The write must commit revisions that read back;
a mocked manifest is not runtime evidence.

Candidate consumers resolve the selected artifacts in isolated projects and
run with independent executors. All nine acceptance checks must pass: executor
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
python3 scripts/ci/consumer_smoke.py --candidate target/public-candidate \
  --line 4.0 --platform linux-x86_64 --central --output target/public-consumer
```

After the public Central consumer succeeds, prepare the
GitHub release announcement from [the 3.0 release notes](release-notes/3.0.md).
Replace `{{VERSION}}` with the published version and link the release to its
source tag. This announcement is a maintainer step outside the release workflow.
