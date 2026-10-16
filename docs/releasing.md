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
| Entry points | Universal and platform-specific Maven coordinates |

The eight consumer jobs cover both Spark lines, both architectures and both OS
versions. Each job tests both entry points against the same candidate. Each
platform's native JAR is built once on Ubuntu 22.04 and reused on both OS
versions. A failed platform or consumer blocks publication.

This release targets glibc. musl/Alpine is outside the matrix. macOS profiles
remain available for local builds; they are not published by this workflow.
Other Spark lines continue to compile in ordinary CI. Open-source release
bundles disable Cardinal.

The ELF audit rejects imported symbol requirements above GLIBC 2.35, GLIBCXX
3.4.30 and CXXABI 1.3.13. Its report records the actual requirements and native
JAR digest. These bounds and consumer tests establish the tested OS baseline;
they do not establish compatibility with every Linux distribution or CPU of
the same architecture. CPU instructions are a separate constraint: inspect the
native provenance and validate on deployment CPUs, especially when using custom
compiler flags or Cardinal's `-march=native` build. System libraries and the
running JRE's signal-chaining library remain runtime prerequisites.

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
The existing `sbt assembly` for Spark 4.0 and `sbt spark42/assembly` for Spark 4.2
remain available for local fat-JAR workflows.

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

Load the **running JRE's** `libjsig.so` before both the driver and every executor
start. Paths may differ between driver and workers. For example, with an
available release version and Spark 4.0 installed:

```bash
release_version=2.0.0-rc.1  # replace with the released version to consume
worker_jsig=/absolute/path/on/workers/to/jdk21/lib/libjsig.so
export LD_PRELOAD="$JAVA_HOME/lib/libjsig.so"
"$SPARK_HOME/bin/spark-submit" \
  --packages "com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:$release_version" \
  --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions \
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
python3 scripts/release.py plan --version "$release_version" --output target/release-plan.json
sbt "-Drelease.version=$release_version" releaseJvmArtifacts
```

The JVM task writes `target/release-jvm/4.0` and `target/release-jvm/4.2`, each
with its binary, sources, Javadoc and `artifact.json`. A matrix job can instead
run `spark40/releaseJvmArtifacts` or `spark42/releaseJvmArtifacts`. These exports
do not select or compile a native bundle.

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
python3 scripts/release.py stage --plan target/release-plan.json \
  --inputs target/release-inputs --output target/release-candidate
python3 scripts/release.py verify --candidate target/release-candidate
```

The candidate contains `repository/` in Maven layout and
`release-manifest.json`. The manifest records the final version, source commit,
public submodule pins, matrix, Maven paths and SHA-256 digests. Staging checks the
JVM exports, native provenance, sidecar checksums and matching ABI reports.

## Consumer gates and CI triggers

On each consumer host, select the matching platform and run each Spark line:

```bash
python3 scripts/ci/consumer_smoke.py --candidate target/release-candidate \
  --line 4.0 --platform linux-x86_64 --output target/consumer-4.0
```

The script installs the matching PySpark distribution, creates a separate sbt
project containing only the smoke fixtures, and resolves production classes
from the candidate. It runs real `spark-submit --packages` for both entry types
with fresh dependency caches. Resolved JAR digests and native platform sets must
match the manifest. No implementation source is compiled into the fixture JAR.

The workload exercises real Storage and Knowhere code: write, read, deletes,
exact vector search, index building, approximate search and output writes.
`local-cluster` starts an independent executor JVM, and the smoke checks its PID
is different from the driver's. Both processes preload their JRE's `libjsig`.
Missing native libraries or canceled native tests cannot satisfy this gate.

This is an **offline native end-to-end test on local files**, not a live Milvus
service test. The separate `integration40/test` suites require live Milvus and
MinIO; this release workflow does not run them. Registration and online service
behavior therefore remain outside this gate.

| Event | Required work |
|---|---|
| Pull request to `main` or `refactor/v2` | Existing formatting, compile, unit, script and Helm checks, plus JVM release export for both lines |
| Push to those branches or manual CI dispatch | The checks above, native builds, candidate verification and all eight consumer jobs |
| Accepted `v2.*` release tag | Full CI explicitly enabled, nonempty validated candidate, controlled publication and public Central consumer check |

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
actual toolchain identity:

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

## Configure and publish a release

Repository files do not configure GitHub protection rules or prove that stored
credentials work. The repository already has the following secret names; their
values have not been read or validated for this workflow:

| GitHub secret | Environment variable in the publication step | Required value |
|---|---|---|
| `PGP_SECRET` | `GPG_PRIVATE_KEY` | ASCII-armored private signing key, or its base64 encoding |
| `PGP_PASSPHRASE` | `GPG_PASSPHRASE` | Signing key passphrase |
| `SONATYPE_USERNAME` | `CENTRAL_USERNAME` | Central Portal user-token username |
| `SONATYPE_PASSWORD` | `CENTRAL_TOKEN` | Central Portal user-token password |

The Sonatype names are retained for compatibility with repository configuration;
their values must be valid **Central Portal** token credentials. Existing legacy
Sonatype credentials are not evidence of Portal access. The signing public key
must be available for Central verification and the account must control the
`com.zilliz` namespace.

A maintainer must configure the `maven-central` GitHub environment with required
reviewers and permitted release tags. No environment was configured when this
workflow was introduced. Protect creation, replacement and deletion of `v2.*`
tags and ensure releases point to reviewed commits. Restrict tag bypass and
environment administration appropriately. These are repository settings, not
checks that committing this YAML enables.

Release accepts `v2.x.y` tags and prereleases such as `v2.0.0-rc.1`. It verifies
that the candidate source commit is the tagged commit, then enters the
`maven-central` environment. Build and consumer jobs have no signing or Central
credentials. The publication job imports the key into a temporary GPG home,
signs the already tested files, uploads a `USER_MANAGED` deployment, waits for
Central validation and requests publication. It does not rebuild the candidate.

The equivalent final commands, with the signing key already imported and
credentials provided by a trusted environment, are:

```bash
python3 scripts/release.py verify --candidate target/release-candidate
python3 scripts/release.py bundle --candidate target/release-candidate --output target/central.zip
python3 scripts/release.py publish --bundle target/central.zip
```

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
