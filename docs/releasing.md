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

`project/public-packages.txt` declares package ownership for the five modules.
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
run with independent executors. Reader consumers assert fixed V3 rows,
projection, deletes, empty results and repeated actions. Linux
image jobs run the exact reader assembly.

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
reviewers, a deployment policy limited to `v3.*` tags and the four secrets
above; committing the workflow does not configure GitHub environments. Protect
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
