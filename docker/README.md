# Build and run the connector image

[build.sh](build.sh) packages an existing Spark-Milvus assembly with Spark
4.0.1, Scala 2.13, Java 21, Python, the example sources and the frozen sample.
The 2.0.0 release publishes the image as `milvusdb/spark-milvus:2.0.0-spark4.0`;
see [the release guide](../docs/releasing.md#publish-the-container-image).
The host needs Docker, Bash and Python 3. Java compilation happens in the image;
the image build does not compile native libraries.

## Build from an assembly

Use a Spark 4.0 / Scala 2.13 assembly containing the matching Linux native
bundle. Download the 2.0.0 assembly for
[x86_64](https://github.com/zilliztech/spark-milvus/releases/download/v2.0.0/spark-milvus-4.0-bundle-2.0.0-linux-x86_64.jar)
or [aarch64](https://github.com/zilliztech/spark-milvus/releases/download/v2.0.0/spark-milvus-4.0-bundle-2.0.0-linux-aarch64.jar),
or build one using [Prepare the runtime](../demo/README.md#prepare-the-runtime)
and the [native build guide](../native/build/README.md). A thin JVM JAR or
standalone native JAR is not an assembly.

From the repository root:

```bash
./docker/build.sh \
  --assembly "/absolute/path/to/spark-milvus-4.0-bundle-2.0.0-linux-x86_64.jar" \
  --image spark-milvus-demo:4.0.1-jdk21-amd64 \
  --platform linux/amd64
```

| Option | Meaning |
|---|---|
| `--assembly PATH` | Required assembly, including the selected platform's native libraries. |
| `--image TAG` | Required output image name and tag. |
| `--base-image REF` | Compatible base image; defaults to `spark:4.0.1-scala2.13-java21-python3-ubuntu`. |
| `--platform PLATFORM` | `linux/amd64` or `linux/arm64`; defaults to the host architecture. |
| `--push` | Push the image after a successful build. Omit for a local build. |
| `--help` | Show help without requiring Docker or an assembly. |

The helper validates `MilvusSparkPlugin`, the native platform manifest and both
JNI libraries before invoking Docker. It prints the assembly SHA256 and records
it in the image label `io.zilliz.spark-milvus.assembly-sha256`. The Dockerfile
checks the copied JAR's hash, verifies fixture checksums and compiles the Java
examples into a separate application JAR.

The build context is an owned temporary directory populated from an explicit
file whitelist. Git metadata, build outputs, Python caches and additional
private options files are excluded; the directory is removed when the command
exits. Keep real credentials outside the public configuration templates and
mount them at runtime.

## Image contents and runtime

| Container path | Contents |
|---|---|
| `/opt/spark-milvus/connector.jar` | Supplied connector assembly and native libraries. |
| `/opt/spark-milvus/demo-java.jar` | Compiled Java example classes for `spark-submit`. |
| `/opt/spark-milvus/demo/run.sh` | Python/Java launcher, including the REST snapshot-path helpers. |
| `/opt/spark-milvus/demo/python/`, `/opt/spark-milvus/demo/java/`, `/opt/spark-milvus/demo/config/` | Example sources and public configuration templates. |
| `/opt/spark-milvus/core/src/test/resources/read-fixtures/collections/nearest/` | Immutable sample, including persisted indexes and checksum manifest. |
| `/opt/spark-milvus/scripts/ci/check_read_fixtures.py` | Sample integrity checker. |
| `/opt/spark-milvus/LICENSE` | Repository license. |

`JAVA_HOME=/opt/java/openjdk`, `SPARK_HOME=/opt/spark` and
`CONNECTOR_JAR=/opt/spark-milvus/connector.jar` are configured in the image.
It also sets Java's `libjsig` preload, immediate symbol binding and glibc
allocator settings for the native libraries.

The image runs as UID/GID `185:185`. Its working directory is the separate,
writable `/opt/spark/work-dir`; `/tmp` and `/opt/spark-milvus/demo/target` are writable for
temporary runtime and Java launcher outputs. Keeping the working directory
separate from the packaged JARs prevents Spark's resource localization from
overwriting those JARs.

The base image's `/opt/entrypoint.sh` is preserved. It supports Spark's
Kubernetes `driver` and `executor` roles and passes ordinary commands through.
The default command prints demo help.

## Run locally

These commands use the baked 14-row sample, with no Milvus service or storage
credentials:

```bash
DEMO_IMAGE=milvusdb/spark-milvus:2.0.0-spark4.0
docker pull "$DEMO_IMAGE"
docker run --rm "$DEMO_IMAGE"
docker run --rm "$DEMO_IMAGE" /opt/spark-milvus/demo/run.sh python 01_read_snapshot.py
docker run --rm "$DEMO_IMAGE" /opt/spark-milvus/demo/run.sh java ReadSnapshot
docker run --rm "$DEMO_IMAGE" /opt/spark-milvus/demo/run.sh python 04_exact_search.py
docker run --rm "$DEMO_IMAGE" /opt/spark-milvus/demo/run.sh java IndexSearch --explain
```

Use any other entry point from the [example list](../demo/README.md#examples) in the
same way. The local launcher defaults to `MASTER=local[2]`. It configures the
connector extension and plugin. The Java launcher recompiles sources on each
run; Kubernetes deployment uses the precompiled application JAR.

For an existing V3 snapshot, mount private inputs read-only and pass their
container paths. The mounted directory and files must permit UID/GID `185` to
traverse and read them. Storage endpoints must be reachable from the container.

```bash
docker run --rm \
  --mount type=bind,src=/absolute/private-inputs,dst=/run/demo-inputs,readonly \
  "$DEMO_IMAGE" /opt/spark-milvus/demo/run.sh python 04_exact_search.py \
  --options /run/demo-inputs/read-options.json --queries /run/demo-inputs/queries.json
```

The [live validation procedure](../demo/live-validation.md) describes preparing a
disposable Milvus collection and obtaining its snapshot path.

## Registry and Kubernetes deployment

The [2.0.0 image](https://hub.docker.com/r/milvusdb/spark-milvus) is available as
`milvusdb/spark-milvus:2.0.0-spark4.0`, a multi-architecture tag over
`-amd64` and `-arm64` platform images built from the released Linux assemblies.
The [release guide](../docs/releasing.md#publish-the-container-image)
describes the publication jobs and the environment they run in. Use a published
tag, or its registry digest, for deployments that do not need a private build.

For a private build, choose an image repository its nodes can pull, such as
`registry.example/team/spark-milvus-demo`, and a versioned tag. Pass that complete
reference to `--image`; add `--push` only when you intend to publish it. The
helper never logs in to a registry or pushes implicitly.

For reproducible builds, `--base-image` accepts a verified digest reference
such as `spark@sha256:<base-digest>`. After publishing the result, use its registry
digest for deployment. A local image ID is not the registry manifest digest.

The assembly, base image and target nodes must agree on architecture. Build
`linux/amd64` and `linux/arm64` images from their corresponding native assemblies;
`--platform` selects an image target and does not cross-compile native code.
Use distinct tags for separate architectures unless you publish a multi-platform
manifest yourself.

Use the [Helm deployment templates](../deploy/helm/spark-milvus-demo/) to
submit Python or Java demos to Kubernetes. Set `image.repository` and
`image.tag`, or `image.digest` for a pinned result, together with the matching
node architecture and any `image.pullSecrets`. Snapshot examples use an existing
Secret for read options and queries. The
[cluster setup guide](../docs/cluster-setup.md) covers service accounts,
storage identity, networking and resource configuration.

## Recorded validation

On 2026-10-27 the image was built from the assembly at connector commit
`b772cc5f`, using the pinned Spark 4.0.1 base recorded in the
[validation report](../demo/validation/image-2026-10-27.json). Python and Java read,
exact, filtered and persisted-index examples passed against the committed sample.
Python exact search and the precompiled Java index example also passed with two
separate executor JVMs using `local-cluster[2,1,2048]`, UID 185, dropped
capabilities and a read-only root filesystem with writable runtime volumes.
Both indexed segments were used and recall at k=3 was 1.0 for both queries.

This validation used the image's local sample; it did not contact Milvus, publish
an image to a registry or deploy to Kubernetes. The Helm chart was validated
separately by strict lint and its offline render tests.
