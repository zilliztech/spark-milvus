# Build and run the connector image

The image packages a caller-supplied Spark 4.0 / Scala 2.13 assembly and the
public reader demos. It uses Java 21 and contains the Storage native bundle
for one Linux architecture. Building an image does not publish it.

## Run a release image

These commands require a 3.0 version published to Docker Hub. Choose a version
from [GitHub Releases](https://github.com/zilliztech/spark-milvus/releases) and
replace `<published-3.0-version>` below; a CI candidate alone does not publish
an image. The image tag
`milvusdb/spark-milvus:<version>-spark4.0` selects Linux amd64 or arm64 to match
the Docker host. Architecture-specific tags end in `-amd64` or `-arm64`;
`latest` tracks final releases only.

Create `read-options.json` from the [S3 options template](../demo/config/s3-options.json)
with your existing snapshot path and object-storage configuration. Supply the
configured storage identity inside the container too; a host-only credential
file is not available there automatically.

```bash
export RELEASE_VERSION='<published-3.0-version>'
image="milvusdb/spark-milvus:$RELEASE_VERSION-spark4.0"
docker pull "$image"
docker run --rm \
  --mount "type=bind,src=$PWD/read-options.json,dst=/run/read-options.json,readonly" \
  "$image" /opt/spark-milvus/demo/run.sh python 01_read_snapshot.py \
  --options /run/read-options.json
```

This read example inspects snapshots of up to 10,000 visible rows. Use your own
Spark application for larger workloads. For cluster execution, configure access
on every driver and executor as described in [cluster setup](../docs/cluster-setup.md).

## Build an image

```bash
./docker/build.sh \
  --assembly /absolute/path/to/connector-assembly.jar \
  --image spark-milvus-demo:local \
  --platform linux/amd64
```

Use `linux/arm64` with an aarch64 assembly. The script validates the provider,
platform manifest and Storage JNI libraries, prints the assembly checksum,
and stages an explicit list of public files in a temporary build context.
An existing incompatible assembly is rejected. `--push` opts into registry
publication; use a registry and tag you control.

The default base is `spark:4.0.1-scala2.13-java21-python3-ubuntu`. The image
contains `/opt/spark-milvus/connector.jar`, the Python examples, precompiled
Java examples, the fixed V3 read sample and checksum verifier. Spark runs as
UID 185. Java signal chaining and writable temporary directories are required
on both driver and executor.

```bash
docker run --rm spark-milvus-demo:local \
  /opt/spark-milvus/demo/run.sh python 01_read_snapshot.py
docker run --rm spark-milvus-demo:local \
  /opt/spark-milvus/demo/run.sh java FilterProject --explain
```

For your own snapshot, mount a private options JSON and pass `--options`.
All referenced storage objects must be reachable by every executor. See
[cluster setup](../docs/cluster-setup.md) for runtime identity and permissions,
and the [Helm chart](../deploy/helm/spark-milvus-demo/README.md) for Kubernetes.

CI builds the image from the exact candidate assembly on both Linux
architectures and runs Python reads and Java projection checks before
publication. The release workflow permits publication only in the upstream
repository.
