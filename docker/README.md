# Build and run the connector image

The image packages a caller-supplied Spark 4.0 / Scala 2.13 assembly and the
public reader demos. It uses Java 21 and contains the Storage native bundle
for one Linux architecture. Building an image does not publish it.

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
repository. The previous 2.0 GitHub release is not a download source for this
module layout; use artifacts built from the current version.
