# Application development

This guide covers running applications that use Spark-Milvus and building the
connector from source. Start with the README's
[installation instructions](../README.md#installing)
and [Collection read](../README.md#reading-collection-data) and
[write](../README.md#writing-collection-data) examples. The commands below use
the `read_milvus.py` and `ReadMilvus.scala` applications from those examples.

## Running your application

### Resolve dependencies with `--packages`

Spark resolves the selected entry and its transitive dependencies from Maven repositories.
For a first run on a Linux x86_64 machine with Spark 4.0 installed, run the
PySpark example locally:

```bash
export RELEASE_VERSION=3.0.0
export LD_PRELOAD="$JAVA_HOME/lib/libjsig.so"

"$SPARK_HOME/bin/spark-submit" \
  --master 'local[2]' \
  --packages "com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:$RELEASE_VERSION" \
  read_milvus.py
```

For the Scala example, use the same dependency and runtime settings, replacing
`read_milvus.py` with `--class ReadMilvus /path/to/your-application.jar`.

For a cluster run, replace `local[2]` with your cluster's master and set
`--conf spark.executorEnv.LD_PRELOAD=/path/on/workers/to/jdk/lib/libjsig.so`.
Each JVM must preload its own JRE's signal library before startup. The export
above configures a Linux client-mode driver; a remote driver needs the setting
in its own environment too. On macOS, use the `darwin-aarch64` artifact and
that JRE's `lib/libjsig.dylib` through
`DYLD_INSERT_LIBRARIES` on the JVM. The native loader also needs a writable
extraction directory that permits shared-library loading. See
[cluster setup](cluster-setup.md#prepare-every-driver-and-executor) for
worker identities, networking, memory and native runtime configuration.

### Use local JARs with `--jars`

If your cluster cannot resolve Maven dependencies, obtain these two artifacts
from [Maven Central](https://repo.maven.apache.org/maven2/com/zilliz/) at the
same published version and put them under `lib/`:

- `spark-milvus-4.0-jvm_2.13`: the bundled JVM implementation.
- `spark-milvus-native-linux-x86_64`: the matching native platform libraries.

Keep the signal-library settings above and replace `--packages` with:

```bash
--jars "lib/spark-milvus-4.0-jvm_2.13-${RELEASE_VERSION}.jar,lib/spark-milvus-native-linux-x86_64-${RELEASE_VERSION}.jar"
```

Spark distributes the JARs to the driver and executors. The universal and
platform-specific entry JARs rely on their Maven dependencies; passing only an
entry JAR to `--jars` does not install the connector. Thin deployments using
`--jars` also need their declared JVM dependencies; see
[artifact selection](release-notes/3.0.md#maven-dependencies).

## Building the connector from source

To develop the connector itself or build an assembly from source, follow
[Contributing](contributing.md). It covers submodules, the Java 21 build,
native libraries, formatting and tests.
