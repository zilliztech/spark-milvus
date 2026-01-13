# Milvus Spark Connector

## Version Compatibility

**This connector requires Milvus 2.6 or later** (Storage V2).

For Milvus 2.5 and earlier versions, please use the `legacy` branch which is no longer actively maintained.

## Environment Preparation

**Machine Requirements:** Minimum 2 CPU cores and 8GB RAM.

To ensure smooth operation, it is critical to use consistent versions of the required tools. Mismatched versions may lead to compatibility issues.

1. [**SDKMAN**](https://sdkman.io/) is recommended for managing Scala and Spark environments.
2. **Java Version:** 21
3. **Scala Version:** 2.13.16
4. **Spark Version:** 4.0.1 (built with Scala 2.13)
5. **SBT Version:** 1.11.1

If you are using SDKMAN, you can quickly install Java, Scala, and SBT as follows:

```bash
sdk install java 21.0.5-zulu
sdk install scala 2.13.16
sdk install sbt 1.11.1
```

The Spark version provided by SDKMAN only supports Scala 2.12. Therefore, you need to manually install the Spark version compatible with Scala 2.13. You can download it from the following link: [Spark Download](https://www.apache.org/dyn/closer.lua/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3-scala2.13.tgz).

### Spark Submit Configuration

To simplify the `spark-submit` process, create a wrapper script named `spark-submit-wrapper.sh`. Replace the `SPARK_HOME` path with the actual installation directory on your machine.

```bash
#!/bin/bash

export SPARK_HOME=/xxx/spark-4.0.1-bin-hadoop3-scala2.13

if [ ! -d "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME directory does not exist: $SPARK_HOME"
  exit 1
fi

SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
if [ ! -f "$SPARK_SUBMIT" ]; then
  echo "Error: spark-submit not found at: $SPARK_SUBMIT"
  exit 1
fi

exec "$SPARK_SUBMIT" "$@"
```

After saving the script, grant it execution permissions and set up an alias for convenience. Replace the script path with the actual location on your machine.

```bash
chmod +x /xxx/spark-submit-wrapper.sh
alias spark-submit-wrapper="/xxx/spark-submit-wrapper.sh"
```

## Running the Connector

### Add Milvus Spark Connector Dependency

You can use the pre-built Milvus Spark Connector package directly, or compile it yourself by following the instructions in the next section.

**Note:** The official release package is currently used primarily for testing the release process. Active development and updates are concentrated in the SNAPSHOT versions.

- **Official Release:** Available at [Maven Repository](https://mvnrepository.com/artifact/com.zilliz/spark-connector_2.13)
- **Latest SNAPSHOT:** Version 0.1.7-SNAPSHOT

#### Using SNAPSHOT Dependencies

To use the SNAPSHOT version, you need to add the snapshot repository to your build configuration:

**For SBT (build.sbt):**

```
ThisBuild / resolvers += "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"
```

### Build and Package Milvus Spark Connector

Use the following SBT commands to compile, package, and publish the connector locally:

```bash
sbt clean compile package publishLocal
```

- **clean:** Clears previous build artifacts.
- **compile:** Compiles the source code.
- **package:** Packages the compiled code into a JAR file.
- **publishLocal:** Publishes the package to the local repository (primarily for use in connector examples).

To create a fat JAR containing all dependencies, run:

```bash
sbt assembly
```

### Docker Build

You can also build the connector using Docker, which handles all dependencies automatically including the native milvus-storage library.

**Build the Docker image:**

```bash
# Build for current architecture (x86 or ARM64)
docker build -t spark-milvus .

# Build without publishing to Maven Central
docker build --build-arg PUBLISH_TO_CENTRAL=false -t spark-milvus .
```

**Build arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `GIT_BRANCH` | `unknown` | Branch name for version string |
| `PUBLISH_TO_CENTRAL` | `true` | Whether to publish to Maven Central Snapshots |

**Version naming:**

The Docker build uses dynamic versioning: `{branch}-{arch}-SNAPSHOT`

For example:
- `main-amd64-SNAPSHOT` (x86 build from main branch)
- `feature-arm64-SNAPSHOT` (ARM64 build from feature branch)

**Extract built JAR from container:**

```bash
docker create --name temp spark-milvus
docker cp temp:/workspace/target/scala-2.13/spark-connector_2.13-*.jar ./
docker rm temp
```

### Build and Package Milvus Spark Connector Example

Example Project Repository: [Milvus Spark Connector Example](https://github.com/SimFG/milvus-spark-connector-example)

To compile and package the example project, use the following SBT command:

```bash
sbt clean compile package
```

### Run the Test Demo

To execute the test demo, specify the paths to the JAR files generated in the previous steps. Replace `/xxx/` with the actual paths on your machine.

**Note:** If you prefer to use online dependencies instead of building locally, you can download the pre-built assembly JAR from the [GitHub Releases page](https://github.com/SimFG/milvus-spark-connector/releases).

```bash
spark-submit-wrapper --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar --class "example.HelloDemo" /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
```

This command executes the **HelloDemo** class, showcasing how to read collection data from Milvus using the Spark connector.

**Before running this command, please ensure the following:**

* A collection named **hello_spark_milvus** already exists in your local Milvus instance.
* Your local Milvus service is running and accessible.

For more detailed information about how to use the connector, see the [ **API Reference**](docs/reference-en.md).

## License

This project is licensed under the Server Side Public License v1 (SSPLv1) and the GNU Affero General Public License v3 (AGPLv3).