# Choosing release artifacts

Use the connector artifact for your Spark line and Scala binary version, and
use the same connector version for every JVM and native artifact. Spark itself
is supplied by your application or cluster. The
[README installation examples](../README.md#installing) use Spark 4.0,
Scala 2.13 and Java 21.

## Published 3.0.0

All five combinations below are available from Maven Central at version
**3.0.0**, with universal, platform-specific and bundled JVM artifacts.

| Spark | Scala | Universal Maven coordinate |
|---|---|---|
| 3.5 | 2.12 | [`com.zilliz:spark-milvus-3.5_2.12:3.0.0`](https://repo.maven.apache.org/maven2/com/zilliz/spark-milvus-3.5_2.12/3.0.0/) |
| 3.5 | 2.13 | [`com.zilliz:spark-milvus-3.5_2.13:3.0.0`](https://repo.maven.apache.org/maven2/com/zilliz/spark-milvus-3.5_2.13/3.0.0/) |
| 4.0 | 2.13 | [`com.zilliz:spark-milvus-4.0_2.13:3.0.0`](https://repo.maven.apache.org/maven2/com/zilliz/spark-milvus-4.0_2.13/3.0.0/) |
| 4.1 | 2.13 | [`com.zilliz:spark-milvus-4.1_2.13:3.0.0`](https://repo.maven.apache.org/maven2/com/zilliz/spark-milvus-4.1_2.13/3.0.0/) |
| 4.2 | 2.13 | [`com.zilliz:spark-milvus-4.2_2.13:3.0.0`](https://repo.maven.apache.org/maven2/com/zilliz/spark-milvus-4.2_2.13/3.0.0/) |

Each combination supports Linux x86_64, Linux ARM64 and Apple silicon. The
Spark 3.5 bundled JVM artifacts include the Arrow JNI supplement required on
Linux ARM64 for both Scala variants; no separate Arrow supplement is needed
when using these published packages.

Use these universal or platform-specific packages for new integrations. The
previously published Thin artifacts remain available with their original
contents; their historical setup is documented in the
[original 3.0.0 guide](https://github.com/zilliztech/spark-milvus/blob/v3.0.0/docs/releasing.md#using-release-artifacts).
Do not combine Thin modules with bundled JVM or assembly JARs: they contain
overlapping connector classes.

## Choose a dependency entry or local JARs

These coordinates are for Spark 4.0 / Scala 2.13, connector version 3.0.0:

| Maven coordinate | Contents and use |
|---|---|
| `com.zilliz:spark-milvus-4.0_2.13:3.0.0` | Universal entry: resolves the bundled JVM artifact and all three native platforms. The native loader selects the current platform. |
| `com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:3.0.0` | Platform entry: resolves the bundled JVM artifact and only the Linux x86_64 native package. |
| `com.zilliz:spark-milvus-4.0-jvm_2.13:3.0.0` | Bundled JVM implementation and its JVM dependencies, without Milvus native libraries or Spark. |
| `com.zilliz:spark-milvus-native-linux-x86_64:3.0.0` | Milvus Storage native libraries for Linux x86_64; shared across Spark and Scala profiles. |

Replace `4.0` with `3.5`, `4.1` or `4.2` for your Spark line. For Spark 3.5
with Scala 2.12, also replace `_2.13` with `_2.12`. For a different platform,
replace `linux-x86_64` with its platform ID from the table below. Use Maven,
Gradle or sbt to resolve an entry and its dependencies, as shown in the README.

An entry JAR does not contain the implementation: copying it alone cannot
install the connector. For a deployment using local JARs, obtain the bundled
JVM JAR and exactly one matching native platform JAR at the same version from
[Maven Central](https://repo.maven.apache.org/maven2/com/zilliz/). Do not combine
these with an assembly JAR containing the same classes. See
[application development](application-development.md) for running an application
with resolved dependencies or local JARs, and for building from source.

## Native platforms and runtime requirements

| Platform ID | System requirements |
|---|---|
| `linux-x86_64` | Linux x86_64; glibc 2.35+ and GCC 12's C++ runtime or newer |
| `linux-aarch64` | Linux ARM64; glibc 2.35+ and GCC 12's C++ runtime or newer |
| `darwin-aarch64` | Apple silicon; macOS 14+ |

Linux requires `libstdc++` with `GLIBCXX_3.4.30` and `CXXABI_1.3.13`; this
system library is not bundled. Linux binaries are tested on Ubuntu 22.04 and
24.04. Prebuilt native packages do not cover Windows, musl/Alpine or Intel Macs.

Every driver and executor must meet the platform requirements, load its own
JRE's signal library and have a writable native extraction directory that
permits shared-library loading. Use Java 17 for Spark 3.5 and Java 21 for
Spark 4.x in the configurations documented here. See
[cluster setup](cluster-setup.md) for runtime configuration.

## Build provenance

On November 16, 2026, the 3.0.0 inventory was extended with 15 new coordinates:
a bundled JVM artifact, a universal entry and three platform entries for each
of Spark 3.5 / Scala 2.12, Spark 3.5 / Scala 2.13 and Spark 4.1 / Scala 2.13.
These artifacts were built from main commit
[`a64a831`](https://github.com/zilliztech/spark-milvus/commit/a64a831c2d511dd32cbeed77804f4192fc04f69c),
which their POM SCM metadata records.

The original `v3.0.0` tag and `3.0` release branch were not moved for this
publication. The existing Spark 4.0 and 4.2 packages, all three Milvus native
packages and the legacy Thin artifacts were not overwritten. Use each
artifact's POM SCM metadata to identify its build source; the original tag
does not contain the build changes for the 15 added coordinates.
