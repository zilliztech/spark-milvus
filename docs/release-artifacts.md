# Choosing release artifacts

Use the connector artifact for your Spark line and Scala binary version, and
use the same connector version for every JVM and native artifact. Spark itself
is supplied by your application or cluster. The
[README installation examples](../README.md#installing) use Spark 4.0,
Scala 2.13 and Java 21.

## Published 3.0.0

The examples in this guide use artifacts already published to Maven Central.
The available packaging differs by Spark line:

| Spark | Scala | Available in 3.0.0 |
|---|---|---|
| 3.5 | 2.12 or 2.13 | Legacy packaging; see the versioned guide below. Linux ARM64 is not supported. |
| 4.0 | 2.13 | Universal, platform-specific and bundled JVM artifacts |
| 4.1 | 2.13 | Legacy packaging; see the versioned guide below. |
| 4.2 | 2.13 | Universal, platform-specific and bundled JVM artifacts |

For the existing Spark 3.5 and 4.1 artifacts, consult the
[3.0.0 artifact guide](https://github.com/zilliztech/spark-milvus/blob/v3.0.0/docs/releasing.md#using-release-artifacts).
Those published artifacts remain available. New releases will use the unified
packaging described below instead of publishing Thin modules.

**Spark 3.5 on Linux ARM64:** both Scala variants in 3.0.0 depend on Arrow
12.0.1, whose C Data JAR lacks the Linux ARM64 JNI library. Adding the Milvus
ARM64 native package does not supply that Arrow library, so this combination
cannot perform connector reads or writes. The next-release build supplements
the missing library; this does not change the published 3.0.0 artifacts.

## Choose a dependency entry or local JARs

These coordinates are for Spark 4.0 / Scala 2.13, connector version 3.0.0:

| Maven coordinate | Contents and use |
|---|---|
| `com.zilliz:spark-milvus-4.0_2.13:3.0.0` | Universal entry: resolves the bundled JVM artifact and all three native platforms. The native loader selects the current platform. |
| `com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:3.0.0` | Platform entry: resolves the bundled JVM artifact and only the Linux x86_64 native package. |
| `com.zilliz:spark-milvus-4.0-jvm_2.13:3.0.0` | Bundled JVM implementation and its JVM dependencies, without Milvus native libraries or Spark. |
| `com.zilliz:spark-milvus-native-linux-x86_64:3.0.0` | Milvus Storage native libraries for Linux x86_64; shared across Spark and Scala profiles. |

For Spark 4.2, replace `4.0` with `4.2`. For a different platform, replace
`linux-x86_64` with its platform ID from the table below. Use Maven, Gradle or
sbt to resolve an entry and its dependencies, as shown in the README.

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
The Spark 3.5 / Linux ARM64 restriction above still applies to version 3.0.0.

Every driver and executor must meet the platform requirements, load its own
JRE's signal library and have a writable native extraction directory that
permits shared-library loading. Match Java to your Spark distribution; the
next-release consumer matrix uses Java 17 for Spark 3.5 and Java 21 for Spark
4.x. See [cluster setup](cluster-setup.md) for runtime configuration.

## Next release: unified packaging (unreleased)

The next release provides universal, platform-specific and bundled JVM
artifacts for every supported combination:

| Spark | Scala binary versions |
|---|---|
| 3.5 | 2.12, 2.13 |
| 4.0, 4.1, 4.2 | 2.13 |

The universal artifact naming pattern is
`com.zilliz:spark-milvus-<spark-line>_<scala-binary>:<published-version>`;
platform and JVM entries follow the patterns illustrated above. These new
Spark 3.5 and 4.1 entries are **not available at version 3.0.0**. Select a version
only after it appears in [GitHub Releases](https://github.com/zilliztech/spark-milvus/releases)
and Maven Central. The next-release Spark 3.5 bundles also include the missing
Linux ARM64 Arrow JNI library.
