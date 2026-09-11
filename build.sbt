import scala.sys.process.Process

import xerial.sbt.Sonatype._

ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
import Dependencies._

lazy val snapshotRepositoryUrl = sys.env.getOrElse(
  "MAVEN_SNAPSHOT_REPOSITORY_URL",
  "https://central.sonatype.com/repository/maven-snapshots/"
)

lazy val mavenCredentialsFile = file(sys.env.getOrElse(
  "MAVEN_CREDENTIALS_FILE",
  (Path.userHome / ".sbt" / "sonatype_central_credentials").getAbsolutePath
))

// Keep the legacy Sonatype path as a local fallback while CI supplies an
// explicit credentials file for the selected Maven repository.
credentials += {
  if (mavenCredentialsFile.exists) Credentials(mavenCredentialsFile)
  else {
    Credentials(Path.userHome / ".sbt" / "sonatype.credentials")
  }
}

ThisBuild / organizationName := "zilliz"
ThisBuild / organizationHomepage := Some(url("https://zilliz.com/"))
// For cross-compiling (if applicable)
// crossScalaVersions := Seq("2.12.x", "2.13.x")
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / description := "Milvus Spark Connector to use in Spark ETLs to populate a Milvus vector database."
ThisBuild / versionScheme := Some("early-semver")

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true

ThisBuild / publishTo := {
  if (isSnapshot.value) Some("maven-snapshots" at snapshotRepositoryUrl)
  else localStaging.value
}

ThisBuild / licenses := List(
  "Server Side Public License v1" -> new URL(
    "https://raw.githubusercontent.com/mongodb/mongo/refs/heads/master/LICENSE-Community.txt"
  ),
  "GNU Affero General Public License v3 (AGPLv3)" -> new URL(
    "https://www.gnu.org/licenses/agpl-3.0.txt"
  )
)
ThisBuild / homepage := Some(
  url("https://github.com/zilliztech/milvus-spark-connector")
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/zilliztech/milvus-spark-connector"),
    "scm:git@github.com:zilliztech/milvus-spark-connector.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "santiago-wjq",
    name = "Santiago Wu",
    email = "santiago.wu@zilliz.com",
    url = url("https://github.com/santiago-wjq")
  )
)

lazy val arch = System.getProperty("os.arch") match {
  case "amd64" | "x86_64" => "amd64"
  case "aarch64" | "arm64" => "arm64"
  case other => other
}

// Get git branch name from env var (for Docker builds) or git command, sanitize for Maven version
lazy val gitBranch = {
  val branch = sys.env.getOrElse("GIT_BRANCH",
    scala.util.Try(Process("git rev-parse --abbrev-ref HEAD").!!.trim).getOrElse("unknown")
  )
  // Replace invalid characters for Maven version (only alphanumeric, dash, dot, underscore allowed)
  branch.replaceAll("[^a-zA-Z0-9._-]", "-")
}

lazy val milvusProtoDir =
  Def.setting((ThisBuild / baseDirectory).value / "milvus-proto" / "proto")

lazy val IntegrationTest = config("it") extend Test

// Shared JVM/native setup required by both the unit-test and integration-test
// source sets (native library loading, Arrow JNI access, JNI thin JAR).
// javaOptions uses `:=` (not `+=`): sbt has no config-scoped default for it, so
// under `it extend Test` a `+=` would delegate to the already-populated
// `Test / javaOptions` and append the list a second time (see the testOptions
// note below).
lazy val nativeTestSettings: Seq[Setting[_]] = Seq(
  fork := true,
  parallelExecution := true,
  logBuffered := false,
  javaOptions := Seq(
    "-Xss2m",
    "-Xmx4g",
    s"-Djava.library.path=${(baseDirectory.value / "src/main/resources/native").getAbsolutePath}",
    "-Dlog4j2.configurationFile=log4j2.properties",
    "-Dlog4j2.debug=false",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
  ),
  envVars := Map(
    "LD_LIBRARY_PATH" -> (baseDirectory.value / "src/main/resources/native").getAbsolutePath
  ),
  unmanagedJars += baseDirectory.value / "milvus-storage" / "java" / "target" / "scala-2.13" / "milvus-storage-jni_2.13-0.1.0-SNAPSHOT.jar"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .aggregate(v2Modules: _*)
  // Migration-time: every source file now lives in a module, and the root
  // project only assembles the fat jar. The artifact name and publish
  // coordinates are identical to 1.x.
  .dependsOn(spark40, apps40)
  .settings(
    // The 2.0 modules only follow along for compile and test. assembly and
    // publish are not delegated to subprojects, so the Docker build and the
    // release flow behave exactly as they did in 1.x.
    assembly / aggregate := false,
    publish / aggregate := false,
    publishLocal / aggregate := false,
    // The display name follows the repository directory, so only the meta-build
    // is called spark-milvus-build. The publish coordinate stays
    // spark-connector because that is what spark-data-service pins in the
    // cloud. sbt-assembly derives the jar name from `name` by default, so pin
    // it here or the Dockerfile will not find the artifact.
    name := "spark-milvus",
    moduleName := "spark-connector",
    assembly / assemblyJarName := s"spark-connector-assembly-${version.value}.jar",
    assembly / parallelExecution := true,
    assembly / assemblyPackageScala / assembleArtifact := false,
    Compile / compile / parallelExecution := true,
    version := s"2.0.0-${gitBranch}-${arch}-SNAPSHOT",
    organization := "com.zilliz",

    // Disable Scaladoc and sources jar for publish (not needed, speeds up build)
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false,

    // Fork JVM for run to properly load native libraries
    run / fork := true,

    // Unit tests: fast, no external services. `-W` enables ScalaTest's
    // slowpoke detection: it emits an alert if a test exceeds the threshold,
    // but it never fails, cancels or interrupts a test — it is a signal, not
    // a hard timeout. `:=` (not `+=`) keeps the option list config-local.
    inConfig(Test)(nativeTestSettings ++ Seq(
      testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oDF", "-W", "10", "10"))
    )),

    // Integration tests: need Milvus server (:19530) + MinIO (:9000). Same
    // slowpoke detection with a larger threshold. `:=` is required: because
    // `it extend Test`, a `+=` would delegate through to `Test / testOptions`
    // and concatenate both `-W 10 10` and `-W 600 600`, and ScalaTest reads
    // only the first — silently downgrading integration tests to 10s.
    // Defaults.itSettings points the source/resource dirs at src/it/* (otherwise
    // the config would inherit Test's src/test dirs via `extend Test`).
    inConfig(IntegrationTest)(Defaults.itSettings ++ nativeTestSettings ++ Seq(
      testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oDF", "-W", "600", "600"))
    )),

    // JVM options for run
    run / javaOptions ++= Seq(
      "-Xss2m",
      "-Djava.library.path=.",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),

    run / envVars := Map(
      "LD_PRELOAD" -> (baseDirectory.value / s"src/main/resources/native/libmilvus-storage.so").getAbsolutePath
    ),

    // Include test dependencies in run classpath for example applications
    Compile / run / fullClasspath := (Compile / run / fullClasspath).value ++ (Test / fullClasspath).value,

    // Add milvus-storage JNI library as unmanaged dependency
    Compile / unmanagedJars += baseDirectory.value / "milvus-storage" / "java" / "target" / "scala-2.13" / "milvus-storage-jni_2.13-0.1.0-SNAPSHOT.jar",

    // The old log bindings (slf4j-log4j12 / reload4j / log4j 1.x) collide with
    // Spark's log4j2, so exclude them globally.
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("org.slf4j", "slf4j-reload4j"),
      ExclusionRule("log4j", "log4j"),
      ExclusionRule("ch.qos.reload4j", "reload4j")
    ),
    // Filter the slf4j-api jar out at assembly time. It stays available at
    // compile time through transitive dependencies but never enters the fat
    // jar; at runtime the Spark image provides
    // /opt/spark/jars/slf4j-api-2.x.jar. Bundling it loads Logger twice under
    // spark.executor.userClassPathFirst=true and raises LinkageError.
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { f =>
        val n = f.data.getName
        n.startsWith("slf4j-api-")
      }
    },
    libraryDependencies ++= Seq(
      munit % Test,
      scalaTest % Test,
      grpcNetty,
      scalapbRuntime % "protobuf",
      scalapbRuntimeGrpc,
      scalapbCompilerPlugin,
      sparkCore,
      sparkSql,
      sparkCatalyst,
      sparkMLlib,
      parquetHadoop,
      parquetAvro,
      avro,
      hadoopCommon,
      hadoopAws,
      hadoopAliyun,
      awsSdkS3,
      awsSdkS3Transfer,
      awsSdkCore,
      jacksonScala,
      jacksonDatabind,
      arrowFormat,
      arrowVector,
      arrowMemoryCore,
      arrowMemoryNetty,
      arrowCData
    ),
    // Proto generation moved to core (messages) and client (services); the root
    // project only consumes what they produce.
    Compile / packageBin / mappings ++= {
      val bases = Seq(
        (core / Compile / PB.targets).value.head.outputPath,
        (client / Compile / PB.targets).value.head.outputPath
      )
      bases.flatMap { base =>
        (base ** "*.scala").get.map { file =>
          file -> s"generated_protobuf/${file.relativeTo(base).getOrElse(file)}"
        }
      }
    },
    Compile / resourceDirectories += baseDirectory.value / "src" / "main" / "resources",
    // Publish the runnable assembly as the primary Maven artifact. Cloud
    // consumers already resolve this artifact without an assembly classifier.
    Compile / packageBin := assembly.value
  )

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shade_proto.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shade_googlecommon.@1").inAll
  // Note: Arrow cannot be shaded due to JNI bindings with hardcoded class names
  // Use spark.driver.userClassPathFirst=true to prioritize our Arrow version
)

assembly / assemblyMergeStrategy := {
  case PathList("native", xs @ _*) => MergeStrategy.first
  // Handle all Netty native-image files
  case PathList("META-INF", "native-image", "io.netty", _*) =>
    MergeStrategy.discard
  // Handle Netty version properties
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.discard
  // Handle mime.types
  case PathList("mime.types") =>
    MergeStrategy.filterDistinctLines
  // Handle FastDoubleParser notice
  case PathList("META-INF", "FastDoubleParser-NOTICE") =>
    MergeStrategy.discard
  // Handle Arrow git properties
  case PathList("arrow-git.properties") =>
    MergeStrategy.first
  // Handle module-info.class files
  case x if x.endsWith("module-info.class") =>
    MergeStrategy.discard
  // Handle hadoop package-info conflicts
  case PathList("org", "apache", "hadoop", xs @ _*) if xs.last == "package-info.class" =>
    MergeStrategy.first
  // Handle AWS SDK VersionInfo conflicts
  case PathList("software", "amazon", "awssdk", xs @ _*) if xs.last == "VersionInfo.class" =>
    MergeStrategy.first
  // Default case
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// import scalapb.compiler.Version
// val grpcJavaVersion =
//   SettingKey[String]("grpcJavaVersion", "ScalaPB gRPC Java version")
// grpcJavaVersion := Version.grpcJavaVersion

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.


// ---------------------------------------------------------------------------
// The 2.0 multi-module layout. docs/design/modules.md is the contract.
//
// Dependencies only point downward:
//   apps-<line> -> spark-<line> -> compat、client -> core -> native-*
// spark-base is only a source directory shared by the four lines, not a
// project. Making it a project does not remove the synthetic
// spark-base-sources module the IDE shows: that module comes from "several
// projects declare the same source root", not from "it is not a project". And
// the constraint that shared source may only use APIs present on every line is
// already enforced by spark-3.5 compiling it against the lowest line.
// ---------------------------------------------------------------------------

lazy val v2Modules: Seq[ProjectReference] = Seq(
  nativeStorage, nativeVector, core, compat, client,
  spark35, spark40, spark41, spark42,
  apps40, integration40
)

// Layer 1: the two native library wrappers. Plain Java, no Scala suffix.
lazy val nativeStorage = Project("native-storage", file("native-storage"))
  .settings(name := "native-storage",
    moduleName := "spark-milvus-native-storage",
    Modules.javaOnly)

lazy val nativeVector = Project("native-vector", file("native-vector"))
  .settings(name := "native-vector",
    moduleName := "spark-milvus-native-vector",
    Modules.javaOnly)

// Layer 2: the core layer. All computation happens here, and no source file
// mentions org.apache.spark.
lazy val core = Project("core", file("core"))
  .dependsOn(nativeStorage, nativeVector)
  .settings(
    name := "core",
    moduleName := "spark-milvus-core",
    Modules.shared,
    // Arrow is pinned by spark-<line>; core compiles against the interfaces.
    libraryDependencies ++= Seq(
      // Compiled against the interfaces only; the implementation arrives at
      // runtime at the version spark-<line> pinned.
      "org.apache.arrow" % "arrow-vector" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-memory-core" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-c-data" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-format" % Versions.line("4.0").arrow % "provided",
      // The only storage implementation goes through the Hadoop FileSystem
      // API, using the copy Spark ships at runtime.
      hadoopCommon,
      // Three formats the storage layer itself needs: snapshots and backup
      // metadata are JSON, segment manifests are Avro, delete files and column
      // groups are Parquet.
      jacksonDatabind,
      jacksonScala,
      avro,
      parquetHadoop,
      scalapbRuntime % "protobuf",
      scalaTest % Test
    ),
    // parquet-hadoop pulls in a jackson-databind newer than
    // jackson-module-scala, which throws JsonMappingException at runtime. Pin
    // them to one version.
    dependencyOverrides ++= Seq(
      jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    ),
    // The Milvus storage format is itself defined in protobuf: a snapshot
    // embeds a CollectionSchema, and the manifest's field descriptions come
    // from schema.proto. So the two service-free files are generated here with
    // grpc = false, and the ones carrying services are generated in client,
    // which reuses these outputs through its include path rather than
    // generating the same .proto twice.
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / includeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false) -> (Compile / sourceManaged).value / "scalapb"
    )
    // TODO decision 17: where Plan.g4 for Milvus expressions and the antlr
    // runtime live. Spark 3.5 ships antlr 4.9.3 and 4.x ships 4.13.1, and the
    // generated parsers are not interchangeable, so no antlr dependency yet.
  )

// Adapters for the three non-standard entry points. Each one produces a core
// Snapshot or SegmentReader.
lazy val compat = Project("compat", file("compat"))
  .dependsOn(core)
  .settings(
    name := "compat",
    moduleName := "spark-milvus-compat",
    Modules.shared,
    libraryDependencies ++= Seq(
      hadoopCommon,
      jacksonDatabind,
      jacksonScala,
      avro,
      parquetHadoop,
      // The tests write real parquet with parquet-mr's ExampleParquetWriter,
      // which needs FileOutputFormat. The production path does not, so this
      // stays in Test scope.
      hadoopMapreduceClientCore % Test,
      scalaTest % Test
    ),
    dependencyOverrides ++= Seq(
      jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    )
  )

// The client for the online Milvus service: the calls behind DDL, delete and
// the procedures.
lazy val client = Project("client", file("client"))
  .dependsOn(core)
  .settings(
    name := "client",
    moduleName := "spark-milvus-client",
    Modules.shared,
    libraryDependencies ++= Seq(
      grpcNetty,
      scalapbRuntime % "protobuf",
      scalapbRuntimeGrpc,
      jacksonDatabind,
      jacksonScala,
      scalaTest % Test
    ),
    dependencyOverrides ++= Seq(
      jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    ),
    // common.proto and schema.proto are generated in core; here they only go on
    // the include path, so the generated service stubs reference the message
    // classes core already produced.
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / excludeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

// Layer 3: one project per Spark line, all sharing the spark-base sources.
def sparkProject(l: Versions.SparkLine): Project =
  Project(l.projectId, file(s"spark-${l.id}"))
    .dependsOn(core, compat, client)
    .settings(
      name := s"spark-${l.id}",
      moduleName := s"spark-milvus-${l.id}",
      Modules.perLine(l),
      Compile / unmanagedSourceDirectories +=
        Modules.sharedSource((ThisBuild / baseDirectory).value, "spark-base"),
      Compile / unmanagedResourceDirectories +=
        (ThisBuild / baseDirectory).value / "spark-base" / "src" / "main" / "resources",
      libraryDependencies ++=
        Modules.sparkDeps(l) ++ Modules.arrowDeps(l) ++ Modules.legacyDeps(l),
      libraryDependencies += scalaTest % Test,
      inConfig(Test)(Modules.nativeTest),
      // Migration-time: the 1.x write path still uses the upstream
      // milvus-storage Java binding, so until native-storage replaces it all
      // four lines need this unmanaged jar.
      Compile / unmanagedJars += (ThisBuild / baseDirectory).value /
        "milvus-storage" / "java" / "target" / "scala-2.13" /
        "milvus-storage-jni_2.13-0.1.0-SNAPSHOT.jar",
      // The fat jar is a task on this same project, published with a
      // classifier. No separate bundle module is needed; that is a Maven
      // limitation, not an sbt one.
      // TODO shade rules per constraint 6 in section 4 of modules.md: relocate
      // protobuf and guava only, never com.zilliz.milvus.jni.** or
      // org.apache.arrow.**.
      assembly / assemblyJarName := s"spark-milvus-${l.id}-bundle.jar",
      assembly / artifact := (assembly / artifact).value.withClassifier(Some("bundle"))
    )

lazy val spark35 = sparkProject(Versions.line("3.5"))
lazy val spark40 = sparkProject(Versions.line("4.0"))
lazy val spark41 = sparkProject(Versions.line("4.1"))
lazy val spark42 = sparkProject(Versions.line("4.2"))

// Layer 4: the entry points users run, shipped as one fat jar. The four
// packages do not depend on each other. The sources sit directly in this line's
// directory with no shared base: with a single consumer, a shared directory is
// just one more level.
// The module is not called ops because an internal system already has that
// name.
def appsProject(l: Versions.SparkLine, sparkLine: Project): Project =
  Project(s"apps${l.projectId.stripPrefix("spark")}", file(s"apps-${l.id}"))
    .dependsOn(sparkLine)
    .settings(
      name := s"apps-${l.id}",
      moduleName := s"spark-milvus-apps-${l.id}",
      Modules.perLine(l),
      libraryDependencies ++=
        Modules.sparkDeps(l) ++ Modules.arrowDeps(l) ++ Modules.legacyDeps(l),
      libraryDependencies += scalaTest % Test,
      inConfig(Test)(Modules.nativeTest),
      Compile / unmanagedJars += (ThisBuild / baseDirectory).value /
        "milvus-storage" / "java" / "target" / "scala-2.13" /
        "milvus-storage-jni_2.13-0.1.0-SNAPSHOT.jar"
    )

// Built only for the line that runs in the cloud; another line is one more
// line of configuration.
lazy val apps40 = appsProject(Versions.line("4.0"), spark40)


// Integration tests. They need MinIO and Milvus, are never published, and live
// in this line's src/test/scala.
// The module is not called it: sbt's built-in IntegrationTest configuration was
// deprecated in 1.9 and removed in sbt 2, 2.0 does not use it, and reusing the
// word would mislead.
def integrationProject(l: Versions.SparkLine, sparkLine: Project, appsLine: Project): Project =
  Project(s"integration${l.projectId.stripPrefix("spark")}", file(s"integration-${l.id}"))
    .dependsOn(sparkLine, appsLine)
    .settings(
      name := s"integration-${l.id}",
      moduleName := s"spark-milvus-integration-${l.id}",
      Modules.perLine(l),
      libraryDependencies ++= Modules.sparkDeps(l).map(_.withConfigurations(Some("test"))),
      libraryDependencies += scalaTest % Test,
      inConfig(Test)(Modules.nativeTest)
    )

// Integration tests need a real Milvus and MinIO, so one line is enough; add
// another when a line shows a problem of its own.
lazy val integration40 = integrationProject(Versions.line("4.0"), spark40, apps40)
