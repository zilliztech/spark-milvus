import scala.sys.process.Process

import xerial.sbt.Sonatype._

import Dependencies._

// Shared defaults. Per-line Scala and Java settings live in Modules.
ThisBuild / scalaVersion := Versions.scala213
ThisBuild / versionScheme := Some("early-semver")

lazy val milvusProtoDir =
  Def.setting((ThisBuild / baseDirectory).value / "milvus-proto" / "proto")

// Root keeps the existing cloud artifact while sources live in the modules.
lazy val root = (project in file("."))
  // Live-service suites run explicitly through integration40/test.
  .aggregate(
    nativeStorage, nativeVector, core, compat, client,
    spark35, spark40, spark41, spark42, apps40
  )
  .dependsOn(spark40, apps40)
  .settings(
    name := "spark-milvus",
    moduleName := "spark-connector",
    CapabilityIndex.settings,
    Modules.legacyJni,
    Compile / compile / parallelExecution := true,
    libraryDependencies ++= Dependencies.legacyRootDeps,
    publish / skip := false,
    publish / aggregate := false,
    publishLocal / aggregate := false,
    assembly / aggregate := false,
    Compile / packageBin := assembly.value,
    rootRunSettings,
    rootAssemblySettings,
    rootPublishingSettings
  )

// Layer 1: the two native library wrappers. Plain Java, no Scala suffix.
lazy val nativeStorage = Project("native-storage", file("native-storage"))
  .settings(
    name := "native-storage",
    moduleName := "spark-milvus-native-storage",
    Modules.javaOnly,
    publish / skip := true
  )

lazy val nativeVector = Project("native-vector", file("native-vector"))
  .settings(
    name := "native-vector",
    moduleName := "spark-milvus-native-vector",
    Modules.javaOnly,
    publish / skip := true
  )

// Layer 2: the core layer. All computation happens here, and no source file
// mentions org.apache.spark.
lazy val core = Project("core", file("core"))
  .dependsOn(nativeStorage, nativeVector)
  .settings(
    name := "core",
    moduleName := "spark-milvus-core",
    Modules.sparkFreeModuleSettings,
    Modules.jacksonPin,
    publish / skip := true,
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
    // Storage schemas belong to core; service stubs in client reuse them.
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / includeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

// Adapters for the three non-standard entry points. Each one produces a core
// Snapshot or SegmentReader.
lazy val compat = Project("compat", file("compat"))
  .dependsOn(core)
  .settings(
    name := "compat",
    moduleName := "spark-milvus-compat",
    Modules.sparkFreeModuleSettings,
    Modules.jacksonPin,
    publish / skip := true,
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
    )
  )

// The client for the online Milvus service: the calls behind DDL, delete and
// the procedures.
lazy val client = Project("client", file("client"))
  .dependsOn(core)
  .settings(
    name := "client",
    moduleName := "spark-milvus-client",
    Modules.sparkFreeModuleSettings,
    Modules.jacksonPin,
    publish / skip := true,
    libraryDependencies ++= Seq(
      grpcNetty,
      scalapbRuntime % "protobuf",
      scalapbRuntimeGrpc,
      jacksonDatabind,
      jacksonScala,
      scalaTest % Test
    ),
    // Generate only service protos; import the message classes from core.
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / excludeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

// Layer 3: each Spark line compiles spark-base against its own dependencies.
def sparkProject(l: Versions.SparkLine): Project =
  Project(l.projectId, file(s"spark-${l.id}"))
    .dependsOn(core, compat, client)
    .settings(
      name := s"spark-${l.id}",
      moduleName := s"spark-milvus-${l.id}",
      Modules.perLine(l),
      publish / skip := true,
      Modules.legacyJni,
      Compile / unmanagedSourceDirectories +=
        (ThisBuild / baseDirectory).value / "spark-base" / "src" / "main" / "scala",
      Compile / unmanagedResourceDirectories +=
        (ThisBuild / baseDirectory).value / "spark-base" / "src" / "main" / "resources",
      libraryDependencies ++=
        Dependencies.sparkDeps(l) ++ Dependencies.arrowDeps(l) ++ Dependencies.legacyDeps(l),
      libraryDependencies += scalaTest % Test,
      inConfig(Test)(Modules.nativeTest),
      // Per-line bundles are not published yet; add shading before enabling them.
      assembly / assemblyJarName := s"spark-milvus-${l.id}-bundle.jar",
      assembly / artifact := (assembly / artifact).value.withClassifier(Some("bundle"))
    )

lazy val spark35 = sparkProject(Versions.line("3.5"))
lazy val spark40 = sparkProject(Versions.line("4.0"))
lazy val spark41 = sparkProject(Versions.line("4.1"))
lazy val spark42 = sparkProject(Versions.line("4.2"))

// Layer 4: apps currently run on 4.0, so their sources need no shared directory.
lazy val apps40 = Project("apps40", file("apps-4.0"))
  .dependsOn(spark40)
  .settings(
    name := "apps-4.0",
    moduleName := "spark-milvus-apps-4.0",
    Modules.perLine(Versions.line("4.0")),
    publish / skip := true,
    Modules.legacyJni,
    libraryDependencies ++=
      Dependencies.sparkDeps(Versions.line("4.0")) ++
        Dependencies.arrowDeps(Versions.line("4.0")) ++
        Dependencies.legacyDeps(Versions.line("4.0")),
    // Only apps reaches for OSS types, and only in a test that asserts the
    // Aliyun credential provider name. The four Spark lines never touch them.
    libraryDependencies += hadoopAliyun,
    libraryDependencies += scalaTest % Test,
    inConfig(Test)(Modules.nativeTest)
  )

// Integration tests use a separate project because they need live services.
lazy val integration40 = Project("integration40", file("integration-4.0"))
  .dependsOn(spark40, apps40)
  .settings(
    name := "integration-4.0",
    moduleName := "spark-milvus-integration-4.0",
    Modules.perLine(Versions.line("4.0")),
    publish / skip := true,
    libraryDependencies ++=
      Dependencies.sparkDeps(Versions.line("4.0")).map(_.withConfigurations(Some("test"))),
    libraryDependencies += scalaTest % Test,
    inConfig(Test)(Modules.nativeTest)
  )

// Root implementation details. Publication and aggregation policy stay above.
lazy val rootRunSettings: Seq[Setting[_]] = Seq(
  // Example applications need a forked JVM and the test classpath.
  run / fork := true,
  run / javaOptions ++= Seq(
    "-Xss2m",
    "-Djava.library.path=.",
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
  ),
  run / envVars := Map(
    "LD_PRELOAD" -> (baseDirectory.value / "native-storage" / "src" / "main" /
      "resources" / "native" / "libmilvus-storage.so").getAbsolutePath
  ),
  Compile / run / fullClasspath :=
    (Compile / run / fullClasspath).value ++ (Test / fullClasspath).value
)

lazy val rootAssemblySettings: Seq[Setting[_]] = Seq(
  // Keep the filename and primary Maven artifact expected by Docker and cloud jobs.
  assembly / assemblyJarName := s"spark-connector-assembly-${version.value}.jar",
  assembly / parallelExecution := true,
  assembly / assemblyPackageScala / assembleArtifact := false,

  // Preserve the generated-source mappings used by the existing root package task.
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

  // Spark supplies the log binding and slf4j-api; bundled copies conflict with it.
  excludeDependencies ++= Seq(
    ExclusionRule("org.slf4j", "slf4j-log4j12"),
    ExclusionRule("org.slf4j", "slf4j-reload4j"),
    ExclusionRule("log4j", "log4j"),
    ExclusionRule("ch.qos.reload4j", "reload4j")
  ),

  assembly / assemblyExcludedJars := {
    val cp = (assembly / fullClasspath).value
    cp.filter(_.data.getName.startsWith("slf4j-api-"))
  },

  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("com.google.protobuf.**" -> "shade_proto.@1").inAll,
    ShadeRule.rename("com.google.common.**" -> "shade_googlecommon.@1").inAll
    // Arrow JNI bindings contain hardcoded class names, so Arrow stays unshaded.
  ),

  assembly / assemblyMergeStrategy := {
    case PathList("native", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", "native-image", "io.netty", _*) =>
      MergeStrategy.discard
    case PathList("META-INF", "io.netty.versions.properties") =>
      MergeStrategy.discard
    case PathList("mime.types") =>
      MergeStrategy.filterDistinctLines
    case PathList("META-INF", "FastDoubleParser-NOTICE") =>
      MergeStrategy.discard
    case PathList("arrow-git.properties") =>
      MergeStrategy.first
    case x if x.endsWith("module-info.class") =>
      MergeStrategy.discard
    case PathList("org", "apache", "hadoop", xs @ _*) if xs.last == "package-info.class" =>
      MergeStrategy.first
    case PathList("software", "amazon", "awssdk", xs @ _*) if xs.last == "VersionInfo.class" =>
      MergeStrategy.first
    case x =>
      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

// Packaging and publishing. Only the root artifact is released during migration.
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

lazy val snapshotRepositoryUrl = sys.env.getOrElse(
  "MAVEN_SNAPSHOT_REPOSITORY_URL",
  "https://central.sonatype.com/repository/maven-snapshots/"
)

lazy val mavenCredentialsFile = file(sys.env.getOrElse(
  "MAVEN_CREDENTIALS_FILE",
  (Path.userHome / ".sbt" / "sonatype_central_credentials").getAbsolutePath
))

ThisBuild / organizationName := "zilliz"
ThisBuild / organizationHomepage := Some(url("https://zilliz.com/"))
ThisBuild / description :=
  "Milvus Spark Connector to use in Spark ETLs to populate a Milvus vector database."

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

// Docker supplies GIT_BRANCH; local builds use the current checkout.
lazy val gitBranch = {
  val branch = sys.env.getOrElse("GIT_BRANCH",
    scala.util.Try(Process("git rev-parse --abbrev-ref HEAD").!!.trim).getOrElse("unknown")
  )
  branch.replaceAll("[^a-zA-Z0-9._-]", "-")
}

lazy val rootPublishingSettings: Seq[Setting[_]] = Seq(
  organization := "com.zilliz",
  version := s"2.0.0-${gitBranch}-${arch}-SNAPSHOT",
  Compile / packageDoc / publishArtifact := false,
  Compile / packageSrc / publishArtifact := false,

  // Embedded modules remain on the build classpath but have no published artifacts.
  pomPostProcess := {
    val bundledModules = Set(
      (spark40 / organization).value ->
        s"${(spark40 / moduleName).value}_${(spark40 / scalaBinaryVersion).value}",
      (apps40 / organization).value ->
        s"${(apps40 / moduleName).value}_${(apps40 / scalaBinaryVersion).value}"
    )
    val removeBundledModules = new scala.xml.transform.RewriteRule {
      override def transform(node: scala.xml.Node): Seq[scala.xml.Node] = node match {
        case dependency: scala.xml.Elem
            if dependency.label == "dependency" && bundledModules.contains(
              (dependency \ "groupId").text -> (dependency \ "artifactId").text
            ) => scala.xml.NodeSeq.Empty
        case other => other
      }
    }
    val transformer = new scala.xml.transform.RuleTransformer(removeBundledModules)
    (pom: scala.xml.Node) => transformer.transform(pom).head
  },

  // Keep the legacy Sonatype path as a local fallback while CI supplies an
  // explicit credentials file for the selected Maven repository.
  credentials += {
    if (mavenCredentialsFile.exists) Credentials(mavenCredentialsFile)
    else {
      Credentials(Path.userHome / ".sbt" / "sonatype.credentials")
    }
  }
)
