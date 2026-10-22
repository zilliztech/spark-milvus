import java.net.URI
import scala.sys.process.Process

import xerial.sbt.Sonatype._
import Dependencies._
import ReleaseArtifacts.{
  releaseJvmArtifacts,
  releaseJvmClasspath,
  releaseJvmDocumentation,
  releaseJvmSourceMappings,
  ReleaseJvm
}
import ThinArtifacts.{releaseThinArtifacts, thinModule}

// Shared defaults. Per-line Scala and Java settings live in Modules.
ThisBuild / scalaVersion := Versions.selectedScala
ThisBuild / versionScheme := Some("early-semver")

// Runtime resources must follow every production dependency edge without
// making Compile resolve or validate the host's native bundle.
lazy val compileAndRuntime = "compile->compile;runtime->runtime"

// The root is an aggregate only. Select one Spark line per sbt invocation.
lazy val root = project
  .in(file("."))
  .aggregate(nativeRuntime, nativeStorage, nativeVector, core, spark)
  .settings(
    name := "spark-milvus",
    ReadValidation.matrixSettings,
    publish / skip := true,
    publish / aggregate := false,
    publishLocal / aggregate := false,
    assembly / aggregate := false,
    assembly := sys.error("Use spark/assembly for the selected Spark line"),
    releaseJvmArtifacts / aggregate := false,
    releaseJvmArtifacts := (spark / releaseJvmArtifacts).value,
    releaseThinArtifacts / aggregate := false,
    releaseThinArtifacts := (spark / releaseThinArtifacts).value
  )

// Layer 1: upstream native bindings. Storage's Scala API is cross-built;
// Knowhere's API is plain Java.
lazy val nativeRuntime = Project("native-runtime", file("native-runtime"))
  .settings(
    name := "native-runtime",
    moduleName := "spark-milvus-native-runtime",
    Modules.javaOnly,
    NativeBundle.settings,
    ThinArtifacts.settings("native-runtime", Versions.sharedJavaRelease),
    ReadValidation.artifactSettings,
    libraryDependencies ++= Seq(
      "junit" % "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test
    ),
    Test / fork := true,
    publish / skip := true
  )

lazy val nativeStorage = Project("native-storage", file("native-storage"))
  .dependsOn(nativeRuntime % compileAndRuntime)
  .settings(
    name := "native-storage",
    moduleName := "spark-milvus-native-storage",
    Modules.sparkFreeModuleSettings,
    ThinArtifacts.settings("native-storage", Versions.sharedJavaRelease),
    ReadValidation.artifactSettings,
    // Native libraries reach the build only through the unified bundle. A
    // native/ directory here is left by the storage-only build that copied
    // libraries into the sources; packaging it would put a second, older copy
    // of the storage libraries on the classpath.
    Compile / unmanagedResources := {
      val stale = (Compile / resourceDirectory).value / "native"
      require(
        !stale.exists,
        s"Delete $stale: it holds libraries from the removed storage-only native build"
      )
      (Compile / unmanagedResources).value
    },
    // Compile the pinned upstream API against this build's Scala version.
    // Importing upstream's sbt project would also import its Spark/Arrow pins.
    Compile / sourceGenerators += Def.task {
      val upstream =
        (ThisBuild / baseDirectory).value / "milvus-storage" / "java" / "src" / "main"
      require(
        (upstream / "java" / "io" / "milvus" / "storage" / "NativeLibraryLoader.java").isFile,
        "Initialize milvus-storage before compiling: git submodule update --init milvus-storage"
      )
      val output = (Compile / sourceManaged).value / "milvus-storage"
      val sources =
        ((upstream / "java" ** "*.java") +++ (upstream / "scala" ** "*.scala")).get
      val mappings = sources.map { source =>
        source -> (output / IO.relativize(upstream, source).get)
      }
      // Generated copies keep this repository's formatter out of upstream's
      // sources. Sync removes only previously generated files that disappeared.
      Sync.sync(streams.value.cacheStoreFactory.make("storage-api"))(mappings)
      mappings.map(_._2)
    }.taskValue,
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-vector" % Versions
        .line("4.0")
        .arrow % "provided",
      "org.apache.arrow" % "arrow-memory-core" % Versions
        .line("4.0")
        .arrow % "provided",
      "org.apache.arrow" % "arrow-c-data" % Versions
        .line("4.0")
        .arrow % "provided",
      "junit" % "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test
    ),
    publish / skip := true
  )

lazy val nativeVector = Project("native-vector", file("native-vector"))
  .dependsOn(nativeRuntime % compileAndRuntime)
  .settings(
    name := "native-vector",
    moduleName := "spark-milvus-native-vector",
    Modules.javaOnly,
    KnowhereBuild.settings,
    ThinArtifacts.settings("native-vector", Versions.sharedJavaRelease),
    ReadValidation.artifactSettings,
    NativeBundle.validatedNativeBundle := (nativeRuntime / NativeBundle.validatedNativeBundle).value,
    KnowhereBuild.knowhereRuntimeJar := (nativeRuntime / Compile / packageBin).value,
    libraryDependencies ++= Seq(
      "junit" % "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test
    ),
    Test / fork := true,
    publish / skip := true
  )

lazy val milvusProtoDir =
  Def.setting((ThisBuild / baseDirectory).value / "milvus-proto" / "proto")

// Layer 2: the core layer. All computation happens here, and no source file
// mentions org.apache.spark.
lazy val core = project
  .in(file("core"))
  .dependsOn(
    nativeStorage % compileAndRuntime,
    nativeVector % compileAndRuntime
  )
  .settings(
    name := "core",
    moduleName := "spark-milvus-core",
    Modules.sparkFreeModuleSettings,
    ThinArtifacts.settings("core", Versions.sharedJavaRelease),
    ReadValidation.artifactSettings,
    Modules.jacksonPin,
    publish / skip := true,
    // Arrow is pinned by spark-<line>; core compiles against the interfaces.
    libraryDependencies ++= Seq(
      // Compiled against the interfaces only; the implementation arrives at
      // runtime at the version spark-<line> pinned.
      "org.apache.arrow" % "arrow-vector" % Versions
        .line("4.0")
        .arrow % "provided",
      "org.apache.arrow" % "arrow-memory-core" % Versions
        .line("4.0")
        .arrow % "provided",
      "org.apache.arrow" % "arrow-c-data" % Versions
        .line("4.0")
        .arrow % "provided",
      "org.apache.arrow" % "arrow-format" % Versions
        .line("4.0")
        .arrow % "provided",
      // The Arrow artifacts above are interfaces only. Allocating a buffer
      // needs an allocation manager, which Spark supplies at runtime; core's
      // own native tests have no Spark, so they bring their own.
      "org.apache.arrow" % "arrow-memory-netty" % Versions
        .line("4.0")
        .arrow % Test,
      // No source here imports Hadoop. parquet-mr does: ParquetFileReader
      // builds its codec factory on a Hadoop Configuration, so the class has
      // to be on the classpath even for the in-memory reads core does.
      hadoopCommon,
      // Three formats the storage layer itself needs: snapshots
      // are JSON, segment manifests are Avro, delete files and column
      // groups are Parquet.
      jacksonDatabind,
      jacksonScala,
      avro,
      parquetHadoop,
      // The primary-key bloom filter a written segment carries is keyed by
      // xxh3, as Milvus's is.
      zeroAllocationHashing,
      scalapbRuntime % "protobuf",
      scalaTest % Test
    ),
    // The native suites here load libmilvus-storage-jni and hand Arrow C
    // structs across, which needs the library path and the java.nio add-opens
    // the Spark lines already get.
    inConfig(Test)(Modules.nativeTest),
    // Only storage schema messages are generated; no service stubs belong here.
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / includeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

// Layer 3: one module with shared sources and explicitly selected shims.
lazy val spark = Project("spark", file("spark"))
  .dependsOn(core % s"$compileAndRuntime;test->test")
  .configs(ReleaseJvm)
  .settings(
    name := "spark",
    moduleName := s"spark-milvus-${Versions.selected.id}",
    Modules.perLine(Versions.selected),
    Modules.sparkSources(Versions.selected),
    ReadValidation.artifactSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.sparkDeps(Versions.selected) ++
      Dependencies.arrowDeps(Versions.selected) ++ Dependencies.readerDeps(
        Versions.selected
      ),
    dependencyOverrides ++= Dependencies.lineOverrides(Versions.selected),
    libraryDependencies += scalaTest % Test,
    inConfig(Test)(Modules.nativeTest),
    Test / javaHome := (if (Versions.selected.id == "3.5")
                          sys.env.get("SPARK35_TEST_JAVA_HOME").map(file)
                        else None),
    Test / envVars += {
      val jdk =
        (Test / javaHome).value.getOrElse(file(sys.props("java.home")))
      val (variable, library) = Modules.signalChaining(jdk)
      variable -> library.getAbsolutePath
    },
    // Spill changes process-wide thresholds; retry requires its own master.
    // Give each suite a separate JVM with this line's Java and environment.
    Test / testGrouping := {
      val groups = (Test / testGrouping).value
      val options = (Test / forkOptions).value
      groups.flatMap { group =>
        val isolatedNames = Set(
          "com.zilliz.spark.connector.search.SearchMergeTest",
          "com.zilliz.spark.connector.read.V3ReadContractTest"
        )
        val (isolated, remaining) =
          group.tests.partition(test => isolatedNames(test.name))
        if (isolated.isEmpty) Seq(group)
        else {
          val original =
            if (remaining.isEmpty) Seq.empty
            else Seq(group.withTests(remaining))
          original ++ isolated.map(test =>
            group
              .withName(s"${group.name}-${test.name}")
              .withTests(Seq(test))
              .withRunPolicy(Tests.SubProcess(options))
          )
        }
      }
    },
    assembly / artifact := (assembly / artifact).value.withClassifier(
      Some("bundle")
    )
  )
  .settings(jvmReleaseSettings(Versions.selected))
  .settings(thinReleaseSettings)
  .settings(
    if (Versions.selected.id == "3.5") Seq.empty
    else connectClient(Versions.selected)
  )
  .settings(lineBundle(Versions.selected))

// Spark Connect on the 4.x lines. The suite runs the server in its own JVM,
// beside the classic session, and starts the client as a JVM of its own: the
// client jar carries the sql-api classes rebuilt against a relocated Arrow,
// which would replace the classic ones. The client's classpath, the jar and
// this line's classes, reaches the suite as a system property.
lazy val ConnectClient = config("connect-client").hide

def connectClient(l: Versions.SparkLine): Seq[Setting[_]] = Seq(
  ivyConfigurations += ConnectClient,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-connect" % l.spark % Test,
    "org.apache.spark" %% "spark-connect-client-jvm" % l.spark % ConnectClient
  ),
  Test / javaOptions += {
    val client = Classpaths
      .managedJars(ConnectClient, classpathTypes.value, update.value)
      .files
    val classes =
      Seq((Compile / classDirectory).value, (Test / classDirectory).value)
    "-Dmilvus.test.connect.client.classpath=" +
      (client ++ classes)
        .map(_.getAbsolutePath)
        .mkString(java.io.File.pathSeparator)
  }
)

// Thin candidates preserve module boundaries for downstream compilation. Their
// POMs and manifests receive release coordinates without changing local modules.
lazy val thinReleaseSettings: Seq[Setting[_]] =
  ThinArtifacts.settings("spark", Versions.selected.javaRelease) ++ Seq(
    releaseThinArtifacts / aggregate := false,
    releaseThinArtifacts := ThinArtifacts.export(
      (ThisBuild / baseDirectory).value,
      Versions.selected,
      scalaVersion.value,
      scalaBinaryVersion.value,
      Seq(
        (nativeRuntime / thinModule).value,
        (nativeStorage / thinModule).value,
        (nativeVector / thinModule).value,
        (core / thinModule).value,
        thinModule.value
      ),
      (Compile / fullClasspath).value ++ (Runtime / managedClasspath).value,
      (nativeVector / KnowhereBuild.knowhereApiJar).value,
      javaHome.value.getOrElse(file(sys.props("java.home"))),
      streams.value.log
    )
  )

// Maven's JVM implementation excludes the Milvus platform bundle. It uses
// Compile products and the managed Runtime dependency selection, never the
// native Runtime/unmanagedJars path. Existing local assemblies keep that path.
def jvmReleaseSettings(l: Versions.SparkLine): Seq[Setting[_]] =
  inConfig(ReleaseJvm)(baseAssemblySettings ++ fatJarRules) ++ Seq(
    releaseJvmArtifacts / aggregate := false,
    ReleaseArtifacts.prepareReleaseJvm := {
      ReleaseArtifacts.requireVersion()
      IO.createDirectory(
        ReleaseArtifacts.outputDirectory((ThisBuild / baseDirectory).value, l)
      )
    },
    ReleaseJvm / assembly := (ReleaseJvm / assembly)
      .dependsOn(ReleaseArtifacts.prepareReleaseJvm)
      .value,
    releaseJvmClasspath := ReleaseArtifacts.classpath(
      (Compile / fullClasspath).value,
      (Runtime / managedClasspath).value
    ),
    ReleaseJvm / assembly / fullClasspath := releaseJvmClasspath.value,
    ReleaseJvm / assembly / externalDependencyClasspath :=
      releaseJvmClasspath.value.filter(_.data.isFile),
    ReleaseJvm / assembly / mainClass := None,
    ReleaseJvm / assembly / packageOptions := Seq.empty,
    ReleaseJvm / assembly / assemblyOutputPath := {
      val output =
        ReleaseArtifacts.outputDirectory((ThisBuild / baseDirectory).value, l)
      val artifact = ReleaseArtifacts.artifactId(l, scalaBinaryVersion.value)
      output / s"$artifact-$connectorVersion.jar"
    },
    releaseJvmSourceMappings :=
      (nativeRuntime / Compile / packageSrc / mappings).value ++
        (nativeStorage / Compile / packageSrc / mappings).value ++
        (nativeVector / Compile / packageSrc / mappings).value ++
        (core / Compile / packageSrc / mappings).value ++
        (Compile / packageSrc / mappings).value ++
        ReleaseArtifacts.upstreamSources((ThisBuild / baseDirectory).value),
    releaseJvmDocumentation := Seq(
      "native-runtime" -> (nativeRuntime / Compile / doc).value,
      "native-storage" -> (nativeStorage / Compile / doc).value,
      "native-vector" -> (nativeVector / Compile / doc).value,
      "core" -> (core / Compile / doc).value,
      s"spark-${l.id}" -> (Compile / doc).value
    ),
    releaseJvmArtifacts := ReleaseArtifacts.export(
      (ThisBuild / baseDirectory).value,
      l,
      scalaVersion.value,
      scalaBinaryVersion.value,
      (ReleaseJvm / assembly).value,
      releaseJvmSourceMappings.value,
      releaseJvmDocumentation.value,
      (Compile / fullClasspath).value ++ (Runtime / managedClasspath).value,
      releaseJvmClasspath.value,
      javaHome.value.getOrElse(file(sys.props("java.home"))),
      streams.value.log
    )
  )

// The selected Spark line's local bundle.
def lineBundle(l: Versions.SparkLine): Seq[Setting[_]] =
  fatJarRules ++ Seq(
    organization := "com.zilliz",
    version := connectorVersion,
    // The Jackson and Arrow modules Spark ships stay out, so the connector
    // uses Spark's whichever class loader goes first. Bundled, a family splits
    // across the two loaders under userClassPathFirst=true. Arrow's jackson-bom
    // raises jackson-databind past the jackson-module-scala the connector asks
    // for, and the module refuses a databind of another minor version: every
    // snapshot read fails. Spark 4.2's arrow-compression registers a codec
    // factory that is not a subtype of the bundled CompressionCodec.Factory:
    // every task that reads Arrow data fails. arrow-c-data stays, as Spark
    // does not ship it.
    assembly / assemblyExcludedJars ++= {
      val spark = Set(
        "com.fasterxml.jackson.core" -> "jackson-annotations",
        "com.fasterxml.jackson.core" -> "jackson-core",
        "com.fasterxml.jackson.core" -> "jackson-databind",
        "com.fasterxml.jackson.datatype" -> "jackson-datatype-jsr310",
        "com.fasterxml.jackson.module" ->
          s"jackson-module-scala_${scalaBinaryVersion.value}",
        "org.apache.arrow" -> "arrow-format",
        "org.apache.arrow" -> "arrow-memory-core",
        "org.apache.arrow" -> "arrow-memory-netty",
        "org.apache.arrow" -> "arrow-memory-netty-buffer-patch",
        "org.apache.arrow" -> "arrow-vector"
      )
      (assembly / fullClasspath).value.filter(
        _.get(moduleID.key).exists(m => spark(m.organization -> m.name))
      )
    },
    assembly / assemblyJarName :=
      s"spark-milvus-${l.id}-bundle-${version.value}.jar"
  )

// Service-free object-storage tests are explicit and outside root's aggregate.
lazy val integration = project
  .in(file("integration"))
  .dependsOn(spark % compileAndRuntime)
  .settings(
    name := "integration",
    moduleName := "spark-milvus-integration",
    Modules.perLine(Versions.selected),
    publish / skip := true,
    libraryDependencies += hadoopAws.withConfigurations(Some("test")),
    libraryDependencies ++= Dependencies
      .sparkDeps(Versions.selected)
      .map(_.withConfigurations(Some("test"))),
    libraryDependencies += scalaTest % Test,
    inConfig(Test)(Modules.nativeTest)
  )

// Shared packing rules for the selected Spark bundle and JVM export.
lazy val fatJarRules: Seq[Setting[_]] = Seq(
  assembly / parallelExecution := true,
  assembly / assemblyPackageScala / assembleArtifact := false,
  // Spark supplies slf4j-api; a bundled copy conflicts with it.
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
    // Knowhere's manifest checksums describe one complete native dependency set.
    // Conflicting resources must fail packaging instead of selecting one copy.
    case PathList("native", "knowhere", xs @ _*) =>
      NativeBundle.nativeMergeStrategy
    case PathList("native", "milvus", xs @ _*) =>
      NativeBundle.nativeMergeStrategy
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
    case PathList("org", "apache", "hadoop", xs @ _*)
        if xs.last == "package-info.class" =>
      MergeStrategy.first
    case PathList("software", "amazon", "awssdk", xs @ _*)
        if xs.last == "VersionInfo.class" =>
      MergeStrategy.first
    case x =>
      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

// Publication metadata shared by the build.
ThisBuild / organizationName := "zilliz"
ThisBuild / organizationHomepage := Some(url("https://zilliz.com/"))
ThisBuild / description :=
  "Read and search Milvus V3 snapshots directly from Apache Spark."

ThisBuild / licenses := List(
  "Server Side Public License v1" -> URI
    .create(
      "https://raw.githubusercontent.com/mongodb/mongo/refs/heads/master/LICENSE-Community.txt"
    )
    .toURL,
  "GNU Affero General Public License v3 (AGPLv3)" -> URI
    .create(
      "https://www.gnu.org/licenses/agpl-3.0.txt"
    )
    .toURL
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

// Maven repository and publication policy.
lazy val snapshotRepositoryUrl = sys.env.getOrElse(
  "MAVEN_SNAPSHOT_REPOSITORY_URL",
  "https://central.sonatype.com/repository/maven-snapshots/"
)

ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / publishMavenStyle := true

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

ThisBuild / publishTo := {
  if (isSnapshot.value) Some("maven-snapshots" at snapshotRepositoryUrl)
  else localStaging.value
}

// Build version for the selected Spark bundle.
lazy val arch = System.getProperty("os.arch") match {
  case "amd64" | "x86_64"  => "amd64"
  case "aarch64" | "arm64" => "arm64"
  case other               => other
}

// Docker supplies GIT_BRANCH; local builds use the current checkout.
lazy val gitBranch = {
  val branch = sys.env.getOrElse(
    "GIT_BRANCH",
    // The JVM needs libjsig, but preloading it into git can pollute stdout.
    scala.util
      .Try(
        Process(
          "git rev-parse --abbrev-ref HEAD",
          None,
          "LD_PRELOAD" -> ""
        ).!!.trim
      )
      .getOrElse("unknown")
  )
  branch.replaceAll("[^a-zA-Z0-9._-]", "-")
}

// The 2.0 version carried by the selected Spark bundle.
lazy val connectorVersion = ReleaseArtifacts.requestedVersion.getOrElse(
  s"2.0.0-$gitBranch-$arch-SNAPSHOT"
)
