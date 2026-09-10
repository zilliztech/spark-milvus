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
  .settings(
    // 2.0 的新模块只跟着编译和测试。assembly、publish 不下发到子模块，Docker
    // 构建和发布的行为与 1.x 完全一致。
    assembly / aggregate := false,
    publish / aggregate := false,
    publishLocal / aggregate := false,
    name := "spark-connector",
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

    // 老 log binding (slf4j-log4j12 / reload4j / log4j 1.x) 与 spark 的 log4j2 冲突，
    // 全局排除掉。
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("org.slf4j", "slf4j-reload4j"),
      ExclusionRule("log4j", "log4j"),
      ExclusionRule("ch.qos.reload4j", "reload4j")
    ),
    // 在 assembly 阶段过滤掉 slf4j-api jar：
    // 编译时仍可用（来自传递依赖），但不进 fat jar，运行时由 spark 镜像
    // /opt/spark/jars/slf4j-api-2.x.jar 提供，避免 userClassPathFirst=true 时
    // Logger 被加载两份触发 LinkageError
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
    Compile / PB.protoSources += baseDirectory.value / "milvus-proto/proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Compile / unmanagedSourceDirectories += (
      Compile / PB.targets
    ).value.head.outputPath,
    Compile / packageBin / mappings ++= {
      val base = (Compile / PB.targets).value.head.outputPath
      (base ** "*.scala").get.map { file =>
        file -> s"generated_protobuf/${file.relativeTo(base).getOrElse(file)}"
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
// 2.0 的多模块骨架。docs/design/modules.md 是契约。
//
// 1.x 的代码仍然在 root 的 src/main/scala 里，root 的设置一个字没动；新模块现在
// 只有包结构，迁移一个模块一个模块来（modules.md 第 5 节）。依赖只能向下：
//   apps-<line> -> spark-<line> -> compat、client -> core -> native-*
// spark/base 与 apps/base 不是 project，只是各线引用的共享源码目录。
// ---------------------------------------------------------------------------

lazy val v2Modules: Seq[ProjectReference] = Seq(
  nativeStorage, nativeVector, core, compat, client,
  spark35, spark40, spark41, spark42,
  apps35, apps40, apps41, apps42,
  bundle35, bundle40, bundle41, bundle42,
  integration35, integration40, integration41, integration42
)

// 第 1 层：两个原生库的封装。纯 Java，产物不带 Scala 后缀。
lazy val nativeStorage = Project("nativeStorage", file("native/storage"))
  .settings(name := "spark-milvus-native-storage", Modules.javaOnly)

lazy val nativeVector = Project("nativeVector", file("native/vector"))
  .settings(name := "spark-milvus-native-vector", Modules.javaOnly)

// 第 2 层：核心层。全部计算在这里，源码不出现 org.apache.spark。
lazy val core = Project("core", file("core"))
  .dependsOn(nativeStorage, nativeVector)
  .settings(
    name := "spark-milvus-core",
    Modules.shared,
    // Arrow 由 spark-<line> 钉；core 只按 C Data Interface 编译。
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-c-data" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-format" % Versions.line("4.0").arrow % "provided",
      scalaTest % Test
    )
    // TODO 决策 17：Milvus 表达式的 Plan.g4 与 antlr runtime 放哪。Spark 3.5 带
    // antlr 4.9.3、4.x 带 4.13.1，生成的解析器不通用，所以这里先不加 antlr 依赖。
  )

// 三个非标准入口的适配器，产出 core 的 Snapshot 或 SegmentReader。
lazy val compat = Project("compat", file("compat"))
  .dependsOn(core)
  .settings(
    name := "spark-milvus-compat",
    Modules.shared,
    libraryDependencies += scalaTest % Test
  )

// Milvus 在线服务的客户端：DDL、Delete、Procedure 用到的调用。
lazy val client = Project("client", file("client"))
  .dependsOn(core)
  .settings(
    name := "spark-milvus-client",
    Modules.shared,
    libraryDependencies ++= Seq(
      grpcNetty,
      scalapbRuntime % "protobuf",
      scalapbRuntimeGrpc,
      scalaTest % Test
    )
    // TODO 迁移时把 milvus-proto 的生成从 root 移到这里（modules.md 第 5 节）。
  )

// 第 3 层：每条 Spark 线一个 project，共享 spark/base 的源码。
def sparkProject(l: Versions.SparkLine): Project =
  Project(l.projectId, file(s"spark/${l.id}"))
    .dependsOn(core, compat, client)
    .settings(
      name := s"spark-milvus-${l.id}",
      Modules.perLine(l),
      Compile / unmanagedSourceDirectories +=
        Modules.sharedSource((ThisBuild / baseDirectory).value, "spark"),
      libraryDependencies ++= Modules.sparkDeps(l) ++ Modules.arrowDeps(l),
      libraryDependencies += scalaTest % Test
    )

lazy val spark35 = sparkProject(Versions.line("3.5"))
lazy val spark40 = sparkProject(Versions.line("4.0"))
lazy val spark41 = sparkProject(Versions.line("4.1"))
lazy val spark42 = sparkProject(Versions.line("4.2"))

// 第 4 层：对外的入口，每条线一个 fat jar。四个包互不依赖。
// 名字不用 ops：内部已有一个叫 OPS 的系统，容易混。
def appsProject(l: Versions.SparkLine, sparkLine: Project): Project =
  Project(s"apps${l.projectId.stripPrefix("spark")}", file(s"apps/${l.id}"))
    .dependsOn(sparkLine)
    .settings(
      name := s"spark-milvus-apps-${l.id}",
      Modules.perLine(l),
      Compile / unmanagedSourceDirectories +=
        Modules.sharedSource((ThisBuild / baseDirectory).value, "apps"),
      libraryDependencies ++= Modules.sparkDeps(l),
      libraryDependencies += scalaTest % Test
    )

lazy val apps35 = appsProject(Versions.line("3.5"), spark35)
lazy val apps40 = appsProject(Versions.line("4.0"), spark40)
lazy val apps41 = appsProject(Versions.line("4.1"), spark41)
lazy val apps42 = appsProject(Versions.line("4.2"), spark42)

// 打包。TODO：shade 规则按 modules.md 第 4 节第 6 条 —— 只 relocate protobuf 和
// guava，native 与 arrow 不 relocate（JNI 的导出符号已按包名编进 .so）。
def bundleProject(l: Versions.SparkLine, sparkLine: Project): Project =
  Project(s"bundle${l.projectId.stripPrefix("spark")}", file(s"spark/bundle-${l.id}"))
    .dependsOn(sparkLine)
    .settings(
      name := s"spark-milvus-bundle-${l.id}",
      Modules.perLine(l),
      libraryDependencies ++= Modules.sparkDeps(l)
    )

lazy val bundle35 = bundleProject(Versions.line("3.5"), spark35)
lazy val bundle40 = bundleProject(Versions.line("4.0"), spark40)
lazy val bundle41 = bundleProject(Versions.line("4.1"), spark41)
lazy val bundle42 = bundleProject(Versions.line("4.2"), spark42)

// 集成测试。需要 MinIO 和 Milvus，不发布；用例写在各自的 src/test/scala，
// 共享 integration/base 的源码。名字不用 it：sbt 内置的 IntegrationTest 配置
// 从 1.9 起废弃、sbt 2 已删除，2.0 不再用它，沿用这个词会误导。
def integrationProject(l: Versions.SparkLine, sparkLine: Project, appsLine: Project): Project =
  Project(s"integration${l.projectId.stripPrefix("spark")}", file(s"integration/${l.id}"))
    .dependsOn(sparkLine, appsLine)
    .settings(
      name := s"spark-milvus-integration-${l.id}",
      Modules.perLine(l),
      Test / unmanagedSourceDirectories +=
        (ThisBuild / baseDirectory).value / "integration" / "base" / "src" / "test" / "scala",
      libraryDependencies ++= Modules.sparkDeps(l).map(_.withConfigurations(Some("test"))),
      libraryDependencies += scalaTest % Test
    )

lazy val integration35 = integrationProject(Versions.line("3.5"), spark35, apps35)
lazy val integration40 = integrationProject(Versions.line("4.0"), spark40, apps40)
lazy val integration41 = integrationProject(Versions.line("4.1"), spark41, apps41)
lazy val integration42 = integrationProject(Versions.line("4.2"), spark42, apps42)
