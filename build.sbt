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
  // 迁移期：1.x 的源码留在 src/，已经搬进第 2 层的部分从这三个模块取。
  .dependsOn(core, compat, client)
  .settings(
    // 2.0 的新模块只跟着编译和测试。assembly、publish 不下发到子模块，Docker
    // 构建和发布的行为与 1.x 完全一致。
    assembly / aggregate := false,
    publish / aggregate := false,
    publishLocal / aggregate := false,
    // 显示名跟仓库目录走，元构建才会叫 spark-milvus-build；发布坐标保持
    // spark-connector，云上 spark-data-service 钉的是它。assembly 的文件名默认
    // 从 name 推，这里钉死，否则 Dockerfile 找不到产物。
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
    // proto 的生成移到 core（消息）和 client（服务），这里只消费它们的产物。
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
// 2.0 的多模块骨架。docs/design/modules.md 是契约。
//
// 1.x 的代码仍然在 root 的 src/main/scala 里，root 的设置一个字没动；新模块现在
// 只有包结构，迁移一个模块一个模块来（modules.md 第 5 节）。依赖只能向下：
//   apps-<line> -> spark-<line> -> compat、client -> core -> native-*
// spark-base 只是四条线共享的源码目录，不是 project。把它做成 project 去不掉
// IDE 里那个合成的 spark-base-sources 模块 —— 合成模块来自「多个项目声明同一个
// 源码根」，不是来自「它不是项目」；而「共享源码只能用各线都有的 API」这条约束，
// spark-3.5 用最低的线编译它时本来就提供了。
// ---------------------------------------------------------------------------

lazy val v2Modules: Seq[ProjectReference] = Seq(
  nativeStorage, nativeVector, core, compat, client,
  spark35, spark40, spark41, spark42,
  apps40, integration40
)

// 第 1 层：两个原生库的封装。纯 Java，产物不带 Scala 后缀。
lazy val nativeStorage = Project("native-storage", file("native-storage"))
  .settings(name := "native-storage",
    moduleName := "spark-milvus-native-storage",
    Modules.javaOnly)

lazy val nativeVector = Project("native-vector", file("native-vector"))
  .settings(name := "native-vector",
    moduleName := "spark-milvus-native-vector",
    Modules.javaOnly)

// 第 2 层：核心层。全部计算在这里，源码不出现 org.apache.spark。
lazy val core = Project("core", file("core"))
  .dependsOn(nativeStorage, nativeVector)
  .settings(
    name := "core",
    moduleName := "spark-milvus-core",
    Modules.shared,
    // Arrow 由 spark-<line> 钉；core 只按 C Data Interface 编译。
    libraryDependencies ++= Seq(
      // 只按接口编译，实现由 spark-<line> 钉版本后在运行时提供。
      "org.apache.arrow" % "arrow-vector" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-memory-core" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-c-data" % Versions.line("4.0").arrow % "provided",
      "org.apache.arrow" % "arrow-format" % Versions.line("4.0").arrow % "provided",
      // 日志门面：运行时用 Spark 自带的 slf4j-api，版本按 3.5 线的下限编译。
      "org.slf4j" % "slf4j-api" % "2.0.7" % "provided",
      scalapbRuntime % "protobuf",
      scalaTest % Test
    ),
    // Milvus 的存储格式本身是 protobuf 定义的：快照里嵌着 CollectionSchema，
    // Manifest 的字段描述也来自 schema.proto。所以这两个不带 service 的文件在
    // core 生成（grpc = false）；带 service 的在 client 生成，靠 include 路径
    // 复用这里的产物，不重复生成。
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / includeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false) -> (Compile / sourceManaged).value / "scalapb"
    )
    // TODO 决策 17：Milvus 表达式的 Plan.g4 与 antlr runtime 放哪。Spark 3.5 带
    // antlr 4.9.3、4.x 带 4.13.1，生成的解析器不通用，所以这里先不加 antlr 依赖。
  )

// 三个非标准入口的适配器，产出 core 的 Snapshot 或 SegmentReader。
lazy val compat = Project("compat", file("compat"))
  .dependsOn(core)
  .settings(
    name := "compat",
    moduleName := "spark-milvus-compat",
    Modules.shared,
    libraryDependencies += scalaTest % Test
  )

// Milvus 在线服务的客户端：DDL、Delete、Procedure 用到的调用。
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
      scalaTest % Test
    ),
    // common.proto 与 schema.proto 由 core 生成，这里只把它们放进 include 路径，
    // 生成的服务桩引用 core 里已有的消息类。
    Compile / PB.protoSources := Seq(milvusProtoDir.value),
    Compile / PB.generate / excludeFilter := "common.proto" | "schema.proto",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

// 第 3 层：每条 Spark 线一个 project，共享 spark/base 的源码。
def sparkProject(l: Versions.SparkLine): Project =
  Project(l.projectId, file(s"spark-${l.id}"))
    .dependsOn(core, compat, client)
    .settings(
      name := s"spark-${l.id}",
      moduleName := s"spark-milvus-${l.id}",
      Modules.perLine(l),
      Compile / unmanagedSourceDirectories +=
        Modules.sharedSource((ThisBuild / baseDirectory).value, "spark-base"),
      libraryDependencies ++= Modules.sparkDeps(l) ++ Modules.arrowDeps(l),
      libraryDependencies += scalaTest % Test,
      // fat jar 是同一个 project 上的一个任务，带 classifier 发布，不需要单独
      // 的 bundle 模块（Maven 才需要）。
      // TODO shade 规则按 modules.md 第 4 节第 6 条：只 relocate protobuf 和
      // guava，com.zilliz.milvus.jni.** 与 org.apache.arrow.** 不 relocate。
      assembly / assemblyJarName := s"spark-milvus-${l.id}-bundle.jar",
      assembly / artifact := (assembly / artifact).value.withClassifier(Some("bundle"))
    )

lazy val spark35 = sparkProject(Versions.line("3.5"))
lazy val spark40 = sparkProject(Versions.line("4.0"))
lazy val spark41 = sparkProject(Versions.line("4.1"))
lazy val spark42 = sparkProject(Versions.line("4.2"))

// 第 4 层：对外的入口，一个 fat jar。四个包互不依赖。源码直接放在这一条线的
// 目录里，不设共享的 base —— 只有一个消费者时，共享目录只是多一层。
// 名字不用 ops：内部已有一个叫 OPS 的系统，容易混。
def appsProject(l: Versions.SparkLine, sparkLine: Project): Project =
  Project(s"apps${l.projectId.stripPrefix("spark")}", file(s"apps-${l.id}"))
    .dependsOn(sparkLine)
    .settings(
      name := s"apps-${l.id}",
      moduleName := s"spark-milvus-apps-${l.id}",
      Modules.perLine(l),
      libraryDependencies ++= Modules.sparkDeps(l),
      libraryDependencies += scalaTest % Test
    )

// 只在云上跑的那条线上建一个；别的线有人要了再加一行。
lazy val apps40 = appsProject(Versions.line("4.0"), spark40)


// 集成测试。需要 MinIO 和 Milvus，不发布；用例写在这一条线的 src/test/scala。
// 名字不用 it：sbt 内置的 IntegrationTest 配置
// 从 1.9 起废弃、sbt 2 已删除，2.0 不再用它，沿用这个词会误导。
def integrationProject(l: Versions.SparkLine, sparkLine: Project, appsLine: Project): Project =
  Project(s"integration${l.projectId.stripPrefix("spark")}", file(s"integration-${l.id}"))
    .dependsOn(sparkLine, appsLine)
    .settings(
      name := s"integration-${l.id}",
      moduleName := s"spark-milvus-integration-${l.id}",
      Modules.perLine(l),
      libraryDependencies ++= Modules.sparkDeps(l).map(_.withConfigurations(Some("test"))),
      libraryDependencies += scalaTest % Test
    )

// 集成测试要真的 Milvus 和 MinIO，跑一条线够了；某条线出特有的问题再加。
lazy val integration40 = integrationProject(Versions.line("4.0"), spark40, apps40)
