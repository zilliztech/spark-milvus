import sbt._
import sbt.Keys._

/** 2.0 各模块的共用设置与构建约束。见 docs/design/modules.md 第 4 节。 */
object Modules {

  val checkNoSpark = taskKey[Unit]("核心层不得依赖 Spark：源码里出现 org.apache.spark 即编译失败")

  /** 注释里提到 org.apache.spark 是合法的（迁移期到处要写「这个类原来继承 Spark 的
    * Logging」），扫描前先去掉块注释和行注释，只看真代码。
    */
  private def stripComments(source: String): String =
    source
      .replaceAll("(?s)/\\*.*?\\*/", "")
      .replaceAll("(?m)//.*$", "")

  /** 约束 1：core、compat、client 的源码不出现 org.apache.spark。
    *
    * 挂在 Compile / compile 前面，所以违反了就编译不过，而不是等到评审才发现。
    */
  val noSparkImports: Seq[Setting[_]] = Seq(
    checkNoSpark := {
      val log = streams.value.log
      val offenders = (Compile / unmanagedSources).value.filter { f =>
        val n = f.getName
        (n.endsWith(".scala") || n.endsWith(".java")) &&
        stripComments(IO.read(f)).contains("org.apache.spark")
      }
      if (offenders.nonEmpty) {
        offenders.foreach { f =>
          log.error(s"${name.value}：核心层不得依赖 Spark，但 $f 里出现了 org.apache.spark")
        }
        sys.error(s"${name.value} 出现 ${offenders.size} 处 Spark 依赖（docs/design/modules.md 第 4 节第 1 条）")
      }
    },
    Compile / compile := (Compile / compile).dependsOn(checkNoSpark).value
  )

  /** 跨线共用的 Scala 模块：core、compat、client。 */
  val shared: Seq[Setting[_]] = Seq(
    crossScalaVersions := Versions.sharedScalas,
    scalacOptions ++= Seq("-release", Versions.sharedJavaRelease),
    javacOptions ++= Seq("--release", Versions.sharedJavaRelease),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCollectionCompat,
      // core 的日志门面建在 slf4j 上，运行时用 Spark 自带的那份。
      "org.slf4j" % "slf4j-api" % Versions.slf4j % "provided"
    ),
    // 模块有内容之后再开发布
    publish / skip := true
  ) ++ noSparkImports

  /** 第 1 层的两个模块是纯 Java 的，产物不带 Scala 后缀。 */
  val javaOnly: Seq[Setting[_]] = Seq(
    crossPaths := false,
    autoScalaLibrary := false,
    javacOptions ++= Seq("--release", Versions.sharedJavaRelease),
    publish / skip := true
  )

  /** 一条 Spark 线的编译设置：钉本线的 Java、Scala 与 Spark。 */
  def perLine(l: Versions.SparkLine): Seq[Setting[_]] = Seq(
    crossScalaVersions := l.scalas,
    scalacOptions ++= Seq("-release", l.javaRelease),
    javacOptions ++= Seq("--release", l.javaRelease),
    publish / skip := true
  )

  /** 本线的 Spark 模块，一律 provided；Arrow 由本线钉，不从 Spark 传递进来。 */
  def sparkDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("spark-core", "spark-sql", "spark-catalyst").map { m =>
      ("org.apache.spark" %% m % l.spark % "provided")
        .excludeAll(ExclusionRule(organization = "org.apache.arrow"))
    }

  /** 约束 3：Arrow 版本由 spark-<line> 钉，core 只按 C Data Interface 编译。 */
  def arrowDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("arrow-vector", "arrow-memory-core", "arrow-c-data", "arrow-memory-netty")
      .map(m => "org.apache.arrow" % m % l.arrow)

  /** 迁移期：1.x 的连接器源码搬进 spark-base 之后，四条线都要这批依赖。
    *
    * 随着代码往下沉到 core，这个列表只会变短。存储 SDK 与 parquet、avro 最终都
    * 归 core，这里剩下的应该只有 Spark、Arrow 和 Milvus 的客户端。
    */
  def legacyDeps(l: Versions.SparkLine): Seq[ModuleID] = Seq(
    Dependencies.sparkMLlib,
    Dependencies.parquetHadoop,
    Dependencies.parquetAvro,
    Dependencies.avro,
    Dependencies.hadoopCommon,
    Dependencies.hadoopAws,
    Dependencies.hadoopAliyun,
    Dependencies.awsSdkS3,
    Dependencies.awsSdkS3Transfer,
    Dependencies.awsSdkCore,
    Dependencies.jacksonScala,
    Dependencies.jacksonDatabind,
    Dependencies.grpcNetty,
    Dependencies.scalapbRuntimeGrpc,
    Dependencies.munit % Test,
    Dependencies.hadoopMapreduceClientCore % Test
  )

  /** jackson 三件套钉同一个版本：parquet-hadoop 传递进来的 databind 更新，
    * 与 jackson-module-scala 跨版本会直接抛 JsonMappingException。
    *
    * 只给第 2 层用。Spark 自带一套自洽的 jackson，钉死反而会把 databind 压到比
    * Spark 的 jackson-module-scala 低，同样抛异常。
    */
  val jacksonPin: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      Dependencies.jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    )
  )

  /** 跑测试要的 JVM 参数。
    *
    * Arrow 的 MemoryUtil 要 --add-opens 才能初始化，否则一碰列批就抛
    * 「Failed to initialize MemoryUtil」；原生库的路径是 Dockerfile 写入的
    * 那个目录，本机没 build 过就只有依赖它的几个用例失败。
    */
  def nativeTest: Seq[Setting[_]] = Seq(
    fork := true,
    // fork 之后工作目录默认是子项目的目录。1.x 的用例全部按仓库根写相对路径
    // 读 fixture，钉回根目录，路径的含义与迁移前一致。
    baseDirectory := (ThisBuild / baseDirectory).value,
    parallelExecution := true,
    logBuffered := false,
    javaOptions := {
      val nativeDir =
        ((ThisBuild / baseDirectory).value / "src" / "main" / "resources" / "native").getAbsolutePath
      Seq(
        "-Xss2m",
        "-Xmx4g",
        s"-Djava.library.path=$nativeDir",
        "-Dlog4j2.configurationFile=log4j2.properties",
        "-Dlog4j2.debug=false",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
      )
    },
    envVars := Map(
      "LD_LIBRARY_PATH" ->
        ((ThisBuild / baseDirectory).value / "src" / "main" / "resources" / "native").getAbsolutePath
    )
  )

  /** 共享源码目录：spark-base 不是 project，四条线把它加进自己的源码根。 */
  def sharedSource(root: File, dir: String): File =
    root / dir / "src" / "main" / "scala"
}
