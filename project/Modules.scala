import sbt._
import sbt.Keys._

/** 2.0 各模块的共用设置与构建约束。见 docs/design/modules.md 第 4 节。 */
object Modules {

  val checkNoSpark = taskKey[Unit]("核心层不得依赖 Spark：源码里出现 org.apache.spark 即编译失败")

  /** 约束 1：core、compat、client 的源码不出现 org.apache.spark。
    *
    * 挂在 Compile / compile 前面，所以违反了就编译不过，而不是等到评审才发现。
    */
  val noSparkImports: Seq[Setting[_]] = Seq(
    checkNoSpark := {
      val log = streams.value.log
      val offenders = (Compile / sources).value.filter { f =>
        val n = f.getName
        (n.endsWith(".scala") || n.endsWith(".java")) && IO.read(f).contains("org.apache.spark")
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
    libraryDependencies +=
      "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCollectionCompat,
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
    Seq("arrow-vector", "arrow-memory-core", "arrow-c-data")
      .map(m => "org.apache.arrow" % m % l.arrow)

  /** 共享源码目录：spark-base 不是 project，四条线把它加进自己的源码根。 */
  def sharedSource(root: File, dir: String): File =
    root / dir / "src" / "main" / "scala"
}
