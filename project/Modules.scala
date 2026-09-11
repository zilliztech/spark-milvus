import sbt._
import sbt.Keys._

/** Shared settings and build constraints for the 2.0 modules. See section 4 of
  * docs/design/modules.md.
  */
object Modules {

  val checkNoSpark = taskKey[Unit](
    "The core layer must not depend on Spark: org.apache.spark in a source file fails the build"
  )

  /** Mentioning org.apache.spark in a comment is legitimate: during the
    * migration many files need to say "this class used to extend Spark's
    * Logging". Strip block and line comments before scanning so only real code
    * is checked.
    */
  private def stripComments(source: String): String =
    source
      .replaceAll("(?s)/\\*.*?\\*/", "")
      .replaceAll("(?m)//.*$", "")

  /** Constraint 1: no source file in core, compat or client mentions
    * org.apache.spark.
    *
    * It runs ahead of Compile / compile, so a violation fails the build instead
    * of waiting to be caught in review.
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
          log.error(
            s"${name.value}: the core layer must not depend on Spark, but $f mentions org.apache.spark"
          )
        }
        sys.error(
          s"${name.value} has ${offenders.size} Spark dependencies " +
            "(constraint 1, section 4 of docs/design/modules.md)"
        )
      }
    },
    Compile / compile := (Compile / compile).dependsOn(checkNoSpark).value
  )

  /** The Scala modules shared across Spark lines: core, compat and client. */
  val shared: Seq[Setting[_]] = Seq(
    crossScalaVersions := Versions.sharedScalas,
    scalacOptions ++= Seq("-release", Versions.sharedJavaRelease),
    javacOptions ++= Seq("--release", Versions.sharedJavaRelease),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCollectionCompat,
      // The core logging facade is built on slf4j; at runtime it uses the copy
      // Spark ships.
      "org.slf4j" % "slf4j-api" % Versions.slf4j % "provided"
    ),
    // Publishing turns on once a module has content.
    publish / skip := true
  ) ++ noSparkImports

  /** The two layer-1 modules are plain Java, so their artifacts carry no Scala
    * suffix.
    */
  val javaOnly: Seq[Setting[_]] = Seq(
    crossPaths := false,
    autoScalaLibrary := false,
    javacOptions ++= Seq("--release", Versions.sharedJavaRelease),
    publish / skip := true
  )

  /** Compile settings for one Spark line: pin that line's Java, Scala and
    * Spark.
    */
  def perLine(l: Versions.SparkLine): Seq[Setting[_]] = Seq(
    crossScalaVersions := l.scalas,
    scalacOptions ++= Seq("-release", l.javaRelease),
    javacOptions ++= Seq("--release", l.javaRelease),
    publish / skip := true
  )

  /** This line's Spark modules, always provided. Arrow is pinned by the line
    * itself rather than inherited transitively from Spark.
    */
  def sparkDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("spark-core", "spark-sql", "spark-catalyst").map { m =>
      ("org.apache.spark" %% m % l.spark % "provided")
        .excludeAll(ExclusionRule(organization = "org.apache.arrow"))
    }

  /** Constraint 3: the Arrow version is pinned by spark-<line>; core compiles
    * against the interfaces only.
    */
  def arrowDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("arrow-vector", "arrow-memory-core", "arrow-c-data", "arrow-memory-netty")
      .map(m => "org.apache.arrow" % m % l.arrow)

  /** Migration-time: once the 1.x connector sources moved into spark-base, all
    * four lines need this set of dependencies.
    *
    * The list only gets shorter as code sinks down into core. Storage access,
    * parquet and avro all end up there, leaving Spark, Arrow and the Milvus
    * client here.
    */
  def legacyDeps(l: Versions.SparkLine): Seq[ModuleID] = Seq(
    // Pinned to this line's Spark, not the 1.x default. spark-mllib_2.12 does
    // not exist for Spark 4, so the 3.5 line fails to resolve if it inherits
    // the 4.0 version.
    ("org.apache.spark" %% "spark-mllib" % l.spark % "provided,test")
      .excludeAll(ExclusionRule(organization = "org.apache.arrow")),
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

  /** Pins the three jackson artifacts to one version: parquet-hadoop pulls in a
    * newer databind, and mixing it with jackson-module-scala throws
    * JsonMappingException.
    *
    * Layer 2 only. Spark ships a self-consistent jackson set, and pinning there
    * would drag databind below Spark's own jackson-module-scala, which throws
    * the same way.
    */
  val jacksonPin: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      Dependencies.jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    )
  )

  /** The JVM flags the test suites need.
    *
    * Arrow's MemoryUtil cannot initialize without --add-opens; without it the
    * first column batch throws "Failed to initialize MemoryUtil". The native
    * library path points at native-storage's resources, which is where the
    * Dockerfile writes the built libraries, so on a machine that has not built
    * them only the tests that need them fail.
    */
  def nativeTest: Seq[Setting[_]] = Seq(
    fork := true,
    // After forking, the working directory defaults to the subproject's own
    // directory. Every 1.x test reads its fixtures by a path relative to the
    // repository root, so pin it back and the paths keep their old meaning.
    baseDirectory := (ThisBuild / baseDirectory).value,
    parallelExecution := true,
    logBuffered := false,
    javaOptions := {
      val nativeDir =
        ((ThisBuild / baseDirectory).value / "native-storage" / "src" / "main" / "resources" / "native").getAbsolutePath
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
        ((ThisBuild / baseDirectory).value / "native-storage" / "src" / "main" / "resources" / "native").getAbsolutePath
    )
  )

  /** The shared source directory. spark-base is not an sbt project; each of the
    * four lines adds it to its own source roots.
    */
  def sharedSource(root: File, dir: String): File =
    root / dir / "src" / "main" / "scala"
}
