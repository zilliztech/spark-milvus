import sbt._
import sbt.Keys._

/** Shared settings and build constraints for the 2.0 modules. See section 4 of
  * docs/design/modules.md.
  */
object Modules {

  val checkNoSpark = taskKey[Unit](
    "The core layer must not depend on Spark: org.apache.spark in a source file fails the build"
  )

  /** Comments may explain old Spark dependencies without introducing one. */
  private def stripComments(source: String): String =
    source
      .replaceAll("(?s)/\\*.*?\\*/", "")
      .replaceAll("(?m)//.*$", "")

  /** Reject Spark references in the shared modules before compilation. */
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
    // Module publication is not enabled yet.
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

  /** Compile settings for one Spark line's Java and Scala versions. */
  def perLine(l: Versions.SparkLine): Seq[Setting[_]] = Seq(
    crossScalaVersions := l.scalas,
    scalacOptions ++= Seq("-release", l.javaRelease),
    javacOptions ++= Seq("--release", l.javaRelease),
    publish / skip := true
  )

  /** Remove when native-storage replaces the upstream Scala 2.13 binding. */
  val legacyJni: Seq[Setting[_]] = Seq(
    Compile / unmanagedJars += (ThisBuild / baseDirectory).value /
      "milvus-storage" / "java" / "target" / "scala-2.13" /
      "milvus-storage-jni_2.13-0.1.0-SNAPSHOT.jar"
  )

  /** Keep layer 2's Jackson artifacts aligned. Spark modules use Spark's set. */
  val jacksonPin: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      Dependencies.jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    )
  )

  /** Test JVM settings for Arrow access and the native libraries built by Docker. */
  def nativeTest: Seq[Setting[_]] = Seq(
    fork := true,
    // Fixtures are addressed relative to the repository root.
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
