import sbt._
import sbt.Keys._

/** Shared settings and build constraints for the 2.0 modules. See section 4 of
  * docs/design/architecture/modules.md.
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
            "(constraint 1, section 4 of docs/design/architecture/modules.md)"
        )
      }
    },
    Compile / compile := (Compile / compile).dependsOn(checkNoSpark).value
  )

  /** Layer 2's compile baseline, common dependencies and no-Spark check. */
  val sparkFreeModuleSettings: Seq[Setting[_]] = Seq(
    crossScalaVersions := Versions.sharedScalas,
    scalacOptions ++= Seq("-release", Versions.sharedJavaRelease),
    javacOptions ++= Seq("--release", Versions.sharedJavaRelease),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCollectionCompat,
      // The core logging facade is built on slf4j; at runtime it uses the copy
      // Spark ships.
      "org.slf4j" % "slf4j-api" % Versions.slf4j % "provided"
    )
  ) ++ noSparkImports

  /** Knowhere's layer-1 module is plain Java, so its artifact carries no Scala
    * suffix. Storage compiles upstream Scala sources for each shared Scala
    * line.
    */
  val javaOnly: Seq[Setting[_]] = Seq(
    crossPaths := false,
    autoScalaLibrary := false,
    javacOptions ++= Seq("--release", Versions.sharedJavaRelease)
  )

  /** Compile settings for one Spark line's Java and Scala versions. */
  def perLine(l: Versions.SparkLine): Seq[Setting[_]] = Seq(
    crossScalaVersions := l.scalas,
    scalacOptions ++= Seq("-release", l.javaRelease),
    javacOptions ++= Seq("--release", l.javaRelease)
  )

  /** Keep layer 2's Jackson artifacts aligned. Spark modules use Spark's set.
    */
  val jacksonPin: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      Dependencies.jacksonDatabind,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson
    )
  )

  /** Test JVM settings for Arrow access and the native libraries built by
    * Docker.
    */
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
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        // Spark 3.5 on JDK 17+ reaches sun.nio.ch.DirectBuffer from
        // StorageUtils and these others from its own launcher list.
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
      )
    },
    envVars := Map(
      "LD_LIBRARY_PATH" ->
        ((ThisBuild / baseDirectory).value / "native-storage" / "src" / "main" / "resources" / "native").getAbsolutePath
    )
  )
}
