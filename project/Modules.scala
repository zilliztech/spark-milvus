import java.util.Locale

import sbt._
import sbt.Keys._

/** Shared settings and build constraints for the 2.0 modules. See section 4 of
  * docs/design/architecture/modules.md.
  */
object Modules {

  /** The JVM's signal-chaining library is preloaded through
    * `DYLD_INSERT_LIBRARIES` (`libjsig.dylib`) on macOS and `LD_PRELOAD`
    * (`libjsig.so`) elsewhere; the build only has to name the right environment
    * variable.
    */
  val isMacOS: Boolean =
    System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("mac")

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

  /** Keep the Jackson artifacts aligned on one version. Layer 2 applies it so
    * its own classpath is consistent; the Spark line modules use Spark's set.
    * Root applies it too, because its assembly ships jackson-module-scala with
    * Spark and Arrow excluded: Arrow 18 otherwise drags jackson-databind to
    * 2.18 while module-scala stays on `Versions.jackson`, and the module then
    * refuses to register under `spark.driver.userClassPathFirst=true` ("Scala
    * module 2.17.3 requires Jackson Databind version >= 2.17.0 and < 2.18.0"),
    * which fails every snapshot read.
    */
  val jacksonPin: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      Dependencies.jacksonDatabind,
      Dependencies.jacksonScala,
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % Versions.jackson
    )
  )

  /** Test JVM settings for Arrow access and the selected native libraries. */
  def nativeTest: Seq[Setting[_]] = Seq(
    fork := true,
    // Fixtures are addressed relative to the repository root.
    baseDirectory := (ThisBuild / baseDirectory).value,
    parallelExecution := true,
    logBuffered := false,
    javaOptions := {
      val root = (ThisBuild / baseDirectory).value
      val nativeDir = (if (NativeBundle.selected.nonEmpty)
                         root / "target" / "empty-native-path"
                       else
                         root / "native-storage" / "src" / "main" / "resources" / "native").getAbsolutePath
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
    // LD_LIBRARY_PATH is read by the Linux loader, DYLD_LIBRARY_PATH by dyld;
    // setting both is harmless on either platform.
    envVars := (if (NativeBundle.selected.nonEmpty)
                  Map("LD_LIBRARY_PATH" -> "", "LD_BIND_NOW" -> "1")
                else {
                  val nativeDir =
                    ((ThisBuild / baseDirectory).value / "native-storage" / "src" / "main" / "resources" / "native").getAbsolutePath
                  Map(
                    "LD_LIBRARY_PATH" -> nativeDir,
                    "DYLD_LIBRARY_PATH" -> nativeDir
                  )
                })
  )
}
