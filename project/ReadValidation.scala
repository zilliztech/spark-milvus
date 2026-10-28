import java.util.jar.JarFile
import scala.collection.JavaConverters._
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.xml.XML

import sbt._
import sbt.complete.DefaultParsers.spaceDelimited
import sbt.Keys._

/** Checks the real source/POM products without changing publication scope. */
object ReadValidation {
  val exportBuildMatrix =
    inputKey[Unit]("Export the source, shared-library and release matrices")
  val checkReadBuild = taskKey[File](
    "Check sources, source JAR and POM and record clean-owned outputs"
  )

  val matrixSettings: Seq[Setting[_]] = Seq(
    exportBuildMatrix / aggregate := false,
    exportBuildMatrix := {
      val arguments = spaceDelimited("output").parsed
      require(arguments.size == 1, "exportBuildMatrix requires one output path")
      val output = file(arguments.head)
      IO.write(output, Versions.buildMatrixJson)
      streams.value.log.info(s"Exported build matrix: $output")
    }
  )

  val artifactSettings: Seq[Setting[_]] = Seq(
    checkReadBuild / aggregate := false,
    checkReadBuild := {
      (Compile / compile).value
      val sourceFiles = (Compile / sources).value
      require(sourceFiles.nonEmpty, s"No production sources in ${name.value}")
      sourceFiles.foreach(source =>
        require(
          source.isFile && source.length > 0,
          s"Empty or missing source: $source"
        )
      )
      val sourceMappings = (Compile / packageSrc / mappings).value
      val mappedSources = sourceMappings.map(_._1.getCanonicalFile).toSet
      require(
        sourceFiles.forall(source => mappedSources(source.getCanonicalFile)),
        s"Source JAR omits production sources in ${name.value}"
      )
      val sourceJar = (Compile / packageSrc).value
      val jar = new JarFile(sourceJar)
      try {
        val entries =
          jar.entries.asScala.filterNot(_.isDirectory).map(_.getName).toVector
        require(
          entries.distinct.size == entries.size,
          s"Duplicate source JAR entries: $sourceJar"
        )
        sourceMappings.foreach { case (source, entry) =>
          require(entries.contains(entry), s"Missing source JAR entry: $entry")
          val input = jar.getInputStream(jar.getJarEntry(entry))
          try
            require(
              java.util.Arrays
                .equals(IO.readBytes(source), input.readAllBytes()),
              s"Wrong source JAR entry: $entry"
            )
          finally input.close()
        }
        require(
          !entries.exists(_.endsWith(".class")),
          s"Source JAR contains bytecode: $sourceJar"
        )
      } finally jar.close()
      val pomFile = makePom.value
      val pom = XML.loadFile(pomFile)
      val selectedScala = scalaVersion.value
      val selectedBinary = scalaBinaryVersion.value
      val expectedArtifact =
        moduleName.value + (if (crossPaths.value)
                              s"_${scalaBinaryVersion.value}"
                            else "")
      require(
        (pom \ "artifactId").text == expectedArtifact,
        s"Wrong POM artifact: $pomFile"
      )
      val dependencies = pom \ "dependencies" \ "dependency"
      val project = thisProject.value.id
      val sparkDependencies = dependencies.filter(dependency =>
        (dependency \ "groupId").text == "org.apache.spark"
      )
      (if (project == "spark") Some(Versions.selected) else None) match {
        case Some(line) =>
          require(
            sparkDependencies.exists(dependency =>
              (dependency \ "scope").text == "provided"
            ),
            s"Spark module has no provided Spark dependencies: $pomFile"
          )
          sparkDependencies.foreach { dependency =>
            require(
              (dependency \ "version").text == line.spark &&
                Set("provided", "test")((dependency \ "scope").text),
              s"Spark dependency must use ${line.spark} with provided or test scope: $dependency"
            )
          }
        case None =>
          require(
            sparkDependencies.isEmpty,
            s"Shared module depends on Spark: $pomFile"
          )
      }
      dependencies.foreach { dependency =>
        val artifact = (dependency \ "artifactId").text
        val suffix =
          "_(2\\.(?:12|13))$".r.findFirstMatchIn(artifact).map(_.group(1))
        require(
          suffix.forall(_ == selectedBinary),
          s"Dependency Scala suffix differs from $selectedBinary: $dependency"
        )
        if (
          (dependency \ "groupId").text == "org.scala-lang" && artifact == "scala-library"
        )
          require(
            (dependency \ "version").text == selectedScala,
            s"Scala library differs from $selectedScala: $dependency"
          )
      }
      require(
        !dependencies.exists(dependency =>
          (dependency \ "groupId").text == "io.milvus" &&
            (dependency \ "artifactId").text.toLowerCase.contains("sdk")
        ),
        s"POM depends on the Milvus service SDK: $pomFile"
      )
      val root = (ThisBuild / baseDirectory).value
      val output =
        root / "target" / "read-build" / s"$project-${scalaBinaryVersion.value}.json"
      val owned = Seq(
        (Compile / sourceManaged).value,
        (Test / sourceManaged).value,
        (Compile / resourceManaged).value,
        (Test / resourceManaged).value,
        (Compile / classDirectory).value,
        (Test / classDirectory).value,
        sourceJar,
        pomFile
      )
      IO.write(
        output,
        JSONObject(
          Map(
            "project" -> project,
            "scala" -> scalaVersion.value,
            "sourceJar" -> sourceJar.getAbsolutePath,
            "pom" -> pomFile.getAbsolutePath,
            "sources" -> JSONArray(
              sourceFiles.map(_.getAbsolutePath).sorted.toList
            ),
            "cleanOutputs" -> JSONArray(
              owned.map(_.getAbsolutePath).distinct.toList
            )
          )
        ).toString() + "\n"
      )
      streams.value.log.info(
        s"Read build products verified: $project / ${scalaVersion.value}"
      )
      output
    }
  )
}
