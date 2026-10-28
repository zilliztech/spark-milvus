import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util.jar.{Attributes, JarFile, Manifest}
import scala.collection.JavaConverters._
import scala.sys.process.Process
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.xml.{Elem, Text, XML}

import sbt._
import sbt.Keys._

/** Exports the existing module graph as independently consumable JVM artifacts.
  * Only read/search modules belong in the public candidate.
  */
object ThinArtifacts {
  val releaseThinArtifacts = taskKey[Seq[File]](
    "Export Spark 4.0 thin module JARs, sources, documentation, POMs and metadata"
  )
  val thinModule = taskKey[Module]("Collect one module's thin release inputs")

  final case class Module(
      projectId: String,
      artifactId: String,
      javaRelease: String,
      binary: File,
      sources: File,
      documentation: File,
      pom: File,
      providedDependencies: Set[(String, String)]
  )

  def settings(projectId: String, javaRelease: String): Seq[Setting[_]] = Seq(
    thinModule := Module(
      projectId,
      moduleName.value + (if (crossPaths.value) s"_${scalaBinaryVersion.value}"
                          else ""),
      javaRelease,
      (Compile / packageBin).value,
      (Compile / packageSrc).value,
      (Compile / doc).value,
      makePom.value,
      libraryDependencies.value
        .filter(_.configurations.exists(_.split(',').contains("provided")))
        .map(CrossVersion(scalaVersion.value, scalaBinaryVersion.value))
        .map(module => module.organization -> module.name)
        .toSet
    )
  )

  def outputDirectory(root: File): File =
    root / "target" / "release-thin" / "4.0"

  def export(
      root: File,
      line: Versions.SparkLine,
      scalaVersion: String,
      scalaBinary: String,
      modules: Seq[Module],
      resolvedClasspath: Classpath,
      knowhereApi: File,
      jdk: File,
      log: Logger
  ): Seq[File] = {
    val version = ReleaseArtifacts.requireVersion()
    val sourceSha =
      Process(Seq("git", "rev-parse", "HEAD"), root, "LD_PRELOAD" -> "").!!.trim
    require(
      sourceSha.matches("[a-f0-9]{40}"),
      "Thin release source must have an exact Git commit"
    )
    require(
      line.id == "4.0" && scalaBinary == "2.13",
      "Thin releases currently support Spark 4.0 / Scala 2.13"
    )
    val expected = Set(
      "native-runtime",
      "native-storage",
      "native-vector",
      "core",
      "spark"
    )
    require(
      modules
        .map(_.projectId)
        .toSet == expected && modules.size == expected.size,
      "The thin candidate must include the complete five-module closure"
    )
    val destination = outputDirectory(root)
    IO.createDirectory(destination)
    val work = destination / "export-work"
    IO.delete(work)
    IO.createDirectory(work)
    val poms =
      modules.map(module => module.projectId -> XML.loadFile(module.pom)).toMap
    val names = modules.map { module =>
      val id =
        if (module.projectId == "spark")
          s"spark-milvus-${line.id}-thin_$scalaBinary"
        else module.artifactId
      module.projectId -> id
    }.toMap
    require(
      names.values.toSet.size == modules.size,
      "Duplicate artifact identities in thin release outputs"
    )
    val internal = modules.map { module =>
      val pom = poms(module.projectId)
      ((pom \ "groupId").text, (pom \ "artifactId").text) -> names(
        module.projectId
      )
    }.toMap
    require(
      internal.size == modules.size,
      "Duplicate module identities in thin release inputs"
    )
    // Compile precedes managed Runtime, matching the distribution selection
    // used for the JVM release. No unmanaged native bundle is inspected.
    val resolved = resolvedClasspath
      .flatMap(_.get(moduleID.key))
      .groupBy(module => module.organization -> module.name)
      .map { case (key, candidates) =>
        key -> candidates.head
      }
    val exported = modules.map { module =>
      val artifact = names(module.projectId)
      val directory = destination / artifact
      IO.createDirectory(directory)
      val stem = s"$artifact-$version"
      val moduleWork = work / module.projectId
      val binaryInputs = unpack(module.binary, moduleWork / "binary")
      val apiInputs =
        if (module.projectId == "native-vector")
          unpack(knowhereApi, moduleWork / "knowhere-api").filterNot(
            _._2 == "META-INF/MANIFEST.MF"
          )
        else Seq.empty
      val manifestFile = moduleWork / "binary" / "META-INF" / "MANIFEST.MF"
      val input = new FileInputStream(manifestFile)
      val manifest =
        try new Manifest(input)
        finally input.close()
      manifest.getMainAttributes.put(
        Attributes.Name.IMPLEMENTATION_VERSION,
        version
      )
      manifest.getMainAttributes.put(
        Attributes.Name.SPECIFICATION_VERSION,
        version
      )
      val manifestOutput = new FileOutputStream(manifestFile)
      try manifest.write(manifestOutput)
      finally manifestOutput.close()
      val binary = directory / s"$stem.jar"
      ReleaseArtifacts.archive(binaryInputs ++ apiInputs, binary)
      verifyBinary(binary, module.projectId)

      val sources = directory / s"$stem-sources.jar"
      val upstreamSources =
        if (module.projectId == "native-vector")
          ReleaseArtifacts.upstreamSources(root)
        else Seq.empty
      ReleaseArtifacts.archive(
        unpack(module.sources, moduleWork / "sources") ++ upstreamSources,
        sources
      )
      val documentation = directory / s"$stem-javadoc.jar"
      require(
        (module.documentation / "index.html").isFile,
        s"Missing API documentation: ${module.projectId}"
      )
      if (module.projectId == "native-vector") {
        val knowhereDocs = ReleaseArtifacts.upstreamDocumentation(
          root,
          jdk,
          moduleWork / "knowhere-doc"
        )
        val index = moduleWork / "index.html"
        IO.write(
          index,
          "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>Native vector API</title><h1>Native vector API</h1><ul><li><a href=\"api/native-vector/index.html\">Connector adapters</a></li><li><a href=\"api/knowhere/index.html\">Knowhere Java API</a></li></ul></html>"
        )
        ReleaseArtifacts.archive(
          files(module.documentation, "api/native-vector/") ++ files(
            knowhereDocs,
            "api/knowhere/"
          ) :+ (index -> "index.html"),
          documentation
        )
      } else
        ReleaseArtifacts.archive(files(module.documentation), documentation)

      val pom = rewritePom(
        poms(module.projectId),
        artifact,
        version,
        internal,
        resolved,
        module.providedDependencies,
        module.projectId,
        sourceSha
      )
      val pomFile = directory / s"$stem.pom"
      XML.save(pomFile.getAbsolutePath, pom, "UTF-8", xmlDecl = true)
      val outputs = Seq(
        ("", "jar", binary),
        ("sources", "jar", sources),
        ("javadoc", "jar", documentation),
        ("", "pom", pomFile)
      )
      val dependencies = (pom \ "dependencies" \ "dependency").map {
        dependency =>
          val identity = JSONObject(
            Map(
              "groupId" -> (dependency \ "groupId").text,
              "artifactId" -> (dependency \ "artifactId").text,
              "version" -> (dependency \ "version").text
            )
          )
          (dependency \ "scope").text -> identity
      }
      val metadata = JSONObject(
        Map(
          "projectId" -> module.projectId,
          "artifactId" -> artifact,
          "javaRelease" -> module.javaRelease,
          "files" -> JSONArray(outputs.map {
            case (classifier, extension, output) =>
              JSONObject(
                Map(
                  "classifier" -> classifier,
                  "extension" -> extension,
                  "path" -> IO.relativize(destination, output).get,
                  "sha256" -> ReleaseArtifacts.digest(output),
                  "size" -> output.length
                )
              )
          }.toList),
          "runtimeDependencies" -> JSONArray(
            dependencies.filterNot(_._1 == "provided").map(_._2).toList
          ),
          "providedDependencies" -> JSONArray(
            dependencies.filter(_._1 == "provided").map(_._2).toList
          )
        )
      )
      metadata -> outputs.map(_._3)
    }
    val classes = exported.flatMap { case (_, outputs) =>
      val binary = outputs.head
      val jar = new JarFile(binary)
      try
        jar.entries.asScala
          .map(_.getName)
          .filter(_.endsWith(".class"))
          .map(_ -> binary.getName)
          .toVector
      finally jar.close()
    }
    val duplicates = classes
      .groupBy(_._1)
      .collect {
        case (name, owners) if owners.size > 1 =>
          s"$name: ${owners.map(_._2).mkString(", ")}"
      }
      .toSeq
      .sorted
    require(
      duplicates.isEmpty,
      s"Thin modules contain duplicate classes: ${duplicates.mkString("; ")}"
    )
    val metadata = JSONObject(
      Map(
        "schemaVersion" -> 1,
        "kind" -> "thin",
        "groupId" -> "com.zilliz",
        "version" -> version,
        "sourceSha" -> sourceSha,
        "sparkLine" -> line.id,
        "sparkVersion" -> line.spark,
        "scalaVersion" -> scalaVersion,
        "scalaBinaryVersion" -> scalaBinary,
        "modules" -> JSONArray(exported.map(_._1).toList)
      )
    )
    val metadataFile = destination / "thin-artifacts.json"
    IO.write(metadataFile, metadata.toString() + "\n", StandardCharsets.UTF_8)
    IO.delete(work)
    log.info(s"Exported read-only thin release candidate: $destination")
    exported.flatMap(_._2) :+ metadataFile
  }

  private def rewritePom(
      original: Elem,
      artifact: String,
      version: String,
      internal: Map[(String, String), String],
      resolved: Map[(String, String), ModuleID],
      declaredProvided: Set[(String, String)],
      projectId: String,
      sourceSha: String
  ): Elem = {
    val sparkModule = projectId == "spark"
    val dependencies = (original \ "dependencies" \ "dependency")
      .collect {
        // sbt writes `provided,test` dependencies as test in makePom. Retain
        // their production API contract while removing genuine test libraries.
        case dependency: Elem
            if (dependency \ "scope").text != "test" || declaredProvided(
              (dependency \ "groupId").text -> (dependency \ "artifactId").text
            ) =>
          val identity =
            (dependency \ "groupId").text -> (dependency \ "artifactId").text
          internal.get(identity) match {
            case Some(name) =>
              fields(
                dependency,
                Map(
                  "groupId" -> "com.zilliz",
                  "artifactId" -> name,
                  "version" -> version,
                  "scope" -> "compile"
                )
              )
            case None =>
              val selected = resolved.getOrElse(
                identity,
                sys.error(
                  s"Thin POM dependency is not on the resolved classpath: $identity"
                )
              )
              val declaredScope = (dependency \ "scope").text
              // Lower modules keep their standalone library dependencies. Only
              // the Spark surface can rely on its distribution to provide them.
              val scope =
                if (
                  (sparkModule && ReleaseArtifacts.providedBySpark(
                    selected
                  )) || declaredScope == "provided" || declaredProvided(
                    identity
                  )
                ) "provided"
                else if (declaredScope == "runtime") "runtime"
                else "compile"
              fields(
                dependency,
                Map("version" -> selected.revision, "scope" -> scope)
              )
          }
      }
      .groupBy(dependency =>
        (
          (dependency \ "groupId").text,
          (dependency \ "artifactId").text,
          (dependency \ "classifier").text,
          (dependency \ "type").text
        )
      )
      .toSeq
      .sortBy(_._1)
      .map { case (_, alternatives) =>
        val selected = alternatives.minBy(dependency =>
          if ((dependency \ "scope").text == "compile") 0 else 1
        )
        require(
          alternatives
            .map(dependency => (dependency \ "version").text)
            .distinct
            .size == 1,
          "Conflicting versions in thin POM"
        )
        selected
      }
    val header = fields(
      original,
      Map(
        "groupId" -> "com.zilliz",
        "artifactId" -> artifact,
        "version" -> version,
        "name" -> artifact,
        "description" -> publicationDescription(projectId),
        "url" -> "https://github.com/zilliztech/spark-milvus"
      )
    )
    require(
      (header \ "scm").size == 1,
      "Thin POM requires one SCM metadata block"
    )
    header.copy(child = header.child.map {
      case node: Elem if node.label == "dependencies" =>
        node.copy(child = dependencies)
      case node: Elem if node.label == "scm" =>
        fields(
          node,
          Map(
            "url" -> "https://github.com/zilliztech/spark-milvus",
            "connection" -> "scm:git:https://github.com/zilliztech/spark-milvus.git",
            "developerConnection" -> "scm:git:ssh://git@github.com/zilliztech/spark-milvus.git",
            "tag" -> sourceSha
          )
        )
      case node => node
    })
  }

  private def publicationDescription(projectId: String): String = {
    val purpose = projectId match {
      case "native-runtime" =>
        "Shared native loader and source compatibility checks."
      case "native-storage" =>
        "Milvus Storage JVM bindings for segment file access."
      case "native-vector" =>
        "Knowhere Java bindings and vector search/index adapters."
      case "core" =>
        "Milvus V3 schemas, storage formats and read/search computation."
      case "spark" =>
        "Spark 4.0 read-only DataSource and vector search interfaces."
      case other => sys.error(s"Unknown thin module: $other")
    }
    s"Read-only Spark Milvus thin module. $purpose"
  }

  private def fields(element: Elem, replacements: Map[String, String]): Elem = {
    val existing = element.child.collect { case child: Elem =>
      child.label
    }.toSet
    element.copy(child =
      element.child.map {
        case child: Elem if replacements.contains(child.label) =>
          child.copy(child = Seq(Text(replacements(child.label))))
        case child => child
      } ++ replacements.toSeq
        .filterNot(pair => existing(pair._1))
        .sortBy(_._1)
        .map { case (name, value) =>
          Elem(
            null,
            name,
            scala.xml.Null,
            element.scope,
            minimizeEmpty = false,
            Text(value)
          )
        }
    )
  }

  private def unpack(archive: File, directory: File): Seq[(File, String)] = {
    IO.unzip(archive, directory)
    files(directory)
  }

  private def files(directory: File, prefix: String = ""): Seq[(File, String)] =
    (directory ** "*").get
      .filter(_.isFile)
      .map(file => file -> (prefix + IO.relativize(directory, file).get))

  private def verifyBinary(binary: File, projectId: String): Unit = {
    val jar = new JarFile(binary)
    try {
      val names = jar.entries.asScala.map(_.getName).toSet
      val forbidden = names.filter(name =>
        name.startsWith("native/") || name.startsWith(
          "org/apache/spark/"
        ) || name == "META-INF/milvus-native/provenance.json" || name.endsWith(
          ".so"
        ) || name.contains(".so.") || name.endsWith(".dylib") || name.endsWith(
          ".dll"
        )
      )
      require(
        forbidden.isEmpty,
        s"Thin JAR contains native or Spark resources: ${forbidden.toSeq.sorted.mkString(", ")}"
      )
      if (projectId == "native-vector")
        require(
          names("io/knowhere/Knowhere.class"),
          "Thin native-vector JAR must contain the Knowhere Java API"
        )
      if (projectId == "native-runtime")
        require(
          names("META-INF/milvus-native/compatibility.properties"),
          "Thin native-runtime JAR must preserve source compatibility metadata"
        )
    } finally jar.close()
  }
}
