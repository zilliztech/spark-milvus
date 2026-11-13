import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.jar.JarFile
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.JavaConverters._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

import sbt._
import sbt.Keys._

/** Exports immutable JVM release inputs; signing and repository publication are
  * deliberately outside the build. See docs/releasing.md.
  */
object ReleaseArtifacts {
  val ReleaseJvm = config("releaseJvm")
  val arrowCDataSupplement = taskKey[Classpath](
    "Verified Arrow C Data platform supplement shared by local and release assemblies"
  )
  val releaseJvmArtifacts = taskKey[Seq[File]](
    "Export a JVM implementation JAR, sources, API documentation and metadata"
  )
  val releaseJvmClasspath = taskKey[Classpath](
    "JVM assembly inputs, independent of the native runtime classpath"
  )
  val releaseJvmSourceMappings = taskKey[Seq[(File, String)]](
    "Sources of every project and upstream API embedded in the JVM artifact"
  )
  val releaseJvmDocumentation = taskKey[Seq[(String, File)]](
    "Generated API documentation for every embedded project"
  )
  val prepareReleaseJvm = taskKey[Unit](
    "Validate the release version and prepare its output directory"
  )

  val requestedVersion: Option[String] =
    sys.props.get("release.version").map { value =>
      require(
        value
          .matches(
            "3\\.[0-9]+\\.[0-9]+(?:-(?:(?:alpha|beta|rc)\\.[0-9]+(?:\\.[0-9]+)*|central-test\\.[0-9]+))?"
          ) && !value.endsWith("-SNAPSHOT"),
        "release.version must be an explicit release version, for example 3.0.0 or 3.0.0-rc.1"
      )
      value
    }

  def requireVersion(): String = requestedVersion.getOrElse(
    sys.error(
      "Set -Drelease.version=<final-version> before exporting release artifacts"
    )
  )

  def artifactId(line: Versions.SparkLine, scalaBinary: String): String =
    s"spark-milvus-${line.id}-jvm_$scalaBinary"

  /** Spark 3.5 has two Scala variants; both must survive candidate staging. */
  def outputDirectory(
      root: File,
      line: Versions.SparkLine,
      scalaBinary: String
  ): File =
    root / "target" / "release-jvm" / line.id / scalaBinary

  /** These families are supplied by the Spark distribution. parquet-avro and
    * Arrow's C Data bridge are not supplied by Spark and remain in the JAR.
    * Exclude transitive copies too, including codecs' native resources.
    */
  def providedBySpark(module: ModuleID): Boolean = {
    val group = module.organization
    val name = module.name
    group == "org.apache.spark" || group == "org.scala-lang" ||
    group == "org.apache.hadoop" || group == "io.netty" ||
    group == "org.slf4j" || group == "org.apache.avro" ||
    (group == "org.apache.parquet" && name != "parquet-avro") ||
    (group == "org.apache.arrow" && name != "arrow-c-data") ||
    group == "com.fasterxml.jackson.core" ||
    (group == "com.fasterxml.jackson.datatype" && name == "jackson-datatype-jsr310") ||
    (group == "com.fasterxml.jackson.module" && name.startsWith(
      "jackson-module-scala_"
    )) ||
    (group == "org.xerial.snappy" && name == "snappy-java") ||
    (group == "com.github.luben" && name == "zstd-jni")
  }

  def classpath(compile: Classpath, runtimeManaged: Classpath): Classpath = {
    // Internal products come from Compile; include managed runtime dependencies.
    val products = compile.filter(entry =>
      entry.data.isDirectory || entry.get(moduleID.key).isEmpty
    )
    val libraries =
      runtimeManaged.filterNot(_.get(moduleID.key).exists(providedBySpark))
    val seen = scala.collection.mutable.Set.empty[File]
    (products ++ libraries).filter(entry =>
      seen.add(entry.data.getCanonicalFile)
    )
  }

  /** Apache Arrow 12 does not publish its Linux ARM64 C Data JNI resource. The
    * separately audited supplement preserves Arrow's own resource loader and
    * must never change another Spark line's Arrow implementation.
    */
  def supplementClasspath(
      root: File,
      line: Versions.SparkLine,
      log: Logger
  ): Classpath = {
    val configured = sys.props.get("arrow.cdata.supplement")
    if (line.id == "3.5") {
      val path = file(
        configured.getOrElse(
          sys.error(
            "Spark 3.5 packaging requires -Darrow.cdata.supplement=/absolute/path/to/arrow-cdata-12-linux-aarch64.jar"
          )
        )
      )
      require(
        path.isAbsolute && path.isFile,
        "Arrow C Data supplement must be an existing absolute JAR path"
      )
      verifySupplement(root, path, embedded = false, log = log)
      Seq(Attributed.blank(path))
    } else {
      require(
        configured.isEmpty,
        "The Arrow 12 supplement is only valid for Spark 3.5"
      )
      Seq.empty
    }
  }

  private def verifySupplement(
      root: File,
      path: File,
      embedded: Boolean,
      log: Logger
  ): Unit = {
    val arguments = Seq(
      "python3",
      (root / "scripts" / "ci" / "arrow_cdata.py").getAbsolutePath,
      "verify",
      "--input",
      path.getAbsolutePath
    ) ++ (if (embedded) Seq("--embedded") else Seq.empty)
    require(
      Process(arguments, root, "LD_PRELOAD" -> "")
        .!(ProcessLogger(log.info(_), log.error(_))) == 0,
      "Arrow C Data supplement validation failed"
    )
  }

  def export(
      root: File,
      line: Versions.SparkLine,
      scalaVersion: String,
      scalaBinary: String,
      binary: File,
      sources: Seq[(File, String)],
      documentation: Seq[(String, File)],
      resolvedClasspath: Classpath,
      bundledClasspath: Classpath,
      jdk: File,
      log: Logger
  ): Seq[File] = {
    val version = requireVersion()
    require(
      line.scalas.contains(scalaVersion) &&
        scalaBinary == scalaVersion.split('.').take(2).mkString("."),
      s"Unsupported release combination: Spark ${line.id}, Scala $scalaVersion ($scalaBinary)"
    )
    verifyBinary(root, binary)
    if (line.id == "3.5")
      verifySupplement(root, binary, embedded = true, log = log)
    val directory = outputDirectory(root, line, scalaBinary)
    IO.createDirectory(directory)
    val stem = s"${artifactId(line, scalaBinary)}-$version"
    require(
      binary == directory / s"$stem.jar",
      "Unexpected JVM assembly output path"
    )
    val sourcesJar = directory / s"$stem-sources.jar"
    archive(sources, sourcesJar)

    val allDocumentation = documentation
    val index = directory / "api-work" / "index.html"
    IO.write(
      index,
      "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\">" +
        s"<title>Milvus Spark ${line.id} API $version</title>" +
        s"<h1>Milvus Spark ${line.id} API $version</h1><ul>" +
        allDocumentation.map { case (name, _) =>
          s"""<li><a href="api/$name/index.html">$name</a></li>"""
        }.mkString + "</ul></html>"
    )
    val docMappings = allDocumentation.flatMap { case (name, base) =>
      require(
        (base / "index.html").isFile,
        s"Missing generated documentation for $name"
      )
      (base ** "*").get.filter(_.isFile).map { source =>
        source -> s"api/$name/${IO.relativize(base, source).get}"
      }
    }
    val documentationJar = directory / s"$stem-javadoc.jar"
    archive(docMappings :+ (index -> "index.html"), documentationJar)
    IO.delete(directory / "api-work")

    val bundledFiles = bundledClasspath.map(_.data.getCanonicalFile).toSet
    val provided = resolvedClasspath.filterNot(entry =>
      bundledFiles(entry.data.getCanonicalFile)
    )
    val files =
      Seq("" -> binary, "sources" -> sourcesJar, "javadoc" -> documentationJar)
    val sourceSha =
      Process(Seq("git", "rev-parse", "HEAD"), root, "LD_PRELOAD" -> "").!!.trim
    require(
      sourceSha.matches("[a-f0-9]{40}"),
      "Release source must have an exact Git commit"
    )
    val metadata = JSONObject(
      Map(
        "formatVersion" -> 1,
        "groupId" -> "com.zilliz",
        "artifactId" -> artifactId(line, scalaBinary),
        "version" -> version,
        "sparkLine" -> line.id,
        "sparkVersion" -> line.spark,
        "scalaVersion" -> scalaVersion,
        "scalaBinaryVersion" -> scalaBinary,
        "javaRelease" -> line.javaRelease,
        "sourceSha" -> sourceSha,
        "files" -> JSONArray(files.map { case (classifier, file) =>
          JSONObject(
            Map(
              "classifier" -> classifier,
              "extension" -> "jar",
              "path" -> file.getName,
              "sha256" -> digest(file),
              "size" -> file.length
            )
          )
        }.toList),
        "providedDependencies" -> dependencies(provided),
        "bundledDependencies" -> dependencies(bundledClasspath),
        // Source-built native components have no Maven coordinates. Preserve
        // their identity separately so dependency and license audits see them.
        "bundledComponents" -> bundledComponents(binary, line),
        // Every non-provided JVM dependency is embedded by this assembly.
        "runtimeDependencies" -> JSONArray(Nil)
      )
    )
    val metadataFile = directory / "artifact.json"
    IO.write(metadataFile, metadata.toString() + "\n", StandardCharsets.UTF_8)
    log.info(s"Exported JVM release candidate: $directory")
    files.map(_._2) :+ metadataFile
  }

  private def dependencies(classpath: Classpath): JSONArray = {
    // Compile precedes Runtime in the resolved classpath: it includes Spark's
    // version selection. A POM must name each group/artifact only once.
    val modules = classpath
      .filter(_.data.isFile)
      .flatMap(_.get(moduleID.key))
      .groupBy(module => (module.organization, module.name))
      .toSeq
      .sortBy(_._1)
    JSONArray(modules.map { case ((group, artifact), candidates) =>
      JSONObject(
        Map(
          "groupId" -> group,
          "artifactId" -> artifact,
          "version" -> candidates.head.revision
        )
      )
    }.toList)
  }

  private def jsonObject(
      contents: String,
      description: String
  ): Map[String, Any] =
    JSON.parseFull(contents) match {
      case Some(value: Map[_, _]) => value.asInstanceOf[Map[String, Any]]
      case _ => sys.error(s"Invalid JSON object: $description")
    }

  private def bundledComponents(
      binary: File,
      line: Versions.SparkLine
  ): JSONArray = {
    if (line.id != "3.5") JSONArray(Nil)
    else {
      val prefix = "META-INF/spark-milvus/arrow-cdata/12.0.1/"
      val provenance = prefix + "linux-aarch64.json"
      val jar = new JarFile(binary)
      try {
        val input = jar.getInputStream(jar.getJarEntry(provenance))
        val contents =
          try IO.readStream(input)
          finally input.close()
        val record = jsonObject(contents, provenance)
        val source = record("source").asInstanceOf[Map[String, Any]]
        val library = record("library").asInstanceOf[Map[String, Any]]
        JSONArray(
          List(
            JSONObject(
              Map(
                "type" -> "source-built-native",
                "name" -> "Apache Arrow C Data JNI",
                "version" -> source("version"),
                "platform" -> record("platform"),
                "resource" -> library("path"),
                "sha256" -> library("sha256"),
                "size" -> library("size").asInstanceOf[Double].toLong,
                "provenance" -> provenance,
                "licenses" -> JSONArray(
                  List(
                    prefix + "source/LICENSE.txt",
                    prefix + "source/NOTICE.txt"
                  )
                ),
                "source" -> JSONObject(source)
              )
            )
          )
        )
      } finally jar.close()
    }
  }

  private def verifyBinary(root: File, file: File): Unit = {
    // Export and final publication enforce the same four-module and native
    // resource contract. A package prefix alone cannot prove a module exists.
    val contractFile = root / "project" / "release-jvm-contract.json"
    val contract = jsonObject(IO.read(contractFile), contractFile.getPath)
    require(
      contract("formatVersion") == 1.0,
      "Unsupported JVM release contract"
    )
    val required =
      contract("requiredEntries").asInstanceOf[Map[String, List[String]]]
    val allowedNative =
      contract("allowedNativeResources").asInstanceOf[List[String]]
    val nativePattern = contract("nativeResourcePattern").asInstanceOf[String]
    val forbiddenPrefixes =
      contract("forbiddenPrefixes").asInstanceOf[List[String]]
    val forbiddenEntries =
      contract("forbiddenEntries").asInstanceOf[List[String]].toSet
    val jar = new JarFile(file)
    try {
      val names =
        jar.entries.asScala.filterNot(_.isDirectory).map(_.getName).toSet
      required.toSeq.sortBy(_._1).foreach { case (module, entries) =>
        entries.foreach(name =>
          require(
            names(name),
            s"JVM release JAR is missing $module entry: $name"
          )
        )
      }
      val forbidden = names.filter { name =>
        forbiddenPrefixes.exists(name.startsWith) || forbiddenEntries(name) ||
        (name.matches(nativePattern) && !allowedNative.exists(name.matches))
      }
      require(
        forbidden.isEmpty,
        s"JVM release JAR contains platform or Spark resources: ${forbidden.toSeq.sorted.mkString(", ")}"
      )
    } finally jar.close()
  }

  def archive(mappings: Seq[(File, String)], output: File): Unit = {
    require(mappings.nonEmpty, s"Cannot publish an empty archive: $output")
    val entries = mappings.groupBy(_._2).toSeq.sortBy(_._1).map {
      case (name, alternatives) =>
        require(
          !name.startsWith("/") && !name.split('/').contains(".."),
          s"Invalid archive path: $name"
        )
        require(
          alternatives.map(pair => digest(pair._1)).distinct.size == 1,
          s"Conflicting release archive entry: $name"
        )
        alternatives.head
    }
    val zip = new ZipOutputStream(new FileOutputStream(output))
    try
      entries.foreach { case (source, name) =>
        val entry = new ZipEntry(name)
        entry.setTime(315532800000L)
        zip.putNextEntry(entry)
        val input = new FileInputStream(source)
        try transfer(input, bytes => zip.write(bytes._1, 0, bytes._2))
        finally input.close()
        zip.closeEntry()
      }
    finally zip.close()
  }

  def digest(file: File): String = {
    val hash = MessageDigest.getInstance("SHA-256")
    val input = new FileInputStream(file)
    try transfer(input, bytes => hash.update(bytes._1, 0, bytes._2))
    finally input.close()
    hash.digest.map(value => f"${value & 0xff}%02x").mkString
  }

  private def transfer(
      input: FileInputStream,
      consume: ((Array[Byte], Int)) => Unit
  ): Unit = {
    val buffer = new Array[Byte](65536)
    var count = input.read(buffer)
    while (count != -1) {
      consume(buffer -> count)
      count = input.read(buffer)
    }
  }
}
