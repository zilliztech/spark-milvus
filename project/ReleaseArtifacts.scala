import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.jar.JarFile
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.JavaConverters._
import scala.sys.process.Process
import scala.util.parsing.json.{JSONArray, JSONObject}

import sbt._
import sbt.Keys._

/** Exports immutable JVM release inputs; signing and repository publication are
  * deliberately outside the build. See docs/releasing.md.
  */
object ReleaseArtifacts {
  val ReleaseJvm = config("releaseJvm")
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
          .matches("[0-9]+\\.[0-9]+\\.[0-9]+(?:-[A-Za-z0-9][A-Za-z0-9.-]*)?") &&
          !value.endsWith("-SNAPSHOT"),
        "release.version must be an explicit release version, for example 2.0.0 or 2.0.0-rc.1"
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

  def outputDirectory(root: File, line: Versions.SparkLine): File =
    root / "target" / "release-jvm" / line.id

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
    // Internal products and the locally compiled upstream Knowhere API come
    // from Compile; runtime-only managed dependencies must be included too.
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

  def upstreamSources(root: File): Seq[(File, String)] = {
    val base = root / "knowhere" / "java" / "src" / "main" / "java"
    (base ** "*.java").get.map(source =>
      source -> IO.relativize(base, source).get
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
      scalaBinary == "2.13",
      "The release matrix currently supports Scala 2.13"
    )
    verifyBinary(binary)
    val directory = outputDirectory(root, line)
    IO.createDirectory(directory)
    val stem = s"${artifactId(line, scalaBinary)}-$version"
    require(
      binary == directory / s"$stem.jar",
      "Unexpected JVM assembly output path"
    )
    val sourcesJar = directory / s"$stem-sources.jar"
    archive(sources, sourcesJar)

    val knowhereDocs = directory / "api-work" / "knowhere"
    IO.delete(knowhereDocs)
    IO.createDirectory(knowhereDocs)
    val upstream = upstreamSources(root).map(_._1.getAbsolutePath).sorted
    require(
      upstream.nonEmpty,
      "Knowhere API sources are required for release documentation"
    )
    val result = Process(
      Seq(
        (jdk / "bin" / "javadoc").getAbsolutePath,
        "-quiet",
        "-Xdoclint:none",
        "-d",
        knowhereDocs.getAbsolutePath
      ) ++ upstream,
      root
    ).!
    require(result == 0, "Knowhere API documentation generation failed")
    val allDocumentation = documentation :+ ("knowhere-api" -> knowhereDocs)
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

  private def verifyBinary(file: File): Unit = {
    val jar = new JarFile(file)
    try {
      val names = jar.entries.asScala.map(_.getName).toSet
      Seq(
        "io/knowhere/Knowhere.class",
        "io/milvus/storage/NativeLibraryLoader.class",
        "com/zilliz/spark/connector/sources/MilvusDataSource.class",
        "META-INF/milvus-native/compatibility.properties"
      ).foreach(name =>
        require(names(name), s"JVM release JAR is missing $name")
      )
      val forbidden = names.filter { name =>
        // These JVM dependencies carry portable upstream JNI resources. Keep
        // them where their own loaders expect; neither is a Milvus platform bundle.
        val arrowBridge = name.matches(
          "arrow_cdata_jni/(aarch_64|x86_64)/(libarrow_cdata_jni\\.(so|dylib)|arrow_cdata_jni\\.dll)"
        )
        val grpcTransport = name.matches(
          "META-INF/native/(?:lib)?io_grpc_netty_shaded_netty_(?:tcnative|transport_native_epoll)_[A-Za-z0-9_]+\\.(?:so|dylib|dll)"
        )
        val nativeLibrary = name.endsWith(".so") || name.contains(".so.") ||
          name.endsWith(".dylib") || name.endsWith(".dll")
        name.startsWith(
          "native/"
        ) || (nativeLibrary && !arrowBridge && !grpcTransport) ||
        name.startsWith("org/apache/spark/") || name.startsWith(
          "org/apache/hadoop/"
        ) ||
        name == "META-INF/milvus-native/provenance.json"
      }
      require(
        forbidden.isEmpty,
        s"JVM release JAR contains platform or Spark resources: ${forbidden.toSeq.sorted.mkString(", ")}"
      )
    } finally jar.close()
  }

  private def archive(mappings: Seq[(File, String)], output: File): Unit = {
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

  private def digest(file: File): String = {
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
