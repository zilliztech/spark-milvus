import java.io.{ByteArrayOutputStream, File, FileInputStream, InputStream}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption, StandardOpenOption}
import java.security.MessageDigest
import java.util.jar.JarFile
import java.util.Properties
import scala.collection.JavaConverters._
import scala.sys.process.Process
import scala.util.parsing.json.JSON

import sbt._
import sbt.Keys._
import sbtassembly.{CustomMergeStrategy, MergeStrategy}
import sbtassembly.Assembly.JarEntry

/** A platform JAR containing one dependency graph for both upstream JNI APIs.
  */
object NativeBundle {
  // The DiskANN acceptance fixture exists where DiskANN does, which the bundle
  // declares in `with_diskann` rather than the platform name implying it.
  private def knowhereCApiTests(withDiskann: Boolean) = Vector(
    "knowhere_c_api",
    "knowhere_c_api_concurrency"
  ) ++ (if (withDiskann) Vector("knowhere_c_api_diskann_acceptance")
        else Vector.empty)
  private def jvmLoadEntries = NativePlatform.jvmLoadEntries(platform)
  private def auditDlopenEntries = NativePlatform.auditDlopenEntries(platform)
  private def cardinalPlugins = NativePlatform.cardinalPlugins(platform)

  val nativeBundleJar = settingKey[Option[File]](
    "Unified native platform JAR selected by -Dmilvus.native.bundle"
  )
  val verifyNativeBundle = taskKey[File](
    "Verify the unified native platform JAR, source pins and JVM loading"
  )
  val validatedNativeBundle = taskKey[Option[File]](
    "Validate the selected native bundle once for dependent build tasks"
  )

  // sbt-assembly's default deduplicate buffers each complete entry, even when
  // there is only one origin. Native libraries can be hundreds of megabytes.
  val nativeMergeStrategy: MergeStrategy = CustomMergeStrategy(
    "NativeDeduplicate"
  ) { conflicts =>
    val first = conflicts.head
    if (conflicts.size == 1)
      Right(Vector(JarEntry(first.target, first.stream)))
    else {
      val expected = streamFingerprint(first.stream())
      if (
        conflicts.tail
          .forall(entry => streamFingerprint(entry.stream()) == expected)
      )
        Right(Vector(JarEntry(first.target, first.stream)))
      else
        Left(
          "Native resources have different contents:\n" + conflicts.mkString(
            "\n"
          )
        )
    }
  }

  def selected: Option[File] = sys.props.get("milvus.native.bundle").map(file)

  val settings: Seq[Setting[_]] = Seq(
    nativeBundleJar := selected,
    validatedNativeBundle := {
      val root = (ThisBuild / baseDirectory).value
      val output = target.value
      val log = streams.value.log
      nativeBundleJar.value.map { jar =>
        validate(jar, root, output, log)
        jar
      }
    },
    verifyNativeBundle := validatedNativeBundle.value.getOrElse(
      sys.error(
        "Select a platform JAR with -Dmilvus.native.bundle=/absolute/path/to/bundle.jar"
      )
    ),
    Compile / unmanagedJars ++= validatedNativeBundle.value.toSeq.map(
      Attributed.blank
    )
  )

  def platform: String = NativePlatform.current

  private def properties(input: InputStream): Properties = {
    val result = new Properties() {
      override def put(key: Object, value: Object): Object = {
        require(!containsKey(key), s"Duplicate native property: $key")
        super.put(key, value)
      }
    }
    try result.load(input)
    finally input.close()
    result
  }

  def metadata(jar: File): Properties = {
    val archive = new JarFile(jar)
    try {
      val name = s"native/milvus/1/$platform/manifest.properties"
      val entry = Option(archive.getJarEntry(name))
        .getOrElse(sys.error(s"Missing $name in $jar"))
      properties(archive.getInputStream(entry))
    } finally archive.close()
  }

  def storageRevision(repositoryRoot: File): String = {
    val source = repositoryRoot / "milvus-storage"
    require(
      (source / ".git").exists,
      "milvus-storage submodule is not initialized. Run `git submodule update --init milvus-storage`."
    )
    val head = gitOutput(source, Seq("rev-parse", "HEAD"))
    require(
      head.matches("[a-f0-9]{40}"),
      s"milvus-storage submodule HEAD is not a full Git commit: $head"
    )
    val gitlink = gitOutput(
      repositoryRoot,
      Seq("ls-files", "--stage", "--", "milvus-storage")
    ).split("\\r?\\n").filter(_.nonEmpty).toVector
    require(
      gitlink.size == 1,
      "milvus-storage must be recorded as one Git submodule entry"
    )
    val fields = gitlink.head.split("\\s+", 4)
    require(
      fields.length == 4 && fields(0) == "160000" && fields(2) == "0" &&
        fields(1).matches("[a-f0-9]{40}") && fields(3) == "milvus-storage",
      s"Invalid milvus-storage Git submodule entry: ${gitlink.head}"
    )
    require(
      fields(1) == head,
      s"milvus-storage submodule HEAD $head does not match the recorded gitlink ${fields(1)}"
    )
    fields(1)
  }

  def validate(jar: File, root: File, target: File, log: Logger): Unit = {
    require(
      jar.isAbsolute && jar.isFile,
      "milvus.native.bundle must name an existing absolute JAR path"
    )
    val sidecar = properties(
      new FileInputStream(jar.getAbsolutePath + ".properties")
    )
    val jarHash = digest(jar)
    require(
      sidecar.getProperty("jar.sha256") == jarHash,
      s"Native bundle checksum mismatch: $jar"
    )
    val knowhereRevision = KnowhereBuild.knowhereRevision(root)
    val storageRevision = NativeBundle.storageRevision(root)
    val archive = new JarFile(jar)
    try {
      val prefix = s"native/milvus/1/$platform/"
      val entries = archive.entries().asScala.filterNot(_.isDirectory).toVector
      require(
        entries.map(_.getName).distinct.size == entries.size,
        "Duplicate native bundle ZIP entries"
      )
      require(
        entries.forall(entry => !entry.getName.endsWith(".class")),
        "Native bundle must contain resources only"
      )
      require(
        entries
          .filter(_.getName.startsWith("native/"))
          .forall(_.getName.startsWith(prefix)),
        s"Native bundle contains resources outside $prefix"
      )
      val manifest = metadata(jar)
      require(
        manifest.getProperty("format.version") == "1",
        "Unsupported native bundle manifest version"
      )
      require(
        manifest.getProperty("platform") == platform,
        s"Native bundle platform must be $platform"
      )
      require(
        manifest.getProperty("knowhere.revision") == knowhereRevision,
        s"Native bundle must be rebuilt for Knowhere $knowhereRevision"
      )
      require(
        manifest.getProperty("storage.revision") == storageRevision,
        s"Native bundle must be rebuilt for storage $storageRevision"
      )
      require(
        Set("true", "false")(manifest.getProperty("with_cardinal", "")),
        "Invalid Cardinal build feature"
      )
      require(
        Set("true", "false")(manifest.getProperty("with_diskann", "")),
        "Invalid DiskANN build feature"
      )
      def names(key: String): Vector[String] = {
        val value = manifest.getProperty(key, "")
        val names =
          if (value.isEmpty) Vector.empty else value.split(",", -1).toVector
        require(
          names.distinct.size == names.size && names.forall(validPath),
          s"Invalid $key in native bundle"
        )
        names
      }
      val libraries = names("libraries")
      val aliases = names("aliases")
      val loadEntries = names("load.entries")
      require(
        loadEntries == jvmLoadEntries,
        "Invalid JVM load entries in native bundle"
      )
      require(
        libraries.nonEmpty && libraries.toSet.intersect(aliases.toSet).isEmpty,
        "Native library and alias names must be distinct"
      )
      val allNames = libraries.toSet ++ aliases
      require(
        !allNames(NativePlatform.systemZlib(platform)),
        "System zlib must not be included in the native bundle"
      )
      aliases.foreach { alias =>
        require(
          libraries.contains(manifest.getProperty(s"alias.$alias")),
          s"Invalid alias target: $alias"
        )
      }
      validateProvenance(
        archive,
        manifest,
        storageRevision,
        knowhereRevision,
        platform,
        libraries,
        aliases
      )
      val nativeEntries = entries
        .map(_.getName)
        .filter(_.startsWith(prefix))
        .map(_.stripPrefix(prefix))
        .toSet
      require(
        nativeEntries == libraries.toSet + "manifest.properties",
        "Unlisted or missing native bundle resources"
      )
      val directory = target / "native-bundle" / jarHash
      val stamp = directory / "validated"
      val checker = root / "native-build" / "jvm_load.py"
      val javaChecker = root / "native-build" / "NativeLoadCheck.java"
      val stampValue =
        s"jvm-load-v1\n$jarHash\n${auditDlopenEntries.mkString(",")}\n" +
          s"${digest(checker)}\n${digest(javaChecker)}\n" +
          s"${System.getProperty("java.home")}\n${System.getProperty("java.version")}\n"
      // The source checks above run on every invocation; immutable archive bytes
      // together with the checker and JRE determine the cached JVM load result.
      withValidationLock(directory) {
        if (!stamp.isFile || IO.read(stamp) != stampValue) {
          IO.delete(directory)
          IO.createDirectory(directory)
          val lib = directory / "lib"
          IO.createDirectory(lib)
          libraries.foreach { name =>
            val entry = Option(archive.getJarEntry(prefix + name))
              .getOrElse(sys.error(s"Missing native library: $name"))
            val output = lib / name
            IO.createDirectory(output.getParentFile)
            val input = archive.getInputStream(entry)
            try
              Files.copy(
                input,
                output.toPath,
                StandardCopyOption.REPLACE_EXISTING
              )
            finally input.close()
            val expected = manifest.getProperty(s"sha256.$name", "")
            require(
              expected.matches("[a-f0-9]{64}") && digest(output) == expected,
              s"Native library checksum mismatch: $name"
            )
          }
          aliases.foreach { name =>
            val path = (lib / name).toPath
            Files.createDirectories(path.getParent)
            Files.createLink(
              path,
              (lib / manifest.getProperty(s"alias.$name")).toPath
            )
          }
          NativeLibraries.validateUnifiedLinux(
            lib,
            root,
            message => log.info(message)
          )
          IO.write(stamp, stampValue)
        }
      }
      log.info(s"Verified unified native bundle: $jar")
    } finally archive.close()
  }

  def validateProvenance(
      archive: JarFile,
      manifest: Properties,
      storageRevision: String,
      knowhereRevision: String,
      expectedPlatform: String,
      libraries: Vector[String],
      aliases: Vector[String]
  ): Unit = {
    val name = "META-INF/milvus-native/provenance.json"
    val entry = Option(archive.getJarEntry(name))
      .filterNot(_.isDirectory)
      .getOrElse(sys.error(s"Missing $name in native bundle"))
    require(
      entry.getSize > 0 && entry.getSize <= 16L * 1024 * 1024,
      "Invalid native provenance size"
    )
    val input = archive.getInputStream(entry)
    val output = new ByteArrayOutputStream(entry.getSize.toInt)
    try {
      val buffer = new Array[Byte](65536)
      var count = input.read(buffer)
      while (count != -1) {
        output.write(buffer, 0, count)
        require(
          output.size() <= 16 * 1024 * 1024,
          "Native provenance exceeds the size limit"
        )
        count = input.read(buffer)
      }
    } finally input.close()
    val bytes = output.toByteArray
    val expectedDigest = manifest.getProperty("provenance.sha256", "")
    require(
      expectedDigest.matches("[a-f0-9]{64}") && sha256(bytes) == expectedDigest,
      "Native provenance checksum mismatch"
    )
    val provenance = JSON
      .parseFull(new String(bytes, StandardCharsets.UTF_8))
      .collect { case fields: Map[_, _] => stringKeys(fields, "provenance") }
      .getOrElse(sys.error("Invalid native provenance JSON object"))

    def value(key: String): Any = provenance.getOrElse(
      key,
      sys.error(s"Missing native provenance field: $key")
    )
    def string(key: String): String = value(key) match {
      case result: String => result
      case _ => sys.error(s"Invalid native provenance string: $key")
    }
    def boolean(key: String): Boolean = value(key) match {
      case result: Boolean => result
      case _ => sys.error(s"Invalid native provenance boolean: $key")
    }
    def strings(key: String): Vector[String] = value(key) match {
      case result: List[_] if result.forall(_.isInstanceOf[String]) =>
        result.asInstanceOf[List[String]].toVector
      case _ => sys.error(s"Invalid native provenance string list: $key")
    }
    def objectValue(key: String): Map[String, Any] = value(key) match {
      case result: Map[_, _] => stringKeys(result, key)
      case _ => sys.error(s"Invalid native provenance object: $key")
    }

    require(string("audit") == "passed", "Native provenance audit did not pass")
    require(
      string("auditPolicy") == "jvm-load",
      "Native provenance must record JVM load validation"
    )
    val jvmLoadTests = value("jvmLoadTests") match {
      case records: List[_] if records.size == 2 =>
        records.map {
          case record: Map[_, _] => stringKeys(record, "jvmLoadTests")
          case _                 => sys.error("Invalid JVM load test record")
        }
      case _ => sys.error("Native provenance must record both JVM load orders")
    }
    jvmLoadTests.zip(List(jvmLoadEntries, jvmLoadEntries.reverse)).foreach {
      case (record, order) =>
        require(
          record.get("entries").contains(order.toList) &&
            record.get("exit").contains(0.0),
          "Native provenance JVM load order did not pass"
        )
    }
    require(
      value("knowhereCApiTestsExit") == 0.0,
      "Native provenance Knowhere C API tests did not pass"
    )
    require(
      strings("knowhereCApiTests") == knowhereCApiTests(
        boolean("with_diskann")
      ),
      "Native provenance has an incomplete Knowhere C API test set"
    )
    require(
      string("dependency.mode") == "shared",
      "Native provenance must record shared dependencies"
    )
    val relocationRoots = strings("relocationRoots")
    require(
      relocationRoots.distinct.size == relocationRoots.size &&
        relocationRoots.toSet == libraries.toSet,
      "Native provenance relocation roots differ from the manifest libraries"
    )
    val dlopenEntries = strings("dlopenEntries")
    require(
      dlopenEntries == auditDlopenEntries,
      "Native provenance has invalid dlopen entries"
    )
    val bundleEntries = libraries.toSet ++ aliases
    dlopenEntries.foreach(name =>
      require(bundleEntries(name), s"Missing native audit entry: $name")
    )
    require(
      string("storage.revision") == storageRevision &&
        string("storage.revision") == manifest.getProperty("storage.revision"),
      "Native provenance storage revision differs from the manifest or gitlink"
    )
    require(
      string("knowhere.revision") == knowhereRevision &&
        string("knowhere.revision") == manifest.getProperty(
          "knowhere.revision"
        ),
      "Native provenance Knowhere revision differs from the manifest or gitlink"
    )
    require(
      string("platform") == expectedPlatform &&
        string("platform") == manifest.getProperty("platform"),
      "Native provenance platform differs from the manifest"
    )
    val withCardinal = boolean("with_cardinal")
    require(
      withCardinal.toString == manifest.getProperty("with_cardinal"),
      "Native provenance Cardinal flag differs from the manifest"
    )
    require(
      boolean("with_diskann").toString == manifest.getProperty("with_diskann"),
      "Native provenance DiskANN flag differs from the manifest"
    )

    val provenanceLibraries = objectValue("libraries")
    require(
      provenanceLibraries.keySet == libraries.toSet,
      "Native provenance library set differs from the manifest"
    )
    libraries.foreach { library =>
      val record = provenanceLibraries(library) match {
        case result: Map[_, _] => stringKeys(result, s"libraries.$library")
        case _ => sys.error(s"Invalid native provenance library: $library")
      }
      require(
        record
          .get("sha256")
          .contains(
            manifest.getProperty(s"sha256.$library")
          ),
        s"Native provenance library checksum differs from the manifest: $library"
      )
    }
    val provenanceAliases = objectValue("aliases").map {
      case (alias, target: String) => alias -> target
      case (alias, _) =>
        sys.error(s"Invalid native provenance alias target: $alias")
    }
    require(
      provenanceAliases.keySet == aliases.toSet && aliases.forall(alias =>
        provenanceAliases(alias) == manifest.getProperty(s"alias.$alias")
      ),
      "Native provenance aliases differ from the manifest"
    )
    val actualCardinal = provenanceLibraries.keySet.intersect(cardinalPlugins)
    val expectedCardinal = if (withCardinal) cardinalPlugins else Set.empty
    require(
      actualCardinal == expectedCardinal,
      "Native provenance Cardinal libraries differ from with_cardinal"
    )
  }

  private def withValidationLock(directory: File)(body: => Unit): Unit =
    synchronized {
      val parent = directory.getParentFile
      IO.createDirectory(parent)
      // Keep the lock outside the directory replaced during verification. The
      // monitor also prevents overlapping file locks within one sbt process.
      val channel = FileChannel.open(
        (parent / s"${directory.getName}.lock").toPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE
      )
      try {
        val lock = channel.lock()
        try body
        finally lock.release()
      } finally channel.close()
    }

  private def validPath(name: String): Boolean =
    name.nonEmpty && name
      .split("/", -1)
      .forall(part =>
        part.matches("[A-Za-z0-9_+.-]+") && part != "." && part != ".."
      )

  private def gitOutput(directory: File, arguments: Seq[String]): String =
    Process(
      Seq("git") ++ arguments,
      directory,
      "LD_PRELOAD" -> ""
    ).!!.trim

  private def stringKeys(
      value: Map[_, _],
      field: String
  ): Map[String, Any] =
    value.map {
      case (key: String, entry) => key -> entry
      case _ => sys.error(s"Invalid native provenance object keys: $field")
    }

  private def sha256(bytes: Array[Byte]): String = {
    val hash = MessageDigest.getInstance("SHA-256").digest(bytes)
    hash.map(value => f"${value & 0xff}%02x").mkString
  }

  private def digest(file: File): String =
    streamFingerprint(new FileInputStream(file))._2

  private def streamFingerprint(input: InputStream): (Long, String) = {
    val hash = MessageDigest.getInstance("SHA-256")
    try {
      val buffer = new Array[Byte](65536)
      var length = 0L
      var count = input.read(buffer)
      while (count != -1) {
        hash.update(buffer, 0, count)
        length += count
        count = input.read(buffer)
      }
      (length, hash.digest().map(value => f"${value & 0xff}%02x").mkString)
    } finally input.close()
  }
}
