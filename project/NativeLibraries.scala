import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileVisitOption, Files, StandardCopyOption}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import scala.io.Source

/** Validates the complete storage resource directory before it is packaged. */
object NativeLibraries {
  private val entryLibraries = Vector(
    "libmilvus-storage.so",
    "libnative-storage-jni.so"
  )

  private def resources(directory: File): Vector[(String, File)] = {
    val root = directory.toPath.toAbsolutePath.normalize()
    require(
      Files.isDirectory(root),
      s"Missing native resource directory: $root"
    )
    val paths = Files.walk(root, FileVisitOption.FOLLOW_LINKS)
    try
      paths
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path))
        .map { path =>
          root.relativize(path).iterator().asScala.mkString("/") -> path.toFile
        }
        .toVector
        .sortBy(_._1)
    finally paths.close()
  }

  /** SHA-256 of UTF-8 `storage-native-resources-v1\n`, followed by each
    * relative path, NUL, lowercase file SHA-256, and LF. Paths use `/` and
    * Scala String ordering. Files reached through symlinks are hashed by their
    * bytes; every resource path, including aliases, is included.
    */
  def fingerprint(directory: File): String = {
    val result = MessageDigest.getInstance("SHA-256")
    result.update("storage-native-resources-v1\n".getBytes(UTF_8))
    resources(directory).foreach { case (relative, file) =>
      result.update(relative.getBytes(UTF_8))
      result.update(0.toByte)
      result.update(digest(file).getBytes(UTF_8))
      result.update('\n'.toByte)
    }
    hex(result.digest())
  }

  def validateLinux(directory: File, log: String => Unit): Unit = {
    require(
      System.getProperty("os.name").equalsIgnoreCase("Linux"),
      "Linux native relocation checks require a Linux host"
    )
    entryLibraries.foreach { name =>
      val library = new File(directory, name)
      require(library.isFile, s"Missing native entry library: $library")
      val builder = new ProcessBuilder("ldd", "-r", library.getAbsolutePath)
      builder.redirectErrorStream(true)
      val environment = builder.environment()
      environment.remove("LD_PRELOAD")
      environment.remove("LD_LIBRARY_PATH")
      environment.put("LC_ALL", "C")
      val process = builder.start()
      val source = Source.fromInputStream(process.getInputStream, "UTF-8")
      val output =
        try source.mkString
        finally source.close()
      val exit = process.waitFor()
      val unresolved = output.linesIterator.exists { line =>
        line.contains("undefined symbol:") || line.contains("not found")
      }
      require(
        exit == 0 && !unresolved,
        s"Native relocation check failed for $library (exit $exit):\n$output"
      )
      log(s"Verified native relocations: $library")
    }
  }

  /** Check the same source resources and managed replacements sbt will copy,
    * without changing either input or returning temporary resources to sbt.
    */
  def validateMergedLinux(
      original: File,
      managed: File,
      replacements: Seq[File],
      log: String => Unit
  ): Unit = {
    val staging = Files.createTempDirectory("storage-native-validation-")
    try {
      def copy(relative: String, source: File): Unit = {
        val destination = staging.resolve(relative).normalize()
        require(
          destination.startsWith(staging),
          s"Invalid native resource path: $relative"
        )
        Files.createDirectories(destination.getParent)
        Files.copy(
          source.toPath,
          destination,
          StandardCopyOption.REPLACE_EXISTING
        )
      }
      resources(original).foreach { case (relative, source) =>
        copy(relative, source)
      }
      val managedRoot = managed.toPath.toAbsolutePath.normalize()
      replacements.foreach { source =>
        val path = source.toPath.toAbsolutePath.normalize()
        require(
          path.startsWith(managedRoot),
          s"Invalid native replacement: $path"
        )
        copy(managedRoot.relativize(path).toString, source)
      }
      validateLinux(staging.toFile, log)
    } finally {
      val paths = Files.walk(staging)
      try paths.iterator().asScala.toVector.reverse.foreach(Files.delete)
      finally paths.close()
    }
  }

  private def digest(file: File): String = {
    val hash = MessageDigest.getInstance("SHA-256")
    val input = new FileInputStream(file)
    try {
      val buffer = new Array[Byte](65536)
      var length = input.read(buffer)
      while (length != -1) {
        hash.update(buffer, 0, length)
        length = input.read(buffer)
      }
      hex(hash.digest())
    } finally input.close()
  }

  private def hex(bytes: Array[Byte]): String =
    bytes.map(byte => f"${byte & 0xff}%02x").mkString
}
