import java.io.File
import scala.io.Source

/** Loads a unified bundle's staged entry libraries in fresh JVMs before the
  * bundle is packaged.
  */
object NativeLibraries {
  private def unifiedAuditEntries =
    NativePlatform.auditDlopenEntries(NativePlatform.current)

  def validateUnifiedLinux(
      directory: File,
      repository: File,
      log: String => Unit
  ): Unit = {
    unifiedAuditEntries.foreach { name =>
      val library = new File(directory, name)
      require(library.isFile, s"Missing native entry library: $library")
    }
    val checker = new File(repository, "native-build/jvm_load.py")
    require(checker.isFile, s"Missing JVM native load checker: $checker")
    val (exit, output) = nativeCheck(
      Seq(
        "python3",
        checker.getAbsolutePath,
        "--lib-dir",
        directory.getAbsolutePath
      ),
      Map("JAVA_HOME" -> System.getProperty("java.home"))
    )
    require(
      exit == 0,
      s"JVM native load check failed for $directory (exit $exit):\n$output"
    )
    if (output.trim.nonEmpty) log(output.trim)
    log(s"Verified both JVM native load orders: $directory")
  }

  private def nativeCheck(
      arguments: Seq[String],
      extraEnvironment: Map[String, String] = Map.empty
  ): (Int, String) = {
    val builder = new ProcessBuilder(arguments: _*)
    builder.redirectErrorStream(true)
    val environment = builder.environment()
    Seq(
      "LD_PRELOAD",
      "LD_LIBRARY_PATH",
      "LD_AUDIT",
      "LD_DEBUG",
      "LD_BIND_NOW"
    ).foreach(
      environment.remove
    )
    environment.put("LC_ALL", "C")
    extraEnvironment.foreach { case (key, value) =>
      environment.put(key, value)
    }
    val process = builder.start()
    val source = Source.fromInputStream(process.getInputStream, "UTF-8")
    val output =
      try source.mkString
      finally source.close()
    (process.waitFor(), output)
  }
}
