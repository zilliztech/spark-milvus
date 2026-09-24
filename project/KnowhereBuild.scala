import java.io.{File, FileInputStream, InputStream}
import java.lang.{ProcessBuilder => JavaProcessBuilder}
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.Properties
import scala.sys.process.{Process, ProcessLogger}

import sbt._
import sbt.Keys._

/** Builds the upstream Java API without implicitly compiling its native engine.
  */
object KnowhereBuild {
  val knowhereApiJar = taskKey[File](
    "Build the pinned upstream Knowhere Java 11 API JAR"
  )
  val knowhereSmoke = taskKey[Unit](
    "Run the real Knowhere JNI smoke with packaged JARs in a fresh JVM"
  )
  val knowhereRuntimeJar = taskKey[File](
    "Packaged shared native runtime used by the isolated JNI smoke"
  )

  val settings: Seq[Setting[_]] = Seq(
    // Knowhere's native libraries come only from the unified bundle, which
    // records whether it was built with Cardinal.
    Compile / resourceGenerators += Def.task {
      val enabled = NativeBundle.validatedNativeBundle.value.exists { bundle =>
        NativeBundle.metadata(bundle).getProperty("with_cardinal") == "true"
      }
      val content = s"with_cardinal=$enabled\n"
      val output =
        (Compile / resourceManaged).value / "META-INF" / "milvus" / "knowhere-runtime.properties"
      if (!output.isFile || IO.read(output) != content)
        IO.write(output, content)
      Seq(output)
    }.taskValue,
    knowhereApiJar := buildApi(
      readPin(
        (ThisBuild / baseDirectory).value,
        baseDirectory.value / "knowhere.properties"
      ),
      target.value / "knowhere",
      javaHome.value.getOrElse(file(sys.props("java.home"))),
      streams.value.log
    ),
    Compile / unmanagedJars += Attributed.blank(knowhereApiJar.value),
    knowhereSmoke := {
      (Test / compile).value
      val bundle = NativeBundle.validatedNativeBundle.value.getOrElse(
        sys.error(
          "Select the unified native bundle with -Dmilvus.native.bundle=/absolute/path/to/milvus-native-<platform>.jar"
        )
      )
      smoke(
        (Compile / packageBin).value,
        knowhereApiJar.value,
        knowhereRuntimeJar.value,
        bundle,
        (Test / classDirectory).value,
        javaHome.value.getOrElse(file(sys.props("java.home"))),
        target.value,
        (ThisBuild / baseDirectory).value / "knowhere",
        streams.value.log
      )
    }
  )

  private final case class Pin(
      repository: String,
      source: File,
      revision: String,
      apiVersion: String
  )

  private def readPin(repositoryRoot: File, path: File): Pin = {
    val values = properties(path)
    def required(key: String): String =
      Option(values.getProperty(key))
        .filter(_.nonEmpty)
        .getOrElse(
          sys.error(s"Missing $key in $path")
        )
    val repository = required("repository")
    require(
      new URI(repository).getScheme == "https",
      "Knowhere repository requires HTTPS"
    )
    val source = repositoryRoot / "knowhere"
    val revision = knowhereRevision(repositoryRoot)
    val changed = gitOutput(
      source,
      Seq(
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--",
        "LICENSE",
        "java/src/main/java",
        "java/scripts/check_jni_diagnostics.py"
      )
    )
    require(
      changed.isEmpty,
      "Knowhere Java API sources must match the pinned submodule revision"
    )
    Pin(
      repository,
      source,
      revision,
      s"1.0.0-${revision.take(12)}"
    )
  }

  def knowhereRevision(repositoryRoot: File): String = {
    val source = repositoryRoot / "knowhere"
    require(
      (source / ".git").exists &&
        (source / "java" / "src" / "main" / "java").isDirectory,
      "Knowhere submodule is not initialized. Run `git submodule update --init knowhere`."
    )
    val revision = gitOutput(source, Seq("rev-parse", "HEAD"))
    require(
      revision.matches("[a-f0-9]{40}"),
      s"Knowhere submodule HEAD is not a full Git commit: $revision"
    )
    val gitlink = gitOutput(
      repositoryRoot,
      Seq("ls-files", "--stage", "--", "knowhere")
    ).split("\\r?\\n").filter(_.nonEmpty).toVector
    require(
      gitlink.size == 1,
      "Knowhere must be recorded as one Git submodule entry"
    )
    val fields = gitlink.head.split("\\s+", 4)
    require(
      fields.length == 4 && fields(0) == "160000" &&
        fields(1).matches("[a-f0-9]{40}") && fields(3) == "knowhere",
      s"Invalid Knowhere Git submodule entry: ${gitlink.head}"
    )
    require(
      fields(1) == revision,
      s"Knowhere submodule HEAD $revision does not match the recorded gitlink ${fields(1)}"
    )
    revision
  }

  private def buildApi(pin: Pin, root: File, jdk: File, log: Logger): File = {
    val directory = root / pin.revision
    val output = directory / s"knowhere-jni-${pin.apiVersion}.jar"
    val outputDigest = directory / "api.sha256"
    val inputStamp = directory / "api-inputs.txt"
    val expectedInputs = s"${pin.revision}\n${pin.apiVersion}\nrelease=11\n"
    if (
      output.isFile && outputDigest.isFile && inputStamp.isFile &&
      IO.read(inputStamp) == expectedInputs && IO
        .read(outputDigest)
        .trim == digest(output)
    ) return output

    IO.createDirectory(directory)
    val classes = directory / "classes"
    IO.delete(classes)
    IO.createDirectory(classes)
    val sources =
      (pin.source / "java" / "src" / "main" / "java" ** "*.java").get
        .sortBy(_.getPath)
    require(sources.nonEmpty, "Knowhere submodule has no Java API sources")
    val javac = jdk / "bin" / "javac"
    val jarTool = jdk / "bin" / "jar"
    require(
      javac.isFile && jarTool.isFile,
      s"Knowhere API compilation requires a JDK: $jdk"
    )
    log.info(
      s"Compiling the upstream Knowhere API for Java 11 (${pin.revision})"
    )
    run(
      Seq(
        javac.getAbsolutePath,
        "--release",
        "11",
        "-encoding",
        "UTF-8",
        "-d",
        classes.getAbsolutePath
      ) ++
        sources.map(_.getAbsolutePath),
      log
    )
    val metadata = classes / "META-INF" / "knowhere"
    IO.createDirectory(metadata)
    IO.copyFile(pin.source / "LICENSE", metadata / "LICENSE")
    IO.write(
      metadata / "source.properties",
      s"repository=${pin.repository}\ngit.revision=${pin.revision}\n"
    )
    val manifest = directory / "MANIFEST.MF"
    IO.write(
      manifest,
      s"Manifest-Version: 1.0\nImplementation-Title: Knowhere Java bindings\nImplementation-Version: ${pin.apiVersion}\n\n"
    )
    val temporaryJar = directory / "api.jar.building"
    run(
      Seq(
        jarTool.getAbsolutePath,
        "--create",
        "--file",
        temporaryJar.getAbsolutePath,
        "--manifest",
        manifest.getAbsolutePath,
        "-C",
        classes.getAbsolutePath,
        "."
      ),
      log
    )
    Files.move(
      temporaryJar.toPath,
      output.toPath,
      StandardCopyOption.REPLACE_EXISTING,
      StandardCopyOption.ATOMIC_MOVE
    )
    IO.write(outputDigest, digest(output) + "\n")
    IO.write(inputStamp, expectedInputs)
    output
  }

  private def smoke(
      adapter: File,
      api: File,
      runtime: File,
      native: File,
      testClasses: File,
      jdk: File,
      target: File,
      knowhereSource: File,
      log: Logger
  ): Unit = {
    val signalLibrary = jdk / "lib" / "libjsig.so"
    require(
      signalLibrary.isFile,
      s"Knowhere smoke requires this JRE's signal-chaining library: $signalLibrary"
    )
    val directory = target / "knowhere-smoke"
    IO.createDirectory(directory)
    val workingDirectory =
      Files.createTempDirectory(directory.toPath, "process-").toFile
    val output = directory / "jni.log"
    val classpath = Seq(adapter, api, runtime, native, testClasses)
      .map(_.getAbsolutePath)
      .mkString(File.pathSeparator)
    val process = new JavaProcessBuilder(
      (jdk / "bin" / "java").getAbsolutePath,
      "-Xcheck:jni",
      "-XX:-CreateCoredumpOnCrash",
      s"-Djava.io.tmpdir=${workingDirectory.getAbsolutePath}",
      "-cp",
      classpath,
      "com.zilliz.milvus.jni.vector.NativeVectorLibrarySmoke"
    )
    process.directory(workingDirectory)
    process.redirectErrorStream(true)
    process.redirectOutput(output)
    val environment = process.environment()
    Seq(
      "LD_LIBRARY_PATH",
      "LD_AUDIT",
      "CLASSPATH",
      "JAVA_TOOL_OPTIONS",
      "JDK_JAVA_OPTIONS",
      "_JAVA_OPTIONS"
    )
      .foreach(environment.remove)
    environment.put("LD_PRELOAD", signalLibrary.getAbsolutePath)
    environment.put("LD_BIND_NOW", "1")
    log.info(s"Running packaged Knowhere JNI smoke; log: $output")
    val running = process.start()
    if (!running.waitFor(180, TimeUnit.SECONDS)) {
      running.destroyForcibly().waitFor()
      sys.error(s"Knowhere JNI smoke timed out; inspect $output")
    }
    val exit = running.exitValue()
    IO.readLines(output).foreach(line => log.info(line))
    val checker =
      knowhereSource / "java" / "scripts" / "check_jni_diagnostics.py"
    require(
      checker.isFile,
      s"Pinned Knowhere JNI diagnostic checker is missing: $checker"
    )
    run(Seq("python3", checker.getAbsolutePath, output.getAbsolutePath), log)
    require(
      exit == 0,
      s"Knowhere JNI smoke failed ($exit); inspect $output and $workingDirectory"
    )
    IO.delete(workingDirectory)
  }

  private def properties(path: File): Properties = {
    require(path.isFile, s"Missing Knowhere metadata: $path")
    val result = new Properties()
    val input = new FileInputStream(path)
    try result.load(input)
    finally input.close()
    result
  }

  private def digest(path: File): String = {
    val input = new FileInputStream(path)
    try digest(input)
    finally input.close()
  }

  private def digest(input: InputStream): String = {
    val hash = MessageDigest.getInstance("SHA-256")
    val buffer = new Array[Byte](65536)
    var length = input.read(buffer)
    while (length != -1) {
      hash.update(buffer, 0, length)
      length = input.read(buffer)
    }
    hash.digest().map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def run(command: Seq[String], log: Logger): Unit = {
    val exit = Process(command).!(
      ProcessLogger(line => log.info(line), line => log.error(line))
    )
    require(
      exit == 0,
      s"Knowhere build command failed ($exit): ${command.head}"
    )
  }

  private def gitOutput(directory: File, arguments: Seq[String]): String = {
    val output = new StringBuilder
    val errors = new StringBuilder
    val command = Seq("git") ++ arguments
    val exit = Process(command, directory, "LD_PRELOAD" -> "").!(
      ProcessLogger(
        line => output.append(line).append('\n'),
        line => errors.append(line).append('\n')
      )
    )
    require(
      exit == 0,
      s"Git command failed ($exit) in $directory: ${command
          .mkString(" ")}\n${errors.result().trim}"
    )
    output.result().trim
  }
}
