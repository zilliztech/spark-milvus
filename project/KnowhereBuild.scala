import java.io.{File, FileInputStream, InputStream}
import java.lang.{ProcessBuilder => JavaProcessBuilder}
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.jar.JarFile
import java.util.Properties
import scala.collection.JavaConverters._
import scala.sys.process.{Process, ProcessLogger}

import sbt._
import sbt.Keys._

/** Builds the upstream Java API without implicitly compiling its native engine.
  */
object KnowhereBuild {
  val knowhereApiJar = taskKey[File](
    "Build the pinned upstream Knowhere Java 11 API JAR"
  )
  val knowhereNativeJar = settingKey[Option[File]](
    "Optional upstream platform JAR selected by -Dknowhere.native.jar"
  )
  val verifyKnowhereNative = taskKey[File](
    "Require and validate the selected Knowhere platform JAR and source provenance"
  )
  val knowhereSmoke = taskKey[Unit](
    "Run the real Knowhere JNI smoke with packaged JARs in a fresh JVM"
  )
  val knowhereRuntimeJar = taskKey[File](
    "Packaged shared native runtime used by the isolated JNI smoke"
  )
  val knowhereStorageResources = taskKey[Seq[File]](
    "Package the tested common native dependency binaries for storage and Knowhere"
  )

  val storageSettings: Seq[Setting[_]] = Seq(
    knowhereNativeJar := sys.props.get("knowhere.native.jar").map(file),
    knowhereStorageResources := (if (NativeBundle.selected.nonEmpty) Seq.empty
                                 else
                                   storageResources(
                                     knowhereNativeJar.value,
                                     (ThisBuild / baseDirectory).value,
                                     (ThisBuild / baseDirectory).value / "native-vector",
                                     (Compile / resourceDirectory).value,
                                     (Compile / resourceManaged).value,
                                     streams.value.log
                                   )),
    Compile / resourceGenerators += knowhereStorageResources.taskValue,
    Compile / unmanagedResources := {
      val managedRoot = (Compile / resourceManaged).value
      val replacements = knowhereStorageResources.value
        .flatMap(
          IO.relativize(managedRoot, _)
        )
        .toSet
      val sourceRoots = (Compile / unmanagedResourceDirectories).value
      (Compile / unmanagedResources).value.filterNot { resource =>
        sourceRoots.exists { root =>
          IO.relativize(root, resource).exists { relative =>
            replacements(
              relative
            ) || (NativeBundle.selected.nonEmpty && relative
              .startsWith("native/"))
          }
        }
      }
    }
  )

  val settings: Seq[Setting[_]] = Seq(
    knowhereNativeJar := sys.props.get("knowhere.native.jar").map(file),
    Compile / resourceGenerators += Def.task {
      val selected = knowhereNativeJar.value
      val enabled = NativeBundle.validatedNativeBundle.value match {
        case Some(bundle) =>
          NativeBundle.metadata(bundle).getProperty("with_cardinal") == "true"
        case None =>
          selected.exists { jar =>
            validateNative(
              jar,
              readPin(
                (ThisBuild / baseDirectory).value,
                baseDirectory.value / "knowhere.properties"
              )
            )
            properties(file(jar.getAbsolutePath + ".properties"))
              .getProperty("build.with_cardinal", "false") == "true"
          }
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
    // Inter-project dependencies export Compile's unmanaged JARs. A Runtime-only
    // JAR would disappear from core, the Spark projects and root's assembly.
    // This resource-only JAR adds no Java classes to the compile API.
    Compile / unmanagedJars ++= (if (
                                   NativeBundle.validatedNativeBundle.value.nonEmpty
                                 ) {
                                   require(
                                     knowhereNativeJar.value.isEmpty,
                                     "Do not combine milvus.native.bundle and knowhere.native.jar"
                                   )
                                   Seq.empty
                                 } else
                                   selectedNative(
                                     knowhereNativeJar.value,
                                     readPin(
                                       (ThisBuild / baseDirectory).value,
                                       baseDirectory.value / "knowhere.properties"
                                     )
                                   )),
    verifyKnowhereNative := {
      val bundle = NativeBundle.validatedNativeBundle.value
      val jar = bundle
        .orElse(knowhereNativeJar.value)
        .getOrElse(
          sys.error(
            "Select a built Knowhere platform JAR with -Dknowhere.native.jar=/absolute/path/to/platform.jar"
          )
        )
      if (bundle.isEmpty)
        validateNative(
          jar,
          readPin(
            (ThisBuild / baseDirectory).value,
            baseDirectory.value / "knowhere.properties"
          )
        )
      streams.value.log.info(s"Verified Knowhere platform JAR: $jar")
      jar
    },
    knowhereSmoke := {
      (Test / compile).value
      smoke(
        (Compile / packageBin).value,
        knowhereApiJar.value,
        knowhereRuntimeJar.value,
        verifyKnowhereNative.value,
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

  private def selectedNative(
      selected: Option[File],
      pin: Pin
  ): Seq[Attributed[File]] =
    selected.toSeq.map { jar =>
      validateNative(jar, pin)
      Attributed.blank(jar)
    }

  private def storageResources(
      selected: Option[File],
      repositoryRoot: File,
      vectorDirectory: File,
      sourceDirectory: File,
      outputDirectory: File,
      log: Logger
  ): Seq[File] = {
    val nativeRoot = sourceDirectory / "native"
    // Only a directory that holds files counts as Linux resources: the
    // Makefile's directory rule (and its tests) can leave empty platform
    // directories behind, and the storage-only macOS build must not trip the
    // Linux-host requirement over one of those.
    val linuxDirectories = (nativeRoot * "linux-*").get.filter(directory =>
      directory.isDirectory && (directory ** "*").get.exists(_.isFile)
    )
    if (selected.isEmpty && linuxDirectories.isEmpty) return Seq.empty
    val platform = currentPlatform()
    require(
      linuxDirectories.forall(_.getName == platform),
      s"Linux native resources require validation on their target host: " +
        s"host=$platform, resources=${linuxDirectories.map(_.getName).sorted.mkString(",")}."
    )
    val resourcePrefix = s"native/$platform/"
    val originalNative = sourceDirectory / resourcePrefix
    if (!originalNative.isDirectory) return Seq.empty
    NativeLibraries.validateLinux(originalNative, message => log.info(message))
    if (selected.isEmpty) return Seq.empty
    val engine = originalNative / "libmilvus-storage.so"

    val native = selected.get
    validateNative(
      native,
      readPin(
        repositoryRoot,
        vectorDirectory / "knowhere.properties"
      )
    )
    val storageDigest = digest(engine)
    val storageResourcesDigest = NativeLibraries.fingerprint(originalNative)
    val nativeDigest = digest(native)
    val records = (vectorDirectory * "storage-compatibility*.properties").get
    val matching = records.filter { record =>
      val p = properties(record)
      p.getProperty("platform") == platform &&
      p.getProperty("storage.sha256") == storageDigest &&
      p.getProperty("storage.resources.sha256") == storageResourcesDigest &&
      p.getProperty("knowhere.jar.sha256") == nativeDigest
    }
    require(
      matching.size == 1,
      s"Untested storage/Knowhere native combination for $platform: " +
        s"storage SHA-256=$storageDigest, " +
        s"storage resources SHA-256=$storageResourcesDigest, " +
        s"Knowhere JAR SHA-256=$nativeDigest. " +
        s"Validate both library load orders, native storage I/O and Knowhere search " +
        s"before recording the pair under $vectorDirectory/storage-compatibility*.properties."
    )
    val compatibilityFile = matching.head
    val compatibility = properties(compatibilityFile)

    def names(key: String): Vector[String] = {
      val values = Option(compatibility.getProperty(key))
        .getOrElse("")
        .split(",", -1)
        .toVector
      require(
        values.nonEmpty && values.distinct.size == values.size &&
          values.forall(
            _.matches(NativePlatform.libraryPattern(currentPlatform()))
          ),
        s"Invalid $key in $compatibilityFile"
      )
      values
    }
    val libraries = names("libraries")
    val aliases = names("aliases").map { alias =>
      val canonical = compatibility.getProperty(s"alias.$alias")
      require(
        libraries.contains(canonical) && !libraries.contains(alias),
        s"Invalid shared library alias $alias in $compatibilityFile"
      )
      alias -> canonical
    }
    val upstreamPrefix = s"native/knowhere/1/$platform/"
    val jar = new JarFile(native)
    try {
      val manifest = new Properties()
      val manifestInput = jar.getInputStream(
        jar.getJarEntry(upstreamPrefix + "manifest.properties")
      )
      try manifest.load(manifestInput)
      finally manifestInput.close()
      val knowhereLibraries = Set(
        "libknowhere.so",
        "libknowhere_c.so.1",
        "libknowhere_jni.so"
      )
      val upstreamDependencies = manifest
        .getProperty("libraries")
        .split(",")
        .toSet -- knowhereLibraries
      require(
        libraries.toSet == upstreamDependencies,
        s"The tested common dependency list does not match $native"
      )

      def copyEntry(
          name: String,
          relative: String,
          expected: Option[String]
      ): File = {
        val entry = Option(jar.getJarEntry(upstreamPrefix + name)).getOrElse(
          sys.error(s"Missing Knowhere resource $upstreamPrefix$name")
        )
        val output = outputDirectory / relative
        require(
          output.toPath
            .normalize()
            .startsWith(outputDirectory.toPath.normalize()),
          s"Invalid Knowhere resource path: $relative"
        )
        val checksum = expected.getOrElse {
          val input = jar.getInputStream(entry)
          try digest(input)
          finally input.close()
        }
        if (!output.isFile || digest(output) != checksum) {
          IO.createDirectory(output.getParentFile)
          val input = jar.getInputStream(entry)
          try
            Files.copy(
              input,
              output.toPath,
              StandardCopyOption.REPLACE_EXISTING
            )
          finally input.close()
        }
        requireDigest(output, checksum)
        output
      }

      val binaries = (libraries.map(name => name -> name) ++ aliases).map {
        case (name, canonical) =>
          copyEntry(
            canonical,
            resourcePrefix + name,
            Some(manifest.getProperty(s"sha256.$canonical"))
          )
      }
      val metadataPrefix = "META-INF/knowhere-storage-dependencies/"
      val licenseResources = jar
        .entries()
        .asScala
        .filterNot(_.isDirectory)
        .map(_.getName)
        .filter(_.startsWith(upstreamPrefix + "licenses/"))
        .map(_.stripPrefix(upstreamPrefix))
        .filter { name =>
          libraries.exists(library => name.startsWith(s"licenses/$library/"))
        }
        .toVector
        .sorted
      val metadata = (licenseResources ++ Vector(
        "manifest.properties",
        "missing-licenses.txt"
      )).map(name => copyEntry(name, metadataPrefix + name, None))
      val compatibilityOutput = outputDirectory / metadataPrefix /
        "storage-compatibility.properties"
      if (
        !compatibilityOutput.isFile ||
        digest(compatibilityOutput) != digest(compatibilityFile)
      ) IO.copyFile(compatibilityFile, compatibilityOutput)
      NativeLibraries.validateMergedLinux(
        originalNative,
        outputDirectory / resourcePrefix,
        binaries,
        message => log.info(message)
      )
      log.info(s"Packaging verified common native dependencies for $platform")
      // sbt tracks only the resource generator's returned files. When the option
      // is removed, stale generated files are not returned and the original
      // unmanaged storage resources are copied back into the class directory.
      binaries ++ metadata :+ compatibilityOutput
    } finally jar.close()
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

  private def validateNative(path: File, pin: Pin): Unit = {
    require(
      path.isAbsolute && path.isFile,
      "knowhere.native.jar must name an existing absolute JAR path"
    )
    val provenance = properties(file(path.getAbsolutePath + ".properties"))
    require(
      provenance.getProperty("git.revision") == pin.revision,
      s"Knowhere platform JAR must be built from ${pin.revision}: $path"
    )
    requireDigest(path, provenance.getProperty("jar.sha256"))

    val platform = currentPlatform()
    val root = s"native/knowhere/1/$platform/"
    val jar = new JarFile(path)
    try {
      val entries = jar.entries().asScala.toVector
      require(
        entries.map(_.getName).distinct.size == entries.size,
        s"Duplicate entries in Knowhere platform JAR: $path"
      )
      entries.filterNot(_.isDirectory).foreach { entry =>
        val name = entry.getName
        require(
          !name.endsWith(".class"),
          s"Knowhere platform JAR must contain native resources only: $name"
        )
        require(
          !name.startsWith("native/") || name.startsWith(root),
          s"Knowhere platform JAR contains resources outside $root: $name"
        )
      }
      def input(name: String): InputStream = {
        val entry = Option(jar.getJarEntry(root + name)).getOrElse(
          sys.error(s"Missing $root$name in $path")
        )
        jar.getInputStream(entry)
      }
      val manifest = new Properties()
      val manifestInput = input("manifest.properties")
      try manifest.load(manifestInput)
      finally manifestInput.close()
      require(
        manifest.getProperty("cAbiVersion") == "1",
        "Knowhere platform JAR must provide C ABI version 1"
      )
      require(
        manifest.getProperty("platform") == platform,
        s"Knowhere platform JAR must match $platform"
      )
      require(
        manifest.getProperty("entryLibrary") == "libknowhere_jni.so",
        "Invalid Knowhere JNI entry library"
      )
      val libraries = Option(manifest.getProperty("libraries"))
        .getOrElse("")
        .split(",", -1)
        .toVector
      require(
        libraries.nonEmpty && libraries.distinct.size == libraries.size,
        "Invalid Knowhere library list"
      )
      require(
        libraries.contains("libknowhere_jni.so"),
        "Knowhere manifest omits the JNI entry library"
      )
      libraries.foreach { name =>
        require(
          name.matches("[A-Za-z0-9_+.-]+") && name != "." && name != "..",
          s"Invalid Knowhere library name: $name"
        )
        val expected = manifest.getProperty(s"sha256.$name")
        require(
          expected != null && expected.matches("[a-f0-9]{64}"),
          s"Missing checksum for Knowhere library $name"
        )
        val library = input(name)
        val actual =
          try digest(library)
          finally library.close()
        require(
          actual == expected,
          s"Knowhere library checksum mismatch: $name"
        )
      }
      val librarySet = libraries.toSet
      entries.filterNot(_.isDirectory).foreach { entry =>
        val name = entry.getName.stripPrefix(root)
        if (
          name
            .matches(NativePlatform.libraryPattern(currentPlatform(), "[^/]+"))
        ) {
          require(
            librarySet(name),
            s"Unlisted shared library in Knowhere platform JAR: $name"
          )
        }
      }
    } finally jar.close()
  }

  private def currentPlatform(): String = NativePlatform.current

  private def properties(path: File): Properties = {
    require(path.isFile, s"Missing Knowhere metadata: $path")
    val result = new Properties()
    val input = new FileInputStream(path)
    try result.load(input)
    finally input.close()
    result
  }

  private def requireDigest(path: File, expected: String): Unit = {
    require(
      expected != null && expected.matches("[a-f0-9]{64}"),
      s"Missing SHA-256 for $path"
    )
    require(digest(path) == expected, s"Knowhere SHA-256 mismatch: $path")
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
