import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.util.jar.{JarEntry, JarFile, JarOutputStream}
import java.util.Properties
import scala.sys.process.Process
import scala.util.control.NonFatal

import sbt.IO

/** Exercises native bundle checks in the compiled sbt build. */
object NativeBundleValidationProbe {
  private val storageRevision = "a" * 40
  private val knowhereRevision = "b" * 40
  private val platform = "linux-x86_64"
  private val auditEntries = Vector(
    "libmilvus-storage-jni.so",
    "libmilvus-storage.so",
    "libknowhere_jni.so",
    "libknowhere_c.so.1",
    "libknowhere.so"
  )
  private val cardinalPlugins =
    Vector("libcardinalv1.so", "libcardinalv2.so")
  private val aliases =
    Map("libstorage-alias.so" -> "libmilvus-storage.so")
  private val tests = Vector(
    "knowhere_c_api",
    "knowhere_c_api_concurrency",
    "knowhere_c_api_diskann_acceptance"
  )
  private val loadOrders = Vector(
    Vector("libmilvus-storage-jni.so", "libknowhere_jni.so"),
    Vector("libknowhere_jni.so", "libmilvus-storage-jni.so")
  )

  private def quote(value: String): String =
    "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def array(values: Seq[String]): String =
    values.map(quote).mkString("[", ",", "]")

  private def digest(value: Array[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def libraryDigest(name: String): String =
    digest(name.getBytes(StandardCharsets.UTF_8))

  private def provenance(
      libraries: Vector[String] = auditEntries,
      aliasRecords: Map[String, String] = aliases,
      withCardinal: Boolean = false,
      audit: String = "passed",
      auditPolicy: String = "jvm-load",
      jvmOrders: Vector[Vector[String]] = loadOrders,
      jvmExit: Int = 0,
      testExit: Int = 0,
      testNames: Vector[String] = tests,
      dependencyMode: String = "shared",
      relocationRoots: Vector[String] = auditEntries,
      dlopenEntries: Vector[String] = auditEntries,
      storagePin: String = storageRevision,
      knowherePin: String = knowhereRevision,
      recordedPlatform: String = platform
  ): String = {
    val libraryRecords = libraries
      .map(name =>
        quote(name) + ":{\"sha256\":" + quote(libraryDigest(name)) + "}"
      )
      .mkString("{", ",", "}")
    val aliasValues = aliasRecords.toVector
      .sortBy(_._1)
      .map { case (name, target) => s"${quote(name)}:${quote(target)}" }
      .mkString("{", ",", "}")
    val jvmRecords = jvmOrders
      .map(order => s"""{"entries":${array(order)},"exit":$jvmExit}""")
      .mkString("[", ",", "]")
    s"""{"audit":${quote(
        audit
      )},"auditPolicy":${quote(auditPolicy)},"jvmLoadTests":$jvmRecords,"knowhereCApiTestsExit":$testExit,"knowhereCApiTests":${array(
        testNames
      )},"dependency.mode":${quote(dependencyMode)},"relocationRoots":${array(
        relocationRoots
      )},"dlopenEntries":${array(dlopenEntries)},"storage.revision":${quote(
        storagePin
      )},"knowhere.revision":${quote(knowherePin)},"platform":${quote(
        recordedPlatform
      )},"with_cardinal":$withCardinal,"libraries":$libraryRecords,"aliases":$aliasValues}"""
  }

  private def manifest(
      provenanceJson: String,
      libraries: Vector[String] = auditEntries,
      aliasRecords: Map[String, String] = aliases,
      withCardinal: Boolean = false
  ): Properties = {
    val result = new Properties()
    result.setProperty("storage.revision", storageRevision)
    result.setProperty("knowhere.revision", knowhereRevision)
    result.setProperty("platform", platform)
    result.setProperty("with_cardinal", withCardinal.toString)
    result.setProperty("libraries", libraries.mkString(","))
    result.setProperty(
      "aliases",
      aliasRecords.keys.toVector.sorted.mkString(",")
    )
    result.setProperty(
      "provenance.sha256",
      digest(provenanceJson.getBytes(StandardCharsets.UTF_8))
    )
    libraries.foreach(name =>
      result.setProperty(s"sha256.$name", libraryDigest(name))
    )
    aliasRecords.foreach { case (name, target) =>
      result.setProperty(s"alias.$name", target)
    }
    result
  }

  private def withArchive(provenanceJson: Option[String])(
      body: JarFile => Unit
  ): Unit = {
    val path = Files.createTempFile("native-bundle-provenance-", ".jar")
    val output = new JarOutputStream(new FileOutputStream(path.toFile))
    try
      provenanceJson.foreach { value =>
        output.putNextEntry(
          new JarEntry("META-INF/milvus-native/provenance.json")
        )
        output.write(value.getBytes(StandardCharsets.UTF_8))
        output.closeEntry()
      }
    finally output.close()
    val archive = new JarFile(path.toFile)
    try body(archive)
    finally {
      archive.close()
      Files.delete(path)
    }
  }

  private def validate(
      provenanceJson: String,
      manifestLibraries: Vector[String] = auditEntries,
      manifestAliases: Map[String, String] = aliases,
      manifestCardinal: Boolean = false,
      mutateManifest: Properties => Unit = _ => ()
  ): Unit = {
    val properties = manifest(
      provenanceJson,
      manifestLibraries,
      manifestAliases,
      manifestCardinal
    )
    mutateManifest(properties)
    withArchive(Some(provenanceJson))(archive =>
      NativeBundle.validateProvenance(
        archive,
        properties,
        storageRevision,
        knowhereRevision,
        platform,
        manifestLibraries,
        manifestAliases.keys.toVector.sorted
      )
    )
  }

  private def reject(label: String)(body: => Unit): Unit = {
    var rejected = false
    try body
    catch { case NonFatal(_) => rejected = true }
    assert(
      rejected,
      s"Expected invalid native provenance to be rejected: $label"
    )
  }

  private def provenanceChecks(): Unit = {
    validate(provenance())

    reject("missing provenance entry") {
      val properties = manifest(provenance())
      withArchive(None)(archive =>
        NativeBundle.validateProvenance(
          archive,
          properties,
          storageRevision,
          knowhereRevision,
          platform,
          auditEntries,
          aliases.keys.toVector
        )
      )
    }
    reject("provenance digest") {
      validate(
        provenance(),
        mutateManifest = _.setProperty("provenance.sha256", "0" * 64)
      )
    }
    reject("malformed JSON") {
      validate("{")
    }
    reject("audit") {
      validate(provenance(audit = "pending"))
    }
    reject("standalone audit cannot replace JVM validation") {
      validate(provenance(auditPolicy = "standalone"))
    }
    reject("JVM load failure") {
      validate(provenance(jvmExit = 1))
    }
    reject("missing JVM load order") {
      validate(provenance(jvmOrders = loadOrders.take(1)))
    }
    reject("repeated JVM load order") {
      validate(provenance(jvmOrders = Vector.fill(2)(loadOrders.head)))
    }
    reject("Knowhere C API exit") {
      validate(provenance(testExit = 1))
    }
    reject("Knowhere C API test set") {
      validate(provenance(testNames = tests.dropRight(1)))
    }
    reject("dependency mode") {
      validate(provenance(dependencyMode = "static"))
    }
    reject("relocation roots") {
      validate(provenance(relocationRoots = auditEntries.dropRight(1)))
    }
    reject("duplicate relocation roots") {
      validate(provenance(relocationRoots = auditEntries :+ auditEntries.head))
    }
    reject("dlopen entries") {
      validate(provenance(dlopenEntries = auditEntries.reverse))
    }
    val incompleteLibraries = auditEntries.dropRight(2)
    reject("missing audited bundle entries") {
      validate(
        provenance(
          libraries = incompleteLibraries,
          relocationRoots = incompleteLibraries
        ),
        manifestLibraries = incompleteLibraries
      )
    }

    val engineLibrary = "libknowhere.so.1"
    val aliasedLibraries = auditEntries.dropRight(1) :+ engineLibrary
    val aliasedEntries = aliases + (auditEntries.last -> engineLibrary)
    validate(
      provenance(
        libraries = aliasedLibraries,
        aliasRecords = aliasedEntries,
        relocationRoots = aliasedLibraries
      ),
      manifestLibraries = aliasedLibraries,
      manifestAliases = aliasedEntries
    )
    reject("storage revision") {
      validate(provenance(storagePin = "c" * 40))
    }
    reject("Knowhere revision") {
      validate(provenance(knowherePin = "c" * 40))
    }
    reject("platform") {
      validate(provenance(recordedPlatform = "linux-aarch64"))
    }
    reject("Cardinal flag") {
      validate(provenance(withCardinal = true), manifestCardinal = false)
    }
    reject("library set") {
      validate(provenance(libraries = auditEntries.dropRight(1)))
    }
    reject("library checksum") {
      val json =
        provenance().replace(libraryDigest(auditEntries.head), "0" * 64)
      validate(json)
    }
    reject("aliases") {
      validate(provenance(aliasRecords = Map.empty))
    }

    val cardinalLibraries = auditEntries ++ cardinalPlugins
    validate(
      provenance(
        libraries = cardinalLibraries,
        withCardinal = true,
        relocationRoots = cardinalLibraries
      ),
      manifestLibraries = cardinalLibraries,
      manifestCardinal = true
    )
    reject("missing Cardinal plugins") {
      validate(provenance(withCardinal = true), manifestCardinal = true)
    }
    reject("unexpected Cardinal plugins") {
      validate(
        provenance(
          libraries = cardinalLibraries,
          relocationRoots = cardinalLibraries
        ),
        manifestLibraries = cardinalLibraries
      )
    }
  }

  private def git(directory: File, arguments: String*): String =
    Process(Seq("git", "-C", directory.getAbsolutePath) ++ arguments).!!.trim

  private def storageGitlinkChecks(): Unit = {
    val root = Files.createTempDirectory("native-bundle-storage-pin-").toFile
    try {
      git(root, "init", "--quiet")
      val storage = new File(root, "milvus-storage")
      storage.mkdir()
      git(storage, "init", "--quiet")
      git(storage, "config", "user.name", "Native Bundle Probe")
      git(storage, "config", "user.email", "native-bundle@example.invalid")
      IO.write(new File(storage, "source.cc"), "int storage = 1;\n")
      git(storage, "add", "source.cc")
      git(storage, "commit", "--quiet", "-m", "Initial storage source")
      git(root, "-c", "advice.addEmbeddedRepo=false", "add", "milvus-storage")
      val recorded =
        git(root, "ls-files", "--stage", "--", "milvus-storage")
          .split("\\s+")(1)
      assert(NativeBundle.storageRevision(root) == recorded)

      IO.write(new File(storage, "source.cc"), "int storage = 2;\n")
      git(storage, "add", "source.cc")
      git(storage, "commit", "--quiet", "-m", "Move storage source")
      reject("storage HEAD differs from gitlink") {
        NativeBundle.storageRevision(root)
      }
    } finally IO.delete(root)
  }

  private def unifiedJvmLoadChecks(): Unit = {
    val repository = new File(".").getCanonicalFile
    val directory = Files
      .createTempDirectory("native-bundle-jvm-load-")
      .toFile
    try {
      val source = new File(directory, "fixture.c")
      def compile(name: String, body: String): Unit = {
        IO.write(source, body)
        val exit = Process(
          Seq(
            "gcc", "-shared", "-fPIC", source.getAbsolutePath,
            "-o", new File(directory, name).getAbsolutePath
          )
        ).!
        assert(exit == 0, s"Cannot compile native fixture: $name")
      }
      auditEntries.foreach(name => compile(name, "int fixture(void) { return 1; }\n"))
      var messages = Vector.empty[String]
      NativeLibraries.validateUnifiedLinux(
        directory,
        repository,
        message => messages :+= message
      )
      assert(
        messages.exists(_.startsWith("Verified both JVM native load orders:")),
        s"Expected both JVM load orders to pass, got: $messages"
      )

      Files.delete(new File(directory, auditEntries.last).toPath)
      reject("missing fifth native entry") {
        NativeLibraries.validateUnifiedLinux(directory, repository, _ => ())
      }
      compile(auditEntries.last, "int fixture(void) { return 1; }\n")
      compile(loadOrders.head.head, "int JNI_OnLoad(void *vm, void *reserved) { return 0; }\n")
      reject("JVM rejects invalid JNI_OnLoad even when ELF relocation succeeds") {
        NativeLibraries.validateUnifiedLinux(directory, repository, _ => ())
      }
    } finally IO.delete(directory)
  }

  def main(arguments: Array[String]): Unit = {
    provenanceChecks()
    storageGitlinkChecks()
    unifiedJvmLoadChecks()
    println(
      "PASS: native bundle provenance, JVM loading, and source-pin validation reject unverified inputs"
    )
  }
}
