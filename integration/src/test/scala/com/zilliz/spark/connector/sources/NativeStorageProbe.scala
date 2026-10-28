package com.zilliz.spark.connector.sources

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertions

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.spark.connector.options.HadoopStorageKeys

/** Test-only byte oracles and Hadoop-to-native probes for the disposable MinIO
  * fixture. The child entry point checks the native SDK's environment
  * credential chain in a fresh process, with no host AWS profile or cloud role.
  */
object NativeStorageProbe extends Assertions {
  // These objects and digests are committed in collections/nearest/SHA256SUMS.
  // Each MinIO identity is authorized for exactly one of the two segments.
  private val objects = Seq(
    (
      "files/insert_log/10/20/30/_data/0_bf6703b6-ea6e-45a4-aedd-87e937b823bb.parquet",
      "d54356b16e7fa69504d68cff9440283d75334328d4fc17c2632a8a90c1746d70"
    ),
    (
      "files/insert_log/10/20/31/_data/0_9cdca54c-35f1-4624-8a3b-21c78b392a85.parquet",
      "7a23283869d3713a06abd248d3e8fcbcb56201adf308028de6f355bab0cfeb9a"
    )
  )
  private val locationProperties = Seq(
    StorageProperties.Address,
    StorageProperties.BucketName,
    StorageProperties.Region,
    StorageProperties.UseSSL,
    StorageProperties.UseVirtualHost,
    StorageProperties.RootPath,
    StorageProperties.StorageType
  )
  private val successMarker = "NATIVE_ENVIRONMENT_PROBE_OK"
  private val diagnosticLimit = 16384

  def verifyObject(properties: Map[String, String], index: Int): Unit = {
    val (key, expectedSha256) = objects(index)
    val resource = s"/read-fixtures/collections/nearest/$key"
    val input = Option(getClass.getResourceAsStream(resource)).getOrElse(
      fail(s"Missing committed byte oracle: $resource")
    )
    val expected =
      try input.readAllBytes()
      finally input.close()
    val checksum = MessageDigest
      .getInstance("SHA-256")
      .digest(expected)
      .map("%02x".format(_))
      .mkString
    assert(checksum == expectedSha256, s"Committed fixture changed: $key")
    assert(expected.length > 270, "Range oracle requires an interior slice")

    val store = NativeObjectStore.Factory(properties).open()
    try {
      assert(
        store.readAll(key).sameElements(expected),
        s"Full read differs: $key"
      )
      assert(store.size(key) == expected.length.toLong, s"Size differs: $key")
      Seq(13 -> 257, (expected.length - 16) -> 16).foreach {
        case (offset, length) =>
          val actual = store.readAt(
            key,
            offset.toLong,
            length.toLong,
            expected.length.toLong
          )
          assert(
            actual.sameElements(expected.slice(offset, offset + length)),
            s"Range differs at offset $offset: $key"
          )
      }
    } finally store.close()
  }

  def hadoopProperties(
      binding: Map[String, String],
      useEnvironment: Boolean
  ): Map[String, String] = {
    val bucket = binding(StorageProperties.BucketName)
    val prefix = s"fs.s3a.bucket.$bucket."
    val config = new Configuration(false)
    Map(
      "endpoint" -> StorageProperties.Address,
      "endpoint.region" -> StorageProperties.Region,
      "connection.ssl.enabled" -> StorageProperties.UseSSL
    ).foreach { case (suffix, property) =>
      config.set(prefix + suffix, binding(property))
    }
    config.set(
      prefix + "path.style.access",
      (!binding(StorageProperties.UseVirtualHost).toBoolean).toString
    )
    if (useEnvironment) {
      config.set(
        prefix + "aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider"
      )
    } else {
      config.set(prefix + "access.key", binding(StorageProperties.AccessKeyId))
      config.set(
        prefix + "secret.key",
        binding(StorageProperties.AccessKeyValue)
      )
      config.set(
        prefix + "aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }
    val before =
      config.iterator().asScala.map(e => e.getKey -> e.getValue).toMap
    val translated = HadoopStorageKeys.toFsProperties(
      config,
      bucket,
      environment = Map.empty
    )
    Seq(
      StorageProperties.Address,
      StorageProperties.Region,
      StorageProperties.UseSSL,
      StorageProperties.UseVirtualHost
    ).foreach { property =>
      val actual = translated(property)
      val expected = binding(property)
      assert(actual == expected, property)
    }
    val cloudProvider = translated(StorageProperties.CloudProvider)
    assert(cloudProvider == "aws")
    val credentialKeys =
      Seq(StorageProperties.AccessKeyId, StorageProperties.AccessKeyValue)
    if (useEnvironment) {
      val omitsStaticKeys =
        credentialKeys.forall(key => !translated.contains(key))
      assert(
        omitsStaticKeys,
        "Environment translation must not pass static keys"
      )
    } else {
      // Keep credential values out of assertion diagnostics.
      val preservesCredentials = credentialKeys.forall(property =>
        translated.get(property).contains(binding(property))
      )
      assert(
        preservesCredentials,
        "Translation must retain the supplied identity"
      )
    }
    val explicitMode =
      if (useEnvironment) Map.empty[String, String]
      else Map(StorageProperties.UseIam -> binding(StorageProperties.UseIam))
    val properties = HadoopStorageKeys.canonicalProperties(
      translated ++ explicitMode ++ Map(
        StorageProperties.BucketName -> bucket,
        StorageProperties.RootPath -> binding(StorageProperties.RootPath),
        StorageProperties.StorageType -> binding(StorageProperties.StorageType)
      )
    )
    if (useEnvironment) {
      val usesIam = properties.get(StorageProperties.UseIam).contains("true")
      val omitsStaticKeys =
        credentialKeys.forall(key => !properties.contains(key))
      assert(usesIam, "Native environment credentials require fs.use_iam=true")
      assert(
        omitsStaticKeys,
        "Native environment properties must not pass static keys"
      )
    }
    val unchanged =
      config.iterator().asScala.map(e => e.getKey -> e.getValue).toMap == before
    assert(unchanged, "Translation must not mutate Hadoop configuration")
    properties
  }

  /** Only generated fixture credentials enter the child's AWS environment.
    * Location arguments are non-secret; the native libraries inherit the
    * parent's signal-chaining environment and verified bundle classpath.
    */
  def verifyEnvironment(binding: Map[String, String], index: Int): Unit = {
    val directory = Files.createTempDirectory("milvus-native-environment-")
    val profile = directory.resolve("config")
    val credentialsFile = directory.resolve("credentials")
    val output = directory.resolve("output.log")
    val credentials = Seq(
      binding(StorageProperties.AccessKeyId),
      binding(StorageProperties.AccessKeyValue)
    )
    var process: Process = null
    var capture: Thread = null
    val captureFailure = new AtomicReference[Throwable]()
    val sawCompletion = new AtomicBoolean(false)
    try {
      Files.createFile(profile)
      Files.createFile(credentialsFile)
      val command = Seq(
        Paths.get(System.getProperty("java.home"), "bin", "java").toString,
        "-cp",
        System.getProperty("java.class.path"),
        getClass.getName.stripSuffix("$"),
        index.toString
      ) ++ locationProperties.map(binding)
      val builder = new ProcessBuilder(command.asJava)
      val environment = builder.environment()
      environment
        .keySet()
        .asScala
        .filter(_.startsWith("AWS_"))
        .toVector
        .foreach(environment.remove)
      environment.put("AWS_ACCESS_KEY_ID", credentials.head)
      environment.put("AWS_SECRET_ACCESS_KEY", credentials(1))
      environment.put("AWS_EC2_METADATA_DISABLED", "true")
      environment.put("AWS_CONFIG_FILE", profile.toString)
      environment.put("AWS_SHARED_CREDENTIALS_FILE", credentialsFile.toString)
      builder.redirectErrorStream(true)
      process = builder.start()
      process.getOutputStream.close()
      capture = captureOutput(process, output, captureFailure, sawCompletion)
      val completed = process.waitFor(90, TimeUnit.SECONDS)
      if (!completed) {
        process.destroyForcibly()
        process.waitFor(10, TimeUnit.SECONDS)
      }
      capture.join(10000)
      assert(
        !capture.isAlive,
        "Native environment probe output capture did not stop"
      )
      if (captureFailure.get() != null)
        fail(
          "Cannot capture native environment probe output: " + captureFailure
            .get()
            .getClass
            .getSimpleName
        )
      val result = diagnostic(output, credentials)
      assert(completed, "Native environment probe timed out: " + result)
      assert(
        process.exitValue() == 0,
        "Native environment probe failed: " + result
      )
      assert(
        sawCompletion.get(),
        "Native environment probe did not complete: " + result
      )
    } finally {
      if (process != null && process.isAlive) {
        process.destroyForcibly()
        process.waitFor(10, TimeUnit.SECONDS)
      }
      if (process != null) process.getInputStream.close()
      if (capture != null) capture.join(10000)
      Seq(output, credentialsFile, profile).foreach(Files.deleteIfExists)
      Files.deleteIfExists(directory)
    }
  }

  private def captureOutput(
      process: Process,
      path: Path,
      failure: AtomicReference[Throwable],
      sawCompletion: AtomicBoolean
  ): Thread = {
    val capture = new Thread(
      () => {
        try {
          val input = process.getInputStream
          val output = Files.newOutputStream(path)
          try {
            val buffer = new Array[Byte](4096)
            var retained = 0
            var recent = "\n"
            var count = input.read(buffer)
            while (count >= 0) {
              val text =
                recent + new String(buffer, 0, count, StandardCharsets.UTF_8)
              if (
                text.contains("\n" + successMarker + "\n") ||
                text.contains("\n" + successMarker + "\r\n")
              ) sawCompletion.set(true)
              recent = text.takeRight(successMarker.length + 3)
              val keep = math.min(count, diagnosticLimit - retained)
              if (keep > 0) {
                output.write(buffer, 0, keep)
                retained += keep
              }
              count = input.read(buffer)
            }
          } finally {
            try input.close()
            finally output.close()
          }
        } catch { case NonFatal(error) => failure.set(error) }
      },
      "native-environment-probe-output"
    )
    capture.setDaemon(true)
    capture.start()
    capture
  }

  private def diagnostic(path: Path, credentials: Seq[String]): String = {
    val input = Files.newInputStream(path)
    val bytes =
      try input.readNBytes(diagnosticLimit)
      finally input.close()
    val redacted =
      credentials.foldLeft(new String(bytes, StandardCharsets.UTF_8)) {
        case (text, credential) => text.replace(credential, "[REDACTED]")
      }
    // A bounded capture can end inside a credential. Mask any trailing prefix
    // as well, before truncating diagnostics for the parent test's output.
    credentials
      .foldLeft(redacted) { case (text, credential) =>
        val prefix = (1 until credential.length).reverseIterator
          .find(length => text.endsWith(credential.take(length)))
        prefix
          .map(length => text.dropRight(length) + "[REDACTED]")
          .getOrElse(text)
      }
      .take(diagnosticLimit)
  }

  def main(arguments: Array[String]): Unit = {
    require(
      arguments.length == locationProperties.length + 1,
      "Expected fixture index and storage location"
    )
    val binding = locationProperties.zip(arguments.drop(1)).toMap
    verifyObject(
      hadoopProperties(binding, useEnvironment = true),
      arguments.head.toInt
    )
    println(successMarker)
  }
}
