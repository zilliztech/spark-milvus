package com.zilliz.spark.connector.options

import java.security.MessageDigest

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import io.milvus.storage.{MilvusStorageFileSystem, MilvusStorageProperties}

/** The whole path a real read takes: Hadoop-style config as a platform injects
  * it, translated to fs.*, validated, handed through JNI to the C library,
  * which reads a real object out of the UAT bucket.
  *
  * Environment-driven so it stays out of CI (no native library, no
  * credentials). Cancels when unset. See the run recipe in
  * StorageNativeUatTest.
  */
class StorageFullChainUatTest extends AnyFunSuite with Matchers {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private def sha256(b: Array[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(b)
      .map("%02x".format(_))
      .mkString

  test("Hadoop config -> translation -> properties -> JNI -> S3 read") {
    val bucket = env("MILVUS_JNI_S3_BUCKET").getOrElse(
      cancel("set MILVUS_JNI_S3_* and AWS_* to reach the UAT bucket")
    )
    val key =
      env("MILVUS_JNI_S3_KEY").getOrElse(cancel("set MILVUS_JNI_S3_KEY"))
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    val endpoint =
      env("MILVUS_JNI_S3_ENDPOINT").getOrElse(s"s3.$region.amazonaws.com")

    // Start where Spark does: Hadoop keys, as a managed platform injects them.
    val conf = new Configuration(false)
    conf.set("fs.s3a.endpoint", endpoint)
    conf.set("fs.s3a.endpoint.region", region)

    val translated = HadoopStorageKeys.toFsProperties(conf)
    translated(StorageProperties.Address) shouldBe endpoint
    translated(StorageProperties.CloudProvider) shouldBe "aws"

    // A laptop's temporary credentials have no fs.session_token slot, so the
    // path is use_iam reading AWS_* from the environment. On a pod this is
    // IRSA; the property map is otherwise identical.
    val userOptions = translated ++ Map(
      StorageProperties.BucketName -> bucket,
      StorageProperties.UseIam -> "true"
    )
    val props = StorageProperties.from(userOptions)

    val properties = new MilvusStorageProperties()
    var fs: MilvusStorageFileSystem = null
    try {
      properties.create(props)
      fs = new MilvusStorageFileSystem(properties, "")
      val bytes = fs.readFileAll(key)
      info(s"full chain read ${bytes.length} bytes from s3://$bucket/$key")
      env("MILVUS_JNI_EXPECTED_SHA256").foreach(e => sha256(bytes) shouldBe e)
      bytes.length should be > 0
    } finally {
      try if (fs != null) fs.close()
      finally properties.free()
    }
  }
}
