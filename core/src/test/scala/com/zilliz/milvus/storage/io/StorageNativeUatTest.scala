package com.zilliz.milvus.storage.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.Paths
import java.security.MessageDigest
import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.storage.{MilvusStorageFileSystem, MilvusStorageProperties}

/** Reads a real Milvus object through JNI, both from the local filesystem and
  * from the S3 bucket it was captured from.
  *
  * Everything is driven by environment variables so it stays out of CI, which
  * has neither the native library nor S3 credentials. To run it:
  *
  * MILVUS_JNI_LOCAL_FILE=/abs/path/to/uat-binlog.bin \ MILVUS_JNI_S3_BUCKET=...
  * MILVUS_JNI_S3_KEY=... MILVUS_JNI_S3_REGION=us-west-2 \
  * MILVUS_JNI_S3_ENDPOINT=s3.us-west-2.amazonaws.com \ AWS_ACCESS_KEY_ID=...
  * AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=... \ sbt 'core/testOnly
  * *StorageNativeUatTest'
  */
class StorageNativeUatTest extends AnyFunSuite with Matchers {

  private def env(name: String): Option[String] =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty)

  private def sha256(bytes: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
    digest.map("%02x".format(_)).mkString
  }

  test("local backend reads a real Milvus binlog byte-for-byte") {
    val file = env("MILVUS_JNI_LOCAL_FILE").getOrElse(
      cancel("set MILVUS_JNI_LOCAL_FILE to a downloaded binlog")
    )
    val source: Path = Paths.get(file)
    val expected = Files.readAllBytes(source)

    val root = source.getParent
    val key = root.relativize(source).toString

    val properties = new MilvusStorageProperties()
    var fs: MilvusStorageFileSystem = null
    try {
      properties.create(
        Map(
          "fs.storage_type" -> "local",
          "fs.root_path" -> root.toString
        )
      )
      fs = new MilvusStorageFileSystem(properties, "")
      val actual = fs.readFileAll(key)
      actual.length shouldBe expected.length
      sha256(actual) shouldBe sha256(expected)

      fs.fileSize(key) shouldBe expected.length.toLong

      // A ranged read against the middle of the file, checked against the same
      // slice of what S3 handed us.
      val reader = fs.openReader(key, expected.length.toLong)
      try {
        val chunk = reader.readAt(1024L, 4096L)
        chunk shouldBe expected.slice(1024, 1024 + 4096)
      } finally reader.close()
    } finally {
      try if (fs != null) fs.close()
      finally properties.free()
    }
  }

  test("s3 backend reads the same object out of the UAT bucket") {
    val bucket = env("MILVUS_JNI_S3_BUCKET").getOrElse(
      cancel("set MILVUS_JNI_S3_* and AWS_* to reach the UAT bucket")
    )
    val key =
      env("MILVUS_JNI_S3_KEY").getOrElse(cancel("set MILVUS_JNI_S3_KEY"))
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    val endpoint =
      env("MILVUS_JNI_S3_ENDPOINT").getOrElse(s"s3.$region.amazonaws.com")
    val ak = env("AWS_ACCESS_KEY_ID").getOrElse(cancel("set AWS_ACCESS_KEY_ID"))
    val sk = env("AWS_SECRET_ACCESS_KEY").getOrElse(
      cancel("set AWS_SECRET_ACCESS_KEY")
    )

    val props = mutable.Map(
      "fs.storage_type" -> "remote",
      "fs.cloud_provider" -> "aws",
      "fs.address" -> endpoint,
      "fs.bucket_name" -> bucket,
      "fs.region" -> region,
      "fs.use_ssl" -> "true"
    )
    // Two ways to hand credentials to the C layer:
    //   MILVUS_JNI_S3_USE_IAM=true  → let the AWS default chain read
    //     AWS_ACCESS_KEY_ID / SECRET / SESSION_TOKEN from the environment.
    //   otherwise                   → pass the static keys as properties, which
    //     has no slot for a session token (design 3.7), so temporary
    //     credentials get a 403.
    if (env("MILVUS_JNI_S3_USE_IAM").contains("true")) {
      props += "fs.use_iam" -> "true"
    } else {
      props += "fs.access_key_id" -> ak
      props += "fs.access_key_value" -> sk
      env("AWS_SESSION_TOKEN").foreach(t => props += "fs.session_token" -> t)
    }

    val properties = new MilvusStorageProperties()
    var fs: MilvusStorageFileSystem = null
    try {
      properties.create(props.toMap)
      fs = new MilvusStorageFileSystem(properties, "")
      val actual = fs.readFileAll(key)
      info(s"read ${actual.length} bytes from s3://$bucket/$key")
      env("MILVUS_JNI_EXPECTED_SHA256").foreach { expected =>
        sha256(actual) shouldBe expected
      }
      actual.length should be > 0
    } finally {
      try if (fs != null) fs.close()
      finally properties.free()
    }
  }
}
