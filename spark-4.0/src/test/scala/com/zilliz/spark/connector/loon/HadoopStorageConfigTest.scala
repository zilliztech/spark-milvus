package com.zilliz.spark.connector.loon

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties

/** The mapping table is storage-access.html 3.3; these pin it. */
class HadoopStorageConfigTest extends AnyFunSuite with Matchers {

  private def conf(pairs: (String, String)*): Configuration = {
    val c = new Configuration(false)
    pairs.foreach { case (k, v) => c.set(k, v) }
    c
  }

  test("s3a AssumeRole keys become fs.* and imply the aws provider") {
    val out = HadoopStorageConfig.toFsProperties(
      conf(
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/data",
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com",
        "fs.s3a.endpoint.region" -> "us-west-2"
      )
    )
    out(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/data"
    out(StorageProperties.Address) shouldBe "s3.us-west-2.amazonaws.com"
    out(StorageProperties.Region) shouldBe "us-west-2"
    out(StorageProperties.CloudProvider) shouldBe "aws"
  }

  test("s3a static keys translate") {
    val out = HadoopStorageConfig.toFsProperties(
      conf("fs.s3a.access.key" -> "ak", "fs.s3a.secret.key" -> "sk")
    )
    out(StorageProperties.AccessKeyId) shouldBe "ak"
    out(StorageProperties.AccessKeyValue) shouldBe "sk"
    out(StorageProperties.CloudProvider) shouldBe "aws"
  }

  test("oss keys become fs.* and imply the aliyun provider") {
    val out = HadoopStorageConfig.toFsProperties(
      conf(
        "fs.oss.accessKeyId" -> "id",
        "fs.oss.accessKeySecret" -> "secret",
        "fs.oss.endpoint" -> "oss-cn-hangzhou.aliyuncs.com"
      )
    )
    out(StorageProperties.AccessKeyId) shouldBe "id"
    out(StorageProperties.AccessKeyValue) shouldBe "secret"
    out(StorageProperties.Address) shouldBe "oss-cn-hangzhou.aliyuncs.com"
    out(StorageProperties.CloudProvider) shouldBe "aliyun"
  }

  test("no storage keys yields an empty map, safe to merge") {
    HadoopStorageConfig.toFsProperties(
      conf("spark.sql.shuffle.partitions" -> "200")
    ) shouldBe empty
  }

  test("blank values are dropped, not forwarded as empty") {
    HadoopStorageConfig.toFsProperties(
      conf("fs.s3a.assumed.role.arn" -> "   ")
    ) shouldBe empty
  }

  test("both namespaces present leaves the provider unset") {
    // A misconfiguration: let the C layer's validation report it rather than
    // guess a provider.
    val out = HadoopStorageConfig.toFsProperties(
      conf("fs.s3a.access.key" -> "ak", "fs.oss.accessKeyId" -> "id")
    )
    out should not contain key(StorageProperties.CloudProvider)
  }

  test("the shim output feeds StorageProperties end to end") {
    val fs = HadoopStorageConfig.toFsProperties(
      conf(
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com",
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk"
      )
    ) + (StorageProperties.BucketName -> "milvus-bucket")

    val props = StorageProperties.from(fs)
    props(StorageProperties.CloudProvider) shouldBe "aws"
    props(StorageProperties.AccessKeyId) shouldBe "ak"
    props(StorageProperties.BucketName) shouldBe "milvus-bucket"
  }
}
