package com.zilliz.spark.connector.options

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

  // Backfill writes its real configuration under fs.s3a.bucket.<name>.*, so a
  // translator that reads only the global prefix drops the endpoint and the
  // credentials that actually apply.
  test("a per-bucket key beats the global one") {
    val c = conf(
      "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com",
      "fs.s3a.access.key" -> "global-ak",
      "fs.s3a.secret.key" -> "global-sk",
      "fs.s3a.bucket.backfill-src.endpoint" -> "http://minio:9000",
      "fs.s3a.bucket.backfill-src.access.key" -> "bucket-ak",
      "fs.s3a.bucket.backfill-src.secret.key" -> "bucket-sk"
    )

    val out = HadoopStorageConfig.toFsProperties(c, "backfill-src")
    out(StorageProperties.Address) shouldBe "http://minio:9000"
    out(StorageProperties.AccessKeyId) shouldBe "bucket-ak"
    out(StorageProperties.AccessKeyValue) shouldBe "bucket-sk"

    // Another bucket in the same session still gets the global values.
    val other = HadoopStorageConfig.toFsProperties(c, "milvus-storage")
    other(StorageProperties.Address) shouldBe "s3.us-west-2.amazonaws.com"
    other(StorageProperties.AccessKeyId) shouldBe "global-ak"
  }

  test("a per-bucket key is read when no global one exists") {
    val out = HadoopStorageConfig.toFsProperties(
      conf(
        "fs.s3a.bucket.only-here.endpoint" -> "http://minio:9000",
        "fs.s3a.bucket.only-here.access.key" -> "ak",
        "fs.s3a.bucket.only-here.secret.key" -> "sk"
      ),
      "only-here"
    )
    out(StorageProperties.Address) shouldBe "http://minio:9000"
    out(StorageProperties.AccessKeyId) shouldBe "ak"
    out(StorageProperties.CloudProvider) shouldBe "aws"
  }

  // The consequence of missing them: with no access key in sight the IAM
  // fallback fires, and a deployment that configured static credentials
  // silently authenticates as the pod instead of failing.
  test("per-bucket static keys keep the IAM fallback from firing") {
    val props = StorageProperties.from(
      HadoopStorageConfig.toFsProperties(
        conf(
          "fs.s3a.bucket.b1.endpoint" -> "http://minio:9000",
          "fs.s3a.bucket.b1.access.key" -> "ak",
          "fs.s3a.bucket.b1.secret.key" -> "sk"
        ),
        "b1"
      ) + (StorageProperties.BucketName -> "b1")
    )
    props(StorageProperties.AccessKeyId) shouldBe "ak"
    props.get(StorageProperties.UseIam) should not be Some("true")
  }

  test("a per-bucket assumed role beats the global one") {
    val out = HadoopStorageConfig.toFsProperties(
      conf(
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/global",
        "fs.s3a.bucket.b1.assumed.role.arn" -> "arn:aws:iam::1:role/bucket",
        "fs.s3a.bucket.b1.assumed.role.external.id" -> "ext-1"
      ),
      "b1"
    )
    out(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/bucket"
    out(StorageProperties.ExternalId) shouldBe "ext-1"
  }

  test("oss per-bucket keys resolve the same way") {
    val out = HadoopStorageConfig.toFsProperties(
      conf(
        "fs.oss.endpoint" -> "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.bucket.b1.endpoint" -> "oss-cn-beijing.aliyuncs.com",
        "fs.oss.bucket.b1.accessKeyId" -> "id",
        "fs.oss.bucket.b1.accessKeySecret" -> "secret"
      ),
      "b1"
    )
    out(StorageProperties.Address) shouldBe "oss-cn-beijing.aliyuncs.com"
    out(StorageProperties.AccessKeyId) shouldBe "id"
    out(StorageProperties.CloudProvider) shouldBe "aliyun"
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
