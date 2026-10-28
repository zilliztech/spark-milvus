package com.zilliz.milvus.storage.credential

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties._

class StoragePropertiesTest extends AnyFunSuite with Matchers {

  private def failureFor(options: Map[String, String]): String =
    intercept[IllegalArgumentException](from(options)).getMessage

  private val remote = Map(
    BucketName -> "milvus-bucket",
    Address -> "s3.us-west-2.amazonaws.com",
    AccessKeyId -> "ak",
    AccessKeyValue -> "sk"
  )

  test("a complete static-key configuration passes through") {
    val out = from(remote)
    out(BucketName) shouldBe "milvus-bucket"
    out(Address) shouldBe "s3.us-west-2.amazonaws.com"
    out(AccessKeyId) shouldBe "ak"
    out(AccessKeyValue) shouldBe "sk"
  }

  test("conventions are filled in, location and credentials are not") {
    val out = from(remote)
    out(StorageType) shouldBe "remote"
    out(RootPath) shouldBe "files"
    out(UseSSL) shouldBe "false"
  }

  test("bucket and endpoint are required") {
    failureFor(remote - BucketName) should include(BucketName)
    failureFor(remote - Address) should include(Address)
  }

  test("a blank value counts as missing") {
    failureFor(remote + (BucketName -> "   ")) should include(BucketName)
  }

  test("both access keys are required outside IAM and AssumeRole") {
    failureFor(remote - AccessKeyId) should include(AccessKeyId)
    failureFor(remote - AccessKeyValue) should include(AccessKeyValue)
  }

  test("IAM needs no keys, and any that were given are dropped") {
    // Empty or stale keys would override whatever the native chain resolves.
    val out = from(remote + (UseIam -> "true"))
    out should not contain key(AccessKeyId)
    out should not contain key(AccessKeyValue)
    out(UseIam) shouldBe "true"
  }

  test("fs.use_iam is trimmed before it is read") {
    noException should be thrownBy from(
      remote - AccessKeyId - AccessKeyValue + (UseIam -> "  true  ")
    )
  }

  test("known boolean properties are strict and normalized") {
    Seq(UseSSL, UseIam, UseVirtualHost).foreach { key =>
      val error = failureFor(remote + (key -> "enabled"))
      error should include(key)
      error should include("enabled")
    }

    val out = from(remote ++ Map(UseSSL -> "TRUE", UseVirtualHost -> "False"))
    out(UseSSL) shouldBe "true"
    out(UseVirtualHost) shouldBe "false"
  }

  test("external boolean validation identifies its property group") {
    val error = failureFor(
      remote ++ Map(
        "extfs.source.bucket_name" -> "customer-bucket",
        "extfs.source.address" -> "endpoint",
        "extfs.source.use_iam" -> "sometimes"
      )
    )
    error should include("extfs.source.use_iam")
    error should include("sometimes")
  }

  test("a role ARN also means the native layer resolves credentials") {
    val out = from(
      remote - AccessKeyId - AccessKeyValue +
        (RoleArn -> "arn:aws:iam::1:role/r") + (SessionName -> " job ")
    )
    out(RoleArn) shouldBe "arn:aws:iam::1:role/r"
    out(SessionName) shouldBe "job"
    out should not contain key(AccessKeyId)
  }

  test("local storage needs no bucket, endpoint or credentials") {
    val out = from(Map(StorageType -> "local"))
    out(StorageType) shouldBe "local"
    out should not contain key(AccessKeyId)
  }

  test("keys outside the fs. and extfs. namespaces are dropped") {
    from(
      remote + ("milvus.uri" -> "http://x") + ("spark.hadoop.fs.s3a.access.key" -> "leak")
    ) should
      contain noElementsOf Seq("milvus.uri", "spark.hadoop.fs.s3a.access.key")
  }

  test("unknown fs. keys pass through; the C registry checks them on read") {
    from(remote + ("fs.request_timeout_ms" -> "5000"))(
      "fs.request_timeout_ms"
    ) shouldBe "5000"
  }

  test("each extfs group is validated and rendered back under its own name") {
    val out = from(
      remote ++ Map(
        "extfs.source.bucket_name" -> "customer-bucket",
        "extfs.source.address" -> "oss-cn-hangzhou.aliyuncs.com",
        "extfs.source.role_arn" -> "acs:ram::1:role/r"
      )
    )
    out("extfs.source.bucket_name") shouldBe "customer-bucket"
    out("extfs.source.role_arn") shouldBe "acs:ram::1:role/r"
    out("extfs.source.storage_type") shouldBe "remote"
    out(BucketName) shouldBe "milvus-bucket"
  }

  test("an incomplete extfs group fails and says which one") {
    failureFor(
      remote + ("extfs.source.bucket_name" -> "customer-bucket")
    ) should include("extfs.source.address")
  }

  test("values are trimmed on the way through") {
    from(remote + (BucketName -> "  padded  "))(BucketName) shouldBe "padded"
  }

  test("blank role settings are dropped, non-blank ones kept") {
    val out = from(
      remote - AccessKeyId - AccessKeyValue ++ Map(
        RoleArn -> "arn:aws:iam::1:role/r",
        SessionName -> "spark-job",
        ExternalId -> "   "
      )
    )
    out(SessionName) shouldBe "spark-job"
    out should not contain key(ExternalId)
  }

  test("externalNames lists the registered groups") {
    externalNames(
      Map(
        "extfs.milvus.bucket_name" -> "b",
        "extfs.source.bucket_name" -> "c",
        "extfs.source.address" -> "a",
        "fs.bucket_name" -> "d"
      )
    ) shouldBe Seq("milvus", "source")
  }
}
