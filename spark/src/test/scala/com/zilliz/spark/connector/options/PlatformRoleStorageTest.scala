package com.zilliz.spark.connector.options

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.credential.StorageProperties

/** A session configured the way the managed platform configures it: a global
  * AssumedRole provider and the customer-data role.
  */
class PlatformRoleStorageTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("platform-role-storage")
    .config("spark.ui.enabled", "false")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
    )
    .config(
      "spark.hadoop.fs.s3a.assumed.role.arn",
      "arn:aws:iam::1:role/platform"
    )
    .getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private def native(options: Map[String, String]): Map[String, String] = {
    spark
    StorageOptions.storagePropertiesFor(
      StorageOptions.buildHadoopConfForOptions(options, ""),
      "b",
      options
    )
  }

  test("static keys passed to the read are used, not the platform role") {
    val out = native(
      Map(
        StorageProperties.BucketName -> "b",
        StorageProperties.Address -> "minio:9000",
        StorageProperties.AccessKeyId -> "ak",
        StorageProperties.AccessKeyValue -> "sk"
      )
    )
    out should not contain key(StorageProperties.RoleArn)
    out(StorageProperties.AccessKeyId) shouldBe "ak"
  }

  test(
    "fs.use_iam keeps the platform role; the pod identity is only its source"
  ) {
    val out = native(
      Map(
        StorageProperties.BucketName -> "b",
        StorageProperties.Address -> "s3.us-west-2.amazonaws.com",
        StorageProperties.UseIam -> "true"
      )
    )
    out(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/platform"
  }

  test("fs.use_iam replaces a session chain that is not only a role") {
    val providerKey = "fs.s3a.aws.credentials.provider"
    val hadoop = spark.sparkContext.hadoopConfiguration
    val saved = hadoop.get(providerKey)
    hadoop.set(
      providerKey,
      "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider," +
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
    )
    try {
      val base = Map(
        StorageProperties.BucketName -> "b",
        StorageProperties.Address -> "s3.us-west-2.amazonaws.com"
      )
      val out = native(base + (StorageProperties.UseIam -> "true"))
      out should not contain key(StorageProperties.RoleArn)
      out(StorageProperties.UseIam) shouldBe "true"
      // Without it the session chain decides, and it cannot be taken.
      intercept[IllegalArgumentException](native(base))
    } finally hadoop.set(providerKey, saved)
  }

  test(
    "fs.use_iam preserves Tencent global and bucket roles through native translation"
  ) {
    val hadoop = spark.sparkContext.hadoopConfiguration
    val settings = Map(
      "fs.s3a.aws.credentials.provider" -> HadoopStorageKeys.TencentS3RoleProvider,
      "fs.s3a.assumed.role.arn" -> "qcs::cam::uin/123456:roleName/platform",
      "fs.s3a.bucket.b.assumed.role.arn" -> "qcs::cam::uin/234567:roleName/data",
      "fs.s3a.bucket.b.assumed.role.external.id" -> "tenant-id"
    )
    val saved = settings.keys.map(k => k -> Option(hadoop.get(k))).toMap
    settings.foreach { case (k, v) => hadoop.set(k, v) }
    try {
      val options = Map(
        StorageProperties.BucketName -> "b",
        StorageProperties.Address -> "cos.ap-shanghai.myqcloud.com",
        StorageProperties.CloudProvider -> "tencent",
        StorageProperties.UseIam -> "true"
      )
      val configured =
        StorageOptions.buildHadoopConfForOptions(options, "s3a://b/snapshot")
      configured.get(
        "fs.s3a.aws.credentials.provider"
      ) shouldBe HadoopStorageKeys.TencentS3RoleProvider
      val out = StorageOptions.storagePropertiesFor(configured, "b", options)
      out(
        StorageProperties.RoleArn
      ) shouldBe "qcs::cam::uin/234567:roleName/data"
      out(StorageProperties.ExternalId) shouldBe "tenant-id"
      out(StorageProperties.CloudProvider) shouldBe "tencent"
      out(StorageProperties.UseIam) shouldBe "true"
      out should not contain key(StorageProperties.AccessKeyId)
      val global =
        StorageOptions.storagePropertiesFor(configured, "other", options)
      global(
        StorageProperties.RoleArn
      ) shouldBe "qcs::cam::uin/123456:roleName/platform"
    } finally
      saved.foreach { case (k, value) =>
        value match {
          case Some(v) => hadoop.set(k, v); case None => hadoop.unset(k)
        }
      }
  }

  test(
    "fs.use_iam refuses mixed Tencent providers instead of replacing them with AWS"
  ) {
    val hadoop = spark.sparkContext.hadoopConfiguration
    val key = "fs.s3a.aws.credentials.provider"
    val saved = hadoop.get(key)
    hadoop.set(
      key,
      HadoopStorageKeys.TencentS3RoleProvider + "," + HadoopStorageKeys.S3AAssumedRoleProvider
    )
    try {
      intercept[IllegalArgumentException](
        native(Map(StorageProperties.UseIam -> "true"))
      ).getMessage should include("cannot be mixed")
    } finally hadoop.set(key, saved)
  }

  test("Tencent bucket provider overrides the session's AWS role in IAM mode") {
    val hadoop = spark.sparkContext.hadoopConfiguration
    val prefix = "fs.s3a.bucket.b"
    hadoop.set(
      s"$prefix.aws.credentials.provider",
      HadoopStorageKeys.TencentS3RoleProvider
    )
    hadoop.set(
      s"$prefix.assumed.role.arn",
      "qcs::cam::uin/234567:roleName/data"
    )
    try {
      val out = native(
        Map(
          StorageProperties.BucketName -> "b",
          StorageProperties.Address -> "cos.ap-shanghai.myqcloud.com",
          StorageProperties.UseIam -> "true"
        )
      )
      out(StorageProperties.CloudProvider) shouldBe "tencent"
      out(
        StorageProperties.RoleArn
      ) shouldBe "qcs::cam::uin/234567:roleName/data"
    } finally {
      hadoop.unset(s"$prefix.aws.credentials.provider")
      hadoop.unset(s"$prefix.assumed.role.arn")
    }
  }

}
