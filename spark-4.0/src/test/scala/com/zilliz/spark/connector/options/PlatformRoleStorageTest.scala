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
}
