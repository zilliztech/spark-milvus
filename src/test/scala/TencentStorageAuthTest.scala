package com.zilliz.spark.connector

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class TencentStorageAuthTest extends AnyFunSuite {
  test("Tencent accepts numeric role IDs supported by the runtime provider") {
    val conf = configuration()
    conf.set(
      "fs.s3a.bucket.customer.assumed.role.arn",
      "qcs::cam::uin/456:role/100001"
    )
    assert(
      TencentStorageAuth
        .resolve(conf, "customer")
        .get
        .arn == "qcs::cam::uin/456:role/100001"
    )
  }

  private def configuration(): Configuration = {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      TencentStorageAuth.CredentialsProvider
    )
    conf.set("fs.s3a.assumed.role.arn", "qcs::cam::uin/123:roleName/data")
    conf
  }

  test(
    "Tencent bucket roles and ExternalIds remain independent of the global role"
  ) {
    val conf = configuration()
    conf.set("fs.s3a.assumed.role.external.id", "global-external-id")
    conf.set(
      "fs.s3a.bucket.customer.assumed.role.arn",
      "qcs::cam::uin/456:roleName/customer"
    )
    conf.set(
      "fs.s3a.bucket.customer.assumed.role.external.id",
      "customer-external-id"
    )
    conf.set(
      "fs.s3a.bucket.customer.assumed.role.session.name",
      "customer-session"
    )
    conf.set("fs.s3a.access.key", "unrelated-aws-key")
    val customer = TencentStorageAuth.resolve(conf, "customer").get
    assert(customer.arn == "qcs::cam::uin/456:roleName/customer")
    assert(customer.externalId.contains("customer-external-id"))
    assert(customer.sessionName.contains("customer-session"))
    assert(
      TencentStorageAuth
        .resolve(conf, "managed")
        .get
        .arn == "qcs::cam::uin/123:roleName/data"
    )
  }

  test(
    "Tencent rejects missing, empty bucket and foreign role ARNs without identity fallback"
  ) {
    Seq("", "arn:aws:iam::123:role/data").foreach { arn =>
      val conf = configuration()
      conf.set("fs.s3a.bucket.customer.assumed.role.arn", arn)
      intercept[IllegalArgumentException](
        TencentStorageAuth.resolve(conf, "customer")
      )
    }
    val conf = configuration()
    conf.unset("fs.s3a.assumed.role.arn")
    intercept[IllegalArgumentException](
      TencentStorageAuth.resolve(conf, "customer")
    )
  }

  test(
    "Tencent rejects mixed provider chains but leaves other providers alone"
  ) {
    val conf = configuration()
    conf.set(
      "fs.s3a.aws.credentials.provider",
      TencentStorageAuth.CredentialsProvider + ",software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )
    intercept[IllegalArgumentException](
      TencentStorageAuth.resolve(conf, "customer")
    )
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
    )
    assert(TencentStorageAuth.resolve(conf, "customer").isEmpty)
  }
}
