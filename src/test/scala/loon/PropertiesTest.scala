package com.zilliz.spark.connector.loon

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.MilvusOption

/**
 * Unit tests for Properties and FsConfig constants
 */
class PropertiesTest extends AnyFunSuite with Matchers {

  // ============ FsConfig Constants Tests ============

  test("FsConfig constants have correct values") {
    Properties.FsConfig.FsAddress shouldBe "fs.address"
    Properties.FsConfig.FsBucketName shouldBe "fs.bucket_name"
    Properties.FsConfig.FsAccessKeyId shouldBe "fs.access_key_id"
    Properties.FsConfig.FsAccessKeyValue shouldBe "fs.access_key_value"
    Properties.FsConfig.FsRootPath shouldBe "fs.root_path"
    Properties.FsConfig.FsStorageType shouldBe "fs.storage_type"
    Properties.FsConfig.FsCloudProvider shouldBe "fs.cloud_provider"
    Properties.FsConfig.FsIamEndpoint shouldBe "fs.iam_endpoint"
    Properties.FsConfig.FsLogLevel shouldBe "fs.log_level"
    Properties.FsConfig.FsRegion shouldBe "fs.region"
    Properties.FsConfig.FsUseSSL shouldBe "fs.use_ssl"
    Properties.FsConfig.FsSslCaCert shouldBe "fs.ssl_ca_cert"
    Properties.FsConfig.FsUseIam shouldBe "fs.use_iam"
    Properties.FsConfig.FsUseVirtualHost shouldBe "fs.use_virtual_host"
    Properties.FsConfig.FsRequestTimeoutMs shouldBe "fs.request_timeout_ms"
    Properties.FsConfig.FsGcpNativeWithoutAuth shouldBe "fs.gcp_native_without_auth"
    Properties.FsConfig.FsGcpCredentialJson shouldBe "fs.gcp_credential_json"
    Properties.FsConfig.FsUseCustomPartUpload shouldBe "fs.use_custom_part_upload"
  }

  test("FsConfig constants use snake_case naming convention") {
    // Verify all constants follow snake_case pattern for C++ API compatibility
    val allConstants = Seq(
      Properties.FsConfig.FsAddress,
      Properties.FsConfig.FsBucketName,
      Properties.FsConfig.FsAccessKeyId,
      Properties.FsConfig.FsAccessKeyValue,
      Properties.FsConfig.FsRootPath,
      Properties.FsConfig.FsStorageType,
      Properties.FsConfig.FsCloudProvider,
      Properties.FsConfig.FsIamEndpoint,
      Properties.FsConfig.FsLogLevel,
      Properties.FsConfig.FsRegion,
      Properties.FsConfig.FsUseSSL,
      Properties.FsConfig.FsSslCaCert,
      Properties.FsConfig.FsUseIam,
      Properties.FsConfig.FsUseVirtualHost,
      Properties.FsConfig.FsRequestTimeoutMs,
      Properties.FsConfig.FsGcpNativeWithoutAuth,
      Properties.FsConfig.FsGcpCredentialJson,
      Properties.FsConfig.FsUseCustomPartUpload
    )

    allConstants.foreach { constant =>
      constant should startWith("fs.")
      // Check snake_case pattern (no uppercase after first char, underscores allowed)
      constant.substring(3) should fullyMatch regex """[a-z][a-z0-9_]*"""
    }
  }

  test("FsConfig constants are all unique") {
    val allConstants = Seq(
      Properties.FsConfig.FsAddress,
      Properties.FsConfig.FsBucketName,
      Properties.FsConfig.FsAccessKeyId,
      Properties.FsConfig.FsAccessKeyValue,
      Properties.FsConfig.FsRootPath,
      Properties.FsConfig.FsStorageType,
      Properties.FsConfig.FsCloudProvider,
      Properties.FsConfig.FsIamEndpoint,
      Properties.FsConfig.FsLogLevel,
      Properties.FsConfig.FsRegion,
      Properties.FsConfig.FsUseSSL,
      Properties.FsConfig.FsSslCaCert,
      Properties.FsConfig.FsUseIam,
      Properties.FsConfig.FsUseVirtualHost,
      Properties.FsConfig.FsRequestTimeoutMs,
      Properties.FsConfig.FsGcpNativeWithoutAuth,
      Properties.FsConfig.FsGcpCredentialJson,
      Properties.FsConfig.FsUseCustomPartUpload
    )

    allConstants.distinct should have size allConstants.size
  }

  // ============ MilvusOption Integration Tests ============

  test("MilvusOption can be created with FsConfig keys") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      Properties.FsConfig.FsAddress -> "localhost:9000",
      Properties.FsConfig.FsBucketName -> "test-bucket",
      Properties.FsConfig.FsAccessKeyId -> "access123",
      Properties.FsConfig.FsAccessKeyValue -> "secret456",
      Properties.FsConfig.FsRootPath -> "data/milvus",
      Properties.FsConfig.FsUseSSL -> "true",
      Properties.FsConfig.FsRegion -> "us-west-2"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.options(Properties.FsConfig.FsAddress) shouldBe "localhost:9000"
    milvusOption.options(Properties.FsConfig.FsBucketName) shouldBe "test-bucket"
    milvusOption.options(Properties.FsConfig.FsAccessKeyId) shouldBe "access123"
    milvusOption.options(Properties.FsConfig.FsAccessKeyValue) shouldBe "secret456"
    milvusOption.options(Properties.FsConfig.FsRootPath) shouldBe "data/milvus"
    milvusOption.options(Properties.FsConfig.FsUseSSL) shouldBe "true"
    milvusOption.options(Properties.FsConfig.FsRegion) shouldBe "us-west-2"
  }

  test("MilvusOption preserves all FsConfig values including optional ones") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      Properties.FsConfig.FsAddress -> "s3.amazonaws.com",
      Properties.FsConfig.FsBucketName -> "production-bucket",
      Properties.FsConfig.FsAccessKeyId -> "AKIAIOSFODNN7EXAMPLE",
      Properties.FsConfig.FsAccessKeyValue -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      Properties.FsConfig.FsRootPath -> "milvus",
      Properties.FsConfig.FsStorageType -> "remote",
      Properties.FsConfig.FsCloudProvider -> "aws",
      Properties.FsConfig.FsLogLevel -> "info",
      Properties.FsConfig.FsRegion -> "us-east-1",
      Properties.FsConfig.FsUseSSL -> "true",
      Properties.FsConfig.FsUseIam -> "true",
      Properties.FsConfig.FsUseVirtualHost -> "true",
      Properties.FsConfig.FsRequestTimeoutMs -> "30000"
    )

    val milvusOption = MilvusOption(options)

    // All values should be preserved
    milvusOption.options(Properties.FsConfig.FsStorageType) shouldBe "remote"
    milvusOption.options(Properties.FsConfig.FsCloudProvider) shouldBe "aws"
    milvusOption.options(Properties.FsConfig.FsLogLevel) shouldBe "info"
    milvusOption.options(Properties.FsConfig.FsUseIam) shouldBe "true"
    milvusOption.options(Properties.FsConfig.FsUseVirtualHost) shouldBe "true"
    milvusOption.options(Properties.FsConfig.FsRequestTimeoutMs) shouldBe "30000"
  }

  test("MilvusOption handles GCP specific FsConfig options") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      Properties.FsConfig.FsAddress -> "storage.googleapis.com",
      Properties.FsConfig.FsBucketName -> "gcp-bucket",
      Properties.FsConfig.FsAccessKeyId -> "",
      Properties.FsConfig.FsAccessKeyValue -> "",
      Properties.FsConfig.FsRootPath -> "milvus",
      Properties.FsConfig.FsCloudProvider -> "gcp",
      Properties.FsConfig.FsGcpNativeWithoutAuth -> "false",
      Properties.FsConfig.FsGcpCredentialJson -> """{"type": "service_account", "project_id": "test"}"""
    )

    val milvusOption = MilvusOption(options)

    milvusOption.options(Properties.FsConfig.FsCloudProvider) shouldBe "gcp"
    milvusOption.options(Properties.FsConfig.FsGcpNativeWithoutAuth) shouldBe "false"
    milvusOption.options(Properties.FsConfig.FsGcpCredentialJson) should include("service_account")
  }

  // ============ Consistency Tests ============

  test("FsConfig keys match expected format for milvus-storage C++ API") {
    // These keys must match the C++ API exactly
    // Reference: milvus-storage library configuration
    Properties.FsConfig.FsAddress shouldBe "fs.address"
    Properties.FsConfig.FsBucketName shouldBe "fs.bucket_name"
    Properties.FsConfig.FsAccessKeyId shouldBe "fs.access_key_id"
    Properties.FsConfig.FsAccessKeyValue shouldBe "fs.access_key_value"
    Properties.FsConfig.FsStorageType shouldBe "fs.storage_type"
  }

  test("All required FsConfig keys are defined") {
    // These are required for Storage V2 to work
    Properties.FsConfig.FsAddress should not be empty
    Properties.FsConfig.FsBucketName should not be empty
    Properties.FsConfig.FsAccessKeyId should not be empty
    Properties.FsConfig.FsAccessKeyValue should not be empty
    Properties.FsConfig.FsRootPath should not be empty
    Properties.FsConfig.FsStorageType should not be empty
    Properties.FsConfig.FsUseSSL should not be empty
    Properties.FsConfig.FsRegion should not be empty
  }
}
