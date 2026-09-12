package com.zilliz.spark.connector.loon

import scala.collection.JavaConverters._

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.MilvusOption
import io.milvus.storage.MilvusStorageProperties

/** Hands MilvusOption's storage configuration to the native layer.
  *
  * Parsing and validation live in `core.credential`; this object only adapts
  * the option map to the upstream Java binding.
  */
object Properties {

  /** Filesystem configuration constants for Storage V2 (matching milvus-storage
    * C++ API)
    */
  object FsConfig {
    val FsAddress = StorageProperties.Address
    val FsBucketName = StorageProperties.BucketName
    val FsAccessKeyId = StorageProperties.AccessKeyId
    val FsAccessKeyValue = StorageProperties.AccessKeyValue
    val FsRootPath = StorageProperties.RootPath
    val FsStorageType = StorageProperties.StorageType
    val FsCloudProvider = StorageProperties.CloudProvider
    val FsIamEndpoint = "fs.iam_endpoint"
    val FsLogLevel = "fs.log_level"
    val FsRegion = StorageProperties.Region
    val FsUseSSL = StorageProperties.UseSSL
    val FsSslCaCert = "fs.ssl_ca_cert"
    val FsUseIam = StorageProperties.UseIam
    val FsRoleArn = StorageProperties.RoleArn
    val FsSessionName = StorageProperties.SessionName
    val FsExternalId = StorageProperties.ExternalId
    val FsUseVirtualHost = "fs.use_virtual_host"
    val FsRequestTimeoutMs = "fs.request_timeout_ms"
    val FsGcpNativeWithoutAuth = "fs.gcp_native_without_auth"
    val FsGcpCredentialJson = "fs.gcp_credential_json"
    val FsUseCustomPartUpload = "fs.use_custom_part_upload"
  }

  def fromMilvusOption(milvusOption: MilvusOption): MilvusStorageProperties = {
    val propsMap = StorageProperties.from(milvusOption.options)

    val props = new MilvusStorageProperties()
    props.create(new java.util.HashMap[String, String](propsMap.asJava))
    if (!props.isValid) {
      throw new IllegalStateException(
        "Failed to create MilvusStorageProperties"
      )
    }
    props
  }
}
