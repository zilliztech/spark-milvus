package com.zilliz.spark.connector.loon

import java.{util => ju}
import io.milvus.storage.MilvusStorageProperties
import com.zilliz.spark.connector.MilvusOption

/**
 * Utilities for creating MilvusStorageProperties from S3 configuration
 */
object Properties {

  /**
   * Filesystem configuration constants for Storage V2 (matching milvus-storage C++ API)
   */
  object FsConfig {
    val FsAddress = "fs.address"
    val FsBucketName = "fs.bucket_name"
    val FsAccessKeyId = "fs.access_key_id"
    val FsAccessKeyValue = "fs.access_key_value"
    val FsRootPath = "fs.root_path"
    val FsStorageType = "fs.storage_type"
    val FsCloudProvider = "fs.cloud_provider"
    val FsIamEndpoint = "fs.iam_endpoint"
    val FsLogLevel = "fs.log_level"
    val FsRegion = "fs.region"
    val FsUseSSL = "fs.use_ssl"
    val FsSslCaCert = "fs.ssl_ca_cert"
    val FsUseIam = "fs.use_iam"
    val FsUseVirtualHost = "fs.use_virtual_host"
    val FsRequestTimeoutMs = "fs.request_timeout_ms"
    val FsGcpNativeWithoutAuth = "fs.gcp_native_without_auth"
    val FsGcpCredentialJson = "fs.gcp_credential_json"
    val FsUseCustomPartUpload = "fs.use_custom_part_upload"
  }

  /**
   * Create MilvusStorageProperties from MilvusOption
   * Builds properties for Storage V2 FFI from filesystem configuration in MilvusOption
   *
   * @param milvusOption MilvusOption containing filesystem configuration
   * @return MilvusStorageProperties for milvus-storage native library
   */
  def fromMilvusOption(milvusOption: MilvusOption): MilvusStorageProperties = {
    val props = new MilvusStorageProperties()
    val propsMap = new ju.HashMap[String, String]()

    // Extract filesystem configuration with defaults
    val endpoint = milvusOption.options.getOrElse(FsConfig.FsAddress,"localhost:9000")
    val bucket = milvusOption.options.getOrElse(FsConfig.FsBucketName, "a-bucket")
    val rootPath = milvusOption.options.getOrElse(FsConfig.FsRootPath, "files")
    val accessKey = milvusOption.options.getOrElse(FsConfig.FsAccessKeyId, "minioadmin")
    val secretKey = milvusOption.options.getOrElse(FsConfig.FsAccessKeyValue, "minioadmin")
    val useSsl = milvusOption.options.getOrElse(FsConfig.FsUseSSL, "false")
    val storageType = milvusOption.options.getOrElse(FsConfig.FsStorageType, "remote")
    val region = milvusOption.options.getOrElse(FsConfig.FsRegion, "us-west-2")

    // Set required properties
    propsMap.put(FsConfig.FsStorageType, storageType)
    propsMap.put(FsConfig.FsAccessKeyId, accessKey)
    propsMap.put(FsConfig.FsAccessKeyValue, secretKey)
    propsMap.put(FsConfig.FsBucketName, bucket)
    propsMap.put(FsConfig.FsRootPath, rootPath)
    propsMap.put(FsConfig.FsUseSSL, useSsl)
    propsMap.put(FsConfig.FsAddress, endpoint)
    propsMap.put(FsConfig.FsRegion, region)

    // Set optional properties if present
    milvusOption.options.get(FsConfig.FsCloudProvider).foreach(propsMap.put(FsConfig.FsCloudProvider, _))
    milvusOption.options.get(FsConfig.FsIamEndpoint).foreach(propsMap.put(FsConfig.FsIamEndpoint, _))
    milvusOption.options.get(FsConfig.FsLogLevel).foreach(propsMap.put(FsConfig.FsLogLevel, _))
    milvusOption.options.get(FsConfig.FsSslCaCert).foreach(propsMap.put(FsConfig.FsSslCaCert, _))
    milvusOption.options.get(FsConfig.FsUseIam).foreach(propsMap.put(FsConfig.FsUseIam, _))
    milvusOption.options.get(FsConfig.FsUseVirtualHost).foreach(propsMap.put(FsConfig.FsUseVirtualHost, _))
    milvusOption.options.get(FsConfig.FsRequestTimeoutMs).foreach(propsMap.put(FsConfig.FsRequestTimeoutMs, _))
    milvusOption.options.get(FsConfig.FsGcpNativeWithoutAuth).foreach(propsMap.put(FsConfig.FsGcpNativeWithoutAuth, _))
    milvusOption.options.get(FsConfig.FsGcpCredentialJson).foreach(propsMap.put(FsConfig.FsGcpCredentialJson, _))
    milvusOption.options.get(FsConfig.FsUseCustomPartUpload).foreach(propsMap.put(FsConfig.FsUseCustomPartUpload, _))

    props.create(propsMap)
    if (!props.isValid) {
      throw new IllegalStateException("Failed to create MilvusStorageProperties")
    }
    props
  }
}
