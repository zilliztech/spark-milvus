package com.zilliz.spark.connector.loon

import com.zilliz.milvus.storage.credential.StorageProperties

/** The `fs.*` key names, spelled once.
  *
  * Every name here is an alias for the one `core.credential.StorageProperties`
  * defines, so the option a user writes and the key the C layer reads cannot
  * drift apart. Parsing and validation live there; this object holds no logic.
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
}
