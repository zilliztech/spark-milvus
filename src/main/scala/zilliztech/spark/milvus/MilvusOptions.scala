package zilliztech.spark.milvus

import org.apache.spark.sql.util.CaseInsensitiveStringMap

object MilvusOptions {

  // configs for connection
  val MILVUS_URI = "milvus.uri"
  val MILVUS_TOKEN = "milvus.token"
  val MILVUS_HOST = "milvus.host"
  val MILVUS_SEVERNAME = "milvus.servername"
  val MILVUS_PORT = "milvus.port"
  val MILVUS_USERNAME = "milvus.username"
  val MILVUS_PASSWORD = "milvus.password"
  val MILVUS_SECURE = "milvus.secure"
  val MILVUS_CA_CERT = "milvus.caCert"
  val MILVUS_CLIENT_CERT = "milvus.clientCert"
  val MILVUS_CLIENT_KEY = "milvus.clientKey"



  // configs for milvus storage, only used when using MilvusUtils
  val MILVUS_BUCKET = "milvus.bucket"
  val MILVUS_ROOTPATH = "milvus.rootpath"
  val MILVUS_FS = "milvus.fs"
  val MILVUS_STORAGE_ENDPOINT = "milvus.storage.endpoint"
  val MILVUS_STORAGE_USER = "milvus.storage.user"
  val MILVUS_STORAGE_PASSWORD = "milvus.storage.password"
  val MILVUS_STORAGE_USESSL = "milvus.storage.useSSL"

  // configs of collection, use in both reading and writing
  val MILVUS_DATABASE_NAME = "milvus.database.name"
  val MILVUS_COLLECTION_NAME = "milvus.collection.name"
  val MILVUS_PARTITION_NAME = "milvus.partition.name"

  // configs of collection schema, only used in automatic creating collection when writing
  val MILVUS_COLLECTION_PRIMARY_KEY = "milvus.collection.primaryKeyField"
  val MILVUS_COLLECTION_VECTOR_FIELD = "milvus.collection.vectorField"
  val MILVUS_COLLECTION_VECTOR_DIM = "milvus.collection.vectorDim"
  val MILVUS_COLLECTION_AUTOID = "milvus.collection.autoID"

  // configs for zilliz cloud
  val ZILLIZCLOUD_REGION = "zillizcloud.region"
  val ZILLIZCLOUD_INSTANCE_ID = "zillizcloud.instanceID"
  val ZILLIZCLOUD_API_KEY = "zillizcloud.apiKey"

  // max row size in one insert request to Milvus, data will be split and inserted by this size
  val MILVUS_INSERT_MAX_BATCHSIZE = "milvus.insertMaxBatchSize"

}

class MilvusOptions(config: CaseInsensitiveStringMap) extends Serializable {

  import MilvusOptions._

  // connection
  val host: String = config.getOrDefault(MILVUS_HOST, "localhost")
  val port: Int = config.getInt(MILVUS_PORT, 19530)
  val userName: String = config.getOrDefault(MILVUS_USERNAME, "root")
  val password: String = config.getOrDefault(MILVUS_PASSWORD, "milvus")
  val uri: String = config.getOrDefault(MILVUS_URI, "")
  val token: String = config.getOrDefault(MILVUS_TOKEN, "")
  val servername: String = config.getOrDefault(MILVUS_SEVERNAME, "localhost")
  val secure: Boolean = config.getBoolean(MILVUS_SECURE, false)
  val caCert: String = config.getOrDefault(MILVUS_CA_CERT, "")
  val clientKey: String = config.getOrDefault(MILVUS_CLIENT_KEY, "")
  val clientCert: String = config.getOrDefault(MILVUS_CLIENT_CERT, "")
  if (secure && (caCert.isEmpty || clientCert.isEmpty || clientKey.isEmpty)) {
    throw new IllegalArgumentException("Secure connection requires caCert, clientKey, and clientCert to be provided.")
  }

  // zilliz cloud
  val zillizCloudRegion: String = config.getOrDefault(ZILLIZCLOUD_REGION, "")
  val zillizCloudInstanceID: String = config.getOrDefault(ZILLIZCLOUD_INSTANCE_ID, "")
  val zillizCloudAPIKey: String = config.getOrDefault(ZILLIZCLOUD_API_KEY, "")

  val fs: String = config.getOrDefault(MILVUS_FS, "s3a://")
  val bucket: String = config.getOrDefault(MILVUS_BUCKET, "a-bucket")
  val rootPath: String = config.getOrDefault(MILVUS_ROOTPATH, "files")
  val storageUseSSL: String = config.getOrDefault(MILVUS_STORAGE_USESSL, "false")
  val storageEndpoint: String = config.getOrDefault(MILVUS_STORAGE_ENDPOINT, "localhost:9000")
  val storageUser: String = config.getOrDefault(MILVUS_STORAGE_USER, "minioadmin")
  val storagePassword: String = config.getOrDefault(MILVUS_STORAGE_PASSWORD, "minioadmin")

  // collection
  // Zillizcloud forbid listdatabases and have no database concept in serverless mode
  val databaseName: String = config.getOrDefault(MILVUS_DATABASE_NAME, if (isZillizCloud()) { "" } else {"default"})
  val collectionName: String = config.getOrDefault(MILVUS_COLLECTION_NAME, "")
  val partitionName: String = config.getOrDefault(MILVUS_PARTITION_NAME, "")

  val primaryKeyField: String = config.getOrDefault(MILVUS_COLLECTION_PRIMARY_KEY, "")
  val vectorField: String = config.getOrDefault(MILVUS_COLLECTION_VECTOR_FIELD, "")
  val vectorDim: Int = config.getInt(MILVUS_COLLECTION_VECTOR_DIM, 32)
  val isAutoID: Boolean = config.getBoolean(MILVUS_COLLECTION_AUTOID, false)

  // insert option
  val maxBatchSize: Int = config.getInt(MILVUS_INSERT_MAX_BATCHSIZE, 200)

  override def toString = s"MilvusOptions($host, $port, $uri, $token, $userName, secure=$secure)"

  def zillizInstanceID(): String = {
    zillizInstanceIDAndRegion()._1
  }

  def zillizRegion(): String = {
    zillizInstanceIDAndRegion()._2
  }

  private def zillizInstanceIDAndRegion(): (String, String) = {
    if (zillizCloudInstanceID != "" && zillizCloudRegion != "") {
      return (zillizCloudInstanceID, zillizCloudRegion)
    }
    val regex = """https://(in[a-zA-Z0-9-]+)\.([a-zA-Z0-9-]+)\.vectordb\.zillizcloud\.com:\d+""".r

    // 匹配正则表达式
    val matchResult = regex.findFirstMatchIn(uri)
    if (matchResult.isEmpty) {
      throw new Exception("Failed to parse zilliz instance from uri, you can set it directly zillizcloud.instanceID=xxx")
    }
    val instanceID = matchResult.get.group(1)
    val region = matchResult.get.group(2)
    (instanceID, region)
  }

  def isZillizCloud(): Boolean = {
    uri.contains("zillizcloud")
  }
}
