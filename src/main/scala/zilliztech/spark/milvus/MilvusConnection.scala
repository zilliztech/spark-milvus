package zilliztech.spark.milvus

import io.milvus.client.MilvusServiceClient
import io.milvus.param.ConnectParam

import scala.collection.mutable

case class MilvusConnection (
                            client: MilvusServiceClient) {
}

object MilvusConnection {
  private val cache = new mutable.HashMap[String, MilvusServiceClient]

  def acquire(milvusOptions: MilvusOptions): MilvusServiceClient = {
    lazy val connectParam = if (milvusOptions.uri.isEmpty) {
      val builder = ConnectParam.newBuilder
        .withHost(milvusOptions.host)
        .withPort(milvusOptions.port)
        .withAuthorization(milvusOptions.userName, milvusOptions.password)
        .withDatabaseName(milvusOptions.databaseName)
        .withServerPemPath(milvusOptions.caCert)
        .withServerName(milvusOptions.servername)
      
      if (milvusOptions.secure) {
        if (!milvusOptions.clientCert.isEmpty) {
          builder.withServerName(milvusOptions.host)
          builder.withCaPemPath(milvusOptions.caCert)
          builder.withClientKeyPath(milvusOptions.clientKey)
          builder.withClientPemPath(milvusOptions.clientCert)
        }
      }
      
      builder.build
    } else {
      // zilliz cloud
      val builder = ConnectParam.newBuilder
        .withUri(milvusOptions.uri)
        .withToken(milvusOptions.token)
        .withServerPemPath(milvusOptions.caCert)
        .withServerName(milvusOptions.servername)
      
      if (milvusOptions.secure) {
        if (!milvusOptions.clientCert.isEmpty) {
          builder.withServerName(milvusOptions.host)
          builder.withCaPemPath(milvusOptions.caCert)
          builder.withClientKeyPath(milvusOptions.clientKey)
          builder.withClientPemPath(milvusOptions.clientCert)
        }
      }
      
      builder.build
    }

    new MilvusServiceClient(connectParam)
//    cache.getOrElseUpdate(milvusOptions.toString, new MilvusServiceClient(connectParam))
  }
}