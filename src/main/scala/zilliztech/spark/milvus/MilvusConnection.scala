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
      ConnectParam.newBuilder
        .withHost(milvusOptions.host)
        .withPort(milvusOptions.port)
        .withAuthorization(milvusOptions.userName, milvusOptions.password)
        .build
    } else {
      // zilliz cloud
      ConnectParam.newBuilder
        .withUri(milvusOptions.uri)
        .withToken(milvusOptions.token)
        .build
    }

    new MilvusServiceClient(connectParam)
//    cache.getOrElseUpdate(milvusOptions.toString, new MilvusServiceClient(connectParam))
  }
}