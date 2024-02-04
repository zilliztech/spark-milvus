package zilliztech.spark.milvus

import io.milvus.grpc.ErrorCode
import io.milvus.param.collection.{CreateCollectionParam, HasCollectionParam}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

case class Milvus() extends TableProvider with DataSourceRegister {

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]
                       ): Table = {
    val milvusOptions = new MilvusOptions(new CaseInsensitiveStringMap(properties))

    val client = MilvusConnection.acquire(milvusOptions)
    val hasCollectionParams = HasCollectionParam.newBuilder
      .withDatabaseName(milvusOptions.databaseName)
      .withCollectionName(milvusOptions.collectionName)
      .build()
    val hasCollectionResponse = client.hasCollection(hasCollectionParams)
    if (hasCollectionResponse.getStatus != ErrorCode.Success.getNumber) {
      if (hasCollectionResponse.getException != None) {
        throw new Exception("Fail to hasCollection", hasCollectionResponse.getException)
      }
      throw new Exception(s"Fail to hasCollection response: ${hasCollectionResponse.toString}")
    }
    val exist = hasCollectionResponse.getData

    if (!exist) {
      val createCollectionParamBuilder = CreateCollectionParam.newBuilder
        .withDatabaseName(milvusOptions.databaseName)
        .withCollectionName(milvusOptions.collectionName)
      schema.fields.foreach(field => {
        createCollectionParamBuilder.addFieldType(
          MilvusCollection.ToMilvusField(field, milvusOptions)
        )
      })
      val createCollectionParam = createCollectionParamBuilder.build()
      val response = client.createCollection(createCollectionParam)
      if (response.getStatus != ErrorCode.Success.getNumber) {
        if (response.getException != None) {
          throw new Exception("Fail to create collection", response.getException)
        }
        throw new Exception(s"Fail to create collection response: ${response.toString}")
      }
    }
    client.close()
    MilvusCollection(milvusOptions, Some(schema))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOptions = new MilvusOptions(options)
    MilvusCollection(milvusOptions, Option.empty).schema()
  }

  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}

