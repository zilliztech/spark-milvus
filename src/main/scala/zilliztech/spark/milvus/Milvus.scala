package zilliztech.spark.milvus

import io.milvus.grpc.ErrorCode
import io.milvus.param.collection.{CreateCollectionParam, CreateDatabaseParam, HasCollectionParam}
import io.milvus.param.partition.{CreatePartitionParam, HasPartitionParam}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.convert.ImplicitConversions.{`collection asJava`, `iterable AsScalaIterable`}

case class Milvus() extends TableProvider with DataSourceRegister {

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]
                       ): Table = {
    val milvusOptions = new MilvusOptions(new CaseInsensitiveStringMap(properties))
    val isZillizCloud = milvusOptions.isZillizCloud()
    val client = MilvusConnection.acquire(milvusOptions)

    // zillizCloud forbid listDatabases interface
    if (!isZillizCloud) {
      val listDatabasesResponse = client.listDatabases()
      if (listDatabasesResponse.getStatus != ErrorCode.Success.getNumber) {
        throw new Exception(s"Fail to list databases: ${listDatabasesResponse.toString}")
      }
      val databases = listDatabasesResponse.getData.getDbNamesList.asByteStringList().map(x => x.toStringUtf8)
      val databaseExist = databases.contains(
        milvusOptions.databaseName
      )
      if (!databaseExist) {
        val createDatabaseParam = CreateDatabaseParam.newBuilder
          .withDatabaseName(milvusOptions.databaseName).build()
        val response = client.createDatabase(createDatabaseParam)
        if (response.getStatus != ErrorCode.Success.getNumber) {
          if (response.getException != None) {
            throw new Exception("Fail to create database", response.getException)
          }
          throw new Exception(s"Fail to create database response: ${response.toString}")
        }
      }
    }

    val hasCollectionParams = if (isZillizCloud && milvusOptions.databaseName.equals("")) {
      // zilliz cloud may have no database concept(serverless instance)
      HasCollectionParam.newBuilder
        .withCollectionName(milvusOptions.collectionName)
        .build()
    } else {
      HasCollectionParam.newBuilder
        .withDatabaseName(milvusOptions.databaseName)
        .withCollectionName(milvusOptions.collectionName)
        .build()
    }
    val hasCollectionResponse = client.hasCollection(hasCollectionParams)
    if (hasCollectionResponse.getStatus != ErrorCode.Success.getNumber) {
      if (hasCollectionResponse.getException != None) {
        throw new Exception("Fail to hasCollection", hasCollectionResponse.getException)
      }
      throw new Exception(s"Fail to hasCollection response: ${hasCollectionResponse.toString}")
    }
    val collectionExist = hasCollectionResponse.getData

    if (!collectionExist) {
      val createCollectionParamBuilder = if (isZillizCloud && milvusOptions.databaseName.equals("")) {
        CreateCollectionParam.newBuilder
          .withCollectionName(milvusOptions.collectionName)
      } else {
        CreateCollectionParam.newBuilder
          .withDatabaseName(milvusOptions.databaseName)
          .withCollectionName(milvusOptions.collectionName)
      }
      schema.fields.foreach(field => {
        createCollectionParamBuilder.addFieldType(
          MilvusCollection.ToMilvusField(field, milvusOptions)
        )
      })
      val createCollectionParam = createCollectionParamBuilder.build()
      val createCollectionResponse = client.createCollection(createCollectionParam)
      if (createCollectionResponse.getStatus != ErrorCode.Success.getNumber) {
        if (createCollectionResponse.getException != None) {
          throw new Exception("Fail to create collection", createCollectionResponse.getException)
        }
        throw new Exception(s"Fail to create collection response: ${createCollectionResponse.toString}")
      }
    }

    if (!milvusOptions.partitionName.equals("")) {
      val hasPartitionParams = HasPartitionParam.newBuilder
        .withCollectionName(milvusOptions.collectionName)
        .withPartitionName(milvusOptions.partitionName)
        .build()
      val hasPartitionResponse = client.hasPartition(hasPartitionParams)
      if (hasPartitionResponse.getStatus != ErrorCode.Success.getNumber) {
        if (hasPartitionResponse.getException != None) {
          throw new Exception("Fail to hasPartition", hasPartitionResponse.getException)
        }
        throw new Exception(s"Fail to hasPartition response: ${hasPartitionResponse.toString}")
      }
      val partitionExist = hasPartitionResponse.getData

      if (!partitionExist) {
        val createPartitionParam = CreatePartitionParam.newBuilder
          .withCollectionName(milvusOptions.collectionName)
          .withPartitionName(milvusOptions.partitionName)
          .build()
        val createPartitionResponse = client.createPartition(createPartitionParam)
        if (createPartitionResponse.getStatus != ErrorCode.Success.getNumber) {
          if (createPartitionResponse.getException != None) {
            throw new Exception("Fail to create partition", createPartitionResponse.getException)
          }
          throw new Exception(s"Fail to create partition response: ${createPartitionResponse.toString}")
        }
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

