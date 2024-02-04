import InsertDemo.getClass
import io.milvus.client.{MilvusClient, MilvusServiceClient}
import io.milvus.grpc.DataType
import io.milvus.param.{ConnectParam, R, RpcStatus}
import io.milvus.param.collection.{CreateCollectionParam, FieldType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.{MilvusOptions, MilvusUtils}
import zilliztech.spark.milvus.MilvusOptions.{MILVUS_BUCKET, MILVUS_COLLECTION_NAME, MILVUS_HOST, MILVUS_PORT, MILVUS_STORAGE_ENDPOINT, MILVUS_STORAGE_PASSWORD, MILVUS_STORAGE_USER, MILVUS_TOKEN, MILVUS_URI}

import java.util
import scala.collection.JavaConverters._;

object BulkInsertDemo {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]):Unit = {
    val sparkConf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // save data to storage
    val filePath = "data/insert_demo/dim32_1k.json"
    val host = "localhost"
    val port = 19530

    val outputPath = "s3a://a-bucket/dim32_1k.parquet"
    val collectionName = "hello_spark_milvus"
    val properties = Map(
      MILVUS_HOST -> host,
      MILVUS_PORT -> port.toString,
      MILVUS_COLLECTION_NAME -> collectionName,
      MILVUS_STORAGE_USER -> "minioadmin",
      MILVUS_STORAGE_PASSWORD -> "minioadmin",
      MILVUS_STORAGE_ENDPOINT -> "localhost:9000",
      MILVUS_BUCKET -> "a-bucket",
    )
    val milvusOptions = new MilvusOptions(new CaseInsensitiveStringMap(properties.asJava))

    MilvusUtils.configS3Storage(spark, milvusOptions)

    // 1. create milvus collection through milvus SDK
    val connectParam: ConnectParam = ConnectParam.newBuilder
      .withHost(host)
      .withPort(port)
      .build

    val client: MilvusClient = new MilvusServiceClient(connectParam)

    val field1Name: String = "id_field"
    val field2Name: String = "str_field"
    val field3Name: String = "float_vector_field"
    val fieldsSchema: util.List[FieldType] = new util.ArrayList[FieldType]

    fieldsSchema.add(FieldType.newBuilder
      .withPrimaryKey(true)
      .withAutoID(false)
      .withDataType(DataType.Int64)
      .withName(field1Name)
      .build
    )
    fieldsSchema.add(FieldType.newBuilder
      .withDataType(DataType.VarChar)
      .withName(field2Name)
      .withMaxLength(65535)
      .build
    )
    fieldsSchema.add(FieldType.newBuilder
      .withDataType(DataType.FloatVector)
      .withName(field3Name)
      .withDimension(32)
      .build
    )

    // create collection
    val createParam: CreateCollectionParam = CreateCollectionParam.newBuilder
      .withCollectionName(collectionName)
      .withFieldTypes(fieldsSchema)
      .build

    val createR: R[RpcStatus] = client.createCollection(createParam)

    log.info(s"create collection ${collectionName} resp: ${createR.toString}")

    val df = spark.read.json(filePath)

    df.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath)

    MilvusUtils.bulkInsertFromSpark(spark, milvusOptions, outputPath, "parquet")
  }

}
