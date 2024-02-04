import io.milvus.client.{MilvusClient, MilvusServiceClient}
import io.milvus.grpc.DataType
import io.milvus.param.collection.{CreateCollectionParam, FieldType}
import io.milvus.param.{ConnectParam, R, RpcStatus}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import zilliztech.spark.milvus.MilvusOptions._
import zilliztech.spark.milvus.{MilvusOptions, MilvusUtils}

import java.util
import scala.collection.JavaConverters._;

object BulkinsertZillizCloudDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // Fill in user's Zilliz Cloud credentials.
    val uri = ""
    val token = ""
    val apiKey = ""
    val s3Endpoint = "https://s3.us-west-1.amazonaws.com"
    val s3ak = ""
    val s3sk = ""

    // Specify the target Zilliz Cloud vector database collection name.
    val collectionName = "databricks_zilliz_demo2"
    // This file simulates a dataframe from user's vector generation job or a Delta table that contains vectors.
    val inputFilePath = "data/insert_demo/dim32_1k.json"
    // User needs to create an external location on databricks with an S3 bucket and specify the directory in the bucket to store vector data.
    // The vectors will be output to the s3 bucket in specific format that can be loaded to Zilliz Cloud efficiently.
    val outputDir = "s3a://zilliz_spark_demo/"
    // The AWS access key and private key which grants only read access to the above s3 bucket. They will be used by Zilliz Cloud Import Data API to load data from the bucket.

    val properties = Map(
      MILVUS_URI -> uri,
      MILVUS_TOKEN -> token,
      MILVUS_COLLECTION_NAME -> collectionName,
      MILVUS_STORAGE_ENDPOINT -> s3Endpoint,
      MILVUS_STORAGE_USER -> s3ak,
      MILVUS_STORAGE_PASSWORD -> s3sk,
      ZILLIZCLOUD_API_KEY -> apiKey,
    )
    val milvusOptions = new MilvusOptions(new CaseInsensitiveStringMap(properties.asJava))
    MilvusUtils.configS3Storage(spark, milvusOptions)

    // 1. Create Zilliz Cloud vector db collection through SDK, and define the schema of the collection.
    val connectParam: ConnectParam = ConnectParam.newBuilder
      .withUri(uri)
      .withToken(token)
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

    // 2. Read data from file to build vector dataframe. The schema of the dataframe must logically match the schema of vector db.
    val df = spark.read
      .schema(new StructType()
        .add(field1Name, IntegerType)
        .add(field2Name, StringType)
        .add(field3Name, ArrayType(FloatType), false))
      .json(inputFilePath)

    // 3. Store all vector data in the s3 bucket to prepare for loading.
    df.repartition(1)
      .write
      .format("mjson")
      .mode("overwrite")
      .save(outputDir)

    MilvusUtils.bulkInsertFromSpark(spark, milvusOptions, outputDir, "json")
  }
}

