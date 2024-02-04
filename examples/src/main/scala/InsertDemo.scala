import io.milvus.client.{MilvusClient, MilvusServiceClient}
import io.milvus.grpc.{DataType, FlushResponse}
import io.milvus.param.collection.{CreateCollectionParam, FieldType, FlushParam, LoadCollectionParam}
import io.milvus.param.dml.SearchParam
import io.milvus.param.index.CreateIndexParam
import io.milvus.param.{ConnectParam, IndexType, MetricType, R, RpcStatus}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.MilvusOptions._

import java.util

object InsertDemo {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("InsertDemo")
      .getOrCreate()

    val host = "localhost"
    val port = 19530
    val uri = ""
    val token = ""
    val collectionName = "hello_spark_milvus"
    val filePath = "data/insert_demo/dim32_1k.json"

    // 1. create milvus collection through milvus SDK
    val connectParam: ConnectParam = ConnectParam.newBuilder
      .withHost(host)
      .withPort(port)
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

    log.info(s"create collection ${collectionName} resp: ${createR.toString}")

    // 2. read data from file
    val df = spark.read
      .schema(new StructType()
        .add(field1Name, LongType)
        .add(field2Name, StringType)
        .add(field3Name, ArrayType(FloatType), false))
      .json(filePath)

    // 3. configure output target
    val milvusOptions = Map(
      MILVUS_URI -> uri,
      MILVUS_TOKEN -> token,
      MILVUS_HOST -> host,
      MILVUS_PORT -> port.toString,
      MILVUS_COLLECTION_NAME -> collectionName,
//      MILVUS_COLLECTION_VECTOR_FIELD -> "float_vector_field",
//      MILVUS_COLLECTION_VECTOR_DIM -> "32",
//      MILVUS_COLLECTION_PRIMARY_KEY -> "id_field",
    )

    // 3, insert data to milvus collection
    // inner is a iterator insert by row through Milvus SDK insert API
    df.write
      .options(milvusOptions)
      .format("milvus")
      .mode(SaveMode.Append)
      .save()

    // 4, flush data
    val flushParam: FlushParam = FlushParam.newBuilder
      .addCollectionName(collectionName)
      .build
    val flushR: R[FlushResponse] = client.flush(flushParam)
    log.info(s"flush response ${flushR}")

    // 5, create index
    val createIndexParam = CreateIndexParam.newBuilder()
      .withCollectionName(collectionName)
      .withIndexName("index_name")
      .withFieldName("float_vector_field")
      .withMetricType(MetricType.L2)
      .withIndexType(IndexType.AUTOINDEX)
      .build()
    val createIndexR = client.createIndex(createIndexParam)
    log.info(s"create index response ${createIndexR}")

    // 6, load collection
    val loadCollectionParam = LoadCollectionParam.newBuilder().withCollectionName(collectionName).build()
    val loadCollectionR = client.loadCollection(loadCollectionParam)
    log.info(s"load collection response ${loadCollectionR}")

    // 7, search
    val fieldList: util.List[String] = new util.ArrayList[String]()
    fieldList.add("float_vector_field")

    // 8, use the first row as search vector
    val searchVectors = util.Arrays.asList(df.first().getList(2))
    val searchParam = SearchParam.newBuilder()
      .withCollectionName(collectionName)
      .withMetricType(MetricType.L2)
      .withOutFields(fieldList)
      .withVectors(searchVectors)
      .withVectorFieldName("float_vector_field")
      .withTopK(10)
      .build()
    val searchParamR = client.search(searchParam)
    log.info(s"search response ${searchParamR}")

    client.close()
  }
}
