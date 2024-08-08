import io.milvus.client.{MilvusClient, MilvusServiceClient}
import io.milvus.grpc.{DataType, FlushResponse}
import io.milvus.param.{ConnectParam, IndexType, MetricType, R, RpcStatus}
import io.milvus.param.collection.{CreateCollectionParam, FieldType, FlushParam, LoadCollectionParam}
import io.milvus.param.index.CreateIndexParam
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.MilvusOptions.{MILVUS_COLLECTION_NAME, MILVUS_HOST, MILVUS_PORT, MILVUS_TOKEN, MILVUS_URI}

import java.util

object BF16VectorInsertDemo {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("BF16VectorInsertDemo")
      .getOrCreate()

    val host = "172.18.50.31"
    val port = 19530
    val uri = ""
    val token = ""
    val collectionName = "b16f_vector_test"
    val filePath = "data/insert_demo/data.json"

    // 1. create milvus collection through milvus SDK
    val connectParam: ConnectParam = ConnectParam.newBuilder
      .withHost(host)
      .withPort(port)
      .withUri(uri)
      .withToken(token)
      .build

    val client: MilvusClient = new MilvusServiceClient(connectParam)

    val idField: String = "id"
    val vectorField: String = "bf16_vector"

    val fieldsSchema: util.List[FieldType] = new util.ArrayList[FieldType]

    fieldsSchema.add(FieldType.newBuilder
      .withPrimaryKey(true)
      .withAutoID(false)
      .withDataType(DataType.Int64)
      .withName(idField)
      .build
    )
    fieldsSchema.add(FieldType.newBuilder
      .withDataType(DataType.BFloat16Vector)
      .withName(vectorField)
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
        .add(idField, LongType)
        .add(vectorField, StringType)
      )
      .json(filePath)

    // 3. configure output target
    val milvusOptions = Map(
      MILVUS_URI -> uri,
      MILVUS_TOKEN -> token,
      MILVUS_HOST -> host,
      MILVUS_PORT -> port.toString,
      MILVUS_COLLECTION_NAME -> collectionName,
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
      .withFieldName(vectorField)
      .withMetricType(MetricType.COSINE)
      .withIndexType(IndexType.AUTOINDEX)
      .build()
    val createIndexR = client.createIndex(createIndexParam)
    log.info(s"create index response ${createIndexR}")

    // 6, load collection
    val loadCollectionParam = LoadCollectionParam.newBuilder().withCollectionName(collectionName).build()
    val loadCollectionR = client.loadCollection(loadCollectionParam)
    log.info(s"load collection response ${loadCollectionR}")

    client.close()
  }
}
