import io.milvus.client.{MilvusClient, MilvusServiceClient}
import io.milvus.grpc.{DataType, FlushResponse}
import io.milvus.param.collection.{CreateCollectionParam, FieldType, FlushParam, LoadCollectionParam}
import io.milvus.param.dml.SearchParam
import io.milvus.param.index.CreateIndexParam
import io.milvus.param._
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.MilvusOptions._

import java.util

object SparseVectorInsertDemo {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("SparseVectorDemo")
      .getOrCreate()

    val host = "localhost"
    val port = 19530
    val uri = ""
    val token = ""
    val collectionName = "sparse_vector_demo"
    val filePath = "data/insert_demo/sparse_demo.sample.100.json"

    // 1. create milvus collection through milvus SDK
    val connectParam: ConnectParam = ConnectParam.newBuilder
      .withHost(host)
      .withPort(port)
      .withUri(uri)
      .withToken(token)
      .build

    val client: MilvusClient = new MilvusServiceClient(connectParam)

    val idField: String = "id_field"
    val sparseVectorField: String = "sparse_vector_field"
    val vcharField: String = "vchar_field"

    val fieldsSchema: util.List[FieldType] = new util.ArrayList[FieldType]

    fieldsSchema.add(FieldType.newBuilder
      .withPrimaryKey(true)
      .withAutoID(false)
      .withDataType(DataType.Int64)
      .withName(idField)
      .build
    )
    fieldsSchema.add(FieldType.newBuilder
      .withDataType(DataType.SparseFloatVector)
      .withName(sparseVectorField)
      .build
    )
    fieldsSchema.add(FieldType.newBuilder
      .withDataType(DataType.VarChar)
      .withName(vcharField)
      .withMaxLength(32)
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
        .add(sparseVectorField, StringType)
        .add(vcharField, StringType)
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
      .withFieldName(sparseVectorField)
      .withMetricType(MetricType.IP)
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
    fieldList.add(sparseVectorField)

    // 8, search
    import java.util.{ArrayList, List => JavaList, SortedMap => JavaSortedMap}

    // Create a Java SortedMap
    val searchVector: JavaSortedMap[java.lang.Long, java.lang.Float] = new java.util.TreeMap()
    searchVector.put(7L, 0.2218893160534381f)
    searchVector.put(1L, 0.985183160534381f)

    // Create a Java List and add the Java SortedMap to it
    val searchVectors: JavaList[JavaSortedMap[java.lang.Long, java.lang.Float]] = new ArrayList()
    searchVectors.add(searchVector)

    val searchParam = SearchParam.newBuilder()
      .withCollectionName(collectionName)
      .withMetricType(MetricType.IP)
      .withOutFields(fieldList)
      .withSparseFloatVectors(searchVectors)
      .withVectorFieldName(sparseVectorField)
      .withTopK(10)
      .build()
    val searchParamR = client.search(searchParam)
    log.info(s"search response ${searchParamR}")

    client.close()
  }

}
