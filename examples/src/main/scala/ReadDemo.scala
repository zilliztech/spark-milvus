import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import zilliztech.spark.milvus.{MilvusOptions, MilvusUtils}

import scala.collection.JavaConverters._;

object ReadDemo extends App {

  val sparkConf = new SparkConf().setMaster("local")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val host = "localhost"
  val port = 19530
  val user = "root"
  val password = "Milvus"
  val fs = "s3a://"
  val bucketName = "a-bucket"
  val rootPath = "files"
  val minioAK = "minioadmin"
  val minioSK = "minioadmin"
  val minioEndpoint = "localhost:9000"
  val collectionName = "hello_spark_milvus"

  val properties = Map(
    MilvusOptions.MILVUS_HOST -> host,
    MilvusOptions.MILVUS_PORT -> port.toString,
    MilvusOptions.MILVUS_COLLECTION_NAME -> collectionName,
    MilvusOptions.MILVUS_BUCKET -> bucketName,
    MilvusOptions.MILVUS_ROOTPATH -> rootPath,
    MilvusOptions.MILVUS_FS -> fs,
    MilvusOptions.MILVUS_STORAGE_ENDPOINT -> minioEndpoint,
    MilvusOptions.MILVUS_STORAGE_USER -> minioAK,
    MilvusOptions.MILVUS_STORAGE_PASSWORD -> minioSK,
  )

  // 1, configurations
  val milvusOptions = new MilvusOptions(new CaseInsensitiveStringMap(properties.asJava))

  // 2, batch read milvus collection data to dataframe
  val collectionDF = MilvusUtils.readMilvusCollection(spark, milvusOptions)

  collectionDF.show()
}
