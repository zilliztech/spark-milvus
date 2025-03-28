import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import zilliztech.spark.milvus.{MilvusOptions, MilvusUtils}

import scala.collection.JavaConverters._;

// NOTE: This is a demo code to read data from Milvus collection and show it in Spark DataFrame.
// If you want to run this code, please set the REPO_PATH environment variable to the path of your local repository, which is the parent directory of the spark-milvus directory.
// For example, if your spark-milvus directory is /home/user/spark-milvus, set REPO_PATH=/home/user
object ReadDemo extends App {

  val sparkConf = new SparkConf().setMaster("local")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import scala.io.Source
  import scala.util.Try
  val filePath = sys.env.getOrElse("REPO_PATH", "") + "/spark-milvus/examples/read_demo_milvus.properties"
  val configMap: Map[String, String] = Try {
    Source.fromFile(filePath).getLines()
      .filter(line => line.trim.nonEmpty && !line.trim.startsWith("#"))
      .map { line =>
        val parts = line.split("=", 2).map(_.trim)
        parts(0) -> parts.lift(1).getOrElse("")
      }.toMap
  }.getOrElse(Map.empty)
  val milvusOptions = new MilvusOptions(new CaseInsensitiveStringMap(configMap.asJava))

  // 2, batch read milvus collection data to dataframe
  val collectionDF = MilvusUtils.readMilvusCollection(spark, milvusOptions)

  collectionDF.show()
}
