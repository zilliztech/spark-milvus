import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadMilvusBinlogDemo {
  def main(args: Array[String]):Unit = {

    val sparkConf = new SparkConf().setMaster("local")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val path = "data/read_binlog_demo/vector_binlog"

    val df = spark.read.
      format("milvusbinlog")
      .load(path)
      .withColumnRenamed("val", "topic")

    df.show()
  }
}


