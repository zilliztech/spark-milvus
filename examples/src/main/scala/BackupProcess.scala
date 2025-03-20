import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object BackupProcess {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val defaultConfigs = Map(
      "storage" -> "oss",
      "bucket" -> "vts-data",
      "backup_path" -> "newBackup/",
      "minio_endpoint" -> "https://oss-cn-shanghai.aliyuncs.com",
      "region" -> "cn-shanghai",
      "ak" -> "",
      "sk" -> "",
      "backup_collection" -> "test_col_lxelwhu",
      "backup_database" -> "test_db"
    )

    val backupDF = BackupFileUtils.processBackup(spark, defaultConfigs)
    log.info("Backup process completed.")

    import spark.implicits._
    val ossDF = Seq(
      (1, "Alice", 55, 58.10, "Shanghai"),
      (2, "David", 53, 80.03, "Shanghai"),
      (3, "David", 46, 79.44, "Shenzhen"),
      (4, "David", 41, 50.37, "Guangzhou"),
      (5, "Charlie", 35, 69.56, "Shanghai")
    ).toDF("id", "name", "age", "score", "city")

    val finalDF = backupDF.join(ossDF, Seq("id"), "left")

    // Save as Parquet
    val outputPath = s"./data/backup_output"

    // Save as Parquet to OSS
//    val ossOutputPath = s"oss://${defaultConfigs("bucket")}/backup_output"
//    log.info(s"Saving as Parquet to OSS: $ossOutputPath")

    log.info(s"Saving as Parquet: $outputPath")
    finalDF.coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(outputPath)
    log.info(s"Successfully saved as Parquet: $outputPath")
    finalDF.show(10) // Display the final aggregated DataFrame

  }
}