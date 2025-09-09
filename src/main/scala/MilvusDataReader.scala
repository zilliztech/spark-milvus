package com.zilliz.spark.connector

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MilvusDataReader {
  def read(
      spark: SparkSession,
      config: MilvusDataReaderConfig
  ): DataFrame = {

    val fileSystemType = config.options.getOrElse(
      MilvusOption.S3FileSystemTypeName,
      "s3a://"
    )

    val insertDF = spark.read
      .format("milvus")
      .options(config.options)
      .option(
        MilvusOption.S3FileSystemTypeName,
        fileSystemType
      )
      .option(MilvusOption.ReaderType, "insert")
      .option(MilvusOption.MilvusUri, config.uri)
      .option(MilvusOption.MilvusToken, config.token)
      .option(MilvusOption.MilvusCollectionName, config.collectionName)
      .load()

    val deleteDF = spark.read
      .format("milvusbinlog")
      .options(config.options)
      .option(
        MilvusOption.S3FileSystemTypeName,
        fileSystemType
      )
      .option(MilvusOption.ReaderType, "delete")
      .option(MilvusOption.MilvusUri, config.uri)
      .option(MilvusOption.MilvusToken, config.token)
      .option(MilvusOption.MilvusCollectionName, config.collectionName)
      .load()

    val insertPkColName = insertDF.schema.fields(2).name
    val deletePKColName = "data"
    val timestampColName = "timestamp"

    // only keep the latest delete record for each pk
    val windowSpecDelete =
      Window
        .partitionBy(col(deletePKColName))
        .orderBy(col(timestampColName).desc)
    val deleteDFUniqueWindow = deleteDF
      .withColumn("rn", row_number().over(windowSpecDelete))
      .filter(col("rn") === 1)
      .drop("rn")

    val deleteDFRenamedWindow = deleteDFUniqueWindow
      .withColumnRenamed(deletePKColName, "delete_pk")
      .withColumnRenamed(timestampColName, "delete_ts")

    val finalInsertDFWindow = insertDF.join(
      deleteDFRenamedWindow,
      (col(insertPkColName) === col("delete_pk")) && (col("delete_ts") > col(
        timestampColName
      )),
      "left_anti"
    )

    val columnsToDrop = Seq("row_id", "timestamp")
    finalInsertDFWindow.drop(columnsToDrop: _*)
  }
}

case class MilvusDataReaderConfig(
    uri: String,
    token: String,
    collectionName: String,
    options: Map[String, String] = Map.empty
)
