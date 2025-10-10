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

    // Read insert data using unified milvus format
    // The milvus format now automatically detects and handles both V1 and V2 segments
    val insertDF = spark.read
      .format("milvus")
      .options(config.options)
      .option(MilvusOption.S3FileSystemTypeName, fileSystemType)
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

    // Check if there are any delete records
    if (deleteDF.isEmpty) {
      println(s"No delete records found, returning insert data as-is")
      // Drop system columns if they exist
      val columnsToDropIfExist = Seq("row_id", "timestamp")
      val existingColumns = insertDF.schema.fieldNames.toSet
      val actualColumnsToDrop = columnsToDropIfExist.filter(existingColumns.contains)
      if (actualColumnsToDrop.nonEmpty) {
        insertDF.drop(actualColumnsToDrop: _*)
      } else {
        insertDF
      }
    } else {
      // Determine primary key column name
      // For V1 format, row_id and timestamp columns exist at positions 0 and 1
      // For V2 format, there are no system columns
      val hasSystemColumns = insertDF.schema.fieldNames.contains("row_id") ||
                             insertDF.schema.fieldNames.contains("timestamp")

      val insertPkColName = if (hasSystemColumns) {
        // V1 format: primary key is at index 2 (after row_id and timestamp)
        insertDF.schema.fields(2).name
      } else {
        // V2 format: primary key is the first field
        insertDF.schema.fields(0).name
      }

      val deletePKColName = "data"
      val deleteTsColName = "timestamp"

      // only keep the latest delete record for each pk
      val windowSpecDelete =
        Window
          .partitionBy(col(deletePKColName))
          .orderBy(col(deleteTsColName).desc)
      val deleteDFUniqueWindow = deleteDF
        .withColumn("rn", row_number().over(windowSpecDelete))
        .filter(col("rn") === 1)
        .drop("rn")

      val deleteDFRenamedWindow = deleteDFUniqueWindow
        .withColumnRenamed(deletePKColName, "delete_pk")
        .withColumnRenamed(deleteTsColName, "delete_ts")

      // For V2 format, we can't compare timestamps (insert DF doesn't have timestamp)
      // So we just filter out all records that have been deleted
      val finalInsertDFWindow = if (hasSystemColumns) {
        // V1 format: can use timestamp for comparison
        insertDF.join(
          deleteDFRenamedWindow,
          (col(insertPkColName) === col("delete_pk")) &&
          (col("delete_ts") > col("timestamp")),
          "left_anti"
        )
      } else {
        // V2 format: just filter by primary key (no timestamp available)
        insertDF.join(
          deleteDFRenamedWindow.select("delete_pk"),
          col(insertPkColName) === col("delete_pk"),
          "left_anti"
        )
      }

      // Drop system columns if they exist
      val columnsToDropIfExist = Seq("row_id", "timestamp")
      val existingColumns = finalInsertDFWindow.schema.fieldNames.toSet
      val actualColumnsToDrop = columnsToDropIfExist.filter(existingColumns.contains)
      if (actualColumnsToDrop.nonEmpty) {
        finalInsertDFWindow.drop(actualColumnsToDrop: _*)
      } else {
        finalInsertDFWindow
      }
    }
  }
}

case class MilvusDataReaderConfig(
    uri: String,
    token: String,
    collectionName: String,
    options: Map[String, String] = Map.empty
)
