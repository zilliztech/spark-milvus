package com.zilliz.spark.connector.sources

import java.{util => ju}

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.table.MilvusTables
import com.zilliz.spark.connector.write.{ColumnReplaceTables, WriteResultTable}

/** One provider for the three tables of `format("milvus")`: a fixed snapshot to
  * read, the column replacement a DataFrame writes when the options name
  * `milvus.write.operation`, and the receipt that write published, read with
  * `milvus.read.mode=write_result`.
  *
  * For a write Spark passes the DataFrame's schema to `getTable` and never asks
  * to infer one, because the provider accepts external metadata.
  */
case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    if (MilvusOption.writeOperation(options).nonEmpty)
      ColumnReplaceTables.load(options, Option(schema))
    else if (MilvusOption.isWriteResultRead(options))
      WriteResultTable.load(options, Option(schema))
    else MilvusTables.load(options, Option(schema))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    if (MilvusOption.writeOperation(options).nonEmpty)
      throw new IllegalArgumentException(
        s"'${MilvusOption.WriteOperation}' is a write option; remove it to read"
      )
    else if (MilvusOption.isWriteResultRead(options)) WriteResultTable.Schema
    else MilvusTables.inferSchema(options)

  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}
