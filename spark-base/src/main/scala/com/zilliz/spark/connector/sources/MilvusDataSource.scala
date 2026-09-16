package com.zilliz.spark.connector.sources

import java.{util => ju}

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.options.SnapshotReference
import com.zilliz.spark.connector.table.MilvusTables

case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table =
    MilvusTables.load(
      new CaseInsensitiveStringMap(properties),
      Some(schema),
      SnapshotReference.Configured
    )

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    MilvusTables.inferSchema(options)

  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}
