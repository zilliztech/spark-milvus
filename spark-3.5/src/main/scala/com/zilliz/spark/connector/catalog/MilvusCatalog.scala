package com.zilliz.spark.connector.catalog

import java.{util => ju}

import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/** Public Spark 3.5 catalog entry. */
final class MilvusCatalog extends MilvusCatalogBase {
  override def createTable(
      identifier: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = unsupportedCreate()
}
