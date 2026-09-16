package com.zilliz.spark.connector.catalog

import java.{util => ju}

import org.apache.spark.sql.connector.catalog.{Column, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform

/** Public Spark 4.0 catalog entry. */
final class MilvusCatalog extends MilvusCatalogBase {
  override def createTable(
      identifier: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = unsupportedCreate()
}
