package com.zilliz.spark.connector.catalog

import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableInfo}

/** Public Spark 4.1 catalog entry. */
final class MilvusCatalog extends MilvusCatalogBase {
  override def createTable(
      identifier: Identifier,
      tableInfo: TableInfo
  ): Table = unsupportedCreate()
}
