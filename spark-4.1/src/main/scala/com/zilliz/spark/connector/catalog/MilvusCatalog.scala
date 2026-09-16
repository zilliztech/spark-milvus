package com.zilliz.spark.connector.catalog

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableInfo}

/** Public Spark 4.1 catalog entry. */
final class MilvusCatalog extends MilvusCatalogBase {
  override def createTable(
      identifier: Identifier,
      tableInfo: TableInfo
  ): Table =
    createTable(
      MilvusCatalogCreate(
        identifier,
        Option(tableInfo.columns()).getOrElse(Array.empty).toSeq.map { column =>
          MilvusCatalogColumn(
            column.name(),
            column.dataType(),
            column.nullable(),
            Option(column.comment()),
            column.defaultValue() != null,
            Option(column.generationExpression()),
            column.identityColumnSpec() != null
          )
        },
        Option(tableInfo.partitions()).exists(_.nonEmpty),
        Option(tableInfo.constraints()).exists(_.nonEmpty),
        Option(tableInfo.properties())
          .map(_.asScala.toMap)
          .getOrElse(Map.empty)
      )
    )
}
