package com.zilliz.spark.connector.catalog

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{Column, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform

/** Public Spark 4.0 catalog entry. */
final class MilvusCatalog extends MilvusCatalogBase {
  override def createTable(
      identifier: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: ju.Map[String, String]
  ): Table =
    createTable(
      MilvusCatalogCreate(
        identifier,
        Option(columns).getOrElse(Array.empty).toSeq.map { column =>
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
        Option(partitions).exists(_.nonEmpty),
        hasConstraints = false,
        Option(properties).map(_.asScala.toMap).getOrElse(Map.empty)
      )
    )
}
