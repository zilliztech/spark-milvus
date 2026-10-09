package com.zilliz.spark.connector.catalog

import java.{util => ju}
import scala.jdk.CollectionConverters._

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
  ): Table =
    createTable(
      MilvusCatalogCreate(
        identifier,
        schema.fields.toSeq.map { field =>
          MilvusCatalogColumn(
            field.name,
            field.dataType,
            field.nullable,
            field.getComment,
            field.getCurrentDefaultValue.nonEmpty
          )
        },
        Option(partitions).exists(_.nonEmpty),
        hasConstraints = false,
        Option(properties).map(_.asScala.toMap).getOrElse(Map.empty)
      )
    )
}
