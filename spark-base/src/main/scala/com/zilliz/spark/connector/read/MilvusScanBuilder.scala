package com.zilliz.spark.connector.read

import java.{util => ju}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownFilters,
  SupportsPushDownLimit,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.options.MilvusOption

class MilvusScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    snapshot: Snapshot
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit
    with Logging {
  private var currentSchema = schema

  // Spark only offers a limit when nothing above the scan would change the
  // row set: every filter pushed or none present. We take it per partition and
  // report it as partial, so Spark keeps its own global Limit on top.
  private var pushedLimit: Option[Int] = None

  override def pushLimit(limit: Int): Boolean = {
    pushedLimit = Some(limit)
    true
  }

  override def isPartiallyPushed: Boolean = true
  private var currentOptions = options
  private val extraColumns = options
    .getOrDefault(MilvusOption.MilvusExtraColumns, "")
    .split(",")
    .map(_.trim)
    .filter(_.nonEmpty)
    .map(MilvusOption.normalizeExtraColumnName)
    .toSeq

  // Filters accepted by the connector. This remains empty until predicate
  // pushdown can preserve the complete Spark SQL semantics.
  private var pushedFilterArray: Array[Filter] = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (currentOptions.getOrDefault(MilvusOption.ReaderFieldIDs, "").nonEmpty) {
      return
    }
    val fieldName2ID = mutable.Map[String, Long]()
    schema.fields
      .filterNot(f => extraColumns.contains(f.name))
      .zipWithIndex
      .foreach { case (field, index) =>
        if (index < 2) {
          fieldName2ID(field.name) = index
        } else {
          fieldName2ID(field.name) = index + 98
        }
      }
    var fieldNames = Seq[String]()
    requiredSchema.fields.foreach(field => {
      if (fieldName2ID.contains(field.name)) {
        fieldNames = fieldNames :+ field.name
      }
    })

    // Add vector column if vector search is enabled
    val vectorColumn = Option(
      options.get(MilvusOption.VectorSearchVectorColumn)
    ).getOrElse("vector")
    val hasVectorSearch = Option(
      options.get(MilvusOption.VectorSearchQueryVector)
    ).isDefined
    if (
      hasVectorSearch && fieldName2ID.contains(vectorColumn) && !fieldNames
        .contains(vectorColumn)
    ) {
      fieldNames = fieldNames :+ vectorColumn
    }

    fieldNames = fieldNames.sortBy(fieldName => fieldName2ID(fieldName))
    logInfo(s"fieldNames after sort: $fieldNames")
    if (fieldNames.isEmpty && fieldName2ID.nonEmpty) {
      val fallbackFieldName = fieldName2ID.minBy(_._2)._1
      fieldNames = fieldNames :+ fallbackFieldName
      logInfo(s"fieldNames after add fallback field: $fieldNames")
    }

    val tmpMap = new ju.HashMap[String, String]()
    options.asScala.foreach { case (key, value) =>
      tmpMap.put(key, value)
    }
    // Only set ReaderFieldIDs if fieldNames is not empty
    if (fieldNames.nonEmpty) {
      val readerFieldIDsStr = fieldNames
        .map(fieldName => fieldName2ID(fieldName).toString)
        .mkString(",")
      tmpMap.put(
        MilvusOption.ReaderFieldIDs,
        readerFieldIDsStr
      )
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnPartition) &&
      !fieldNames.contains(MilvusOption.MilvusExtraColumnPartition)
    ) {
      fieldNames = fieldNames :+ MilvusOption.MilvusExtraColumnPartition
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnSegmentID) &&
      !fieldNames.contains(MilvusOption.MilvusExtraColumnSegmentID)
    ) {
      fieldNames = fieldNames :+ MilvusOption.MilvusExtraColumnSegmentID
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnRowOffset) &&
      !fieldNames.contains(MilvusOption.MilvusExtraColumnRowOffset)
    ) {
      fieldNames = fieldNames :+ MilvusOption.MilvusExtraColumnRowOffset
    }

    currentOptions = new CaseInsensitiveStringMap(tmpMap)
    currentSchema = StructType(
      fieldNames.map(fieldName =>
        schema.fields.find(field => field.name == fieldName).get
      )
    )
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Spark removes accepted filters from its plan, so accepting one is safe
    // only when the connector preserves the complete Spark SQL semantics. The
    // current legacy Filter evaluator does not, notably for null comparisons;
    // keep every filter in Spark until predicate pushdown is complete.
    pushedFilterArray = Array.empty
    filters
  }

  override def pushedFilters(): Array[Filter] = pushedFilterArray

  override def build(): Scan = {
    new MilvusScan(
      currentSchema,
      currentOptions,
      snapshot,
      pushedFilterArray,
      pushedLimit
    )
  }
}
