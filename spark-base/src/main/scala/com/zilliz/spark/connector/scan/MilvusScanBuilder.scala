package com.zilliz.spark.connector.scan

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
import org.apache.spark.sql.types.{
  DataTypes => SparkDataTypes,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.compat.backup.BackupMetaReader
import com.zilliz.spark.connector.options.MilvusOption

class MilvusScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    preParsedBackupMeta: Option[BackupMetaReader.BackupInfo] = None
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

  // Store the filters that can be pushed down
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

    // Add fields referenced in pushed filters to ensure they are not pruned
    pushedFilterArray.foreach { filter =>
      val filterColumns = extractFilterColumns(filter)
      filterColumns.foreach { colName =>
        if (fieldName2ID.contains(colName) && !fieldNames.contains(colName)) {
          fieldNames = fieldNames :+ colName
        }
      }
    }

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
    // V2 packed reader does not apply filters server-side yet — return all
    // as unsupported so Spark applies them post-read.
    // TODO: implement filter pushdown for V2 packed reader in a separate PR.
    val isPackedV2 = Option(options.get(MilvusOption.SnapshotV2Segments))
      .exists(_.nonEmpty)
    val isBackupMode = MilvusOption.isBackupMode(options)
    if (isPackedV2 || isBackupMode) {
      pushedFilterArray = Array.empty
      return filters
    }
    val (supportedFilters, unsupportedFilters) =
      filters.partition(isSupportedFilter)
    pushedFilterArray = supportedFilters
    unsupportedFilters
  }

  override def pushedFilters(): Array[Filter] = pushedFilterArray

  private def isSupportedFilter(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    filter match {
      // Support equality filters on string and numeric columns
      case EqualTo(attr, _) => isStringOrNumericColumn(attr)
      // Support numeric comparison filters only on numeric columns
      case GreaterThan(attr, _)        => isNumericColumn(attr)
      case GreaterThanOrEqual(attr, _) => isNumericColumn(attr)
      case LessThan(attr, _)           => isNumericColumn(attr)
      case LessThanOrEqual(attr, _)    => isNumericColumn(attr)
      // Support IN filters on string and numeric columns
      case In(attr, _)     => isStringOrNumericColumn(attr)
      case IsNull(attr)    => isStringOrNumericColumn(attr)
      case IsNotNull(attr) => isStringOrNumericColumn(attr)
      // Support AND combinations of supported filters
      case And(left, right) =>
        isSupportedFilter(left) && isSupportedFilter(right)
      // Support OR combinations of supported filters
      case Or(left, right) =>
        isSupportedFilter(left) && isSupportedFilter(right)
      case _ => false
    }
  }

  private def isStringOrNumericColumn(columnName: String): Boolean = {
    schema.fields.find(_.name == columnName) match {
      case Some(field) =>
        field.dataType match {
          case StringType | LongType | SparkDataTypes.IntegerType |
              SparkDataTypes.DoubleType | SparkDataTypes.FloatType |
              SparkDataTypes.BooleanType =>
            true
          case _ => false
        }
      case None => false
    }
  }

  private def isNumericColumn(columnName: String): Boolean = {
    schema.fields.find(_.name == columnName) match {
      case Some(field) =>
        field.dataType match {
          case LongType | SparkDataTypes.IntegerType |
              SparkDataTypes.DoubleType | SparkDataTypes.FloatType =>
            true
          case _ => false
        }
      case None => false
    }
  }

  /** Extract all column names referenced in a filter
    */
  private def extractFilterColumns(filter: Filter): Seq[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attr, _)            => Seq(attr)
      case GreaterThan(attr, _)        => Seq(attr)
      case GreaterThanOrEqual(attr, _) => Seq(attr)
      case LessThan(attr, _)           => Seq(attr)
      case LessThanOrEqual(attr, _)    => Seq(attr)
      case In(attr, _)                 => Seq(attr)
      case IsNull(attr)                => Seq(attr)
      case IsNotNull(attr)             => Seq(attr)
      case And(left, right) =>
        extractFilterColumns(left) ++ extractFilterColumns(right)
      case Or(left, right) =>
        extractFilterColumns(left) ++ extractFilterColumns(right)
      case _ => Seq.empty
    }
  }

  override def build(): Scan = {
    new MilvusScan(
      currentSchema,
      currentOptions,
      pushedFilterArray,
      preParsedBackupMeta,
      pushedLimit
    )
  }
}
