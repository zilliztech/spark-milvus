package com.zilliz.spark.connector.read

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownLimit,
  SupportsPushDownRequiredColumns,
  SupportsPushDownV2Filters
}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.{And, Evaluator, Expr, PredicateExpr}
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.expr.SparkPredicateTranslator
import com.zilliz.spark.connector.options.MilvusOption

class MilvusScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    snapshot: Snapshot
) extends ScanBuilder
    with SupportsPushDownV2Filters
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
  private val milvusOption = MilvusOption(options)
  private val extraColumns = milvusOption.extraColumns
  private val milvusFilter: Option[Expr] = milvusOption.milvusFilter
  private val milvusFilterFieldIds: Set[Long] = milvusFilter
    .map { expression =>
      try Evaluator.validate(expression, snapshot.schema.fields)
      catch {
        case error: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Option '${MilvusOption.MilvusFilter}' is invalid: ${error.getMessage}",
            error
          )
      }
      val fieldsByName = snapshot.schema.fields
        .map(field => field.name -> field)
        .toMap
      expression.fields.map(name => fieldsByName(name).fieldID)
    }
    .getOrElse(Set.empty)
  private val configuredReaderFieldIds =
    MilvusOption.readerFieldIds(options)
  private val scanFieldIds = schema.fields.iterator
    .filter(_.metadata.contains(FieldMetadata.MilvusFieldIdMetadataKey))
    .map(_.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey))
    .toSeq
  private val filterNeedsHiddenFields =
    !milvusFilterFieldIds.subsetOf(scanFieldIds.toSet)

  private var pushedPredicateArray: Array[Predicate] = Array.empty[Predicate]
  private var pushedExpression: Option[PredicateExpr] = None
  private var predicateFieldIds: Set[Long] = Set.empty

  // None means Spark did not prune the scan, so the native reader keeps its
  // existing all-fields behavior. Some(empty) is a real empty projection and
  // still needs either a predicate field or one fallback physical field to
  // drive the Arrow batches and their row counts.
  private var projectedFieldIds: Option[Seq[Long]] = None
  private var emptyProjectionFallbackId: Option[Long] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val fieldsByName = schema.fields.map(field => field.name -> field).toMap
    val requestedFields = requiredSchema.fields.map { field =>
      fieldsByName.getOrElse(
        field.name,
        throw new IllegalArgumentException(
          s"Spark requested unknown field '${field.name}'; available fields: ${fieldsByName.keys.toSeq.sorted
              .mkString(", ")}"
        )
      )
    }

    def fieldId(field: StructField): Long = {
      val metadata = field.metadata
      if (
        !metadata.contains(
          FieldMetadata.MilvusFieldIdMetadataKey
        )
      ) {
        throw new IllegalArgumentException(
          s"Field '${field.name}' has no ${FieldMetadata.MilvusFieldIdMetadataKey} metadata; " +
            "the scan cannot bind it to the fixed snapshot"
        )
      }
      metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
    }

    val requestedDataFields = requestedFields.filterNot(field =>
      extraColumns.contains(field.name) &&
        MetadataColumns.isSyntheticColumn(field.name)
    )
    val scanFields = requestedFields.toSeq
    val neededFieldIds = requestedDataFields.map(fieldId).toSeq

    // A metadata-only or empty projection still needs one physical column so
    // the native reader can produce batches and their row counts. This field
    // is not added to Spark's output schema.
    if (neededFieldIds.isEmpty) {
      val fallbackId = schema.fields
        .filterNot(field =>
          extraColumns.contains(field.name) &&
            MetadataColumns.isSyntheticColumn(field.name)
        )
        .map(fieldId)
        .sorted
        .headOption
        .getOrElse(
          throw new IllegalArgumentException(
            "The fixed snapshot schema has no physical field to drive an empty or metadata-only projection"
          )
        )
      emptyProjectionFallbackId = Some(fallbackId)
    } else {
      emptyProjectionFallbackId = None
    }
    projectedFieldIds = Some(neededFieldIds.distinct)
    currentSchema = StructType(scanFields)
    refreshReaderFieldIds()
  }

  override def pushPredicates(
      predicates: Array[Predicate]
  ): Array[Predicate] = {
    val accepted =
      Array.newBuilder[(Predicate, SparkPredicateTranslator.Translated)]
    val residual = Array.newBuilder[Predicate]
    predicates.foreach { predicate =>
      SparkPredicateTranslator.translate(predicate, schema) match {
        case Some(translated) => accepted += predicate -> translated
        case None             => residual += predicate
      }
    }
    val translated = accepted.result()
    pushedPredicateArray = translated.map(_._1)
    pushedExpression = translated.iterator
      .map(_._2.expr)
      .reduceLeftOption[PredicateExpr](And.apply)
    predicateFieldIds = translated.iterator.flatMap(_._2.fieldIds).toSet
    refreshReaderFieldIds()
    residual.result()
  }

  override def pushedPredicates(): Array[Predicate] =
    pushedPredicateArray.clone()

  /** Recompute the native projection from the two independently mutable Spark
    * callbacks. Spark normally pushes predicates before pruning columns, but
    * the DataSource contract does not make correctness depend on that order.
    */
  private def refreshReaderFieldIds(): Unit = {
    val baseFieldIds = projectedFieldIds
      .orElse(
        if (configuredReaderFieldIds.nonEmpty) Some(configuredReaderFieldIds)
        else None
      )
      .orElse(
        if (filterNeedsHiddenFields) Some(scanFieldIds)
        else None
      )
    baseFieldIds.foreach { projected =>
      val selected = (projected ++ predicateFieldIds.toSeq.sorted ++
        milvusFilterFieldIds.toSeq.sorted).distinct
      val neededFieldIds =
        if (selected.nonEmpty) selected
        else emptyProjectionFallbackId.toSeq
      logInfo(s"Milvus field ids required by the scan: $neededFieldIds")
      val tmpMap = new ju.HashMap[String, String]()
      options.asScala.foreach { case (key, value) =>
        if (!key.equalsIgnoreCase(MilvusOption.ReaderFieldIDs)) {
          tmpMap.put(key, value)
        }
      }
      tmpMap.put(
        MilvusOption.ReaderFieldIDs,
        neededFieldIds.mkString(",")
      )
      currentOptions = new CaseInsensitiveStringMap(tmpMap)
    }
  }

  override def build(): Scan = {
    refreshReaderFieldIds()
    new MilvusScan(
      currentSchema,
      currentOptions,
      snapshot,
      pushedExpression,
      pushedLimit,
      planningSchema = schema
    )
  }
}
