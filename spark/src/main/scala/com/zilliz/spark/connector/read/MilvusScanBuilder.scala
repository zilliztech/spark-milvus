package com.zilliz.spark.connector.read

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownLimit,
  SupportsPushDownRequiredColumns,
  SupportsPushDownV2Filters
}
import org.apache.spark.sql.types.{LongType, NullType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.{And, PredicateExpr}
import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.expr.SparkPredicateTranslator
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.RowIdentity

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
  private val availableSchema = StructType(
    schema.fields.toSeq ++ MetadataColumns.hiddenFields(
      schema,
      snapshot.schema,
      physicalRead = MilvusOption.isPhysicalRead(options)
    )
  )
  private val fieldsByName =
    availableSchema.fields.map(field => field.name -> field).toMap
  private val hasPartitionMetadata =
    fieldsByName
      .get(MetadataColumns.PartitionId)
      .exists(MetadataColumns.isSynthetic)

  private var pushedPredicateArray: Array[Predicate] = Array.empty[Predicate]
  private var pushedExpression: Option[PredicateExpr] = None
  private var predicateFieldIds: Set[Long] = Set.empty
  private var partitionIds: Option[Set[Long]] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requestedFields = requiredSchema.fields.map { field =>
      fieldsByName.getOrElse(
        field.name,
        throw new IllegalArgumentException(
          s"Spark requested unknown field '${field.name}'; available fields: ${fieldsByName.keys.toSeq.sorted
              .mkString(", ")}"
        )
      )
    }

    if (snapshot.id.isEmpty && requestedFields.contains(RowIdentity.field)) {
      throw new IllegalArgumentException(
        s"Snapshot '${snapshot.name}' has no snapshot_info.id, so its rows cannot carry " +
          s"'${RowIdentity.ColumnName}'. Read a snapshot file that Milvus created."
      )
    }
    currentSchema = StructType(requestedFields)
  }

  private def fieldId(field: StructField): Long = {
    require(
      field.metadata.contains(FieldMetadata.MilvusFieldIdMetadataKey),
      s"Field '${field.name}' has no ${FieldMetadata.MilvusFieldIdMetadataKey} metadata; " +
        "the scan cannot bind it to the fixed snapshot"
    )
    field.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
  }

  override def pushPredicates(
      predicates: Array[Predicate]
  ): Array[Predicate] = {
    val accepted = Array.newBuilder[Predicate]
    val expressions = Seq.newBuilder[SparkPredicateTranslator.Translated]
    val partitionFilters = Seq.newBuilder[Set[Long]]
    val residual = Array.newBuilder[Predicate]
    predicates.foreach { predicate =>
      matchingPartitionIds(predicate) match {
        case Some(ids) =>
          accepted += predicate
          partitionFilters += ids
        case None =>
          SparkPredicateTranslator.translate(predicate, availableSchema) match {
            case Some(translated) =>
              accepted += predicate
              expressions += translated
            case None => residual += predicate
          }
      }
    }
    val translated = expressions.result()
    pushedPredicateArray = accepted.result()
    partitionIds = partitionFilters.result().reduceLeftOption(_ intersect _)
    pushedExpression = translated.iterator
      .map(_.expr)
      .reduceLeftOption[PredicateExpr](And.apply)
    predicateFieldIds = translated.iterator.flatMap(_.fieldIds).toSet
    residual.result()
  }

  override def pushedPredicates(): Array[Predicate] =
    pushedPredicateArray.clone()

  /** Exact partition matches for a top-level equality or IN. A null literal
    * cannot make either predicate true on a non-null partition ID.
    */
  private def matchingPartitionIds(predicate: Predicate): Option[Set[Long]] = {
    if (!hasPartitionMetadata || predicate == null) return None
    try {
      val operator = predicate.name()
      if (operator != "=" && operator != "IN") return None
      predicate.children() match {
        case Array(ref: NamedReference, values @ _*)
            if ref
              .fieldNames()
              .sameElements(Array(MetadataColumns.PartitionId)) &&
              (operator == "IN" || values.size == 1) =>
          val ids = Set.newBuilder[Long]
          values.foreach {
            case value: Literal[_] =>
              (value.dataType(), value.value()) match {
                case (LongType, id: Long)        => ids += id
                case (LongType | NullType, null) => // NULL cannot match.
                case _                           => return None
              }
            case _ => return None
          }
          Some(ids.result())
        case _ => None
      }
    } catch {
      // As in SparkPredicateTranslator, malformed V2 extension nodes are residuals.
      case NonFatal(_) => None
    }
  }

  /** Projection and filter callbacks can arrive in either order. An empty or
    * position-only output still needs a physical column for batch counts.
    */
  private def neededFieldIds: Seq[Long] = {
    val projected =
      currentSchema.fields.filterNot(MetadataColumns.isSynthetic).map(fieldId)
    val selected = (projected.toSeq ++ predicateFieldIds.toSeq.sorted).distinct
    if (selected.nonEmpty) selected
    else {
      // Count rows using the declared timestamp or primary key, even when the
      // visible Spark schema contains only vectors. Both also serve live deletes.
      val fallback = snapshot.schema.fields
        .find(_.fieldID == SchemaMapper.TimestampFieldId)
        .orElse(snapshot.primaryKeyField)
        .orElse(snapshot.schema.fields.sortBy(_.fieldID).headOption)
        .fold(SchemaMapper.TimestampFieldId)(_.fieldID)
      Seq(fallback)
    }
  }

  override def build(): Scan = {
    val selectedSnapshot = partitionIds.fold(snapshot) { ids =>
      val segments =
        snapshot.dataSegments.iterator.filter(s => ids(s.partitionId))
      snapshot.retainDataSegments(segments.map(_.id).toSet)
    }
    val readIds = neededFieldIds
    logInfo(s"Milvus field ids required by the scan: $readIds")
    new MilvusScan(
      currentSchema,
      options,
      selectedSnapshot,
      pushedExpression,
      pushedLimit,
      planningSchema = availableSchema,
      neededFieldIds = readIds
    )
  }
}
