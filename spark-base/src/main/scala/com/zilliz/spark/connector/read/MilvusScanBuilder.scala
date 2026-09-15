package com.zilliz.spark.connector.read

import java.{util => ju}
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
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.schema.FieldMetadata
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
  private val extraColumns = MilvusOption.extraColumns(options)

  // Filters accepted by the connector. This remains empty until predicate
  // pushdown can preserve the complete Spark SQL semantics.
  private var pushedFilterArray: Array[Filter] = Array.empty[Filter]

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
    var scanFields = requestedFields.toSeq
    var neededFieldIds = requestedDataFields.map(fieldId).toSeq

    // Add vector column if vector search is enabled
    val vectorColumn = Option(
      options.get(MilvusOption.VectorSearchVectorColumn)
    ).map(_.trim).filter(_.nonEmpty).getOrElse("vector")
    val hasVectorSearch =
      Option(options.get(MilvusOption.VectorSearchQueryVector))
        .exists(_.trim.nonEmpty)
    if (hasVectorSearch) {
      val vectorField = fieldsByName.getOrElse(
        vectorColumn,
        throw new IllegalArgumentException(
          s"Vector search column '$vectorColumn' is not present in the read schema"
        )
      )
      neededFieldIds = neededFieldIds :+ fieldId(vectorField)
      if (!scanFields.exists(_.name == vectorField.name)) {
        // The row reader performs vector search before Spark's projection
        // above the scan. It therefore needs the vector in its own schema even
        // when the final select does not expose it.
        scanFields = scanFields :+ vectorField
      }
    }

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
      neededFieldIds = Seq(fallbackId)
    }
    neededFieldIds = neededFieldIds.distinct
    logInfo(s"Milvus field ids required by the scan: $neededFieldIds")

    val tmpMap = new ju.HashMap[String, String]()
    options.asScala.foreach { case (key, value) =>
      tmpMap.put(key, value)
    }
    tmpMap.put(
      MilvusOption.ReaderFieldIDs,
      neededFieldIds.mkString(",")
    )

    currentOptions = new CaseInsensitiveStringMap(tmpMap)
    currentSchema = StructType(scanFields)
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
