package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.DataFrame

/** One source-side component of the key used to match collection rows with
  * backfill data.
  *
  * The current public behavior resolves exactly one component: the collection
  * primary key. Keeping the runtime representation component-based lets the
  * execution pipeline support a future composite or logical key without
  * changing its read, validation, or join contracts again.
  */
private[backfill] final case class ResolvedJoinComponent(
    sourceColumn: String,
    fieldId: Long,
    sourceField: Option[StructField],
    internalColumn: String
)

private[backfill] final case class ResolvedJoinKey(
    kind: String,
    components: Seq[ResolvedJoinComponent]
) {
  require(components.nonEmpty, "resolved join key must not be empty")

  val sourceColumns: Seq[String] = components.map(_.sourceColumn)
  val internalColumns: Seq[String] = components.map(_.internalColumn)
  val fieldIds: Seq[Long] = components.map(_.fieldId)
}

private[backfill] object ResolvedJoinKey {
  private val InternalColumnPrefix = "__bf_join_"

  def internalColumn(index: Int): String =
    s"$InternalColumnPrefix${index}__"

  def primaryKey(
      name: String,
      fieldId: Long,
      sourceField: Option[StructField]
  ): ResolvedJoinKey =
    ResolvedJoinKey(
      kind = "primary_key",
      components = Seq(
        ResolvedJoinComponent(
          sourceColumn = name,
          fieldId = fieldId,
          sourceField = sourceField,
          internalColumn = internalColumn(0)
        )
      )
    )
}

/** Backfill input after column mapping has separated identity columns from
  * fields that will be written.
  */
private[backfill] final case class PreparedBackfillData(
    dataFrame: DataFrame,
    joinColumns: Seq[String],
    targetFieldNames: Seq[String]
)

private[backfill] final case class JoinKeyStats(
    rowCount: Long,
    nullKeyRowCount: Long,
    distinctValidKeyCount: Long
)

private[backfill] final case class SourceReadProjection(
    fieldIds: Seq[Long],
    schema: StructType
)
