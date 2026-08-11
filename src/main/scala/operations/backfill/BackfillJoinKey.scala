package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.DataFrame

/** Public specification for the row identity used to match snapshot rows with
  * backfill input rows.
  */
sealed trait BackfillJoinKey extends Product with Serializable

object BackfillJoinKey {

  /** Preserve the historical behavior: resolve and join on the collection
    * primary key.
    */
  case object PrimaryKey extends BackfillJoinKey

  /** Join on an exact, persisted field name from the snapshot schema. */
  final case class PhysicalField(name: String) extends BackfillJoinKey
}

/** One source-side component of the key used to match collection rows with
  * backfill data.
  *
  * Current public strategies resolve exactly one persisted field. Keeping the
  * runtime representation component-based lets the execution pipeline support a
  * future composite or logical key without changing its read, validation, or
  * join contracts again.
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

  /** Assign deterministic private aliases that do not collide with user or
    * collection columns under Spark's configured name resolver.
    */
  def withCollisionFreeInternalColumns(
      occupiedNames: Seq[String],
      resolver: (String, String) => Boolean
  ): ResolvedJoinKey = {
    var allocatedNames = occupiedNames
    val allocatedComponents = components.zipWithIndex.map {
      case (component, index) =>
        var collisionOrdinal = 0
        var candidate =
          ResolvedJoinKey.internalColumn(index, collisionOrdinal)
        while (allocatedNames.exists(resolver(_, candidate))) {
          collisionOrdinal += 1
          candidate = ResolvedJoinKey.internalColumn(index, collisionOrdinal)
        }
        allocatedNames = allocatedNames :+ candidate
        component.copy(internalColumn = candidate)
    }
    copy(components = allocatedComponents)
  }
}

private[backfill] object ResolvedJoinKey {
  private val InternalColumnPrefix = "__bf_join_"

  def internalColumn(index: Int, collisionOrdinal: Int = 0): String =
    if (collisionOrdinal == 0) s"$InternalColumnPrefix${index}__"
    else s"$InternalColumnPrefix${index}_${collisionOrdinal}__"

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

  def physicalField(
      name: String,
      fieldId: Long,
      sourceField: StructField
  ): ResolvedJoinKey =
    ResolvedJoinKey(
      kind = "physical",
      components = Seq(
        ResolvedJoinComponent(
          sourceColumn = name,
          fieldId = fieldId,
          sourceField = Some(sourceField),
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
    joinKey: ResolvedJoinKey,
    targetFieldNames: Seq[String]
) {
  val joinColumns: Seq[String] = joinKey.internalColumns
}

private[backfill] final case class JoinKeyStats(
    rowCount: Long,
    nullKeyRowCount: Long,
    distinctValidKeyCount: Long
)

private[backfill] final case class SourceReadProjection(
    fieldIds: Seq[Long],
    schema: StructType
)
