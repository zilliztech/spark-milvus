package com.zilliz.spark.connector.table

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.SparkTypes
import io.milvus.grpc.schema.CollectionSchema

/** Builds a table and its schema from the same validated options and snapshot
  * source for DataSource loads and schema inference.
  */
private[connector] object MilvusTables {

  def load(
      options: CaseInsensitiveStringMap,
      sparkSchema: Option[StructType]
  ): MilvusTable = {
    val milvusOption = validate(options)
    MilvusTable(resolve(milvusOption), milvusOption, sparkSchema)
  }

  /** The Spark schema derived from the resolved snapshot's collection schema.
    */
  def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = validate(options)
    val snapshot = resolve(milvusOption)
    sparkSchemaOf(
      snapshot.schema,
      MilvusOption.readVectorRaw(options)
    )
  }

  /** The snapshot this table is about, resolved once. A failure here is the
    * table's failure: there is no schema or plan without it.
    */
  private def resolve(milvusOption: MilvusOption): Snapshot =
    SnapshotSources
      .forRead(milvusOption)
      .snapshot()
      .fold(
        {
          case e: IllegalArgumentException => throw e
          case e =>
            throw new IllegalArgumentException(
              s"Cannot resolve the snapshot to read: ${e.getMessage}",
              e
            )
        },
        identity
      )

  private def validate(options: CaseInsensitiveStringMap): MilvusOption = {
    val milvusOption = MilvusOption(options)
    MilvusOption.validateSnapshotModeOptions(options)
    if (!MilvusOption.isSnapshotMode(options)) {
      throw new IllegalArgumentException(
        s"Public reads require '${MilvusOption.SnapshotPath}' or explicit V3 snapshot metadata"
      )
    }
    milvusOption
  }

  private[connector] def sparkSchemaOf(
      schema: CollectionSchema,
      rawVectors: Boolean
  ): StructType = {
    val fieldsById = schema.fields.groupBy(_.fieldID)
    val duplicateIds = fieldsById
      .collect {
        case (id, fields) if fields.size > 1 => id
      }
      .toSeq
      .sorted
    if (duplicateIds.nonEmpty) {
      throw new IllegalArgumentException(
        s"Snapshot schema contains duplicate field id(s): ${duplicateIds.mkString(", ")}"
      )
    }
    val fields = schema.fields.map(SparkTypes.toStructField(_, rawVectors))
    val clashes = fields
      .groupBy(_.name.toLowerCase(java.util.Locale.ROOT))
      .collect { case (_, same) if same.size > 1 => same.map(_.name) }
      .toSeq
    if (clashes.nonEmpty) {
      throw new IllegalArgumentException(
        s"Snapshot schema contains field names that collide under case-insensitive resolution: " +
          clashes.map(_.mkString(" and ")).mkString(", ")
      )
    }
    StructType(fields)
  }
}
