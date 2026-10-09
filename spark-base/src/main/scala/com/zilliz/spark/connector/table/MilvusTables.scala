package com.zilliz.spark.connector.table

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SnapshotReference,
  SnapshotSources
}
import com.zilliz.spark.connector.types.SparkTypes
import io.milvus.grpc.schema.CollectionSchema

/** Builds a table and its schema from the same validated options and snapshot
  * source. Both the DataSource and Catalog entries use this object so snapshot
  * resolution and schema rules cannot diverge.
  */
private[connector] object MilvusTables {

  def load(
      options: CaseInsensitiveStringMap,
      sparkSchema: Option[StructType],
      snapshotReference: SnapshotReference
  ): MilvusTable = {
    val milvusOption = validate(options)
    MilvusTable(
      resolve(
        milvusOption,
        withSegments = true,
        snapshotReference = snapshotReference
      ),
      milvusOption,
      sparkSchema
    )
  }

  /** The Spark schema of the snapshot's collection schema, one column per
    * field. Segments are not materialized for this.
    */
  def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = validate(options)
    val snapshot = resolve(
      milvusOption,
      withSegments = false,
      snapshotReference = SnapshotReference.Configured
    )
    sparkSchemaOf(
      snapshot.schema,
      MilvusOption.readVectorRaw(options),
      MilvusOption.readerFieldIds(options)
    )
  }

  /** The snapshot this table is about, resolved once. A failure here is the
    * table's failure: there is no schema or plan without it.
    */
  private def resolve(
      milvusOption: MilvusOption,
      withSegments: Boolean,
      snapshotReference: SnapshotReference
  ): Snapshot =
    SnapshotSources
      .forRead(milvusOption, withSegments, snapshotReference)
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
    MilvusOption.validateBackupModeOptions(options)
    if (
      milvusOption.uri.isEmpty && !MilvusOption.isSnapshotMode(options) &&
      !MilvusOption.isBackupMode(options)
    ) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusUri}' is required for reading milvus data."
      )
    }
    milvusOption
  }

  private def sparkSchemaOf(
      schema: CollectionSchema,
      rawVectors: Boolean,
      selectedFieldIds: Seq[Long]
  ): StructType = {
    val allFields = SchemaMapper.missingSystemFields(schema) ++ schema.fields
    val fieldsById = allFields.groupBy(_.fieldID)
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
    val missingIds = selectedFieldIds.filterNot(fieldsById.contains)
    if (missingIds.nonEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.ReaderFieldIDs}' requests unknown field id(s) ${missingIds
            .mkString(", ")}; " +
          s"snapshot field ids are ${fieldsById.keys.toSeq.sorted.mkString(", ")}"
      )
    }
    val selectedFields =
      if (selectedFieldIds.isEmpty) schema.fields
      else selectedFieldIds.map(id => fieldsById(id).head)
    val fields = selectedFields.map(SparkTypes.toStructField(_, rawVectors))
    rejectCaseInsensitiveDuplicates(fields)
    StructType(fields)
  }

  /** Selecting system field 1 ("Timestamp") next to a user field named
    * "timestamp" gives two columns Spark cannot tell apart, since it resolves
    * names case-insensitively. That is refused with the alternative named.
    */
  private[connector] def rejectCaseInsensitiveDuplicates(
      fields: Seq[StructField]
  ): Unit = {
    val clashes = fields
      .groupBy(_.name.toLowerCase(java.util.Locale.ROOT))
      .collect { case (_, same) if same.size > 1 => same.map(_.name) }
      .toSeq
    if (clashes.nonEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.ReaderFieldIDs}' selects fields whose names differ only in case: " +
          s"${clashes.map(_.mkString(" and ")).mkString("; ")}; " +
          s"read the Milvus timestamp through '${MilvusOption.MilvusExtraColumns}=${MilvusOption.MilvusExtraColumnTimestamp}' instead"
      )
    }
  }
}
