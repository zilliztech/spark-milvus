package com.zilliz.spark.connector.sources

import java.{util => ju}

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.spark.connector.options.{MilvusOption, SnapshotSources}
import com.zilliz.spark.connector.table.MilvusTable
import com.zilliz.spark.connector.types.SparkTypes
import io.milvus.grpc.schema.CollectionSchema

case class MilvusDataSource() extends TableProvider with DataSourceRegister {

  /** The snapshot this read is about, resolved once. A failure here is the
    * read's failure: there is no schema and no plan without it.
    */
  private def resolve(
      milvusOption: MilvusOption,
      withSegments: Boolean
  ): com.zilliz.milvus.storage.snapshot.Snapshot =
    SnapshotSources
      .forRead(milvusOption, withSegments)
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

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val milvusOption = validate(new CaseInsensitiveStringMap(properties))
    MilvusTable(
      resolve(milvusOption, withSegments = true),
      milvusOption,
      Some(schema)
    )
  }

  /** The Spark schema of the snapshot's collection schema, one column per
    * field. Segments are not materialized for this.
    */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = validate(options)
    val snapshot = resolve(milvusOption, withSegments = false)
    sparkSchemaOf(
      snapshot.schema,
      MilvusOption.readVectorRaw(options),
      MilvusOption.readerFieldIds(options)
    )
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
    StructType(selectedFields.map(SparkTypes.toStructField(_, rawVectors)))
  }

  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}
