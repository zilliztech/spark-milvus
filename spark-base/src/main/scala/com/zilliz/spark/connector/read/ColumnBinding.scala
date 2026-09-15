package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.exec.{
  SegmentReader,
  SegmentReaderRegistry
}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import io.milvus.grpc.schema.{CollectionSchema, FieldSchema}

/** Everything a reader has to work out before it can open a segment: which
  * columns to ask the native layer for, what to call them, and how to tell a
  * deleted row.
  *
  * It exists because both the row reader and the columnar reader need exactly
  * this, and the two storage lines answer it differently. Leaving it inside
  * each row reader meant the columnar path would have had to derive it a third
  * and fourth time, and the four copies would drift.
  *
  * What differs between the lines is only the naming. A manifest matches
  * columns by field id, so its Arrow schema carries ids as names and the
  * primary key column is called "100"; a column-group layout uses the field's
  * own name. Everything after that, including telling a deleted row, is the
  * same and lives here.
  */
sealed trait ColumnBinding {

  def task: SegmentReadTask

  /** The schema handed to the native reader, named the way this line names
    * columns.
    */
  def arrowSchema: Schema

  /** The columns to read, already widened to include the primary key and the
    * timestamp when deletes have to be evaluated.
    */
  def neededColumns: Seq[String]

  /** A field id to the column name this line uses for it. */
  def columnNameFor: Long => Option[String]

  /** Spark output field name to the Arrow column it is read from, for the
    * fields whose two names differ.
    *
    * The manifest line names every column by field id, so `vec` is read from
    * the Arrow column `101`; the column-group line keeps the field's own name
    * and only renames the system fields, where `row_id` is read from `RowID`. A
    * reader that looks the Spark name up directly finds no column and fails on
    * the first batch.
    *
    * Absent means the two names are the same, which is why [[arrowColumnFor]]
    * rather than the map is what readers call.
    */
  def arrowColumnNames: Map[String, String]

  def pkColumnName: String

  def timestampColumnName: String

  protected def milvusSchema: CollectionSchema

  final def arrowColumnFor(sparkFieldName: String): String =
    arrowColumnNames.getOrElse(sparkFieldName, sparkFieldName)

  final def pkField: Option[FieldSchema] =
    milvusSchema.fields.find(_.isPrimaryKey)

  final def deletePlan: DeletePlan = task.deletePlan

  final def appliesDeletes: Boolean = task.appliesDeletes

  final def open(allocator: BufferAllocator): SegmentReader =
    SegmentReaderRegistry.open(
      task,
      arrowSchema,
      neededColumns,
      columnNameFor,
      allocator
    )

  /** Whether the row at `rowIndex` of `batch` has been deleted.
    *
    * Deletes are keyed by primary key and timestamp, not by row number, so this
    * needs both columns present — which is why `neededColumns` pulls them in
    * even when the query did not ask for them.
    */
  final def isDeleted(batch: VectorSchemaRoot, rowIndex: Int): Boolean = {
    if (!appliesDeletes) return false
    val field = pkField.getOrElse {
      throw new IllegalArgumentException(
        "applying deletes needs a primary key field in the schema"
      )
    }
    val pkVector = batch.getVector(pkColumnName)
    if (pkVector == null) {
      throw new IllegalStateException(
        s"applying deletes needs the primary key column '$pkColumnName' loaded"
      )
    }
    val tsVector = batch.getVector(timestampColumnName)
    if (tsVector == null) {
      throw new IllegalStateException(
        s"applying deletes needs the timestamp column '$timestampColumnName' loaded"
      )
    }
    MilvusV2PartitionReader.rowDeleted(
      deletePlan,
      field,
      pkVector,
      tsVector.asInstanceOf[BigIntVector],
      rowIndex,
      pkColumnName
    )
  }
}

object ColumnBinding {

  /** Builds the setup for whichever line the partition belongs to.
    *
    * `schema` is the data schema, metadata extra columns already removed: it
    * says which columns the query wants.
    */
  def apply(
      partition: MilvusInputPartition,
      schema: StructType
  ): ColumnBinding = partition match {
    case p: MilvusV2InputPartition => V2ColumnBinding(p, schema)
    case p: MilvusV3InputPartition => V3ColumnBinding(p, schema)
  }
}

/** The column-group line: columns are named after the field. */
final case class V2ColumnBinding(
    partition: MilvusV2InputPartition,
    schema: StructType,
    override val task: SegmentReadTask
) extends ColumnBinding {

  protected val milvusSchema: CollectionSchema =
    CollectionSchema.parseFrom(task.schemaBytes)

  private val fieldMappings =
    MilvusV2PartitionReader.buildFieldMappings(milvusSchema)

  override val arrowSchema: Schema =
    SchemaMapper.convertToArrowSchema(milvusSchema)

  override val columnNameFor: Long => Option[String] =
    fieldMappings.fieldIdToName.get

  // Only the system fields differ: a user field's Arrow column carries the
  // field's own name.
  override val arrowColumnNames: Map[String, String] =
    fieldMappings.fieldNameToArrowColumn

  override val pkColumnName: String = pkField
    .map(field =>
      fieldMappings.fieldIdToName.getOrElse(field.fieldID, field.name)
    )
    .getOrElse("")

  override val timestampColumnName: String = "Timestamp"

  override val neededColumns: Seq[String] = {
    val columnGroups = task.layout match {
      case com.zilliz.milvus.storage.snapshot.SegmentLayout.ColumnGroups(gs) =>
        gs
      case other =>
        throw new IllegalArgumentException(
          s"the column-group line needs a materialized layout, got $other"
        )
    }
    val projected = MilvusV2PartitionReader.projectedFieldIds(
      schema,
      fieldMappings,
      task.neededFieldIds,
      appliesDeletes,
      deletePlan,
      pkField.map(_.fieldID).getOrElse(-1L),
      tsFieldId = 1L
    )
    MilvusV2PartitionReader
      .resolveNeededColumns(schema, columnGroups, fieldMappings, projected)
      .toSeq
  }
}

object V2ColumnBinding {
  def apply(
      partition: MilvusV2InputPartition,
      schema: StructType
  ): V2ColumnBinding =
    V2ColumnBinding(partition, schema, partition.task)
}

/** The manifest line: columns are named by field id, because that is what the
  * manifest records and what the native reader matches on.
  */
final case class V3ColumnBinding(
    partition: MilvusV3InputPartition,
    schema: StructType,
    override val task: SegmentReadTask
) extends ColumnBinding {

  protected val milvusSchema: CollectionSchema =
    CollectionSchema.parseFrom(task.schemaBytes)

  private val fieldNameToId: Map[String, Long] =
    MilvusV3PartitionReader.buildFieldNameToId(milvusSchema)

  override val arrowSchema: Schema =
    SchemaMapper.convertToArrowSchemaWithFieldIdNames(milvusSchema)

  override val columnNameFor: Long => Option[String] = id => Some(id.toString)

  override val arrowColumnNames: Map[String, String] =
    fieldNameToId.map { case (name, id) => name -> id.toString }

  override val pkColumnName: String =
    pkField.map(_.fieldID.toString).getOrElse("")

  override val timestampColumnName: String =
    MilvusV3PartitionReader.TimestampColumnName

  override val neededColumns: Seq[String] = {
    val requested =
      schema.fieldNames.toSeq.flatMap(fieldNameToId.get).map(_.toString)
    if (!appliesDeletes) requested
    else {
      val pk = pkField
        .map(_.fieldID.toString)
        .getOrElse(
          throw new IllegalArgumentException(
            "applying deletes needs a primary key field in the schema"
          )
        )
      (requested ++ Seq(pk, timestampColumnName)).distinct
    }
  }
}

object V3ColumnBinding {
  def apply(
      partition: MilvusV3InputPartition,
      schema: StructType
  ): V3ColumnBinding = V3ColumnBinding(partition, schema, partition.task)
}
