package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.delete.MilvusDeletePlan
import com.zilliz.milvus.storage.read.exec.{
  SegmentReader,
  SegmentReaderRegistry
}
import com.zilliz.milvus.storage.read.plan.InputSpec
import com.zilliz.milvus.storage.schema.SchemaMapper
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
sealed trait SegmentReadSetup {

  def spec: InputSpec

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

  def pkColumnName: String

  def timestampColumnName: String

  protected def milvusSchema: CollectionSchema

  final def pkField: Option[FieldSchema] =
    milvusSchema.fields.find(_.isPrimaryKey)

  final def deletePlan: MilvusDeletePlan = spec.deletePlan

  final def appliesDeletes: Boolean = spec.appliesDeletes

  final def open(allocator: BufferAllocator): SegmentReader =
    SegmentReaderRegistry.open(
      spec,
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
    MilvusPackedV2PartitionReader.rowDeleted(
      deletePlan,
      field,
      pkVector,
      tsVector.asInstanceOf[BigIntVector],
      rowIndex,
      pkColumnName
    )
  }
}

object SegmentReadSetup {

  /** Builds the setup for whichever line the partition belongs to.
    *
    * `schema` is the data schema, metadata extra columns already removed: it
    * says which columns the query wants.
    */
  def apply(
      partition: MilvusInputPartition,
      schema: StructType
  ): SegmentReadSetup = partition match {
    case p: MilvusPackedV2InputPartition  => PackedV2ReadSetup(p, schema)
    case p: MilvusStorageV3InputPartition => LoonReadSetup(p, schema)
  }
}

/** The column-group line: columns are named after the field. */
final case class PackedV2ReadSetup(
    partition: MilvusPackedV2InputPartition,
    schema: StructType,
    override val spec: InputSpec
) extends SegmentReadSetup {

  protected val milvusSchema: CollectionSchema =
    CollectionSchema.parseFrom(spec.schemaBytes)

  private val fieldMappings =
    MilvusPackedV2PartitionReader.buildFieldMappings(milvusSchema)

  override val arrowSchema: Schema =
    SchemaMapper.convertToArrowSchema(milvusSchema)

  override val columnNameFor: Long => Option[String] =
    fieldMappings.fieldIdToName.get

  override val pkColumnName: String = pkField
    .map(field =>
      fieldMappings.fieldIdToName.getOrElse(field.fieldID, field.name)
    )
    .getOrElse("")

  override val timestampColumnName: String = "Timestamp"

  override val neededColumns: Seq[String] = {
    val columnGroups = spec.layout match {
      case com.zilliz.milvus.storage.read.plan.SegmentLayout.ColumnGroups(gs) =>
        gs
      case other =>
        throw new IllegalArgumentException(
          s"the column-group line needs a materialized layout, got $other"
        )
    }
    val projected = MilvusPackedV2PartitionReader.projectedFieldIds(
      schema,
      fieldMappings,
      spec.neededFieldIds,
      appliesDeletes,
      deletePlan,
      pkField.map(_.fieldID).getOrElse(-1L),
      tsFieldId = 1L
    )
    MilvusPackedV2PartitionReader
      .resolveNeededColumns(schema, columnGroups, fieldMappings, projected)
      .toSeq
  }
}

object PackedV2ReadSetup {
  def apply(
      partition: MilvusPackedV2InputPartition,
      schema: StructType
  ): PackedV2ReadSetup =
    PackedV2ReadSetup(partition, schema, partition.spec)
}

/** The manifest line: columns are named by field id, because that is what the
  * manifest records and what the native reader matches on.
  */
final case class LoonReadSetup(
    partition: MilvusStorageV3InputPartition,
    schema: StructType,
    override val spec: InputSpec
) extends SegmentReadSetup {

  protected val milvusSchema: CollectionSchema =
    CollectionSchema.parseFrom(spec.schemaBytes)

  private val fieldNameToId: Map[String, Long] =
    MilvusLoonPartitionReader.buildFieldNameToId(milvusSchema)

  override val arrowSchema: Schema =
    SchemaMapper.convertToArrowSchemaWithFieldIdNames(milvusSchema)

  override val columnNameFor: Long => Option[String] = id => Some(id.toString)

  override val pkColumnName: String =
    pkField.map(_.fieldID.toString).getOrElse("")

  override val timestampColumnName: String =
    MilvusLoonPartitionReader.TimestampColumnName

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

object LoonReadSetup {
  def apply(
      partition: MilvusStorageV3InputPartition,
      schema: StructType
  ): LoonReadSetup = LoonReadSetup(partition, schema, partition.spec)
}
