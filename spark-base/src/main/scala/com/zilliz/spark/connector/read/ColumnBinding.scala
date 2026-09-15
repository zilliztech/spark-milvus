package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.exec.{
  DeletePlans,
  SegmentReader,
  SegmentReaderRegistry
}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.{SegmentLayout, V2ColumnGroup}
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

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

  /** Read once per task, on the executor, from the files the task names. */
  final lazy val deletePlan: DeletePlan = DeletePlans.of(task, pkField)

  final def appliesDeletes: Boolean = task.appliesDeletes

  final def open(allocator: BufferAllocator): SegmentReader = {
    // Resolve and close the delete-file store before the segment reader opens
    // its own native handles. This ordering applies to both the row and
    // columnar callers; in particular, passing isDeleted as a function to the
    // columnar reader must not defer delete I/O until its first batch.
    deletePlan
    SegmentReaderRegistry.open(
      task,
      arrowSchema,
      neededColumns,
      columnNameFor,
      allocator
    )
  }

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
    ColumnBinding.rowDeleted(
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

  /** Whether the row at `rowIndex` is deleted: its primary key is in the plan
    * with a delete timestamp after the row's own. Both lines key deletes the
    * same way, so this is line-independent.
    */
  private[read] def rowDeleted(
      deletePlan: DeletePlan,
      pkField: FieldSchema,
      pkVector: org.apache.arrow.vector.ValueVector,
      tsVector: BigIntVector,
      rowIndex: Int,
      pkColumnName: String
  ): Boolean = {
    if (pkVector.isNull(rowIndex) || tsVector.isNull(rowIndex)) {
      false
    } else {
      val rowTs = tsVector.get(rowIndex)
      pkField.dataType match {
        case DataType.Int64 =>
          deletePlan.containsLongPk(
            pkVector.asInstanceOf[BigIntVector].get(rowIndex),
            rowTs
          )
        case DataType.VarChar =>
          val value = pkVector match {
            case v: VarCharVector =>
              UTF8String.fromBytes(v.get(rowIndex)).toString
            case v: VarBinaryVector =>
              UTF8String.fromBytes(v.get(rowIndex)).toString
            case other =>
              throw new IllegalStateException(
                s"V2 delete filtering expected VarChar/VarBinary PK vector for $pkColumnName, got ${other.getClass.getSimpleName}"
              )
          }
          deletePlan.containsStringPk(value, rowTs)
        case other =>
          throw new IllegalArgumentException(
            s"V2 delete filtering only supports Int64/VarChar PKs, got $other"
          )
      }
    }
  }

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
    V2ColumnBinding.buildFieldMappings(milvusSchema)

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

  override val timestampColumnName: String =
    fieldMappings.fieldIdToName.getOrElse(1L, "Timestamp")

  override val neededColumns: Seq[String] = {
    val columnGroups = task.layout match {
      case com.zilliz.milvus.storage.snapshot.SegmentLayout.ColumnGroups(gs) =>
        gs
      case other =>
        throw new IllegalArgumentException(
          s"the column-group line needs a materialized layout, got $other"
        )
    }
    val projected = V2ColumnBinding.projectedFieldIds(
      schema,
      fieldMappings,
      task.neededFieldIds,
      appliesDeletes,
      deletePlan,
      pkField.map(_.fieldID).getOrElse(-1L),
      tsFieldId = 1L
    )
    V2ColumnBinding
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

  private[read] case class FieldMappings(
      fieldIdToName: Map[Long, String],
      fieldNameToId: Map[String, Long],
      fieldNameToArrowColumn: Map[String, String]
  )

  private[read] val SystemFieldAliases: Seq[(String, (Long, String))] = Seq(
    "RowID" -> (0L, "RowID"),
    "row_id" -> (0L, "RowID"),
    "rowid" -> (0L, "RowID"),
    "Timestamp" -> (1L, "Timestamp"),
    "timestamp" -> (1L, "Timestamp"),
    MilvusOption.MilvusExtraColumnTimestamp -> (1L, "Timestamp")
  )

  private[read] def buildFieldMappings(
      milvusSchema: CollectionSchema
  ): FieldMappings = {
    val systemFields = Map(0L -> "RowID", 1L -> "Timestamp")
    val userFields = milvusSchema.fields.map(f => f.fieldID -> f.name).toMap
    val fieldIdToName = systemFields ++ userFields
    val userFieldNames = milvusSchema.fields.map(_.name).toSet
    val systemAliases = SystemFieldAliases.filterNot { case (alias, _) =>
      userFieldNames.contains(alias)
    }
    val userFieldNameToId =
      milvusSchema.fields.map(f => f.name -> f.fieldID).toMap
    val systemFieldNameToId = systemAliases.map { case (alias, (id, _)) =>
      alias -> id
    }.toMap
    val systemFieldNameToArrowColumn = systemAliases.map {
      case (alias, (id, fallbackColumn)) =>
        alias -> fieldIdToName.getOrElse(id, fallbackColumn)
    }.toMap

    FieldMappings(
      fieldIdToName,
      systemFieldNameToId ++ userFieldNameToId,
      systemFieldNameToArrowColumn
    )
  }

  private[read] def projectedFieldIds(
      sourceSchema: StructType,
      fieldMappings: FieldMappings,
      neededColumnFieldIds: Seq[Long],
      applyDeletes: Boolean,
      deletePlan: DeletePlan,
      pkFieldId: Long,
      tsFieldId: Long = 1L
  ): Seq[Long] = {
    if (!applyDeletes || deletePlan.isEmpty) {
      neededColumnFieldIds
    } else if (neededColumnFieldIds.nonEmpty) {
      (neededColumnFieldIds ++ Seq(pkFieldId, tsFieldId)).distinct
    } else {
      (sourceSchema.fieldNames.toSeq.flatMap(
        fieldMappings.fieldNameToId.get
      ) ++ Seq(pkFieldId, tsFieldId)).distinct
    }
  }

  private[read] def resolveNeededColumns(
      sourceSchema: StructType,
      columnGroups: Seq[V2ColumnGroup],
      fieldMappings: FieldMappings,
      neededColumnFieldIds: Seq[Long]
  ): Array[String] = {
    if (columnGroups.isEmpty) {
      return Array.empty
    }

    val declaredFieldIds = columnGroups.flatMap(_.fieldIds).toSet
    val requestedFieldIds: Seq[Long] =
      if (neededColumnFieldIds.nonEmpty) {
        val missingIds = neededColumnFieldIds.filterNot(
          fieldMappings.fieldIdToName.contains
        )
        if (missingIds.nonEmpty) {
          throw new IllegalArgumentException(
            s"V2 requested unknown field IDs: ${missingIds.distinct.mkString(",")}" +
              s"; schema field IDs=${fieldMappings.fieldIdToName.keys.toSeq.sorted.mkString(",")}"
          )
        }
        neededColumnFieldIds
      } else {
        val missingNames = sourceSchema.fieldNames.filterNot(
          fieldMappings.fieldNameToId.contains
        )
        if (missingNames.nonEmpty) {
          throw new IllegalArgumentException(
            s"V2 requested unknown columns: ${missingNames.distinct.mkString(",")}" +
              s"; schema columns=${fieldMappings.fieldNameToId.keys.toSeq.sorted.mkString(",")}"
          )
        }
        sourceSchema.fieldNames.toSeq.flatMap(fieldMappings.fieldNameToId.get)
      }

    val missingFieldIds = requestedFieldIds.filterNot(declaredFieldIds.contains)
    if (missingFieldIds.nonEmpty) {
      val missingColumns = missingFieldIds
        .flatMap(fieldMappings.fieldIdToName.get)
        .distinct
      val declaredColumns = declaredFieldIds
        .flatMap(fieldMappings.fieldIdToName.get)
        .toSeq
        .sorted
      throw new IllegalArgumentException(
        s"V2 column groups do not contain requested columns: ${missingColumns
            .mkString(",")}" +
          s"; declared columns=${declaredColumns.mkString(",")}"
      )
    }

    requestedFieldIds.flatMap(fieldMappings.fieldIdToName.get).toArray
  }
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
    V3ColumnBinding.buildFieldNameToId(milvusSchema)

  override val arrowSchema: Schema =
    SchemaMapper.convertToArrowSchemaWithFieldIdNames(milvusSchema)

  override val columnNameFor: Long => Option[String] = id => Some(id.toString)

  override val arrowColumnNames: Map[String, String] =
    fieldNameToId.map { case (name, id) => name -> id.toString }

  override val pkColumnName: String =
    pkField.map(_.fieldID.toString).getOrElse("")

  override val timestampColumnName: String =
    V3ColumnBinding.TimestampColumnName

  override val neededColumns: Seq[String] = {
    val requested = if (task.neededFieldIds.nonEmpty) {
      val knownIds = fieldNameToId.values.toSet
      val missingIds = task.neededFieldIds.filterNot(knownIds)
      if (missingIds.nonEmpty) {
        throw new IllegalArgumentException(
          s"V3 requested unknown field IDs: ${missingIds.distinct.mkString(",")}" +
            s"; schema field IDs=${knownIds.toSeq.sorted.mkString(",")}"
        )
      }
      task.neededFieldIds.map(_.toString)
    } else {
      schema.fieldNames.toSeq.flatMap(fieldNameToId.get).map(_.toString)
    }
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

  private[read] val TimestampColumnName = "1"

  private[read] val SystemFieldAliases: Seq[(String, Long)] = Seq(
    "RowID" -> 0L,
    "row_id" -> 0L,
    "rowid" -> 0L,
    "Timestamp" -> 1L,
    "timestamp" -> 1L,
    MilvusOption.MilvusExtraColumnTimestamp -> 1L
  )

  private[read] def buildFieldNameToId(
      milvusSchema: CollectionSchema
  ): Map[String, Long] = {
    val userFieldNames = milvusSchema.fields.map(_.name).toSet
    val systemFields = SystemFieldAliases.filterNot { case (alias, _) =>
      userFieldNames.contains(alias)
    }.toMap
    val userFields = milvusSchema.fields.map { field =>
      field.name -> field.fieldID
    }.toMap
    systemFields ++ userFields
  }
}
