package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.exec.{DeletePlans, SegmentReader}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, FieldSchema}

/** Everything a reader has to work out before it can open a segment: which
  * columns to ask the native layer for, what to call them, and how to tell a
  * deleted row.
  *
  * V3 manifests use field ids as Arrow column names. Input adapters may also
  * supply fully resolved physical columns through the neutral binding.
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
    SegmentReader.open(
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
    if (!appliesDeletes || deletePlan.isEmpty) return false
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
    DeletePlans.rowDeleted(
      deletePlan,
      field,
      pkVector,
      tsVector.asInstanceOf[BigIntVector],
      rowIndex
    )
  }
}

object ColumnBinding {

  /** Binds the columns using the layout described by the task.
    *
    * `schema` is the data schema, metadata extra columns already removed: it
    * says which columns the query wants.
    */
  def apply(
      partition: MilvusInputPartition,
      schema: StructType
  ): ColumnBinding = partition.task.layout match {
    case SegmentLayout.Manifest(_, _) => V3ColumnBinding(partition.task, schema)
    case _: SegmentLayout.PhysicalColumnGroups =>
      PhysicalColumnBinding(partition.task, schema)
  }
}

/** Already resolved Arrow names and files supplied by an input adapter. No
  * storage-version or legacy naming rule is inferred at this boundary.
  */
final case class PhysicalColumnBinding(
    override val task: SegmentReadTask,
    schema: StructType
) extends ColumnBinding {
  private val physical = task.layout match {
    case value: SegmentLayout.PhysicalColumnGroups => value
    case _ =>
      throw new IllegalArgumentException(
        "a physical binding needs resolved column groups"
      )
  }
  protected val milvusSchema: CollectionSchema =
    CollectionSchema.parseFrom(task.schemaBytes)
  override val arrowSchema: Schema = Schema.fromJSON(physical.arrowSchemaJson)
  private val names = arrowSchema.getFields.asScala.map(_.getName).toSet
  require(
    physical.fieldNames.values.forall(names),
    "physical field names differ from the Arrow schema"
  )
  require(
    physical.outputColumns.values.forall(names),
    "physical output names differ from the Arrow schema"
  )
  require(
    physical.groups.flatMap(_.columnNames).forall(names),
    "physical groups name unknown Arrow columns"
  )
  override val columnNameFor: Long => Option[String] = physical.fieldNames.get
  override val arrowColumnNames: Map[String, String] = physical.outputColumns
  override val pkColumnName: String =
    pkField.flatMap(field => columnNameFor(field.fieldID)).getOrElse("")
  override val timestampColumnName: String = columnNameFor(1L).getOrElse("")
  override lazy val neededColumns: Seq[String] = {
    val requested =
      if (task.neededFieldIds.nonEmpty) task.neededFieldIds.map { id =>
        columnNameFor(id).getOrElse(
          throw new IllegalArgumentException(s"Unknown physical field id $id")
        )
      }
      else schema.fieldNames.toSeq.map(arrowColumnFor)
    val required = if (appliesDeletes && !deletePlan.isEmpty) {
      require(
        pkColumnName.nonEmpty && timestampColumnName.nonEmpty,
        "applying deletes requires primary key and timestamp physical columns"
      )
      (requested ++ Seq(pkColumnName, timestampColumnName)).distinct
    } else requested.distinct
    val available = physical.groups.flatMap(_.columnNames).toSet
    require(
      required.forall(available),
      "physical column groups do not contain all requested columns"
    )
    required
  }
}

/** The manifest line: columns are named by field id, because that is what the
  * manifest records and what the native reader matches on.
  */
final case class V3ColumnBinding(
    override val task: SegmentReadTask,
    schema: StructType
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

  override lazy val neededColumns: Seq[String] = {
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
