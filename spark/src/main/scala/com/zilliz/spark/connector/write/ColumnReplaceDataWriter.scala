package com.zilliz.spark.connector.write

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BaseVariableWidthVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{
  DataWriter,
  DataWriterFactory,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.io.{
  NativeObjectStore,
  NativeWritableObjectStore
}
import com.zilliz.milvus.storage.manifest.V3ManifestReader
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.write.{
  ColumnGroupFiles,
  V3SegmentWriter,
  WriterResources
}
import com.zilliz.spark.connector.types.RowIdentity

/** What every write task of one column replacement needs. Segment descriptors
  * travel separately by broadcast, because a collection can have many.
  *
  * @param rowIdOrdinal
  *   the position of `_milvus_row_id` in the rows Spark hands the writer
  * @param targetOrdinals
  *   the position of each replaced column, in `targetSchema` order
  */
final case class ColumnReplaceTask(
    snapshotId: Long,
    rowIdOrdinal: Int,
    targetOrdinals: Seq[Int],
    targetSchema: StructType,
    storage: Map[String, String],
    bucket: String,
    params: WriteOptions
)

object ColumnReplaceTask {

  /** Binds the plan to the rows of the query being written, which Spark
    * resolved against the table schema by name.
    */
  def of(
      plan: ColumnReplacePlan,
      querySchema: StructType
  ): ColumnReplaceTask = {
    val positions = querySchema.fieldNames.zipWithIndex.toMap
    def position(name: String): Int =
      positions.getOrElse(
        name,
        throw new IllegalStateException(
          s"the rows being written have no column '$name'; they have ${querySchema.fieldNames.mkString(", ")}"
        )
      )
    require(
      querySchema.size == plan.targetSchema.size + 1,
      s"the rows being written have columns ${querySchema.fieldNames.mkString(", ")}, " +
        s"but the write replaces ${plan.targetSchema.fieldNames.mkString(", ")}"
    )
    ColumnReplaceTask(
      snapshotId = plan.snapshot.id,
      rowIdOrdinal = position(RowIdentity.ColumnName),
      targetOrdinals = plan.targetSchema.fieldNames.toSeq.map(position),
      targetSchema = plan.targetSchema,
      storage = plan.storage,
      bucket = plan.bucket,
      params = plan.params
    )
  }
}

/** The new files of one segment, as the attempt that wrote them reports them to
  * the driver.
  */
final case class WrittenSegment(
    segmentId: Long,
    rows: Long,
    columnGroups: Seq[ColumnGroupFiles]
)

final case class ColumnReplaceCommitMessage(segments: Seq[WrittenSegment])
    extends WriterCommitMessage

final class ColumnReplaceWriterFactory(
    task: ColumnReplaceTask,
    segments: Broadcast[Map[Long, PlannedSegment]]
) extends DataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] =
    new ColumnReplaceDataWriter(task, segments.value)
}

/** Writes the replaced columns of the segments one Spark partition receives.
  *
  * Rows arrive clustered by segment and ordered by offset, so a segment's rows
  * are one run and the writer moves to the next segment when the identity
  * changes. Each run must hold the segment's offsets from zero to its stored
  * row count exactly once; a gap, a repeat, a second run of one segment or a
  * row of another snapshot fails the task before anything is committed. Files
  * go into the segment's own directory under names no other attempt uses, and
  * nothing here touches a manifest.
  */
final class ColumnReplaceDataWriter(
    task: ColumnReplaceTask,
    segments: Map[Long, PlannedSegment]
) extends DataWriter[InternalRow]
    with Logging {

  private val targetFieldIds: Set[Long] = task.targetSchema.fields.iterator
    .map(_.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey))
    .toSet
  private val finished = mutable.ArrayBuffer.empty[WrittenSegment]
  private val started = mutable.Set.empty[Long]
  private var current: SegmentColumnsWriter = null
  private var statisticsStore: NativeObjectStore = null

  override def write(row: InternalRow): Unit = {
    if (row.isNullAt(task.rowIdOrdinal)) {
      throw new IllegalArgumentException(
        s"A row has a null '${RowIdentity.ColumnName}'; carry the column from the physical read unchanged"
      )
    }
    val identity = row.getStruct(task.rowIdOrdinal, RowIdentity.dataType.size)
    if (
      identity.isNullAt(RowIdentity.SnapshotIdOrdinal) ||
      identity.isNullAt(RowIdentity.SegmentIdOrdinal) ||
      identity.isNullAt(RowIdentity.RowOffsetOrdinal)
    ) {
      throw new IllegalArgumentException(
        s"A row has an incomplete '${RowIdentity.ColumnName}'; carry the column from the physical read unchanged"
      )
    }
    val snapshotId = identity.getLong(RowIdentity.SnapshotIdOrdinal)
    if (snapshotId != task.snapshotId) {
      throw new IllegalArgumentException(
        s"A row was read from snapshot $snapshotId, but this write replaces columns of snapshot " +
          s"${task.snapshotId}; write every row back with the snapshot it was read from"
      )
    }
    val segmentId = identity.getLong(RowIdentity.SegmentIdOrdinal)
    if (current == null || current.segment.id != segmentId) start(segmentId)
    current.write(identity.getLong(RowIdentity.RowOffsetOrdinal), row)
  }

  override def commit(): WriterCommitMessage = {
    finishCurrent()
    ColumnReplaceCommitMessage(finished.toList)
  }

  /** No manifest references this attempt's files, because the driver commits
    * only attempts Spark adopted, so they can go.
    */
  override def abort(): Unit = {
    val files = finished.toList.flatMap(_.columnGroups.flatMap(_.files))
    try if (current != null) current.abort()
    finally {
      current = null
      deleteQuietly(files)
    }
  }

  override def close(): Unit = {
    val writer = current
    val store = statisticsStore
    current = null
    statisticsStore = null
    WriterResources.closeAll(
      null,
      () => if (writer != null) writer.abort(),
      () => if (store != null) store.close()
    )
  }

  private def start(segmentId: Long): Unit = {
    finishCurrent()
    if (!started.add(segmentId)) {
      throw new IllegalStateException(
        s"The rows of segment $segmentId reached this writer in more than one run; " +
          "the write requires them clustered by segment"
      )
    }
    val segment = segments.getOrElse(
      segmentId,
      throw new IllegalArgumentException(
        s"A row identity names segment $segmentId, which stores no rows in snapshot ${task.snapshotId}"
      )
    )
    refuseStatistics(segment)
    current = new SegmentColumnsWriter(segment, task)
  }

  private def finishCurrent(): Unit =
    if (current != null) {
      val writer = current
      current = null
      finished += writer.finish()
    }

  /** A statistic of a replaced field, such as a text index or JSON key
    * statistics, would keep describing the old values after the swap.
    */
  private def refuseStatistics(segment: PlannedSegment): Unit = {
    if (statisticsStore == null)
      statisticsStore = NativeObjectStore.Factory(task.storage).open()
    val statistics = V3ManifestReader
      .loadStatistics(
        segment.basePath,
        segment.sourceVersion,
        task.bucket,
        statisticsStore
      )
      .fold(
        error =>
          throw new IllegalStateException(
            s"Cannot read the statistics of segment ${segment.id} at revision ${segment.sourceVersion}: ${error.getMessage}",
            error
          ),
        identity
      )
    val stale = statistics.keys.toSeq.sorted.filter(key =>
      ColumnReplaceDataWriter.statisticFieldId(key).exists(targetFieldIds)
    )
    if (stale.nonEmpty) {
      throw new IllegalArgumentException(
        s"Segment ${segment.id} keeps statistics ${stale.mkString(", ")} of a replaced field. Replacing the " +
          "column would leave them describing the old values, so the write refuses it"
      )
    }
  }

  private def deleteQuietly(files: Seq[String]): Unit =
    if (files.nonEmpty) {
      try {
        val store = NativeWritableObjectStore.open(task.storage)
        WriterResources.closing(store.close()) {
          files.foreach { file =>
            try store.delete(file)
            catch {
              case NonFatal(e) =>
                logWarning(
                  s"Could not delete abandoned file $file: ${e.getMessage}"
                )
            }
          }
        }
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Could not delete ${files.size} abandoned file(s): ${e.getMessage}"
          )
      }
    }
}

object ColumnReplaceDataWriter {

  /** The field a manifest statistic describes: its key ends in `.<field id>`,
    * as in `bloom_filter.100` or `text_index.101`.
    */
  private[write] def statisticFieldId(key: String): Option[Long] = {
    val dot = key.lastIndexOf('.')
    if (dot < 0 || dot == key.length - 1) None
    else {
      val suffix = key.substring(dot + 1)
      if (suffix.forall(Character.isDigit)) Some(suffix.toLong) else None
    }
  }
}

/** The replaced columns of one segment, one column group per field, written in
  * stored row order. The writer checks each offset as it arrives: the next one
  * must be exactly one past the last.
  */
private final class SegmentColumnsWriter(
    val segment: PlannedSegment,
    task: ColumnReplaceTask
) {
  private val schema = task.targetSchema
  private val fieldIds = schema.fields
    .map(field =>
      field.name -> field.metadata
        .getLong(FieldMetadata.MilvusFieldIdMetadataKey)
    )
    .toMap
  private val columns =
    schema.fieldNames.toSeq.map(name => fieldIds(name).toString)
  private val arrowSchema =
    WriteSchema.arrowSchemaFor(schema, fieldIds, withSystemFields = false)
  private val types = schema.fields.map(_.dataType)
  private val ordinals = task.targetOrdinals.toArray
  private val required = schema.fields.map(!_.nullable)
  private val values = new Array[Any](schema.size)
  private val projected = new GenericInternalRow(values)
  private val allocator = new RootAllocator(Long.MaxValue)
  private var root: VectorSchemaRoot = null
  private var writer: V3SegmentWriter = null
  private var batchRows = 0
  private var next = 0L
  private var released = false

  try root = newRoot()
  catch {
    case failure: Throwable =>
      WriterResources.closeAll(failure, () => release())
      throw failure
  }

  def write(offset: Long, row: InternalRow): Unit = {
    if (offset != next) {
      if (offset >= 0L && offset < next)
        throw new IllegalArgumentException(
          s"Row offset $offset of segment ${segment.id} arrived more than once; " +
            "every stored row must be written exactly once"
        )
      else if (offset >= segment.rows || offset < 0L)
        throw new IllegalArgumentException(
          s"Row offset $offset is outside the ${segment.rows} stored rows of segment ${segment.id}"
        )
      else
        throw new IllegalArgumentException(
          s"Segment ${segment.id} has no row at offset $next before offset $offset; every stored row, " +
            "rows marked deleted included, must be written exactly once"
        )
    }
    if (offset >= segment.rows) {
      throw new IllegalArgumentException(
        s"Row offset $offset is outside the ${segment.rows} stored rows of segment ${segment.id}"
      )
    }
    var index = 0
    while (index < ordinals.length) {
      val ordinal = ordinals(index)
      if (row.isNullAt(ordinal)) {
        if (required(index)) {
          throw new IllegalArgumentException(
            s"Column '${schema.fields(index).name}' is NULL at offset $offset of segment ${segment.id}, " +
              "but its field is not nullable"
          )
        }
        values(index) = null
      } else values(index) = row.get(ordinal, types(index))
      index += 1
    }
    ArrowWriter.internalRowToArrow(root, batchRows, projected, schema)
    batchRows += 1
    root.setRowCount(batchRows)
    next += 1L
    if (batchRows >= task.params.batchRows) flush()
  }

  /** Closes the files and describes them. The segment is complete only if every
    * stored offset arrived.
    */
  def finish(): WrittenSegment =
    WriterResources.closing(release()) {
      if (next != segment.rows) {
        throw new IllegalArgumentException(
          s"Segment ${segment.id} stores ${segment.rows} rows, but only $next arrived; every stored row, " +
            "rows marked deleted included, must be written exactly once"
        )
      }
      flush()
      val groups = writer.finish()
      val described = WriterResources.closing(groups.close())(groups.describe)
      val written = described.flatMap(_.columns)
      require(
        described.forall(_.columns.size == 1) &&
          written.sorted == columns.sorted,
        s"segment ${segment.id} was written as column groups ${described.map(_.columns).mkString(", ")}, " +
          s"not one group per replaced field ${columns.mkString(", ")}"
      )
      require(
        described.forall(group =>
          group.format == ColumnGroupFiles.Parquet && group.rows == segment.rows
        ),
        s"segment ${segment.id} column groups do not each cover its ${segment.rows} rows in parquet"
      )
      WrittenSegment(segment.id, segment.rows, described)
    }

  def abort(): Unit = release()

  private def flush(): Unit = {
    if (batchRows == 0) return
    if (writer == null) {
      val properties = task.params.writerProperties(task.storage) ++ Map(
        "writer.format" -> ColumnGroupFiles.Parquet,
        "writer.split.schema_based.formats" -> columns
          .map(_ => ColumnGroupFiles.Parquet)
          .mkString(",")
      )
      writer = new V3SegmentWriter(
        segment.basePath,
        arrowSchema,
        properties,
        allocator,
        columns.map(id => s"^${id}$$")
      )
    }
    root.setRowCount(batchRows)
    writer.write(root)
    val previous = root
    root = newRoot()
    batchRows = 0
    previous.close()
  }

  private def newRoot(): VectorSchemaRoot = {
    val value = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      value.getFieldVectors.asScala.foreach {
        case vector: VarCharVector =>
          vector.setInitialCapacity(
            task.params.batchRows,
            task.params.variableWidthBytesPerValue
          )
        case vector: BaseVariableWidthVector =>
          vector.setInitialCapacity(
            task.params.batchRows,
            task.params.variableWidthBytesPerValue
          )
        case vector => vector.setInitialCapacity(task.params.batchRows)
      }
      value.allocateNew()
      value.setRowCount(0)
      value
    } catch {
      case failure: Throwable =>
        WriterResources.closeAll(failure, () => value.close())
        throw failure
    }
  }

  private def release(): Unit = {
    if (released) return
    released = true
    WriterResources.closeAll(
      null,
      () => if (writer != null) writer.close(),
      () => if (root != null) root.close(),
      () => allocator.close()
    )
  }
}
