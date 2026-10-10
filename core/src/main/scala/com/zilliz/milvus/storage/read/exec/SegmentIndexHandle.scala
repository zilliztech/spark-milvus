package com.zilliz.milvus.storage.read.exec

import java.util.Locale
import scala.util.Try

import com.zilliz.milvus.jni.vector.NativeVectorIndex
import com.zilliz.milvus.storage.codec.{
  IndexFileCodec,
  IndexFileDecoder,
  MilvusIndexFileDecoder,
  VectorIndexFamilies
}
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout
}

/** One segment's vector index, open and ready to be searched.
  *
  * Opening it is what the Milvus format does with the index a snapshot pinned:
  * it checks the descriptor, reads the objects, decodes them and hands the
  * vector library the payload. A computation searches the handle and never sees
  * the files or the store it came from
  * (docs/design/architecture/vector-search.html sections 2.4 and 2.5).
  */
final class SegmentIndexHandle private (
    private[storage] val index: NativeVectorIndex,
    val metric: MetricType,
    val indexType: String,
    val segmentId: Long,
    val buildId: Long,
    val bytes: Long,
    val loadNanos: Long,
    val mapping: IndexRowMapping
) extends AutoCloseable {
  private var closed = false

  def rows: Long = index.rows()
  def dimension: Int = index.dimension()

  /** The rows of the segment this index was built from, which is more than the
    * index holds when the column has nulls.
    */
  def segmentRows: Long = mapping.segmentRows

  /** Which family the search parameters belong to (section 2.4). */
  def family: String = SegmentIndexHandle.familyOf(indexType)

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      index.close()
    }
  }
}

object SegmentIndexHandle {

  /** Why a segment's metadata offers no index a search can use. An index search
    * scans such a segment exactly, whatever the reason: an APPROX nearest-by
    * join, whose answer may be the exact one
    * (docs/design/architecture/dataframe-api.html section 2).
    */
  sealed abstract class Unusable(val label: String) extends Serializable

  object Unusable {
    case object NoIndex extends Unusable("no index on the field")
    case object NoMetadata extends Unusable("no index metadata in the snapshot")
    case object OtherMetric extends Unusable("an index of another metric")
    case object UnloadedType
        extends Unusable("an index type this connector does not load")
  }

  /** The index the snapshot's metadata gives `fieldId` in a segment, when this
    * connector can search it by `metric`, or why it cannot. Only the metadata
    * is read; whether the index matches the pinned segment is [[select]]'s.
    */
  def usable(
      segmentId: Long,
      indexes: SegmentIndexes,
      fieldId: Long,
      metric: MetricType
  ): Either[Unusable, SegmentIndex] = indexes match {
    case SegmentIndexes.Unknown   => Left(Unusable.NoMetadata)
    case SegmentIndexes.Unindexed => Left(Unusable.NoIndex)
    case SegmentIndexes.Available(all) =>
      val matches = all.filter(_.fieldId == fieldId)
      require(
        matches.size <= 1,
        s"Ambiguous index for segment $segmentId, field $fieldId"
      )
      matches.headOption match {
        case None => Left(Unusable.NoIndex)
        case Some(index) =>
          val indexType =
            index.indexType.map(_.toUpperCase(Locale.ROOT)).getOrElse("")
          if (!Supported.contains(indexType)) Left(Unusable.UnloadedType)
          else if (
            !index.metricType.flatMap(MetricType.fromName).contains(metric)
          )
            Left(Unusable.OtherMetric)
          else Right(index)
      }
  }

  /** The persisted index that serves `fieldId` in `task`'s segment, checked
    * against what the snapshot pinned, or `None` when the segment has no index
    * the search can use, which an index search scans exactly. An index that
    * differs from the pinned segment fails: the metadata contradicts itself.
    * Planning checks every task with this before any of them runs; the task
    * checks again on the executor.
    */
  def select(
      task: SegmentReadTask,
      fieldId: Long,
      metric: MetricType
  ): Option[SegmentIndex] = {
    task.layout match {
      case SegmentLayout.Manifest(_, version) =>
        require(
          version >= 0,
          "Persisted index search requires a pinned data manifest version"
        )
      case _ =>
    }
    usable(task.segmentId, task.indexes, fieldId, metric) match {
      case Right(descriptor) =>
        require(
          descriptor.segmentId == task.segmentId && descriptor.partitionId == task.partitionId,
          s"Index identity differs from the pinned segment ${task.segmentId}"
        )
        require(
          task.expectedRows.contains(descriptor.rowCount),
          s"Index row count differs from the pinned segment ${task.segmentId}"
        )
        require(
          descriptor.rowCount > 0 && descriptor.rowCount <= Int.MaxValue,
          s"Segment ${task.segmentId} bitmap exceeds supported row count"
        )
        Some(descriptor)
      case Left(_) => None
    }
  }

  /** Checks a whole plan before any task runs and names every segment whose
    * index metadata contradicts the segment the snapshot pinned.
    */
  def check(
      tasks: Seq[SegmentReadTask],
      fieldId: Long,
      metric: MetricType
  ): Unit = {
    val failures = tasks.flatMap { task =>
      Try(select(task, fieldId, metric)).failed.toOption
        .map(_.getMessage)
    }
    if (failures.nonEmpty) {
      val shown = failures.take(20).mkString("; ")
      val rest =
        if (failures.size > 20) s"; ${failures.size - 20} more" else ""
      throw new IllegalArgumentException(
        s"Index search cannot run on ${failures.size} of ${tasks.size} segments: $shown$rest"
      )
    }
  }

  /** The index of this task's segment, read through the store the task names.
    */
  def open(
      task: SegmentReadTask,
      index: SegmentIndex,
      layout: VectorLayout,
      nullable: Boolean
  ): SegmentIndexHandle = {
    val store = NativeObjectStore.Factory(task.properties).open()
    try
      open(
        index,
        layout,
        nullable,
        task.expectedRows.getOrElse(index.rowCount),
        store
      )
    finally store.close()
  }

  /** The index families this connector loads, and the family one type belongs
    * to, both from `core.codec.VectorIndexFamilies`: what can be loaded is what
    * the persisted format check knows the byte markers for.
    */
  private val Supported = VectorIndexFamilies.Supported

  def familyOf(indexType: String): String =
    VectorIndexFamilies.familyOf(indexType)

  /** The range a persisted index has to be in for this connector to load it: a
    * supported index type over the column's own element type, with a metric and
    * a format version the snapshot states. A nullable column is indexed over
    * the rows that have a value, and the index files say which those are. A
    * binary vector column is not searched (docs/design/README.md, decision log
    * 2026-10-09), so its indexes are not loaded. Anything else fails here,
    * before a file is read.
    */
  def open(
      index: SegmentIndex,
      layout: VectorLayout,
      nullable: Boolean,
      segmentRows: Long,
      store: ObjectStore,
      decoder: IndexFileDecoder = MilvusIndexFileDecoder
  ): SegmentIndexHandle = {
    require(
      layout.elementType != VectorElementType.Bit,
      "A binary vector index is not searched"
    )
    require(index.rowCount > 0, "Index row count must be positive")
    val indexType = index.indexType
      .getOrElse(
        throw new IllegalArgumentException(
          "Index metadata is missing index_type"
        )
      )
      .toUpperCase(Locale.ROOT)
    require(
      Supported.contains(indexType),
      s"Persisted index search does not support $indexType; it loads ${Supported.toSeq.sorted.mkString(", ")}"
    )
    val declared = index.metricType.getOrElse(
      throw new IllegalArgumentException(
        "Index metadata is missing metric_type"
      )
    )
    val metrics = MetricType.forElementType(layout.elementType)
    val metric = MetricType
      .fromName(declared)
      .filter(metrics.contains)
      .getOrElse(
        throw new IllegalArgumentException(
          s"A ${layout.elementType} index takes ${metrics
              .map(_.name)
              .sorted
              .mkString(" or ")}, not ${declared.toUpperCase(Locale.ROOT)}"
        )
      )
    require(
      index.currentIndexVersion.exists(_ >= 0),
      "Snapshot must declare the persisted vector index format version"
    )
    require(
      segmentRows > 0,
      s"Segment ${index.segmentId} declares $segmentRows rows"
    )
    // A nullable column's index names its valid_data bitmap among its files, so
    // an index that cannot say which rows it holds fails before anything is
    // read.
    require(
      !nullable || index.filePaths.exists(_.endsWith("/valid_data")),
      s"Segment ${index.segmentId} has a nullable vector column and its index files carry no valid_data bitmap"
    )
    val loaded = IndexFileCodec.load(index, layout, store, decoder)
    try {
      val mapping = loaded.validRows match {
        case Some(bitmap) => IndexRowMapping.of(bitmap, segmentRows)
        case None         => IndexRowMapping.identity(segmentRows)
      }
      require(
        !nullable || !mapping.isIdentity,
        s"Segment ${index.segmentId} declares a nullable vector column and no valid_data bitmap was read"
      )
      require(
        mapping.rows == loaded.index.rows(),
        s"The index holds ${loaded.index.rows()} rows and valid_data marks ${mapping.rows}"
      )
      new SegmentIndexHandle(
        loaded.index,
        metric,
        indexType,
        index.segmentId,
        index.buildId,
        loaded.bytes,
        loaded.nanos,
        mapping
      )
    } catch {
      case failure: Throwable =>
        loaded.index.close()
        throw failure
    }
  }

}
