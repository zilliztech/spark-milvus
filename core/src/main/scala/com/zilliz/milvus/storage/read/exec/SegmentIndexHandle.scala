package com.zilliz.milvus.storage.read.exec

import java.util.Locale
import scala.util.Try

import com.zilliz.milvus.jni.vector.NativeVectorIndex
import com.zilliz.milvus.storage.codec.{
  IndexFileCodec,
  IndexFileDecoder,
  MilvusIndexFileDecoder
}
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
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
    val metric: String,
    val segmentId: Long,
    val buildId: Long,
    val bytes: Long,
    val loadNanos: Long
) extends AutoCloseable {
  private var closed = false

  def rows: Long = index.rows()
  def dimension: Int = index.dimension()

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      index.close()
    }
  }
}

object SegmentIndexHandle {

  /** The persisted index that serves `fieldId` in `task`'s segment, checked
    * against what the snapshot pinned: `None` only when the snapshot says the
    * segment has no index and the search allows that. Planning checks every
    * task with this before any of them runs; the task checks again on the
    * executor.
    */
  def select(
      task: SegmentReadTask,
      fieldId: Long,
      metric: String,
      allowUnindexed: Boolean
  ): Option[SegmentIndex] = {
    task.layout match {
      case SegmentLayout.Manifest(_, version) =>
        require(
          version >= 0,
          "Persisted index search requires a pinned data manifest version"
        )
      case _ =>
    }
    val selected = task.indexes match {
      case SegmentIndexes.Available(indexes) =>
        val matches = indexes.filter(_.fieldId == fieldId)
        require(
          matches.size <= 1,
          s"Ambiguous index for segment ${task.segmentId}, field $fieldId"
        )
        matches.headOption
      case SegmentIndexes.Unindexed => None
      case SegmentIndexes.Unknown =>
        throw new IllegalArgumentException(
          s"Snapshot has no index metadata for segment ${task.segmentId}"
        )
    }
    require(
      selected.nonEmpty || allowUnindexed,
      s"No persisted index for segment ${task.segmentId}, field $fieldId"
    )
    selected.foreach { descriptor =>
      require(
        descriptor.segmentId == task.segmentId && descriptor.partitionId == task.partitionId,
        s"Index identity differs from the pinned segment ${task.segmentId}"
      )
      require(
        descriptor.metricType.exists(_.equalsIgnoreCase(metric)),
        s"Query metric differs from the persisted index metric of segment ${task.segmentId}"
      )
      require(
        task.expectedRows.contains(descriptor.rowCount),
        s"Index row count differs from the pinned segment ${task.segmentId}"
      )
      require(
        descriptor.rowCount > 0 && descriptor.rowCount <= Int.MaxValue,
        s"Segment ${task.segmentId} bitmap exceeds supported row count"
      )
    }
    selected
  }

  /** Checks a whole plan before any task runs and names every segment that
    * cannot serve the search.
    */
  def check(
      tasks: Seq[SegmentReadTask],
      fieldId: Long,
      metric: String,
      allowUnindexed: Boolean
  ): Unit = {
    val failures = tasks.flatMap { task =>
      Try(select(task, fieldId, metric, allowUnindexed)).failed.toOption
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
      dimension: Int,
      nullable: Boolean
  ): SegmentIndexHandle = {
    val store = NativeObjectStore.Factory(task.properties).open()
    try open(index, dimension, nullable, store)
    finally store.close()
  }

  /** The range a persisted index has to be in for this connector to load it: an
    * HNSW index over a non-nullable float vector, with a metric and a format
    * version the snapshot states. Anything else fails here, before a file is
    * read.
    */
  def open(
      index: SegmentIndex,
      dimension: Int,
      nullable: Boolean,
      store: ObjectStore,
      decoder: IndexFileDecoder = MilvusIndexFileDecoder
  ): SegmentIndexHandle = {
    require(
      !nullable,
      "Persisted index search currently requires a non-nullable FloatVector"
    )
    require(
      dimension > 0 && index.rowCount > 0,
      "Index dimensions and row count must be positive"
    )
    require(
      index.indexType.exists(_.equalsIgnoreCase("HNSW")),
      "Persisted index search currently supports HNSW FloatVector indexes"
    )
    val metric = index.metricType
      .getOrElse(
        throw new IllegalArgumentException(
          "Index metadata is missing metric_type"
        )
      )
      .toUpperCase(Locale.ROOT)
    require(
      Set("L2", "IP", "COSINE").contains(metric),
      s"Unsupported index metric: $metric"
    )
    require(
      index.currentIndexVersion.exists(_ >= 0),
      "Snapshot must declare the persisted vector index format version"
    )
    val loaded = IndexFileCodec.load(index, dimension, store, decoder)
    new SegmentIndexHandle(
      loaded.index,
      metric,
      index.segmentId,
      index.buildId,
      loaded.bytes,
      loaded.nanos
    )
  }
}
