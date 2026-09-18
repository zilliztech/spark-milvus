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
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
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
      val indexType =
        descriptor.indexType.map(_.toUpperCase(Locale.ROOT)).getOrElse("")
      require(
        Supported.contains(indexType),
        s"Segment ${task.segmentId} carries a $indexType index; this connector loads ${Supported.toSeq.sorted
            .mkString(", ")}"
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

  /** The index families this connector loads: the graph indexes Knowhere
    * registers under an HNSW name, the inverted-list indexes under an IVF name,
    * and the two flat indexes, each over the element type the column carries.
    */
  val HnswFamily: Set[String] = Set("HNSW", "HNSW_SQ", "HNSW_PQ", "HNSW_PRQ")

  val IvfFamily: Set[String] =
    Set("IVF_FLAT", "IVF_SQ8", "IVF_PQ", "BIN_IVF_FLAT")

  val FlatFamily: Set[String] = Set("FLAT", "BIN_FLAT")

  private val Supported = HnswFamily ++ IvfFamily ++ FlatFamily

  def familyOf(indexType: String): String =
    if (HnswFamily.contains(indexType)) "HNSW"
    else if (IvfFamily.contains(indexType)) "IVF"
    else "FLAT"

  /** The metrics each element type can be searched by. */
  def metricsOf(layout: VectorLayout): Set[String] =
    if (layout.elementType == VectorElementType.Bit) Set("HAMMING", "JACCARD")
    else Set("L2", "IP", "COSINE")

  /** The range a persisted index has to be in for this connector to load it: a
    * supported index type over the column's own element type, with a metric and
    * a format version the snapshot states. A nullable column is indexed over
    * the rows that have a value, and the index files say which those are.
    * Anything else fails here, before a file is read.
    */
  def open(
      index: SegmentIndex,
      layout: VectorLayout,
      nullable: Boolean,
      segmentRows: Long,
      store: ObjectStore,
      decoder: IndexFileDecoder = MilvusIndexFileDecoder
  ): SegmentIndexHandle = {
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
    val metric = index.metricType
      .getOrElse(
        throw new IllegalArgumentException(
          "Index metadata is missing metric_type"
        )
      )
      .toUpperCase(Locale.ROOT)
    val metrics = metricsOf(layout)
    require(
      metrics.contains(metric),
      s"A ${layout.elementType} index takes ${metrics.toSeq.sorted
          .mkString(" or ")}, not $metric"
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
