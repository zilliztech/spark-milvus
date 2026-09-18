package com.zilliz.milvus.storage.read.exec

import java.util.Locale

import com.zilliz.milvus.jni.vector.NativeVectorIndex
import com.zilliz.milvus.storage.codec.{
  IndexFileCodec,
  IndexFileDecoder,
  MilvusIndexFileDecoder
}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.snapshot.SegmentIndex

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
    val buildId: Long
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
    new SegmentIndexHandle(
      IndexFileCodec.load(index, dimension, store, decoder),
      metric,
      index.segmentId,
      index.buildId
    )
  }
}
