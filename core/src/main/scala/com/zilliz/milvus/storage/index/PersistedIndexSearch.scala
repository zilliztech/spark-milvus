package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.ByteOrder
import java.util.Locale
import scala.collection.mutable
import scala.util.Try

import org.apache.arrow.memory.{ArrowBuf, RootAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorIndex
import com.zilliz.milvus.storage.codec.{
  IndexFileCodec,
  IndexFileDecoder,
  MilvusIndexFileDecoder
}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.snapshot.SegmentIndex
import com.zilliz.milvus.storage.Logging

/** Query-scoped owner of an existing segment index. It never builds an index.
  */
final class PersistedIndexSearch private (
    index: NativeVectorIndex,
    metric: String,
    segmentId: Long,
    buildId: Long
) extends AutoCloseable
    with Logging {
  import PersistedIndexSearch._

  private var closed = false
  def rows: Long = index.rows()
  def dimension: Int = index.dimension()

  /** Exclusions use physical segment row offsets; 1 means excluded. */
  def search(
      query: Array[Float],
      topK: Int,
      excluded: Long => Boolean = _ => false,
      parameters: Map[String, String] = Map.empty
  ): Vector[Hit] = synchronized {
    require(!closed, "Persisted index search is closed")
    require(
      query != null && query.length == dimension,
      s"Query must have dimension $dimension"
    )
    val values = query.clone()
    require(
      values.forall(JavaFloat.isFinite),
      "Query vector must be finite"
    )
    require(
      metric != "COSINE" || values.exists(_ != 0.0f),
      "COSINE query must have nonzero norm"
    )
    require(excluded != null, "Row exclusions must be provided")
    require(topK > 0, "topK must be positive")
    val requestedEf = searchEf(topK, parameters)
    require(
      rows <= 256L * 1024 * 1024 * 8,
      "Segment exclusion bitmap exceeds the supported size"
    )
    val maskBytes = (rows + 7L) / 8L
    val allocator = new RootAllocator()
    val allocated = mutable.ArrayBuffer.empty[ArrowBuf]
    val started = System.nanoTime()
    var nativeCalls = 0
    var returnedCount = 0
    var succeeded = false
    def buffer(size: Long): ArrowBuf = {
      require(
        size >= 0 && size <= Int.MaxValue,
        "Query buffer exceeds ByteBuffer capacity"
      )
      val result = allocator.buffer(size)
      allocated += result
      result
    }
    try {
      val mask = buffer(maskBytes)
      mask.setZero(0, maskBytes)
      var visible = 0L
      var row = 0L
      while (row < rows) {
        if (excluded(row)) {
          mask.setByte(
            row / 8L,
            mask.getByte(row / 8L) | (1 << (row & 7L).toInt)
          )
        } else visible += 1
        row += 1
      }
      if (visible == 0) {
        succeeded = true
        return Vector.empty
      }
      val count = math.min(topK.toLong, visible).toInt
      val queryBuffer = buffer(dimension.toLong * 4L)
      values.indices.foreach(i =>
        queryBuffer.setFloat(i.toLong * 4L, values(i))
      )
      val ids = buffer(count.toLong * 8L)
      val scores = buffer(count.toLong * 4L)
      def bytes(value: ArrowBuf, size: Long) =
        value.nioBuffer(0, size.toInt).order(ByteOrder.nativeOrder())
      var ef = requestedEf
      var attempt = 0
      var results = Vector.empty[Hit]
      do {
        val nativeParameters = s"""{"metric_type":"$metric","ef":$ef}"""
        nativeCalls += 1
        index.search(
          bytes(queryBuffer, dimension.toLong * 4L),
          count,
          bytes(mask, maskBytes),
          bytes(ids, count.toLong * 8L),
          bytes(scores, count.toLong * 4L),
          nativeParameters
        )
        val seen = mutable.HashSet.empty[Long]
        results = (0 until count).flatMap { slot =>
          val id = ids.getLong(slot.toLong * 8L)
          if (id == -1L) None
          else {
            require(
              id >= 0 && id < rows && seen.add(id),
              s"Knowhere returned an invalid or duplicate index row ID $id"
            )
            require(
              (mask.getByte(id / 8L) & (1 << (id & 7L).toInt)) == 0,
              s"Knowhere returned an excluded index row ID $id"
            )
            val score = scores.getFloat(slot.toLong * 4L)
            require(
              JavaFloat.isFinite(score) && (metric != "L2" || score >= 0),
              "Knowhere returned an invalid index score"
            )
            Some(Hit(id, score.toDouble))
          }
        }.toVector
        attempt += 1
        if (results.size < count && ef.toLong < rows && attempt < 8) {
          ef =
            math.min(rows, math.min(Int.MaxValue.toLong, ef.toLong * 2L)).toInt
        } else {
          require(
            results.size == count,
            s"HNSW search returned ${results.size} of $count visible hits after $attempt attempts"
          )
        }
      } while (results.size < count)
      val sorted =
        if (metric == "L2") results.sortBy(hit => (hit.score, hit.rowOffset))
        else results.sortBy(hit => (-hit.score, hit.rowOffset))
      returnedCount = sorted.size
      succeeded = true
      sorted
    } finally {
      allocated.reverseIterator.foreach(_.close())
      allocator.close()
      logInfo(
        s"Persisted index searched: segment=$segmentId, build=$buildId, metric=$metric, " +
          s"topK=$topK, nativeSearchCalls=$nativeCalls, hits=$returnedCount, succeeded=$succeeded, " +
          s"elapsedMillis=${(System.nanoTime() - started) / 1000000L}"
      )
    }
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      index.close()
    }
  }
}

object PersistedIndexSearch {
  final case class Hit(rowOffset: Long, score: Double)

  private[index] def searchEf(
      topK: Int,
      parameters: Map[String, String]
  ): Int = {
    require(
      parameters != null && parameters.keySet.subsetOf(Set("ef")),
      "HNSW search supports only the ef parameter"
    )
    val ef = parameters
      .get("ef")
      .map { value =>
        require(
          value != null && value.matches("[0-9]+"),
          "HNSW ef must be a positive integer"
        )
        val parsed = Try(value.toInt)
          .getOrElse(
            throw new IllegalArgumentException(
              "HNSW ef exceeds the supported integer range"
            )
          )
        require(parsed > 0, "HNSW ef must be positive")
        parsed
      }
      .getOrElse(math.max(64, topK))
    require(ef >= topK, "HNSW ef must be at least topK")
    ef
  }

  def load(
      index: SegmentIndex,
      dimension: Int,
      nullable: Boolean,
      store: ObjectStore,
      decoder: IndexFileDecoder = MilvusIndexFileDecoder
  ): PersistedIndexSearch = {
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
    new PersistedIndexSearch(
      IndexFileCodec.load(index, dimension, store, decoder),
      metric,
      index.segmentId,
      index.buildId
    )
  }
}
