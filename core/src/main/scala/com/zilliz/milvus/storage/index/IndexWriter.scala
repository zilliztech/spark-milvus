package com.zilliz.milvus.storage.index

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Locale

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorIndex
import com.zilliz.milvus.storage.read.exec.{SegmentIndexHandle, SegmentVectors}
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.Logging

/** Builds one segment's vector index and hands over the payloads Knowhere
  * serialized.
  *
  * This is the computation half of building: the vectors arrive as one buffer
  * the caller filled, Knowhere builds and serializes, and what comes back is a
  * set of named payloads. Encoding them into Milvus's index files and writing
  * them belongs to the Milvus `TableFormat` side
  * (docs/design/architecture/vector-search.html section 2.7).
  *
  * What can be built is what section 2.4's loader can read back: the same index
  * types, the same metrics per element type. An index this connector could not
  * load again would be delivered to nobody.
  */
object IndexWriter extends Logging {

  private val mapper = new ObjectMapper()

  /** A built index, its payloads and what a segment record has to say about it.
    */
  final class Built private[index] (
      private[storage] val payloads: NativeVectorIndex.Built,
      val indexType: String,
      val metric: String,
      val indexVersion: Int,
      val rows: Long,
      val dimension: Int,
      val validData: Option[Array[Byte]]
  ) extends AutoCloseable {

    /** The payload names, which are the names the index files take. */
    def names: Seq[String] = payloads.names().toSeq

    def length(name: String): Long = payloads.length(name)

    def read(name: String, offset: Long, destination: ByteBuffer): Unit =
      payloads.read(name, offset, destination)

    /** The bytes the whole index serialized to. */
    def bytes: Long = names.map(length).sum

    override def close(): Unit = payloads.close()
  }

  /** One segment's vectors gathered into the single buffer a build takes.
    *
    * `rows` counts the rows that have a value; `validData` is the bitmap that
    * says which segment rows those were, and is absent when every row has one.
    */
  final class Assembled private[index] (
      val buffer: ArrowBuf,
      val rows: Long,
      val segmentRows: Long,
      val validData: Option[Array[Byte]]
  ) extends AutoCloseable {
    def bytes: ByteBuffer =
      buffer
        .nioBuffer(0, buffer.capacity().toInt)
        .order(ByteOrder.nativeOrder())

    override def close(): Unit = buffer.close()
  }

  /** Copies every batch of a segment into one buffer, leaving out the rows
    * without a value and recording them.
    *
    * Milvus indexes a segment as it was written, so deleted rows are in the
    * index and are excluded when a search runs, not here. A row whose vector is
    * null has nothing to index, so it is skipped and its place is recorded in
    * the `valid_data` bitmap the index files carry (section 2.7).
    */
  def assemble(
      vectors: SegmentVectors,
      layout: VectorLayout,
      segmentRows: Long,
      allocator: BufferAllocator
  ): Assembled = {
    require(
      segmentRows > 0 && segmentRows <= Int.MaxValue,
      s"A segment of $segmentRows rows is outside what one build takes"
    )
    val bytes = Math.multiplyExact(segmentRows, layout.rowBytes.toLong)
    require(
      bytes <= Int.MaxValue,
      s"A segment of $bytes vector bytes exceeds one build buffer"
    )
    val target = allocator.buffer(bytes)
    var present = new java.util.BitSet(segmentRows.toInt)
    var rows = 0L
    var seen = 0L
    try {
      var next = vectors.next()
      while (next.nonEmpty) {
        val batch = next.get
        try {
          var row = 0
          while (row < batch.rows) {
            if (!batch.excluded.get(row)) {
              target.setBytes(
                rows * layout.rowBytes,
                batch.base.buffer,
                row * layout.rowBytes,
                layout.rowBytes
              )
              present.set((seen + row).toInt)
              rows += 1L
            }
            row += 1
          }
          seen += batch.rows.toLong
        } finally batch.close()
        next = vectors.next()
      }
      require(
        seen == segmentRows,
        s"The segment gave $seen of the $segmentRows rows it declared"
      )
      require(rows > 0, "Every vector of the segment is null; nothing to index")
      val validData =
        if (rows == segmentRows) None else Some(bitmapOf(present, segmentRows))
      logInfo(
        s"Segment vectors assembled: rows=$rows of $segmentRows, bytes=${rows * layout.rowBytes}"
      )
      new Assembled(target, rows, segmentRows, validData)
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  /** The bitmap layout the index files use: bit `i` of byte `i / 8`, least
    * significant bit first.
    */
  private[index] def bitmapOf(
      present: java.util.BitSet,
      rows: Long
  ): Array[Byte] = {
    val bytes = new Array[Byte](((rows + 7L) / 8L).toInt)
    var row = present.nextSetBit(0)
    while (row >= 0 && row < rows) {
      bytes(row / 8) = (bytes(row / 8) | (1 << (row % 8))).toByte
      row = present.nextSetBit(row + 1)
    }
    bytes
  }

  /** Builds over `rows` vectors lying end to end in `vectors`.
    *
    * A nullable column is indexed over the rows that have a value, so the
    * caller compacts those rows into the buffer and passes the `valid_data`
    * bitmap that says which segment rows they were.
    */
  def build(
      vectors: ByteBuffer,
      rows: Long,
      layout: VectorLayout,
      indexType: String,
      metric: String,
      indexVersion: Int,
      parameters: Map[String, String] = Map.empty,
      validData: Option[Array[Byte]] = None
  ): Built = {
    val name = indexType.toUpperCase(Locale.ROOT)
    val distance = metric.toUpperCase(Locale.ROOT)
    require(
      SegmentIndexHandle.HnswFamily.contains(name) ||
        SegmentIndexHandle.IvfFamily.contains(name) ||
        SegmentIndexHandle.FlatFamily.contains(name),
      s"This connector builds the index types it loads, not $name"
    )
    val metrics = SegmentIndexHandle.metricsOf(layout)
    require(
      metrics.contains(distance),
      s"A ${layout.elementType} index takes ${metrics.toSeq.sorted
          .mkString(" or ")}, not $distance"
    )
    require(rows > 0, s"An index is built over $rows rows")
    require(
      vectors != null && vectors.capacity().toLong >= rows * layout.rowBytes,
      s"The buffer holds ${if (vectors == null) 0
        else vectors.capacity()} bytes; $rows rows of ${layout.dimension} dimensions need ${rows * layout.rowBytes}"
    )
    require(
      parameters.forall { case (key, value) =>
        key != null && key.nonEmpty && value != null && value.nonEmpty
      },
      "Index build parameters must be named and nonempty"
    )
    val built = NativeVectorIndex.build(
      name,
      layout.dtype,
      indexVersion,
      vectors,
      rows,
      layout.dimension,
      buildParameters(distance, layout, parameters)
    )
    try {
      val result = new Built(
        built,
        name,
        distance,
        indexVersion,
        rows,
        layout.dimension,
        validData
      )
      logInfo(
        s"Index built: type=$name, metric=$distance, rows=$rows, dimension=${layout.dimension}, " +
          s"version=$indexVersion, payloads=${result.names
              .mkString(",")}, bytes=${result.bytes}"
      )
      result
    } catch {
      case failure: Throwable =>
        built.close()
        throw failure
    }
  }

  /** The JSON Knowhere takes: the metric and the dimension it always needs,
    * plus whatever the caller tuned.
    */
  private[index] def buildParameters(
      metric: String,
      layout: VectorLayout,
      parameters: Map[String, String]
  ): String = {
    val node = mapper.createObjectNode()
    node.put("metric_type", metric)
    node.put("dim", layout.dimension)
    parameters.toSeq.sortBy(_._1).foreach { case (key, value) =>
      require(
        key != "metric_type" && key != "dim",
        s"$key is decided by the column and the search, not by a build parameter"
      )
      // Knowhere reads numbers as numbers and everything else as a string.
      if (value.matches("-?[0-9]+")) node.put(key, value.toLong)
      else if (value.matches("-?[0-9]*\\.[0-9]+")) node.put(key, value.toDouble)
      else if (value == "true" || value == "false")
        node.put(key, value.toBoolean)
      else node.put(key, value)
    }
    mapper.writeValueAsString(node)
  }
}
