package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.snapshot.SegmentIndex

/** The Milvus index file format: slices, identity and payload markers
  * (docs/design/architecture/vector-search.html section 2.5).
  */
class IndexFileCodecTest extends AnyFunSuite {
  private val descriptor = SegmentIndex(
    1,
    2,
    3,
    4,
    5,
    6,
    "vector_index",
    Map("index_type" -> "HNSW", "metric_type" -> "COSINE"),
    Vector("build/HNSW"),
    10,
    100,
    1,
    Some(8),
    Some(0)
  )

  private final class Payload(bytes: Array[Byte], id: Long = 3L)
      extends DecodedIndexFile {
    var closed = false
    override val collectionId = 1L
    override val partitionId = 2L
    override val segmentId = id
    override val fieldId = 4L
    override val buildId = 6L
    override val payloadLength: Long = bytes.length.toLong
    override def readPayload(offset: Long, destination: ByteBuffer): Unit =
      destination.put(bytes, offset.toInt, destination.remaining())
    override def close(): Unit = { closed = true }
  }

  private final class CardinalStore(val length: Long) extends ObjectStore {
    val reads = ArrayBuffer.empty[(Long, Long)]
    var shortRead = false
    var validFooter = true
    override def size(key: String): Long = length
    override def readAt(
        key: String,
        offset: Long,
        count: Long,
        fileSize: Long
    ): Array[Byte] = {
      assert(fileSize == length)
      assert(offset >= 0 && count <= 8 * 1024 * 1024)
      reads += ((offset, count))
      val bytes = new Array[Byte](count.toInt - (if (shortRead) 1 else 0))
      if (validFooter && offset + bytes.length == length) {
        ByteBuffer
          .wrap(bytes, bytes.length - 24, 24)
          .order(ByteOrder.LITTLE_ENDIAN)
          .putInt(0x43415244)
          .putInt(1)
          .putLong(length - 32)
          .putLong(16)
      }
      bytes
    }
    override def readAll(key: String): Array[Byte] =
      throw new UnsupportedOperationException
    override def list(key: String, recursive: Boolean): Seq[FileInfo] =
      throw new UnsupportedOperationException
    override def exists(key: String): Boolean =
      throw new UnsupportedOperationException
    override def write(key: String, data: Array[Byte]): Unit =
      throw new UnsupportedOperationException
    override def createDir(key: String, recursive: Boolean): Unit =
      throw new UnsupportedOperationException
    override def delete(key: String): Unit =
      throw new UnsupportedOperationException
    override def close(): Unit = ()
  }

  test("large raw Cardinal objects validate the footer before loading JNI") {
    val store = new CardinalStore(256L * 1024 * 1024 + 37)
    store.validFooter = false
    val index = descriptor.copy(
      filePaths = Vector("build/_mem.index.bin"),
      currentIndexVersion = Some(10)
    )
    val failure = intercept[IllegalArgumentException] {
      IndexFileCodec.load(
        index,
        VectorLayout(VectorElementType.Float32, 128),
        store,
        null
      )
    }
    assert(failure.getMessage.contains("missing Cardinal CARD footer"))
    assert(store.reads.toSeq == Seq((store.length - 24, 24L)))
  }

  test(
    "raw Cardinal payloads above 256 MiB transfer bounded contiguous ranges"
  ) {
    val store = new CardinalStore(256L * 1024 * 1024 + 37)
    val payload =
      new IndexFileCodec.CardinalPayload(store, "build/_mem.index.bin")
    var copied = 0L
    val footer = new Array[Byte](24)
    payload.copyTo { (offset, buffer) =>
      assert(offset == copied)
      assert(buffer.isDirect && buffer.remaining() <= 1024 * 1024)
      copied += buffer.remaining()
      if (copied == payload.length) {
        buffer.position(buffer.limit() - 24)
        buffer.get(footer)
      }
    }
    assert(copied == store.length)
    assert(payload.bytesRead == store.length + 24)
    assert(store.reads.size == 34)
    val parsed = ByteBuffer.wrap(footer).order(ByteOrder.LITTLE_ENDIAN)
    assert(parsed.getInt() == 0x43415244)
    assert(parsed.getInt() == 1)
    assert(parsed.getLong() == store.length - 32)
    assert(parsed.getLong() == 16)
  }

  test("raw Cardinal rejects out of bounds sizes and short reads") {
    Seq(0L, 23L, 1024L * 1024 * 1024 + 1).foreach { length =>
      val store = new CardinalStore(length)
      intercept[IllegalArgumentException] {
        new IndexFileCodec.CardinalPayload(store, "build/_mem.index.bin")
      }
      assert(store.reads.isEmpty)
    }
    val store = new CardinalStore(4096)
    val payload =
      new IndexFileCodec.CardinalPayload(store, "build/_mem.index.bin")
    store.shortRead = true
    val failure = intercept[IllegalArgumentException] {
      payload.copyTo { (_, _) => fail("Short reads must not reach BinarySet") }
    }
    assert(failure.getMessage.contains("size changed"))
    intercept[IllegalArgumentException] {
      new IndexFileCodec.CardinalPayload(store, "build/_mem.index.bin")
    }
  }

  test("Milvus envelope objects retain the 256 MiB limit") {
    val store = new CardinalStore(256L * 1024 * 1024 + 1)
    val failure = intercept[IllegalArgumentException] {
      IndexFileCodec.load(
        descriptor.copy(filePaths = Vector("build/SLICE_META")),
        VectorLayout(VectorElementType.Float32, 128),
        store,
        null
      )
    }
    assert(failure.getMessage.contains("256 MiB"))
    assert(store.reads.isEmpty)
  }

  test(
    "SLICE_META accepts the persisted trailing NUL and exact variable slice lengths"
  ) {
    val bytes =
      """{"meta":[{"name":"HNSW","slice_num":3,"total_len":19}]}""".getBytes(
        UTF_8
      )
    assert(
      IndexFileCodec.parseSlices(bytes :+ 0.toByte) ==
        Vector(IndexFileCodec.Slice("HNSW", 3, 19L))
    )
    val invalid = Seq(
      """{"meta":[{"name":"../HNSW","slice_num":1,"total_len":19}]}""",
      """{"meta":[{"name":"HNSW","slice_num":0,"total_len":19}]}""",
      """{"meta":[{"name":"HNSW","slice_num":1,"total_len":0}]}""",
      """{"meta":[{"name":"HNSW","slice_num":1,"total_len":1073741825}]}""",
      """{"meta":[{"name":"HNSW","slice_num":1,"total_len":1},{"name":"HNSW","slice_num":2,"total_len":2}]}"""
    )
    invalid.foreach { json =>
      intercept[IllegalArgumentException](
        IndexFileCodec.parseSlices(json.getBytes(UTF_8))
      )
    }
  }

  test(
    "payload identity must match every identifier from the pinned snapshot"
  ) {
    val payload = new Payload(Array[Byte](1))
    IndexFileCodec.validateIdentity(descriptor, payload)
    Seq(
      descriptor.copy(collectionId = 9),
      descriptor.copy(partitionId = 9),
      descriptor.copy(segmentId = 9),
      descriptor.copy(fieldId = 9),
      descriptor.copy(buildId = 9)
    ).foreach { wrong =>
      intercept[IllegalArgumentException](
        IndexFileCodec.validateIdentity(wrong, payload)
      )
    }
  }

  test(
    "assembled payload markers select the matching engine in both library builds"
  ) {
    def probe(bytes: Array[Byte]): IndexFileCodec.PayloadFormatProbe = {
      val result = new IndexFileCodec.PayloadFormatProbe(bytes.length)
      // Marker bytes cross arbitrary slice boundaries and buffer positions.
      var offset = 0
      while (offset < bytes.length) {
        val count = math.min(3, bytes.length - offset)
        val buffer = ByteBuffer.allocate(count + 2)
        buffer.position(2)
        buffer.put(bytes, offset, count)
        buffer.flip()
        buffer.position(2)
        result.capture(buffer, offset.toLong)
        offset += count
      }
      result
    }
    // The HNSW family writes several markers for one index type: flat L2 is
    // IHNf, flat cosine IHN9, and scalar quantization IHNs or IHNa.
    Seq("IHNf", "IHN9").foreach { magic =>
      val bytes = magic.getBytes(UTF_8) ++ new Array[Byte](60)
      assert(probe(bytes).engine("HNSW", 8, false) == "HNSW")
      assert(probe(bytes).engine("HNSW", 8, true) == "HNSW_DEPRECATED")
      intercept[IllegalArgumentException](probe(bytes).engine("HNSW", 5, true))
    }
    Seq("IHNs", "IHNa").foreach { magic =>
      val bytes = magic.getBytes(UTF_8) ++ new Array[Byte](60)
      assert(probe(bytes).engine("HNSW_SQ", 8, false) == "HNSW_SQ")
      assert(probe(bytes).engine("HNSW_SQ", 8, true) == "HNSW_SQ")
    }
    // The markers Knowhere actually writes: an IVF index is `Iw*`, a binary
    // index `IB*` whatever its structure, and a flat float index `IxF*` —
    // `IxF2` over squared L2, `IxFI` over inner product. A real FLAT build over
    // the UAT collection is what showed the last of these was missing.
    Seq(
      "IwFl" -> "IVF_FLAT",
      "IwSq" -> "IVF_SQ8",
      "IBwF" -> "BIN_IVF_FLAT",
      "IBxF" -> "BIN_FLAT",
      "IxF2" -> "FLAT",
      "IxFI" -> "FLAT"
    )
      .foreach { case (magic, indexType) =>
        val bytes = magic.getBytes(UTF_8) ++ new Array[Byte](60)
        assert(probe(bytes).engine(indexType, 8, false) == indexType)
      }
    val cardinal = new Array[Byte](64)
    ByteBuffer
      .wrap(cardinal, 40, 24)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(0x43415244)
      .putInt(1)
      .putLong(32)
      .putLong(16)
    assert(probe(cardinal).engine("HNSW", 10, true) == "HNSW")
    intercept[IllegalArgumentException](
      probe(cardinal).engine("HNSW", 10, false)
    )
    intercept[IllegalArgumentException](probe(cardinal).engine("HNSW", 8, true))
    intercept[IllegalArgumentException](
      probe(cardinal).engine("HNSW_SQ", 10, true)
    )
    // A stream of the other family, or of no family, is refused.
    Seq(
      "IwFl" -> "HNSW",
      "IHNf" -> "IVF_FLAT",
      "IxF2" -> "IVF_FLAT",
      "IwFl" -> "FLAT",
      "????" -> "HNSW"
    ).foreach { case (magic, indexType) =>
      intercept[IllegalArgumentException] {
        probe(magic.getBytes(UTF_8) ++ new Array[Byte](60))
          .engine(indexType, 10, true)
      }
    }
  }

  test(
    "Cardinal footer validates native magic, version and metadata ranges before JNI"
  ) {
    val bytes = new Array[Byte](64)
    def footer(
        magic: Int,
        version: Int,
        global: Long,
        tenant: Long
    ): Array[Byte] = {
      ByteBuffer
        .wrap(bytes, 40, 24)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(magic)
        .putInt(version)
        .putLong(global)
        .putLong(tenant)
      bytes.clone()
    }
    IndexFileCodec.validateCardinalFooter(footer(0x43415244, 1, 32, 16))
    Seq(
      footer(0, 1, 32, 16),
      footer(0x43415244, 2, 32, 16),
      footer(0x43415244, 1, 32, -1),
      footer(0x43415244, 1, 40, 16),
      footer(0x43415244, 1, 16, 32),
      Array.emptyByteArray
    ).foreach { broken =>
      intercept[IllegalArgumentException](
        IndexFileCodec.validateCardinalFooter(broken)
      )
    }
  }
}
