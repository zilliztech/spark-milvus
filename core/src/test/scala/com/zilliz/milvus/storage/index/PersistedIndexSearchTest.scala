package com.zilliz.milvus.storage.index

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.{FailingObjectStore, LocalObjectStore}
import com.zilliz.milvus.storage.snapshot.SegmentIndex

class PersistedIndexSearchTest extends AnyFunSuite {
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

  test("unsupported search parameters cannot silently alter the query") {
    assert(PersistedIndexSearch.searchEf(5, Map.empty) == 64)
    assert(PersistedIndexSearch.searchEf(100, Map.empty) == 100)
    assert(PersistedIndexSearch.searchEf(5, Map("ef" -> "128")) == 128)
    Seq(
      Map("nprobe" -> "8"),
      Map("metric_type" -> "IP"),
      Map("ef" -> "0"),
      Map("ef" -> "4"),
      Map("ef" -> "-1"),
      Map("ef" -> "1.5"),
      Map("ef" -> "2147483648"),
      Map("ef" -> null)
    ).foreach { parameters =>
      intercept[IllegalArgumentException](
        PersistedIndexSearch.searchEf(5, parameters)
      )
    }
  }

  test(
    "incomplete and unsupported metadata fails before opening native libraries or files"
  ) {
    val store =
      new FailingObjectStore(new AssertionError("Unexpected object read"))
    val unsupported = Seq(
      descriptor.copy(currentIndexVersion = None),
      descriptor.copy(parameters =
        descriptor.parameters.updated("index_type", "IVF_FLAT")
      ),
      descriptor.copy(parameters =
        descriptor.parameters.updated("metric_type", "HAMMING")
      ),
      descriptor.copy(parameters = descriptor.parameters - "metric_type"),
      descriptor.copy(rowCount = 0)
    )
    unsupported.foreach { index =>
      intercept[IllegalArgumentException](
        PersistedIndexSearch.load(index, 4, false, store)
      )
    }
    intercept[IllegalArgumentException](
      PersistedIndexSearch.load(descriptor, 4, true, store)
    )
    intercept[IllegalArgumentException](
      PersistedIndexSearch.load(descriptor, 0, false, store)
    )
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

  test("unknown payload names are rejected without being renamed to HNSW") {
    val store = new FailingObjectStore(
      new AssertionError("Unsupported layout must not be read")
    )
    val error = intercept[IllegalArgumentException] {
      PersistedIndexSearch.load(
        descriptor.copy(filePaths = Vector("build/unknown.index.bin")),
        4,
        false,
        store
      )
    }
    assert(
      error.getMessage.contains("Unsupported persisted HNSW payload layout")
    )
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
    val store = new FailingObjectStore(
      new AssertionError("Unsupported version must not be read")
    )
    val error = intercept[IllegalArgumentException] {
      PersistedIndexSearch.load(
        descriptor.copy(filePaths = Vector("build/_mem.index.bin")),
        4,
        false,
        store
      )
    }
    assert(error.getMessage.contains("version 9 or later"))
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
    Seq("IHNf", "IHN9").foreach { magic =>
      val bytes = magic.getBytes(UTF_8) ++ new Array[Byte](60)
      assert(probe(bytes).engineType(8, false) == "HNSW")
      assert(probe(bytes).engineType(8, true) == "HNSW_DEPRECATED")
      intercept[IllegalArgumentException](probe(bytes).engineType(5, true))
    }
    val cardinal = new Array[Byte](64)
    ByteBuffer
      .wrap(cardinal, 40, 24)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(0x43415244)
      .putInt(1)
      .putLong(32)
      .putLong(16)
    assert(probe(cardinal).engineType(10, true) == "HNSW")
    intercept[IllegalArgumentException](probe(cardinal).engineType(10, false))
    intercept[IllegalArgumentException](probe(cardinal).engineType(8, true))
    Seq("IHNp", "IHMV", "????").foreach { unsupported =>
      intercept[IllegalArgumentException] {
        probe(unsupported.getBytes(UTF_8) ++ new Array[Byte](60))
          .engineType(10, true)
      }
    }
  }

  test(
    "missing slices and mismatched identities close decoded metadata before failing"
  ) {
    val directory = Files.createTempDirectory("index-codec-test-")
    val store = new LocalObjectStore(directory.toString)
    try {
      val meta =
        """{"meta":[{"name":"HNSW","slice_num":2,"total_len":19}]}""".getBytes(
          UTF_8
        )
      store.write("SLICE_META", meta)
      var payload: Payload = null
      val decoder = new IndexFileDecoder {
        override def decode(bytes: Array[Byte]): DecodedIndexFile = {
          payload = new Payload(bytes)
          payload
        }
      }
      val missing = descriptor.copy(filePaths = Vector("SLICE_META", "HNSW_0"))
      val error = intercept[IllegalArgumentException] {
        PersistedIndexSearch.load(missing, 4, false, store, decoder)
      }
      assert(error.getMessage.contains("missing or overlap"))
      assert(payload.closed)
      val excessive =
        """{"meta":[{"name":"HNSW","slice_num":2,"total_len":19},{"name":"extra","slice_num":2,"total_len":19}]}"""
          .getBytes(UTF_8)
      store.write("SLICE_META", excessive)
      val tooMany = intercept[IllegalArgumentException] {
        PersistedIndexSearch.load(missing, 4, false, store, decoder)
      }
      assert(tooMany.getMessage.contains("more slices than the snapshot"))
      assert(payload.closed)
      val mismatch = new IndexFileDecoder {
        override def decode(bytes: Array[Byte]): DecodedIndexFile = {
          payload = new Payload(bytes, 99)
          payload
        }
      }
      intercept[IllegalArgumentException] {
        PersistedIndexSearch.load(missing, 4, false, store, mismatch)
      }
      assert(payload.closed)
    } finally {
      store.close()
      Files.deleteIfExists(directory.resolve("SLICE_META"))
      Files.delete(directory)
    }
  }
}
