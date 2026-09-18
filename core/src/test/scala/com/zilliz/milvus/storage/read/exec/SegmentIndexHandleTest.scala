package com.zilliz.milvus.storage.read.exec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.codec.{DecodedIndexFile, IndexFileDecoder}
import com.zilliz.milvus.storage.io.{FailingObjectStore, LocalObjectStore}
import com.zilliz.milvus.storage.snapshot.SegmentIndex

/** What the Milvus format side accepts before it opens an index
  * (docs/design/architecture/vector-search.html sections 2.4 and 2.5).
  */
class SegmentIndexHandleTest extends AnyFunSuite {
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
        SegmentIndexHandle.open(index, 4, false, store)
      )
    }
    intercept[IllegalArgumentException](
      SegmentIndexHandle.open(descriptor, 4, true, store)
    )
    intercept[IllegalArgumentException](
      SegmentIndexHandle.open(descriptor, 0, false, store)
    )
  }

  test("unknown payload names are rejected without being renamed to HNSW") {
    val store = new FailingObjectStore(
      new AssertionError("Unsupported layout must not be read")
    )
    val error = intercept[IllegalArgumentException] {
      SegmentIndexHandle.open(
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

  test("an index format version below the engine's minimum is refused") {
    val store = new FailingObjectStore(
      new AssertionError("Unsupported version must not be read")
    )
    val error = intercept[IllegalArgumentException] {
      SegmentIndexHandle.open(
        descriptor.copy(filePaths = Vector("build/_mem.index.bin")),
        4,
        false,
        store
      )
    }
    assert(error.getMessage.contains("version 9 or later"))
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
        SegmentIndexHandle.open(missing, 4, false, store, decoder)
      }
      assert(error.getMessage.contains("missing or overlap"))
      assert(payload.closed)
      val excessive =
        """{"meta":[{"name":"HNSW","slice_num":2,"total_len":19},{"name":"extra","slice_num":2,"total_len":19}]}"""
          .getBytes(UTF_8)
      store.write("SLICE_META", excessive)
      val tooMany = intercept[IllegalArgumentException] {
        SegmentIndexHandle.open(missing, 4, false, store, decoder)
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
        SegmentIndexHandle.open(missing, 4, false, store, mismatch)
      }
      assert(payload.closed)
    } finally {
      store.close()
      Files.deleteIfExists(directory.resolve("SLICE_META"))
      Files.delete(directory)
    }
  }
}
