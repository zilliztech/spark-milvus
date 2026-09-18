package com.zilliz.milvus.storage.read.exec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.{DecodedIndexFile, IndexFileDecoder}
import com.zilliz.milvus.storage.io.{FailingObjectStore, LocalObjectStore}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout
}

/** What the Milvus format side accepts before it opens an index
  * (docs/design/architecture/vector-search.html sections 2.4 and 2.5).
  */
class SegmentIndexHandleTest extends AnyFunSuite with Matchers {
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

  private val task = SegmentReadTask(
    3L,
    2L,
    SegmentLayout.Manifest("must-not-open", 1L),
    Array.emptyByteArray,
    Map.empty,
    indexes = SegmentIndexes.Unindexed,
    snapshotRows = Some(10L)
  )

  private val available = descriptor.copy(segmentId = 3L, partitionId = 2L)

  test("a segment the snapshot says has no index needs allowUnindexed") {
    val failure = the[IllegalArgumentException] thrownBy SegmentIndexHandle
      .select(task, 4L, "COSINE", allowUnindexed = false)

    failure.getMessage should include("No persisted index for segment 3")
    SegmentIndexHandle.select(
      task,
      4L,
      "COSINE",
      allowUnindexed = true
    ) shouldBe None
  }

  test("a segment whose index the snapshot cannot describe is refused") {
    the[IllegalArgumentException] thrownBy SegmentIndexHandle.select(
      task.copy(indexes = SegmentIndexes.Unknown),
      4L,
      "COSINE",
      allowUnindexed = true
    )
  }

  test("index search needs a pinned manifest version") {
    the[IllegalArgumentException] thrownBy SegmentIndexHandle.select(
      task.copy(layout = SegmentLayout.Manifest("absent", -1L)),
      4L,
      "COSINE",
      allowUnindexed = true
    )
  }

  test("an index that differs from the pinned segment is refused") {
    Seq(
      Vector(available, available.copy(buildId = 7L)),
      Vector(available.copy(rowCount = 9L)),
      Vector(available.copy(segmentId = 9L)),
      Vector(available.copy(partitionId = 9L)),
      Vector(
        available.copy(parameters =
          available.parameters.updated("metric_type", "IP")
        )
      )
    ).foreach { indexes =>
      the[IllegalArgumentException] thrownBy SegmentIndexHandle.select(
        task.copy(indexes = SegmentIndexes.Available(indexes)),
        4L,
        "COSINE",
        allowUnindexed = true
      )
    }
  }

  test("the index of the pinned segment is the one selected") {
    SegmentIndexHandle.select(
      task.copy(indexes = SegmentIndexes.Available(Vector(available))),
      4L,
      "COSINE",
      allowUnindexed = false
    ) shouldBe Some(available)
  }

  test("planning names every segment that cannot serve the search") {
    val failure = the[IllegalArgumentException] thrownBy SegmentIndexHandle
      .check(
        Seq(
          task.copy(segmentId = 3L),
          task.copy(segmentId = 4L),
          task.copy(
            segmentId = 5L,
            indexes =
              SegmentIndexes.Available(Vector(available.copy(segmentId = 5L)))
          )
        ),
        4L,
        "COSINE",
        allowUnindexed = false
      )

    failure.getMessage should include("2 of 3 segments")
    failure.getMessage should include("segment 3")
    failure.getMessage should include("segment 4")
    SegmentIndexHandle.check(Seq.empty, 4L, "COSINE", allowUnindexed = false)
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
