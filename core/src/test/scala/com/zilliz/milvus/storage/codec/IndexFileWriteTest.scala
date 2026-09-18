package com.zilliz.milvus.storage.codec

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.ByteBuffer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.LocalObjectStore

/** The index objects a build writes, read back by the decoder that loads them
  * (docs/design/architecture/vector-search.html sections 2.5 and 2.7).
  */
class IndexFileWriteTest extends AnyFunSuite with Matchers {

  private val target = IndexFileCodec.IndexTarget(
    collectionId = 10L,
    partitionId = 20L,
    segmentId = 30L,
    fieldId = 101L,
    buildId = 900L,
    indexVersion = 1L,
    storePathVersion = 0,
    nullable = false,
    rootPath = "files"
  )

  private def payload(name: String, bytes: Array[Byte]) =
    (name, bytes.length.toLong)

  private def reader(payloads: Map[String, Array[Byte]])(
      name: String,
      offset: Long,
      destination: ByteBuffer
  ): Unit = {
    val source = payloads(name)
    val length = destination.remaining()
    destination.put(source, offset.toInt, length)
  }

  test("an index prefix follows the store path version") {
    IndexFileCodec.prefixOf(target) shouldBe "files/index_files/900/1/20/30"
    IndexFileCodec.prefixOf(
      target.copy(storePathVersion = 1)
    ) shouldBe "files/index_v1/10/20/30/900/1"
    IndexFileCodec.prefixOf(
      target.copy(rootPath = "")
    ) shouldBe "index_files/900/1/20/30"
  }

  test("a payload comes back from the object it was written to") {
    val directory = Files.createTempDirectory("index-write-test-")
    val store = new LocalObjectStore(directory.toString)
    val bytes = Array.tabulate(1000)(index => (index % 251).toByte)
    try {
      val written = IndexFileCodec.write(
        Seq(payload("HNSW", bytes)),
        reader(Map("HNSW" -> bytes)),
        target,
        store
      )

      written.map(_.key) shouldBe Seq("files/index_files/900/1/20/30/HNSW")
      written.head.bytes should be > 1000L

      val decoded =
        MilvusIndexFileDecoder.decode(store.readAll(written.head.key))
      try {
        decoded.collectionId shouldBe 10L
        decoded.partitionId shouldBe 20L
        decoded.segmentId shouldBe 30L
        decoded.fieldId shouldBe 101L
        decoded.buildId shouldBe 900L
        decoded.payloadLength shouldBe 1000L
        val back = new Array[Byte](1000)
        decoded.readPayload(0, ByteBuffer.wrap(back))
        back shouldBe bytes
      } finally decoded.close()
    } finally store.close()
  }

  test("a payload longer than a slice is split and SLICE_META says how") {
    val directory = Files.createTempDirectory("index-slice-test-")
    val store = new LocalObjectStore(directory.toString)
    val bytes = Array.tabulate(2500)(index => (index % 97).toByte)
    try {
      val written = IndexFileCodec.write(
        Seq(payload("HNSW", bytes)),
        reader(Map("HNSW" -> bytes)),
        target,
        store,
        sliceBytes = 1000L
      )

      written.map(_.key.split("/").last) shouldBe Seq(
        "HNSW_0",
        "HNSW_1",
        "HNSW_2",
        "SLICE_META"
      )

      val meta = MilvusIndexFileDecoder.decode(
        store.readAll("files/index_files/900/1/20/30/SLICE_META")
      )
      val json =
        try {
          val buffer = new Array[Byte](meta.payloadLength.toInt)
          meta.readPayload(0, ByteBuffer.wrap(buffer))
          new String(buffer, UTF_8)
        } finally meta.close()

      IndexFileCodec.parseSlices(json.getBytes(UTF_8)) shouldBe Vector(
        IndexFileCodec.Slice("HNSW", 3, 2500L)
      )

      // The three objects put the payload back together byte for byte.
      val assembled = Seq("HNSW_0", "HNSW_1", "HNSW_2").flatMap { name =>
        val slice = MilvusIndexFileDecoder.decode(
          store.readAll(s"files/index_files/900/1/20/30/$name")
        )
        try {
          val buffer = new Array[Byte](slice.payloadLength.toInt)
          slice.readPayload(0, ByteBuffer.wrap(buffer))
          buffer.toSeq
        } finally slice.close()
      }
      assembled shouldBe bytes.toSeq
    } finally store.close()
  }

  test("a nullable index says so, and its bitmap is an object of its own") {
    val directory = Files.createTempDirectory("index-nullable-test-")
    val store = new LocalObjectStore(directory.toString)
    val index = Array.tabulate(64)(_.toByte)
    val bitmap = Array[Byte](0x0f, 0x33)
    try {
      val written = IndexFileCodec.write(
        Seq(payload("HNSW", index), payload("valid_data", bitmap)),
        reader(Map("HNSW" -> index, "valid_data" -> bitmap)),
        target.copy(nullable = true),
        store
      )

      written.map(_.key.split("/").last) shouldBe Seq("HNSW", "valid_data")
      val decoded = MilvusIndexFileDecoder.decode(
        store.readAll("files/index_files/900/1/20/30/valid_data")
      )
      try {
        val back = new Array[Byte](2)
        decoded.readPayload(0, ByteBuffer.wrap(back))
        back shouldBe bitmap
      } finally decoded.close()
    } finally store.close()
  }

  test("an empty or repeated payload is refused before anything is written") {
    val store = new LocalObjectStore(
      Files.createTempDirectory("index-refuse-test-").toString
    )
    try {
      the[IllegalArgumentException] thrownBy IndexFileCodec.write(
        Seq.empty,
        reader(Map.empty),
        target,
        store
      )
      the[IllegalArgumentException] thrownBy IndexFileCodec.write(
        Seq(("HNSW", 0L)),
        reader(Map("HNSW" -> Array.emptyByteArray)),
        target,
        store
      )
      the[IllegalArgumentException] thrownBy IndexFileCodec.write(
        Seq(("HNSW", 4L), ("HNSW", 4L)),
        reader(Map("HNSW" -> Array[Byte](1, 2, 3, 4))),
        target,
        store
      )
    } finally store.close()
  }
}
