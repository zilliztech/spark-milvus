package com.zilliz.milvus.storage.codec

import java.nio.ByteBuffer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.BinlogFixture

class MilvusIndexFileDecoderTest extends AnyFunSuite with Matchers {
  private def payload(file: DecodedIndexFile): Vector[Byte] = {
    val bytes = ByteBuffer.allocate(file.payloadLength.toInt)
    file.readPayload(0, bytes)
    bytes.position() shouldBe file.payloadLength.toInt
    bytes.array().toVector
  }

  test(
    "raw Milvus index payload preserves identity, ranges and close ownership"
  ) {
    val bytes = Array[Byte](0, -1, 127, -128, 1, 2)
    val file = MilvusIndexFileDecoder.decode(BinlogFixture.encode(bytes))
    file.collectionId shouldBe BinlogFixture.CollectionId
    file.partitionId shouldBe BinlogFixture.PartitionId
    file.segmentId shouldBe BinlogFixture.SegmentId
    file.fieldId shouldBe BinlogFixture.FieldId
    file.buildId shouldBe BinlogFixture.BuildId
    payload(file) shouldBe bytes.toVector
    val part = ByteBuffer.allocate(4)
    part.position(1)
    file.readPayload(2, part)
    part.array().toVector shouldBe Vector[Byte](0, 127, -128, 1)
    intercept[IllegalArgumentException](
      file.readPayload(-1, ByteBuffer.allocate(1))
    )
    intercept[IllegalArgumentException](
      file.readPayload(6, ByteBuffer.allocate(1))
    )
    file.close()
    file.close()
    intercept[IllegalArgumentException](
      file.readPayload(0, ByteBuffer.allocate(1))
    )
  }

  test("INT8 Parquet payload reassembles signed bytes from every row group") {
    val file = MilvusIndexFileDecoder.decode(
      BinlogFixture.encode(BinlogFixture.parquet("int8"), dataType = 2)
    )
    try payload(file) shouldBe Vector[Byte](-128, -1, 0, 1, 127)
    finally file.close()
  }

  test("legacy STRING Parquet payload preserves arbitrary binary bytes") {
    val file = MilvusIndexFileDecoder.decode(
      BinlogFixture.encode(BinlogFixture.parquet("string"), dataType = 20)
    )
    try payload(file) shouldBe Vector[Byte](0, -1, 127, -128, 1, 2)
    finally file.close()
  }

  test(
    "invalid Parquet values, missing identity and unsupported envelopes fail"
  ) {
    Seq(
      BinlogFixture.encode(
        BinlogFixture.parquet("int8-out-of-range"),
        dataType = 2
      ),
      BinlogFixture.encode(
        BinlogFixture.parquet("string-multiple"),
        dataType = 20
      ),
      BinlogFixture.encode(BinlogFixture.parquet("null"), dataType = 20),
      BinlogFixture.encode(BinlogFixture.parquet("string"), dataType = 2),
      BinlogFixture.encode(Array[Byte](1), extras = "{}"),
      BinlogFixture.encode(
        Array[Byte](1),
        extras =
          s"""{"indexBuildID":"${BinlogFixture.BuildId}","nullable":"bad"}"""
      ),
      BinlogFixture.encode(Array[Byte](1), eventType = 2),
      BinlogFixture.encode(Array.emptyByteArray)
    ).foreach { bytes =>
      intercept[IllegalArgumentException](MilvusIndexFileDecoder.decode(bytes))
    }
    // A nullable column's index says so and names the rows it holds in its
    // valid_data payload, which the loader maps back to segment rows.
    val nullable = MilvusIndexFileDecoder.decode(
      BinlogFixture.encode(
        Array[Byte](1),
        extras =
          s"""{"indexBuildID":"${BinlogFixture.BuildId}","nullable":true}"""
      )
    )
    try assert(nullable.payloadLength == 1L)
    finally nullable.close()
    intercept[UnsupportedOperationException] {
      MilvusIndexFileDecoder.decode(
        BinlogFixture.encode(Array[Byte](1), dataType = 101)
      )
    }
  }
}
