package com.zilliz.milvus.storage.codec

import java.io.EOFException
import java.nio.{ByteBuffer, ByteOrder}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BinlogCodecTest extends AnyFunSuite with Matchers {
  test("the descriptor preserves 64-bit identity and the event payload") {
    val payload = Array[Byte](0, -1, 127, -128)
    val file = BinlogCodec.parse(BinlogFixture.encode(payload), "index.bin")
    file.collectionId shouldBe BinlogFixture.CollectionId
    file.partitionId shouldBe BinlogFixture.PartitionId
    file.segmentId shouldBe BinlogFixture.SegmentId
    file.fieldId shouldBe BinlogFixture.FieldId
    file.extras
      .get("indexBuildID")
      .asText() shouldBe BinlogFixture.BuildId.toString
    file.events.map(_.kind) shouldBe Vector(7)
    file.events.head.payload.toSeq shouldBe payload.toSeq
  }

  test("malformed headers and truncated events fail before yielding payloads") {
    val valid = BinlogFixture.encode(Array[Byte](1, 2, 3))
    val descriptorEnd =
      ByteBuffer.wrap(valid).order(ByteOrder.LITTLE_ENDIAN).getInt(17)
    val invalid = Seq(
      BinlogFixture.intAt(valid, 0, 0),
      BinlogFixture.intAt(valid, 13, 16),
      BinlogFixture.intAt(valid, 17, descriptorEnd + 1),
      BinlogFixture.intAt(valid, 81, -1),
      BinlogFixture.intAt(valid, descriptorEnd + 9, Int.MaxValue),
      valid.dropRight(1)
    )
    invalid.foreach { bytes =>
      val error = intercept[IllegalArgumentException](
        BinlogCodec.parse(bytes, "broken.bin")
      )
      error.getMessage should include("broken.bin")
    }
    intercept[EOFException](BinlogCodec.parse(valid.take(3), "short.bin"))
    val badType = valid.clone()
    badType(descriptorEnd + 8) = 8
    intercept[IllegalArgumentException](BinlogCodec.parse(badType, "event.bin"))
    val badPost = valid.clone()
    badPost(80) = 15
    intercept[IllegalArgumentException](BinlogCodec.parse(badPost, "post.bin"))
  }

  test("an event whose next position Milvus left unset (-1) parses") {
    // Milvus's serde writers (internal/storage/serde_delta.go and
    // serde_events.go) never set NextPosition, so newEventHeader's -1 reaches
    // the file; every L0 delete binlog on the UAT instance has it.
    val valid = BinlogFixture.encode(Array[Byte](1, 2, 3), eventType = 3)
    val descriptorEnd =
      ByteBuffer.wrap(valid).order(ByteOrder.LITTLE_ENDIAN).getInt(17)
    val unset = BinlogFixture.intAt(valid, descriptorEnd + 13, -1)
    val file = BinlogCodec.parse(unset, "delta.bin")
    file.events.map(_.kind) shouldBe Vector(3)
    file.events.head.payload.toSeq shouldBe Seq[Byte](1, 2, 3)
  }

  test("descriptor extras must be an object and encryption is rejected") {
    Seq("[]", "null", " ").foreach { extras =>
      intercept[IllegalArgumentException] {
        BinlogCodec.parse(
          BinlogFixture.encode(Array[Byte](1), extras = extras),
          "extras.bin"
        )
      }
    }
    Seq(
      """{"edek":"ciphertext"}""",
      """{"edek":""}""",
      """{"encryption_zone":1}"""
    ).foreach { extras =>
      val error = intercept[UnsupportedOperationException] {
        BinlogCodec.parse(
          BinlogFixture.encode(Array[Byte](1), extras = extras),
          "encrypted.bin"
        )
      }
      error.getMessage should include("Encrypted")
    }
  }

  test(
    "unnamed INT8 Parquet columns preserve signed values across row groups"
  ) {
    val values = Vector.newBuilder[Int]
    BinlogCodec.forEachRow(BinlogFixture.parquet("int8")) { row =>
      row.columnCount shouldBe 1
      row.isNull(0) shouldBe false
      values += row.getInt(0)
    }
    values.result() shouldBe Vector(-128, -1, 0, 1, 127)
  }

  test("Parquet magic and footer bounds are checked before decoding") {
    val bytes = BinlogFixture.parquet("int8")
    val invalid = Seq(
      bytes.take(7),
      bytes.updated(0, 0.toByte),
      bytes.updated(bytes.length - 1, 0.toByte),
      BinlogFixture.intAt(bytes, bytes.length - 8, -1),
      BinlogFixture.intAt(bytes, bytes.length - 8, Int.MaxValue)
    )
    invalid.foreach { payload =>
      intercept[IllegalArgumentException](
        BinlogCodec.forEachRow(payload)(_ =>
          fail("must not decode a corrupt payload")
        )
      )
    }
  }
}
