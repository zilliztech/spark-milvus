package com.zilliz.milvus.storage.manifest

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.jdk.CollectionConverters._

import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{
  GenericData,
  GenericDatumReader,
  GenericDatumWriter,
  GenericRecord
}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Synthetic full-schema records exercise Milvus v3.0.4's OCF framing. */
class SnapshotSegmentContainerTest extends AnyFunSuite with Matchers {
  private def record(): GenericRecord = {
    val schema = SnapshotSegmentSchema.schemaFor(4)
    val bytes = SnapshotSegmentFixture.encode(
      indexes = Vector(SnapshotSegmentFixture.index()),
      channelName = "source-channel",
      statsLogs = Seq(
        FieldBinlogEntry(100L, Seq(BinlogEntry(9L, "files/stats_log/9", 2L)))
      )
    )
    new GenericDatumReader[GenericRecord](schema).read(
      null,
      DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null)
    )
  }

  private def container(
      value: GenericRecord,
      copies: Int = 1,
      codec: CodecFactory = CodecFactory.nullCodec()
  ): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writer = new DataFileWriter[GenericRecord](
      new GenericDatumWriter[GenericRecord](value.getSchema)
    )
    writer.setCodec(codec)
    writer.create(value.getSchema, out)
    try {
      (0 until copies).foreach(_ => writer.append(value))
    } finally writer.close()
    out.toByteArray
  }

  private def changed(
      value: GenericRecord,
      fields: Seq[Schema.Field],
      values: Map[String, AnyRef] = Map.empty
  ): GenericRecord = {
    val original = value.getSchema
    val schema = Schema.createRecord(
      original.getName,
      original.getDoc,
      original.getNamespace,
      false
    )
    schema.setFields(
      fields
        .map(f => new Schema.Field(f.name, f.schema, f.doc, f.defaultVal))
        .asJava
    )
    val result = new GenericData.Record(schema)
    fields.foreach(f =>
      result.put(f.name, values.getOrElse(f.name, value.get(f.name)))
    )
    result
  }

  private def parse(bytes: Array[Byte]): SnapshotSegmentEntry =
    SnapshotSegmentReader.parse(bytes, 6).fold(throw _, identity)

  private def error(bytes: Array[Byte], version: Int = 6): String =
    SnapshotSegmentReader.parse(bytes, version).swap.toOption.get.getMessage

  test(
    "V6 reads the embedded V4 schema and preserves files and exact index IDs"
  ) {
    val value = record()
    val entry = parse(container(value))
    entry.segmentId shouldBe 30L
    entry.storageVersion shouldBe 3L
    entry.numOfRows shouldBe 2L
    entry.channelName shouldBe "source-channel"
    entry.indexFiles shouldBe Some(Vector(SnapshotSegmentFixture.index()))
    entry.statsLogFiles shouldBe Seq(
      FieldBinlogEntry(100L, Seq(BinlogEntry(9L, "files/stats_log/9", 2L)))
    )
  }

  test(
    "embedded schemas may reorder fields and add ignored fields without changing their meaning"
  ) {
    val original = record()
    val extra = new Schema.Field(
      "future_field",
      Schema.create(Schema.Type.STRING),
      null,
      null
    )
    val reordered = changed(
      original,
      extra +: original.getSchema.getFields.asScala.toSeq.reverse,
      Map("future_field" -> "ignored")
    )
    parse(container(reordered)) shouldBe parse(container(original))
  }

  test("compressed OCF blocks are read through Avro's declared codec") {
    val value = record()
    parse(
      container(value, codec = CodecFactory.deflateCodec(6))
    ) shouldBe parse(container(value))
  }

  test("V6 skips an unused index marker as the stable Milvus reader does") {
    val original = record()
    val marker = new Schema.Field(
      "manifest_has_index",
      Schema.create(Schema.Type.BOOLEAN),
      null,
      null
    )
    Seq(false, true).foreach { flag =>
      val extended = changed(
        original,
        original.getSchema.getFields.asScala.toSeq :+ marker,
        Map("manifest_has_index" -> java.lang.Boolean.valueOf(flag))
      )
      parse(container(extended)) shouldBe parse(container(original))
    }
  }

  test(
    "missing or incompatible consumed fields fail even when their arrays are empty"
  ) {
    val value = record()
    val fields = value.getSchema.getFields.asScala.toSeq
    error(
      container(changed(value, fields.filterNot(_.name == "num_of_rows")))
    ) should include("num_of_rows")
    val wrongRows = fields.map(f =>
      if (f.name == "num_of_rows")
        new Schema.Field(f.name, Schema.create(Schema.Type.STRING), null, null)
      else f
    )
    error(
      container(changed(value, wrongRows, Map("num_of_rows" -> "2")))
    ) should include("must be LONG")
    val wrongIndexes = fields.map(f =>
      if (f.name == "index_files")
        new Schema.Field(
          f.name,
          Schema.createArray(Schema.create(Schema.Type.STRING)),
          null,
          null
        )
      else f
    )
    error(
      container(
        changed(
          value,
          wrongIndexes,
          Map("index_files" -> Seq.empty[String].asJava)
        )
      )
    ) should include("must be RECORD")
  }

  test("a container must contain exactly one complete record") {
    val value = record()
    error(container(value, copies = 0)) should include("no record")
    error(container(value, copies = 2)) should include("exactly one record")
    val bytes = container(value)
    Seq(1, 4, 16, bytes.length / 2, bytes.length - 1).foreach { length =>
      withClue(s"length=$length: ") {
        SnapshotSegmentReader.parse(bytes.take(length), 6).isLeft shouldBe true
      }
    }
    // A truncated next-block header is not a valid EOF after the first record.
    SnapshotSegmentReader.parse(bytes ++ Array[Byte](2), 6).isLeft shouldBe true
    val badSync = bytes.clone()
    badSync(badSync.length - 1) = (badSync.last ^ 1).toByte
    error(badSync) should include("sync marker")
  }

  test(
    "snapshot versions choose the encoding and unknown versions fail closed"
  ) {
    val bytes = container(record())
    SnapshotSegmentSchema.supportedVersions.foreach(version =>
      error(bytes, version) should include("requires raw Avro binary")
    )
    error(SnapshotSegmentFixture.encode()) should include(
      "requires an Avro object container"
    )
    Seq(-1, 0, 1, 2, 3, 5, 7, Int.MaxValue).foreach(version =>
      error(bytes, version) should include(
        "Unsupported Milvus manifest schema version"
      )
    )
  }
}
