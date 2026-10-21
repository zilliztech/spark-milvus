package consumer.write

import java.io.ByteArrayOutputStream
import scala.jdk.CollectionConverters._

import org.apache.avro.generic.{
  GenericData,
  GenericDatumReader,
  GenericDatumWriter,
  GenericRecord
}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentFixture,
  SnapshotSegmentReader,
  SnapshotSegmentSchema,
  SnapshotSegmentWriter
}

/** A writer outside the connector's packages consumes the shared full schema
  * without relying on the reader's projection or package-scoped access.
  */
class SnapshotSegmentWriterTest extends AnyFunSuite with Matchers {

  test("replacing indexes preserves fields outside the reader projection") {
    SnapshotSegmentSchema.supportedVersions.foreach { version =>
      val schema = SnapshotSegmentSchema.schemaFor(version)
      val source =
        decode(SnapshotSegmentFixture.encode(version = version), schema)
      source.put("channel_name", "source-channel")
      val tailSchema =
        schema.getField("json_key_index_files").schema().getElementType
      val stats = new GenericData.Record(tailSchema.getField("stats").schema())
      stats.put("field_id", 102L)
      stats.put("version", 7L)
      stats.put("files", Seq("files/json-key-index/source").asJava)
      stats.put("log_size", 100L)
      stats.put("memory_size", 200L)
      stats.put("build_id", 900L)
      stats.put("json_key_stats_data_format", 1L)
      val tail = new GenericData.Record(tailSchema)
      tail.put("field_id", 102L)
      tail.put("stats", stats)
      source.put("json_key_index_files", Seq(tail).asJava)
      val before = encode(source, schema)
      val index = SnapshotSegmentFixture.index()
      val after =
        SnapshotSegmentWriter.rewriteIndexes(before, Vector(index), version)
      val original = decode(before, schema)
      val rewritten = decode(after, schema)
      schema.getFields.asScala.filterNot(_.name() == "index_files").foreach {
        field =>
          withClue(s"schema $version field ${field.name()}: ") {
            rewritten.get(field.name()) shouldBe original.get(field.name())
          }
      }
      val expected =
        if (version == 1) index.copy(indexStorePathVersion = None) else index
      SnapshotSegmentReader
        .parse(after, version)
        .fold(throw _, identity)
        .indexFiles shouldBe Some(Vector(expected))
    }
  }

  private def decode(bytes: Array[Byte], schema: Schema): GenericRecord =
    new GenericDatumReader[GenericRecord](schema)
      .read(null, DecoderFactory.get().binaryDecoder(bytes, null))

  private def encode(record: GenericRecord, schema: Schema): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(bytes, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()
    bytes.toByteArray
  }
}
