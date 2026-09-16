package com.zilliz.milvus.storage.manifest

import java.io.ByteArrayOutputStream
import java.lang.{Boolean => JavaBoolean, Integer, Long => JavaLong}
import java.nio.ByteBuffer
import java.util.{ArrayList, HashMap}
import scala.jdk.CollectionConverters._

import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.Schema

/** Full writer-schema records for exercising snapshot metadata, including
  * fields after the subset consumed by the connector.
  */
object SegmentManifestFixture {
  def index(segmentId: Long = 30L, rowCount: Long = 2L): AvroIndexFileEntry =
    AvroIndexFileEntry(
      segmentId = segmentId,
      fieldId = 101L,
      indexId = 469076449917763071L,
      buildId = 469076449917967340L,
      name = "v_hnsw",
      parameters = Map(
        "index_type" -> "HNSW",
        "metric_type" -> "COSINE",
        "dim" -> "4",
        "M" -> "30"
      ),
      filePaths =
        Vector("files/index_files/469076449917967340/1/20/30/_mem.index.bin"),
      rowCount = rowCount,
      serializedSize = 4096L,
      indexVersion = 1L,
      currentIndexVersion = Some(10),
      indexStorePathVersion = Some(0)
    )

  def encode(
      version: Int = 4,
      segmentId: Long = 30L,
      partitionId: Long = 20L,
      rows: Long = 2L,
      storageVersion: Long = 3L,
      indexes: Vector[AvroIndexFileEntry] = Vector.empty,
      segmentLevel: Long = 2L
  ): Array[Byte] = {
    val in =
      getClass.getResourceAsStream(s"/milvus-segment-manifest-v$version.avsc")
    val schema =
      try new Schema.Parser().parse(in)
      finally in.close()
    val record = empty(schema).asInstanceOf[GenericRecord]
    record.put("segment_id", segmentId)
    record.put("partition_id", partitionId)
    record.put("segment_level", segmentLevel)
    record.put("num_of_rows", rows)
    record.put("storage_version", storageVersion)
    val indexSchema = schema.getField("index_files").schema.getElementType
    record.put(
      "index_files",
      indexes.map { index =>
        val value = empty(indexSchema).asInstanceOf[GenericRecord]
        value.put("segment_id", index.segmentId)
        value.put("field_id", index.fieldId)
        value.put("index_id", index.indexId)
        value.put("build_id", index.buildId)
        value.put("index_name", index.name)
        val paramSchema =
          indexSchema.getField("index_params").schema.getElementType
        value.put(
          "index_params",
          index.parameters.toSeq.map { case (key, text) =>
            val pair = new GenericData.Record(paramSchema)
            pair.put("key", key)
            pair.put("value", text)
            pair
          }.asJava
        )
        value.put("index_file_paths", index.filePaths.asJava)
        value.put("num_rows", index.rowCount)
        value.put("serialized_size", index.serializedSize)
        value.put("index_version", index.indexVersion)
        value.put(
          "current_index_version",
          index.currentIndexVersion.getOrElse(0)
        )
        if (indexSchema.getField("index_store_path_version") != null)
          value.put(
            "index_store_path_version",
            index.indexStorePathVersion.getOrElse(0)
          )
        value
      }.asJava
    )
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  private def empty(schema: Schema): AnyRef = schema.getType match {
    case Schema.Type.RECORD =>
      val record = new GenericData.Record(schema)
      schema.getFields.asScala.foreach(field =>
        record.put(field.name, empty(field.schema))
      )
      record
    case Schema.Type.ARRAY   => new ArrayList[AnyRef]()
    case Schema.Type.MAP     => new HashMap[String, AnyRef]()
    case Schema.Type.STRING  => ""
    case Schema.Type.BYTES   => ByteBuffer.allocate(0)
    case Schema.Type.LONG    => JavaLong.valueOf(0L)
    case Schema.Type.INT     => Integer.valueOf(0)
    case Schema.Type.BOOLEAN => JavaBoolean.FALSE
    case Schema.Type.UNION   => empty(schema.getTypes.get(0))
    case Schema.Type.NULL    => null
    case other =>
      throw new IllegalArgumentException(
        s"unsupported fixture schema type $other"
      )
  }
}
