package com.zilliz.milvus.storage.manifest

import java.io.ByteArrayOutputStream
import java.lang.{Boolean => JavaBoolean, Integer, Long => JavaLong}
import java.nio.ByteBuffer
import java.util.{ArrayList, HashMap}
import scala.jdk.CollectionConverters._

import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.Schema

/** Writes one segment's manifest as the bytes [[SegmentManifestReader]] reads.
  *
  * Milvus marshals these records with `hamba/avro`, so the object carries no
  * container header and the field order is the writer schema's. The reader
  * stops after `index_files` because nothing above it needs the later fields; a
  * writer cannot, because Avro binary is positional and Milvus reads the whole
  * record. Every field of the bundled schema is therefore written, with a zero
  * value where the connector has nothing to say.
  *
  * Those zero values are assumptions, not facts: `channel_name`, the two
  * positions, `is_sorted` and, for a V3 segment, `binlog_files` have not been
  * checked against what Milvus writes or requires, because the only verified
  * reader of these objects is this connector, which does not use them. A
  * Milvus-side consumer has to settle them first
  * (docs/design/architecture/vector-search.html section 2.7).
  */
object SegmentManifestWriter {

  /** The schema version written, and the `format_version` of a snapshot that
    * names these objects.
    */
  val CurrentSchemaVersion: Int = 4

  /** What only a writer supplies: the fields Milvus fills from the write path
    * that a read projection does not carry.
    */
  final case class SegmentFacts(
      channelName: String = "",
      commitTimestamp: Long = 0L,
      isSorted: Boolean = false
  )

  def encode(
      entry: AvroManifestEntry,
      facts: SegmentFacts = SegmentFacts(),
      schemaVersion: Int = CurrentSchemaVersion
  ): Array[Byte] = {
    val schema = schemaFor(schemaVersion)
    val record = blank(schema).asInstanceOf[GenericRecord]
    record.put("segment_id", entry.segmentId)
    record.put("partition_id", entry.partitionId)
    record.put("segment_level", entry.segmentLevel)
    record.put("channel_name", facts.channelName)
    record.put("num_of_rows", entry.numOfRows)
    record.put("storage_version", entry.storageVersion)
    record.put("is_sorted", JavaBoolean.valueOf(facts.isSorted))
    record.put("commit_timestamp", facts.commitTimestamp)
    putBinlogs(record, schema, "binlog_files", entry.binlogFiles)
    putBinlogs(record, schema, "deltalog_files", entry.deltaLogFiles)
    putBinlogs(record, schema, "statslog_files", entry.statsLogFiles)
    putIndexes(record, schema, entry.indexFiles.getOrElse(Vector.empty))
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  def supportedSchemaVersions: Seq[Int] =
    SegmentManifestReader.supportedSchemaVersions

  private def putBinlogs(
      record: GenericRecord,
      schema: Schema,
      field: String,
      groups: Seq[AvroFieldBinlogEntry]
  ): Unit = {
    val groupSchema = schema.getField(field).schema.getElementType
    val binlogSchema = groupSchema.getField("binlogs").schema.getElementType
    record.put(
      field,
      groups.map { group =>
        val value = blank(groupSchema).asInstanceOf[GenericRecord]
        value.put("field_id", group.slotFieldId)
        value.put(
          "binlogs",
          group.binlogs.map { log =>
            val binlog = blank(binlogSchema).asInstanceOf[GenericRecord]
            binlog.put("log_id", log.logId)
            binlog.put("log_path", log.logPath)
            binlog.put("entries_num", log.entriesNum)
            binlog
          }.asJava
        )
        value
      }.asJava
    )
  }

  private def putIndexes(
      record: GenericRecord,
      schema: Schema,
      indexes: Vector[AvroIndexFileEntry]
  ): Unit = {
    val indexSchema = schema.getField("index_files").schema.getElementType
    val paramSchema = indexSchema.getField("index_params").schema.getElementType
    record.put(
      "index_files",
      indexes.map { index =>
        val value = blank(indexSchema).asInstanceOf[GenericRecord]
        value.put("segment_id", index.segmentId)
        value.put("field_id", index.fieldId)
        value.put("index_id", index.indexId)
        value.put("build_id", index.buildId)
        value.put("index_name", index.name)
        value.put(
          "index_params",
          index.parameters.toSeq
            .sortBy(_._1)
            .map { case (key, text) =>
              val pair = new GenericData.Record(paramSchema)
              pair.put("key", key)
              pair.put("value", text)
              pair
            }
            .asJava
        )
        value.put("index_file_paths", index.filePaths.asJava)
        value.put("num_rows", index.rowCount)
        value.put("serialized_size", index.serializedSize)
        value.put("index_version", index.indexVersion)
        value.put(
          "current_index_version",
          Integer.valueOf(index.currentIndexVersion.getOrElse(0))
        )
        if (indexSchema.getField("index_store_path_version") != null)
          value.put(
            "index_store_path_version",
            Integer.valueOf(index.indexStorePathVersion.getOrElse(0))
          )
        value
      }.asJava
    )
  }

  private def schemaFor(version: Int): Schema = {
    val resource = s"/milvus-segment-manifest-v$version.avsc"
    val in = Option(getClass.getResourceAsStream(resource)).getOrElse {
      throw new IllegalArgumentException(
        s"Unsupported Milvus manifest schema version $version"
      )
    }
    try
      new Schema.Parser()
        .parse(scala.io.Source.fromInputStream(in, "UTF-8").mkString)
    finally in.close()
  }

  /** A record with every field present and empty, so that only what the caller
    * knows has to be set and the encoder never meets a null.
    */
  private def blank(schema: Schema): AnyRef = schema.getType match {
    case Schema.Type.RECORD =>
      val record = new GenericData.Record(schema)
      schema.getFields.asScala.foreach(field =>
        record.put(field.name, blank(field.schema))
      )
      record
    case Schema.Type.ARRAY   => new ArrayList[AnyRef]()
    case Schema.Type.MAP     => new HashMap[String, AnyRef]()
    case Schema.Type.STRING  => ""
    case Schema.Type.BYTES   => ByteBuffer.allocate(0)
    case Schema.Type.LONG    => JavaLong.valueOf(0L)
    case Schema.Type.INT     => Integer.valueOf(0)
    case Schema.Type.BOOLEAN => JavaBoolean.FALSE
    case Schema.Type.UNION   => blank(schema.getTypes.get(0))
    case Schema.Type.NULL    => null
    case other =>
      throw new IllegalArgumentException(
        s"A segment manifest cannot hold a $other field"
      )
  }
}
