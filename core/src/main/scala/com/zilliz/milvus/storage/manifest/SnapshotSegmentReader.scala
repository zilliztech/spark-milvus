package com.zilliz.milvus.storage.manifest

import java.io.ByteArrayInputStream
import java.util.{List => JavaList}
import java.util.Arrays
import scala.jdk.CollectionConverters._

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8
import org.apache.avro.Schema

/** Low-level mirror of one AVRO binlog group (`AvroFieldBinlog`):
  *   - `slotFieldId`: the value milvus writes for `AvroFieldBinlog.field_id`.
  *     This is NOT the real field ID(s) inside the parquet file — it is the
  *     column-group slot (also used as the directory name). To learn the real
  *     fields, read the parquet's `group_field_id_list` kv-metadata.
  *   - `binlogs`: one [[BinlogEntry]] per physical parquet file belonging to
  *     this group, in writer-assigned order (sort by `logId` for rows).
  */
case class FieldBinlogEntry(
    slotFieldId: Long,
    binlogs: Seq[BinlogEntry]
)

case class BinlogEntry(
    logId: Long,
    logPath: String,
    entriesNum: Long
)

/** The index descriptor carried by a segment Avro record. Collection and
  * partition identity are supplied by the enclosing snapshot and segment.
  */
case class IndexFileEntry(
    segmentId: Long,
    fieldId: Long,
    indexId: Long,
    buildId: Long,
    name: String,
    parameters: Map[String, String],
    filePaths: Vector[String],
    rowCount: Long,
    serializedSize: Long,
    indexVersion: Long,
    currentIndexVersion: Option[Int],
    indexStorePathVersion: Option[Int]
)

/** The fields of one snapshot segment record (Milvus's AVRO `ManifestEntry`)
  * used by snapshot and segment planning.
  *
  * `storageVersion` uses the authoritative constants from
  * `milvus/internal/storage/rw.go`: StorageV1=0, StorageV2=2, StorageV3=3.
  */
case class SnapshotSegmentEntry(
    segmentId: Long,
    partitionId: Long,
    segmentLevel: Long,
    numOfRows: Long,
    storageVersion: Long,
    binlogFiles: Seq[FieldBinlogEntry],
    deltaLogFiles: Seq[FieldBinlogEntry],
    statsLogFiles: Seq[FieldBinlogEntry] = Seq.empty,
    indexFiles: Option[Vector[IndexFileEntry]] = None
)

/** Decoder for the per-segment AVRO records a Milvus snapshot lists in its
  * `manifest_list`, written by milvus-datacoord.
  *
  * Version 4 is schemaless Avro binary and requires its exact bundled writer
  * schema. Version 6 uses an Avro object container with an embedded writer
  * schema, as Milvus v3.0.4 writes in `internal/snapshotio/snapshot.go`. That
  * container must describe exactly one segment and retain the fields and types
  * this reader consumes; additional fields do not change their meaning.
  *
  * Usage:
  * {{{
  *   val bytes: Array[Byte] = readBytesFromS3(path)
  *   SnapshotSegmentReader.parse(bytes, version) match {
  *     case Right(entry) => // validate the storage version before planning
  *     case Left(err) => throw err
  *   }
  * }}}
  */
object SnapshotSegmentReader extends com.zilliz.milvus.storage.Logging {

  private val LastNeededField = "index_files"
  private val ContainerVersion = 6
  private val ContainerMagic = Array[Byte]('O', 'b', 'j', 1)

  def supportsFormatVersion(version: Int): Boolean =
    version == ContainerVersion || SnapshotSegmentSchema.supportedVersions
      .contains(version)

  private lazy val schemas: Map[Int, Schema] =
    SnapshotSegmentSchema.supportedVersions.map { version =>
      version -> truncatedSchema(
        SnapshotSegmentSchema.schemaFor(version),
        LastNeededField,
        version
      )
    }.toMap

  private def schemaFor(version: Int): Schema =
    schemas.getOrElse(
      version,
      throw new IllegalArgumentException(
        s"Unsupported Milvus manifest schema version $version"
      )
    )

  /** Build a record schema containing only the prefix of `full`'s fields up to
    * and including `lastField`. Field objects are reconstructed so the new
    * record owns them (avro forbids a field being attached to two records).
    */
  private def truncatedSchema(
      full: Schema,
      lastField: String,
      version: Int
  ): Schema = {
    val idx = full.getFields.asScala.indexWhere(_.name == lastField)
    if (idx < 0) {
      throw new IllegalStateException(
        s"bundled Milvus manifest schema version $version is missing required field '$lastField'"
      )
    }
    val truncated = Schema.createRecord(
      full.getName,
      full.getDoc,
      full.getNamespace,
      full.isError
    )
    val prefixFields = full.getFields.asScala.take(idx + 1).map { f =>
      new Schema.Field(f.name, f.schema, f.doc, f.defaultVal)
    }
    truncated.setFields(prefixFields.toList.asJava)
    truncated
  }

  /** Decode the raw bytes of one snapshot segment record (`*.avro`) into the
    * subset of fields needed by snapshot and segment planning.
    *
    * @return
    *   `Right(entry)` on success, or `Left(throwable)` on any parse error.
    */
  def parse(
      avroBytes: Array[Byte],
      manifestSchemaVersion: Int
  ): Either[Throwable, SnapshotSegmentEntry] = {
    try {
      if (manifestSchemaVersion == ContainerVersion) {
        Right(readContainer(avroBytes))
      } else {
        val schema = schemaFor(manifestSchemaVersion)
        require(
          !avroBytes.startsWith(ContainerMagic),
          s"Milvus manifest schema version $manifestSchemaVersion requires raw Avro binary, not a container"
        )
        val reader = new GenericDatumReader[GenericRecord](schema)
        val decoder = DecoderFactory
          .get()
          .binaryDecoder(new ByteArrayInputStream(avroBytes), null)
        Right(projectEntry(reader.read(null, decoder)))
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }

  private def readContainer(bytes: Array[Byte]): SnapshotSegmentEntry = {
    validateContainerFraming(bytes)
    val reader = new DataFileStream[GenericRecord](
      new ByteArrayInputStream(bytes),
      new GenericDatumReader[GenericRecord]()
    )
    try {
      validateContainerSchema(reader.getSchema)
      require(reader.hasNext, "Milvus manifest container contains no record")
      val entry = projectEntry(reader.next())
      require(
        !reader.hasNext,
        "Milvus manifest container contains multiple records"
      )
      entry
    } finally reader.close()
  }

  /** Check framing before Avro allocates a block. DataFileStream treats EOF
    * while starting another block as normal completion, including a partial
    * block header; every input byte must instead belong to a complete block.
    */
  private def validateContainerFraming(bytes: Array[Byte]): Unit = {
    require(
      bytes.startsWith(ContainerMagic),
      s"Milvus manifest schema version $ContainerVersion requires an Avro object container"
    )
    val input = new ByteArrayInputStream(bytes)
    val decoder = DecoderFactory.get().directBinaryDecoder(input, null)
    decoder.skipFixed(ContainerMagic.length)
    def size(value: Long, remaining: Int, description: String): Int = {
      require(
        value >= 0 && value <= remaining,
        s"Invalid Milvus manifest container $description size $value ($remaining bytes remain)"
      )
      value.toInt
    }
    def skipValue(): Unit = {
      val length = decoder.readLong()
      decoder.skipFixed(size(length, input.available(), "metadata value"))
    }
    var entries = decoder.readLong()
    while (entries != 0) {
      require(entries != Long.MinValue, "Invalid Avro metadata block count")
      val count = math.abs(entries)
      val blockSize =
        if (entries < 0)
          Some(size(decoder.readLong(), input.available(), "metadata block"))
        else None
      val before = input.available()
      require(count <= before / 2, "Truncated Avro container metadata")
      var index = 0L
      while (index < count) {
        skipValue()
        skipValue()
        index += 1
      }
      blockSize.foreach(expected =>
        require(
          before - input.available() == expected,
          "Invalid Avro metadata block size"
        )
      )
      entries = decoder.readLong()
    }
    val sync = new Array[Byte](16)
    decoder.readFixed(sync)
    var records = 0L
    while (input.available() > 0) {
      val count = decoder.readLong()
      require(
        count > 0 && count <= 1 - records,
        "Milvus manifest container must contain exactly one record"
      )
      val length = decoder.readLong()
      decoder.skipFixed(
        size(length, input.available() - sync.length, "data block")
      )
      val marker = new Array[Byte](sync.length)
      decoder.readFixed(marker)
      require(Arrays.equals(sync, marker), "Invalid Avro container sync marker")
      records += count
    }
    require(records == 1, "Milvus manifest container contains no record")
  }

  /** Validate the consumed projection by field name, not by writer field order.
    * A self-describing schema may add fields, but cannot silently remove or
    * reinterpret the identity, row counts, physical files or index metadata.
    */
  private def validateContainerSchema(schema: Schema): Unit = {
    def expect(actual: Schema, expected: Schema.Type, path: String): Schema = {
      require(
        actual.getType == expected,
        s"Milvus manifest field '$path' must be $expected, got ${actual.getType}"
      )
      actual
    }
    def field(
        record: Schema,
        name: String,
        kind: Schema.Type,
        path: String
    ): Schema = {
      expect(record, Schema.Type.RECORD, path)
      val value = Option(record.getField(name)).getOrElse(
        throw new IllegalArgumentException(
          s"Milvus manifest schema is missing required field '$path.$name'"
        )
      )
      expect(value.schema(), kind, s"$path.$name")
    }
    def array(
        record: Schema,
        name: String,
        kind: Schema.Type,
        path: String
    ): Schema =
      expect(
        field(record, name, Schema.Type.ARRAY, path).getElementType,
        kind,
        s"$path.$name[]"
      )
    def optional(
        record: Schema,
        name: String,
        kind: Schema.Type,
        path: String
    ): Unit =
      Option(record.getField(name)).foreach(value =>
        expect(value.schema(), kind, s"$path.$name")
      )

    val root = "segment"
    Seq(
      "segment_id",
      "partition_id",
      "segment_level",
      "num_of_rows",
      "storage_version"
    )
      .foreach(field(schema, _, Schema.Type.LONG, root))
    Seq("binlog_files", "deltalog_files", "statslog_files").foreach { name =>
      val group = array(schema, name, Schema.Type.RECORD, root)
      val path = s"$root.$name[]"
      field(group, "field_id", Schema.Type.LONG, path)
      val log = array(group, "binlogs", Schema.Type.RECORD, path)
      Seq("log_id", "entries_num").foreach(
        field(log, _, Schema.Type.LONG, s"$path.binlogs[]")
      )
      field(log, "log_path", Schema.Type.STRING, s"$path.binlogs[]")
    }
    val index = array(schema, "index_files", Schema.Type.RECORD, root)
    val path = s"$root.index_files[]"
    Seq(
      "segment_id",
      "field_id",
      "index_id",
      "build_id",
      "num_rows",
      "serialized_size",
      "index_version"
    )
      .foreach(field(index, _, Schema.Type.LONG, path))
    field(index, "index_name", Schema.Type.STRING, path)
    array(index, "index_file_paths", Schema.Type.STRING, path)
    val parameter = array(index, "index_params", Schema.Type.RECORD, path)
    Seq("key", "value").foreach(
      field(parameter, _, Schema.Type.STRING, s"$path.index_params[]")
    )
    optional(index, "current_index_version", Schema.Type.INT, path)
    optional(index, "index_store_path_version", Schema.Type.INT, path)
  }

  // -------------------------------------------------------------------------
  // Private helpers — project GenericRecord -> our case classes.
  // -------------------------------------------------------------------------

  /** Avro returns strings as `Utf8`, longs as boxed `java.lang.Long`, etc.
    * These helpers do the narrowing once in one place so the projection code
    * stays readable.
    */
  private def asLong(v: Any): Long = v match {
    case l: java.lang.Long    => l.longValue()
    case i: java.lang.Integer => i.longValue()
    case other =>
      throw new IllegalStateException(
        s"expected long, got ${other.getClass.getName}: $other"
      )
  }

  private def asString(v: Any): String = v match {
    case u: Utf8   => u.toString
    case s: String => s
    case null      => null
    case other =>
      throw new IllegalStateException(
        s"expected string, got ${other.getClass.getName}: $other"
      )
  }

  private def projectEntry(rec: GenericRecord): SnapshotSegmentEntry = {
    SnapshotSegmentEntry(
      segmentId = asLong(rec.get("segment_id")),
      partitionId = asLong(rec.get("partition_id")),
      segmentLevel = asLong(rec.get("segment_level")),
      numOfRows = asLong(rec.get("num_of_rows")),
      storageVersion = asLong(rec.get("storage_version")),
      binlogFiles = projectFieldBinlogs(rec.get("binlog_files")),
      deltaLogFiles = projectFieldBinlogs(rec.get("deltalog_files")),
      statsLogFiles = projectFieldBinlogs(rec.get("statslog_files")),
      indexFiles = Some(projectIndexes(rec.get("index_files")))
    )
  }

  private def projectIndexes(value: Any): Vector[IndexFileEntry] = {
    value.asInstanceOf[JavaList[GenericRecord]].asScala.toVector.map { rec =>
      def optionalInt(name: String): Option[Int] =
        Option(rec.getSchema.getField(name))
          .map(_ => asLong(rec.get(name)).toInt)
      val parameters = rec
        .get("index_params")
        .asInstanceOf[JavaList[GenericRecord]]
        .asScala
        .map { pair =>
          asString(pair.get("key")) -> asString(pair.get("value"))
        }
        .toMap
      IndexFileEntry(
        segmentId = asLong(rec.get("segment_id")),
        fieldId = asLong(rec.get("field_id")),
        indexId = asLong(rec.get("index_id")),
        buildId = asLong(rec.get("build_id")),
        name = asString(rec.get("index_name")),
        parameters = parameters,
        filePaths = rec
          .get("index_file_paths")
          .asInstanceOf[JavaList[Any]]
          .asScala
          .map(asString)
          .toVector,
        rowCount = asLong(rec.get("num_rows")),
        serializedSize = asLong(rec.get("serialized_size")),
        indexVersion = asLong(rec.get("index_version")),
        currentIndexVersion = optionalInt("current_index_version"),
        indexStorePathVersion = optionalInt("index_store_path_version")
      )
    }
  }

  private def projectFieldBinlogs(v: Any): Seq[FieldBinlogEntry] = {
    // Avro arrays deserialize to java.util.List (actually GenericData.Array).
    val list = v.asInstanceOf[JavaList[GenericRecord]]
    list.asScala.toSeq.map { afb =>
      FieldBinlogEntry(
        slotFieldId = asLong(afb.get("field_id")),
        binlogs = afb
          .get("binlogs")
          .asInstanceOf[JavaList[GenericRecord]]
          .asScala
          .toSeq
          .map(bl =>
            BinlogEntry(
              logId = asLong(bl.get("log_id")),
              logPath = asString(bl.get("log_path")),
              entriesNum = asLong(bl.get("entries_num"))
            )
          )
      )
    }
  }
}
