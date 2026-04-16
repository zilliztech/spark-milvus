package com.zilliz.spark.connector.read

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._

import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8
import org.apache.avro.Schema
import org.apache.spark.internal.Logging

/** Low-level mirror of one AVRO binlog group (`AvroFieldBinlog`):
  *   - `slotFieldId`: the value milvus writes for `AvroFieldBinlog.field_id`.
  *     This is NOT the real field ID(s) inside the parquet file — it is the
  *     column-group slot (also used as the directory name). To learn the real
  *     fields, read the parquet's `group_field_id_list` kv-metadata.
  *   - `binlogs`: one [[AvroBinlogEntry]] per physical parquet file belonging
  *     to this group, in writer-assigned order (sort by `logId` for rows).
  */
private[read] case class AvroFieldBinlogEntry(
    slotFieldId: Long,
    binlogs: Seq[AvroBinlogEntry]
)

private[read] case class AvroBinlogEntry(
    logId: Long,
    logPath: String,
    entriesNum: Long
)

/** The subset of AVRO `ManifestEntry` fields that backfill actually needs.
  *
  * `storageVersion` uses the authoritative constants from
  * `milvus/internal/storage/rw.go`: StorageV1=0, StorageV2=2, StorageV3=3.
  */
private[read] case class AvroManifestEntry(
    segmentId: Long,
    partitionId: Long,
    numOfRows: Long,
    storageVersion: Long,
    binlogFiles: Seq[AvroFieldBinlogEntry]
)

/** Decoder for per-segment manifest AVRO files written by milvus-datacoord.
  *
  * The binary is schemaless (milvus uses `hamba/avro avro.Marshal` — no OCF
  * container header), so the decoder must be given the exact writer schema. We
  * bundle that schema as a classpath resource; its content is the verbatim
  * `getProperAvroSchema()` string from milvus `internal/datacoord/snapshot.go`.
  *
  * Usage:
  * {{{
  *   val bytes: Array[Byte] = readBytesFromS3(path)
  *   MilvusSegmentManifestReader.parse(bytes) match {
  *     case Right(entry) if entry.storageVersion == 2L => ...
  *     case Right(entry) => // skip — v0/v1/v3 handled elsewhere
  *     case Left(err) => throw err
  *   }
  * }}}
  */
object MilvusSegmentManifestReader extends Logging {

  /** Classpath path of the bundled AVSC (same content as milvus's
    * `getProperAvroSchema()`).
    */
  private val SchemaResource = "/milvus-segment-manifest.avsc"

  /** The last field we read from each AVRO record. Avro binary is positional
    * and has no container header here, so the reader schema must match the
    * writer schema exactly for whatever it parses. By stopping at
    * `binlog_files` (the last field backfill actually uses) we make all
    * trailing fields — present or absent — effectively optional: extra bytes
    * after this field are simply ignored, and writers that emit fewer trailing
    * fields still decode because the prefix they share with us is identical.
    * Update this if backfill starts consuming a later field.
    */
  private val LastNeededField = "binlog_files"

  /** Parsed lazily once. `Schema.Parser` is not thread-safe but the parsed
    * `Schema` is immutable, so lazy val is fine. We truncate the bundled schema
    * to the prefix ending at `LastNeededField` so trailing fields are treated
    * as optional across milvus versions.
    */
  private lazy val schema: Schema = truncatedSchema(fullSchema, LastNeededField)

  private lazy val fullSchema: Schema = {
    val in = Option(getClass.getResourceAsStream(SchemaResource)).getOrElse {
      throw new IllegalStateException(
        s"bundled AVSC resource $SchemaResource not found on classpath"
      )
    }
    try {
      val schemaText =
        scala.io.Source.fromInputStream(in, "UTF-8").mkString
      new Schema.Parser().parse(schemaText)
    } finally {
      in.close()
    }
  }

  /** Build a record schema containing only the prefix of `full`'s fields up to
    * and including `lastField`. Field objects are reconstructed so the new
    * record owns them (avro forbids a field being attached to two records).
    */
  private def truncatedSchema(full: Schema, lastField: String): Schema = {
    val idx = full.getFields.asScala.indexWhere(_.name == lastField)
    if (idx < 0) {
      throw new IllegalStateException(
        s"bundled AVSC $SchemaResource is missing required field '$lastField'"
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

  /** Decode the raw bytes of one per-segment `*.avro` file into the subset of
    * fields needed by backfill.
    *
    * @return
    *   `Right(entry)` on success, or `Left(throwable)` on any parse error.
    */
  def parse(avroBytes: Array[Byte]): Either[Throwable, AvroManifestEntry] = {
    try {
      val reader = new GenericDatumReader[GenericRecord](schema)
      val decoder =
        DecoderFactory
          .get()
          .binaryDecoder(new ByteArrayInputStream(avroBytes), null)
      val rec = reader.read(null, decoder)
      Right(projectEntry(rec))
    } catch {
      case e: Throwable => Left(e)
    }
  }

  /** Join an AVRO entry with the segment's `group_field_id_list` kv-metadata
    * (read from any one of the segment's parquet files) to produce the runtime
    * `V2SegmentInfo` with real field IDs per column group.
    *
    * @param entry
    *   Parsed AVRO manifest (must have `storageVersion == 2L`).
    * @param groupFieldIdList
    *   Positional list of groups, each element being the real field IDs carried
    *   by that group. Obtained from `MilvusParquetFooterReader` by splitting
    *   the kv string `"100,0,1;101;102"` on `;` and then `,`.
    */
  def toV2SegmentInfo(
      entry: AvroManifestEntry,
      groupFieldIdList: Seq[Seq[Long]]
  ): Either[Throwable, V2SegmentInfo] = {
    if (entry.storageVersion != 2L) {
      Left(
        new IllegalArgumentException(
          s"expected storageVersion=2 (StorageV2), got ${entry.storageVersion} " +
            s"for segmentId=${entry.segmentId}"
        )
      )
    } else if (entry.binlogFiles.isEmpty) {
      // Empty segment — no column groups to build; downstream must handle.
      Right(
        V2SegmentInfo(
          segmentId = entry.segmentId,
          partitionId = entry.partitionId,
          numOfRows = entry.numOfRows,
          storageVersion = entry.storageVersion,
          columnGroups = Seq.empty
        )
      )
    } else if (entry.binlogFiles.size != groupFieldIdList.size) {
      Left(
        new IllegalStateException(
          s"AVRO/parquet column-group count mismatch for segment " +
            s"${entry.segmentId}: avro has ${entry.binlogFiles.size} " +
            s"binlog entries but group_field_id_list has ${groupFieldIdList.size} groups"
        )
      )
    } else {
      val cgs = entry.binlogFiles
        .zip(groupFieldIdList)
        .map { case (afb, realFieldIds) =>
          // Sort binlogs by logId so reads stream in row order.
          val sorted = afb.binlogs.sortBy(_.logId)
          V2ColumnGroup(
            fieldIds = realFieldIds,
            filePaths = sorted.map(_.logPath),
            fileRowCounts = sorted.map(_.entriesNum)
          )
        }
      Right(
        V2SegmentInfo(
          segmentId = entry.segmentId,
          partitionId = entry.partitionId,
          numOfRows = entry.numOfRows,
          storageVersion = entry.storageVersion,
          columnGroups = cgs
        )
      )
    }
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

  private def asRecord(v: Any): GenericRecord = v match {
    case r: GenericRecord => r
    case other =>
      throw new IllegalStateException(
        s"expected GenericRecord, got ${other.getClass.getName}: $other"
      )
  }

  private def projectEntry(rec: GenericRecord): AvroManifestEntry = {
    AvroManifestEntry(
      segmentId = asLong(rec.get("segment_id")),
      partitionId = asLong(rec.get("partition_id")),
      numOfRows = asLong(rec.get("num_of_rows")),
      storageVersion = asLong(rec.get("storage_version")),
      binlogFiles = projectFieldBinlogs(rec.get("binlog_files"))
    )
  }

  private def projectFieldBinlogs(v: Any): Seq[AvroFieldBinlogEntry] = {
    // Avro arrays deserialize to java.util.List (actually GenericData.Array).
    val list = v.asInstanceOf[java.util.List[GenericRecord]]
    list.asScala.toSeq.map { afb =>
      AvroFieldBinlogEntry(
        slotFieldId = asLong(afb.get("field_id")),
        binlogs = afb
          .get("binlogs")
          .asInstanceOf[java.util.List[GenericRecord]]
          .asScala
          .toSeq
          .map(bl =>
            AvroBinlogEntry(
              logId = asLong(bl.get("log_id")),
              logPath = asString(bl.get("log_path")),
              entriesNum = asLong(bl.get("entries_num"))
            )
          )
      )
    }
  }
}
