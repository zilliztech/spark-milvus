package com.zilliz.milvus.storage.delete

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.format.{FileMetaData => ThriftFileMetaData, Util}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.{InputFile, SeekableInputStream}
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.VersionParser

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.{DeltaLogFile, Segment}
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

object DeltaLogReader extends com.zilliz.milvus.storage.Logging {
  private val MagicNumber = 0xfffabc
  private val DescriptorEventType: Byte = 0
  private val DeleteEventType: Byte = 2
  private val EventTypeCount = 8
  private val BaseEventHeaderSize = 17
  private val DescriptorEventDataFixPartSize = 52
  private val DeleteEventDataFixPartSize = 16
  private val MultiFieldVersion = "MULTI_FIELD"

  private val mapper = new ObjectMapper()

  def loadDeletePlansBySegment(
      segments: Seq[Segment],
      milvusSchema: CollectionSchema,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, Map[Long, DeletePlan]] = {
    val pkField = primaryKeyField(milvusSchema)
    val deleteOnlySegments = segments.filterNot(_.hasData)
    val dataSegments = segments.filter(_.hasData)

    for {
      globalPlans <- loadPartitionScopedDeletePlans(
        deleteOnlySegments,
        pkField,
        bucket,
        store
      )
      ownPlans <- sequence(
        dataSegments.map { seg =>
          loadDeletePlan(seg.deltaLogs, pkField, bucket, store).map { ownPlan =>
            seg.id -> ownPlan
          }
        }
      )
    } yield mergeInheritedDeletePlans(
      dataSegments,
      globalPlans,
      ownPlans.toMap
    )
  }

  private val AllPartitionsId = -1L

  def mergeInheritedDeletePlans(
      dataSegments: Seq[Segment],
      inheritedPlansByPartition: Map[Long, DeletePlan],
      ownPlansBySegment: Map[Long, DeletePlan]
  ): Map[Long, DeletePlan] = {
    dataSegments.iterator.map { seg =>
      val inheritedPlan = effectiveInheritedDeletePlan(
        seg.partitionId,
        inheritedPlansByPartition
      )
      val ownPlan = ownPlansBySegment.getOrElse(
        seg.id,
        DeletePlan.empty
      )
      seg.id -> DeletePlan.union(inheritedPlan, ownPlan)
    }.toMap
  }

  def loadPartitionScopedDeletePlans(
      deleteOnlySegments: Seq[Segment],
      pkField: FieldSchema,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, Map[Long, DeletePlan]] = {
    sequence(
      deleteOnlySegments.groupBy(_.partitionId).toSeq.map {
        case (partitionId, segments) =>
          loadDeletePlan(
            segments.flatMap(_.deltaLogs),
            pkField,
            bucket,
            store
          ).map(partitionId -> _)
      }
    ).map(_.toMap)
  }

  def effectiveInheritedDeletePlan(
      partitionId: Long,
      inheritedPlansByPartition: Map[Long, DeletePlan]
  ): DeletePlan = {
    val collectionWidePlan = inheritedPlansByPartition.getOrElse(
      AllPartitionsId,
      DeletePlan.empty
    )
    val partitionPlan = inheritedPlansByPartition.getOrElse(
      partitionId,
      DeletePlan.empty
    )
    DeletePlan.union(collectionWidePlan, partitionPlan)
  }

  def inheritedDeletePlanPartitionMarker(
      partitionId: Long,
      inheritedPlansByPartition: Map[Long, DeletePlan]
  ): Option[Long] =
    if (
      inheritedPlansByPartition.contains(AllPartitionsId) ||
      inheritedPlansByPartition.contains(partitionId)
    ) Some(partitionId)
    else None

  def loadDeletePlan(
      deltaLogs: Seq[DeltaLogFile],
      pkField: FieldSchema,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, DeletePlan] = {
    try {
      validatePkType(pkField)
      val plans = deltaLogs.map { log =>
        val at = StoragePath.parse(log.logPath, bucket)
        decodeDeletePlan(store.readAll(at), pkField, at.key)
      }
      sequence(plans).map(DeletePlan.union)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  /** Two encodings of a delete file are read, told apart by the first bytes
    * (decision 13): a bare parquet file with a pk column and a ts column, which
    * is what a V3 segment's `_delta/` holds, and the binlog event container of
    * the V2 line, whose parquet payloads sit behind a descriptor event.
    */
  private def decodeDeletePlan(
      bytes: Array[Byte],
      pkField: FieldSchema,
      path: String
  ): Either[Throwable, DeletePlan] = {
    try {
      if (isParquet(bytes)) {
        Right(decodePayloadPlan(bytes, multiField = true, pkField, path))
      } else {
        val container = parseContainer(bytes, path)
        val plans = container.payloads.map { payload =>
          decodePayloadPlan(payload, container.multiField, pkField, path)
        }
        Right(DeletePlan.union(plans))
      }
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  private val ParquetMagic = "PAR1".getBytes(StandardCharsets.US_ASCII)

  private def isParquet(bytes: Array[Byte]): Boolean =
    bytes.length >= 4 && java.util.Arrays.equals(
      java.util.Arrays.copyOfRange(bytes, 0, 4),
      ParquetMagic
    )

  private def decodePayloadPlan(
      payload: Array[Byte],
      multiField: Boolean,
      pkField: FieldSchema,
      path: String
  ): DeletePlan = {
    val longs = mutable.HashMap.empty[Long, Long]
    val strings = mutable.HashMap.empty[String, Long]
    forEachRow(payload) { row =>
      if (multiField) {
        appendMultiFieldDelete(row, pkField, path, longs, strings)
      } else {
        appendLegacyDelete(
          extractLegacyDelete(row, path),
          pkField,
          path,
          longs,
          strings
        )
      }
    }

    pkField.dataType match {
      case DataType.Int64   => DeletePlan.fromLongPks(longs.toMap)
      case DataType.VarChar => DeletePlan.fromStringPks(strings.toMap)
      case other =>
        throw new IllegalArgumentException(
          s"unsupported primary key type $other for delete logs"
        )
    }
  }

  private def appendMultiFieldDelete(
      row: ParquetRow,
      pkField: FieldSchema,
      path: String,
      longs: mutable.Map[Long, Long],
      strings: mutable.Map[String, Long]
  ): Unit = {
    val fieldCount = row.columnCount
    if (fieldCount < 2) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path must contain pk and ts columns, found $fieldCount columns"
      )
    }
    if (row.isNull(0)) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path is missing the pk value"
      )
    }
    if (row.isNull(1)) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path is missing the ts value"
      )
    }
    val deleteTs = row.getLong(1)

    pkField.dataType match {
      case DataType.Int64 =>
        val pk = row.getLong(0)
        longs.update(pk, math.max(longs.getOrElse(pk, Long.MinValue), deleteTs))
      case DataType.VarChar =>
        val pk = row.getString(0)
        strings.update(
          pk,
          math.max(strings.getOrElse(pk, Long.MinValue), deleteTs)
        )
      case other =>
        throw new IllegalArgumentException(
          s"unsupported primary key type $other for delete logs"
        )
    }
  }

  private def extractLegacyDelete(row: ParquetRow, path: String): String = {
    val fieldCount = row.columnCount
    if (fieldCount < 1) {
      throw new IllegalStateException(
        s"legacy delete log payload in $path is missing the delta column"
      )
    }
    if (row.isNull(0)) {
      throw new IllegalStateException(
        s"legacy delete log payload in $path has an empty delta value"
      )
    }
    row.getString(0)
  }

  /** Walks every row of a parquet payload, one row group at a time.
    *
    * The read goes through parquet's column readers, not its record assembly:
    * the column readers address a column by its descriptor and need nothing
    * from Hadoop's MapReduce classes, which record assembly's `ParquetReader`
    * pulls in.
    */
  private def forEachRow(payload: Array[Byte])(f: ParquetRow => Unit): Unit = {
    val file =
      ParquetFileReader.open(new InMemoryInputFile(withColumnNames(payload)))
    try {
      val meta = file.getFooter.getFileMetaData
      val columns = meta.getSchema.getColumns.asScala.toIndexedSeq
      val writerVersion =
        try VersionParser.parse(meta.getCreatedBy)
        catch { case NonFatal(_) => null }
      var pages = file.readNextRowGroup()
      while (pages != null) {
        val row = new ParquetRow(columns.map { column =>
          new ColumnReaderImpl(
            column,
            pages.getPageReader(column),
            NoConverter,
            writerVersion
          )
        })
        var remaining = pages.getRowCount
        while (remaining > 0) {
          f(row)
          row.advance()
          remaining -= 1
        }
        pages = file.readNextRowGroup()
      }
    } finally {
      file.close()
    }
  }

  /** Gives every unnamed column of a flat parquet file a name.
    *
    * parquet-mr identifies a column by its path of names, both in the schema
    * and in the row group's column chunks. The `_delta` files milvus-storage
    * writes for a V3 segment carry two columns with empty names, so parquet-mr
    * sees one path twice, reads one chunk and fails on the other. Naming the
    * columns in the footer, `column_0`, `column_1`, and so on, leaves the data
    * pages untouched and makes the file readable. A file whose columns are all
    * named is returned as is.
    */
  private def withColumnNames(payload: Array[Byte]): Array[Byte] = {
    val footerLength = ByteBuffer
      .wrap(payload, payload.length - 8, 4)
      .order(ByteOrder.LITTLE_ENDIAN)
      .getInt
    val footerStart = payload.length - 8 - footerLength
    val footer: ThriftFileMetaData = Util.readFileMetaData(
      new ByteArrayInputStream(payload, footerStart, footerLength)
    )
    val elements = footer.getSchema.asScala
    val leaves = elements.drop(1)
    if (leaves.forall(e => !e.getName.isEmpty)) return payload
    if (leaves.exists(e => e.isSetNum_children && e.getNum_children > 0)) {
      throw new IllegalStateException(
        "delete log parquet payload has unnamed columns in a nested schema"
      )
    }
    leaves.zipWithIndex.foreach { case (leaf, i) =>
      if (leaf.getName.isEmpty) leaf.setName(s"column_$i")
    }
    footer.getRow_groups.asScala.foreach { rowGroup =>
      rowGroup.getColumns.asScala.zipWithIndex.foreach { case (chunk, i) =>
        chunk.getMeta_data.setPath_in_schema(
          java.util.List.of(leaves(i).getName)
        )
      }
    }
    val out = new ByteArrayOutputStream(payload.length)
    out.write(payload, 0, footerStart)
    val footerOut = new ByteArrayOutputStream(footerLength)
    Util.writeFileMetaData(footer, footerOut)
    footerOut.writeTo(out)
    out.write(
      ByteBuffer
        .allocate(4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(footerOut.size())
        .array()
    )
    out.write(ParquetMagic)
    out.toByteArray
  }

  /** The current row of a row group, read column by column. */
  private final class ParquetRow(readers: IndexedSeq[ColumnReaderImpl]) {
    def columnCount: Int = readers.size

    def isNull(column: Int): Boolean = {
      val reader = readers(column)
      reader.getCurrentDefinitionLevel < reader.getDescriptor.getMaxDefinitionLevel
    }

    def getLong(column: Int): Long = readers(column).getLong

    def getString(column: Int): String = {
      val reader = readers(column)
      reader.getDescriptor.getPrimitiveType.getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY |
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
          reader.getBinary.toStringUsingUTF8
        case other =>
          throw new IllegalStateException(
            s"delete log column $column is $other, expected a string"
          )
      }
    }

    def advance(): Unit = readers.foreach(_.consume())
  }

  /** Column readers require a converter; the values are pulled directly. */
  private object NoConverter extends PrimitiveConverter

  private def appendLegacyDelete(
      raw: String,
      pkField: FieldSchema,
      path: String,
      longs: mutable.Map[Long, Long],
      strings: mutable.Map[String, Long]
  ): Unit = {
    val trimmed = raw.trim
    if (trimmed.startsWith("{")) {
      val json = mapper.readTree(trimmed)
      val pkType = json.path("pkType").asInt(Int.MinValue)
      val pkNode = json.path("pk")
      if (pkType == Int.MinValue || pkNode.isMissingNode) {
        throw new IllegalStateException(
          s"invalid legacy delete log JSON in $path: $trimmed"
        )
      }
      val expectedPkType = pkField.dataType match {
        case DataType.Int64   => DataType.Int64.value
        case DataType.VarChar => DataType.VarChar.value
        case other =>
          throw new IllegalArgumentException(
            s"unsupported primary key type $other for delete logs"
          )
      }
      if (pkType != expectedPkType) {
        throw new IllegalStateException(
          s"delete log pkType $pkType in $path does not match collection PK type ${pkField.dataType.value}"
        )
      }
      val tsNode = json.path("ts")
      if (!tsNode.canConvertToLong) {
        throw new IllegalStateException(
          s"delete log ts in $path must be numeric: $trimmed"
        )
      }
      val deleteTs = tsNode.longValue()
      pkField.dataType match {
        case DataType.Int64 =>
          if (!pkNode.isNumber) {
            throw new IllegalStateException(
              s"delete log pk in $path must be numeric for Int64 PKs: $trimmed"
            )
          }
          val pk = pkNode.longValue()
          longs.update(
            pk,
            math.max(longs.getOrElse(pk, Long.MinValue), deleteTs)
          )
        case DataType.VarChar =>
          if (!pkNode.isTextual) {
            throw new IllegalStateException(
              s"delete log pk in $path must be textual for VarChar PKs: $trimmed"
            )
          }
          val pk = pkNode.textValue()
          strings.update(
            pk,
            math.max(strings.getOrElse(pk, Long.MinValue), deleteTs)
          )
        case _ =>
      }
    } else {
      val parts = trimmed.split(",", 2)
      if (parts.length != 2) {
        throw new IllegalStateException(
          s"invalid legacy delete log payload in $path: $trimmed"
        )
      }
      if (pkField.dataType != DataType.Int64) {
        throw new IllegalStateException(
          s"legacy 'pk,ts' delete log payload in $path only supports Int64 PKs"
        )
      }
      val pk = parts(0).trim.toLong
      val deleteTs = parts(1).trim.toLong
      longs.update(pk, math.max(longs.getOrElse(pk, Long.MinValue), deleteTs))
    }
  }

  private def parseContainer(
      bytes: Array[Byte],
      path: String
  ): ParsedContainer = {
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val magic = buf.getInt()
    if (magic != MagicNumber) {
      throw new IllegalStateException(
        f"invalid deltalog magic number in $path: expected 0x$MagicNumber%x got 0x$magic%x"
      )
    }

    val descriptorHeader = readHeader(buf, s"descriptor header in $path")
    if (descriptorHeader.typeCode != DescriptorEventType) {
      throw new IllegalStateException(
        s"expected descriptor event at start of $path, got type ${descriptorHeader.typeCode}"
      )
    }

    skipFully(
      buf,
      DescriptorEventDataFixPartSize,
      s"descriptor fix part in $path"
    )
    val postHeaderLengths = new Array[Int](EventTypeCount)
    var idx = 0
    while (idx < EventTypeCount) {
      ensureRemaining(buf, 1, s"descriptor post-header lengths in $path")
      postHeaderLengths(idx) = buf.get() & 0xff
      idx += 1
    }
    ensureRemaining(buf, 4, s"descriptor extras length in $path")
    val extraLength = buf.getInt()
    if (extraLength < 0) {
      throw new IllegalStateException(
        s"negative descriptor extras length $extraLength in $path"
      )
    }
    ensureRemaining(buf, extraLength, s"descriptor extras in $path")
    val extraBytes = new Array[Byte](extraLength)
    buf.get(extraBytes)
    val extras =
      if (extraBytes.isEmpty) mapper.createObjectNode()
      else mapper.readTree(extraBytes)
    val multiField =
      Option(extras.get("version")).exists(_.asText() == MultiFieldVersion)

    val payloads = mutable.ArrayBuffer.empty[Array[Byte]]
    while (buf.hasRemaining) {
      val header = readHeader(buf, s"event header in $path")
      val fixPartSize =
        if (header.typeCode >= 0 && header.typeCode < postHeaderLengths.length)
          postHeaderLengths(header.typeCode)
        else 0
      if (fixPartSize < 0) {
        throw new IllegalStateException(
          s"negative event fix-part size $fixPartSize in $path"
        )
      }
      ensureRemaining(buf, fixPartSize, s"event fix part in $path")
      skipFully(buf, fixPartSize, s"event fix part in $path")
      val payloadLength = header.eventLength - BaseEventHeaderSize - fixPartSize
      if (payloadLength < 0) {
        throw new IllegalStateException(
          s"negative event payload length $payloadLength in $path"
        )
      }
      ensureRemaining(buf, payloadLength, s"event payload in $path")
      val payload = new Array[Byte](payloadLength)
      buf.get(payload)
      if (header.typeCode == DeleteEventType && payload.nonEmpty) {
        payloads += payload
      }
    }

    ParsedContainer(multiField = multiField, payloads = payloads.toSeq)
  }

  private def readHeader(buf: ByteBuffer, context: String): ParsedHeader = {
    ensureRemaining(buf, BaseEventHeaderSize, context)
    buf.getLong()
    val typeCode = buf.get()
    val eventLength = buf.getInt()
    buf.getInt()
    ParsedHeader(typeCode, eventLength)
  }

  private def primaryKeyField(milvusSchema: CollectionSchema): FieldSchema = {
    val pkField = milvusSchema.fields.find(_.isPrimaryKey).getOrElse {
      throw new IllegalArgumentException("No primary key field found in schema")
    }
    validatePkType(pkField)
    pkField
  }

  private def validatePkType(pkField: FieldSchema): Unit = {
    pkField.dataType match {
      case DataType.Int64 | DataType.VarChar =>
      case other =>
        throw new IllegalArgumentException(
          s"StorageV2 delete logs only support Int64/VarChar PKs, got $other"
        )
    }
  }

  private def asLong(value: Any, context: String): Long = value match {
    case n: java.lang.Long    => n.longValue()
    case n: java.lang.Integer => n.longValue()
    case n: java.lang.Short   => n.longValue()
    case n: java.lang.Byte    => n.longValue()
    case n: java.lang.Number  => n.longValue()
    case other =>
      throw new IllegalStateException(s"expected numeric $context, got $other")
  }

  private def ensureRemaining(
      buf: ByteBuffer,
      needed: Int,
      context: String
  ): Unit = {
    if (buf.remaining() < needed) {
      throw new EOFException(
        s"unexpected EOF while reading $context: need $needed bytes, only ${buf.remaining()} remain"
      )
    }
  }

  private def skipFully(buf: ByteBuffer, length: Int, context: String): Unit = {
    ensureRemaining(buf, length, context)
    buf.position(buf.position() + length)
  }

  private def sequence[A](
      items: Seq[Either[Throwable, A]]
  ): Either[Throwable, Seq[A]] = {
    val out = mutable.ArrayBuffer.empty[A]
    items.foreach {
      case Right(value) => out += value
      case Left(err)    => return Left(err)
    }
    Right(out.toSeq)
  }

  private final case class ParsedHeader(typeCode: Byte, eventLength: Int)
  private final case class ParsedContainer(
      multiField: Boolean,
      payloads: Seq[Array[Byte]]
  )

  private final class InMemoryInputFile(bytes: Array[Byte]) extends InputFile {
    override def getLength: Long = bytes.length.toLong

    override def newStream(): SeekableInputStream =
      new SeekableInputStream {
        private var pos = 0

        override def getPos: Long = pos.toLong

        override def seek(newPos: Long): Unit = {
          if (newPos < 0 || newPos > bytes.length) {
            throw new EOFException(
              s"invalid seek position $newPos for in-memory parquet payload of ${bytes.length} bytes"
            )
          }
          pos = newPos.toInt
        }

        override def read(): Int = {
          if (pos >= bytes.length) -1
          else {
            val value = bytes(pos) & 0xff
            pos += 1
            value
          }
        }

        override def read(b: Array[Byte], off: Int, len: Int): Int = {
          if (pos >= bytes.length) {
            -1
          } else {
            val toRead = math.min(len, bytes.length - pos)
            System.arraycopy(bytes, pos, b, off, toRead)
            pos += toRead
            toRead
          }
        }

        override def readFully(target: Array[Byte]): Unit =
          readFully(target, 0, target.length)

        override def read(target: ByteBuffer): Int = {
          if (pos >= bytes.length) {
            -1
          } else {
            val toRead = math.min(target.remaining(), bytes.length - pos)
            target.put(bytes, pos, toRead)
            pos += toRead
            toRead
          }
        }

        override def readFully(target: ByteBuffer): Unit = {
          val len = target.remaining()
          ensureAvailable(len)
          target.put(bytes, pos, len)
          pos += len
        }

        override def readFully(
            target: Array[Byte],
            start: Int,
            len: Int
        ): Unit = {
          ensureAvailable(len)
          System.arraycopy(bytes, pos, target, start, len)
          pos += len
        }

        private def ensureAvailable(len: Int): Unit = {
          if (pos + len > bytes.length) {
            throw new EOFException(
              s"unexpected EOF while reading in-memory parquet payload: need $len bytes at offset $pos, payload size=${bytes.length}"
            )
          }
        }
      }
  }
}
