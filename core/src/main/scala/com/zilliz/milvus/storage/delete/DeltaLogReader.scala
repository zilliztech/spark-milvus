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

import com.zilliz.milvus.storage.codec.BinlogCodec
import com.zilliz.milvus.storage.codec.BinlogCodec.{forEachRow, ParquetRow}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.{DeltaLogFile, Segment}
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

object DeltaLogReader extends com.zilliz.milvus.storage.Logging {
  private val DeleteEventType: Byte = 2
  private val MultiFieldVersion = "MULTI_FIELD"

  private val mapper = new ObjectMapper()

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
    val container = BinlogCodec.parse(bytes, path)
    ParsedContainer(
      Option(container.extras.get("version"))
        .exists(_.asText() == MultiFieldVersion),
      container.events
        .filter(_.kind == DeleteEventType)
        .map(_.payload)
        .filter(_.nonEmpty)
    )
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

  private final case class ParsedContainer(
      multiField: Boolean,
      payloads: Seq[Array[Byte]]
  )

}
