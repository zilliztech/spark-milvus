package com.zilliz.spark.connector.read

import org.apache.arrow.c.{
  ArrowArray,
  ArrowSchema,
  CDataDictionaryProvider,
  Data
}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.serde.ArrowConverter
import com.zilliz.spark.connector.MilvusOption
import io.milvus.grpc.schema.CollectionSchema
import io.milvus.storage.{
  ArrowUtils,
  MilvusStorageColumnGroups,
  MilvusStorageProperties,
  MilvusStorageReader,
  NativeLibraryLoader
}

object MilvusPackedV2PartitionReader {
  private[read] case class FieldMappings(
      fieldIdToName: Map[Long, String],
      fieldNameToId: Map[String, Long],
      fieldNameToArrowColumn: Map[String, String]
  )

  private[read] val SystemFieldAliases: Seq[(String, (Long, String))] = Seq(
    "RowID" -> (0L, "RowID"),
    "row_id" -> (0L, "RowID"),
    "rowid" -> (0L, "RowID"),
    "Timestamp" -> (1L, "Timestamp"),
    "timestamp" -> (1L, "Timestamp")
  )

  private[read] def buildFieldMappings(
      milvusSchema: CollectionSchema
  ): FieldMappings = {
    val systemFields = Map(0L -> "RowID", 1L -> "Timestamp")
    val userFields = milvusSchema.fields.map(f => f.fieldID -> f.name).toMap
    val fieldIdToName = systemFields ++ userFields
    val userFieldNames = milvusSchema.fields.map(_.name).toSet
    val systemAliases = SystemFieldAliases.filterNot { case (alias, _) =>
      userFieldNames.contains(alias)
    }
    val userFieldNameToId =
      milvusSchema.fields.map(f => f.name -> f.fieldID).toMap
    val systemFieldNameToId = systemAliases.map { case (alias, (id, _)) =>
      alias -> id
    }.toMap
    val systemFieldNameToArrowColumn = systemAliases.map {
      case (alias, (_, column)) => alias -> column
    }.toMap

    FieldMappings(
      fieldIdToName,
      systemFieldNameToId ++ userFieldNameToId,
      systemFieldNameToArrowColumn
    )
  }

  private[read] def resolveNeededColumns(
      sourceSchema: StructType,
      columnGroups: Seq[V2ColumnGroup],
      fieldMappings: FieldMappings,
      neededColumnFieldIds: Seq[Long]
  ): Array[String] = {
    val declaredFieldIds = columnGroups.flatMap(_.fieldIds).toSet
    val requestedFieldIds: Seq[Long] =
      if (neededColumnFieldIds.nonEmpty) {
        val missingIds = neededColumnFieldIds.filterNot(
          fieldMappings.fieldIdToName.contains
        )
        if (missingIds.nonEmpty) {
          throw new IllegalArgumentException(
            s"Packed V2 requested unknown field IDs: ${missingIds.distinct.mkString(",")}" +
              s"; schema field IDs=${fieldMappings.fieldIdToName.keys.toSeq.sorted.mkString(",")}"
          )
        }
        neededColumnFieldIds
      } else {
        val missingNames = sourceSchema.fieldNames.filterNot(
          fieldMappings.fieldNameToId.contains
        )
        if (missingNames.nonEmpty) {
          throw new IllegalArgumentException(
            s"Packed V2 requested unknown columns: ${missingNames.distinct.mkString(",")}" +
              s"; schema columns=${fieldMappings.fieldNameToId.keys.toSeq.sorted.mkString(",")}"
          )
        }
        sourceSchema.fieldNames.toSeq.map(fieldMappings.fieldNameToId)
      }
    val missingFieldIds = requestedFieldIds.filterNot(declaredFieldIds.contains)
    if (missingFieldIds.nonEmpty) {
      val missingColumns = missingFieldIds
        .flatMap(fieldMappings.fieldIdToName.get)
        .distinct
      val declaredColumns = declaredFieldIds
        .flatMap(fieldMappings.fieldIdToName.get)
        .toSeq
        .sorted
      throw new IllegalArgumentException(
        s"Packed V2 column groups do not contain requested columns: ${missingColumns
            .mkString(",")}" +
          s"; declared columns=${declaredColumns.mkString(",")}"
      )
    }
    requestedFieldIds.flatMap(fieldMappings.fieldIdToName.get).toArray
  }
}

/** Spark partition reader for milvus-segment-info `storage_version = 2`
  * (StorageV2, non-manifest packed parquet) segments.
  *
  * Unlike [[MilvusLoonPartitionReader]] (which handles StorageV3: manifest
  * based, resolves a milvus-storage `.milvus_manifest` file via
  * `MilvusStorageManifest.getColumnGroupsScala`), this reader is handed a
  * pre-materialized set of [[V2ColumnGroup]]s recovered from the snapshot AVRO
  * (slot -> file paths) + one parquet footer's `group_field_id_list`
  * kv-metadata (slot -> real field IDs). The packed reader then opens only the
  * files for the requested columns.
  *
  * The reader iterates the Arrow stream sequentially (no vector search, no
  * filter pushdown). Backfill only needs the PK column plus positional row
  * metadata added by [[MilvusPartitionReaderFactory]], so the minimal shape
  * fits well.
  */
class MilvusPackedV2PartitionReader(
    schema: StructType,
    columnGroups: Seq[V2ColumnGroup],
    milvusSchema: CollectionSchema,
    milvusOption: MilvusOption,
    neededColumnFieldIds: Seq[Long]
) extends PartitionReader[InternalRow]
    with Logging {

  NativeLibraryLoader.loadLibrary()

  private val allocator = ArrowUtils.getAllocator
  private val sourceSchema = schema

  private val fieldMappings =
    MilvusPackedV2PartitionReader.buildFieldMappings(milvusSchema)
  private val fieldNameToArrowColumn = fieldMappings.fieldNameToArrowColumn

  // Which column names to ask the packed reader for. Prefer explicit
  // projection from neededColumnFieldIds; fall back to sourceSchema's Spark
  // field names. In both cases we coerce to logical names that the parquet
  // actually carries.
  private val neededColumns: Array[String] =
    MilvusPackedV2PartitionReader.resolveNeededColumns(
      sourceSchema,
      columnGroups,
      fieldMappings,
      neededColumnFieldIds
    )

  // V2 packed parquet uses logical names (e.g. "ID"). When callers request
  // aliases such as row_id/timestamp, ArrowConverter must look up the canonical
  // packed column names RowID/Timestamp instead.

  // Native resource handles. Initialized to safe defaults so a partial-init
  // failure can roll back whatever was allocated so far. Spark only calls
  // close() on a fully-constructed reader; any throw from the init block
  // below has to release its own resources before bubbling out.
  private var arrowSchemaObj: ArrowSchema = null
  private var readerProperties: MilvusStorageProperties = null
  private var columnGroupsPtr: Long = 0L
  private var reader: MilvusStorageReader = null
  // Per-batch record batch reader handle (see milvus-storage
  // loon_record_batch_reader_*). We deliberately avoid the ArrowArrayStream
  // path because Arrow Java's `ArrowReader.loadNextBatch` shares one
  // VectorSchemaRoot across batches and ignores the ArrowArray `offset`
  // field — when the underlying C++ reader emits `RecordBatch::Slice`
  // results, every batch after the first would show the same data.
  private var rbrHandle: Long = 0L
  private var dictProvider: CDataDictionaryProvider = null

  try {
    // Arrow schema for the read: columns named by their logical names so the
    // packed reader can find them by name in the on-disk parquet.
    val arrowSchema =
      com.zilliz.spark.connector.MilvusSchemaUtil.convertToArrowSchema(
        milvusSchema
      )
    arrowSchemaObj = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, arrowSchemaObj)

    readerProperties = Properties.fromMilvusOption(milvusOption)

    // Build LoonColumnGroups directly from the materialized v2 layout. Each
    // group's `columns` is the LOGICAL names of its fields (matching what
    // milvus segcore wrote into the parquet). Row counts (per file) come
    // from the AVRO's AvroBinlog.entries_num — required because the packed
    // reader rejects negative end indices.
    val cols = columnGroups.map { cg =>
      cg.fieldIds.flatMap(fieldMappings.fieldIdToName.get).toArray
    }.toArray
    val files = columnGroups.map(_.filePaths.toArray).toArray
    val rowCounts = columnGroups.map { cg =>
      require(
        cg.fileRowCounts.size == cg.filePaths.size,
        s"V2ColumnGroup with fields=${cg.fieldIds} has ${cg.filePaths.size} files " +
          s"but ${cg.fileRowCounts.size} row counts; both must match"
      )
      cg.fileRowCounts.toArray
    }.toArray
    columnGroupsPtr =
      MilvusStorageColumnGroups.createFromGroups(cols, files, rowCounts)

    reader = new MilvusStorageReader()
    reader.create(
      columnGroupsPtr,
      arrowSchemaObj.memoryAddress(),
      neededColumns,
      readerProperties
    )
    if (!reader.isValid) {
      throw new IllegalStateException(
        "Failed to create MilvusStorageReader for V2 packed segment"
      )
    }

    rbrHandle = reader.openRecordBatchReaderScala()
    dictProvider = new CDataDictionaryProvider()
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  // Per-batch state: each loadNextBatch() imports a FRESH RecordBatch
  // into a NEW VectorSchemaRoot. The previous root (if any) is closed
  // before the next import so buffers are correctly reclaimed. This
  // intentionally does not reuse a single root across batches — Arrow
  // Java's shared-root import drops the per-batch `offset`.
  //
  // Initialized inside its own try/catch: Spark only calls close() on
  // a fully-constructed reader, so a throw from the first loadNextBatch
  // (e.g. a malformed parquet chunk) would otherwise leak every native
  // handle allocated in the main init block above.
  private var _currentBatch: VectorSchemaRoot = null
  private var _currentRowIndex: Int = 0
  try {
    _currentBatch = loadNextBatch()
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  private def loadNextBatch(): VectorSchemaRoot = {
    if (rbrHandle == 0L) return null

    val cArr = ArrowArray.allocateNew(allocator)
    val cSchema = ArrowSchema.allocateNew(allocator)
    var gotBatch = false
    try {
      gotBatch = reader.readNextBatchScala(
        rbrHandle,
        cArr.memoryAddress(),
        cSchema.memoryAddress()
      )
      if (!gotBatch) {
        null
      } else {
        // Import the ArrowArray+ArrowSchema as a RecordBatch-backed
        // VectorSchemaRoot. Import takes ownership of the release
        // callbacks (moves them out), so the subsequent `.close()` on
        // cArr / cSchema is a no-op w.r.t. the live buffers; closing
        // the returned root (in close()/loadNextBatch) is what releases
        // them.
        Data.importVectorSchemaRoot(allocator, cArr, cSchema, dictProvider)
      }
    } finally {
      // Always close the C-struct wrappers: after a successful import
      // their release callback has been moved into the root and this is
      // a no-op; on failure we need to release them to avoid leaks.
      try cArr.close()
      catch { case e: Throwable => logWarning("close cArr failed", e) }
      try cSchema.close()
      catch { case e: Throwable => logWarning("close cSchema failed", e) }
    }
  }

  override def next(): Boolean = {
    // Advance past exhausted batches; close the previous root before
    // loading the next so we bound JVM direct memory to one live batch.
    while (
      _currentBatch != null && _currentRowIndex >= _currentBatch.getRowCount
    ) {
      val exhausted = _currentBatch
      _currentBatch = null
      try exhausted.close()
      catch {
        case e: Throwable => logWarning("close exhausted batch failed", e)
      }
      _currentBatch = loadNextBatch()
      _currentRowIndex = 0
    }
    _currentBatch != null && _currentRowIndex < _currentBatch.getRowCount
  }

  override def get(): InternalRow = {
    if (_currentBatch == null) {
      throw new IllegalStateException("No batch loaded")
    }
    val row = ArrowConverter.arrowToInternalRow(
      _currentBatch,
      _currentRowIndex,
      sourceSchema,
      fieldNameToArrowColumn
    )
    _currentRowIndex += 1
    row
  }

  override def close(): Unit = releaseAll()

  // Each native resource is released in its own try-catch so one failing
  // release doesn't strand the rest. Also reused by the constructor's
  // failure path to roll back a partial init.
  private def releaseAll(): Unit = {
    if (_currentBatch != null) {
      try _currentBatch.close()
      catch { case e: Throwable => logWarning("close currentBatch failed", e) }
      _currentBatch = null
    }
    if (rbrHandle != 0L) {
      try reader.destroyRecordBatchReaderScala(rbrHandle)
      catch { case e: Throwable => logWarning("destroy rbrHandle failed", e) }
      rbrHandle = 0L
    }
    if (dictProvider != null) {
      try dictProvider.close()
      catch { case e: Throwable => logWarning("close dictProvider failed", e) }
      dictProvider = null
    }
    if (reader != null) {
      try reader.destroy()
      catch { case e: Throwable => logWarning("destroy reader failed", e) }
      reader = null
    }
    if (columnGroupsPtr != 0L) {
      try MilvusStorageColumnGroups.destroy(columnGroupsPtr)
      catch {
        case e: Throwable => logWarning("destroy columnGroupsPtr failed", e)
      }
      columnGroupsPtr = 0L
    }
    if (arrowSchemaObj != null) {
      try arrowSchemaObj.close()
      catch {
        case e: Throwable => logWarning("close arrowSchemaObj failed", e)
      }
      arrowSchemaObj = null
    }
    if (readerProperties != null) {
      try readerProperties.free()
      catch {
        case e: Throwable => logWarning("free readerProperties failed", e)
      }
      readerProperties = null
    }
  }
}
