package com.zilliz.spark.connector.read

import org.apache.arrow.c.{ArrowArrayStream, ArrowSchema, Data}
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

/** Spark partition reader for milvus-segment-info `storage_version = 2`
  * (non-manifest packed parquet) segments.
  *
  * Unlike [[MilvusLoonPartitionReader]], which resolves a milvus-storage
  * `.milvus_manifest` file via `MilvusStorageManifest.getColumnGroupsScala`,
  * this reader is handed a pre-materialized set of [[V2ColumnGroup]]s recovered
  * from the snapshot AVRO (slot -> file paths) + one parquet footer's
  * `group_field_id_list` kv-metadata (slot -> real field IDs). The packed
  * reader then opens only the files for the requested columns.
  *
  * The reader iterates the Arrow stream sequentially (no vector search, no
  * filter pushdown). Backfill only needs the PK column plus positional row
  * metadata added by [[MilvusPartitionReaderFactory]], so the minimal shape
  * fits well.
  */
class MilvusLoonV2PartitionReader(
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

  // Field ID -> logical column name. StorageV2 packed-parquet files written
  // by milvus segcore use the field's logical name in the parquet schema
  // (with PARQUET:field_id metadata for cross-reference). The packed reader
  // matches columns by name, so we must pass logical names — NOT
  // field-id-as-string like the V3 reader does.
  private val fieldIdToName: Map[Long, String] = {
    val systemFields = Map(0L -> "RowID", 1L -> "Timestamp")
    val userFields =
      milvusSchema.fields.map(f => f.fieldID -> f.name).toMap
    systemFields ++ userFields
  }
  private val fieldNameToId: Map[String, Long] =
    fieldIdToName.map { case (id, name) => name -> id }

  // V3 readers pass a {sparkName -> fieldId-as-string} map to ArrowConverter
  // because their on-disk parquet uses fieldId-as-string column names. V2
  // packed parquet uses LOGICAL names (e.g. "ID"), so we must NOT remap —
  // ArrowConverter calls `root.getVector(field.name)` directly when the
  // mapping is empty, which is exactly what we need here. Remapping would
  // make it look up a non-existent column and silently null every cell.
  private val fieldNameToArrowColumn: Map[String, String] = Map.empty

  // Which column names to ask the packed reader for. Prefer explicit
  // projection from neededColumnFieldIds; fall back to sourceSchema's Spark
  // field names. In both cases we coerce to logical names that the parquet
  // actually carries.
  private val neededColumns: Array[String] = {
    val explicitNames: Seq[String] =
      if (neededColumnFieldIds.nonEmpty)
        neededColumnFieldIds.flatMap(fid => fieldIdToName.get(fid))
      else sourceSchema.fieldNames.toSeq.filter(fieldNameToId.contains)
    // Drop names that aren't declared by any of the v2 column groups —
    // `loon_reader_new` would reject unknown columns.
    val declared: Set[String] =
      columnGroups.flatMap(_.fieldIds.flatMap(fieldIdToName.get)).toSet
    explicitNames.filter(name => declared.contains(name)).toArray
  }

  // Native resource handles. Initialized to safe defaults so a partial-init
  // failure can roll back whatever was allocated so far. Spark only calls
  // close() on a fully-constructed reader; any throw from the init block
  // below has to release its own resources before bubbling out.
  private var arrowSchemaObj: ArrowSchema = null
  private var readerProperties: MilvusStorageProperties = null
  private var columnGroupsPtr: Long = 0L
  private var reader: MilvusStorageReader = null
  private var arrowArrayStream: ArrowArrayStream = null
  private var arrowReader: org.apache.arrow.vector.ipc.ArrowReader = null

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
      cg.fieldIds.flatMap(fieldIdToName.get).toArray
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

    val recordBatchReaderPtr = reader.getRecordBatchReaderScala()
    arrowArrayStream = ArrowArrayStream.wrap(recordBatchReaderPtr)
    arrowReader = Data.importArrayStream(allocator, arrowArrayStream)
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  private var _currentBatch: VectorSchemaRoot = {
    if (arrowReader.loadNextBatch()) arrowReader.getVectorSchemaRoot else null
  }
  private var _currentRowIndex: Int = 0

  override def next(): Boolean = {
    // Advance past exhausted batches.
    while (
      _currentBatch != null && _currentRowIndex >= _currentBatch.getRowCount
    ) {
      if (arrowReader.loadNextBatch()) {
        _currentBatch = arrowReader.getVectorSchemaRoot
        _currentRowIndex = 0
      } else {
        _currentBatch = null
      }
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
    if (arrowReader != null) {
      try arrowReader.close()
      catch { case e: Throwable => logWarning("close arrowReader failed", e) }
      arrowReader = null
    }
    if (arrowArrayStream != null) {
      try arrowArrayStream.close()
      catch {
        case e: Throwable => logWarning("close arrowArrayStream failed", e)
      }
      arrowArrayStream = null
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
