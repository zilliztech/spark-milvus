package com.zilliz.spark.connector.read

import org.apache.arrow.c.{
  ArrowArray,
  ArrowSchema,
  CDataDictionaryProvider,
  Data
}
import org.apache.arrow.vector.{
  BigIntVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.serde.ArrowConverter
import com.zilliz.spark.connector.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import io.milvus.storage.{
  ArrowUtils,
  MilvusStorageColumnGroups,
  MilvusStorageProperties,
  MilvusStorageReader,
  NativeLibraryLoader
}

object MilvusPackedV2PartitionReader {
  private val ToleratedUnmappedColumns = Set("$meta")

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

  private[read] def projectedFieldIds(
      sourceSchema: StructType,
      fieldMappings: FieldMappings,
      neededColumnFieldIds: Seq[Long],
      applyDeletes: Boolean,
      deletePlan: MilvusDeletePlan,
      pkFieldId: Long,
      tsFieldId: Long = 1L
  ): Seq[Long] = {
    if (!applyDeletes || deletePlan.isEmpty) {
      neededColumnFieldIds
    } else if (neededColumnFieldIds.nonEmpty) {
      (neededColumnFieldIds ++ Seq(pkFieldId, tsFieldId)).distinct
    } else {
      (sourceSchema.fieldNames.toSeq.flatMap(
        fieldMappings.fieldNameToId.get
      ) ++ Seq(pkFieldId, tsFieldId)).distinct
    }
  }

  private[read] def rowDeleted(
      deletePlan: MilvusDeletePlan,
      pkField: FieldSchema,
      pkVector: org.apache.arrow.vector.ValueVector,
      tsVector: BigIntVector,
      rowIndex: Int,
      pkColumnName: String
  ): Boolean = {
    if (pkVector.isNull(rowIndex) || tsVector.isNull(rowIndex)) {
      false
    } else {
      val rowTs = tsVector.get(rowIndex)
      pkField.dataType match {
        case DataType.Int64 =>
          deletePlan.containsLongPk(
            pkVector.asInstanceOf[BigIntVector].get(rowIndex),
            rowTs
          )
        case DataType.VarChar =>
          val value = pkVector match {
            case v: VarCharVector =>
              UTF8String.fromBytes(v.get(rowIndex)).toString
            case v: VarBinaryVector =>
              UTF8String.fromBytes(v.get(rowIndex)).toString
            case other =>
              throw new IllegalStateException(
                s"Packed V2 delete filtering expected VarChar/VarBinary PK vector for $pkColumnName, got ${other.getClass.getSimpleName}"
              )
          }
          deletePlan.containsStringPk(value, rowTs)
        case other =>
          throw new IllegalArgumentException(
            s"Packed V2 delete filtering only supports Int64/VarChar PKs, got $other"
          )
      }
    }
  }

  private[read] def resolveNeededColumns(
      sourceSchema: StructType,
      columnGroups: Seq[V2ColumnGroup],
      fieldMappings: FieldMappings,
      neededColumnFieldIds: Seq[Long]
  ): Array[String] = {
    if (columnGroups.isEmpty) {
      return Array.empty
    }

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
        val missingNames = sourceSchema.fieldNames.filterNot(name =>
          fieldMappings.fieldNameToId.contains(name) ||
            ToleratedUnmappedColumns.contains(name)
        )
        if (missingNames.nonEmpty) {
          throw new IllegalArgumentException(
            s"Packed V2 requested unknown columns: ${missingNames.distinct.mkString(",")}" +
              s"; schema columns=${fieldMappings.fieldNameToId.keys.toSeq.sorted.mkString(",")}"
          )
        }
        sourceSchema.fieldNames.toSeq.flatMap(fieldMappings.fieldNameToId.get)
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

class MilvusPackedV2PartitionReader(
    schema: StructType,
    columnGroups: Seq[V2ColumnGroup],
    milvusSchema: CollectionSchema,
    milvusOption: MilvusOption,
    neededColumnFieldIds: Seq[Long],
    applyDeletes: Boolean,
    deletePlan: MilvusDeletePlan
) extends PartitionReader[InternalRow]
    with Logging {

  NativeLibraryLoader.loadLibrary()

  private val allocator = ArrowUtils.getAllocator
  private val sourceSchema = schema
  private val pkField = milvusSchema.fields.find(_.isPrimaryKey).getOrElse {
    throw new IllegalArgumentException("No primary key field found in schema")
  }
  private val fieldMappings =
    MilvusPackedV2PartitionReader.buildFieldMappings(milvusSchema)
  private val fieldNameToArrowColumn = fieldMappings.fieldNameToArrowColumn
  private val effectiveNeededColumnFieldIds =
    MilvusPackedV2PartitionReader.projectedFieldIds(
      sourceSchema,
      fieldMappings,
      neededColumnFieldIds,
      applyDeletes,
      deletePlan,
      pkField.fieldID,
      tsFieldId = 1L
    )
  private val neededColumns: Array[String] =
    MilvusPackedV2PartitionReader.resolveNeededColumns(
      sourceSchema,
      columnGroups,
      fieldMappings,
      effectiveNeededColumnFieldIds
    )
  private val pkColumnName =
    fieldMappings.fieldIdToName.getOrElse(pkField.fieldID, pkField.name)

  private var arrowSchemaObj: ArrowSchema = null
  private var readerProperties: MilvusStorageProperties = null
  private var columnGroupsPtr: Long = 0L
  private var reader: MilvusStorageReader = null
  private var rbrHandle: Long = 0L
  private var dictProvider: CDataDictionaryProvider = null

  try {
    val arrowSchema =
      com.zilliz.spark.connector.MilvusSchemaUtil.convertToArrowSchema(
        milvusSchema
      )
    arrowSchemaObj = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, arrowSchemaObj)

    readerProperties = Properties.fromMilvusOption(milvusOption)

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

  /** Total rows the packed reader must deliver, recovered from the per-file row
    * counts fed to `MilvusStorageColumnGroups.createFromGroups`. All column
    * groups of a segment carry the same row total, so the head group's sum is
    * the segment-wide expectation.
    *
    * This is the "not silently short" guard: a pre-fix milvus-storage library
    * (before milvus-storage#657) drops every file after the first in a
    * multi-file column group, and the native cross-group row check cannot catch
    * it when all groups share the same file split. Without a delivered-row
    * comparison the scan would end on the first null batch and return a short
    * DataFrame with no error and no log.
    */
  private val expectedTotalRows: Long =
    columnGroups.headOption.map(_.fileRowCounts.sum).getOrElse(0L)

  /** Physical rows observed so far (regardless of delete filtering): the sum of
    * every batch's row count as batches are exhausted. Delete filtering skips
    * rows in [[next]] but never removes them from a batch, so this counter
    * reaches the native reader's full delivered row count at EOF.
    */
  private var observedPhysicalRows: Long = 0L
  private var rowCountVerified: Boolean = false

  private var currentBatch: VectorSchemaRoot = null
  private var currentRowIndex: Int = 0
  private var currentBatchStartRowOffset: Long = 0L
  private var _lastReturnedRowOffset: Long = -1L

  def lastReturnedRowOffset: Long = _lastReturnedRowOffset

  try {
    currentBatch = loadNextBatch()
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
        Data.importVectorSchemaRoot(allocator, cArr, cSchema, dictProvider)
      }
    } finally {
      try cArr.close()
      catch { case e: Throwable => logWarning("close cArr failed", e) }
      try cSchema.close()
      catch { case e: Throwable => logWarning("close cSchema failed", e) }
    }
  }

  override def next(): Boolean = {
    while (true) {
      while (
        currentBatch != null && currentRowIndex >= currentBatch.getRowCount
      ) {
        val exhausted = currentBatch
        currentBatch = null
        currentBatchStartRowOffset += exhausted.getRowCount.toLong
        observedPhysicalRows += exhausted.getRowCount.toLong
        try exhausted.close()
        catch {
          case e: Throwable => logWarning("close exhausted batch failed", e)
        }
        currentBatch = loadNextBatch()
        currentRowIndex = 0
      }

      if (currentBatch == null) {
        verifyRowCount()
        return false
      }

      if (applyDeletes && !deletePlan.isEmpty && isDeleted(currentRowIndex)) {
        currentRowIndex += 1
      } else {
        return true
      }
    }
    false
  }

  override def get(): InternalRow = {
    if (currentBatch == null) {
      throw new IllegalStateException("No batch loaded")
    }
    _lastReturnedRowOffset = currentBatchStartRowOffset + currentRowIndex
    val row = ArrowConverter.arrowToInternalRow(
      currentBatch,
      currentRowIndex,
      sourceSchema,
      fieldNameToArrowColumn
    )
    currentRowIndex += 1
    row
  }

  override def close(): Unit = releaseAll()

  /** Guard the no-partial-read contract at EOF: the packed reader must have
    * delivered exactly `expectedTotalRows` physical rows. A short scan is
    * refused loudly instead of silently returning fewer rows than the column
    * groups declare — the failure mode of a pre-fix milvus-storage library
    * (milvus-storage#657) on multi-file column groups.
    *
    * Runs once: Spark calls `next()` to exhaustion for a full scan; a `close()`
    * before EOF (e.g. a caller that stops early) deliberately skips the check.
    */
  private def verifyRowCount(): Unit = {
    if (rowCountVerified) return
    rowCountVerified = true
    if (observedPhysicalRows != expectedTotalRows) {
      throw new IllegalStateException(
        s"Packed V2 reader delivered $observedPhysicalRows rows for segment " +
          s"with ${columnGroups.size} column group(s) spanning " +
          s"${columnGroups.map(_.filePaths.size).sum} file(s), expected " +
          s"$expectedTotalRows (sum of per-file row counts). This usually means " +
          "the native milvus-storage library predates the BuildLoonColumnGroups " +
          "per-file range fix (milvus-storage#657) and silently dropped rows " +
          "after the first file of a column group; refusing to return a short " +
          "DataFrame"
      )
    }
  }

  private def isDeleted(rowIndex: Int): Boolean = {
    val pkVector = currentBatch.getVector(pkColumnName)
    if (pkVector == null) {
      throw new IllegalStateException(
        s"Packed V2 delete filtering requires PK column $pkColumnName to be loaded"
      )
    }
    val rawTsVector = currentBatch.getVector("Timestamp")
    if (rawTsVector == null) {
      throw new IllegalStateException(
        "Packed V2 delete filtering requires Timestamp column to be loaded"
      )
    }
    MilvusPackedV2PartitionReader.rowDeleted(
      deletePlan,
      pkField,
      pkVector,
      rawTsVector.asInstanceOf[BigIntVector],
      rowIndex,
      pkColumnName
    )
  }

  private def releaseAll(): Unit = {
    if (currentBatch != null) {
      try currentBatch.close()
      catch { case e: Throwable => logWarning("close currentBatch failed", e) }
      currentBatch = null
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
