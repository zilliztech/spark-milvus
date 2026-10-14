package com.zilliz.spark.connector.apps.backfill

import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}
import com.zilliz.spark.connector.write.{
  MilvusV2Writer,
  MilvusV3CommitMessage,
  MilvusV3WriterFactory,
  V2BinlogFile,
  WriteSchema
}
import io.milvus.grpc.schema.CollectionSchema

/** Distributes merged rows by segment and writes the new columns in snapshot
  * row order. V2 writes packed column groups; V3 advances the segment manifest.
  * The caller owns the run's job manifest and registration lifecycle.
  */
private[backfill] object BackfillSegmentWrite {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Job-internal columns in the write projection, never user target fields.
    * The marker is added to the backfill side before the left join so we can
    * count how many snapshot rows found a join-key match (non-null marker →
    * matched). Kept on the DataFrame all the way down to the per-segment
    * partition function, and stripped from the projection written to parquet.
    */
  private[backfill] val MatchFlagCol = "__bf_matched__"
  private[backfill] val SegmentIdCol = MilvusOption.MilvusExtraColumnSegmentID
  private[backfill] val RowOffsetCol = MilvusOption.MilvusExtraColumnRowOffset

  /** Per-field flag columns added in any mode that reads snapshot-side target
    * values (coalesce + overwrite). For each new field, one boolean marker
    * records whether the written value came from the snapshot and another
    * whether it came from the backfill data file. Semantics differ per mode:
    *   - coalesce: usedSnapshot = snapshot non-null; usedBf = snapshot null AND
    *     bf non-null
    *   - overwrite: usedSnapshot = join key unmatched; usedBf = join key
    *     matched
    * Used by the writer to tally the per-field `usedSourceByField` /
    * `usedDataFileByField` counts surfaced in `SegmentBackfillResult`.
    */
  private[backfill] def usedSnapshotCol(field: String): String =
    s"__bf_used_src_${field}__"
  private[backfill] def usedBfCol(field: String): String =
    s"__bf_used_bf_${field}__"

  /** Process each segment separately by distributing to Spark executors Each
    * segment is processed by exactly one FFI writer on a single executor
    * Supports multi-partition collections by tracking partition ID per segment
    */
  def write(
      spark: SparkSession,
      joinedDF: DataFrame,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      segmentBasePathMap: Map[Long, String],
      v2SegmentIdSet: Set[Long],
      config: BackfillConfig,
      newFieldNames: Seq[String],
      collectionSchema: CollectionSchema,
      targetFieldOverrides: Map[
        String,
        StructField
      ]
  ): Either[BackfillError, Map[Long, SegmentBackfillResult]] = {

    try {
      // Prepare data: select only needed columns and add _segment_id for
      // partitioning. Trailing column is the backfill match flag — retained
      // here so executors can count join-key-matched rows, stripped from the
      // projection that reaches the writer. In any mode that reads snapshot fields
      // (coalesce / overwrite), two extra boolean flag columns per new field
      // follow the match flag (usedSnapshot, usedBf interleaved, in
      // `newFieldNames` order) so the writer can compute per-field
      // usedSourceByField / usedDataFileByField counts.
      val readsSnapshotFields = config.readsSnapshotFields
      val flagColNames: Seq[String] =
        if (readsSnapshotFields)
          newFieldNames.flatMap(n => Seq(usedSnapshotCol(n), usedBfCol(n)))
        else Seq.empty
      val preparedDF = joinedDF
        .select(
          (Seq(SegmentIdCol, RowOffsetCol) ++ newFieldNames ++ Seq(
            MatchFlagCol
          ) ++ flagColNames).map(col): _*
        )

      // Get the schema for new fields only (without _segment_id, _row_offset,
      // or the match flag)
      // The write schema: the new columns checked against the collection and
      // carrying their field ids and dimensions, which is what names the
      // columns in the written files.
      val targetSchema =
        try
          WriteSchema.resolve(
            StructType(
              newFieldNames.map(fieldName =>
                targetFieldOverrides.getOrElse(
                  fieldName,
                  preparedDF.schema.fields.find(_.name == fieldName).get
                )
              )
            ),
            collectionSchema,
            WriteSchema.Mode.Columns
          )
        catch {
          case e: IllegalArgumentException =>
            return Left(SchemaValidationError(e.getMessage))
        }

      val segmentIds = segmentToPartitionMap.keys.toArray
      val segmentPartitioner = new SegmentPartitioner(segmentIds)

      // Repartition using custom partitioner, then sort by _row_offset within each partition
      // CRITICAL: .copy() is required because queryExecution.toRdd produces an iterator
      // that reuses the same UnsafeRow buffer. Without copy, keyBy/partitionBy's
      // ExternalSorter stores references to the same mutable buffer, causing all
      // rows to contain the last row's data.
      val repartitionedRDD = preparedDF.queryExecution.toRdd
        .map(_.copy()) // Materialize each row to avoid UnsafeRow reuse
        .keyBy(_.getLong(0)) // _segment_id is at index 0
        .partitionBy(segmentPartitioner)
        .values
        .mapPartitions(iter =>
          iter.toSeq.sortBy(_.getLong(1)).iterator
        ) // Sort by _row_offset

      // Broadcast configuration to executors
      val broadcastConfig = spark.sparkContext.broadcast(config)
      val broadcastCollectionID = spark.sparkContext.broadcast(collectionID)
      val broadcastSegmentToPartitionMap =
        spark.sparkContext.broadcast(segmentToPartitionMap)
      val broadcastSegmentBasePathMap =
        spark.sparkContext.broadcast(segmentBasePathMap)
      val broadcastV2SegmentIdSet = spark.sparkContext.broadcast(v2SegmentIdSet)
      val broadcastTargetSchema = spark.sparkContext.broadcast(targetSchema)

      val results = repartitionedRDD
        .mapPartitions { iter =>
          if (!iter.hasNext) Iterator.empty
          else
            processSegmentPartition(
              iter,
              broadcastConfig.value,
              broadcastCollectionID.value,
              broadcastSegmentToPartitionMap.value,
              broadcastSegmentBasePathMap.value,
              broadcastV2SegmentIdSet.value,
              broadcastTargetSchema.value
            )
        }
        .collect()

      // Cleanup broadcast variables
      broadcastConfig.unpersist()
      broadcastCollectionID.unpersist()
      broadcastSegmentToPartitionMap.unpersist()
      broadcastSegmentBasePathMap.unpersist()
      broadcastV2SegmentIdSet.unpersist()
      broadcastTargetSchema.unpersist()

      // Check for failures
      val failures = results.filter(_._2.isDefined)
      if (failures.nonEmpty) {
        val firstFailure = failures.head
        val error = firstFailure._2.get
        return Left(
          WriteError(
            segmentId = firstFailure._1.segmentId,
            outputPath = firstFailure._1.outputPath,
            message =
              s"Failed to write ${failures.length} segment(s): ${error.getMessage}",
            cause = Some(error)
          )
        )
      }

      // Extract successful results
      val successfulResults = results.map { case (result, _) =>
        result.segmentId -> result
      }.toMap

      // Log summary statistics
      val totalTime = results.map(_._1.executionTimeMs).sum
      val avgTime = if (results.nonEmpty) totalTime / results.length else 0
      val totalRows = results.map(_._1.rowCount).sum
      val totalSnapshotRows = results.map(_._1.sourceRowCount).sum
      val totalMatched = results.map(_._1.matchedRowCount).sum
      val matchRateStr =
        if (totalSnapshotRows > 0)
          f"${totalMatched.toDouble / totalSnapshotRows * 100}%.2f%%"
        else "n/a"

      logger.info("=== Backfill Summary ===")
      logger.info(s"Total segments: ${results.length}")
      logger.info(s"Total source rows: $totalSnapshotRows")
      logger.info(
        s"Total matched rows: $totalMatched (match rate: $matchRateStr)"
      )
      logger.info(s"Total rows written: $totalRows")
      logger.info(s"Total time for all segments: ${totalTime}ms")
      logger.info(s"Average time per segment: ${avgTime}ms")
      if (readsSnapshotFields) {
        logger.info(
          s"Per-field provenance (mode=${config.mode}, aggregated):"
        )
        newFieldNames.foreach { f =>
          val snapshotCount =
            results.map(_._1.usedSourceByField.getOrElse(f, 0L)).sum
          val df = results.map(_._1.usedDataFileByField.getOrElse(f, 0L)).sum
          val nullOut = totalSnapshotRows - snapshotCount - df
          logger.info(
            s"  $f: source=$snapshotCount, dataFile=$df, null=$nullOut"
          )
        }
      }
      results.sortBy(_._1.segmentId).foreach { case (r, _) =>
        val segRate =
          if (r.sourceRowCount > 0)
            f"${r.matchedRowCount.toDouble / r.sourceRowCount * 100}%.2f%%"
          else "n/a"
        logger.info(
          s"Segment ${r.segmentId}: source=${r.sourceRowCount}, matched=${r.matchedRowCount} ($segRate), written=${r.rowCount}"
        )
      }

      Right(successfulResults)

    } catch {
      case e: Exception =>
        logger.error("Failed to process segments", e)
        Left(
          SegmentProcessingError(
            segmentId = -1,
            message = s"Failed to process segments: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** The writer of one segment's new column groups, built on the executor. The
    * rows go into the segment's own directory (`milvus.writer.customPath`) and
    * the run's job manifest is written once by the driver, so the segment write
    * opens no job of its own and writes nothing under the staging prefix. The
    * explicit `fs.*` of `writeOptions` is the whole storage configuration.
    */
  private[backfill] def segmentWriter(
      targetSchema: StructType,
      writeOptions: Map[String, String],
      jobId: String
  ): DataWriter[InternalRow] =
    new MilvusV3WriterFactory(
      targetSchema,
      MilvusOption(new CaseInsensitiveStringMap(writeOptions.asJava)),
      jobId,
      HadoopStorageKeys.canonicalProperties(writeOptions)
    ).createWriter(0, System.currentTimeMillis())

  private def processSegmentPartition(
      iter: Iterator[InternalRow],
      config: BackfillConfig,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      segmentBasePathMap: Map[Long, String],
      v2SegmentIdSet: Set[Long],
      targetSchema: StructType
  ): Iterator[(SegmentBackfillResult, Option[Throwable])] = {

    val firstRow = iter.next()
    val segmentID = firstRow.getLong(0)
    val partitionID = segmentToPartitionMap(segmentID)
    val startTime = System.currentTimeMillis()

    // StorageV2 (packed-parquet, no manifest) segments: write one parquet per
    // new field at files/insert_log/.../{fieldID}/{logID} via the V2 writer.
    if (v2SegmentIdSet.contains(segmentID)) {
      return processV2SegmentPartition(
        iter,
        firstRow,
        segmentID,
        partitionID,
        collectionID,
        targetSchema,
        config,
        startTime
      )
    }

    // Create writer — use manifest basePath if available, otherwise generate path
    val writeOptions = segmentBasePathMap.get(segmentID) match {
      case Some(basePath) =>
        config.getS3WriteOptionsForBasePath(basePath)
      case None =>
        config.getS3WriteOptions(collectionID, partitionID, segmentID)
    }
    val outputPath = writeOptions("milvus.writer.customPath")

    val writer = segmentWriter(
      targetSchema,
      writeOptions,
      config.jobId.getOrElse(
        throw new IllegalStateException(
          "the job id is resolved at the start of run"
        )
      )
    )

    try {
      val counts = writeRows(
        Iterator.single(firstRow) ++ iter,
        targetSchema,
        config.readsSnapshotFields
      )(writer.write)

      val commitMessage = writer.commit()
      val (manifestPaths, committedVersion) = commitMessage match {
        case msg: MilvusV3CommitMessage =>
          (Seq(msg.manifestPath), msg.committedVersion)
        case _ => (Seq.empty[String], -1L)
      }

      writer.close()

      Iterator.single(
        (
          SegmentBackfillResult(
            segmentId = segmentID,
            rowCount = counts.rows,
            manifestPaths = manifestPaths,
            outputPath = outputPath,
            executionTimeMs = System.currentTimeMillis() - startTime,
            committedVersion = committedVersion,
            sourceRowCount = counts.rows,
            matchedRowCount = counts.matched,
            usedSourceByField = counts.usedSnapshot,
            usedDataFileByField = counts.usedBackfill
          ),
          None
        )
      )

    } catch {
      case e: Exception =>
        writer.abort()
        writer.close()
        Iterator.single(
          (
            SegmentBackfillResult(
              segmentId = segmentID,
              rowCount = 0,
              manifestPaths = Seq.empty,
              outputPath = outputPath,
              executionTimeMs = System.currentTimeMillis() - startTime
            ),
            Some(e)
          )
        )
    }
  }

  /** StorageV2 write path: writes one parquet per new field under
    * `files/insert_log/{coll}/{part}/{seg}/{newFieldID}/{logID}` via
    * [[com.zilliz.spark.connector.write.MilvusV2Writer]]. Backfill always emits
    * single-field column groups, so `columnGroupID` in the path equals the new
    * field's ID (milvus convention for 1-field groups).
    */
  private def processV2SegmentPartition(
      iter: Iterator[InternalRow],
      firstRow: InternalRow,
      segmentID: Long,
      partitionID: Long,
      collectionID: Long,
      targetSchema: StructType,
      config: BackfillConfig,
      startTime: Long
  ): Iterator[(SegmentBackfillResult, Option[Throwable])] = {

    // Field ids in targetSchema order, from the metadata WriteSchema put there.
    val fieldNames = targetSchema.fieldNames.toSeq
    val fieldIds = targetSchema.fields.toSeq.map(
      _.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
    )

    // Simple monotonic logID allocator seeded by task-start nanos. Plan
    // names this as a future injection point (caller-provided global ID),
    // but a monotonic local sequence is sufficient for the single-task
    // single-segment scope (no collisions expected within one file).
    val logIdBase = System.nanoTime()
    val logIdCounter = new AtomicLong(logIdBase)
    val allocator: () => Long = () => logIdCounter.incrementAndGet()

    val outputRoot = BackfillStorage.storagePath(
      config,
      s"${config.s3RootPath.stripSuffix("/")}/insert_log/$collectionID/$partitionID/$segmentID"
    )

    // Reuse the same MilvusOption plumbing the V3 writer uses — the new V2
    // writer talks to S3 via Arrow's filesystem (inside milvus-storage), so
    // it needs the FS config, not Hadoop S3A config.
    val writeOptions = config.getS3WriteOptionsForBasePath(
      s"${config.s3RootPath.stripSuffix("/")}/insert_log/$collectionID/$partitionID/$segmentID"
    )
    val milvusOption = MilvusOption(
      new CaseInsensitiveStringMap(writeOptions.asJava)
    )

    val writer = new MilvusV2Writer(
      collectionId = collectionID,
      partitionId = partitionID,
      segmentId = segmentID,
      newFieldNames = fieldNames,
      newFieldIds = fieldIds,
      targetSchema = targetSchema,
      milvusOption = milvusOption,
      allocateLogId = allocator
    )

    try {
      val counts = writeRows(
        Iterator.single(firstRow) ++ iter,
        targetSchema,
        config.readsSnapshotFields
      )(writer.write)
      val produced: Seq[V2BinlogFile] = writer.close()

      val manifestPaths = produced.map(_.path)
      // Single-field column groups (backfill invariant): one V2ColumnGroup
      // artifact per produced file, with fieldIds = [fieldId].
      val columnGroupArtifacts = produced.map { pf =>
        V2ColumnGroupArtifact(
          fieldIds = Seq(pf.fieldId),
          binlogFiles = Seq(pf.path),
          rowCount = pf.rowsWritten
        )
      }
      Iterator.single(
        (
          SegmentBackfillResult(
            segmentId = segmentID,
            rowCount = counts.rows,
            manifestPaths = manifestPaths,
            outputPath = outputRoot,
            executionTimeMs = System.currentTimeMillis() - startTime,
            committedVersion = -1L,
            v2Artifact = Some(
              V2SegmentArtifact(
                segmentId = segmentID,
                storageVersion = 2L,
                columnGroups = columnGroupArtifacts
              )
            ),
            sourceRowCount = counts.rows,
            matchedRowCount = counts.matched,
            usedSourceByField = counts.usedSnapshot,
            usedDataFileByField = counts.usedBackfill
          ),
          None
        )
      )
    } catch {
      case e: Exception =>
        writer.abort()
        Iterator.single(
          (
            SegmentBackfillResult(
              segmentId = segmentID,
              rowCount = 0,
              manifestPaths = Seq.empty,
              outputPath = outputRoot,
              executionTimeMs = System.currentTimeMillis() - startTime
            ),
            Some(e)
          )
        )
    }
  }

  private[backfill] final case class RowCounts(
      rows: Long,
      matched: Long,
      usedSnapshot: Map[String, Long],
      usedBackfill: Map[String, Long]
  )

  /** Strip the internal columns and count provenance for either storage
    * version. The input is already in snapshot row order. Count a written row
    * only after the writer accepts it; a write failure propagates to that
    * writer's abort.
    */
  private[backfill] def writeRows(
      rows: Iterator[InternalRow],
      schema: StructType,
      readsSnapshotFields: Boolean
  )(write: InternalRow => Unit): RowCounts = {
    val fields = schema.fields
    val fieldIndices = fields.indices
    val matchFlag = 2 + fields.length
    val usedSnapshot = Array.fill(fields.length)(0L)
    val usedBackfill = Array.fill(fields.length)(0L)
    var written = 0L
    var matched = 0L
    rows.foreach { row =>
      val values =
        fieldIndices.map(i => row.get(i + 2, fields(i).dataType)).toArray
      if (!row.isNullAt(matchFlag)) matched += 1
      if (readsSnapshotFields) {
        var i = 0
        while (i < fields.length) {
          val snapshotFlag = matchFlag + 1 + 2 * i
          val backfillFlag = snapshotFlag + 1
          if (!row.isNullAt(snapshotFlag) && row.getBoolean(snapshotFlag))
            usedSnapshot(i) += 1
          if (!row.isNullAt(backfillFlag) && row.getBoolean(backfillFlag))
            usedBackfill(i) += 1
          i += 1
        }
      }
      write(new GenericInternalRow(values))
      written += 1
    }
    val names = schema.fieldNames
    RowCounts(
      written,
      matched,
      if (readsSnapshotFields) names.zip(usedSnapshot).toMap else Map.empty,
      if (readsSnapshotFields) names.zip(usedBackfill).toMap else Map.empty
    )
  }
}
