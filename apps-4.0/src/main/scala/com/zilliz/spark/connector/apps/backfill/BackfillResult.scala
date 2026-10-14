package com.zilliz.spark.connector.apps.backfill

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** One StorageV2 (packed-parquet) column group produced by a backfill write.
  * Mirrors the snapshot-AVRO shape so the caller can patch the snapshot's
  * manifest entry directly: add one `AvroFieldBinlog` per entry here, with
  * `field_id = fieldIds.head` when `fieldIds.size == 1` (backfill's invariant).
  */
case class V2ColumnGroupArtifact(
    fieldIds: Seq[Long],
    binlogFiles: Seq[String],
    rowCount: Long
)

/** StorageV2 artifact for one segment — the shape a caller needs to augment the
  * segment's AVRO manifest with the newly-written fields.
  */
case class V2SegmentArtifact(
    segmentId: Long,
    storageVersion: Long, // 2 for StorageV2
    columnGroups: Seq[V2ColumnGroupArtifact]
)

/** Result of backfilling a single segment.
  *
  * Public `source*` / `usedSource*` names are retained for compatibility and
  * refer to snapshot rows and values, not the external backfill input.
  */
case class SegmentBackfillResult(
    segmentId: Long,
    rowCount: Long,
    manifestPaths: Seq[String],
    outputPath: String,
    executionTimeMs: Long,
    committedVersion: Long = -1,
    /** Populated for StorageV2 (non-manifest packed parquet) segments only. V3
      * segments continue to record their manifest path + version in the fields
      * above; for V2 there is no manifest, so consumers should read this
      * artifact to patch the snapshot.
      */
    v2Artifact: Option[V2SegmentArtifact] = None,
    /** Snapshot row count for this segment — equal to `rowCount` on the success
      * path because the left join preserves every snapshot row, but kept
      * separate so consumers don't have to rely on that invariant.
      */
    sourceRowCount: Long = 0L,
    /** Snapshot rows whose resolved join key matched the backfill parquet.
      * Derived from a `__bf_matched__` marker column injected before the left
      * join.
      */
    matchedRowCount: Long = 0L,
    /** Per-field count of rows that kept the snapshot value: non-null values in
      * coalesce mode, or unmatched rows in overwrite mode. Empty in replace
      * mode. The legacy field name is retained in the result API and JSON.
      */
    usedSourceByField: Map[String, Long] = Map.empty,
    /** Per-field count of rows that took the backfill value: snapshot null and
      * backfill non-null in coalesce mode, or any matched row in overwrite
      * mode, including a null backfill value. Empty in replace mode.
      */
    usedDataFileByField: Map[String, Long] = Map.empty
)

/** Comprehensive result of backfill operation.
  *
  * Public names and summary labels containing `source` retain their legacy
  * spelling; they count snapshot rows or values, not external input rows.
  */
case class BackfillResult(
    success: Boolean,
    segmentsProcessed: Int,
    totalRowsWritten: Long,
    manifestPaths: Seq[String],
    segmentResults: Map[Long, SegmentBackfillResult],
    executionTimeMs: Long,
    collectionId: Long,
    /** Partition every processed segment belongs to, or 0 when the segments
      * span several partitions. Milvus skips its partition check for 0 (and,
      * since milvus-io/milvus#53330, for any non-positive value); a positive
      * value must match each segment's partition or the commit fails.
      */
    partitionId: Long,
    schemaVersion: Int,
    newFieldNames: Seq[String],
    totalSourceRows: Long = 0L,
    totalBackfillDataRows: Long = 0L,
    totalMatchedRows: Long = 0L,
    /** Aggregated per-field snapshot provenance counts from coalesce or
      * overwrite mode. Empty in replace mode.
      */
    totalUsedSourceByField: Map[String, Long] = Map.empty,
    /** Aggregated per-field backfill provenance counts from coalesce or
      * overwrite mode. Empty in replace mode.
      */
    totalUsedDataFileByField: Map[String, Long] = Map.empty,
    /** `{root}/staging/{job}` where the job manifest and marker were written;
      * what `register` takes. Empty when nothing was committed.
      */
    stagingPrefix: String = ""
) {

  private def matchRateStr(matched: Long, total: Long): String =
    if (total <= 0) "n/a" else f"${matched.toDouble / total * 100}%.2f%%"

  /** Get a summary string of the backfill operation
    */
  def summary: String = {
    val v2Count = segmentResults.count(_._2.v2Artifact.isDefined)
    val snapshotRate = matchRateStr(totalMatchedRows, totalSourceRows)
    val coalesceBlock =
      if (totalUsedSourceByField.isEmpty && totalUsedDataFileByField.isEmpty)
        ""
      else {
        val fieldLines = newFieldNames.map { f =>
          val snapshotCount = totalUsedSourceByField.getOrElse(f, 0L)
          val df = totalUsedDataFileByField.getOrElse(f, 0L)
          val nullOut = totalSourceRows - snapshotCount - df
          s"    $f: source=$snapshotCount, dataFile=$df, null=$nullOut"
        }
        s"  Coalesce Provenance (per field):\n${fieldLines.mkString("\n")}\n"
      }
    s"""Backfill Summary:
       |  Status: ${if (success) "SUCCESS"
      else "FAILED"}
       |  Segments Processed: $segmentsProcessed
       |  Total Source Rows: $totalSourceRows
       |  Total Backfill Data File Rows: $totalBackfillDataRows
       |  Total Matched Source Rows: $totalMatchedRows ($snapshotRate of source rows)
       |  Total Rows Written: $totalRowsWritten
       |  Execution Time: ${executionTimeMs}ms
       |  Collection ID: $collectionId
       |  Partition ID: $partitionId
       |  Schema Version: $schemaVersion
       |  New Fields: ${newFieldNames.mkString(", ")}
       |  Manifest Paths: ${manifestPaths.size} files
       |  StorageV2 Segments: $v2Count / ${segmentResults.size}
       |$coalesceBlock""".stripMargin
  }

  /** Get detailed per-segment results
    */
  def segmentSummary: String = {
    val segmentLines =
      segmentResults.toSeq.sortBy(_._1).map { case (segId, result) =>
        val tag = if (result.v2Artifact.isDefined) "[v2]" else "[v3]"
        val rate = matchRateStr(result.matchedRowCount, result.sourceRowCount)
        s"    Segment $segId $tag: source=${result.sourceRowCount}, matched=${result.matchedRowCount} ($rate), written=${result.rowCount}, version=${result.committedVersion}, ${result.executionTimeMs}ms, path=${result.outputPath}"
      }
    s"Segment Details:\n${segmentLines.mkString("\n")}"
  }

  /** Serialize this result to a JSON string.
    *
    * StorageV2 segments additionally carry a `storage_version` /
    * `column_groups` block (same shape as the milvus snapshot AVRO's
    * `ManifestEntry`) so the caller can mechanically extend each segment's
    * existing manifest with the new field's binlog paths.
    */
  def toJson: String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val segments = segmentResults.toSeq
      .sortBy(_._1)
      .map { case (segId, r) =>
        val base = scala.collection.mutable.LinkedHashMap[String, Any](
          "version" -> r.committedVersion,
          "rowCount" -> r.rowCount,
          "sourceRowCount" -> r.sourceRowCount,
          "matchedRowCount" -> r.matchedRowCount,
          "executionTimeMs" -> r.executionTimeMs,
          "outputPath" -> r.outputPath,
          "manifestPaths" -> r.manifestPaths
        )
        if (r.usedSourceByField.nonEmpty)
          base += "usedSourceByField" -> r.usedSourceByField
        if (r.usedDataFileByField.nonEmpty)
          base += "usedDataFileByField" -> r.usedDataFileByField
        r.v2Artifact.foreach { art =>
          base += "storage_version" -> art.storageVersion
          base += "column_groups" -> art.columnGroups.map { cg =>
            Map(
              "field_ids" -> cg.fieldIds,
              "binlog_files" -> cg.binlogFiles,
              "row_count" -> cg.rowCount
            )
          }
        }
        segId.toString -> base.toMap
      }
      .toMap

    val result = scala.collection.mutable.LinkedHashMap[String, Any](
      "success" -> success,
      "collectionId" -> collectionId,
      "partitionId" -> partitionId,
      "schemaVersion" -> schemaVersion,
      "segmentsProcessed" -> segmentsProcessed,
      "totalSourceRows" -> totalSourceRows,
      "totalBackfillDataRows" -> totalBackfillDataRows,
      "totalMatchedRows" -> totalMatchedRows,
      "totalRowsWritten" -> totalRowsWritten,
      "executionTimeMs" -> executionTimeMs,
      "newFieldNames" -> newFieldNames,
      "segments" -> segments
    )
    if (totalUsedSourceByField.nonEmpty)
      result += "totalUsedSourceByField" -> totalUsedSourceByField
    if (totalUsedDataFileByField.nonEmpty)
      result += "totalUsedDataFileByField" -> totalUsedDataFileByField

    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result)
  }

  /** Check if all segments were processed successfully
    */
  def allSegmentsSuccessful: Boolean =
    success && segmentsProcessed == segmentResults.size

  /** Get total execution time in seconds
    */
  def executionTimeSec: Double = executionTimeMs / 1000.0
}

object BackfillResult {

  /** Create a successful result
    */
  def success(
      segmentResults: Map[Long, SegmentBackfillResult],
      executionTimeMs: Long,
      collectionId: Long,
      partitionId: Long,
      schemaVersion: Int,
      newFieldNames: Seq[String],
      totalBackfillDataRows: Long = 0L
  ): BackfillResult = {
    val totalRows = segmentResults.values.map(_.rowCount).sum
    val totalSnapshotRows = segmentResults.values.map(_.sourceRowCount).sum
    val totalMatched = segmentResults.values.map(_.matchedRowCount).sum
    val allManifests = segmentResults.values.flatMap(_.manifestPaths).toSeq

    def sumByField(
        pick: SegmentBackfillResult => Map[String, Long]
    ): Map[String, Long] =
      segmentResults.values.foldLeft(Map.empty[String, Long]) { (acc, r) =>
        pick(r).foldLeft(acc) { case (a, (k, v)) =>
          a.updated(k, a.getOrElse(k, 0L) + v)
        }
      }
    val totalUsedSnapshot = sumByField(_.usedSourceByField)
    val totalUsedDf = sumByField(_.usedDataFileByField)

    BackfillResult(
      success = true,
      segmentsProcessed = segmentResults.size,
      totalRowsWritten = totalRows,
      manifestPaths = allManifests,
      segmentResults = segmentResults,
      executionTimeMs = executionTimeMs,
      collectionId = collectionId,
      partitionId = partitionId,
      schemaVersion = schemaVersion,
      newFieldNames = newFieldNames,
      totalSourceRows = totalSnapshotRows,
      totalBackfillDataRows = totalBackfillDataRows,
      totalMatchedRows = totalMatched,
      totalUsedSourceByField = totalUsedSnapshot,
      totalUsedDataFileByField = totalUsedDf
    )
  }

  /** Create a failed result
    */
  def failure(
      executionTimeMs: Long,
      collectionId: Long = -1,
      partitionId: Long = -1
  ): BackfillResult = {
    BackfillResult(
      success = false,
      segmentsProcessed = 0,
      totalRowsWritten = 0,
      manifestPaths = Seq.empty,
      segmentResults = Map.empty,
      executionTimeMs = executionTimeMs,
      collectionId = collectionId,
      partitionId = partitionId,
      schemaVersion = 0,
      newFieldNames = Seq.empty
    )
  }
}
