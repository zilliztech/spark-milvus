package com.zilliz.spark.connector.operations.backfill

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

/** Result of backfilling a single segment
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
    v2Artifact: Option[V2SegmentArtifact] = None
)

/** Comprehensive result of backfill operation
  */
case class BackfillResult(
    success: Boolean,
    segmentsProcessed: Int,
    totalRowsWritten: Long,
    manifestPaths: Seq[String],
    segmentResults: Map[Long, SegmentBackfillResult],
    executionTimeMs: Long,
    collectionId: Long,
    partitionId: Long,
    newFieldNames: Seq[String]
) {

  /** Get a summary string of the backfill operation
    */
  def summary: String = {
    val v2Count = segmentResults.count(_._2.v2Artifact.isDefined)
    s"""Backfill Summary:
       |  Status: ${if (success) "SUCCESS" else "FAILED"}
       |  Segments Processed: $segmentsProcessed
       |  Total Rows Written: $totalRowsWritten
       |  Execution Time: ${executionTimeMs}ms
       |  Collection ID: $collectionId
       |  Partition ID: $partitionId
       |  New Fields: ${newFieldNames.mkString(", ")}
       |  Manifest Paths: ${manifestPaths.size} files
       |  StorageV2 Segments: $v2Count / ${segmentResults.size}
       |""".stripMargin
  }

  /** Get detailed per-segment results
    */
  def segmentSummary: String = {
    val segmentLines =
      segmentResults.toSeq.sortBy(_._1).map { case (segId, result) =>
        val tag = if (result.v2Artifact.isDefined) "[v2]" else "[v3]"
        s"    Segment $segId $tag: ${result.rowCount} rows, version=${result.committedVersion}, ${result.executionTimeMs}ms, path=${result.outputPath}"
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
          "executionTimeMs" -> r.executionTimeMs,
          "outputPath" -> r.outputPath,
          "manifestPaths" -> r.manifestPaths
        )
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

    val result = Map(
      "success" -> success,
      "collectionId" -> collectionId,
      "partitionId" -> partitionId,
      "segmentsProcessed" -> segmentsProcessed,
      "totalRowsWritten" -> totalRowsWritten,
      "executionTimeMs" -> executionTimeMs,
      "newFieldNames" -> newFieldNames,
      "segments" -> segments
    )

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
      newFieldNames: Seq[String]
  ): BackfillResult = {
    val totalRows = segmentResults.values.map(_.rowCount).sum
    val allManifests = segmentResults.values.flatMap(_.manifestPaths).toSeq

    BackfillResult(
      success = true,
      segmentsProcessed = segmentResults.size,
      totalRowsWritten = totalRows,
      manifestPaths = allManifests,
      segmentResults = segmentResults,
      executionTimeMs = executionTimeMs,
      collectionId = collectionId,
      partitionId = partitionId,
      newFieldNames = newFieldNames
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
      newFieldNames = Seq.empty
    )
  }
}
