package com.zilliz.spark.connector.operations.backfill

/**
 * Result of backfilling a single segment
 */
case class SegmentBackfillResult(
    segmentId: Long,
    rowCount: Long,
    manifestPaths: Seq[String],
    outputPath: String,
    executionTimeMs: Long
)

/**
 * Comprehensive result of backfill operation
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
  /**
   * Get a summary string of the backfill operation
   */
  def summary: String = {
    s"""Backfill Summary:
       |  Status: ${if (success) "SUCCESS" else "FAILED"}
       |  Segments Processed: $segmentsProcessed
       |  Total Rows Written: $totalRowsWritten
       |  Execution Time: ${executionTimeMs}ms
       |  Collection ID: $collectionId
       |  Partition ID: $partitionId
       |  New Fields: ${newFieldNames.mkString(", ")}
       |  Manifest Paths: ${manifestPaths.size} files
       |""".stripMargin
  }

  /**
   * Get detailed per-segment results
   */
  def segmentSummary: String = {
    val segmentLines = segmentResults.toSeq.sortBy(_._1).map { case (segId, result) =>
      s"    Segment $segId: ${result.rowCount} rows, ${result.executionTimeMs}ms, ${result.manifestPaths.size} manifests"
    }
    s"Segment Details:\n${segmentLines.mkString("\n")}"
  }

  /**
   * Check if all segments were processed successfully
   */
  def allSegmentsSuccessful: Boolean = success && segmentsProcessed == segmentResults.size

  /**
   * Get total execution time in seconds
   */
  def executionTimeSec: Double = executionTimeMs / 1000.0
}

object BackfillResult {
  /**
   * Create a successful result
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

  /**
   * Create a failed result
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
