package com.zilliz.spark.connector.operations.backfill

/**
 * Sealed trait representing all possible errors during backfill operations
 */
sealed trait BackfillError {
  def message: String
  def cause: Option[Throwable]
}

/**
 * Error connecting to Milvus or retrieving metadata
 */
case class ConnectionError(
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

/**
 * Error validating schema compatibility between original collection and new field data
 */
case class SchemaValidationError(
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

/**
 * Error reading new field data from the provided path
 */
case class DataReadError(
    path: String,
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

/**
 * Error processing a specific segment during backfill
 */
case class SegmentProcessingError(
    segmentId: Long,
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

/**
 * Error writing backfill data for a segment
 */
case class WriteError(
    segmentId: Long,
    outputPath: String,
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

/**
 * Error with Spark configuration or join strategy
 */
case class SparkConfigError(
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

/**
 * General unexpected error during backfill
 */
case class UnexpectedError(
    message: String,
    cause: Option[Throwable] = None
) extends BackfillError

object BackfillError {
  /**
   * Helper to create error from exception
   */
  def fromException(e: Throwable): BackfillError = {
    UnexpectedError(
      message = s"Unexpected error: ${e.getMessage}",
      cause = Some(e)
    )
  }

  /**
   * Helper to create connection error from exception
   */
  def connectionError(message: String, e: Throwable): ConnectionError = {
    ConnectionError(message, Some(e))
  }

  /**
   * Helper to create schema validation error
   */
  def schemaError(message: String): SchemaValidationError = {
    SchemaValidationError(message)
  }

  /**
   * Helper to create data read error
   */
  def dataReadError(path: String, message: String, e: Option[Throwable] = None): DataReadError = {
    DataReadError(path, message, e)
  }

  /**
   * Helper to create segment processing error
   */
  def segmentError(segmentId: Long, message: String, e: Throwable): SegmentProcessingError = {
    SegmentProcessingError(segmentId, message, Some(e))
  }

  /**
   * Helper to create write error
   */
  def writeError(segmentId: Long, path: String, message: String, e: Throwable): WriteError = {
    WriteError(segmentId, path, message, Some(e))
  }
}
