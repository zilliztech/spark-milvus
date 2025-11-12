package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import com.zilliz.spark.connector.write.{MilvusLoonBatchWrite, MilvusLoonCommitMessage, MilvusLoonWriter}
import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams, MilvusOption}

import scala.collection.JavaConverters._


/**
 * Backfill operation for Milvus collections
 *
 * This object provides functionality to backfill new fields into existing Milvus collections
 * by reading the original data, joining with new field data, and writing per-segment binlog files.
 */
object MilvusBackfill {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Backfill new fields into a Milvus collection
   *
   * @param spark SparkSession
   * @param backfillDataPath Path to Parquet file containing new field data with schema (pk, new_field1, new_field2, ...)
   * @param config Backfill configuration
   * @return Either error or successful result
   */
  def run(
      spark: SparkSession,
      backfillDataPath: String,
      config: BackfillConfig
  ): Either[BackfillError, BackfillResult] = {

    val startTime = System.currentTimeMillis()

    // Validate configuration
    config.validate() match {
      case Left(error) => return Left(SchemaValidationError(s"Invalid configuration: $error"))
      case Right(_) => // Continue
    }

    try {
      // Read backfill data from Parquet
      val backfillDF = readBackfillData(spark, backfillDataPath) match {
        case Left(error) => return Left(error)
        case Right(df) => df
      }

      // Read original collection data with segment metadata
      val originalDF = readCollectionWithMetadata(spark, config) match {
        case Left(error) => return Left(error)
        case Right(df) => df
      }

      // Validate schema compatibility and get primary key name
      val pkName = validateSchemaCompatibility(originalDF, backfillDF, config) match {
        case Left(error) => return Left(error)
        case Right(name) => name
      }

      // Perform Sort Merge Join
      val joinedDF = performJoin(originalDF, backfillDF, pkName)

      // Retrieve Milvus metadata (collection ID, partition ID)
      // TODO: Currently get through milvus client, once Milvus snapshot feature is ready,
      // we can get the collection ID and partition ID from the snapshot file.
      val (collectionID, partitionID) = retrieveMilvusMetadata(config) match {
        case Left(error) => return Left(error)
        case Right(ids) => ids
      }

      // Extract new field names
      val newFieldNames = backfillDF.schema.fields
        .map(_.name)
        .filterNot(_ == "pk")
        .toSeq

      // Process each segment
      val segmentResults = processSegments(
        spark,
        joinedDF,
        collectionID,
        partitionID,
        config,
        newFieldNames
      ) match {
        case Left(error) => return Left(error)
        case Right(results) => results
      }

      // Build final result
      val executionTime = System.currentTimeMillis() - startTime
      val result = BackfillResult.success(
        segmentResults = segmentResults,
        executionTimeMs = executionTime,
        collectionId = collectionID,
        partitionId = partitionID,
        newFieldNames = newFieldNames
      )

      Right(result)

    } catch {
      case e: Exception =>
        val executionTime = System.currentTimeMillis() - startTime
        logger.error("Backfill operation failed", e)
        Left(BackfillError.fromException(e))
    }
  }

  /**
   * Read backfill data from Parquet file
   */
  private def readBackfillData(
      spark: SparkSession,
      path: String
  ): Either[BackfillError, DataFrame] = {
    try {
      val df = spark.read.parquet(path)

      // Validate that it has a 'pk' column
      if (!df.columns.contains("pk")) {
        return Left(DataReadError(
          path = path,
          message = "Backfill data must contain a 'pk' column"
        ))
      }

      // Validate that it has at least one other column
      if (df.columns.length < 2) {
        return Left(DataReadError(
          path = path,
          message = "New field data must contain at least one field besides 'pk'"
        ))
      }

      Right(df)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read Parquet file from $path", e)
        Left(DataReadError(
          path = path,
          message = s"Failed to read Parquet file: ${e.getMessage}",
          cause = Some(e)
        ))
    }
  }

  /**
   * Read collection data with segment_id and row_offset metadata
   * segment_id and row_offset are used to match with the original sequence of rows for each segment
   */
  private def readCollectionWithMetadata(
      spark: SparkSession,
      config: BackfillConfig
  ): Either[BackfillError, DataFrame] = {
    try {
      val options = config.getMilvusReadOptions
      val df = spark.read
        .format("milvus")
        .options(options)
        .load()

      // Validate that segment_id and row_offset are present
      if (!df.columns.contains("segment_id") || !df.columns.contains("row_offset")) {
        return Left(ConnectionError(
          message = "Failed to read collection data with segment_id and row_offset. " +
            "Ensure milvus.extra.columns is set correctly."
        ))
      }

      Right(df)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read Milvus collection ${config.collectionName}", e)
        Left(ConnectionError(
          message = s"Failed to read Milvus collection ${config.collectionName}: ${e.getMessage}",
          cause = Some(e)
        ))
    }
  }

  /**
   * Validate schema compatibility between original and new field data
   * Returns the primary key field name if validation succeeds
   */
  private def validateSchemaCompatibility(
      originalDF: DataFrame,
      backfillDF: DataFrame,
      config: BackfillConfig
  ): Either[BackfillError, String] = {
    // Get the actual primary key field name from Milvus collection
    var client: MilvusClient = null
    try {
      client = MilvusClient(
        MilvusConnectionParams(
          uri = config.milvusUri,
          token = config.milvusToken,
          databaseName = config.databaseName
        )
      )

      val pkName = client.getPKName(config.databaseName, config.collectionName) match {
        case scala.util.Success(name) => name
        case scala.util.Failure(e) =>
          return Left(ConnectionError(
            message = s"Failed to get primary key name for collection ${config.collectionName}: ${e.getMessage}",
            cause = Some(e)
          ))
      }

      // Find the primary key field in original data
      val pkField = originalDF.schema.fields.find(_.name == pkName)
        .getOrElse {
          return Left(SchemaValidationError(
            s"Original collection data must have primary key field '$pkName'"
          ))
        }

      // Find the pk field in new field data
      val newPkField = backfillDF.schema.fields.find(_.name == "pk")
        .getOrElse {
          return Left(SchemaValidationError("New field data must have 'pk' field"))
        }

      // Validate types match
      if (pkField.dataType != newPkField.dataType) {
        return Left(SchemaValidationError(
          s"Primary key type mismatch: original=${pkField.dataType}, new=${newPkField.dataType}"
        ))
      }

      Right(pkName)

    } catch {
      case e: Exception =>
        logger.error("Failed to validate schema compatibility", e)
        Left(ConnectionError(
          message = s"Failed to validate schema compatibility: ${e.getMessage}",
          cause = Some(e)
        ))
    } finally {
      if (client != null) {
        try {
          client.close()
        } catch {
          case _: Exception => // Ignore close errors
        }
      }
    }
  }

  /**
   * Perform left join between original and new field data
   */
  private def performJoin(
      originalDF: DataFrame,
      backfillDF: DataFrame,
      pkName: String
  ): DataFrame = {
    originalDF.join(backfillDF, originalDF(pkName) === backfillDF("pk"), "left")
  }

  /**
   * Retrieve Milvus metadata (collection ID, partition ID)
   */
  private def retrieveMilvusMetadata(
      config: BackfillConfig
  ): Either[BackfillError, (Long, Long)] = {
    var client: MilvusClient = null
    try {
      client = MilvusClient(
        MilvusConnectionParams(
          uri = config.milvusUri,
          token = config.milvusToken,
          databaseName = config.databaseName
        )
      )

      val segments = client.getSegments(config.databaseName, config.collectionName)
        .getOrElse {
          return Left(ConnectionError(
            message = s"No segments found for collection ${config.collectionName}"
          ))
        }

      if (segments.isEmpty) {
        return Left(ConnectionError(
          message = s"Collection ${config.collectionName} has no segments"
        ))
      }

      val firstSegment = segments.head
      Right((firstSegment.collectionID, firstSegment.partitionID))

    } catch {
      case e: Exception =>
        logger.error(s"Failed to retrieve Milvus metadata for collection ${config.collectionName}", e)
        Left(ConnectionError(
          message = s"Failed to retrieve Milvus metadata: ${e.getMessage}",
          cause = Some(e)
        ))
    } finally {
      if (client != null) {
        try {
          client.close()
        } catch {
          case _: Exception => // Ignore close errors
        }
      }
    }
  }

  /**
   * Process each segment separately by distributing to Spark executors
   * Each segment is processed by exactly one FFI writer on a single executor
   */
  private def processSegments(
      spark: SparkSession,
      joinedDF: DataFrame,
      collectionID: Long,
      partitionID: Long,
      config: BackfillConfig,
      newFieldNames: Seq[String]
  ): Either[BackfillError, Map[Long, SegmentBackfillResult]] = {

    try {
      // Prepare data: select only needed columns and add segment_id for partitioning
      val preparedDF = joinedDF
        .select((Seq("segment_id", "row_offset") ++ newFieldNames).map(col): _*)

      // Repartition by segment_id to ensure all rows of same segment go to same partition
      // Sort within partitions to maintain row_offset order
      val repartitionedDF = preparedDF
        .repartition(col("segment_id"))
        .sortWithinPartitions("segment_id", "row_offset")

      // Get the schema for new fields only (without segment_id and row_offset)
      val targetSchema = org.apache.spark.sql.types.StructType(
        newFieldNames.map(fieldName =>
          preparedDF.schema.fields.find(_.name == fieldName).get
        )
      )

      // Broadcast configuration to executors
      val broadcastConfig = spark.sparkContext.broadcast(config)
      val broadcastCollectionID = spark.sparkContext.broadcast(collectionID)
      val broadcastPartitionID = spark.sparkContext.broadcast(partitionID)
      val broadcastTargetSchema = spark.sparkContext.broadcast(targetSchema)

      // Get the underlying RDD[InternalRow] for efficient processing
      val internalRowRDD = repartitionedDF.queryExecution.toRdd

      // Process each partition (which may contain one or more segments)
      // Each segment will be written by exactly one FFI writer
      val results = internalRowRDD.mapPartitions { iter =>
        if (!iter.hasNext) {
          // Empty partition
          Iterator.empty
        } else {
          val cfg = broadcastConfig.value
          val collID = broadcastCollectionID.value
          val partID = broadcastPartitionID.value
          val schema = broadcastTargetSchema.value

          // Group rows by segment_id within this partition
          val segmentGroups = groupRowsBySegmentId(iter)

          // Process each segment in this partition
          segmentGroups.map { case (segmentID, rows) =>
            processSegmentWithWriter(
              segmentID = segmentID,
              rows = rows,
              collectionID = collID,
              partitionID = partID,
              config = cfg,
              targetSchema = schema
            )
          }
        }
      }.collect()

      // Cleanup broadcast variables
      broadcastConfig.unpersist()
      broadcastCollectionID.unpersist()
      broadcastPartitionID.unpersist()
      broadcastTargetSchema.unpersist()

      // Check for failures
      val failures = results.filter(_._2.isDefined)
      if (failures.nonEmpty) {
        val firstFailure = failures.head
        val error = firstFailure._2.get
        return Left(WriteError(
          segmentId = firstFailure._1.segmentId,
          outputPath = firstFailure._1.outputPath,
          message = s"Failed to write ${failures.length} segment(s): ${error.getMessage}",
          cause = Some(error)
        ))
      }

      // Extract successful results
      val successfulResults = results.map { case (result, _) =>
        result.segmentId -> result
      }.toMap

      // Log summary statistics
      val totalTime = results.map(_._1.executionTimeMs).sum
      val avgTime = if (results.nonEmpty) totalTime / results.length else 0
      val totalRows = results.map(_._1.rowCount).sum

      logger.info("=== Backfill Summary ===")
      logger.info(s"Total segments: ${results.length}")
      logger.info(s"Total rows processed: $totalRows")
      logger.info(s"Total time for all segments: ${totalTime}ms")
      logger.info(s"Average time per segment: ${avgTime}ms")

      Right(successfulResults)

    } catch {
      case e: Exception =>
        logger.error("Failed to process segments", e)
        Left(SegmentProcessingError(
          segmentId = -1,
          message = s"Failed to process segments: ${e.getMessage}",
          cause = Some(e)
        ))
    }
  }

  /**
   * Group InternalRows by segment_id
   * Assumes rows are already sorted by segment_id (from sortWithinPartitions)
   */
  private def groupRowsBySegmentId(
      iter: Iterator[org.apache.spark.sql.catalyst.InternalRow]
  ): Iterator[(Long, Seq[org.apache.spark.sql.catalyst.InternalRow])] = {
    if (!iter.hasNext) {
      return Iterator.empty
    }

    // Use a mutable buffer to accumulate rows for current segment
    val buffer = scala.collection.mutable.ListBuffer[(Long, Seq[org.apache.spark.sql.catalyst.InternalRow])]()
    var currentSegmentID: Long = -1
    var currentRows = scala.collection.mutable.ListBuffer[org.apache.spark.sql.catalyst.InternalRow]()

    iter.foreach { row =>
      val segmentID = row.getLong(0) // segment_id is first column

      if (currentSegmentID == -1) {
        // First row
        currentSegmentID = segmentID
        currentRows += row.copy() // Copy to avoid mutation
      } else if (segmentID == currentSegmentID) {
        // Same segment, accumulate
        currentRows += row.copy()
      } else {
        // New segment, flush previous
        buffer += ((currentSegmentID, currentRows.toSeq))
        currentSegmentID = segmentID
        currentRows = scala.collection.mutable.ListBuffer(row.copy())
      }
    }

    // Flush last segment
    if (currentRows.nonEmpty) {
      buffer += ((currentSegmentID, currentRows.toSeq))
    }

    buffer.iterator
  }

  /**
   * Process a single segment using MilvusLoonWriter
   * Creates one FFI writer per segment for thread safety
   */
  private def processSegmentWithWriter(
      segmentID: Long,
      rows: Seq[org.apache.spark.sql.catalyst.InternalRow],
      collectionID: Long,
      partitionID: Long,
      config: BackfillConfig,
      targetSchema: org.apache.spark.sql.types.StructType
  ): (SegmentBackfillResult, Option[Throwable]) = {

    val segmentStartTime = System.currentTimeMillis()

    try {
      // Get write options for this segment
      val writeOptions = config.getS3WriteOptions(collectionID, partitionID, segmentID)
      val outputPath = writeOptions("milvus.writer.customPath")

      logger.info(s"Executor processing segment $segmentID: ${rows.length} rows -> $outputPath")

      // Create MilvusOption from write options
      val optionsMap = new CaseInsensitiveStringMap(writeOptions.asJava)
      val milvusOption = MilvusOption(optionsMap)

      // Create batch write infrastructure
      val batchWrite = new MilvusLoonBatchWrite(targetSchema, milvusOption)
      val writerFactory = batchWrite.createBatchWriterFactory(null)

      // Create a single writer for this segment
      val writer = writerFactory.createWriter(0, System.currentTimeMillis())

      try {
        // Write all rows for this segment
        // Extract only the new field columns (skip segment_id and row_offset)
        rows.foreach { row =>
          // Create a new InternalRow with only the target fields (columns 2+)
          val targetRow = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
            (2 until row.numFields).map(i => row.get(i, targetSchema.fields(i - 2).dataType)).toArray
          )
          writer.write(targetRow)
        }

        // Commit the writer
        val commitMessage = writer.commit()

        // Extract manifest path from commit message
        val manifestPaths = commitMessage match {
          case msg: MilvusLoonCommitMessage => Seq(msg.manifestPath)
          case _ => Seq.empty
        }

        val segmentExecutionTime = System.currentTimeMillis() - segmentStartTime

        logger.info(s"Segment $segmentID completed successfully in ${segmentExecutionTime}ms")
        logger.info(s"Segment $segmentID manifest paths: ${manifestPaths.mkString(", ")}")

        // Commit the batch write
        batchWrite.commit(Array(commitMessage))

        (SegmentBackfillResult(
          segmentId = segmentID,
          rowCount = rows.length.toLong,
          manifestPaths = manifestPaths,
          outputPath = outputPath,
          executionTimeMs = segmentExecutionTime
        ), None)

      } catch {
        case e: Exception =>
          writer.abort()
          throw e
      } finally {
        writer.close()
      }

    } catch {
      case e: Exception =>
        val segmentExecutionTime = System.currentTimeMillis() - segmentStartTime
        logger.error(s"Segment $segmentID failed after ${segmentExecutionTime}ms", e)

        (SegmentBackfillResult(
          segmentId = segmentID,
          rowCount = rows.length.toLong,
          manifestPaths = Seq.empty,
          outputPath = "",
          executionTimeMs = segmentExecutionTime
        ), Some(e))
    }
  }
}
