package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.write.DataWriter
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

    // Create Milvus client once for all operations
    var client: MilvusClient = null
    try {
      client = MilvusClient(
        MilvusConnectionParams(
          uri = config.milvusUri,
          token = config.milvusToken,
          databaseName = config.databaseName
        )
      )

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
      val pkName = validateSchemaCompatibility(originalDF, backfillDF, config, client) match {
        case Left(error) => return Left(error)
        case Right(name) => name
      }

      // Perform Sort Merge Join
      val joinedDF = performJoin(originalDF, backfillDF, pkName)

      // Retrieve Milvus metadata (collection ID and segment-to-partition mapping)
      // TODO: Currently get through milvus client, once Milvus snapshot feature is ready,
      // we can get the collection ID and segment-to-partition mapping from the snapshot file.
      val (collectionID, segmentToPartitionMap) = retrieveMilvusMetadata(config, client) match {
        case Left(error) => return Left(error)
        case Right(metadata) => metadata
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
        segmentToPartitionMap,
        config,
        newFieldNames
      ) match {
        case Left(error) => return Left(error)
        case Right(results) => results
      }

      // Build final result
      val executionTime = System.currentTimeMillis() - startTime

      // Get all unique partition IDs that were processed
      val partitionIDs = segmentToPartitionMap.values.toSet

      val result = BackfillResult.success(
        segmentResults = segmentResults,
        executionTimeMs = executionTime,
        collectionId = collectionID,
        partitionId = if (partitionIDs.size == 1) partitionIDs.head else -1, // -1 indicates multi-partition
        newFieldNames = newFieldNames
      )

      Right(result)

    } catch {
      case e: Exception =>
        val executionTime = System.currentTimeMillis() - startTime
        logger.error("Backfill operation failed", e)
        Left(BackfillError.fromException(e))
    } finally {
      if (client != null) {
        try {
          client.close()
        } catch {
          case e: Exception =>
            logger.warn("Failed to close Milvus client", e)
        }
      }
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
      config: BackfillConfig,
      client: MilvusClient
  ): Either[BackfillError, String] = {
    try {
      // Get the actual primary key field name from Milvus collection
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
   * Retrieve Milvus metadata (collection ID and segment-to-partition mapping)
   * Supports multi-partition collections by tracking partition ID for each segment
   */
  private def retrieveMilvusMetadata(
      config: BackfillConfig,
      client: MilvusClient
  ): Either[BackfillError, (Long, Map[Long, Long])] = {
    try {
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

      val collectionID = segments.head.collectionID

      // Build mapping of segment ID -> partition ID to support multi-partition collections
      val segmentToPartitionMap = segments.map { seg =>
        seg.segmentID -> seg.partitionID
      }.toMap

      logger.info(s"Retrieved metadata for ${segments.length} segments across ${segmentToPartitionMap.values.toSet.size} partition(s)")

      Right((collectionID, segmentToPartitionMap))

    } catch {
      case e: Exception =>
        logger.error(s"Failed to retrieve Milvus metadata for collection ${config.collectionName}", e)
        Left(ConnectionError(
          message = s"Failed to retrieve Milvus metadata: ${e.getMessage}",
          cause = Some(e)
        ))
    }
  }

  /**
   * Process each segment separately by distributing to Spark executors
   * Each segment is processed by exactly one FFI writer on a single executor
   * Supports multi-partition collections by tracking partition ID per segment
   */
  private def processSegments(
      spark: SparkSession,
      joinedDF: DataFrame,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      config: BackfillConfig,
      newFieldNames: Seq[String]
  ): Either[BackfillError, Map[Long, SegmentBackfillResult]] = {

    try {
      // Prepare data: select only needed columns and add segment_id for partitioning
      val preparedDF = joinedDF
        .select((Seq("segment_id", "row_offset") ++ newFieldNames).map(col): _*)

      // Get the schema for new fields only (without segment_id and row_offset)
      val targetSchema = org.apache.spark.sql.types.StructType(
        newFieldNames.map(fieldName =>
          preparedDF.schema.fields.find(_.name == fieldName).get
        )
      )

      val segmentIds = segmentToPartitionMap.keys.toArray
      val segmentPartitioner = new SegmentPartitioner(segmentIds)

      // Repartition using custom partitioner, then sort by row_offset within each partition
      val repartitionedRDD = preparedDF.queryExecution.toRdd
        .keyBy(_.getLong(0))  // segment_id is at index 0
        .partitionBy(segmentPartitioner)
        .values
        .mapPartitions(iter => iter.toSeq.sortBy(_.getLong(1)).iterator)  // Sort by row_offset

      // Broadcast configuration to executors
      val broadcastConfig = spark.sparkContext.broadcast(config)
      val broadcastCollectionID = spark.sparkContext.broadcast(collectionID)
      val broadcastSegmentToPartitionMap = spark.sparkContext.broadcast(segmentToPartitionMap)
      val broadcastTargetSchema = spark.sparkContext.broadcast(targetSchema)

      val results = repartitionedRDD.mapPartitions { iter =>
        if (!iter.hasNext) Iterator.empty
        else processSegmentPartition(
          iter,
          broadcastConfig.value,
          broadcastCollectionID.value,
          broadcastSegmentToPartitionMap.value,
          broadcastTargetSchema.value
        )
      }.collect()

      // Cleanup broadcast variables
      broadcastConfig.unpersist()
      broadcastCollectionID.unpersist()
      broadcastSegmentToPartitionMap.unpersist()
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
   * Process a single partition containing exactly one segment
   * This is called by each Spark executor to write one segment's data
   */
  private def processSegmentPartition(
      iter: Iterator[InternalRow],
      config: BackfillConfig,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      targetSchema: org.apache.spark.sql.types.StructType
  ): Iterator[(SegmentBackfillResult, Option[Throwable])] = {

    val firstRow = iter.next()
    val segmentID = firstRow.getLong(0)
    val partitionID = segmentToPartitionMap(segmentID)
    val startTime = System.currentTimeMillis()

    // Create writer
    val writeOptions = config.getS3WriteOptions(collectionID, partitionID, segmentID)
    val outputPath = writeOptions("milvus.writer.customPath")
    val optionsMap = new CaseInsensitiveStringMap(writeOptions.asJava)
    val batchWrite = new MilvusLoonBatchWrite(targetSchema, MilvusOption(optionsMap))
    val writer = batchWrite.createBatchWriterFactory(null).createWriter(0, System.currentTimeMillis())

    try {
      var rowCount = 0L
      var nullRowCount = 0L

      def writeRow(row: InternalRow): Unit = {
        val targetFields = (2 until row.numFields).map(i =>
          row.get(i, targetSchema.fields(i - 2).dataType)
        ).toArray
        if (targetFields.forall(_ == null)) nullRowCount += 1
        writer.write(new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(targetFields))
        rowCount += 1
      }

      writeRow(firstRow)
      iter.foreach(writeRow)

      val commitMessage = writer.commit()
      val manifestPaths = commitMessage match {
        case msg: MilvusLoonCommitMessage => Seq(msg.manifestPath)
        case _ => Seq.empty
      }

      batchWrite.commit(Array(commitMessage))
      writer.close()

      Iterator.single((SegmentBackfillResult(
        segmentId = segmentID,
        rowCount = rowCount,
        manifestPaths = manifestPaths,
        outputPath = outputPath,
        executionTimeMs = System.currentTimeMillis() - startTime
      ), None))

    } catch {
      case e: Exception =>
        writer.abort()
        writer.close()
        Iterator.single((SegmentBackfillResult(
          segmentId = segmentID,
          rowCount = 0,
          manifestPaths = Seq.empty,
          outputPath = outputPath,
          executionTimeMs = System.currentTimeMillis() - startTime
        ), Some(e)))
    }
  }

}
