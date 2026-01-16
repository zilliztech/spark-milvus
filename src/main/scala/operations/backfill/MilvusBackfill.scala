package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.write.DataWriter
import org.slf4j.LoggerFactory

import com.zilliz.spark.connector.write.{MilvusLoonBatchWrite, MilvusLoonCommitMessage, MilvusLoonWriter}
import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams, MilvusOption}
import com.zilliz.spark.connector.read.{MilvusSnapshotReader, SnapshotMetadata, StorageV2ManifestItem}

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
   * @param snapshotPath Path to Milvus snapshot metadata JSON file
   * @param config Backfill configuration
   * @return Either error or successful result
   */
  def run(
      spark: SparkSession,
      backfillDataPath: String,
      snapshotPath: String,
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

      // Get PK field and snapshot metadata from collection schema (try snapshot first, fallback to client)
      val pkFieldWithMetadata = getPkFieldAndMetadata(spark, snapshotPath, config, client) match {
        case Left(error) => return Left(error)
        case Right(result) => result
      }

      val pkName = pkFieldWithMetadata.pkField.name
      val pkFieldId = pkFieldWithMetadata.pkField.fieldID
      val snapshotMetadata = pkFieldWithMetadata.snapshotMetadata

      // Read backfill data from Parquet
      val backfillDF = readBackfillData(spark, backfillDataPath) match {
        case Left(error) => return Left(error)
        case Right(df) => df
      }

      // Read original collection data with segment metadata
      // If snapshot metadata is available, use snapshot-based reading (no client calls)
      val originalDF = readCollectionWithMetadata(spark, config, pkFieldId, snapshotMetadata) match {
        case Left(error) => return Left(error)
        case Right(df) => df
      }

      // Validate schema compatibility
      validateSchemaCompatibility(originalDF, backfillDF, pkName) match {
        case Left(error) => return Left(error)
        case Right(_) => // Continue
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
   *
   * @param pkFieldId Primary key field ID to read only PK field
   * @param snapshotMetadata Optional snapshot metadata for offline reading (no client connection)
   */
  private def readCollectionWithMetadata(
      spark: SparkSession,
      config: BackfillConfig,
      pkFieldId: Long,
      snapshotMetadata: Option[SnapshotMetadata]
  ): Either[BackfillError, DataFrame] = {
    try {
      var options = config.getMilvusReadOptions
      options = options + (MilvusOption.ReaderFieldIDs -> pkFieldId.toString)

      // If snapshot metadata is available, use snapshot-based reading (no client calls)
      snapshotMetadata.foreach { metadata =>
        logger.info("Using snapshot-based reading mode (no Milvus client connection for data read)")

        // Enable snapshot mode flag
        options = options + (MilvusOption.SnapshotMode -> "true")

        // Add snapshot collection ID
        options = options + (MilvusOption.SnapshotCollectionId -> metadata.snapshotInfo.collectionId.toString)

        // Add snapshot partition IDs
        options = options + (MilvusOption.SnapshotPartitionIds -> metadata.snapshotInfo.partitionIds.mkString(","))

        // Convert snapshot schema to protobuf bytes and pass as Base64
        val schemaBytes = MilvusSnapshotReader.toProtobufSchemaBytes(metadata.collection.schema)
        val schemaBytesBase64 = java.util.Base64.getEncoder.encodeToString(schemaBytes)
        options = options + (MilvusOption.SnapshotSchemaBytes -> schemaBytesBase64)
        logger.info(s"Passed schema bytes (${schemaBytes.length} bytes) to datasource")

        // Read actual manifest files and pass to datasource
        metadata.storageV2ManifestList.foreach { manifestList =>
          // For each segment, read the actual manifest file from base_path
          val manifestsWithContent = manifestList.flatMap { item =>
            // Parse the simplified manifest to get base_path
            val simplifiedManifest = MilvusSnapshotReader.parseManifestContent(item.manifest)
            simplifiedManifest match {
              case Right(content) =>
                // Read the actual manifest file from base_path/manifest-{ver}
                val manifestPath = s"s3a://${content.basePath}/manifest-${content.ver}"
                logger.info(s"Reading manifest from: $manifestPath")
                try {
                  val manifestContent = spark.read.text(manifestPath)
                    .collect()
                    .map(_.getString(0))
                    .mkString("\n")
                  logger.info(s"Read manifest content (${manifestContent.length} chars) for segment ${item.segmentID}")

                  // Transform manifest columns from field IDs to field names
                  // Storage V2 manifest uses field IDs ("100", "101"), but FFI reader expects field names
                  // Also strip bucket/rootPath prefix from paths since FFI reader will prepend them
                  val transformedManifest = MilvusSnapshotReader.transformManifestColumnsToNames(
                    manifestContent,
                    metadata.collection.schema,
                    bucket = Some(config.s3BucketName),
                    rootPath = Some(config.s3RootPath)
                  ) match {
                    case Right(transformed) =>
                      logger.info(s"Transformed manifest for segment ${item.segmentID}")
                      logger.info(s"Transformed manifest: ${transformed.take(500)}...")
                      transformed
                    case Left(e) =>
                      logger.warn(s"Failed to transform manifest, using original: ${e.getMessage}")
                      manifestContent
                  }

                  // Create new StorageV2ManifestItem with transformed manifest content
                  Some(StorageV2ManifestItem(item.rawSegmentID, transformedManifest))
                } catch {
                  case e: Exception =>
                    logger.error(s"Failed to read manifest from $manifestPath: ${e.getMessage}")
                    None
                }
              case Left(e) =>
                logger.error(s"Failed to parse simplified manifest: ${e.getMessage}")
                None
            }
          }

          if (manifestsWithContent.nonEmpty) {
            val manifestJson = MilvusSnapshotReader.serializeManifestList(manifestsWithContent)
            options = options + (MilvusOption.SnapshotManifests -> manifestJson)
            logger.info(s"Passed ${manifestsWithContent.size} segment manifests with full content to datasource")
          } else {
            logger.warn("No valid manifests found after reading manifest files")
          }
        }
      }

      // Build schema from snapshot if available (for snapshot mode)
      val df = snapshotMetadata match {
        case Some(metadata) =>
          // For snapshot mode, only include the PK field we need to read (not all user fields)
          // FFI reader will only read the columns specified in the schema
          val pkField = metadata.collection.schema.fields.find(_.getFieldIDAsLong == pkFieldId)
          val pkSchema = pkField match {
            case Some(field) =>
              // Create schema with only the PK field
              import org.apache.spark.sql.types._
              val pkFieldType = MilvusSnapshotReader.fieldToSparkType(field)
              StructType(Seq(StructField(field.name, pkFieldType, nullable = true)))
            case None =>
              // Fallback: use full schema if PK field not found
              logger.warn(s"PK field with ID $pkFieldId not found in snapshot schema, using full schema")
              MilvusSnapshotReader.toSparkSchema(metadata.collection.schema, includeSystemFields = false)
          }

          // Add extra columns for segment tracking
          val fullSchema = pkSchema
            .add("segment_id", org.apache.spark.sql.types.LongType, false)
            .add("row_offset", org.apache.spark.sql.types.LongType, false)

          logger.info(s"Reading with schema: ${fullSchema.fieldNames.mkString(", ")}")

          spark.read
            .schema(fullSchema)
            .format("milvus")
            .options(options)
            .load()

        case None =>
          // Client-based mode (existing behavior)
          spark.read
            .format("milvus")
            .options(options)
            .load()
      }

      df.show(10, truncate = false)

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
   */
  private def validateSchemaCompatibility(
      originalDF: DataFrame,
      backfillDF: DataFrame,
      pkName: String
  ): Either[BackfillError, Unit] = {
    try {
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

      Right(())

    } catch {
      case e: Exception =>
        logger.error("Failed to validate schema compatibility", e)
        Left(SchemaValidationError(
          s"Failed to validate schema compatibility: ${e.getMessage}"
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

  /**
   * Read snapshot JSON content from S3 or local file system.
   * Returns the JSON string.
   */
  private def readSnapshotJson(
      spark: SparkSession,
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, String] = {
    if (snapshotPath == null || snapshotPath.isEmpty) {
      return Right("") // Empty path means use client fallback
    }

    try {
      // Check if it's an S3 path
      if (snapshotPath.startsWith("s3://") || snapshotPath.startsWith("s3a://")) {
        logger.info(s"Reading snapshot from S3: $snapshotPath")

        // Construct full S3 path (ensure s3a:// scheme for Hadoop)
        val s3Path = if (snapshotPath.startsWith("s3://")) {
          snapshotPath.replace("s3://", "s3a://")
        } else {
          snapshotPath
        }
        logger.info(s"S3 path after normalization: $s3Path")

        // Configure S3 settings in Spark's Hadoop Configuration
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        hadoopConf.set("fs.s3a.endpoint", config.s3Endpoint)
        hadoopConf.set("fs.s3a.access.key", config.s3AccessKey)
        hadoopConf.set("fs.s3a.secret.key", config.s3SecretKey)
        hadoopConf.set("fs.s3a.path.style.access", "true")
        hadoopConf.set("fs.s3a.connection.ssl.enabled", if (config.s3UseSSL) "true" else "false")

        logger.info(s"Hadoop S3 config: endpoint=${config.s3Endpoint}, bucket=a-bucket, useSSL=${config.s3UseSSL}")

        // Use Spark's DataFrame API to read the file (avoids Hadoop version issues)
        logger.info(s"Reading file using Spark DataFrame API...")
        val df = spark.read.text(s3Path)
        val json = df.collect().map(_.getString(0)).mkString("\n")

        logger.info(s"Successfully read snapshot JSON from S3 (${json.length} chars)")
        Right(json)

      } else {
        // Local file path, read directly
        logger.info(s"Reading snapshot from local file: $snapshotPath")
        val source = scala.io.Source.fromFile(snapshotPath)
        try {
          val json = source.mkString
          Right(json)
        } finally {
          source.close()
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read snapshot JSON: ${e.getMessage}", e)
        Left(DataReadError(snapshotPath, s"Failed to read snapshot file: ${e.getMessage}", Some(e)))
    }
  }

  /**
   * Get primary key field and snapshot metadata from snapshot file with client fallback strategy.
   * Returns both the PK field info and optionally the full snapshot metadata for later use.
   */
  private def getPkFieldAndMetadata(
      spark: SparkSession,
      snapshotPath: String,
      config: BackfillConfig,
      client: MilvusClient
  ): Either[BackfillError, PkFieldWithMetadata] = {
    // Read snapshot JSON content (from S3 or local file)
    val snapshotJson = readSnapshotJson(spark, snapshotPath, config) match {
      case Left(error) => return Left(error)
      case Right(json) => json
    }

    // Try to get PK field from snapshot first
    if (snapshotJson.nonEmpty) {
      MilvusSnapshotReader.parseSnapshotMetadata(snapshotJson) match {
        case Right(metadata) =>
          val pkField = metadata.collection.schema.fields.find(_.isPrimaryKey.getOrElse(false)).get
          Right(PkFieldWithMetadata(
            PkFieldInfo(pkField.name, pkField.getFieldIDAsLong),
            Some(metadata)  // Return the full metadata for snapshot-based reading
          ))

        case Left(snapshotError) =>
          // Fall back to Milvus client
          logger.warn(s"Failed to parse snapshot metadata: ${snapshotError.getMessage}, falling back to Milvus client")
          client.getPkField(config.databaseName, config.collectionName) match {
            case scala.util.Success((pkName, fieldId)) =>
              Right(PkFieldWithMetadata(PkFieldInfo(pkName, fieldId), None))

            case scala.util.Failure(e) =>
              val errorMsg = s"Failed to get PK field from both snapshot and Milvus client. " +
                s"Snapshot error: ${snapshotError.getMessage}. Client error: ${e.getMessage}"
              logger.error(errorMsg, e)
              Left(ConnectionError(message = errorMsg, cause = Some(e)))
          }
      }
    } else {
      // Empty snapshot path, use client directly
      client.getPkField(config.databaseName, config.collectionName) match {
        case scala.util.Success((pkName, fieldId)) =>
          Right(PkFieldWithMetadata(PkFieldInfo(pkName, fieldId), None))

        case scala.util.Failure(e) =>
          val errorMsg = s"Failed to get PK field from Milvus client: ${e.getMessage}"
          logger.error(errorMsg, e)
          Left(ConnectionError(message = errorMsg, cause = Some(e)))
      }
    }
  }

  /**
   * Case class to hold PK field information
   */
  private case class PkFieldInfo(name: String, fieldID: Long)

  /**
   * Case class to hold PK field info with optional snapshot metadata
   */
  private case class PkFieldWithMetadata(
      pkField: PkFieldInfo,
      snapshotMetadata: Option[SnapshotMetadata]
  )

}
