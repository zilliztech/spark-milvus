package com.zilliz.spark.connector.operations.backfill

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import com.zilliz.spark.connector.{
  MilvusClient,
  MilvusConnectionParams,
  MilvusOption
}
import com.zilliz.spark.connector.read.{
  MilvusSnapshotReader,
  SnapshotMetadata,
  StorageV2ManifestItem
}
import com.zilliz.spark.connector.write.{
  MilvusLoonBatchWrite,
  MilvusLoonCommitMessage,
  MilvusLoonWriter
}

/** Backfill operation for Milvus collections
  *
  * This object provides functionality to backfill new fields into existing
  * Milvus collections by reading the original data, joining with new field
  * data, and writing per-segment binlog files.
  */
object MilvusBackfill {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Backfill new fields into a Milvus collection
    *
    * @param spark
    *   SparkSession
    * @param backfillDataPath
    *   Path to Parquet file containing new field data with schema (pk,
    *   new_field1, new_field2, ...)
    * @param snapshotPath
    *   Path to Milvus snapshot metadata JSON file
    * @param config
    *   Backfill configuration
    * @return
    *   Either error or successful result
    */
  def run(
      spark: SparkSession,
      backfillDataPath: String,
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, BackfillResult] = {

    val startTime = System.currentTimeMillis()

    // Validate S3/writer configuration (always required)
    config.validate() match {
      case Left(error) =>
        return Left(SchemaValidationError(s"Invalid configuration: $error"))
      case Right(_) => // Continue
    }

    logger.info(s"Backfill mode: ${config.mode}")

    // Step 1: Try to load snapshot metadata
    val snapshotMetadataOpt =
      loadSnapshotMetadata(spark, snapshotPath, config) match {
        case Left(error) => return Left(error)
        case Right(opt)  => opt
      }

    // Step 1b: Eagerly materialize StorageV2 segments (AVRO manifests +
    // parquet-footer join) so both the read path (which serializes them into
    // the DataSource option) and the write path (which dispatches per
    // segment's storage version) share the same view without hitting S3 twice.
    val v2Segments: Seq[com.zilliz.spark.connector.read.V2SegmentInfo] =
      snapshotMetadataOpt match {
        case Some(meta) if meta.manifestList.nonEmpty =>
          loadV2Segments(spark, meta, config) match {
            case Right(segs) => segs
            case Left(err)   => return Left(err)
          }
        case _ => Seq.empty
      }

    // Step 2: Create Milvus client only if no snapshot is available
    var client: MilvusClient = null
    if (snapshotMetadataOpt.isEmpty) {
      config.validateForClientMode() match {
        case Left(error) =>
          return Left(
            SchemaValidationError(
              s"No snapshot provided and invalid client configuration: $error"
            )
          )
        case Right(_) =>
      }
      client = MilvusClient(
        MilvusConnectionParams(
          uri = config.milvusUri,
          token = config.milvusToken,
          databaseName = config.databaseName
        )
      )
    }

    try {
      // Step 3: Get PK field info
      val (pkName, pkFieldId) = snapshotMetadataOpt match {
        case Some(metadata) =>
          val pkField = metadata.collection.schema.fields
            .find(_.isPrimaryKey.getOrElse(false))
            .getOrElse(
              return Left(
                SchemaValidationError(
                  "No primary key field found in snapshot schema"
                )
              )
            )
          (pkField.name, pkField.getFieldIDAsLong)
        case None =>
          client.getPkField(config.databaseName, config.collectionName) match {
            case scala.util.Success((name, id)) => (name, id)
            case scala.util.Failure(e) =>
              return Left(
                ConnectionError(
                  message = s"Failed to get PK field: ${e.getMessage}",
                  cause = Some(e)
                )
              )
          }
      }

      // Read backfill data from Parquet
      val rawBackfillDF =
        readBackfillData(spark, backfillDataPath, config) match {
          case Left(error) => return Left(error)
          case Right(df)   => df
        }

      // Reproject via the column mapping (or legacy implicit mapping) so that
      // downstream code can assume the DataFrame's column names match the
      // Milvus schema exactly — in particular, the pk column is named pkName.
      val backfillDF = applyColumnMapping(
        rawBackfillDF,
        pkName,
        config.columnMapping
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      // Extract new field names (all post-mapping columns except the PK).
      val newFieldNames = backfillDF.schema.fields
        .map(_.name)
        .filterNot(_ == pkName)
        .toSeq

      // Build field name -> field ID mapping from collection schema. Resolved
      // early (was post-join) so coalesce mode can ask the reader to also
      // materialize the target fields from source.
      val fieldNameToId: Map[String, Long] = snapshotMetadataOpt match {
        case Some(metadata) =>
          MilvusSnapshotReader.getFieldNameToIdMap(metadata.collection.schema)
        case None =>
          return Left(
            SchemaValidationError(
              "ADDFIELD backfill requires field ID mapping from snapshot. " +
                "Please provide a snapshot path to resolve correct field IDs."
            )
          )
      }

      val missing = newFieldNames.filterNot(fieldNameToId.contains)
      if (missing.nonEmpty) {
        return Left(
          SchemaValidationError(
            s"Fields not found in snapshot schema: ${missing.mkString(", ")}. " +
              s"Available fields: ${fieldNameToId.keys.mkString(", ")}"
          )
        )
      }
      val newFieldNameToId = newFieldNames.map(n => n -> fieldNameToId(n)).toMap

      // In coalesce mode, also read each target field from source so
      // `coalesce(src, backfill)` can keep the existing value when non-null.
      // Requires a snapshot — we need the field's Spark type.
      val isCoalesceMode = config.mode == MilvusOption.BackfillModeCoalesce
      val extraReadFields
          : Seq[(String, Long, org.apache.spark.sql.types.StructField)] =
        if (isCoalesceMode) {
          val metadata = snapshotMetadataOpt.getOrElse {
            return Left(
              SchemaValidationError(
                s"--mode=${MilvusOption.BackfillModeCoalesce} requires a " +
                  "snapshot path so source field values can be read"
              )
            )
          }
          newFieldNames.map { n =>
            val fid = newFieldNameToId(n)
            val field = metadata.collection.schema.fields
              .find(_.getFieldIDAsLong == fid)
              .getOrElse(
                return Left(
                  SchemaValidationError(
                    s"Field '$n' (id=$fid) not found in snapshot schema"
                  )
                )
              )
            val sparkType = MilvusSnapshotReader.fieldToSparkType(field)
            (
              n,
              fid,
              org.apache.spark.sql.types
                .StructField(n, sparkType, nullable = true)
            )
          }
        } else Seq.empty

      // In coalesce mode, parquet column types must match snapshot field
      // types exactly (see validateCoalesceTypes for rationale).
      if (isCoalesceMode) {
        validateCoalesceTypes(backfillDF.schema, extraReadFields) match {
          case Left(error) => return Left(error)
          case Right(_)    => // Continue
        }
      }

      // Read original collection data with segment metadata
      val originalDF = readCollectionWithMetadata(
        spark,
        config,
        pkFieldId,
        snapshotMetadataOpt,
        v2Segments,
        extraReadFields
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      // Validate schema compatibility
      validateSchemaCompatibility(originalDF, backfillDF, pkName) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      // Merge original and backfill DataFrames according to mode.
      val joinedDF = performJoin(
        originalDF,
        backfillDF,
        pkName,
        newFieldNames,
        config.mode
      )

      // Step 4: Get collection metadata (collectionID, segment-to-partition mapping, base paths)
      val (collectionID, segmentToPartitionMap, segmentBasePathMap) =
        snapshotMetadataOpt match {
          case Some(metadata) =>
            extractMetadataFromSnapshot(metadata, v2Segments)
          case None =>
            val (colId, segPartMap) =
              retrieveMilvusMetadata(config, client) match {
                case Left(error)     => return Left(error)
                case Right(metadata) => metadata
              }
            (colId, segPartMap, Map.empty[Long, String])
        }

      // Set of segment IDs that are StorageV2 (packed-parquet, no manifest).
      // V3 segments continue to use the existing MilvusLoonWriter flow.
      val v2SegmentIdSet: Set[Long] = v2Segments.map(_.segmentId).toSet

      // Process each segment
      val segmentResults = processSegments(
        spark,
        joinedDF,
        collectionID,
        segmentToPartitionMap,
        segmentBasePathMap,
        v2SegmentIdSet,
        config,
        newFieldNames,
        newFieldNameToId
      ) match {
        case Left(error)    => return Left(error)
        case Right(results) => results
      }

      // Build final result
      val executionTime = System.currentTimeMillis() - startTime
      val partitionIDs = segmentToPartitionMap.values.toSet

      val result = BackfillResult.success(
        segmentResults = segmentResults,
        executionTimeMs = executionTime,
        collectionId = collectionID,
        partitionId = if (partitionIDs.size == 1) partitionIDs.head else -1,
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

  /** Read backfill data from Parquet file
    */
  private def readBackfillData(
      spark: SparkSession,
      rawPath: String,
      config: BackfillConfig
  ): Either[BackfillError, DataFrame] = {
    // Hadoop 3.4.1 has separate s3:// and s3a:// FileSystem implementations
    // and per-bucket fs.s3a.bucket.<b>.* config is only honored by
    // S3AFileSystem. Force the s3a scheme so the credentials we just wrote
    // actually take effect.
    val path = normalizeS3Scheme(rawPath)
    try {
      // Ensure Hadoop S3A is configured for the source bucket (may differ
      // from the Milvus storage bucket) before reading the parquet.
      configureHadoopS3ForPath(spark, path, config, isSource = true)
      val df = spark.read.parquet(path)

      // Minimum shape check; pk presence and field-count checks are enforced
      // after column mapping is applied (see applyColumnMapping).
      if (df.columns.isEmpty) {
        return Left(
          DataReadError(
            path = path,
            message = "Backfill parquet is empty (no columns)"
          )
        )
      }

      Right(df)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read Parquet file from $path", e)
        Left(
          DataReadError(
            path = path,
            message = s"Failed to read Parquet file: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** Project the raw backfill DataFrame through a parquet-column → Milvus-field
    * mapping so downstream code sees column names that match the Milvus schema
    * exactly (including the PK, which must be named `pkName`).
    *
    * When `userMapping` is None, a legacy implicit mapping is synthesized: the
    * literal `"pk"` column is renamed to `pkName`, every other column is kept
    * as-is. This preserves the pre-existing contract (parquet must have a `pk`
    * column plus one or more field columns).
    */
  private[backfill] def applyColumnMapping(
      df: DataFrame,
      pkName: String,
      userMapping: Option[Map[String, String]]
  ): Either[BackfillError, DataFrame] = {
    val cols = df.columns.toSeq
    val colSet = cols.toSet

    val mapping: Map[String, String] = userMapping match {
      case Some(m) => m
      case None    =>
        // Legacy: require a literal "pk" column; transparently rename it to pkName.
        if (!colSet.contains("pk")) {
          return Left(
            SchemaValidationError(
              "Backfill parquet must contain a 'pk' column (or supply --column-mapping to rename the PK column)"
            )
          )
        }
        // If the parquet already has a column named pkName, the implicit
        // {pk→pkName} rename would collide with it. Surface a dedicated error
        // rather than letting the generic duplicate-target check fire and
        // reference "column mapping" — users in the legacy path never passed
        // --column-mapping.
        if (pkName != "pk" && colSet.contains(pkName)) {
          return Left(
            SchemaValidationError(
              s"Backfill parquet contains both a 'pk' column and a column named '$pkName' " +
                s"(the collection's primary-key field). Remove one, or supply --column-mapping to disambiguate."
            )
          )
        }
        cols.map(c => if (c == "pk") c -> pkName else c -> c).toMap
    }

    // Mapping keys must all exist in the parquet.
    val missingSrc = mapping.keySet.diff(colSet)
    if (missingSrc.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"column mapping references parquet columns that don't exist: " +
            s"${missingSrc.mkString(", ")}. Available: ${cols.mkString(", ")}"
        )
      )
    }

    // Mapping values must be unique; two parquet columns cannot both point at
    // the same Milvus field.
    val dupTargets = mapping.values.groupBy(identity).collect {
      case (k, v) if v.size > 1 => k
    }
    if (dupTargets.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"column mapping has duplicate targets: ${dupTargets.mkString(", ")}"
        )
      )
    }

    // The PK field must appear as a target so we can locate it after renaming.
    if (!mapping.values.toSet.contains(pkName)) {
      return Left(
        SchemaValidationError(
          s"column mapping must include the primary key field '$pkName' as a target"
        )
      )
    }

    // At least one non-pk field must remain.
    val newFieldTargets = mapping.values.toSet - pkName
    if (newFieldTargets.isEmpty) {
      return Left(
        SchemaValidationError(
          "column mapping must include at least one non-PK field to backfill"
        )
      )
    }

    // Single-pass aliased select. A foldLeft of withColumnRenamed would rename
    // sequentially and corrupt chains like {a→b, b→c} (the second rename would
    // hit the already-renamed column) and swaps like {a→b, b→a}.
    val orderedKeys = cols.filter(mapping.contains)
    val renamed = df.select(
      orderedKeys.map(src => df.col(src).as(mapping(src))): _*
    )
    Right(renamed)
  }

  /** Read collection data with segment_id and row_offset metadata segment_id
    * and row_offset are used to match with the original sequence of rows for
    * each segment
    *
    * @param pkFieldId
    *   Primary key field ID to read only PK field
    * @param snapshotMetadata
    *   Optional snapshot metadata for offline reading (no client connection)
    */
  private def readCollectionWithMetadata(
      spark: SparkSession,
      config: BackfillConfig,
      pkFieldId: Long,
      snapshotMetadata: Option[SnapshotMetadata],
      v2Segments: Seq[com.zilliz.spark.connector.read.V2SegmentInfo],
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ] = Seq.empty
  ): Either[BackfillError, DataFrame] = {
    try {
      var options = config.getMilvusReadOptions
      val allFieldIds = pkFieldId +: extraReadFields.map(_._2)
      options =
        options + (MilvusOption.ReaderFieldIDs -> allFieldIds.mkString(","))

      // If snapshot metadata is available, use snapshot-based reading (no client calls)
      snapshotMetadata.foreach { metadata =>
        // Enable snapshot mode flag
        options = options + (MilvusOption.SnapshotMode -> "true")

        // Override connection options for snapshot mode (no client needed)
        options = options + ("milvus.uri" -> "dummy://snapshot-mode")
        options =
          options + ("milvus.collection.name" -> metadata.collection.schema.name)

        // Add snapshot collection ID
        options =
          options + (MilvusOption.SnapshotCollectionId -> metadata.snapshotInfo.collectionId.toString)

        // Add snapshot partition IDs
        options =
          options + (MilvusOption.SnapshotPartitionIds -> metadata.snapshotInfo.partitionIds
            .mkString(","))

        // Convert snapshot schema to protobuf bytes and pass as Base64
        val schemaBytes =
          MilvusSnapshotReader.toProtobufSchemaBytes(metadata.collection.schema)
        val schemaBytesBase64 =
          java.util.Base64.getEncoder.encodeToString(schemaBytes)
        options =
          options + (MilvusOption.SnapshotSchemaBytes -> schemaBytesBase64)

        metadata.storageV2ManifestList.foreach { manifestList =>
          // Pass original manifest JSON (containing both ver and base_path) so that
          // the DataSource can extract readVersion and lock reads to snapshot version
          if (manifestList.nonEmpty) {
            val manifestJson =
              MilvusSnapshotReader.serializeManifestList(manifestList)
            options = options + (MilvusOption.SnapshotManifests -> manifestJson)
          } else {
            logger.warn("No valid manifests found in snapshot")
          }
        }

        // Pre-loaded StorageV2 (non-manifest packed parquet) segments — hand
        // them to the DataSource via SnapshotV2Segments so planner can emit
        // MilvusPackedV2InputPartitions. Loading itself happened earlier in
        // `run()` via `loadV2Segments`.
        if (v2Segments.nonEmpty) {
          val segJson = MilvusSnapshotReader.serializeV2Segments(v2Segments)
          options = options + (MilvusOption.SnapshotV2Segments -> segJson)
          logger.info(
            s"Attached ${v2Segments.size} StorageV2 packed segment(s) to read options"
          )
        }
      }

      // Build schema from snapshot if available (for snapshot mode)
      val df = snapshotMetadata match {
        case Some(metadata) =>
          // For snapshot mode, only include the PK field we need to read (not all user fields)
          // FFI reader will only read the columns specified in the schema
          val pkField = metadata.collection.schema.fields
            .find(_.getFieldIDAsLong == pkFieldId)
          val pkSchema = pkField match {
            case Some(field) =>
              // Create schema with only the PK field
              import org.apache.spark.sql.types._
              val pkFieldType = MilvusSnapshotReader.fieldToSparkType(field)
              StructType(
                Seq(StructField(field.name, pkFieldType, nullable = true))
              )
            case None =>
              // Fallback: use full schema if PK field not found
              logger.warn(
                s"PK field with ID $pkFieldId not found in snapshot schema, using full schema"
              )
              MilvusSnapshotReader.toSparkSchema(
                metadata.collection.schema,
                includeSystemFields = false
              )
          }

          // Extend with any extra fields requested (coalesce mode). Field
          // order in the schema must match the ReaderFieldIDs order.
          val withExtras = extraReadFields.foldLeft(pkSchema) {
            case (acc, (_, _, field)) => acc.add(field)
          }

          // Add extra columns for segment tracking
          val fullSchema = withExtras
            .add("segment_id", org.apache.spark.sql.types.LongType, false)
            .add("row_offset", org.apache.spark.sql.types.LongType, false)

          logger.info(
            s"Reading with schema: ${fullSchema.fieldNames.mkString(", ")}"
          )

          spark.read
            .schema(fullSchema)
            .format("com.zilliz.spark.connector.sources.MilvusDataSource")
            .options(options)
            .load()

        case None =>
          // Client-based mode (existing behavior)
          spark.read
            .format("com.zilliz.spark.connector.sources.MilvusDataSource")
            .options(options)
            .load()
      }

      // Validate that segment_id and row_offset are present
      if (
        !df.columns.contains("segment_id") || !df.columns.contains("row_offset")
      ) {
        return Left(
          ConnectionError(
            message =
              "Failed to read collection data with segment_id and row_offset. " +
                "Ensure milvus.extra.columns is set correctly."
          )
        )
      }

      Right(df)
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to read Milvus collection ${config.collectionName}",
          e
        )
        Left(
          ConnectionError(
            message =
              s"Failed to read Milvus collection ${config.collectionName}: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** Coalesce mode requires parquet column types to match snapshot field types
    * exactly. Spark's `coalesce(src, bf)` would otherwise widen to a common
    * supertype (e.g. Int + Long → Long) and the writer would emit binlogs whose
    * Arrow type no longer matches the Milvus field — Milvus would later misread
    * them.
    */
  private[backfill] def validateCoalesceTypes(
      backfillSchema: org.apache.spark.sql.types.StructType,
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ]
  ): Either[BackfillError, Unit] = {
    val backfillTypes =
      backfillSchema.fields.map(f => f.name -> f.dataType).toMap
    val mismatches = extraReadFields.collect {
      case (name, _, srcField)
          if backfillTypes
            .get(name)
            .exists(_ != srcField.dataType) =>
        s"$name (snapshot=${srcField.dataType.simpleString}, " +
          s"parquet=${backfillTypes(name).simpleString})"
    }
    if (mismatches.nonEmpty) {
      Left(
        SchemaValidationError(
          s"--mode=${MilvusOption.BackfillModeCoalesce} requires backfill " +
            s"parquet column types to match snapshot field types exactly. " +
            s"Mismatched: ${mismatches.mkString(", ")}"
        )
      )
    } else {
      Right(())
    }
  }

  /** Validate schema compatibility between original and new field data
    */
  private def validateSchemaCompatibility(
      originalDF: DataFrame,
      backfillDF: DataFrame,
      pkName: String
  ): Either[BackfillError, Unit] = {
    try {
      // Find the primary key field in original data
      val pkField = originalDF.schema.fields
        .find(_.name == pkName)
        .getOrElse {
          return Left(
            SchemaValidationError(
              s"Original collection data must have primary key field '$pkName'"
            )
          )
        }

      // Find the pk field in new field data (post-mapping column name = pkName)
      val newPkField = backfillDF.schema.fields
        .find(_.name == pkName)
        .getOrElse {
          return Left(
            SchemaValidationError(
              s"Backfill data must have PK field '$pkName' (after column mapping)"
            )
          )
        }

      // Validate types match
      if (pkField.dataType != newPkField.dataType) {
        return Left(
          SchemaValidationError(
            s"Primary key type mismatch: original=${pkField.dataType}, new=${newPkField.dataType}"
          )
        )
      }

      Right(())

    } catch {
      case e: Exception =>
        logger.error("Failed to validate schema compatibility", e)
        Left(
          SchemaValidationError(
            s"Failed to validate schema compatibility: ${e.getMessage}"
          )
        )
    }
  }

  /** Merge original (source) rows with backfill rows per `mode`.
    *
    *   - overwrite: left join on PK; backfill value replaces source (source
    *     only contributes PK + segment tracking columns).
    *   - coalesce: original side carries source values for each target field;
    *     after the left join, compute `coalesce(src, backfill)` per field (keep
    *     source when non-null, otherwise use backfill).
    */
  private[backfill] def performJoin(
      originalDF: DataFrame,
      backfillDF: DataFrame,
      pkName: String,
      newFieldNames: Seq[String],
      mode: String
  ): DataFrame = {
    mode match {
      case MilvusOption.BackfillModeCoalesce =>
        // Rename backfill-side target columns to avoid name collisions with
        // source-side columns now present on originalDF.
        val suffix = "__bf__"
        val renamedBackfill = newFieldNames.foldLeft(backfillDF) { (df, n) =>
          df.withColumnRenamed(n, n + suffix)
        }
        val joined = originalDF.join(renamedBackfill, Seq(pkName), "left")
        newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(n, coalesce(df.col(n), df.col(n + suffix)))
            .drop(n + suffix)
        }

      case _ =>
        // Use the using-column join form: both sides share the pkName column
        // (guaranteed by applyColumnMapping), so Spark collapses it into one,
        // avoiding ambiguous-reference errors downstream.
        originalDF.join(backfillDF, Seq(pkName), "left")
    }
  }

  /** Retrieve Milvus metadata (collection ID and segment-to-partition mapping)
    * Supports multi-partition collections by tracking partition ID for each
    * segment
    */
  private def retrieveMilvusMetadata(
      config: BackfillConfig,
      client: MilvusClient
  ): Either[BackfillError, (Long, Map[Long, Long])] = {
    try {
      val segments = client
        .getSegments(config.databaseName, config.collectionName)
        .getOrElse {
          return Left(
            ConnectionError(
              message =
                s"No segments found for collection ${config.collectionName}"
            )
          )
        }

      if (segments.isEmpty) {
        return Left(
          ConnectionError(
            message = s"Collection ${config.collectionName} has no segments"
          )
        )
      }

      val collectionID = segments.head.collectionID

      // Build mapping of segment ID -> partition ID to support multi-partition collections
      val segmentToPartitionMap = segments.map { seg =>
        seg.segmentID -> seg.partitionID
      }.toMap

      Right((collectionID, segmentToPartitionMap))

    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to retrieve Milvus metadata for collection ${config.collectionName}",
          e
        )
        Left(
          ConnectionError(
            message = s"Failed to retrieve Milvus metadata: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** Process each segment separately by distributing to Spark executors Each
    * segment is processed by exactly one FFI writer on a single executor
    * Supports multi-partition collections by tracking partition ID per segment
    */
  private def processSegments(
      spark: SparkSession,
      joinedDF: DataFrame,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      segmentBasePathMap: Map[Long, String],
      v2SegmentIdSet: Set[Long],
      config: BackfillConfig,
      newFieldNames: Seq[String],
      fieldNameToId: Map[String, Long] = Map.empty
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
      // CRITICAL: .copy() is required because queryExecution.toRdd produces an iterator
      // that reuses the same UnsafeRow buffer. Without copy, keyBy/partitionBy's
      // ExternalSorter stores references to the same mutable buffer, causing all
      // rows to contain the last row's data.
      val repartitionedRDD = preparedDF.queryExecution.toRdd
        .map(_.copy()) // Materialize each row to avoid UnsafeRow reuse
        .keyBy(_.getLong(0)) // segment_id is at index 0
        .partitionBy(segmentPartitioner)
        .values
        .mapPartitions(iter =>
          iter.toSeq.sortBy(_.getLong(1)).iterator
        ) // Sort by row_offset

      // Broadcast configuration to executors
      val broadcastConfig = spark.sparkContext.broadcast(config)
      val broadcastCollectionID = spark.sparkContext.broadcast(collectionID)
      val broadcastSegmentToPartitionMap =
        spark.sparkContext.broadcast(segmentToPartitionMap)
      val broadcastSegmentBasePathMap =
        spark.sparkContext.broadcast(segmentBasePathMap)
      val broadcastV2SegmentIdSet = spark.sparkContext.broadcast(v2SegmentIdSet)
      val broadcastTargetSchema = spark.sparkContext.broadcast(targetSchema)
      val broadcastFieldNameToId = spark.sparkContext.broadcast(fieldNameToId)

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
              broadcastTargetSchema.value,
              broadcastFieldNameToId.value
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
      broadcastFieldNameToId.unpersist()

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

      logger.info("=== Backfill Summary ===")
      logger.info(s"Total segments: ${results.length}")
      logger.info(s"Total rows processed: $totalRows")
      logger.info(s"Total time for all segments: ${totalTime}ms")
      logger.info(s"Average time per segment: ${avgTime}ms")

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

  /** Process a single partition containing exactly one segment This is called
    * by each Spark executor to write one segment's data
    */
  private def processSegmentPartition(
      iter: Iterator[InternalRow],
      config: BackfillConfig,
      collectionID: Long,
      segmentToPartitionMap: Map[Long, Long],
      segmentBasePathMap: Map[Long, String],
      v2SegmentIdSet: Set[Long],
      targetSchema: org.apache.spark.sql.types.StructType,
      fieldNameToId: Map[String, Long] = Map.empty
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
        fieldNameToId,
        config,
        startTime
      )
    }

    // Create writer — use manifest basePath if available, otherwise generate path
    val writeOptions = segmentBasePathMap.get(segmentID) match {
      case Some(basePath) =>
        config.getS3WriteOptionsForBasePath(basePath, segmentID, fieldNameToId)
      case None =>
        config.getS3WriteOptions(
          collectionID,
          partitionID,
          segmentID,
          fieldNameToId
        )
    }
    val outputPath = writeOptions("milvus.writer.customPath")

    val optionsMap = new CaseInsensitiveStringMap(writeOptions.asJava)
    val batchWrite =
      new MilvusLoonBatchWrite(targetSchema, MilvusOption(optionsMap))
    val writer = batchWrite
      .createBatchWriterFactory(null)
      .createWriter(0, System.currentTimeMillis())

    try {
      var rowCount = 0L
      var nullRowCount = 0L

      def writeRow(row: InternalRow): Unit = {
        val targetFields = (2 until row.numFields)
          .map(i => row.get(i, targetSchema.fields(i - 2).dataType))
          .toArray
        if (targetFields.forall(_ == null)) nullRowCount += 1
        writer.write(
          new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
            targetFields
          )
        )
        rowCount += 1
      }

      writeRow(firstRow)
      iter.foreach(writeRow)

      val commitMessage = writer.commit()
      val (manifestPaths, committedVersion) = commitMessage match {
        case msg: MilvusLoonCommitMessage =>
          (Seq(msg.manifestPath), msg.committedVersion)
        case _ => (Seq.empty[String], -1L)
      }

      batchWrite.commit(Array(commitMessage))
      writer.close()

      Iterator.single(
        (
          SegmentBackfillResult(
            segmentId = segmentID,
            rowCount = rowCount,
            manifestPaths = manifestPaths,
            outputPath = outputPath,
            executionTimeMs = System.currentTimeMillis() - startTime,
            committedVersion = committedVersion
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

  /** Decode the StorageV2 segments referenced by the snapshot's
    * `manifest_list`. Each AVRO gives us slot→paths; the matching parquet
    * footer's `group_field_id_list` recovers the real field IDs per column
    * group. Called once per backfill and threaded into both the read and write
    * paths.
    */
  private def loadV2Segments(
      spark: SparkSession,
      metadata: SnapshotMetadata,
      config: BackfillConfig
  ): Either[BackfillError, Seq[
    com.zilliz.spark.connector.read.V2SegmentInfo
  ]] = {
    if (metadata.manifestList.isEmpty) return Right(Seq.empty)
    try {
      // Register S3A bucket credentials so V2SegmentLoader can read AVRO +
      // parquet footers from minio / S3. The main bucket (not the source
      // parquet bucket) holds the snapshot.
      configureHadoopS3ForPath(
        spark,
        s"s3a://${config.s3BucketName}/",
        config,
        isSource = false
      )
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      com.zilliz.spark.connector.read.V2SegmentLoader
        .loadV2Segments(
          metadata.manifestList,
          config.s3BucketName,
          hadoopConf
        ) match {
        case Right(segs) =>
          logger.info(
            s"Loaded ${segs.size} StorageV2 segment(s) from snapshot AVRO manifest_list"
          )
          Right(segs)
        case Left(err) =>
          Left(
            SchemaValidationError(
              s"Failed to load StorageV2 segments from AVRO manifests: ${err.getMessage}"
            )
          )
      }
    } catch {
      case e: Exception =>
        Left(
          SchemaValidationError(
            s"Failed to load StorageV2 segments: ${e.getMessage}"
          )
        )
    }
  }

  /** Load and parse snapshot metadata from file. Returns None if snapshot path
    * is empty. Returns Left(error) if snapshot path is provided but parsing
    * fails.
    */
  private def loadSnapshotMetadata(
      spark: SparkSession,
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Option[SnapshotMetadata]] = {
    if (snapshotPath == null || snapshotPath.isEmpty) return Right(None)

    readSnapshotJson(spark, snapshotPath, config) match {
      case Right(json) if json.nonEmpty =>
        MilvusSnapshotReader.parseSnapshotMetadata(json) match {
          case Right(metadata) => Right(Some(metadata))
          case Left(e) =>
            Left(
              SchemaValidationError(
                s"Failed to parse snapshot metadata: ${e.getMessage}"
              )
            )
        }
      case Right(_) =>
        Left(SchemaValidationError(s"Snapshot file is empty: $snapshotPath"))
      case Left(e) =>
        Left(
          SchemaValidationError(s"Failed to read snapshot file: ${e.message}")
        )
    }
  }

  /** StorageV2 write path: writes one parquet per new field under
    * `files/insert_log/{coll}/{part}/{seg}/{newFieldID}/{logID}` via
    * [[com.zilliz.spark.connector.write.MilvusV2BinlogWriter]]. Backfill always
    * emits single-field column groups, so `columnGroupID` in the path equals
    * the new field's ID (milvus convention for 1-field groups).
    */
  private def processV2SegmentPartition(
      iter: Iterator[InternalRow],
      firstRow: InternalRow,
      segmentID: Long,
      partitionID: Long,
      collectionID: Long,
      targetSchema: org.apache.spark.sql.types.StructType,
      fieldNameToId: Map[String, Long],
      config: BackfillConfig,
      startTime: Long
  ): Iterator[(SegmentBackfillResult, Option[Throwable])] = {
    import com.zilliz.spark.connector.write.{MilvusV2BinlogWriter, V2BinlogFile}

    // Build per-field mapping in targetSchema order.
    val fieldNames = targetSchema.fieldNames.toSeq
    val fieldIds = fieldNames.map { name =>
      fieldNameToId.getOrElse(
        name,
        throw new IllegalStateException(
          s"StorageV2 backfill for segment $segmentID: field '$name' has no field ID in the snapshot schema"
        )
      )
    }

    // Simple monotonic logID allocator seeded by task-start nanos. Plan
    // names this as a future injection point (caller-provided global ID),
    // but a monotonic local sequence is sufficient for the single-task
    // single-segment scope (no collisions expected within one file).
    val logIdBase = System.nanoTime()
    val logIdCounter = new java.util.concurrent.atomic.AtomicLong(logIdBase)
    val allocator: () => Long = () => logIdCounter.incrementAndGet()

    val outputRoot =
      s"s3a://${config.s3BucketName}/${config.s3RootPath.stripSuffix("/")}/insert_log/" +
        s"$collectionID/$partitionID/$segmentID"

    // Reuse the same MilvusOption plumbing the V3 writer uses — the new V2
    // writer talks to S3 via Arrow's filesystem (inside milvus-storage), so
    // it needs the FS config, not Hadoop S3A config.
    val writeOptions = config.getS3WriteOptionsForBasePath(
      s"${config.s3RootPath.stripSuffix("/")}/insert_log/$collectionID/$partitionID/$segmentID",
      segmentID,
      fieldNameToId
    )
    val milvusOption = MilvusOption(
      new CaseInsensitiveStringMap(writeOptions.asJava)
    )

    val writer = new MilvusV2BinlogWriter(
      collectionId = collectionID,
      partitionId = partitionID,
      segmentId = segmentID,
      newFieldNames = fieldNames,
      newFieldIds = fieldIds,
      targetSchema = targetSchema,
      milvusOption = milvusOption,
      allocateLogId = allocator
    )

    var rowCount = 0L
    try {
      // The first two columns of each row are (segment_id, row_offset); the
      // V2 writer only wants the trailing data columns, so we build a
      // projected row before calling write().
      def projected(row: InternalRow): InternalRow = {
        val values = (2 until row.numFields)
          .map(i => row.get(i, targetSchema.fields(i - 2).dataType))
          .toArray
        new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values)
      }

      writer.write(projected(firstRow))
      rowCount += 1
      iter.foreach { row =>
        writer.write(projected(row))
        rowCount += 1
      }
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
            rowCount = rowCount,
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
            )
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

  /** Extract collection metadata from snapshot: collectionID,
    * segment-to-partition mapping, segment base paths. Partition IDs are
    * derived from manifest basePaths:
    * {rootPath}/insert_log/{col_id}/{part_id}/{seg_id}
    */
  private def extractMetadataFromSnapshot(
      metadata: SnapshotMetadata,
      v2Segments: Seq[com.zilliz.spark.connector.read.V2SegmentInfo] = Seq.empty
  ): (Long, Map[Long, Long], Map[Long, String]) = {
    val collectionID = metadata.snapshotInfo.collectionId

    val manifestList = metadata.storageV2ManifestList.getOrElse(Seq.empty)
    var segmentToPartitionMap = Map.empty[Long, Long]
    var segmentBasePathMap = Map.empty[Long, String]

    // V3 (manifest-based) segments: basePath carries partition id too.
    for (item <- manifestList) {
      val segId = item.segmentID
      MilvusSnapshotReader.parseManifestContent(item.manifest) match {
        case Right(mc) =>
          // Extract partition ID from basePath: .../insert_log/{col_id}/{part_id}/{seg_id}
          val parts = mc.basePath.split("/")
          val insertLogIdx = parts.indexOf("insert_log")
          if (insertLogIdx >= 0 && insertLogIdx + 2 < parts.length) {
            try {
              val partitionId = parts(insertLogIdx + 2).toLong
              segmentBasePathMap += (segId -> mc.basePath)
              segmentToPartitionMap += (segId -> partitionId)
            } catch {
              case _: NumberFormatException =>
                logger.warn(
                  s"Skipping segment $segId: failed to parse partition ID from basePath: ${mc.basePath}"
                )
            }
          } else {
            logger.warn(
              s"Skipping segment $segId: basePath does not contain expected insert_log structure: ${mc.basePath}"
            )
          }
        case Left(e) =>
          logger.warn(
            s"Failed to parse manifest for segment $segId: ${e.getMessage}"
          )
      }
    }

    // V2 (packed-parquet) segments: AVRO gives us partition id directly; no
    // basePath — downstream dispatcher uses `v2SegmentIdSet` to pick the
    // V2-specific writer and construct per-field paths itself.
    for (seg <- v2Segments) {
      segmentToPartitionMap += (seg.segmentId -> seg.partitionId)
    }

    logger.info(
      s"Extracted from snapshot: collectionID=$collectionID, " +
        s"segments=${segmentToPartitionMap.keys.mkString(",")} " +
        s"(v3=${segmentBasePathMap.size}, v2=${v2Segments.size})"
    )

    (collectionID, segmentToPartitionMap, segmentBasePathMap)
  }

  /** Write backfill result JSON to the given output path (S3 or local). Uses
    * Spark's Hadoop FileSystem API for portability.
    *
    * Returns Right(()) on success and Left(BackfillError) on failure — callers
    * MUST check the result and propagate failure to the user. The previous
    * version swallowed exceptions, which caused silent successes (exit 0 with
    * success message printed but no result file written).
    *
    * The output path may live in a bucket whose credentials are not yet
    * configured on the Spark Hadoop conf, so we run
    * [[configureHadoopS3ForPath]] for it first (treated as a "main bucket" path
    * — same credentials as the Milvus storage bucket; override via --source-*
    * if you need a separate sink for results).
    */
  def writeResultJson(
      spark: SparkSession,
      result: BackfillResult,
      rawOutputPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Unit] = {
    // Same s3:// → s3a:// normalization as readSnapshotJson /
    // readBackfillData. Without it, an s3:// URL would route to the legacy
    // S3FileSystem which ignores fs.s3a.bucket.<b>.* config.
    val outputPath = normalizeS3Scheme(rawOutputPath)
    try {
      configureHadoopS3ForPath(spark, outputPath, config, isSource = false)
      val hadoopPath = new Path(outputPath)
      val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val out = fs.create(hadoopPath, true)
      try {
        out.write(
          result.toJson.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        )
      } finally {
        out.close()
      }
      logger.info(s"Backfill result JSON written to: $outputPath")
      Right(())
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to write result JSON to $outputPath: ${e.getMessage}",
          e
        )
        Left(
          WriteError(
            segmentId = -1,
            outputPath = outputPath,
            message = s"Failed to write result JSON: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** Normalize an `s3://` URL to `s3a://`. The legacy `s3://` scheme maps to
    * Hadoop's S3FileSystem (or, on 3.4.x, an alias that does NOT honor
    * `fs.s3a.bucket.<b>.*` per-bucket config), so per-bucket credentials we
    * write would be silently ignored. All read/write code paths in this object
    * route through this helper before touching Hadoop FS APIs.
    */
  private[backfill] def normalizeS3Scheme(path: String): String = {
    if (path == null) null
    else if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else path
  }

  /** Configure Hadoop S3A credentials for the bucket referenced by `path`.
    *
    * Uses per-bucket keys (`fs.s3a.bucket.<bucket>.*`) so that the backfill
    * *source* bucket and the Milvus storage bucket (snapshot / segments) can
    * each use their own endpoint and credentials within the same Spark session.
    * No-op for non-S3 paths.
    *
    * @param isSource
    *   true when configuring the backfill input bucket — in that case we
    *   consult the `sourceS3*` overrides and fall back to the main credentials
    *   when a particular field is unset.
    */
  private[backfill] def configureHadoopS3ForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    if (path == null) return
    if (!(path.startsWith("s3://") || path.startsWith("s3a://"))) return

    // Extract bucket name from s3(a)://bucket/key
    val withoutScheme = path.stripPrefix("s3a://").stripPrefix("s3://")
    val bucket = {
      val slash = withoutScheme.indexOf('/')
      if (slash < 0) withoutScheme else withoutScheme.substring(0, slash)
    }
    if (bucket.isEmpty) return

    // Resolve the effective credentials for this bucket. For the source bucket
    // any unset override falls back to the main credentials, preserving the
    // existing single-bucket behavior.
    val endpoint =
      if (isSource) config.sourceS3Endpoint.getOrElse(config.s3Endpoint)
      else config.s3Endpoint
    val accessKey =
      if (isSource) config.sourceS3AccessKey.getOrElse(config.s3AccessKey)
      else config.s3AccessKey
    val secretKey =
      if (isSource) config.sourceS3SecretKey.getOrElse(config.s3SecretKey)
      else config.s3SecretKey
    val useSSL =
      if (isSource) config.sourceS3UseSSL.getOrElse(config.s3UseSSL)
      else config.s3UseSSL
    val useIam =
      if (isSource) config.sourceS3UseIam.getOrElse(config.s3UseIam)
      else config.s3UseIam
    val region =
      if (isSource) config.sourceS3Region.getOrElse(config.s3Region)
      else config.s3Region

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val prefix = s"fs.s3a.bucket.$bucket"

    // Endpoint + path style + SSL are safe to set in both IAM and static modes
    if (endpoint != null && endpoint.nonEmpty) {
      hadoopConf.set(s"$prefix.endpoint", endpoint)
    }
    if (region != null && region.nonEmpty) {
      // Newer hadoop-aws (3.3.x+) reads endpoint.region; set both keys for
      // compatibility with older versions that only honor `region`.
      hadoopConf.set(s"$prefix.endpoint.region", region)
      hadoopConf.set(s"$prefix.region", region)
    }
    hadoopConf.set(s"$prefix.path.style.access", "true")
    hadoopConf.set(
      s"$prefix.connection.ssl.enabled",
      if (useSSL) "true" else "false"
    )

    if (useIam) {
      // Build an explicit IRSA/EKS-friendly provider chain instead of the
      // v1 DefaultAWSCredentialsProviderChain, which has historically been
      // unreliable on EKS pods (it does not always pick up the projected
      // service-account web-identity token before falling back to the
      // EC2 instance profile of the node — leaking the node's role).
      //
      // Order matters:
      //   1. WebIdentityTokenCredentialsProvider — IRSA / GKE Workload Identity
      //   2. EnvironmentVariableCredentialsProvider — local dev / CI overrides
      //   3. IAMInstanceCredentialsProvider — EC2 / EKS node role fallback
      hadoopConf.set(
        s"$prefix.aws.credentials.provider",
        Seq(
          "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
          "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
          "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
        ).mkString(",")
      )
    } else {
      hadoopConf.set(s"$prefix.access.key", accessKey)
      hadoopConf.set(s"$prefix.secret.key", secretKey)
      // Force the simple static-credentials provider for this bucket so it
      // doesn't get shadowed by a globally configured provider chain.
      hadoopConf.set(
        s"$prefix.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }

    logger.info(
      s"Configured Hadoop S3A for bucket '$bucket' " +
        s"(endpoint=$endpoint, useIam=$useIam, isSource=$isSource)"
    )
  }

  /** Read snapshot JSON content from S3 or local file system. Returns the JSON
    * string.
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
      if (
        snapshotPath.startsWith("s3://") || snapshotPath.startsWith("s3a://")
      ) {

        // Construct full S3 path (ensure s3a:// scheme for Hadoop)
        val s3Path = if (snapshotPath.startsWith("s3://")) {
          normalizeS3Scheme(snapshotPath)
        } else {
          snapshotPath
        }

        // Configure S3 settings on Spark's Hadoop Configuration (per-bucket
        // so that snapshot bucket and backfill source bucket can use
        // different credentials in the same Spark session).
        configureHadoopS3ForPath(spark, s3Path, config, isSource = false)

        // Use Spark's DataFrame API to read the file (avoids Hadoop version issues)
        val df = spark.read.text(s3Path)
        val json = df.collect().map(_.getString(0)).mkString("\n")

        Right(json)

      } else {
        // Local file path, read directly
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
        Left(
          DataReadError(
            snapshotPath,
            s"Failed to read snapshot file: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }

}
