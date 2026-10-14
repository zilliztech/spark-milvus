package com.zilliz.spark.connector.apps.backfill

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.json.{
  CollectionSchemaJson,
  FieldJson,
  ManifestContentJson,
  SegmentListJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.snapshot.Segment
import com.zilliz.spark.connector.apps.backfill.BackfillSegmentWrite.{
  usedBfCol,
  usedSnapshotCol,
  MatchFlagCol,
  RowOffsetCol,
  SegmentIdCol
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.table.SnapshotSparkSchema
import io.milvus.grpc.schema.{CollectionSchema, DataType => MilvusDataType}

/** Backfill operation for Milvus collections
  *
  * This object provides functionality to backfill new fields into existing
  * Milvus collections by reading snapshot rows, joining with new field data,
  * and writing per-segment binlog files.
  */
object MilvusBackfill {

  /** Partition ID stamped on the top-level result JSON.
    *
    * Milvus DataCoord (`CommitBackfillResult`) validates every segment's
    * partition against a positive value and skips the check otherwise. Builds
    * before milvus-io/milvus#53330 skip the check only for `0`, so `0` is the
    * one value that means "no partition restriction" on every build. A backfill
    * that spans more than one partition must therefore report `0`. The previous
    * `-1` sentinel was compared against partition -1 by those builds, which
    * rejected every segment ("no backfill segments passed pre-validation").
    */
  /** Commits the job manifest of the V3 segments this run wrote into and
    * returns the staging prefix, or "" when there was none to commit.
    */
  private def writeJobManifest(
      config: BackfillConfig,
      segmentResults: Map[Long, SegmentBackfillResult]
  ): String = {
    import com.zilliz.milvus.storage.write.commit.{
      CommittedSegment,
      JobDescriptor,
      JobOwner,
      JobWriteMode
    }

    val segments = segmentResults.values.toSeq
      .filter(_.committedVersion > 0L)
      .sortBy(_.segmentId)
      .map { r =>
        CommittedSegment(
          partitionId = 0,
          basePath = r.outputPath,
          manifestVersion = r.committedVersion,
          rowCount = r.rowCount,
          segmentId = Some(r.segmentId)
        )
      }
    if (segments.isEmpty) return ""
    val descriptor = Option(config.collectionName)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { collection =>
        val database = Option(config.databaseName)
          .map(_.trim)
          .filter(_.nonEmpty)
          .getOrElse("default")
        JobDescriptor(JobOwner(database, collection), JobWriteMode.Backfill)
      }
    val store = BackfillStorage.stagingStore(config)
    try
      commitJobManifest(
        store,
        stagingLayoutFor(config),
        segments,
        descriptor
      )
    finally store.close()
  }

  /** The job id of one run: the caller's, else a new one. Two runs never share
    * an id by default, even in one Spark application (decided 2026-09-16);
    * `run` resolves it once and keeps it.
    */
  private[backfill] def jobIdFor(config: BackfillConfig): String =
    config.jobId.getOrElse(java.util.UUID.randomUUID().toString)

  private def stagingLayoutFor(
      config: BackfillConfig
  ): com.zilliz.milvus.storage.write.exec.StagingLayout =
    com.zilliz.milvus.storage.write.exec.StagingLayout(
      config.stagingRoot.getOrElse(config.s3RootPath),
      config.jobId.getOrElse(
        throw new IllegalStateException(
          "the job id is resolved at the start of run"
        )
      )
    )

  /** Refuses a job id that already has a committed manifest, before a single
    * segment is written: a caller that names an id owns it, and reusing one is
    * an error, not a rerun (decided 2026-09-16).
    */
  private[backfill] def ensureJobIdUnused(
      store: com.zilliz.milvus.storage.io.ObjectStore,
      layout: com.zilliz.milvus.storage.write.exec.StagingLayout
  ): Unit =
    if (
      new com.zilliz.milvus.storage.write.commit.Committer(
        store,
        layout
      ).isCommitted
    ) {
      throw new IllegalStateException(
        s"job '${layout.jobId}' is already committed at ${layout.prefix}; " +
          "a backfill job id cannot be reused, give this run a new one"
      )
    }

  /** Commits this run's manifest. A manifest found already committed under the
    * id can only belong to another run (the id was checked before writing), so
    * it is an error; returning its prefix would hand the other run's versions
    * to `register`.
    */
  private[backfill] def commitJobManifest(
      store: com.zilliz.milvus.storage.io.ObjectStore,
      layout: com.zilliz.milvus.storage.write.exec.StagingLayout,
      segments: Seq[com.zilliz.milvus.storage.write.commit.CommittedSegment],
      descriptor: Option[
        com.zilliz.milvus.storage.write.commit.JobDescriptor
      ] = None
  ): String = {
    import com.zilliz.milvus.storage.write.commit.{CommitOutcome, Committer}
    new Committer(store, layout)
      .commit(segments, descriptor = descriptor) match {
      case CommitOutcome.Committed =>
        logger.info(
          s"Job manifest of ${segments.size} segment(s) committed at ${layout.manifest}"
        )
        layout.prefix
      case CommitOutcome.AlreadyCommitted =>
        throw new IllegalStateException(
          s"job '${layout.jobId}' was committed by another run at ${layout.prefix} " +
            "while this run was writing; its manifest versions were not recorded"
        )
    }
  }

  private[backfill] def resolveResultPartitionId(
      partitionIDs: Set[Long]
  ): Long =
    if (partitionIDs.size == 1) partitionIDs.head else 0L

  private val logger = LoggerFactory.getLogger(getClass)

  private[backfill] final case class BackfillSource(
      dataFrame: DataFrame,
      checkpointRDD: Option[RDD[Row]]
  )

  private[backfill] val ApplyDeletesToSnapshotRows = false

  private val SupportedPhysicalJoinMilvusTypes: Set[Int] = Set(
    MilvusDataType.Int8.value,
    MilvusDataType.Int16.value,
    MilvusDataType.Int32.value,
    MilvusDataType.Int64.value,
    MilvusDataType.String.value,
    MilvusDataType.VarChar.value
  )

  private val SystemFieldIdsByName: Map[String, Long] = Map(
    "RowID" -> SchemaMapper.RowIdFieldId,
    "Timestamp" -> SchemaMapper.TimestampFieldId
  )

  private val ReservedJoinKeyFieldNames: Set[String] = Set(
    MilvusOption.MilvusExtraColumnSegmentIDAlias,
    MilvusOption.MilvusExtraColumnRowOffsetAlias,
    MilvusOption.MilvusExtraColumnSegmentID,
    MilvusOption.MilvusExtraColumnRowOffset
  )

  /** Resolve the public join-key specification against an exact snapshot schema
    * field. Physical keys deliberately support only stable scalar equality
    * types and never rely on Spark's case-insensitive resolver.
    */
  private[backfill] def resolveJoinKey(
      schema: CollectionSchemaJson,
      spec: BackfillJoinKey
  ): Either[BackfillError, ResolvedJoinKey] = {
    val normalizedSpec = spec match {
      case BackfillJoinKey.PhysicalField(name) =>
        BackfillJoinKey.PhysicalField(Option(name).map(_.trim).getOrElse(""))
      case BackfillJoinKey.PrimaryKey => BackfillJoinKey.PrimaryKey
    }

    val selected = normalizedSpec match {
      case BackfillJoinKey.PrimaryKey =>
        schema.fields
          .find(_.isPrimaryKey.contains(true))
          .toRight(
            SchemaValidationError(
              "No primary key field found in snapshot schema"
            )
          )
      case BackfillJoinKey.PhysicalField(name) if name.isEmpty =>
        Left(
          SchemaValidationError("Physical join-key field name cannot be blank")
        )
      case BackfillJoinKey.PhysicalField(name) =>
        schema.fields
          .find(_.name == name)
          .toRight(
            SchemaValidationError(
              s"Physical join-key field '$name' was not found in snapshot schema. " +
                s"Available fields: ${schema.fields.map(_.name).mkString(", ")}"
            )
          )
    }

    selected.flatMap { field =>
      normalizedSpec match {
        case _ if ReservedJoinKeyFieldNames.contains(field.name) =>
          Left(
            SchemaValidationError(
              "Snapshot field '" + field.name + "' cannot be used as the backfill join key because " +
                "its name is reserved for required metadata columns '" +
                MilvusOption.MilvusExtraColumnSegmentID + "' and '" +
                MilvusOption.MilvusExtraColumnRowOffset + "'. " +
                "Choose another persisted field with --join-key."
            )
          )
        case _: BackfillJoinKey.PhysicalField
            if !SupportedPhysicalJoinMilvusTypes.contains(field.dataType) =>
          Left(
            SchemaValidationError(
              s"Physical join-key field '${field.name}' has unsupported type " +
                s"(Milvus type ${field.dataType}). Supported types are Int8, Int16, Int32, " +
                "Int64, String, and VarChar."
            )
          )
        case _: BackfillJoinKey.PhysicalField
            if field.nullable.contains(true) =>
          Left(
            SchemaValidationError(
              s"Physical join-key field '${field.name}' is nullable. " +
                "Physical join keys must be non-nullable because Spark equality joins do not match NULL values."
            )
          )
        case _ =>
          val sparkFieldResult =
            try {
              Right(SnapshotSparkSchema.fieldToStructField(field))
            } catch {
              case e: Exception =>
                Left(
                  SchemaValidationError(
                    s"Failed to resolve join-key field '${field.name}': ${e.getMessage}",
                    Some(e)
                  )
                )
            }

          sparkFieldResult.map { sparkField =>
            normalizedSpec match {
              case BackfillJoinKey.PrimaryKey =>
                ResolvedJoinKey.primaryKey(
                  field.name,
                  field.getFieldIDAsLong,
                  Some(sparkField)
                )
              case _: BackfillJoinKey.PhysicalField =>
                ResolvedJoinKey.physicalField(
                  field.name,
                  field.getFieldIDAsLong,
                  sparkField
                )
            }
          }
      }
    }
  }

  /** Resolve requested writer targets from the snapshot schema and reject
    * fields whose values are owned by Milvus rather than by backfill output.
    * This shared boundary protects both primary-key and physical-key mapping
    * paths before source reads or writes begin.
    */
  private[backfill] def resolveBackfillTargetFields(
      schema: CollectionSchemaJson,
      targetFieldNames: Seq[String]
  ): Either[BackfillError, Map[String, FieldJson]] = {
    val fieldsByName = schema.fields.map(field => field.name -> field).toMap
    val missing = targetFieldNames.filterNot(fieldsByName.contains)
    if (missing.nonEmpty) {
      Left(
        SchemaValidationError(
          s"Fields not found in snapshot schema: ${missing.mkString(", ")}. " +
            s"Available fields: ${fieldsByName.keys.mkString(", ")}"
        )
      )
    } else {
      val resolved = targetFieldNames.map(name => name -> fieldsByName(name))
      val unsupported = resolved.flatMap { case (name, field) =>
        val isSystemField =
          SystemFieldIdsByName.get(name).contains(field.getFieldIDAsLong)
        Seq(
          if (field.isPrimaryKey.contains(true)) Some("primary key") else None,
          if (field.isPartitionKey.contains(true)) Some("partition key")
          else None,
          if (field.isDynamic.contains(true)) Some("dynamic field") else None,
          if (field.isFunctionOutput.contains(true)) Some("function output")
          else None,
          if (isSystemField) Some("system field") else None
        ).flatten.map(role => s"'$name' ($role)")
      }

      if (unsupported.nonEmpty) {
        Left(
          SchemaValidationError(
            "Backfill targets must be ordinary writable collection fields; " +
              s"unsupported targets: ${unsupported.mkString(", ")}"
          )
        )
      } else {
        Right(resolved.toMap)
      }
    }
  }

  /** Suffix appended to backfill-side target columns while both sides of a
    * target field are live on the joined frame (coalesce + overwrite).
    */
  private[backfill] val BackfillSideSuffix = "__bf__"

  /** Every column name the join/merge pipeline synthesizes on the joined frame
    * for the given targets and resolved join key. Kept in one place so the
    * pre-read reservation check below cannot drift from the generators.
    */
  private[backfill] def reservedInternalColumnNames(
      newFieldNames: Seq[String],
      joinKey: ResolvedJoinKey
  ): Seq[String] =
    (Seq(MatchFlagCol) ++ joinKey.internalColumns ++ newFieldNames.flatMap {
      n => Seq(usedSnapshotCol(n), usedBfCol(n), n + BackfillSideSuffix)
    }).distinct

  /** Reject target names that resolve (under Spark's configured resolver,
    * case-insensitive by default) to any internal column generated later. A
    * colliding target would either be silently overwritten by marker data or
    * make the merge projection ambiguous, so fail before any source read.
    */
  private[backfill] def validateTargetNamesAgainstInternalColumns(
      newFieldNames: Seq[String],
      joinKey: ResolvedJoinKey,
      resolver: (String, String) => Boolean
  ): Either[BackfillError, Unit] = {
    val reserved = reservedInternalColumnNames(newFieldNames, joinKey)
    val collisions = newFieldNames.flatMap { name =>
      reserved.collect {
        case internal if resolver(name, internal) =>
          s"'$name' (reserved as '$internal')"
      }
    }
    if (collisions.nonEmpty) {
      Left(
        SchemaValidationError(
          "Backfill target columns collide with names reserved for internal " +
            s"use by the backfill join: ${collisions.mkString(", ")}. " +
            "Rename the column (or use --column-mapping) and retry."
        )
      )
    } else {
      Right(())
    }
  }

  /** Backfill new fields into a Milvus collection
    *
    * @param spark
    *   SparkSession
    * @param backfillDataPath
    *   Path to Parquet file containing the configured join key and new field
    *   data
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
      initialConfig: BackfillConfig
  ): Either[BackfillError, BackfillResult] = {

    val startTime = System.currentTimeMillis()
    val config =
      try {
        initialConfig.withHadoopStorageAssumeRole(
          spark.sparkContext.hadoopConfiguration,
          spark.sparkContext.applicationId
        )
      } catch {
        case e: IllegalArgumentException =>
          return Left(
            SchemaValidationError(
              s"Invalid Hadoop storage AssumeRole configuration: ${e.getMessage}"
            )
          )
      }

    // Validate S3/writer configuration (always required)
    config.validate() match {
      case Left(error) =>
        return Left(SchemaValidationError(s"Invalid configuration: $error"))
      case Right(_) => // Continue
    }

    if (Option(snapshotPath).forall(_.trim.isEmpty)) {
      return Left(
        SchemaValidationError(
          "ADDFIELD backfill requires collection schema from a snapshot"
        )
      )
    }

    // One job id for the whole run.
    val runConfig = config.copy(jobId = Some(jobIdFor(config)))

    // OSS source reads use a local-checkpointed RDD to sever the external
    // storage lineage while scoped credentials are installed. Keep the actual
    // persisted RDD so its blocks can be released after the transformed
    // backfill DataFrame is materialized, including on failure.
    var sourceCheckpointRDD: Option[RDD[Row]] = None
    var cachedBackfillDF: DataFrame = null

    logger.info(s"Backfill mode: ${config.mode}")

    val snapshotMetadata = loadSnapshotMetadata(snapshotPath, config) match {
      case Left(error)     => return Left(error)
      case Right(metadata) => metadata
    }

    // Materialize V2 segments once so the read and write paths share the
    // snapshot's Avro manifests and parquet-footer field mapping.
    val v2Segments = loadV2Segments(snapshotMetadata, config) match {
      case Left(error)     => return Left(error)
      case Right(segments) => segments
    }

    try {
      val baseJoinKey =
        resolveJoinKey(
          snapshotMetadata.collection.schema,
          config.joinKey
        ) match {
          case Left(error) => return Left(error)
          case Right(key)  => key
        }
      logger.info(
        s"Backfill join key: kind=${baseJoinKey.kind}, " +
          s"fields=${baseJoinKey.fieldNames.mkString(",")}, " +
          s"fieldIds=${baseJoinKey.fieldIds.mkString(",")}"
      )

      // Read backfill data from Parquet
      val rawBackfillDF =
        readBackfillData(spark, backfillDataPath, config) match {
          case Left(error) => return Left(error)
          case Right(source) =>
            sourceCheckpointRDD = source.checkpointRDD
            source.dataFrame
        }

      // Reproject via the selected join-key mapping contract, then separate
      // the join component from fields that will be written. The join column is
      // normalized to an internal alias so downstream execution no longer
      // depends on the collection field name.
      val preparedBackfill = prepareBackfillData(
        rawBackfillDF,
        baseJoinKey,
        config.joinKey,
        config.columnMapping,
        snapshotMetadata.collection.schema.fields.map(_.name)
      ) match {
        case Left(error) => return Left(error)
        case Right(data) => data
      }
      val mappedBackfillDF = preparedBackfill.dataFrame
      val joinKey = preparedBackfill.joinKey

      val newFieldNames = preparedBackfill.targetFieldNames

      // Reserve every internal column the join/merge step will synthesize
      // (match flag, join aliases, per-field provenance flags, backfill-side
      // suffix) under Spark's configured resolver. A colliding target would be
      // silently overwritten by marker data (overwrite mode) or blow up with
      // AnalysisException on the coalesce rename/self-reference.
      validateTargetNamesAgainstInternalColumns(
        newFieldNames,
        joinKey,
        spark.sessionState.conf.resolver
      ) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      val targetFieldsByName = resolveBackfillTargetFields(
        snapshotMetadata.collection.schema,
        newFieldNames
      ) match {
        case Left(error)  => return Left(error)
        case Right(value) => value
      }
      // The writer takes field ids and dimensions from the collection schema.
      val collectionSchema = CollectionSchema.parseFrom(
        snapshotMetadata.collection.schema.toProtobufBytes
      )

      // User parquet uses normal ingestion-friendly vector shapes (numeric
      // arrays, byte arrays, sparse maps/structs/JSON). Encode vector
      // fields to Milvus's physical per-row bytes before caching or joining so
      // Spark never widens or rewrites half/int8/sparse representations.
      val backfillDF = BackfillVectorColumns.encode(
        mappedBackfillDF,
        targetFieldsByName
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      val targetVectorFields = targetFieldsByName.collect {
        case (name, field) if BackfillVectorColumns.isVectorField(field) =>
          name -> BackfillVectorColumns.canonicalStructField(field)
      }

      val targetWriteFields = newFieldNames.map { n =>
        val field = targetFieldsByName(n)
        val structField = targetVectorFields.getOrElse(
          n,
          SnapshotSparkSchema.fieldToStructField(field)
        )
        (n, field.getFieldIDAsLong, structField)
      }

      // Every mode writes parquet-side values with their current Spark types.
      // Replace has no implicit cast, so validate before any writer runs.
      validateBackfillTargetFieldTypes(
        backfillDF.schema,
        targetWriteFields,
        config.mode
      ) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      // Non-OSS sources retain their recoverable external-storage lineage, so
      // cache the transformed DataFrame to avoid a second scan. OSS sources
      // instead retain the single local-checkpoint RDD until every consumer
      // completes; caching this derived DataFrame would hold two full copies.
      if (sourceCheckpointRDD.isEmpty) {
        backfillDF.cache()
        cachedBackfillDF = backfillDF
      }

      val joinKeyStats = validateJoinKeyCardinality(
        backfillDF,
        joinKey.internalColumns,
        joinKey.fieldNames,
        side = "Backfill parquet"
      ) match {
        case Left(error)  => return Left(error)
        case Right(stats) => stats
      }
      val backfillRowCount = joinKeyStats.rowCount
      logger.info(
        s"Backfill data file rows: $backfillRowCount " +
          s"(distinct join keys: ${joinKeyStats.distinctValidKeyCount})"
      )

      // In coalesce / overwrite modes, also read each target field from the snapshot
      // so the merge step (coalesce(snapshot,bf) or when(matched, bf).otherwise(snapshot))
      // can compare snapshot and parquet values per row. Requires a snapshot —
      // we need the field's Spark type.
      val readsSnapshotFields = config.readsSnapshotFields
      val extraReadFields
          : Seq[(String, Long, org.apache.spark.sql.types.StructField)] =
        if (readsSnapshotFields) targetWriteFields else Seq.empty

      // Read snapshot rows with segment metadata
      val snapshotDF = readCollectionWithMetadata(
        spark,
        config,
        joinKey,
        snapshotMetadata,
        v2Segments,
        extraReadFields
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      // This check only inspects schemas. Run it before any snapshot-side action
      // so a mismatched parquet key fails without scanning the collection.
      validateJoinKeyCompatibility(
        snapshotDF,
        backfillDF,
        joinKey
      ) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      // Merge snapshot and backfill DataFrames according to mode.
      val joinedDF = performJoin(
        snapshotDF,
        backfillDF,
        joinKey.internalColumns,
        newFieldNames,
        config.mode
      )

      val (collectionID, segmentToPartitionMap, segmentBasePathMap) =
        extractMetadataFromSnapshot(
          snapshotMetadata,
          v2Segments,
          config.s3BucketName,
          config.s3Endpoint
        )

      // Set of segment IDs that are StorageV2 (packed-parquet, no manifest).
      // V3 segments are written through MilvusV3WriterFactory.
      val v2SegmentIdSet: Set[Long] = v2Segments.map(_.id).toSet

      // Process each segment
      // The job id is checked before the first segment is written.
      try {
        val store = BackfillStorage.stagingStore(runConfig)
        try ensureJobIdUnused(store, stagingLayoutFor(runConfig))
        finally store.close()
      } catch {
        case e: Exception =>
          return Left(
            WriteError(
              segmentId = 0L,
              outputPath = stagingLayoutFor(runConfig).prefix,
              message = e.getMessage,
              cause = Some(e)
            )
          )
      }

      val segmentResults = BackfillSegmentWrite.write(
        spark,
        joinedDF,
        collectionID,
        segmentToPartitionMap,
        segmentBasePathMap,
        v2SegmentIdSet,
        runConfig,
        newFieldNames,
        collectionSchema,
        targetVectorFields
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
        partitionId = resolveResultPartitionId(partitionIDs),
        schemaVersion = snapshotMetadata.collection.schema.version,
        newFieldNames = newFieldNames,
        totalBackfillDataRows = backfillRowCount
      )

      // The job manifest: every V3 segment this run wrote into, with its id
      // and new manifest version, committed under the staging prefix for
      // `register` to hand to Milvus.
      val stagingPrefix = writeJobManifest(runConfig, segmentResults)

      Right(result.copy(stagingPrefix = stagingPrefix))

    } catch {
      case e: Exception =>
        logger.error("Backfill operation failed", e)
        Left(BackfillError.fromException(e))
    } finally {
      if (cachedBackfillDF != null) {
        try {
          cachedBackfillDF.unpersist()
        } catch {
          case e: Exception =>
            logger.warn("Failed to unpersist backfill DataFrame", e)
        }
      }
      sourceCheckpointRDD.foreach { checkpointRDD =>
        try {
          checkpointRDD.unpersist()
        } catch {
          case e: Exception =>
            logger.warn("Failed to unpersist source checkpoint RDD", e)
        }
      }
    }
  }

  /** Read the backfill input by dispatching on `config.inputFormat`.
    *
    * The guard is keyed to the same `ImplementedInputFormats` set that
    * `BackfillConfig.validate()` uses, so the two cannot drift: a format with a
    * reader is dispatched here, anything else fails with the same
    * not-implemented contract. `run()` rejects unimplemented formats earlier,
    * at validate() before any snapshot/S3 work.
    */
  private def readBackfillData(
      spark: SparkSession,
      rawPath: String,
      config: BackfillConfig
  ): Either[BackfillError, BackfillSource] = {
    val format = config.inputFormat.trim
    if (!BackfillConfig.ImplementedInputFormats.contains(format)) {
      return Left(
        DataReadError(
          path = rawPath,
          message = s"Input format '$format' is not implemented; only " +
            s"${BackfillConfig.ImplementedInputFormats.mkString("[", ", ", "]")} " +
            "have a reader"
        )
      )
    }
    format match {
      case "parquet" => readParquet(spark, rawPath, config)
    }
  }

  private def readParquet(
      spark: SparkSession,
      rawPath: String,
      config: BackfillConfig
  ): Either[BackfillError, BackfillSource] = {
    // Hadoop 3.4.1 has separate s3:// and s3a:// FileSystem implementations
    // and per-bucket fs.s3a.bucket.<b>.* config is only honored by
    // S3AFileSystem. Force the s3a scheme so the credentials we just wrote
    // actually take effect.
    val path = BackfillStorage.normalizeObjectStorageScheme(rawPath, config)
    try {
      // Configure the source bucket before reading the parquet. OSS must be
      // fully materialized while this temporary credential scope is active;
      // S3A uses persistent per-bucket configuration and does not need it.
      Right(
        BackfillStorage
          .withScopedHadoopStorage(spark, path, config, isSource = true) {
            // Materialize while source-bucket credentials are installed. Spark
            // evaluates DataFrame reads lazily, so returning an unmaterialized DF
            // would allow later main-bucket configuration to leak into this read.
            val df = spark.read.parquet(path)
            if (df.columns.isEmpty) {
              throw new IllegalArgumentException(
                "Backfill parquet is empty (no columns)"
              )
            }
            if (path.startsWith("oss://")) {
              localCheckpointBackfillData(spark, df)
            } else {
              BackfillSource(df, None)
            }
          }
      )
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

  private[backfill] def localCheckpointBackfillData(
      spark: SparkSession,
      dataFrame: DataFrame
  ): BackfillSource = {
    // Copy external Rows before checkpointing because Spark's physical plans
    // may reuse mutable row objects. The checkpoint severs the OSS lineage,
    // and retaining this exact RDD gives run() a reliable cleanup handle.
    val checkpointRDD = dataFrame.rdd.map(_.copy())
    checkpointRDD.localCheckpoint()
    try {
      checkpointRDD.count()
      BackfillSource(
        spark.createDataFrame(checkpointRDD, dataFrame.schema),
        Some(checkpointRDD)
      )
    } catch {
      case e: Exception =>
        try checkpointRDD.unpersist()
        catch {
          case cleanupError: Exception => e.addSuppressed(cleanupError)
        }
        throw e
    }
  }

  /** Resolve the default mapping for the selected join key, validate its
    * targets, and rename in one projection so chains and swaps stay intact.
    */
  private[backfill] def applyColumnMapping(
      df: DataFrame,
      joinFieldName: String,
      joinSpec: BackfillJoinKey,
      userMapping: Option[Map[String, String]]
  ): Either[BackfillError, DataFrame] = {
    val columns = df.columns.toSeq
    val columnSet = columns.toSet
    val primaryKey = joinSpec == BackfillJoinKey.PrimaryKey
    val mapping = userMapping.getOrElse {
      if (primaryKey) {
        if (!columnSet.contains("pk")) {
          return Left(
            SchemaValidationError(
              "Backfill parquet must contain a 'pk' column (or supply --column-mapping to rename the PK column)"
            )
          )
        }
        if (joinFieldName != "pk" && columnSet.contains(joinFieldName)) {
          return Left(
            SchemaValidationError(
              s"Backfill parquet contains both a 'pk' column and a column named '$joinFieldName' " +
                "(the collection's primary-key field). Remove one, or supply --column-mapping to disambiguate."
            )
          )
        }
        columns
          .map(name => name -> (if (name == "pk") joinFieldName else name))
          .toMap
      } else {
        if (!columnSet.contains(joinFieldName)) {
          return Left(
            SchemaValidationError(
              s"Backfill parquet must contain the physical join-key column " +
                s"'$joinFieldName' (or supply --column-mapping to rename the input key)"
            )
          )
        }
        columns.map(name => name -> name).toMap
      }
    }

    val missingSources = mapping.keySet.diff(columnSet)
    if (missingSources.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"column mapping references parquet columns that don't exist: " +
            s"${missingSources.mkString(", ")}. Available: ${columns.mkString(", ")}"
        )
      )
    }
    val duplicateTargets = mapping.values.groupBy(identity).collect {
      case (name, values) if values.size > 1 => name
    }
    if (duplicateTargets.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"column mapping has duplicate targets: ${duplicateTargets.mkString(", ")}"
        )
      )
    }
    val targets = mapping.values.toSet
    if (!targets.contains(joinFieldName)) {
      val key =
        if (primaryKey) "the primary key field" else "physical join-key field"
      return Left(
        SchemaValidationError(
          s"column mapping must include $key '$joinFieldName' as a target"
        )
      )
    }
    if ((targets - joinFieldName).isEmpty) {
      val target = if (primaryKey) "non-PK" else "non-join-key"
      return Left(
        SchemaValidationError(
          s"column mapping must include at least one $target field to backfill"
        )
      )
    }
    Right(
      df.select(
        columns
          .filter(mapping.contains)
          .map(name => df.col(name).as(mapping(name))): _*
      )
    )
  }

  private[backfill] def prepareBackfillData(
      df: DataFrame,
      joinKey: ResolvedJoinKey,
      joinSpec: BackfillJoinKey,
      userMapping: Option[Map[String, String]],
      collectionFieldNames: Seq[String]
  ): Either[BackfillError, PreparedBackfillData] = {
    val keyColumn = joinKey.fieldNames.head
    val mappedResult = applyColumnMapping(df, keyColumn, joinSpec, userMapping)

    mappedResult.flatMap { mapped =>
      val mappedColumns = mapped.columns.toSeq
      val missingKeys = joinKey.fieldNames.filterNot(mappedColumns.contains)
      if (missingKeys.nonEmpty) {
        Left(
          SchemaValidationError(
            s"Backfill data is missing join-key columns after column mapping: " +
              missingKeys.mkString(", ")
          )
        )
      } else {
        val caseSensitive = df.sparkSession.conf
          .get("spark.sql.caseSensitive", "false")
          .toBoolean
        val resolver: (String, String) => Boolean =
          if (caseSensitive) (left, right) => left == right
          else (left, right) => left.equalsIgnoreCase(right)
        val allocatedJoinKey = joinKey.withCollisionFreeInternalColumns(
          mappedColumns ++ collectionFieldNames,
          resolver
        )
        val keyAliases =
          allocatedJoinKey.components.map(c => c.fieldName -> c).toMap
        val normalized = mapped.select(
          mappedColumns.map { name =>
            keyAliases.get(name) match {
              case Some(component) =>
                mapped.col(name).as(component.internalColumn)
              case None => mapped.col(name)
            }
          }: _*
        )
        val targetFieldNames =
          mappedColumns.filterNot(allocatedJoinKey.fieldNames.contains)
        Right(
          PreparedBackfillData(
            dataFrame = normalized,
            joinKey = allocatedJoinKey,
            targetFieldNames = targetFieldNames
          )
        )
      }
    }
  }

  /** Build the ordered field-ID/schema projection used by snapshot reads.
    * Deduplication happens on the pair before either side is emitted so the
    * supplied Spark schema always stays aligned with `ReaderFieldIDs`.
    */
  private[backfill] def buildSnapshotReadProjection(
      joinKey: ResolvedJoinKey,
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ]
  ): Either[BackfillError, SnapshotReadProjection] = {
    val missingSchemas = joinKey.components.filter(_.sparkField.isEmpty)
    if (missingSchemas.nonEmpty) {
      Left(
        SchemaValidationError(
          s"Missing snapshot schema for join-key fields: " +
            missingSchemas.map(_.fieldName).mkString(", ")
        )
      )
    } else {
      val requested =
        joinKey.components.map(c => (c.fieldId, c.sparkField.get)) ++
          extraReadFields.map { case (_, fieldId, field) => (fieldId, field) }
      val deduped = requested
        .foldLeft(
          (
            Vector.empty[(Long, org.apache.spark.sql.types.StructField)],
            Set.empty[Long]
          )
        ) { case ((fields, seen), item @ (fieldId, _)) =>
          if (seen.contains(fieldId)) (fields, seen)
          else (fields :+ item, seen + fieldId)
        }
        ._1

      Right(
        SnapshotReadProjection(
          fieldIds = deduped.map(_._1),
          schema = org.apache.spark.sql.types.StructType(deduped.map(_._2))
        )
      )
    }
  }

  /** Normalize snapshot-side join columns in one projection. A sequential
    * `withColumnRenamed` chain can corrupt swaps or rename chains when a
    * snapshot field happens to use another component's internal name.
    */
  private[backfill] def normalizeSnapshotJoinColumns(
      df: DataFrame,
      joinKey: ResolvedJoinKey
  ): DataFrame = {
    val snapshotAliases =
      joinKey.components.map(c => c.fieldName -> c.internalColumn).toMap
    df.select(
      df.columns.toSeq.map { name =>
        snapshotAliases.get(name) match {
          case Some(alias) => df.col(name).as(alias)
          case None        => df.col(name)
        }
      }: _*
    )
  }

  /** Read collection data with _segment_id and _row_offset metadata. They are
    * used to match with the original sequence of rows for each segment
    *
    * @param joinKey
    *   Resolved snapshot fields used to match backfill input rows
    * @param snapshotMetadata
    *   Snapshot metadata that pins the collection schema and segments
    */
  private def readCollectionWithMetadata(
      spark: SparkSession,
      config: BackfillConfig,
      joinKey: ResolvedJoinKey,
      snapshotMetadata: SnapshotJson,
      v2Segments: Seq[Segment],
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ]
  ): Either[BackfillError, DataFrame] = {
    try {
      var options = config.getMilvusReadOptions
      // Backfill writes one value per physical snapshot row. Applying deletes
      // would remove rows and invalidate _row_offset-based field alignment.
      options = options + (MilvusOption.ReadApplyDeletes ->
        ApplyDeletesToSnapshotRows.toString)

      // Enable snapshot mode flag
      options = options + (MilvusOption.SnapshotMode -> "true")

      // Override connection options for snapshot mode (no client needed)
      options = options + ("milvus.uri" -> "dummy://snapshot-mode")
      options =
        options + ("milvus.collection.name" -> snapshotMetadata.collection.schema.name)

      // Add snapshot collection ID
      options =
        options + (MilvusOption.SnapshotCollectionId -> snapshotMetadata.snapshotInfo.collectionId.toString)

      // Add snapshot partition IDs
      options =
        options + (MilvusOption.SnapshotPartitionIds -> snapshotMetadata.snapshotInfo.partitionIds
          .mkString(","))

      // Convert snapshot schema to protobuf bytes and pass as Base64
      val schemaBytes =
        snapshotMetadata.collection.schema.toProtobufBytes
      val schemaBytesBase64 =
        java.util.Base64.getEncoder.encodeToString(schemaBytes)
      options =
        options + (MilvusOption.SnapshotSchemaBytes -> schemaBytesBase64)

      snapshotMetadata.storageV2ManifestList.foreach { manifestList =>
        // Pass original manifest JSON (containing both ver and base_path) so that
        // the DataSource can extract readVersion and lock reads to snapshot version
        if (manifestList.nonEmpty) {
          val manifestJson =
            SegmentListJson.encodeManifestItems(manifestList)
          options = options + (MilvusOption.SnapshotManifests -> manifestJson)
        } else {
          logger.warn("No valid manifests found in snapshot")
        }
      }

      // Pre-loaded StorageV2 (non-manifest packed parquet) segments — hand
      // them to the DataSource via SnapshotV2Segments so planner can emit
      // MilvusInputPartitions. Loading itself happened earlier in
      // `run()` via `loadV2Segments`.
      if (v2Segments.nonEmpty) {
        val segJson = SegmentListJson.encodeV2Segments(v2Segments)
        options = options + (MilvusOption.SnapshotV2Segments -> segJson)
        logger.info(
          s"Attached ${v2Segments.size} StorageV2 packed segment(s) to read options"
        )
      }

      // The supplied schema and ReaderFieldIDs must stay in the same order.
      // The component sequence also supports future logical/composite key
      // resolvers.
      val projection =
        buildSnapshotReadProjection(joinKey, extraReadFields) match {
          case Left(error)  => return Left(error)
          case Right(value) => value
        }
      options = options + (MilvusOption.ReaderFieldIDs -> projection.fieldIds
        .mkString(","))

      logger.info(
        s"Reading with schema: ${projection.schema.fieldNames.mkString(", ")}"
      )

      val df = spark.read
        .schema(projection.schema)
        .format("com.zilliz.spark.connector.sources.MilvusDataSource")
        .options(options)
        .load()

      val missingSnapshotJoinColumns =
        joinKey.fieldNames.filterNot(df.columns.contains)
      if (missingSnapshotJoinColumns.nonEmpty) {
        return Left(
          ConnectionError(
            message = s"Failed to read collection join-key columns: " +
              missingSnapshotJoinColumns.mkString(", ")
          )
        )
      }

      if (
        !df.columns.contains(SegmentIdCol) || !df.columns.contains(RowOffsetCol)
      ) {
        return Left(
          ConnectionError(
            message =
              s"Failed to read collection data with $SegmentIdCol and $RowOffsetCol. " +
                "Ensure milvus.extra.columns is set correctly."
          )
        )
      }

      Right(normalizeSnapshotJoinColumns(df, joinKey))
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

  /** Every backfill mode requires parquet target-column types to match the
    * snapshot field types exactly. Replace writes parquet columns verbatim;
    * coalesce and overwrite additionally synthesize a per-row choice between
    * snapshot and parquet values. Allowing either an input mismatch or Spark
    * widening would make the writer emit binlogs whose Arrow type no longer
    * matches the Milvus field.
    */
  private[backfill] def validateBackfillTargetFieldTypes(
      backfillSchema: org.apache.spark.sql.types.StructType,
      targetFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ],
      mode: String
  ): Either[BackfillError, Unit] = {
    val backfillTypes =
      backfillSchema.fields.map(f => f.name -> f.dataType).toMap
    val mismatches = targetFields.collect {
      case (name, _, snapshotField)
          if backfillTypes
            .get(name)
            .exists(_ != snapshotField.dataType) =>
        s"$name (snapshot=${snapshotField.dataType.simpleString}, " +
          s"parquet=${backfillTypes(name).simpleString})"
    }
    if (mismatches.nonEmpty) {
      Left(
        SchemaValidationError(
          s"--mode=$mode requires backfill parquet target-column types to match " +
            s"snapshot field types exactly; cast them in the input ETL before running backfill. " +
            s"Mismatched: ${mismatches.mkString(", ")}"
        )
      )
    } else {
      Right(())
    }
  }

  /** Validate that backfill input contains a non-null, unique join key.
    *
    * A duplicate on the backfill side would fan one snapshot row out into
    * multiple joined rows and corrupt per-segment row counts. Nulls are
    * reported separately because Spark equi-joins never match null keys.
    */
  private[backfill] def validateJoinKeyCardinality(
      df: DataFrame,
      joinColumns: Seq[String],
      displayColumns: Seq[String],
      side: String
  ): Either[BackfillError, JoinKeyStats] = {
    if (joinColumns.isEmpty) {
      return Left(SchemaValidationError("Join key must not be empty"))
    }

    val missing = joinColumns.filterNot(df.columns.contains)
    if (missing.nonEmpty) {
      return Left(
        SchemaValidationError(
          s"$side is missing internal join-key columns: ${missing.mkString(", ")}"
        )
      )
    }

    try {
      val hasNull = joinColumns.map(name => col(name).isNull).reduce(_ || _)
      val keyValue = struct(joinColumns.map(col): _*)
      val counts = df
        .agg(
          count(lit(1)).as("__bf_key_rows__"),
          coalesce(
            sum(when(hasNull, lit(1L)).otherwise(lit(0L))),
            lit(0L)
          ).as("__bf_null_key_rows__"),
          countDistinct(when(!hasNull, keyValue)).as(
            "__bf_distinct_valid_keys__"
          )
        )
        .head()

      val stats = JoinKeyStats(
        rowCount = counts.getAs[Long]("__bf_key_rows__"),
        nullKeyRowCount = counts.getAs[Long]("__bf_null_key_rows__"),
        distinctValidKeyCount = counts.getAs[Long](
          "__bf_distinct_valid_keys__"
        )
      )
      val display = displayColumns.mkString("(", ", ", ")")

      if (stats.nullKeyRowCount > 0) {
        Left(
          SchemaValidationError(
            s"$side contains ${stats.nullKeyRowCount} row(s) with null join key " +
              s"$display. Join keys must be non-null."
          )
        )
      } else if (stats.distinctValidKeyCount != stats.rowCount) {
        Left(
          SchemaValidationError(
            s"$side contains duplicate join-key values " +
              s"(columns=$display, rows=${stats.rowCount}, " +
              s"distinct=${stats.distinctValidKeyCount}). " +
              "Deduplicate the input on the join key and retry."
          )
        )
      } else {
        Right(stats)
      }
    } catch {
      case e: Exception =>
        Left(
          SchemaValidationError(
            s"Failed to validate $side join key: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }

  /** Validate join-key schema compatibility between snapshot and backfill data.
    */
  private[backfill] def validateJoinKeyCompatibility(
      snapshotDF: DataFrame,
      backfillDF: DataFrame,
      joinKey: ResolvedJoinKey
  ): Either[BackfillError, Unit] = {
    try {
      joinKey.components.foreach { component =>
        val snapshotField = snapshotDF.schema.fields
          .find(_.name == component.internalColumn)
          .getOrElse {
            return Left(
              SchemaValidationError(
                s"Original collection data must have join-key field " +
                  s"'${component.fieldName}'"
              )
            )
          }

        val backfillField = backfillDF.schema.fields
          .find(_.name == component.internalColumn)
          .getOrElse {
            return Left(
              SchemaValidationError(
                s"Backfill data must have join-key field " +
                  s"'${component.fieldName}' after column mapping"
              )
            )
          }

        if (snapshotField.dataType != backfillField.dataType) {
          return Left(
            SchemaValidationError(
              s"Join-key type mismatch for '${component.fieldName}': " +
                s"original=${snapshotField.dataType}, new=${backfillField.dataType}"
            )
          )
        }
      }

      Right(())

    } catch {
      case e: Exception =>
        logger.error("Failed to validate join-key schema compatibility", e)
        Left(
          SchemaValidationError(
            s"Failed to validate join-key schema compatibility: ${e.getMessage}"
          )
        )
    }
  }

  /** Merge snapshot rows with backfill rows per `mode`.
    *
    *   - replace: left join on the resolved key; backfill value replaces the
    *     snapshot value (the snapshot only contributes join + segment tracking
    *     columns). Unmatched snapshot rows end up with null target columns.
    *   - coalesce: the snapshot carries the target fields; after the left join,
    *     compute `coalesce(snapshot, backfill)` per field (snapshot wins when
    *     non-null, otherwise use backfill). Unmatched snapshot rows keep their
    *     original target values.
    *   - overwrite: the snapshot carries the target fields; after the left
    *     join, compute `when(matched, backfill).otherwise(snapshot)` per field
    *     (file wins when the join key matched, even if the file value is null).
    *     Unmatched snapshot rows keep their existing target values.
    */
  private[backfill] def performJoin(
      snapshotDF: DataFrame,
      backfillDF: DataFrame,
      joinColumns: Seq[String],
      newFieldNames: Seq[String],
      mode: String
  ): DataFrame = {
    val backfillWithFlag =
      backfillDF.withColumn(MatchFlagCol, lit(true))
    mode match {
      case MilvusOption.BackfillModeCoalesce =>
        // Rename backfill-side target columns to avoid name collisions with
        // snapshot-side columns now present on snapshotDF.
        val suffix = BackfillSideSuffix
        val renamedBackfill = newFieldNames.foldLeft(backfillWithFlag) {
          (df, n) =>
            df.withColumnRenamed(n, n + suffix)
        }
        val joined = snapshotDF.join(renamedBackfill, joinColumns, "left")
        // Attach per-field provenance flags BEFORE the coalesce rewrites `n`,
        // so the flags bind to the unresolved snapshot-side `n` attribute. This
        // keeps the flags accurate even though the later coalesce shadows the
        // original `n` column in the output schema.
        val withFlags = newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(usedSnapshotCol(n), df.col(n).isNotNull)
            .withColumn(
              usedBfCol(n),
              df.col(n).isNull && df.col(n + suffix).isNotNull
            )
        }
        newFieldNames.foldLeft(withFlags) { (df, n) =>
          df.withColumn(n, coalesce(df.col(n), df.col(n + suffix)))
            .drop(n + suffix)
        }

      case MilvusOption.BackfillModeOverwrite =>
        // Same suffix-rename scaffolding as coalesce — both modes need both
        // sides' target columns live on the joined frame.
        val suffix = BackfillSideSuffix
        val renamedBackfill = newFieldNames.foldLeft(backfillWithFlag) {
          (df, n) =>
            df.withColumnRenamed(n, n + suffix)
        }
        val joined = snapshotDF.join(renamedBackfill, joinColumns, "left")
        // Match flag is null for unmatched left rows, non-null for matched.
        // Using it (rather than `bf.isNotNull`) preserves overwrite's "file
        // wins, null included" semantics when the file explicitly stores null.
        val matched = col(MatchFlagCol).isNotNull
        val withFlags = newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(usedSnapshotCol(n), !matched)
            .withColumn(usedBfCol(n), matched)
        }
        newFieldNames.foldLeft(withFlags) { (df, n) =>
          df.withColumn(
            n,
            when(matched, df.col(n + suffix)).otherwise(df.col(n))
          ).drop(n + suffix)
        }

      case MilvusOption.BackfillModeReplace =>
        // Use the using-column join form: both sides share the internal join
        // columns, so Spark collapses them into one,
        // avoiding ambiguous-reference errors downstream.
        snapshotDF.join(backfillWithFlag, joinColumns, "left")

      case other =>
        // Defensive: validate() rejects anything else, but keep a clear error
        // in case a new mode constant is added without extending this match.
        throw new IllegalArgumentException(
          s"Unknown backfill mode '$other'"
        )
    }
  }

  /** Workaround until Milvus snapshot exposes `FieldBinlog.child_fields`: when
    * the same Milvus fieldID is claimed by multiple V2 column groups (e.g. an
    * original multi-field group at slot < 100 PLUS a single-field group at slot
    * \== fieldID written by a prior addfield+backfill), keep the field only in
    * the group with the largest `slotFieldId`.
    *
    * Backfill writes single-field groups with `slot == fieldID` (>= 100), and
    * Milvus segcore allocates multi-field group slots from the smallest unused
    * int < 100, so "max slot wins" is equivalent to "newer single-field group
    * wins" under the current column-group naming convention. When Milvus's
    * snapshot starts emitting `FieldBinlog.child_fields`, this should be
    * replaced by the authoritative mapping (see `FooterV2SegmentResolver` line
    * 88-94 for the parquet-footer-based reconciliation that this defends
    * against).
    *
    * Skips dedup entirely when any contributing group has `slotFieldId < 0L`
    * (the sentinel for "unknown slot", e.g. the snapshot-JSON DTO path that
    * doesn't carry the AVRO slot id) so we don't accidentally drop fields when
    * the slot signal is missing. We must NOT use `0L` as the sentinel because
    * RowID's column group is legitimately at slot 0 — using 0L would
    * short-circuit dedup on every AVRO-loaded segment.
    */
  private[backfill] def dedupColumnGroupsBySlot(
      seg: Segment
  ): Segment = {
    val before = seg.columnGroups
    val deduped = seg.dedupColumnGroupsBySlot
    if (before != deduped.columnGroups) {
      logger.info(
        s"V2 dedup segment=${seg.id}: removed overlapping field ids " +
          "claimed by older slots"
      )
    }
    deduped
  }

  /** Decode the StorageV2 segments referenced by the snapshot's
    * `manifest_list`. Each AVRO gives us slot→paths; the matching parquet
    * footer's `group_field_id_list` recovers the real field IDs per column
    * group. Called once per backfill and threaded into both the read and write
    * paths.
    */
  private def loadV2Segments(
      metadata: SnapshotJson,
      config: BackfillConfig
  ): Either[BackfillError, Seq[Segment]] = {
    if (metadata.manifestList.isEmpty) return Right(Seq.empty)
    try {
      // The Avro manifests and parquet footers are read through the native
      // store the fs.* options describe (the main bucket, IAM or static
      // keys), the same store every other driver-side read uses.
      val store = com.zilliz.milvus.storage.io.NativeObjectStore
        .Factory(
          com.zilliz.milvus.storage.credential.StorageProperties
            .from(config.getMilvusReadOptions)
        )
        .open()
      val loaded =
        try
          com.zilliz.milvus.storage.compat.v2.FooterV2SegmentResolver
            .loadV2Segments(
              metadata.manifestList,
              config.s3BucketName,
              store,
              manifestSchemaVersion = metadata.manifestSchemaVersion,
              applyDeletes = ApplyDeletesToSnapshotRows,
              storageScheme = BackfillStorage.storageScheme(config),
              endpoint = config.s3Endpoint
            )
        finally store.close()
      loaded match {
        case Right(segs) =>
          // Workaround for Milvus snapshot not yet exposing FieldBinlog.child_fields:
          // some segments carry both an old multi-field column group (slot < 100,
          // declaring fields like 102..114) AND newer single-field groups (slot ==
          // fieldID, written by a prior addfield+backfill). The C++ packed reader
          // picks an undefined source when the same fieldID appears in multiple
          // groups, often returning the older slot's stale (mostly-null) data —
          // which silently breaks coalesce-mode merges. dedupColumnGroupsBySlot
          // strips overlapping fieldIDs from the older (smaller-slot) groups so
          // each fieldID resolves to exactly one column group.
          val deduped = segs.map(dedupColumnGroupsBySlot)
          logger.info(
            s"Loaded ${deduped.size} StorageV2 segment(s) from snapshot AVRO manifest_list (post-dedup)"
          )
          Right(deduped)
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

  /** Load the required snapshot, reporting read and parse failures. */
  private def loadSnapshotMetadata(
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, SnapshotJson] = {
    BackfillStorage.readSnapshotJson(snapshotPath, config) match {
      case Right(json) if json.nonEmpty =>
        SnapshotJson.parse(json) match {
          case Right(metadata) => Right(metadata)
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

  /** Extract collection metadata from snapshot: collectionID,
    * segment-to-partition mapping, segment base paths. Partition IDs are
    * derived from manifest basePaths:
    * {rootPath}/insert_log/{col_id}/{part_id}/{seg_id}
    */
  private[backfill] def extractMetadataFromSnapshot(
      metadata: SnapshotJson,
      v2Segments: Seq[Segment] = Seq.empty,
      bucket: String = "",
      endpoint: String = ""
  ): (Long, Map[Long, Long], Map[Long, String]) = {
    val collectionID = metadata.snapshotInfo.collectionId

    val manifestList = metadata.storageV2ManifestList.getOrElse(Seq.empty)
    var segmentToPartitionMap = Map.empty[Long, Long]
    var segmentBasePathMap = Map.empty[Long, String]

    // V3 (manifest-based) segments: basePath carries partition id too.
    for (item <- manifestList) {
      val segId = item.segmentID
      ManifestContentJson.parse(item.manifest) match {
        case Right(mc) =>
          val located = StoragePath.parseMilvus(mc.basePath, bucket, endpoint)
          if (
            bucket.nonEmpty && located.hasBucket && located.bucket != bucket
          ) {
            throw new IllegalArgumentException(
              s"segment $segId manifest is in bucket '${located.bucket}' " +
                s"but backfill is bound to '$bucket': ${mc.basePath}"
            )
          }
          val basePath = located.key
          // Extract partition ID from basePath: .../insert_log/{col_id}/{part_id}/{seg_id}
          val parts = basePath.split("/")
          val insertLogIdx = parts.indexOf("insert_log")
          if (insertLogIdx >= 0 && insertLogIdx + 2 < parts.length) {
            try {
              val partitionId = parts(insertLogIdx + 2).toLong
              segmentBasePathMap += (segId -> basePath)
              segmentToPartitionMap += (segId -> partitionId)
            } catch {
              case _: NumberFormatException =>
                logger.warn(
                  s"Skipping segment $segId: failed to parse partition ID from basePath: $basePath"
                )
            }
          } else {
            logger.warn(
              s"Skipping segment $segId: basePath does not contain expected insert_log structure: $basePath"
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
      segmentToPartitionMap += (seg.id -> seg.partitionId)
    }

    logger.info(
      s"Extracted from snapshot: collectionID=$collectionID, " +
        s"segments=${segmentToPartitionMap.keys.mkString(",")} " +
        s"(v3=${segmentBasePathMap.size}, v2=${v2Segments.size})"
    )

    (collectionID, segmentToPartitionMap, segmentBasePathMap)
  }

}
