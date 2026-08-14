package com.zilliz.spark.connector.operations.backfill

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

  private[backfill] final case class BackfillSource(
      dataFrame: DataFrame,
      checkpointRDD: Option[RDD[Row]]
  )

  /** Marker column added to the backfill side before the left join so we can
    * count how many source rows found a join-key match (non-null marker →
    * matched). Kept on the DataFrame all the way down to the per-segment
    * partition function, and stripped from the projection written to parquet.
    */
  private[backfill] val MatchFlagCol = "__bf_matched__"
  private[backfill] val SegmentIdCol = MilvusOption.MilvusExtraColumnSegmentID
  private[backfill] val RowOffsetCol = MilvusOption.MilvusExtraColumnRowOffset

  /** Per-field flag columns added in any mode that reads source-side target
    * values (coalesce + overwrite). For each new field, one boolean marker
    * records whether the written value came from the source side and another
    * whether it came from the backfill data file. Semantics differ per mode:
    *   - coalesce: usedSrc = src non-null; usedBf = src null AND bf non-null
    *   - overwrite: usedSrc = join key unmatched; usedBf = join key matched
    * Used by the writer to tally the per-field `usedSourceByField` /
    * `usedDataFileByField` counts surfaced in `SegmentBackfillResult`.
    */
  private[backfill] def usedSrcCol(field: String): String =
    s"__bf_used_src_${field}__"
  private[backfill] def usedBfCol(field: String): String =
    s"__bf_used_bf_${field}__"

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

    // OSS source reads use a local-checkpointed RDD to sever the external
    // storage lineage while scoped credentials are installed. Keep the actual
    // persisted RDD so its blocks can be released after the transformed
    // backfill DataFrame is materialized, including on failure.
    var sourceCheckpointRDD: Option[RDD[Row]] = None
    var cachedBackfillDF: DataFrame = null

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
      // Step 3: Resolve the current primary-key join into the generic runtime
      // join-key model. PR1 intentionally keeps PK as the only public behavior;
      // PR2 will add explicit physical-field resolution on top of this seam.
      val baseJoinKey = snapshotMetadataOpt match {
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
          ResolvedJoinKey.primaryKey(
            pkField.name,
            pkField.getFieldIDAsLong,
            Some(MilvusSnapshotReader.fieldToStructField(pkField))
          )
        case None =>
          client.getPkField(config.databaseName, config.collectionName) match {
            case scala.util.Success((name, id)) =>
              ResolvedJoinKey.primaryKey(name, id, None)
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
          case Right(source) =>
            sourceCheckpointRDD = source.checkpointRDD
            source.dataFrame
        }

      // Reproject via the existing PK column-mapping contract, then separate
      // the join component from fields that will be written. The join column is
      // normalized to an internal alias so downstream execution no longer
      // depends on the collection's PK name.
      val preparedBackfill = prepareBackfillData(
        rawBackfillDF,
        baseJoinKey,
        config.columnMapping,
        snapshotMetadataOpt.toSeq.flatMap(
          _.collection.schema.fields.map(_.name)
        )
      ) match {
        case Left(error) => return Left(error)
        case Right(data) => data
      }
      val mappedBackfillDF = preparedBackfill.dataFrame
      val joinKey = preparedBackfill.joinKey

      val newFieldNames = preparedBackfill.targetFieldNames

      // Reject a backfill column named MatchFlagCol: the join adds a
      // lit(true) marker under that name, which would silently overwrite a
      // user column (overwrite mode) or blow up with AnalysisException on
      // the coalesce rename/self-reference (coalesce mode).
      if (newFieldNames.contains(MatchFlagCol)) {
        return Left(
          SchemaValidationError(
            s"Backfill parquet contains a column named '$MatchFlagCol', " +
              "which is reserved for internal use by the backfill join. " +
              "Rename the column (or use --column-mapping) and retry."
          )
        )
      }

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

      val snapshotMetadata = snapshotMetadataOpt.getOrElse {
        return Left(
          SchemaValidationError(
            "ADDFIELD backfill requires collection schema from a snapshot"
          )
        )
      }
      val targetFieldsByName = newFieldNames.map { name =>
        val field = snapshotMetadata.collection.schema.fields
          .find(_.name == name)
          .getOrElse(
            return Left(
              SchemaValidationError(
                s"Field '$name' not found in snapshot collection schema"
              )
            )
          )
        name -> field
      }.toMap

      // User parquet uses normal ingestion-friendly vector shapes (numeric
      // arrays, byte arrays, sparse maps/structs/JSON). Normalize vector
      // fields to Milvus's physical per-row bytes before caching or joining so
      // Spark never widens or rewrites half/int8/sparse representations.
      val backfillDF = VectorBackfillSupport.normalizeVectorColumns(
        mappedBackfillDF,
        targetFieldsByName
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      // Cache so the upcoming join-key aggregation doesn't force
      // a second parquet scan when performJoin consumes the DF later. The
      // count also eagerly validates every normalized vector row.
      backfillDF.cache()
      cachedBackfillDF = backfillDF

      val joinKeyStats = validateJoinKeyCardinality(
        backfillDF,
        joinKey.internalColumns,
        joinKey.sourceColumns,
        side = "Backfill parquet"
      ) match {
        case Left(error)  => return Left(error)
        case Right(stats) => stats
      }
      val backfillRowCount = joinKeyStats.rowCount
      sourceCheckpointRDD.foreach(_.unpersist())
      sourceCheckpointRDD = None
      logger.info(
        s"Backfill data file rows: $backfillRowCount " +
          s"(distinct join keys: ${joinKeyStats.distinctValidKeyCount})"
      )

      val targetVectorFields = targetFieldsByName.collect {
        case (name, field) if VectorBackfillSupport.isVectorField(field) =>
          name -> VectorBackfillSupport.canonicalStructField(field)
      }

      // In coalesce / overwrite modes, also read each target field from source
      // so the merge step (coalesce(src,bf) or when(matched, bf).otherwise(src))
      // can compare source and parquet values per row. Requires a snapshot —
      // we need the field's Spark type.
      val readsSourceFields = config.readsSourceFields
      val extraReadFields
          : Seq[(String, Long, org.apache.spark.sql.types.StructField)] =
        if (readsSourceFields) {
          newFieldNames.map { n =>
            val fid = newFieldNameToId(n)
            val field = targetFieldsByName(n)
            val structField = targetVectorFields.getOrElse(
              n,
              MilvusSnapshotReader.fieldToStructField(field)
            )
            (
              n,
              fid,
              structField
            )
          }
        } else Seq.empty

      // In any source-reading mode, parquet column types must match snapshot
      // field types exactly (see validateMergeableFieldTypes for rationale).
      if (readsSourceFields) {
        validateMergeableFieldTypes(
          backfillDF.schema,
          extraReadFields,
          config.mode
        ) match {
          case Left(error) => return Left(error)
          case Right(_)    => // Continue
        }
      }

      // Read original collection data with segment metadata
      val originalDF = readCollectionWithMetadata(
        spark,
        config,
        joinKey,
        snapshotMetadataOpt,
        v2Segments,
        extraReadFields
      ) match {
        case Left(error) => return Left(error)
        case Right(df)   => df
      }

      // Validate join-key schema compatibility.
      validateJoinKeyCompatibility(originalDF, backfillDF, joinKey) match {
        case Left(error) => return Left(error)
        case Right(_)    => // Continue
      }

      // Merge original and backfill DataFrames according to mode.
      val joinedDF = performJoin(
        originalDF,
        backfillDF,
        joinKey.internalColumns,
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
        newFieldNameToId,
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
        partitionId = if (partitionIDs.size == 1) partitionIDs.head else -1,
        schemaVersion = snapshotMetadataOpt
          .map(_.collection.schema.version)
          .getOrElse(0),
        newFieldNames = newFieldNames,
        totalBackfillDataRows = backfillRowCount
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

  /** Read backfill data from Parquet file
    */
  private def readBackfillData(
      spark: SparkSession,
      rawPath: String,
      config: BackfillConfig
  ): Either[BackfillError, BackfillSource] = {
    // Hadoop 3.4.1 has separate s3:// and s3a:// FileSystem implementations
    // and per-bucket fs.s3a.bucket.<b>.* config is only honored by
    // S3AFileSystem. Force the s3a scheme so the credentials we just wrote
    // actually take effect.
    val path = normalizeObjectStorageScheme(rawPath, config)
    try {
      // Configure the source bucket before reading the parquet. OSS must be
      // fully materialized while this temporary credential scope is active;
      // S3A uses persistent per-bucket configuration and does not need it.
      Right(withScopedHadoopStorage(spark, path, config, isSource = true) {
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
      })
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

  /** Apply the existing PK mapping contract, then normalize identity columns to
    * private join aliases and return target fields explicitly.
    *
    * PR1 still resolves exactly one primary-key component. The sequence-based
    * shape is intentional so the read and join stages do not need another
    * signature change when a later PR adds physical or composite keys.
    */
  private[backfill] def prepareBackfillData(
      df: DataFrame,
      joinKey: ResolvedJoinKey,
      userMapping: Option[Map[String, String]],
      collectionFieldNames: Seq[String] = Seq.empty
  ): Either[BackfillError, PreparedBackfillData] = {
    // The existing mapping contract identifies the first (currently only)
    // component through the collection PK target. Any additional internal
    // components must already be present after mapping; this keeps the helper
    // sequence-shaped without exposing composite keys publicly in PR1.
    val keyColumn = joinKey.sourceColumns.head
    applyColumnMapping(df, keyColumn, userMapping).flatMap { mapped =>
      val mappedColumns = mapped.columns.toSeq
      val missingKeys = joinKey.sourceColumns.filterNot(mappedColumns.contains)
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
          allocatedJoinKey.components.map(c => c.sourceColumn -> c).toMap
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
          mappedColumns.filterNot(allocatedJoinKey.sourceColumns.contains)
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
  private[backfill] def buildSourceReadProjection(
      joinKey: ResolvedJoinKey,
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ]
  ): Either[BackfillError, SourceReadProjection] = {
    val missingSchemas = joinKey.components.filter(_.sourceField.isEmpty)
    if (missingSchemas.nonEmpty) {
      Left(
        SchemaValidationError(
          s"Missing snapshot schema for join-key fields: " +
            missingSchemas.map(_.sourceColumn).mkString(", ")
        )
      )
    } else {
      val requested =
        joinKey.components.map(c => (c.fieldId, c.sourceField.get)) ++
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
        SourceReadProjection(
          fieldIds = deduped.map(_._1),
          schema = org.apache.spark.sql.types.StructType(deduped.map(_._2))
        )
      )
    }
  }

  /** Normalize source-side join columns in one projection. A sequential
    * `withColumnRenamed` chain can corrupt swaps or rename chains when a source
    * field happens to use another component's internal name.
    */
  private[backfill] def normalizeSourceJoinColumns(
      df: DataFrame,
      joinKey: ResolvedJoinKey
  ): DataFrame = {
    val sourceAliases =
      joinKey.components.map(c => c.sourceColumn -> c.internalColumn).toMap
    df.select(
      df.columns.toSeq.map { name =>
        sourceAliases.get(name) match {
          case Some(alias) => df.col(name).as(alias)
          case None        => df.col(name)
        }
      }: _*
    )
  }

  /** Read collection data with $segment_id and $row_offset metadata $segment_id
    * and $row_offset are used to match with the original sequence of rows for
    * each segment
    *
    * @param joinKey
    *   Resolved source fields used to match backfill input rows
    * @param snapshotMetadata
    *   Optional snapshot metadata for offline reading (no client connection)
    */
  private def readCollectionWithMetadata(
      spark: SparkSession,
      config: BackfillConfig,
      joinKey: ResolvedJoinKey,
      snapshotMetadata: Option[SnapshotMetadata],
      v2Segments: Seq[com.zilliz.spark.connector.read.V2SegmentInfo],
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ] = Seq.empty
  ): Either[BackfillError, DataFrame] = {
    try {
      var options = config.getMilvusReadOptions
      val allFieldIds = (joinKey.fieldIds ++ extraReadFields.map(_._2)).distinct
      options =
        options + (MilvusOption.ReaderFieldIDs -> allFieldIds.mkString(","))
      options = options + (MilvusOption.ReadApplyDeletes -> "false")

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
        case Some(_) =>
          // The supplied schema and ReaderFieldIDs must stay in the same order.
          // PR1 resolves the PK into one component; the component sequence also
          // supports future physical/composite key resolvers.
          val projection =
            buildSourceReadProjection(joinKey, extraReadFields) match {
              case Left(error)  => return Left(error)
              case Right(value) => value
            }
          options =
            options + (MilvusOption.ReaderFieldIDs -> projection.fieldIds
              .mkString(","))

          logger.info(
            s"Reading with schema: ${projection.schema.fieldNames.mkString(", ")}"
          )

          spark.read
            .schema(projection.schema)
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

      val missingSourceJoinColumns =
        joinKey.sourceColumns.filterNot(df.columns.contains)
      if (missingSourceJoinColumns.nonEmpty) {
        return Left(
          ConnectionError(
            message = s"Failed to read collection join-key columns: " +
              missingSourceJoinColumns.mkString(", ")
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

      Right(normalizeSourceJoinColumns(df, joinKey))
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

  /** Coalesce and overwrite modes require parquet column types to match
    * snapshot field types exactly. Both modes synthesize a per-row choice
    * between the source-side and parquet-side values (`coalesce(src, bf)` in
    * coalesce, `when(matched, bf).otherwise(src)` in overwrite) — Spark would
    * otherwise widen to a common supertype (e.g. Int + Long → Long) and the
    * writer would emit binlogs whose Arrow type no longer matches the Milvus
    * field, which Milvus would later misread.
    */
  private[backfill] def validateMergeableFieldTypes(
      backfillSchema: org.apache.spark.sql.types.StructType,
      extraReadFields: Seq[
        (String, Long, org.apache.spark.sql.types.StructField)
      ],
      mode: String
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
          s"--mode=$mode requires backfill parquet column types to match " +
            s"snapshot field types exactly. " +
            s"Mismatched: ${mismatches.mkString(", ")}"
        )
      )
    } else {
      Right(())
    }
  }

  /** Validate that a DataFrame contains a non-null, unique join key.
    *
    * A duplicate on the backfill side would fan one source row out into
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

  /** Validate join-key schema compatibility between source and backfill data.
    */
  private[backfill] def validateJoinKeyCompatibility(
      originalDF: DataFrame,
      backfillDF: DataFrame,
      joinKey: ResolvedJoinKey
  ): Either[BackfillError, Unit] = {
    try {
      joinKey.components.foreach { component =>
        val sourceField = originalDF.schema.fields
          .find(_.name == component.internalColumn)
          .getOrElse {
            return Left(
              SchemaValidationError(
                s"Original collection data must have join-key field " +
                  s"'${component.sourceColumn}'"
              )
            )
          }

        val backfillField = backfillDF.schema.fields
          .find(_.name == component.internalColumn)
          .getOrElse {
            return Left(
              SchemaValidationError(
                s"Backfill data must have join-key field " +
                  s"'${component.sourceColumn}' after column mapping"
              )
            )
          }

        if (sourceField.dataType != backfillField.dataType) {
          return Left(
            SchemaValidationError(
              s"Join-key type mismatch for '${component.sourceColumn}': " +
                s"original=${sourceField.dataType}, new=${backfillField.dataType}"
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

  /** Merge original (source) rows with backfill rows per `mode`.
    *
    *   - replace: left join on the resolved key; backfill value replaces source
    *     (source only contributes join + segment tracking columns). Unmatched
    *     source rows end up with null target columns.
    *   - coalesce: source side carries the target fields; after the left join,
    *     compute `coalesce(src, backfill)` per field (source wins when
    *     non-null, otherwise use backfill). Unmatched source rows keep their
    *     original target values.
    *   - overwrite: source side carries the target fields; after the left join,
    *     compute `when(matched, backfill).otherwise(src)` per field (file wins
    *     when the join key matched, even if the file value is null). Unmatched
    *     source rows keep their original target values.
    */
  private[backfill] def performJoin(
      originalDF: DataFrame,
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
        // source-side columns now present on originalDF.
        val suffix = "__bf__"
        val renamedBackfill = newFieldNames.foldLeft(backfillWithFlag) {
          (df, n) =>
            df.withColumnRenamed(n, n + suffix)
        }
        val joined = originalDF.join(renamedBackfill, joinColumns, "left")
        // Attach per-field provenance flags BEFORE the coalesce rewrites `n`,
        // so the flags bind to the unresolved source-side `n` attribute. This
        // keeps the flags accurate even though the later coalesce shadows the
        // original `n` column in the output schema.
        val withFlags = newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(usedSrcCol(n), df.col(n).isNotNull)
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
        val suffix = "__bf__"
        val renamedBackfill = newFieldNames.foldLeft(backfillWithFlag) {
          (df, n) =>
            df.withColumnRenamed(n, n + suffix)
        }
        val joined = originalDF.join(renamedBackfill, joinColumns, "left")
        // Match flag is null for unmatched left rows, non-null for matched.
        // Using it (rather than `bf.isNotNull`) preserves overwrite's "file
        // wins, null included" semantics when the file explicitly stores null.
        val matched = col(MatchFlagCol).isNotNull
        val withFlags = newFieldNames.foldLeft(joined) { (df, n) =>
          df.withColumn(usedSrcCol(n), !matched)
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
        originalDF.join(backfillWithFlag, joinColumns, "left")

      case other =>
        // Defensive: validate() rejects anything else, but keep a clear error
        // in case a new mode constant is added without extending this match.
        throw new IllegalArgumentException(
          s"Unknown backfill mode '$other'"
        )
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
      fieldNameToId: Map[String, Long] = Map.empty,
      targetFieldOverrides: Map[
        String,
        org.apache.spark.sql.types.StructField
      ] = Map.empty
  ): Either[BackfillError, Map[Long, SegmentBackfillResult]] = {

    try {
      // Prepare data: select only needed columns and add $segment_id for
      // partitioning. Trailing column is the backfill match flag — retained
      // here so executors can count PK-matched rows, stripped from the
      // projection that reaches the writer. In any source-reading mode
      // (coalesce / overwrite), two extra boolean flag columns per new field
      // follow the match flag (usedSrc, usedBf interleaved, in
      // `newFieldNames` order) so the writer can compute per-field
      // usedSourceByField / usedDataFileByField counts.
      val readsSourceFields = config.readsSourceFields
      val flagColNames: Seq[String] =
        if (readsSourceFields)
          newFieldNames.flatMap(n => Seq(usedSrcCol(n), usedBfCol(n)))
        else Seq.empty
      val preparedDF = joinedDF
        .select(
          (Seq(SegmentIdCol, RowOffsetCol) ++ newFieldNames ++ Seq(
            MatchFlagCol
          ) ++ flagColNames).map(col): _*
        )

      // Get the schema for new fields only (without $segment_id, $row_offset,
      // or the match flag)
      val targetSchema = org.apache.spark.sql.types.StructType(
        newFieldNames.map(fieldName =>
          targetFieldOverrides.getOrElse(
            fieldName,
            preparedDF.schema.fields.find(_.name == fieldName).get
          )
        )
      )

      val segmentIds = segmentToPartitionMap.keys.toArray
      val segmentPartitioner = new SegmentPartitioner(segmentIds)

      // Repartition using custom partitioner, then sort by $row_offset within each partition
      // CRITICAL: .copy() is required because queryExecution.toRdd produces an iterator
      // that reuses the same UnsafeRow buffer. Without copy, keyBy/partitionBy's
      // ExternalSorter stores references to the same mutable buffer, causing all
      // rows to contain the last row's data.
      val repartitionedRDD = preparedDF.queryExecution.toRdd
        .map(_.copy()) // Materialize each row to avoid UnsafeRow reuse
        .keyBy(_.getLong(0)) // $segment_id is at index 0
        .partitionBy(segmentPartitioner)
        .values
        .mapPartitions(iter =>
          iter.toSeq.sortBy(_.getLong(1)).iterator
        ) // Sort by $row_offset

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
      val totalSource = results.map(_._1.sourceRowCount).sum
      val totalMatched = results.map(_._1.matchedRowCount).sum
      val matchRateStr =
        if (totalSource > 0)
          f"${totalMatched.toDouble / totalSource * 100}%.2f%%"
        else "n/a"

      logger.info("=== Backfill Summary ===")
      logger.info(s"Total segments: ${results.length}")
      logger.info(s"Total source rows: $totalSource")
      logger.info(
        s"Total matched rows: $totalMatched (match rate: $matchRateStr)"
      )
      logger.info(s"Total rows written: $totalRows")
      logger.info(s"Total time for all segments: ${totalTime}ms")
      logger.info(s"Average time per segment: ${avgTime}ms")
      if (readsSourceFields) {
        logger.info(
          s"Per-field provenance (mode=${config.mode}, aggregated):"
        )
        newFieldNames.foreach { f =>
          val src = results.map(_._1.usedSourceByField.getOrElse(f, 0L)).sum
          val df = results.map(_._1.usedDataFileByField.getOrElse(f, 0L)).sum
          val nullOut = totalSource - src - df
          logger.info(s"  $f: source=$src, dataFile=$df, null=$nullOut")
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

    val readsSourceFields = config.readsSourceFields
    val newFieldNames = targetSchema.fieldNames.toSeq
    val numNewFields = newFieldNames.size
    // Row layout (fixed prefix): [$segment_id, $row_offset, ...newFields,
    // __bf_matched__, (usedSrc, usedBf)*numNewFields in source-reading modes
    // (coalesce / overwrite) only].
    val matchFlagIdx = 2 + numNewFields
    val firstFlagIdx = matchFlagIdx + 1

    try {
      var rowCount = 0L
      var nullRowCount = 0L
      var matchedRowCount = 0L
      val usedSrcCounts = Array.fill(numNewFields)(0L)
      val usedBfCounts = Array.fill(numNewFields)(0L)

      def writeRow(row: InternalRow): Unit = {
        val dataEnd = matchFlagIdx
        val targetFields = (2 until dataEnd)
          .map(i => row.get(i, targetSchema.fields(i - 2).dataType))
          .toArray
        if (targetFields.forall(_ == null)) nullRowCount += 1
        if (!row.isNullAt(matchFlagIdx)) matchedRowCount += 1
        if (readsSourceFields) {
          var i = 0
          while (i < numNewFields) {
            val srcIdx = firstFlagIdx + 2 * i
            val bfIdx = srcIdx + 1
            if (!row.isNullAt(srcIdx) && row.getBoolean(srcIdx))
              usedSrcCounts(i) += 1
            if (!row.isNullAt(bfIdx) && row.getBoolean(bfIdx))
              usedBfCounts(i) += 1
            i += 1
          }
        }
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

      val (usedSrcByField, usedBfByField) =
        if (readsSourceFields)
          (
            newFieldNames.zip(usedSrcCounts).toMap,
            newFieldNames.zip(usedBfCounts).toMap
          )
        else (Map.empty[String, Long], Map.empty[String, Long])

      Iterator.single(
        (
          SegmentBackfillResult(
            segmentId = segmentID,
            rowCount = rowCount,
            manifestPaths = manifestPaths,
            outputPath = outputPath,
            executionTimeMs = System.currentTimeMillis() - startTime,
            committedVersion = committedVersion,
            sourceRowCount = rowCount,
            matchedRowCount = matchedRowCount,
            usedSourceByField = usedSrcByField,
            usedDataFileByField = usedBfByField
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
    * replaced by the authoritative mapping (see `V2SegmentLoader` line 88-94
    * for the parquet-footer-based reconciliation that this defends against).
    *
    * Skips dedup entirely when any contributing group has `slotFieldId < 0L`
    * (the sentinel for "unknown slot", e.g. the snapshot-JSON DTO path that
    * doesn't carry the AVRO slot id) so we don't accidentally drop fields when
    * the slot signal is missing. We must NOT use `0L` as the sentinel because
    * RowID's column group is legitimately at slot 0 — using 0L would
    * short-circuit dedup on every AVRO-loaded segment.
    */
  private[backfill] def dedupColumnGroupsBySlot(
      seg: com.zilliz.spark.connector.read.V2SegmentInfo
  ): com.zilliz.spark.connector.read.V2SegmentInfo = {
    val groups = seg.columnGroups
    if (groups.isEmpty || groups.exists(_.slotFieldId < 0L)) return seg

    val maxSlotPerField: Map[Long, Long] =
      groups
        .flatMap(g => g.fieldIds.map(fid => fid -> g.slotFieldId))
        .groupBy(_._1)
        .map { case (fid, pairs) => fid -> pairs.map(_._2).max }

    val rebuilt = groups.flatMap { g =>
      val keptFids =
        g.fieldIds.filter(fid => maxSlotPerField(fid) == g.slotFieldId)
      val stripped = g.fieldIds.diff(keptFids)
      if (stripped.nonEmpty) {
        logger.info(
          s"V2 dedup segment=${seg.segmentId} slot=${g.slotFieldId}: " +
            s"stripped fieldIds=${stripped.mkString(",")} " +
            s"(owned by larger slots ${stripped.map(maxSlotPerField).mkString(",")})"
        )
      }
      if (keptFids.isEmpty) None
      else Some(g.copy(fieldIds = keptFids))
    }

    seg.copy(columnGroups = rebuilt)
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
      // Configure a private Hadoop view so V2SegmentLoader can read AVRO and
      // parquet footers without mutating the Spark session's shared OSS
      // credentials. The main bucket (not the source bucket) holds these
      // snapshot artifacts.
      val hadoopConf = new org.apache.hadoop.conf.Configuration(
        spark.sparkContext.hadoopConfiguration
      )
      configureHadoopStorageForPath(
        hadoopConf,
        storagePath(config, ""),
        config,
        isSource = false
      )
      hadoopConf.set("fs.oss.impl.disable.cache", "true")
      com.zilliz.spark.connector.read.V2SegmentLoader
        .loadV2Segments(
          metadata.manifestList,
          config.s3BucketName,
          hadoopConf,
          manifestSchemaVersion = metadata.manifestSchemaVersion,
          applyDeletes = false,
          storageScheme = storageScheme(config)
        ) match {
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

    val outputRoot = storagePath(
      config,
      s"${config.s3RootPath.stripSuffix("/")}/insert_log/$collectionID/$partitionID/$segmentID"
    )

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

    val readsSourceFields = config.readsSourceFields
    val numNewFields = fieldNames.size
    // Row layout (fixed prefix): [$segment_id, $row_offset, ...newFields,
    // __bf_matched__, (usedSrc, usedBf)*numNewFields in source-reading modes
    // (coalesce / overwrite) only].
    val matchFlagIdx = 2 + numNewFields
    val firstFlagIdx = matchFlagIdx + 1

    var rowCount = 0L
    var matchedRowCount = 0L
    val usedSrcCounts = Array.fill(numNewFields)(0L)
    val usedBfCounts = Array.fill(numNewFields)(0L)
    try {
      // The V2 writer only wants the newField columns; strip the tracking
      // columns, the match flag, and any source-reading-mode provenance flags
      // (the match flag + per-field flags are folded into the counters
      // instead).
      def projected(row: InternalRow): InternalRow = {
        val dataEnd = matchFlagIdx
        val values = (2 until dataEnd)
          .map(i => row.get(i, targetSchema.fields(i - 2).dataType))
          .toArray
        new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values)
      }
      def countMatch(row: InternalRow): Unit = {
        if (!row.isNullAt(matchFlagIdx)) matchedRowCount += 1
        if (readsSourceFields) {
          var i = 0
          while (i < numNewFields) {
            val srcIdx = firstFlagIdx + 2 * i
            val bfIdx = srcIdx + 1
            if (!row.isNullAt(srcIdx) && row.getBoolean(srcIdx))
              usedSrcCounts(i) += 1
            if (!row.isNullAt(bfIdx) && row.getBoolean(bfIdx))
              usedBfCounts(i) += 1
            i += 1
          }
        }
      }

      countMatch(firstRow)
      writer.write(projected(firstRow))
      rowCount += 1
      iter.foreach { row =>
        countMatch(row)
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
      val (usedSrcByField, usedBfByField) =
        if (readsSourceFields)
          (
            fieldNames.zip(usedSrcCounts).toMap,
            fieldNames.zip(usedBfCounts).toMap
          )
        else (Map.empty[String, Long], Map.empty[String, Long])
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
            ),
            sourceRowCount = rowCount,
            matchedRowCount = matchedRowCount,
            usedSourceByField = usedSrcByField,
            usedDataFileByField = usedBfByField
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
    val outputPath = normalizeObjectStorageScheme(rawOutputPath, config)
    try {
      withScopedHadoopStorage(spark, outputPath, config, isSource = false) {
        val hadoopPath = new Path(outputPath)
        val fs =
          hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val out = fs.create(hadoopPath, true)
        try {
          out.write(
            result.toJson.getBytes(java.nio.charset.StandardCharsets.UTF_8)
          )
        } finally {
          out.close()
        }
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

  private[backfill] def normalizeObjectStorageScheme(
      path: String,
      config: BackfillConfig
  ): String = {
    if (path == null) null
    else if (config.s3CloudProvider.trim.equalsIgnoreCase("aliyun")) {
      if (path.startsWith("s3://")) "oss://" + path.stripPrefix("s3://")
      else if (path.startsWith("s3a://")) "oss://" + path.stripPrefix("s3a://")
      else path
    } else normalizeS3Scheme(path)
  }

  private[backfill] def storageScheme(config: BackfillConfig): String =
    if (config.s3CloudProvider.trim.equalsIgnoreCase("aliyun")) "oss"
    else "s3a"

  private[backfill] def storagePath(
      config: BackfillConfig,
      suffix: String
  ): String = {
    storageScheme(config) + "://" + config.s3BucketName + "/" + suffix
      .stripPrefix("/")
  }

  private val OssHadoopKeys = Seq(
    "fs.oss.impl",
    "fs.oss.impl.disable.cache",
    "fs.oss.endpoint",
    "fs.oss.connection.secure.enabled",
    "fs.oss.accessKeyId",
    "fs.oss.accessKeySecret",
    BackfillConfig.HadoopOssCredentialsProvider
  )

  private[backfill] def withScopedHadoopStorage[T](
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  )(operation: => T): T = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    if (path == null || !path.startsWith("oss://")) {
      configureHadoopStorageForPath(hadoopConf, path, config, isSource)
      return operation
    }

    val previous = mutable.LinkedHashMap.empty[String, Option[String]]
    OssHadoopKeys.foreach(key => previous.put(key, Option(hadoopConf.get(key))))
    try {
      configureHadoopStorageForPath(hadoopConf, path, config, isSource)
      hadoopConf.set("fs.oss.impl.disable.cache", "true")
      operation
    } finally {
      previous.foreach {
        case (key, Some(value)) => hadoopConf.set(key, value)
        case (key, None)        => hadoopConf.unset(key)
      }
    }
  }

  private[backfill] def configureHadoopStorageForPath(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    val normalized = path
    if (normalized != null && normalized.startsWith("oss://")) {
      configureHadoopOssForPath(hadoopConf, normalized, config, isSource)
    } else {
      configureHadoopS3ForPath(hadoopConf, normalized, config, isSource)
    }
  }

  private[backfill] def configureHadoopStorageForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = configureHadoopStorageForPath(
    spark.sparkContext.hadoopConfiguration,
    normalizeObjectStorageScheme(path, config),
    config,
    isSource
  )

  private[backfill] def configureHadoopOssForPath(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    if (path == null || !path.startsWith("oss://")) return
    val withoutScheme = path.stripPrefix("oss://")
    val slash = withoutScheme.indexOf('/')
    val bucket =
      if (slash < 0) withoutScheme else withoutScheme.substring(0, slash)
    if (bucket.isEmpty) return

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
    hadoopConf.set(
      "fs.oss.impl",
      "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
    )
    if (endpoint != null && endpoint.nonEmpty)
      hadoopConf.set("fs.oss.endpoint", endpoint)
    if (useSSL) hadoopConf.set("fs.oss.connection.secure.enabled", "true")
    else hadoopConf.unset("fs.oss.connection.secure.enabled")
    if (useIam) {
      val provider = Option(
        hadoopConf.getTrimmed(BackfillConfig.HadoopOssCredentialsProvider)
      ).filter(_.nonEmpty)
      if (provider.isEmpty) {
        throw new IllegalArgumentException(
          s"${BackfillConfig.HadoopOssCredentialsProvider} must be configured for OSS IAM access"
        )
      }
      hadoopConf.unset("fs.oss.accessKeyId")
      hadoopConf.unset("fs.oss.accessKeySecret")
    } else {
      hadoopConf.set("fs.oss.accessKeyId", accessKey)
      hadoopConf.set("fs.oss.accessKeySecret", secretKey)
      // hadoop-aliyun constructs AliyunCredentialsProvider itself from the
      // accessKeyId/accessKeySecret properties. The class has no (URI,
      // Configuration) or no-arg constructor, so configuring it explicitly
      // causes filesystem initialization to fail.
      hadoopConf.unset(BackfillConfig.HadoopOssCredentialsProvider)
    }
  }

  private[backfill] def configureHadoopOssForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = configureHadoopOssForPath(
    spark.sparkContext.hadoopConfiguration,
    path,
    config,
    isSource
  )

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
      hadoopConf: org.apache.hadoop.conf.Configuration,
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
      val bucketProviderKey = s"$prefix.aws.credentials.provider"
      val assumedRole =
        BackfillConfig.resolveAwsS3AssumeRole(hadoopConf, bucket)

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
      // A managed runtime may already have selected an AssumeRole provider
      // globally for its data role or per bucket for an external volume. Do
      // not replace that security boundary with the pod's ambient identity.
      if (assumedRole.isEmpty) {
        hadoopConf.set(
          bucketProviderKey,
          Seq(
            "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
          ).mkString(",")
        )
      }
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

  private[backfill] def configureHadoopS3ForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = configureHadoopS3ForPath(
    spark.sparkContext.hadoopConfiguration,
    path,
    config,
    isSource
  )

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
        snapshotPath.startsWith("s3://") || snapshotPath.startsWith(
          "s3a://"
        ) || snapshotPath.startsWith("oss://")
      ) {

        // Construct full S3 path (ensure s3a:// scheme for Hadoop)
        val s3Path = normalizeObjectStorageScheme(snapshotPath, config)

        // Configure S3 settings on Spark's Hadoop Configuration (per-bucket
        // so that snapshot bucket and backfill source bucket can use
        // different credentials in the same Spark session).
        val json = withScopedHadoopStorage(
          spark,
          s3Path,
          config,
          isSource = false
        ) {
          // Use Spark's DataFrame API to read the file (avoids Hadoop version issues)
          spark.read.text(s3Path).collect().map(_.getString(0)).mkString("\n")
        }

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
