package com.zilliz.spark.connector.table

import java.{util => ju}
import java.util.Base64
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{
  LongType,
  MetadataBuilder,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.api.{MilvusClient, MilvusCollectionInfo}
import com.zilliz.milvus.storage.compat.backup.BackupMetaReader
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.MilvusSnapshotReader
import com.zilliz.spark.connector.options.{
  BackupSelection,
  ReadMode,
  StorageOptions
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.scan.MilvusScanBuilder
import com.zilliz.spark.connector.types.DataTypeUtil
import com.zilliz.spark.connector.write.MilvusWriteBuilder
import io.milvus.grpc.schema.CollectionSchema

case class MilvusTable(
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends Table
    with SupportsWrite
    with SupportsRead
    with Logging {
  var milvusCollection: MilvusCollectionInfo = _
  var partitionID: Long = 0L
  // Vector columns come out as stored bytes when the read asks for them raw,
  // which changes the schema, so it is read here rather than at the reader.
  private val rawVectors: Boolean = MilvusOption.readVectorRaw(
    milvusOption.options
  )
  // full_meta.json parsed during initFromBackup, threaded directly to the scan
  // planner (never through options, so it is neither re-serialized nor shipped
  // to executors).
  private var parsedBackupMeta: Option[BackupMetaReader.BackupInfo] = None
  private val readMode: ReadMode = MilvusOption.readMode(milvusOption.options)
  initInfo()
  val fieldIDs: Seq[String] =
    if (milvusOption.fieldIDs.nonEmpty) {
      milvusOption.fieldIDs.split(",").toSeq
    } else {
      Seq[String]()
    }
  logInfo(s"MilvusTable fieldIDs: $fieldIDs")

  def initInfo(): Unit =
    readMode match {
      case ReadMode.Snapshot =>
        logInfo(
          "Snapshot mode enabled - skipping Milvus client connection for collection info"
        )
        initFromSnapshot()
      case ReadMode.Backup =>
        logInfo(
          "Backup mode enabled - skipping Milvus client connection for collection info"
        )
        initFromBackup()
      case ReadMode.Client => initFromClient()
    }

  /** Initialize collection info from a milvus-backup export (no client
    * connection). The collection schema is materialized from the backup's
    * `full_meta.json` so downstream metadata rehydration (e.g. Milvus data type
    * / vector dimension on Spark fields) works identically to snapshot mode.
    * The collection is selected by `milvus.database.name` +
    * `milvus.collection.name`, and its id is read from the meta.
    *
    * When no `.schema()` is provided, the backup meta is the only source of the
    * Spark schema, so a read failure (or a missing collection / schema) fails
    * loudly rather than degrading to a RowID/Timestamp-only schema that
    * silently drops user columns. With an explicit `.schema()` the meta read is
    * best-effort (it only feeds metadata rehydration).
    */
  private def initFromBackup(): Unit = {
    partitionID = 0L

    // Without an explicit .schema(), the Spark schema is derived from the
    // backup meta; failing to read it must surface loudly rather than degrade
    // to a RowID/Timestamp-only schema that silently drops every user column.
    val needsSchema = !sparkSchema.exists(_.nonEmpty)

    val parsedMeta: Option[
      (BackupMetaReader.BackupInfo, BackupMetaReader.CollectionBackup)
    ] =
      MilvusOption.backupDir(milvusOption.options).flatMap { dir =>
        val conf =
          StorageOptions.buildHadoopConfForOptions(milvusOption.options, dir)
        val maxBytes = StorageOptions.backupMaxJsonBytes(
          new CaseInsensitiveStringMap(milvusOption.options.asJava)
        )
        BackupMetaReader
          .readMeta(
            StorageOptions.storeFor(
              conf,
              StorageOptions.snapshotBucket(dir).getOrElse(""),
              milvusOption.options
            ),
            dir,
            maxBytes
          ) match {
          case Left(err) =>
            if (needsSchema) {
              throw new IllegalArgumentException(
                s"Failed to read backup meta at ${BackupMetaReader
                    .metaPath(dir)} (required because no .schema() was " +
                  s"provided): ${err.getMessage}",
                err
              )
            } else {
              logWarning(
                s"Could not read backup meta at table init: ${err.getMessage}"
              )
              None
            }
          case Right(meta) =>
            BackupSelection.resolveBackupCollection(
              meta,
              milvusOption.databaseName,
              milvusOption.collectionName
            ) match {
              case Right(coll) => Some((meta, coll))
              case Left(msg) =>
                if (needsSchema) {
                  throw new IllegalArgumentException(msg)
                } else {
                  logWarning(
                    s"Backup collection resolution failed at table init: $msg"
                  )
                  None
                }
            }
        }
      }
    parsedBackupMeta = parsedMeta.map(_._1)

    val parsedCollection = parsedMeta.map(_._2)
    val collectionId = parsedCollection.map(_.collectionId).getOrElse(0L)

    val schemaFromMeta = parsedCollection.flatMap(_.schema).map { s =>
      BackupMetaReader.validateDynamicFieldSchema(s)
      CollectionSchema.parseFrom(BackupMetaReader.toProtobufSchemaBytes(s))
    }

    if (needsSchema && parsedCollection.nonEmpty && schemaFromMeta.isEmpty) {
      throw new IllegalArgumentException(
        s"Backup collection '${parsedCollection.get.collectionName}' has no " +
          "schema; cannot derive a read schema without .schema()"
      )
    }

    schemaFromMeta match {
      case Some(schema) =>
        logInfo(
          s"Initialized from backup: collectionID=$collectionId, " +
            s"schema='${schema.name}' with ${schema.fields.size} fields"
        )
      case None =>
        logWarning(
          "Could not materialize collection schema from backup meta; " +
            "relying on the caller-provided Spark schema"
        )
    }

    milvusCollection = MilvusCollectionInfo(
      dbName = milvusOption.databaseName,
      collectionName = milvusOption.collectionName,
      collectionID = collectionId,
      schema = schemaFromMeta.getOrElse(createMinimalCollectionSchema(None))
    )
  }

  /** Initialize collection info from snapshot metadata (no client connection)
    */
  private def initFromSnapshot(): Unit = {
    // Get collection ID from options
    val collectionId = milvusOption.options
      .get(MilvusOption.SnapshotCollectionId)
      .map(_.toLong)
      .getOrElse(0L)

    // Get partition IDs from options
    val partitionIds = milvusOption.options
      .get(MilvusOption.SnapshotPartitionIds)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).map(_.toLong).toSeq)
      .getOrElse(Seq.empty[Long])

    // Use first partition ID if available
    partitionID = partitionIds.headOption.getOrElse(0L)

    val snapshotSchema = milvusOption.options
      .get(MilvusOption.SnapshotSchemaJson)
      .map(parseSnapshotSchemaJson)
      .orElse {
        milvusOption.options
          .get(MilvusOption.SnapshotSchemaBytes)
          .map(parseSnapshotSchemaBytes)
      }

    // Create a minimal MilvusCollectionInfo
    // For snapshot mode, we use the passed-in sparkSchema for actual schema operations
    milvusCollection = MilvusCollectionInfo(
      dbName = milvusOption.databaseName,
      collectionName = milvusOption.collectionName,
      collectionID = collectionId,
      schema = createMinimalCollectionSchema(snapshotSchema)
    )

    logInfo(
      s"Initialized from snapshot: collectionID=$collectionId, partitionID=$partitionID"
    )
  }

  /** Create a minimal CollectionSchema for snapshot mode This is used when we
    * have snapshot data but need a protobuf schema structure
    */
  private def parseSnapshotSchemaJson(json: String): CollectionSchema = {
    MilvusSnapshotReader.parseSnapshotMetadata(json) match {
      case Right(metadata) =>
        CollectionSchema.parseFrom(
          MilvusSnapshotReader.toProtobufSchemaBytes(metadata.collection.schema)
        )
      case Left(err) =>
        throw new IllegalArgumentException(
          s"Failed to parse ${MilvusOption.SnapshotSchemaJson}: $err"
        )
    }
  }

  private def parseSnapshotSchemaBytes(base64: String): CollectionSchema = {
    try {
      CollectionSchema.parseFrom(Base64.getDecoder.decode(base64))
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Failed to parse ${MilvusOption.SnapshotSchemaBytes}: ${e.getMessage}",
          e
        )
    }
  }

  private def createMinimalCollectionSchema(
      snapshotSchema: Option[CollectionSchema]
  ): CollectionSchema = {
    snapshotSchema.getOrElse {
      CollectionSchema(
        name = milvusOption.collectionName,
        fields = Seq.empty
      )
    }
  }

  /** Initialize collection info from Milvus client (existing behavior)
    */
  private def initFromClient(): Unit = {
    val client = MilvusClient(milvusOption.connectionParams)
    try {
      milvusCollection = client
        .getCollectionInfo(
          milvusOption.databaseName,
          milvusOption.collectionName
        )
        .getOrElse(
          throw new Exception(
            s"Collection ${milvusOption.collectionName} not found"
          )
        )
      if (milvusOption.partitionName.nonEmpty) {
        partitionID = client
          .getPartitionID(
            milvusOption.databaseName,
            milvusOption.collectionName,
            milvusOption.partitionName
          )
          .getOrElse(
            throw new Exception(
              s"Partition ${milvusOption.partitionName} not found"
            )
          )
      }
    } finally {
      client.close()
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    MilvusWriteBuilder(milvusOption, info)
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // Merge table properties with scan options. Scan options take precedence.
    val mergedOptions: ju.Map[String, String] = new ju.HashMap[String, String]()
    mergedOptions.putAll(properties)
    mergedOptions.putAll(options)
    if (mergedOptions.get(MilvusOption.MilvusCollectionID) == null) {
      mergedOptions.put(
        MilvusOption.MilvusCollectionID,
        milvusCollection.collectionID.toString
      )
    }
    if (partitionID != 0L) {
      mergedOptions.put(
        MilvusOption.MilvusPartitionID,
        partitionID.toString
      )
    }

    val allOptions = new CaseInsensitiveStringMap(mergedOptions)
    new MilvusScanBuilder(schema(), allOptions, parsedBackupMeta)
  }

  override def name(): String = milvusOption.collectionName

  private def rehydrateSnapshotSchemaMetadata(
      baseSchema: StructType
  ): StructType = {
    val collectionFieldByName = milvusCollection.schema.fields.map { field =>
      field.name -> field
    }.toMap

    val fields = baseSchema.fields.map { field =>
      if (milvusOption.extraColumns.contains(field.name)) {
        field
      } else {
        collectionFieldByName.get(field.name) match {
          case Some(collectionField) =>
            val collectionMetadata = DataTypeUtil.metadata(collectionField)
            val metadataBuilder = new MetadataBuilder()
              .withMetadata(field.metadata)

            if (
              !field.metadata.contains(FieldMetadata.MilvusDataTypeMetadataKey)
            ) {
              metadataBuilder.putLong(
                FieldMetadata.MilvusDataTypeMetadataKey,
                collectionMetadata.getLong(
                  FieldMetadata.MilvusDataTypeMetadataKey
                )
              )
            }
            val existingTypeMatchesCollection =
              !field.metadata.contains(
                FieldMetadata.MilvusDataTypeMetadataKey
              ) || field.metadata.getLong(
                FieldMetadata.MilvusDataTypeMetadataKey
              ) == collectionMetadata.getLong(
                FieldMetadata.MilvusDataTypeMetadataKey
              )
            if (
              collectionMetadata.contains(
                FieldMetadata.MilvusVectorDimensionMetadataKey
              ) && !field.metadata.contains(
                FieldMetadata.MilvusVectorDimensionMetadataKey
              ) && existingTypeMatchesCollection
            ) {
              metadataBuilder.putLong(
                FieldMetadata.MilvusVectorDimensionMetadataKey,
                collectionMetadata.getLong(
                  FieldMetadata.MilvusVectorDimensionMetadataKey
                )
              )
            }

            field.copy(metadata = metadataBuilder.build())
          case None =>
            field
        }
      }
    }

    StructType(fields)
  }

  private def appendExtraColumns(
      baseSchema: StructType,
      rejectLegacyAliases: Boolean
  ): StructType = {
    var fields = baseSchema.fields.toSeq

    def failIfPresent(alias: String, canonical: String): Unit = {
      if (rejectLegacyAliases && fields.exists(_.name == alias)) {
        throw new IllegalArgumentException(
          s"Field '$alias' is a legacy alias for metadata extra column '$canonical'; use '$canonical' in the schema or remove it and request '$canonical' via ${MilvusOption.MilvusExtraColumns}"
        )
      }
    }

    def addIfRequested(name: String, field: StructField): Unit = {
      if (milvusOption.extraColumns.contains(name)) {
        if (fields.exists(_.name == name)) {
          throw new IllegalArgumentException(
            s"Requested metadata extra column '$name' conflicts with an existing field named '$name'"
          )
        }
        fields = fields :+ field
      }
    }

    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnSegmentID
      )
    ) {
      failIfPresent(
        MilvusOption.MilvusExtraColumnSegmentIDAlias,
        MilvusOption.MilvusExtraColumnSegmentID
      )
    }
    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnRowOffset
      )
    ) {
      failIfPresent(
        MilvusOption.MilvusExtraColumnRowOffsetAlias,
        MilvusOption.MilvusExtraColumnRowOffset
      )
    }

    addIfRequested(
      MilvusOption.MilvusExtraColumnPartition,
      StructField(
        MilvusOption.MilvusExtraColumnPartition,
        StringType,
        nullable = true
      )
    )
    addIfRequested(
      MilvusOption.MilvusExtraColumnSegmentID,
      StructField(
        MilvusOption.MilvusExtraColumnSegmentID,
        LongType,
        nullable = false
      )
    )
    addIfRequested(
      MilvusOption.MilvusExtraColumnRowOffset,
      StructField(
        MilvusOption.MilvusExtraColumnRowOffset,
        LongType,
        nullable = false
      )
    )

    StructType(fields)
  }

  override def schema(): StructType = {
    // In snapshot mode with provided sparkSchema, use it directly
    // This avoids the need to parse milvusCollection.schema which may be incomplete
    if (
      readMode != ReadMode.Client && sparkSchema.isDefined && sparkSchema.get.nonEmpty
    ) {
      logInfo(
        s"Using provided sparkSchema in snapshot mode: ${sparkSchema.get.fieldNames.mkString(", ")}"
      )
      return appendExtraColumns(
        rehydrateSnapshotSchemaMetadata(sparkSchema.get),
        rejectLegacyAliases = true
      )
    }

    // Client-based mode or snapshot mode without provided schema: compute from milvusCollection
    var fields = Seq[StructField]()
    val fieldName2ID = mutable.Map[String, Long]()
    milvusCollection.schema.fields.zipWithIndex.foreach { case (field, index) =>
      fieldName2ID(field.name) = if (field.fieldID == 0) {
        index + 100
      } else {
        field.fieldID
      }
    }
    val missingSystemFields = SchemaMapper
      .missingSystemFields(milvusCollection.schema)
      .map(_.fieldID)
      .toSet
    if (
      missingSystemFields.contains(0L) &&
      (fieldIDs.isEmpty || fieldIDs.contains("0"))
    ) {
      fields = fields :+ StructField("RowID", LongType, nullable = false)
    }
    if (
      missingSystemFields.contains(1L) &&
      (fieldIDs.isEmpty || fieldIDs.contains("1"))
    ) {
      fields = fields :+ StructField("Timestamp", LongType, nullable = false)
    }
    val filteredFields = milvusCollection.schema.fields
      .filter(field =>
        fieldIDs.isEmpty || fieldIDs.contains(fieldName2ID(field.name).toString)
      )
    fields = fields ++ filteredFields.map(field =>
      StructField(
        field.name,
        DataTypeUtil.toDataType(field, rawVectors),
        field.nullable,
        DataTypeUtil.metadata(field)
      )
    )
    // Safely get maxFieldID, default to 100 if empty
    val maxFieldID =
      if (fieldName2ID.values.nonEmpty) fieldName2ID.values.max else 100L
    // Only append $meta if the schema loop did not already emit it (backup
    // mode materializes $meta from the meta, unlike client mode where
    // DescribeCollection omits dynamic fields).
    val alreadyHasMeta = fields.exists(_.name == "$meta")
    if (
      !alreadyHasMeta &&
      milvusCollection.schema.enableDynamicField &&
      (fieldIDs.isEmpty || fieldIDs.contains((maxFieldID + 1).toString))
    ) {
      fields = fields :+ StructField("$meta", StringType, nullable = true)
    }
    appendExtraColumns(StructType(fields), rejectLegacyAliases = false)
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE,
      TableCapability.BATCH_READ
    ).asJava
  }
}
