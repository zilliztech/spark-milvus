package com.zilliz.spark.connector.table

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{
  DoubleType,
  LongType,
  MetadataBuilder,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.{Snapshot, SnapshotOrigin}
import com.zilliz.spark.connector.options.{
  MilvusOption,
  ReadMode,
  StorageOptions
}
import com.zilliz.spark.connector.read.MilvusScanBuilder
import com.zilliz.spark.connector.types.SparkTypes
import com.zilliz.spark.connector.write.MilvusV3WriteBuilder
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** One collection as one table load sees it: the [[Snapshot]] `MilvusTables`
  * resolved through `SnapshotSources`, the Spark schema derived from it, and
  * the scan builder that plans against it. Opens nothing itself.
  *
  * @param sparkSchema
  *   for a DataSource load, the schema Spark passed to `getTable`: the user's
  *   `.schema()` or the inferred one. Outside client mode it is the read
  *   schema, with the Milvus type metadata restored from the snapshot.
  */
case class MilvusTable(
    snapshot: Snapshot,
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends Table
    with SupportsRead
    with SupportsWrite {
  // Vector columns come out as stored bytes when the read asks for them raw,
  // which changes the schema, so it is read here rather than at the reader.
  private val rawVectors: Boolean = MilvusOption.readVectorRaw(
    milvusOption.options
  )
  private val readMode: ReadMode = MilvusOption.readMode(milvusOption.options)
  val fieldIDs: Seq[String] =
    if (milvusOption.fieldIDs.nonEmpty) milvusOption.fieldIDs.split(",").toSeq
    else Seq.empty
  private val selectedFieldIds: Seq[Long] =
    MilvusOption.readerFieldIds(milvusOption.options)

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // Scan options take precedence over the table's.
    val mergedOptions: ju.Map[String, String] = new ju.HashMap[String, String]()
    mergedOptions.putAll(milvusOption.options.asJava)
    mergedOptions.putAll(options)
    if (mergedOptions.get(MilvusOption.MilvusCollectionID) == null) {
      mergedOptions.put(
        MilvusOption.MilvusCollectionID,
        snapshot.collectionId.toString
      )
    }
    new MilvusScanBuilder(
      schema(),
      new CaseInsensitiveStringMap(mergedOptions),
      snapshot
    )
  }

  override def name(): String = milvusOption.collectionName

  private def rehydrateSnapshotSchemaMetadata(
      baseSchema: StructType
  ): StructType = {
    val collectionFieldByName =
      (SchemaMapper.missingSystemFields(
        snapshot.schema
      ) ++ snapshot.schema.fields)
        .map(field => field.name -> field)
        .toMap

    def conflictingMetadata(
        field: StructField,
        key: String,
        expected: Any,
        actual: Any
    ): Nothing =
      throw new IllegalArgumentException(
        s"Field '${field.name}' has $key=$actual, but snapshot schema requires $expected"
      )

    val fields = baseSchema.fields.map { field =>
      if (milvusOption.extraColumns.contains(field.name)) {
        field
      } else {
        val normalizedExtraName =
          MilvusOption.normalizeExtraColumnName(field.name)
        if (
          normalizedExtraName != field.name &&
          milvusOption.extraColumns.contains(normalizedExtraName)
        ) {
          throw new IllegalArgumentException(
            s"Field '${field.name}' is a legacy alias for metadata extra column '$normalizedExtraName'; " +
              s"use '$normalizedExtraName' in the schema or remove it and request '$normalizedExtraName' via ${MilvusOption.MilvusExtraColumns}"
          )
        }
        collectionFieldByName.get(field.name) match {
          case Some(collectionField) =>
            val expectedType = SparkTypes.toDataType(
              collectionField,
              rawVectors
            )
            if (field.dataType != expectedType) {
              throw new IllegalArgumentException(
                s"Field '${field.name}' has Spark type ${field.dataType.catalogString}, " +
                  s"but snapshot field ${collectionField.fieldID} requires ${expectedType.catalogString}"
              )
            }
            val collectionMetadata = SparkTypes.metadata(collectionField)
            val metadataBuilder = new MetadataBuilder()
              .withMetadata(field.metadata)

            Seq(
              FieldMetadata.MilvusDataTypeMetadataKey,
              FieldMetadata.MilvusFieldIdMetadataKey,
              FieldMetadata.MilvusVectorDimensionMetadataKey
            ).foreach { key =>
              if (collectionMetadata.contains(key)) {
                val expected = collectionMetadata.getLong(key)
                if (field.metadata.contains(key)) {
                  val actual = field.metadata.getLong(key)
                  if (actual != expected) {
                    conflictingMetadata(field, key, expected, actual)
                  }
                } else {
                  metadataBuilder.putLong(key, expected)
                }
              }
            }

            Seq(
              FieldMetadata.MilvusPrimaryKeyMetadataKey,
              FieldMetadata.MilvusPartitionKeyMetadataKey,
              FieldMetadata.MilvusClusteringKeyMetadataKey
            ).foreach { key =>
              val expected = collectionMetadata.contains(key) &&
                collectionMetadata.getBoolean(key)
              if (field.metadata.contains(key)) {
                val actual = field.metadata.getBoolean(key)
                if (actual != expected) {
                  conflictingMetadata(field, key, expected, actual)
                }
              } else if (expected) {
                metadataBuilder.putBoolean(key, true)
              }
            }

            field.copy(
              nullable = collectionField.nullable,
              metadata = metadataBuilder.build()
            )
          case None =>
            throw new IllegalArgumentException(
              s"Field '${field.name}' is not present in snapshot schema; available fields: ${collectionFieldByName.keys.toSeq.sorted
                  .mkString(", ")}"
            )
        }
      }
    }

    StructType(fields)
  }

  /** `fieldIDs` and an externally supplied schema describe the same physical
    * projection. Reject a mismatch instead of silently reading a different set
    * of fields from the schema Spark exposes.
    */
  private def validateExternalProjection(schema: StructType): Unit = {
    if (selectedFieldIds.isEmpty) return

    val actualIds = schema.fields
      .filterNot(field => milvusOption.extraColumns.contains(field.name))
      .map(_.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey))
      .toSeq
    val missing = selectedFieldIds.filterNot(actualIds.contains)
    val unexpected = actualIds.filterNot(selectedFieldIds.contains)
    if (missing.nonEmpty || unexpected.nonEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.ReaderFieldIDs}' and the supplied Spark schema select different fields; " +
          s"option ids=${selectedFieldIds.mkString(",")}, schema ids=${actualIds.mkString(",")}"
      )
    }
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
    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnTimestamp
      )
    ) {
      val timestamp =
        (snapshot.schema.fields ++ SchemaMapper.missingSystemFields(
          snapshot.schema
        )).find(_.fieldID == 1L)
          .getOrElse(
            throw new IllegalArgumentException(
              "Snapshot schema cannot provide Milvus timestamp field id 1"
            )
          )
      if (timestamp.dataType != MilvusDataType.Int64) {
        throw new IllegalArgumentException(
          s"Snapshot field id 1 must be the Int64 timestamp, got ${timestamp.dataType}"
        )
      }
      addIfRequested(
        MilvusOption.MilvusExtraColumnTimestamp,
        SparkTypes
          .toStructField(timestamp, rawVectors = false)
          .copy(name = MilvusOption.MilvusExtraColumnTimestamp)
      )
    }

    StructType(fields)
  }

  override def schema(): StructType = {
    // In snapshot mode with provided sparkSchema, use it directly
    // This avoids the need to parse snapshot.schema which may be incomplete
    if (
      readMode != ReadMode.Client && sparkSchema.isDefined && sparkSchema.get.nonEmpty
    ) {
      val hydrated = rehydrateSnapshotSchemaMetadata(sparkSchema.get)
      validateExternalProjection(hydrated)
      return appendExtraColumns(
        hydrated,
        rejectLegacyAliases = true
      )
    }

    // Client-based mode or snapshot mode without provided schema: derive every
    // id and type from the fixed snapshot. The two canonical system fields are
    // the only fields core may add; user and dynamic field ids are never
    // inferred from their position.
    val allSnapshotFields =
      SchemaMapper.missingSystemFields(
        snapshot.schema
      ) ++ snapshot.schema.fields
    val fieldsById = allSnapshotFields.groupBy(_.fieldID)
    val duplicateIds = fieldsById
      .collect {
        case (id, fields) if fields.size > 1 => id
      }
      .toSeq
      .sorted
    if (duplicateIds.nonEmpty) {
      throw new IllegalArgumentException(
        s"Snapshot schema contains duplicate field id(s): ${duplicateIds.mkString(", ")}"
      )
    }
    val missingIds = selectedFieldIds.filterNot(fieldsById.contains)
    if (missingIds.nonEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.ReaderFieldIDs}' requests unknown field id(s) ${missingIds
            .mkString(", ")}; " +
          s"snapshot field ids are ${fieldsById.keys.toSeq.sorted.mkString(", ")}"
      )
    }
    val snapshotFields =
      if (selectedFieldIds.isEmpty) snapshot.schema.fields
      else selectedFieldIds.map(id => fieldsById(id).head)
    val fields = snapshotFields.map(SparkTypes.toStructField(_, rawVectors))
    MilvusTables.rejectCaseInsensitiveDuplicates(fields)
    appendExtraColumns(StructType(fields), rejectLegacyAliases = false)
  }

  /** The write goes through the same snapshot's collection schema. A backup is
    * an exported, read-only copy, so a table resolved from one does not take
    * writes.
    */
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new MilvusV3WriteBuilder(
      info.schema(),
      snapshot.schema,
      milvusOption,
      () =>
        StorageOptions.writeStorageProperties(
          milvusOption.options,
          snapshot.bucket
        )
    )

  override def capabilities(): ju.Set[TableCapability] = {
    val writable = snapshot.origin match {
      case SnapshotOrigin.Backup(_) => Set.empty[TableCapability]
      case _                        => Set(TableCapability.BATCH_WRITE)
    }
    (Set[TableCapability](TableCapability.BATCH_READ) ++ writable).asJava
  }
}
