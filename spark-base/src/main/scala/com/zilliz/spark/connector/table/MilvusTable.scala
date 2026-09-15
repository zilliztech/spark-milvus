package com.zilliz.spark.connector.table

import java.{util => ju}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{
  LongType,
  MetadataBuilder,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.options.{MilvusOption, ReadMode}
import com.zilliz.spark.connector.read.MilvusScanBuilder
import com.zilliz.spark.connector.types.SparkTypes

/** One collection as one read sees it: the [[Snapshot]] `getTable` resolved
  * through `SnapshotSources`, the Spark schema derived from it, and the scan
  * builder that plans against it. Opens nothing itself.
  *
  * @param sparkSchema
  *   the schema Spark passed to `getTable`: the user's `.schema()` or the
  *   inferred one. Outside client mode it is the read schema, with the Milvus
  *   type metadata restored from the snapshot.
  */
case class MilvusTable(
    snapshot: Snapshot,
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends Table
    with SupportsRead {
  // Vector columns come out as stored bytes when the read asks for them raw,
  // which changes the schema, so it is read here rather than at the reader.
  private val rawVectors: Boolean = MilvusOption.readVectorRaw(
    milvusOption.options
  )
  private val readMode: ReadMode = MilvusOption.readMode(milvusOption.options)
  val fieldIDs: Seq[String] =
    if (milvusOption.fieldIDs.nonEmpty) milvusOption.fieldIDs.split(",").toSeq
    else Seq.empty

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
    val collectionFieldByName = snapshot.schema.fields.map { field =>
      field.name -> field
    }.toMap

    val fields = baseSchema.fields.map { field =>
      if (milvusOption.extraColumns.contains(field.name)) {
        field
      } else {
        collectionFieldByName.get(field.name) match {
          case Some(collectionField) =>
            val collectionMetadata = SparkTypes.metadata(collectionField)
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
    // This avoids the need to parse snapshot.schema which may be incomplete
    if (
      readMode != ReadMode.Client && sparkSchema.isDefined && sparkSchema.get.nonEmpty
    ) {
      return appendExtraColumns(
        rehydrateSnapshotSchemaMetadata(sparkSchema.get),
        rejectLegacyAliases = true
      )
    }

    // Client-based mode or snapshot mode without provided schema: compute from milvusCollection
    var fields = Seq[StructField]()
    val fieldName2ID = mutable.Map[String, Long]()
    snapshot.schema.fields.zipWithIndex.foreach { case (field, index) =>
      fieldName2ID(field.name) = if (field.fieldID == 0) {
        index + 100
      } else {
        field.fieldID
      }
    }
    val missingSystemFields = SchemaMapper
      .missingSystemFields(snapshot.schema)
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
    val filteredFields = snapshot.schema.fields
      .filter(field =>
        fieldIDs.isEmpty || fieldIDs.contains(fieldName2ID(field.name).toString)
      )
    fields = fields ++ filteredFields.map(field =>
      StructField(
        field.name,
        SparkTypes.toDataType(field, rawVectors),
        field.nullable,
        SparkTypes.metadata(field)
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
      snapshot.schema.enableDynamicField &&
      (fieldIDs.isEmpty || fieldIDs.contains((maxFieldID + 1).toString))
    ) {
      fields = fields :+ StructField("$meta", StringType, nullable = true)
    }
    appendExtraColumns(StructType(fields), rejectLegacyAliases = false)
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_READ
    ).asJava
  }
}
