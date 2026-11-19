package com.zilliz.spark.connector.table

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{
  MetadataColumn,
  SupportsMetadataColumns,
  SupportsRead,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{
  DataType,
  MetadataBuilder,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.{MetadataColumns, MilvusScanBuilder}
import com.zilliz.spark.connector.types.SparkTypes

/** One collection as one table load sees it: the [[Snapshot]] `MilvusTables`
  * resolved through `SnapshotSources`, the Spark schema derived from it, and
  * the scan builder that plans against it. Opens nothing itself.
  *
  * @param sparkSchema
  *   for a DataSource load, the schema Spark passed to `getTable`: the user's
  *   `.schema()` or the inferred one. It is the read schema, with the Milvus
  *   type metadata restored from the snapshot.
  */
case class MilvusTable(
    snapshot: Snapshot,
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends Table
    with SupportsRead
    with SupportsMetadataColumns {
  // Vector columns come out as stored bytes when the read asks for them raw,
  // which changes the schema, so it is read here rather than at the reader.
  private val rawVectors: Boolean = MilvusOption.readVectorRaw(
    milvusOption.options
  )
  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder =
    new MilvusScanBuilder(schema(), readOptions(options), snapshot)

  /** The options one read of this table runs with: the read's own over the
    * table's, a key the read gives replacing the table's in any case, and the
    * option names are compared case-insensitively.
    */
  def readOptions(
      options: CaseInsensitiveStringMap
  ): CaseInsensitiveStringMap = {
    val merged: ju.Map[String, String] = new ju.HashMap[String, String]()
    milvusOption.options.foreach { case (key, value) =>
      if (!options.containsKey(key)) merged.put(key, value)
    }
    merged.putAll(options)
    new CaseInsensitiveStringMap(merged)
  }

  override def name(): String = snapshot.schema.name

  override def metadataColumns(): Array[MetadataColumn] = {
    MetadataColumns
      .hiddenFields(
        schema(),
        snapshot.schema,
        physicalRead = MilvusOption.isPhysicalRead(milvusOption.options)
      )
      .map { field =>
        new MetadataColumn {
          override def name(): String = field.name
          override def dataType(): DataType = field.dataType
          override def isNullable(): Boolean = field.nullable
        }
      }
      .toArray
  }

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

    StructType(fields)
  }

  override def schema(): StructType =
    sparkSchema
      .filter(_.nonEmpty)
      .map(rehydrateSnapshotSchemaMetadata)
      .getOrElse(MilvusTables.sparkSchemaOf(snapshot.schema, rawVectors))

  override def capabilities(): ju.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava
}
