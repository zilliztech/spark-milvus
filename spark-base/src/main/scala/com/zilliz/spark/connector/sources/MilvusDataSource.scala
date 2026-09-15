package com.zilliz.spark.connector.sources

import java.{util => ju}

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.snapshot.{SnapshotCatalog, V2SegmentResolver}
import com.zilliz.milvus.storage.snapshot.json.SnapshotJson
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}
import com.zilliz.spark.connector.table.MilvusTable
import com.zilliz.spark.connector.table.SnapshotSparkSchema
import com.zilliz.spark.connector.types.SparkTypes
import io.milvus.grpc.schema.CollectionSchema

case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val milvusOption = MilvusOption(options)
    MilvusOption.validateSnapshotModeOptions(options)
    MilvusOption.validateBackupModeOptions(options)
    val isSnapshotMode = MilvusOption.isSnapshotMode(options)
    val isBackupMode = MilvusOption.isBackupMode(options)
    if (milvusOption.uri.isEmpty && !isSnapshotMode && !isBackupMode) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusUri}' is required for reading milvus data."
      )
    }
    MilvusTable(
      milvusOption,
      Some(schema)
    )
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = MilvusOption(options)
    val rawVectors = MilvusOption.readVectorRaw(options)

    // Check for snapshot mode - use snapshot schema if provided
    MilvusOption.validateSnapshotModeOptions(options)
    MilvusOption.validateBackupModeOptions(options)
    val isSnapshotMode = MilvusOption.isSnapshotMode(options)
    val isBackupMode = MilvusOption.isBackupMode(options)

    if (isSnapshotMode) {
      Option(options.get(MilvusOption.SnapshotPath))
        .map(_.trim)
        .filter(_.nonEmpty)
        .map { path =>
          // The snapshot JSON is the schema's source; V2 segments are not
          // materialized for that.
          val bucket =
            StorageOptions.resolveConnectorS3Bucket(milvusOption.options)
          val store = StorageOptions.storeFor(
            StorageOptions.buildHadoopConfForOptions(milvusOption.options, ""),
            bucket,
            milvusOption.options
          )
          val snapshot = new SnapshotCatalog(
            store,
            bucket,
            V2SegmentResolver.Skipped
          ).read(path)
          sparkSchemaOf(snapshot.schema, rawVectors)
        }
        .orElse {
          // The 1.x form: the schema JSON travels in an option.
          Option(options.get(MilvusOption.SnapshotSchemaJson)).flatMap { json =>
            SnapshotJson.parse(json) match {
              case Right(metadata) =>
                Some(
                  SnapshotSparkSchema.toSparkSchema(
                    metadata.collection.schema,
                    includeSystemFields = true
                  )
                )
              case Left(_) => None
            }
          }
        }
        .getOrElse {
          // If no snapshot schema provided, return empty schema
          // The actual schema should be provided via .schema() call
          StructType(Seq.empty)
        }
    } else if (isBackupMode) {
      // Backup mode is fully offline: the schema is materialized from the
      // backup's full_meta.json when the read plans its partitions. Return an
      // empty schema here; callers supply the real schema via .schema().
      StructType(Seq.empty)
    } else {
      // Client-based mode (existing behavior)
      if (milvusOption.collectionName.isEmpty) {
        throw new IllegalArgumentException("collectionName cannot be empty")
      }
      val client = MilvusClient(milvusOption.connectionParams)
      try {
        val result = client.getCollectionSchema(
          milvusOption.databaseName,
          milvusOption.collectionName
        )
        val schema = result.getOrElse(
          throw new Exception(
            s"Failed to get collection schema: ${result.failed.get.getMessage}"
          )
        )
        sparkSchemaOf(schema, rawVectors)
      } finally {
        client.close()
      }
    }
  }

  /** The Spark schema of a protobuf collection schema, one column per field. */
  private def sparkSchemaOf(
      schema: CollectionSchema,
      rawVectors: Boolean
  ): StructType =
    StructType(
      schema.fields.map(field =>
        StructField(
          field.name,
          SparkTypes.toDataType(field, rawVectors),
          field.nullable,
          SparkTypes.metadata(field)
        )
      )
    )
  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}
