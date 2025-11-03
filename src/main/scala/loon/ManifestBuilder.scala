package com.zilliz.spark.connector.loon

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import io.milvus.grpc.schema.CollectionSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import scala.util.{Try, Success, Failure}

/**
 * Manifest data structure
 */
case class Manifest(
    @JsonProperty("column_groups") columnGroups: Seq[ColumnGroup],
    @JsonProperty("version") version: Int
)

/**
 * Column Group data structure
 */
case class ColumnGroup(
    @JsonProperty("columns") columns: Seq[String],
    @JsonProperty("format") format: String,
    @JsonProperty("paths") paths: Seq[String]
)


/**
 * Manifest Builder for Milvus Storage V2 binlog files
 *
 * Background on Milvus Storage V2 binlog structure:
 * - Binlog 0: Contains Primary Key + Clustering Key + RowID(0) + Timestamp(1)
 * - Binlog 1: Contains remaining scalar fields (if exists)
 * - Binlog N (>100): Independent binlog for specific field (usually vectors)
 *
 */
object ManifestBuilder {

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m
  }

  /**
   * Build manifest directly from schema and binlog files
   *
   * @param schema Collection schema
   * @param binlogFilesMap Binlog field ID to file paths mapping
   * @param version Manifest version (default 0)
   * @return Manifest object
   */
  def buildManifest(
      schema: CollectionSchema,
      binlogFilesMap: Map[String, Seq[String]],
      version: Int = 0
  ): Manifest = {
    // Infer field mapping from schema and binlog structure
    val fieldMapping = inferFieldMapping(schema, binlogFilesMap.keySet)

    val columnGroups = binlogFilesMap.map { case (binlogFieldID, files) =>
      val fieldIDs = fieldMapping.getOrElse(binlogFieldID, {
        Seq(binlogFieldID.toLong)
      })

      // Convert field IDs to field names
      val columns = fieldIDs.map(fieldID => getFieldNameByID(schema, fieldID))

      ColumnGroup(
        columns = columns,
        format = "parquet",
        paths = files
      )
    }.toSeq

    Manifest(
      columnGroups = columnGroups,
      version = version
    )
  }

  /**
   * Infer field mapping from schema and binlog structure
   *
   * Rules:
   * - Binlog 0: Primary Key + Clustering Key + RowID(0) + Timestamp(1)
   * - Binlog 1: Other scalar fields not in binlog 0 or independent binlogs
   * - Binlog N (>100): Independent field (usually vectors)
   *
   * @param schema Collection schema
   * @param binlogFieldIDs Set of binlog field IDs from segment info
   * @return Map of binlog field ID to list of actual Milvus field IDs
   */
  private def inferFieldMapping(
      schema: CollectionSchema,
      binlogFieldIDs: Set[String]
  ): Map[String, Seq[Long]] = {
    import scala.collection.mutable

    // Find primary key and clustering key
    val primaryKeyField = schema.fields.find(_.isPrimaryKey)
    val clusteringKeyField = schema.fields.find(_.isClusteringKey)

    // Get all fields that have their own independent binlog
    val independentBinlogFieldIDs = binlogFieldIDs
      .filter(id => id != "0" && id != "1")
      .map(_.toLong)
      .toSet

    // All scalar fields (not in independent binlogs), sorted by field ID
    val scalarFields = schema.fields
      .filterNot(f => independentBinlogFieldIDs.contains(f.fieldID))
      .sortBy(_.fieldID)

    val mapping = mutable.Map[String, Seq[Long]]()

    // Binlog 0: PK + Clustering Key + RowID + Timestamp
    if (binlogFieldIDs.contains("0")) {
      val field0Group = mutable.ArrayBuffer[Long]()
      primaryKeyField.foreach(f => field0Group += f.fieldID)
      clusteringKeyField.foreach(f => field0Group += f.fieldID)
      field0Group += 0L  // RowID
      field0Group += 1L  // Timestamp
      mapping("0") = field0Group.toSeq
    }

    // Binlog 1: Other scalar fields
    if (binlogFieldIDs.contains("1")) {
      val otherScalarFields = scalarFields
        .filterNot(_.isPrimaryKey)
        .filterNot(_.isClusteringKey)
        .map(_.fieldID)
      mapping("1") = otherScalarFields
    }

    // Independent binlogs: Each field maps to itself
    independentBinlogFieldIDs.foreach { fieldID =>
      mapping(fieldID.toString) = Seq(fieldID)
    }

    mapping.toMap
  }

  /**
   * Get field name by field ID from schema
   *
   * Special handling:
   * - Field ID 0 -> "RowID" (system field)
   * - Field ID 1 -> "Timestamp" (system field)
   * - Others -> find from schema or use default name
   */
  private def getFieldNameByID(schema: CollectionSchema, fieldID: Long): String = {
    fieldID match {
      case 0 => "RowID"
      case 1 => "Timestamp"
      case _ =>
        schema.fields
          .find(_.fieldID == fieldID)
          .map(_.name)
          .getOrElse(s"field_$fieldID")
    }
  }

  /**
   * Build manifest for a specific segment using MilvusClient to fetch segment info
   *
   * @param schema Collection schema
   * @param collectionID Collection ID
   * @param partitionID Partition ID
   * @param segmentID Segment ID
   * @param client MilvusClient instance to fetch segment info
   * @param bucket S3 bucket name
   * @param rootPath S3 root path
   * @return Manifest JSON string
   */
  def buildManifestForSegment(
      schema: CollectionSchema,
      collectionID: String,
      partitionID: String,
      segmentID: String,
      client: com.zilliz.spark.connector.MilvusClient,
      bucket: String,
      rootPath: String
  ): String = {
    val segmentInfo = client.getSegmentInfo(collectionID.toLong, segmentID.toLong)
      .getOrElse(throw new IllegalStateException(
        s"Failed to get segment info for segment $segmentID"
      ))

    if (segmentInfo.insertLogIDs.isEmpty) {
      throw new IllegalStateException(
        s"No insert logs found for segment $segmentID"
      )
    }

    val binlogFilesMap = segmentInfo.insertLogIDs
      .groupBy(_.split("/")(0))  // Group by field ID
      .map { case (fieldID, logIDs) =>
        val paths = logIDs.map { logID =>
          s"$bucket/$rootPath/insert_log/$collectionID/$partitionID/$segmentID/$logID"
        }
        fieldID -> paths
      }

    val manifest = buildManifest(
      schema,
      binlogFilesMap,
      version = 0
    )

    toJson(manifest)
  }

  /**
   * Convert Manifest to JSON string
   */
  def toJson(manifest: Manifest): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest)
  }

  /**
   * Parse Manifest from JSON string
   */
  def fromJson(json: String): Manifest = {
    mapper.readValue(json, classOf[Manifest])
  }
}
