package com.zilliz.spark.connector.read

import com.fasterxml.jackson.annotation.{JsonAlias, JsonProperty}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.cfg.CoercionAction
import com.fasterxml.jackson.databind.cfg.CoercionInputShape
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.spark.sql.types._

/**
 * Helper object for converting JSON values that may be either numeric or string to Long/Int
 * Milvus snapshot JSON format may serialize numbers as strings in some versions
 */
private[read] object JsonTypeConverter {
  def toLong(value: Any): Long = value match {
    case l: Long => l
    case i: Int => i.toLong
    case n: Number => n.longValue()
    case s: String => s.toLong
    case node: JsonNode if node.isNumber => node.asLong()
    case node: JsonNode if node.isTextual => node.asText().toLong
    case _ => 0L
  }

  def toInt(value: Any): Int = value match {
    case i: Int => i
    case l: Long => l.toInt
    case n: Number => n.intValue()
    case s: String => s.toInt
    case node: JsonNode if node.isNumber => node.asInt()
    case node: JsonNode if node.isTextual => node.asText().toInt
    case _ => 0
  }

  def toOptionalInt(value: Option[Any]): Option[Int] = value.map {
    case i: Int => i
    case l: Long => l.toInt
    case n: Number => n.intValue()
    case s: String => s.toInt
    case node: JsonNode if node.isNumber => node.asInt()
    case node: JsonNode if node.isTextual => node.asText().toInt
    case _ => 0
  }

  def toLongSeq(value: Any): Seq[Long] = value match {
    case seq: Seq[_] => seq.map(toLong)
    case node: JsonNode if node.isArray =>
      import scala.jdk.CollectionConverters._
      node.elements().asScala.map(n => toLong(n)).toSeq
    case _ => Seq.empty
  }

  /**
   * Convert a JsonNode or Any value to a data type code.
   * Handles both numeric values (e.g., 5) and string type names (e.g., "Int64")
   */
  def toDataTypeCode(value: Any): Int = value match {
    case i: Int => i
    case l: Long => l.toInt
    case n: Number => n.intValue()
    case s: String =>
      // Try parsing as number first, then as type name
      try {
        s.toInt
      } catch {
        case _: NumberFormatException => Field.dataTypeNameToCode(s)
      }
    case node: JsonNode if node.isNumber => node.asInt()
    case node: JsonNode if node.isTextual =>
      val text = node.asText()
      try {
        text.toInt
      } catch {
        case _: NumberFormatException => Field.dataTypeNameToCode(text)
      }
    case _ => 0
  }
}

/**
 * Type parameter for Milvus field
 */
case class TypeParam(
    @JsonProperty("key") key: String,
    @JsonProperty("value") value: String
)

/**
 * Field schema definition
 */
case class Field(
    @JsonProperty("fieldID") fieldID: Option[JsonNode] = None,  // Can be Int or Long from JSON
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("data_type") rawDataType: Option[JsonNode] = None,  // Can be Int or String (e.g., 5 or "Int64")
    @JsonProperty("is_primary_key") isPrimaryKey: Option[Boolean] = None,
    @JsonProperty("is_clustering_key") isClusteringKey: Option[Boolean] = None,
    @JsonProperty("type_params") typeParams: Option[Seq[TypeParam]] = None
) {
  /**
   * Get the data type as Int, handling both numeric and string formats
   * String format examples: "Bool", "Int8", "Int16", "Int32", "Int64", "Float", "Double",
   *                         "String", "VarChar", "JSON", "Array", "FloatVector", etc.
   */
  def dataType: Int = rawDataType.map(JsonTypeConverter.toDataTypeCode).getOrElse(0)

  def getTypeParam(key: String): Option[String] = {
    typeParams.flatMap(_.find(_.key == key).map(_.value))
  }

  def getFieldIDAsLong: Long = {
    fieldID match {
      case Some(node) => JsonTypeConverter.toLong(node)
      case _ => 0L
    }
  }
}

object Field {
  /**
   * Mapping from data type name strings to numeric codes
   * Based on Milvus DataType enum values
   */
  private val dataTypeNameToCodeMap: Map[String, Int] = Map(
    "None" -> 0,
    "Bool" -> 1,
    "Int8" -> 2,
    "Int16" -> 3,
    "Int32" -> 4,
    "Int64" -> 5,
    "Float" -> 10,
    "Double" -> 11,
    "String" -> 20,
    "VarChar" -> 21,
    "JSON" -> 22,
    "Array" -> 23,
    // Array element types (23-30 based on element type)
    "ArrayBool" -> 23,
    "ArrayInt8" -> 24,
    "ArrayInt16" -> 25,
    "ArrayInt32" -> 26,
    "ArrayInt64" -> 27,
    "ArrayFloat" -> 28,
    "ArrayDouble" -> 29,
    "ArrayVarChar" -> 30,
    // Vector types
    "BinaryVector" -> 100,
    "FloatVector" -> 101,
    "Float16Vector" -> 102,
    "BFloat16Vector" -> 103,
    "SparseFloatVector" -> 104
  )

  def dataTypeNameToCode(name: String): Int = {
    dataTypeNameToCodeMap.getOrElse(name, 0)
  }
}

/**
 * Property key-value pair
 */
case class Property(
    @JsonProperty("key") key: String,
    @JsonProperty("value") value: String
)

/**
 * Collection schema definition
 */
case class CollectionSchema(
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("fields") fields: Seq[Field],
    @JsonProperty("properties") properties: Option[Seq[Property]] = None
) {
  def getFieldByName(name: String): Option[Field] = {
    fields.find(_.name == name)
  }
}

/**
 * Collection metadata
 */
case class Collection(
    @JsonProperty("schema") schema: CollectionSchema,
    @JsonProperty("num_partitions") rawNumPartitions: Option[JsonNode] = None,  // Can be Int or String
    @JsonProperty("num_shards") rawNumShards: Option[JsonNode] = None,  // Can be Int or String
    @JsonProperty("properties") properties: Option[Seq[Property]] = None,
    @JsonProperty("consistency_level") rawConsistencyLevel: Option[JsonNode] = None  // Can be Int or String
) {
  def numPartitions: Option[Int] = rawNumPartitions.map(n => JsonTypeConverter.toInt(n))
  def numShards: Option[Int] = rawNumShards.map(n => JsonTypeConverter.toInt(n))
  def consistencyLevel: Option[Int] = rawConsistencyLevel.map(n => JsonTypeConverter.toInt(n))
}

/**
 * Snapshot information
 */
case class SnapshotInfo(
    @JsonProperty("name") name: String,
    @JsonProperty("id") rawId: Option[JsonNode] = None,  // Can be Long or String from JSON
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("collection_id") rawCollectionId: Option[JsonNode] = None,  // Can be Long or String from JSON
    @JsonProperty("partition_ids") rawPartitionIds: Option[JsonNode] = None,  // Can be Seq[Long] or Seq[String] from JSON
    @JsonProperty("create_ts") rawCreateTs: Option[JsonNode] = None,  // Can be Long or String from JSON
    @JsonProperty("state") state: Option[String] = None,  // New field: snapshot state
    @JsonProperty("pending_start_time") pendingStartTime: Option[JsonNode] = None  // New field
) {
  def id: Long = rawId.map(JsonTypeConverter.toLong).getOrElse(0L)
  def collectionId: Long = rawCollectionId.map(JsonTypeConverter.toLong).getOrElse(0L)
  def partitionIds: Seq[Long] = rawPartitionIds.map(JsonTypeConverter.toLongSeq).getOrElse(Seq.empty)
  def createTs: Long = rawCreateTs.map(JsonTypeConverter.toLong).getOrElse(0L)
}

/**
 * Parsed manifest content from Storage V2
 */
case class ManifestContent(
    @JsonProperty("ver") ver: Int,
    @JsonProperty("base_path") basePath: String
)

/**
 * Storage V2 manifest item
 */
case class StorageV2ManifestItem(
    @JsonProperty("segmentID") rawSegmentID: Option[JsonNode] = None,  // Can be Long or String from JSON
    @JsonProperty("manifest") manifest: String = "",
    @JsonProperty("segmentIDLong") rawSegmentIDLong: Option[JsonNode] = None  // For serialization (using JsonNode to handle Int/Long)
) {
  def segmentID: Long = {
    // First try rawSegmentIDLong (used when creating new items for serialization)
    // Then fall back to rawSegmentID (from original JSON)
    rawSegmentIDLong.map(JsonTypeConverter.toLong)
      .orElse(rawSegmentID.map(JsonTypeConverter.toLong))
      .getOrElse(0L)
  }

  /**
   * Parse the manifest JSON string to extract structured content
   * @return Either containing parsed ManifestContent or error
   */
  private[read] def parseManifest(mapper: ObjectMapper with ScalaObjectMapper): Either[Throwable, ManifestContent] = {
    try {
      Right(mapper.readValue[ManifestContent](manifest))
    } catch {
      case e: Exception => Left(e)
    }
  }
}

object StorageV2ManifestItem {
  import com.fasterxml.jackson.databind.node.LongNode

  /**
   * Create a StorageV2ManifestItem with a Long segmentID (for serialization)
   */
  def apply(segmentID: Long, manifest: String): StorageV2ManifestItem = {
    new StorageV2ManifestItem(None, manifest, Some(LongNode.valueOf(segmentID)))
  }
}

/**
 * Complete snapshot metadata
 */
case class SnapshotMetadata(
    @JsonProperty("snapshot_info") @JsonAlias(Array("snapshot-info")) snapshotInfo: SnapshotInfo,
    @JsonProperty("collection") collection: Collection,
    @JsonProperty("format_version") formatVersion: Option[Int] = None,
    @JsonProperty("indexes") indexes: Seq[Any] = Seq.empty,
    @JsonProperty("manifest_list") @JsonAlias(Array("manifest-list")) manifestList: Seq[String] = Seq.empty,
    @JsonProperty("storagev2_manifest_list") @JsonAlias(Array("storagev2-manifest-list")) storageV2ManifestList: Option[Seq[StorageV2ManifestItem]] = None
)

/**
 * Reader for Milvus snapshot metadata JSON files
 */
object MilvusSnapshotReader {

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
    // Allow coercion from String to numeric types (e.g., "5" -> 5, "123456789" -> 123456789L)
    m.coercionConfigFor(classOf[java.lang.Integer]).setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[java.lang.Long]).setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[Int]).setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[Long]).setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m
  }

  /**
   * Parse snapshot metadata from JSON string
   *
   * @param json JSON string containing snapshot metadata
   * @return Either containing parsed SnapshotMetadata or error
   */
  def parseSnapshotMetadata(json: String): Either[Throwable, SnapshotMetadata] = {
    try {
      Right(mapper.readValue[SnapshotMetadata](json))
    } catch {
      case e: Exception => Left(e)
    }
  }

  /**
   * Read snapshot metadata from file
   *
   * @param path Path to the snapshot metadata JSON file
   * @return Either containing parsed SnapshotMetadata or error
   */
  def readSnapshotMetadataFromFile(path: String): Either[Throwable, SnapshotMetadata] = {
    try {
      val source = scala.io.Source.fromFile(path)
      try {
        val json = source.mkString
        parseSnapshotMetadata(json)
      } finally {
        source.close()
      }
    } catch {
      case e: Exception => Left(e)
    }
  }

  /**
   * Get primary key name from snapshot JSON
   *
   * @param json JSON string containing snapshot metadata
   * @return Either containing primary key field name or error
   */
  def getPkName(json: String): Either[Throwable, String] = {
    parseSnapshotMetadata(json).flatMap { metadata =>
      metadata.collection.schema.fields
        .find(_.isPrimaryKey == Some(true))
        .map(_.name)
        .toRight(new IllegalArgumentException("No primary key field found in snapshot"))
    }
  }

  /**
   * Get primary key name from snapshot file
   *
   * @param path Path to the snapshot metadata JSON file
   * @return Either containing primary key field name or error
   */
  def getPkNameFromFile(path: String): Either[Throwable, String] = {
    readSnapshotMetadataFromFile(path).flatMap { metadata =>
      metadata.collection.schema.fields
        .find(_.isPrimaryKey == Some(true))
        .map(_.name)
        .toRight(new IllegalArgumentException("No primary key field found in snapshot"))
    }
  }

  /**
   * Get collection schema from snapshot file
   *
   * @param path Path to the snapshot metadata JSON file
   * @return Either containing CollectionSchema or error
   */
  def getSchemaFromFile(path: String): Either[Throwable, CollectionSchema] = {
    readSnapshotMetadataFromFile(path).map { metadata =>
      metadata.collection.schema
    }
  }

  /**
   * Get Storage V2 segment manifest map from snapshot file
   * Returns a map from segment ID to parsed manifest content (version and base path)
   *
   * @param path Path to the snapshot metadata JSON file
   * @return Either containing Map[segmentID -> ManifestContent] or error
   */
  def getStorageV2ManifestMap(path: String): Either[Throwable, Map[Long, ManifestContent]] = {
    readSnapshotMetadataFromFile(path).flatMap { metadata =>
      metadata.storageV2ManifestList match {
        case Some(manifestList) =>
          val results = manifestList.map { item =>
            item.parseManifest(mapper).map(content => item.segmentID -> content)
          }

          // Check if all parsing succeeded
          val failures = results.collect { case Left(e) => e }
          if (failures.nonEmpty) {
            Left(new Exception(s"Failed to parse ${failures.size} manifest(s): ${failures.head.getMessage}"))
          } else {
            Right(results.collect { case Right(pair) => pair }.toMap)
          }

        case None =>
          Right(Map.empty[Long, ManifestContent])
      }
    }
  }

  /**
   * Convert snapshot CollectionSchema to Spark StructType
   *
   * @param schema CollectionSchema from snapshot metadata
   * @param includeSystemFields Whether to include RowID and Timestamp system fields
   * @return Spark StructType representing the collection schema
   */
  def toSparkSchema(schema: CollectionSchema, includeSystemFields: Boolean = false): StructType = {
    val userFields = schema.fields
      .filterNot(f => !includeSystemFields && (f.name == "RowID" || f.name == "Timestamp"))
      .map { field =>
        StructField(
          field.name,
          dataTypeToSparkType(field.dataType, field.typeParams),
          nullable = true
        )
      }
    StructType(userFields)
  }

  /**
   * Convert a Field to Spark DataType
   *
   * @param field Field from snapshot schema
   * @return Corresponding Spark DataType
   */
  def fieldToSparkType(field: Field): DataType = {
    dataTypeToSparkType(field.dataType, field.typeParams)
  }

  /**
   * Convert Milvus data type to Spark DataType
   *
   * @param dataType Milvus data type integer code
   * @param typeParams Optional type parameters (e.g., dim for vectors, max_length for varchar)
   * @return Corresponding Spark DataType
   */
  private def dataTypeToSparkType(dataType: Int, typeParams: Option[Seq[TypeParam]]): DataType = {
    dataType match {
      case 1 => BooleanType       // Bool
      case 2 => ByteType          // Int8
      case 3 => ShortType         // Int16
      case 4 => IntegerType       // Int32
      case 5 => LongType          // Int64
      case 10 => FloatType        // Float
      case 11 => DoubleType       // Double
      case 20 => StringType       // String
      case 21 => StringType       // VarChar
      case 22 => StringType       // JSON (as string)
      case 23 => ArrayType(BooleanType)   // Array[Bool]
      case 24 => ArrayType(ByteType)      // Array[Int8]
      case 25 => ArrayType(ShortType)     // Array[Int16]
      case 26 => ArrayType(IntegerType)   // Array[Int32]
      case 27 => ArrayType(LongType)      // Array[Int64]
      case 28 => ArrayType(FloatType)     // Array[Float]
      case 29 => ArrayType(DoubleType)    // Array[Double]
      case 30 => ArrayType(StringType)    // Array[VarChar]
      case 101 => ArrayType(FloatType)    // FloatVector
      case 102 => ArrayType(ByteType)     // BinaryVector
      case 103 => ArrayType(ShortType)    // Float16Vector
      case 104 => ArrayType(ShortType)    // BFloat16Vector
      case 105 => MapType(LongType, FloatType) // SparseFloatVector
      case _ => BinaryType        // Unknown types as binary
    }
  }

  /**
   * Get field ID to name mapping for column pruning
   *
   * @param schema CollectionSchema from snapshot metadata
   * @return Map from field ID to field name
   */
  def getFieldIdMap(schema: CollectionSchema): Map[Long, String] = {
    schema.fields.map(f => f.getFieldIDAsLong -> f.name).toMap
  }

  /**
   * Get field name to ID mapping
   *
   * @param schema CollectionSchema from snapshot metadata
   * @return Map from field name to field ID
   */
  def getFieldNameToIdMap(schema: CollectionSchema): Map[String, Long] = {
    schema.fields.map(f => f.name -> f.getFieldIDAsLong).toMap
  }

  /**
   * Convert snapshot CollectionSchema to protobuf CollectionSchema bytes
   * This is needed for FFI reader which requires protobuf schema format
   *
   * @param schema CollectionSchema from snapshot metadata
   * @return Protobuf CollectionSchema bytes
   */
  def toProtobufSchemaBytes(schema: CollectionSchema): Array[Byte] = {
    import io.milvus.grpc.schema.{CollectionSchema => ProtoCollectionSchema, FieldSchema, DataType}
    import io.milvus.grpc.common.KeyValuePair

    // Filter out system fields (RowID and Timestamp) - only include user fields
    val userFields = schema.fields.filterNot(f => f.name == "RowID" || f.name == "Timestamp")

    val protoFields = userFields.map { field =>
      FieldSchema(
        fieldID = field.getFieldIDAsLong,
        name = field.name,
        description = field.description.getOrElse(""),
        dataType = DataType.fromValue(field.dataType),
        isPrimaryKey = field.isPrimaryKey.getOrElse(false),
        isClusteringKey = field.isClusteringKey.getOrElse(false),
        typeParams = field.typeParams.getOrElse(Seq.empty).map { tp =>
          KeyValuePair(key = tp.key, value = tp.value)
        }
      )
    }

    val protoSchema = ProtoCollectionSchema(
      name = schema.name,
      description = schema.description.getOrElse(""),
      fields = protoFields
    )

    protoSchema.toByteArray
  }

  /**
   * Serialize StorageV2ManifestList to JSON string for passing via options
   *
   * @param manifestList List of StorageV2ManifestItem
   * @return JSON string representation
   */
  def serializeManifestList(manifestList: Seq[StorageV2ManifestItem]): String = {
    mapper.writeValueAsString(manifestList)
  }

  /**
   * Deserialize StorageV2ManifestList from JSON string
   *
   * @param json JSON string representation
   * @return Either containing parsed manifest list or error
   */
  def deserializeManifestList(json: String): Either[Throwable, Seq[StorageV2ManifestItem]] = {
    try {
      Right(mapper.readValue[Seq[StorageV2ManifestItem]](json))
    } catch {
      case e: Exception => Left(e)
    }
  }

  /**
   * Parse simplified manifest content JSON string
   * Format: {"ver":1,"base_path":"..."}
   *
   * @param json JSON string containing simplified manifest
   * @return Either containing parsed ManifestContent or error
   */
  def parseManifestContent(json: String): Either[Throwable, ManifestContent] = {
    try {
      Right(mapper.readValue[ManifestContent](json))
    } catch {
      case e: Exception => Left(e)
    }
  }

  /**
   * Transform Storage V2 manifest JSON by converting column field IDs to field names.
   * The FFI reader expects field names in the columns array, but Storage V2 manifest files
   * use field IDs as strings (e.g., "100", "101", "0", "1").
   *
   * Also strips bucket/rootPath prefix from paths if present, since the FFI reader
   * will prepend these when accessing files.
   *
   * @param manifestJson The original manifest JSON from Storage V2
   * @param schema CollectionSchema from snapshot metadata for ID-to-name mapping
   * @param bucket Optional bucket name to strip from paths
   * @param rootPath Optional root path to strip from paths
   * @return Transformed manifest JSON with field names instead of IDs
   */
  def transformManifestColumnsToNames(
      manifestJson: String,
      schema: CollectionSchema,
      bucket: Option[String] = None,
      rootPath: Option[String] = None
  ): Either[Throwable, String] = {
    try {
      // Parse the manifest JSON
      val manifestObj = mapper.readValue[Map[String, Any]](manifestJson)

      // Build field ID -> name mapping (user fields only, no system fields RowID/Timestamp)
      // System fields (RowID=0, Timestamp=1) will be filtered out from column_groups
      val fieldIdToName: Map[String, String] = {
        schema.fields
          .filterNot(f => f.name == "RowID" || f.name == "Timestamp")
          .map(f => f.getFieldIDAsLong.toString -> f.name)
          .toMap
      }

      // System field IDs to filter out (these are not in the Arrow schema)
      val systemFieldIds = Set("0", "1")

      // Build path prefix to strip (if provided)
      val pathPrefixToStrip = (bucket, rootPath) match {
        case (Some(b), Some(r)) => Some(s"$b/$r/")
        case (Some(b), None) => Some(s"$b/")
        case _ => None
      }

      // Transform column_groups
      val columnGroups = manifestObj.getOrElse("column_groups", Seq.empty) match {
        case groups: Seq[_] =>
          groups.flatMap {
            case group: Map[String, Any] @unchecked =>
              // Transform columns - filter out system fields and map IDs to names
              val columns = group.getOrElse("columns", Seq.empty) match {
                case cols: Seq[_] =>
                  cols.flatMap {
                    case col: String =>
                      // Skip system field IDs (0=RowID, 1=Timestamp)
                      if (systemFieldIds.contains(col)) {
                        None
                      } else {
                        // Map field ID to name, keep if mapping exists
                        fieldIdToName.get(col)
                      }
                    case _ => None
                  }
                case _ => Seq.empty
              }

              // Skip column groups that have no user fields after filtering
              if (columns.isEmpty) {
                None
              } else {
                // Transform paths (strip bucket/rootPath prefix if present)
                val paths = group.getOrElse("paths", Seq.empty) match {
                  case ps: Seq[_] =>
                    ps.map {
                      case path: String =>
                        pathPrefixToStrip match {
                          case Some(prefix) if path.startsWith(prefix) =>
                            path.stripPrefix(prefix)
                          case _ => path
                        }
                      case other => other.toString
                    }
                  case _ => Seq.empty
                }

                Some(group + ("columns" -> columns) + ("paths" -> paths))
              }
            case other => Some(other)
          }
        case _ => Seq.empty
      }

      // Rebuild manifest with transformed column_groups
      // Add "version" field (expected by FFI reader) and remove "metadata" if present
      val transformedManifest = (manifestObj - "metadata") +
        ("column_groups" -> columnGroups) +
        ("version" -> 0)

      Right(mapper.writeValueAsString(transformedManifest))
    } catch {
      case e: Exception => Left(e)
    }
  }
}
