package com.zilliz.spark.connector.read

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.spark.sql.types._

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
    @JsonProperty("fieldID") fieldID: Option[Any],  // Can be Int or Long from JSON
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String],
    @JsonProperty("data_type") dataType: Int,
    @JsonProperty("is_primary_key") isPrimaryKey: Option[Boolean],
    @JsonProperty("is_clustering_key") isClusteringKey: Option[Boolean],
    @JsonProperty("type_params") typeParams: Option[Seq[TypeParam]]
) {
  def getTypeParam(key: String): Option[String] = {
    typeParams.flatMap(_.find(_.key == key).map(_.value))
  }

  def getFieldIDAsLong: Long = {
    fieldID match {
      case Some(l: Long) => l
      case Some(i: Int) => i.toLong
      case Some(n: Number) => n.longValue()
      case _ => 0L
    }
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
    @JsonProperty("description") description: Option[String],
    @JsonProperty("fields") fields: Seq[Field],
    @JsonProperty("properties") properties: Option[Seq[Property]]
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
    @JsonProperty("num_partitions") numPartitions: Option[Int],
    @JsonProperty("num_shards") numShards: Option[Int],
    @JsonProperty("properties") properties: Option[Seq[Property]],
    @JsonProperty("consistency_level") consistencyLevel: Option[Int]
)

/**
 * Snapshot information
 */
case class SnapshotInfo(
    @JsonProperty("name") name: String,
    @JsonProperty("id") id: Long,
    @JsonProperty("description") description: Option[String],
    @JsonProperty("collection_id") collectionId: Long,
    @JsonProperty("partition_ids") partitionIds: Seq[Long],
    @JsonProperty("create_ts") createTs: Long
)

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
    @JsonProperty("segmentID") segmentID: Long,
    @JsonProperty("manifest") manifest: String
) {
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

/**
 * Complete snapshot metadata
 */
case class SnapshotMetadata(
    @JsonProperty("snapshot-info") snapshotInfo: SnapshotInfo,
    @JsonProperty("collection") collection: Collection,
    @JsonProperty("indexes") indexes: Seq[Any],
    @JsonProperty("manifest-list") manifestList: Seq[String],
    @JsonProperty("storagev2-manifest-list") storageV2ManifestList: Option[Seq[StorageV2ManifestItem]]
)

/**
 * Reader for Milvus snapshot metadata JSON files
 */
object MilvusSnapshotReader {

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
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

    val protoFields = schema.fields.map { field =>
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
}
