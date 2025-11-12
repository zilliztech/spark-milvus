package com.zilliz.spark.connector.read

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

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
    @JsonProperty("properties") properties: Option[Seq[Property]]
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
 * Complete snapshot metadata
 */
case class SnapshotMetadata(
    @JsonProperty("snapshot-info") snapshotInfo: SnapshotInfo,
    @JsonProperty("collection") collection: Collection,
    @JsonProperty("indexes") indexes: Seq[Any],
    @JsonProperty("manifest-list") manifestList: Seq[String]
)

/**
 * Reader for Milvus snapshot metadata JSON files
 */
object MilvusSnapshotReader {

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m
  }

  /**
   * Parse snapshot metadata from JSON string
   *
   * @param json JSON string containing snapshot metadata
   * @return Either containing parsed SnapshotMetadata or error
   */
  private def parseSnapshotMetadata(json: String): Either[Throwable, SnapshotMetadata] = {
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
}
