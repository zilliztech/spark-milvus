package com.zilliz.milvus.storage.snapshot.json

import com.fasterxml.jackson.annotation.{JsonAlias, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode

/** A `{key, value}` pair: a field's `type_params` entry or a collection's
  * `properties` entry (`KeyValuePair` in the Milvus proto).
  */
case class KeyValueJson(
    @JsonProperty("key") key: String,
    @JsonProperty("value") value: String
)

/** One entry of `collection.schema.fields`. */
case class FieldJson(
    @JsonProperty("fieldID") fieldID: Option[JsonNode] = None,
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("data_type") rawDataType: Option[JsonNode] = None,
    @JsonProperty("is_primary_key") isPrimaryKey: Option[Boolean] = None,
    @JsonProperty("is_clustering_key") isClusteringKey: Option[Boolean] = None,
    @JsonProperty("type_params") typeParams: Option[Seq[KeyValueJson]] = None,
    @JsonProperty("autoID") @JsonAlias(Array("auto_id")) autoID: Option[
      Boolean
    ] = None,
    @JsonProperty("state") rawState: Option[JsonNode] = None,
    @JsonProperty("element_type") rawElementType: Option[JsonNode] = None,
    @JsonProperty("is_dynamic") isDynamic: Option[Boolean] = None,
    @JsonProperty("is_partition_key") isPartitionKey: Option[Boolean] = None,
    @JsonProperty("nullable") nullable: Option[Boolean] = None,
    @JsonProperty("is_function_output") isFunctionOutput: Option[Boolean] =
      None,
    @JsonProperty("default_value") defaultValue: Option[JsonNode] = None
) {

  /** The data type code, whether the JSON wrote `5` or `"Int64"`. */
  def dataType: Int =
    rawDataType.map(JsonValues.toDataTypeCode).getOrElse(0)

  def getTypeParam(key: String): Option[String] =
    typeParams.flatMap(_.find(_.key == key).map(_.value))

  def getFieldIDAsLong: Long = fieldID.map(JsonValues.toLong).getOrElse(0L)

  def state: Int = rawState.map(JsonValues.toFieldStateCode).getOrElse(0)

  def elementType: Int =
    rawElementType.map(JsonValues.toDataTypeCode).getOrElse(0)
}

case class UnsupportedSnapshotSchemaField(message: String)
    extends IllegalArgumentException(message)

object FieldJson {

  /** `DataType` enum names to codes, as in the Milvus proto. */
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
    "Array" -> 22,
    "JSON" -> 23,
    "Geometry" -> 24,
    "Text" -> 25,
    "Timestamptz" -> 26,
    "BinaryVector" -> 100,
    "FloatVector" -> 101,
    "Float16Vector" -> 102,
    "BFloat16Vector" -> 103,
    "SparseFloatVector" -> 104,
    "Int8Vector" -> 105,
    "ArrayOfVector" -> 106,
    "ArrayOfStruct" -> 200,
    "Struct" -> 201
  )

  private val fieldStateNameToCodeMap: Map[String, Int] = Map(
    "FieldCreated" -> 0,
    "FieldCreating" -> 1,
    "FieldDropping" -> 2,
    "FieldDropped" -> 3
  )

  def dataTypeNameToCode(name: String): Int =
    dataTypeNameToCodeMap.getOrElse(name, 0)

  def fieldStateNameToCode(name: String): Int =
    fieldStateNameToCodeMap.getOrElse(name, 0)
}

/** `collection.schema`. */
case class CollectionSchemaJson(
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("fields") fields: Seq[FieldJson],
    @JsonProperty("properties") properties: Option[Seq[KeyValueJson]] = None,
    @JsonProperty("autoID") @JsonAlias(Array("auto_id")) autoID: Option[
      Boolean
    ] = None,
    @JsonProperty("enable_dynamic_field") enableDynamicField: Option[Boolean] =
      None,
    @JsonProperty("version") version: Int = 0
) {
  def getFieldByName(name: String): Option[FieldJson] =
    fields.find(_.name == name)

  def primaryKey: Option[FieldJson] =
    fields.find(_.isPrimaryKey.contains(true))

  def fieldNamesById: Map[Long, String] =
    fields.map(f => f.getFieldIDAsLong -> f.name).toMap

  def fieldIdsByName: Map[String, Long] =
    fields.map(f => f.name -> f.getFieldIDAsLong).toMap

  /** The same schema as the protobuf `CollectionSchema`, serialized: what the
    * native reader and every planner take. The system fields `RowID` and
    * `Timestamp` are left out, as the proto never lists them.
    */
  def toProtobufBytes: Array[Byte] = {
    import io.milvus.grpc.common.KeyValuePair
    import io.milvus.grpc.schema.{
      DataType,
      FieldSchema,
      FieldState,
      CollectionSchema => ProtoCollectionSchema
    }

    val userFields =
      fields.filterNot(f => f.name == "RowID" || f.name == "Timestamp")

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
        },
        autoID = field.autoID.getOrElse(false),
        state = FieldState.fromValue(field.state),
        elementType = DataType.fromValue(field.elementType),
        isDynamic = field.isDynamic.getOrElse(false),
        isPartitionKey = field.isPartitionKey.getOrElse(false),
        nullable = field.nullable.getOrElse(false),
        isFunctionOutput = field.isFunctionOutput.getOrElse(false),
        defaultValue = field.defaultValue.map(
          CollectionSchemaJson.protobufDefaultValue(_, field.name)
        )
      )
    }

    ProtoCollectionSchema(
      name = name,
      description = description.getOrElse(""),
      autoID = autoID.getOrElse(false),
      fields = protoFields,
      enableDynamicField = enableDynamicField.getOrElse(false),
      properties = properties.getOrElse(Seq.empty).map { prop =>
        KeyValuePair(key = prop.key, value = prop.value)
      }
    ).toByteArray
  }
}

object CollectionSchemaJson {

  /** A field's `default_value` as the proto `ValueField`. The JSON carries one
    * of the `*_data` members, in snake or camel case.
    */
  private def protobufDefaultValue(
      node: JsonNode,
      fieldName: String
  ): io.milvus.grpc.schema.ValueField = {
    import io.milvus.grpc.schema.ValueField

    val value = Option(node).filterNot(_.isNull).getOrElse {
      throw UnsupportedSnapshotSchemaField(
        s"default_value for field $fieldName is null"
      )
    }

    def child(names: String*): Option[JsonNode] =
      names.iterator.map(value.get).find(v => v != null && !v.isNull)

    child("bool_data", "boolData")
      .map(v => ValueField().withBoolData(v.asBoolean()))
      .orElse(
        child("int_data", "intData").map(v =>
          ValueField().withIntData(v.asInt())
        )
      )
      .orElse(
        child("long_data", "longData").map(v =>
          ValueField().withLongData(v.asLong())
        )
      )
      .orElse(
        child("float_data", "floatData").map(v =>
          ValueField().withFloatData(v.floatValue())
        )
      )
      .orElse(
        child("double_data", "doubleData").map(v =>
          ValueField().withDoubleData(v.asDouble())
        )
      )
      .orElse(
        child("string_data", "stringData").map(v =>
          ValueField().withStringData(v.asText())
        )
      )
      .orElse(
        child("bytes_data", "bytesData").map { v =>
          ValueField().withBytesData(
            com.google.protobuf.ByteString.copyFrom(
              java.util.Base64.getDecoder.decode(v.asText())
            )
          )
        }
      )
      .orElse(
        child("timestamptz_data", "timestamptzData").map(v =>
          ValueField().withTimestamptzData(v.asLong())
        )
      )
      .getOrElse {
        throw UnsupportedSnapshotSchemaField(
          s"default_value for field $fieldName uses an unsupported shape: $value"
        )
      }
  }
}

/** `collection`. */
case class CollectionJson(
    @JsonProperty("schema") schema: CollectionSchemaJson,
    @JsonProperty("num_partitions") rawNumPartitions: Option[JsonNode] = None,
    @JsonProperty("num_shards") rawNumShards: Option[JsonNode] = None,
    @JsonProperty("properties") properties: Option[Seq[KeyValueJson]] = None,
    @JsonProperty("consistency_level") rawConsistencyLevel: Option[JsonNode] =
      None
) {
  def numPartitions: Option[Int] = rawNumPartitions.map(JsonValues.toInt)
  def numShards: Option[Int] = rawNumShards.map(JsonValues.toInt)
  def consistencyLevel: Option[Int] = rawConsistencyLevel.map(JsonValues.toInt)
}
