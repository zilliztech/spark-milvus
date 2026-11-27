package com.zilliz.milvus.storage.snapshot.json

import com.fasterxml.jackson.annotation.{JsonAlias, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode

import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoCollectionSchema,
  DataType,
  FieldSchema,
  FieldState,
  StructArrayFieldSchema,
  TypeSchema
}

/** A `{key, value}` pair: a field's `type_params` entry or a collection's
  * `properties` entry (`KeyValuePair` in the Milvus proto).
  */
case class KeyValueJson(
    @JsonProperty("key") key: String,
    @JsonProperty("value") value: String
) {
  def toProtobuf: KeyValuePair = KeyValuePair(key = key, value = value)
}

/** The recursive type of a nested scalar array. */
case class TypeSchemaJson(
    @JsonProperty("leaf_type") @JsonAlias(Array("leafType")) leafType: Option[
      JsonNode
    ] = None,
    @JsonProperty("array_element") @JsonAlias(
      Array("arrayElement")
    ) arrayElement: Option[TypeSchemaJson] = None,
    @JsonProperty("type_params") @JsonAlias(
      Array("typeParams")
    ) typeParams: Seq[KeyValueJson] = Seq.empty,
    @JsonProperty("nullable") nullable: Boolean = false
) {
  def toProtobuf: TypeSchema = {
    val node =
      TypeSchema(typeParams = typeParams.map(_.toProtobuf), nullable = nullable)
    (leafType, arrayElement) match {
      case (Some(leaf), None) =>
        node.withLeafType(DataType.fromValue(JsonValues.toDataTypeCode(leaf)))
      case (None, Some(element)) => node.withArrayElement(element.toProtobuf)
      case _ =>
        throw UnsupportedSnapshotSchemaField(
          "type_schema requires exactly one of leaf_type and array_element"
        )
    }
  }
}

/** Logical parent of the member columns stored in a segment. */
case class StructArrayFieldJson(
    @JsonProperty("fieldID") fieldID: JsonNode,
    @JsonProperty("name") name: String,
    @JsonProperty("fields") fields: Seq[FieldJson],
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("type_params") @JsonAlias(
      Array("typeParams")
    ) typeParams: Seq[KeyValueJson] = Seq.empty,
    @JsonProperty("nullable") nullable: Boolean = false
) {
  def toProtobuf: StructArrayFieldSchema = StructArrayFieldSchema(
    fieldID = JsonValues.toLong(fieldID),
    name = name,
    fields = fields.map(_.toProtobuf),
    description = description.getOrElse(""),
    typeParams = typeParams.map(_.toProtobuf),
    nullable = nullable
  )
}

/** One entry of `collection.schema.fields`. */
case class FieldJson(
    @JsonProperty("fieldID") fieldID: Option[JsonNode] = None,
    @JsonProperty("name") name: String,
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("data_type") @JsonAlias(
      Array("dataType")
    ) rawDataType: Option[JsonNode] = None,
    @JsonProperty("is_primary_key") isPrimaryKey: Option[Boolean] = None,
    @JsonProperty("is_clustering_key") isClusteringKey: Option[Boolean] = None,
    @JsonProperty("type_params") @JsonAlias(
      Array("typeParams")
    ) typeParams: Option[Seq[KeyValueJson]] = None,
    @JsonProperty("autoID") @JsonAlias(Array("auto_id")) autoID: Option[
      Boolean
    ] = None,
    @JsonProperty("state") rawState: Option[JsonNode] = None,
    @JsonProperty("element_type") @JsonAlias(
      Array("elementType")
    ) rawElementType: Option[JsonNode] = None,
    @JsonProperty("is_dynamic") isDynamic: Option[Boolean] = None,
    @JsonProperty("is_partition_key") isPartitionKey: Option[Boolean] = None,
    @JsonProperty("nullable") nullable: Option[Boolean] = None,
    @JsonProperty("is_function_output") isFunctionOutput: Option[Boolean] =
      None,
    @JsonProperty("default_value") defaultValue: Option[JsonNode] = None,
    @JsonProperty("type_schema") @JsonAlias(
      Array("typeSchema")
    ) typeSchema: Option[TypeSchemaJson] = None,
    @JsonProperty("element_nullable") @JsonAlias(
      Array("elementNullable")
    ) elementNullable: Boolean = false
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

  def toProtobuf: FieldSchema = FieldSchema(
    fieldID = getFieldIDAsLong,
    name = name,
    description = description.getOrElse(""),
    dataType = DataType.fromValue(dataType),
    isPrimaryKey = isPrimaryKey.getOrElse(false),
    isClusteringKey = isClusteringKey.getOrElse(false),
    typeParams = typeParams.getOrElse(Seq.empty).map(_.toProtobuf),
    autoID = autoID.getOrElse(false),
    state = FieldState.fromValue(state),
    elementType = DataType.fromValue(elementType),
    isDynamic = isDynamic.getOrElse(false),
    isPartitionKey = isPartitionKey.getOrElse(false),
    nullable = nullable.getOrElse(false),
    elementNullable = elementNullable,
    typeSchema = typeSchema.map(_.toProtobuf),
    isFunctionOutput = isFunctionOutput.getOrElse(false),
    defaultValue =
      defaultValue.map(CollectionSchemaJson.protobufDefaultValue(_, name))
  )
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
    @JsonProperty("version") version: Int = 0,
    @JsonProperty("struct_array_fields") @JsonAlias(
      Array("structArrayFields")
    ) structArrayFields: Seq[StructArrayFieldJson] = Seq.empty
) {
  private def storedFields: Seq[FieldJson] =
    fields ++ structArrayFields.flatMap(_.fields)

  def getFieldByName(name: String): Option[FieldJson] =
    storedFields.find(_.name == name)

  def primaryKey: Option[FieldJson] =
    fields.find(_.isPrimaryKey.contains(true))

  def fieldNamesById: Map[Long, String] =
    storedFields.map(f => f.getFieldIDAsLong -> f.name).toMap

  def fieldIdsByName: Map[String, Long] =
    storedFields.map(f => f.name -> f.getFieldIDAsLong).toMap

  /** The same schema as the protobuf `CollectionSchema`, serialized: what the
    * native reader and every planner take. The system fields `RowID` and
    * `Timestamp` are left out, as the proto never lists them.
    */
  def toProtobuf: ProtoCollectionSchema = {
    val userFields =
      fields.filterNot(f => f.name == "RowID" || f.name == "Timestamp")
    ProtoCollectionSchema(
      name = name,
      description = description.getOrElse(""),
      autoID = autoID.getOrElse(false),
      fields = userFields.map(_.toProtobuf),
      structArrayFields = structArrayFields.map(_.toProtobuf),
      enableDynamicField = enableDynamicField.getOrElse(false),
      properties = properties.getOrElse(Seq.empty).map(_.toProtobuf),
      version = version
    )
  }

  def toProtobufBytes: Array[Byte] = toProtobuf.toByteArray
}

object CollectionSchemaJson {

  /** A field's `default_value` as the proto `ValueField`. The JSON carries one
    * of the `*_data` members, in snake or camel case.
    */
  private[json] def protobufDefaultValue(
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
