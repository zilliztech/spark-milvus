package com.zilliz.spark.connector.table

import org.apache.spark.sql.types.{DataType, StructField, StructType}

import com.zilliz.milvus.storage.snapshot.json.{CollectionSchemaJson, FieldJson}
import com.zilliz.spark.connector.types.SparkTypes
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** The `CollectionSchemaJson` inside a snapshot to a Spark StructType.
  *
  * Parsing the snapshot JSON is layer 2 (`core.snapshot.json`); the Spark types
  * are layer 3, so the mapping sits here.
  */
object SnapshotSparkSchema {

  /** Convert snapshot CollectionSchemaJson to Spark StructType
    *
    * @param schema
    *   CollectionSchemaJson from snapshot metadata
    * @param includeSystemFields
    *   Whether to include RowID and Timestamp system fields
    * @return
    *   Spark StructType representing the collection schema
    */
  def toSparkSchema(
      schema: CollectionSchemaJson,
      includeSystemFields: Boolean = false
  ): StructType = {
    val userFields = schema.fields
      .filterNot(f =>
        !includeSystemFields && (f.name == "RowID" || f.name == "Timestamp")
      )
      .map(fieldToStructField)
    StructType(userFields)
  }

  /** Convert a FieldJson to Spark StructField with Milvus metadata preserved.
    */
  def fieldToStructField(field: FieldJson): StructField =
    SparkTypes.toStructField(toFieldSchema(field))

  /** Convert a FieldJson to Spark DataType
    *
    * @param field
    *   FieldJson from snapshot schema
    * @return
    *   Corresponding Spark DataType
    */
  def fieldToSparkType(field: FieldJson): DataType = {
    fieldToStructField(field).dataType
  }

  /** Snapshot JSON is converted to the same protobuf field that the rest of the
    * read path consumes. Missing booleans use protobuf defaults, matching
    * CollectionSchemaJson.toProtobufBytes.
    */
  private def toFieldSchema(field: FieldJson): FieldSchema =
    FieldSchema(
      fieldID = field.getFieldIDAsLong,
      name = field.name,
      dataType = MilvusDataType.fromValue(field.dataType),
      isPrimaryKey = field.isPrimaryKey.getOrElse(false),
      isClusteringKey = field.isClusteringKey.getOrElse(false),
      typeParams = field.typeParams.getOrElse(Seq.empty).map { param =>
        KeyValuePair(key = param.key, value = param.value)
      },
      elementType = MilvusDataType.fromValue(field.elementType),
      isPartitionKey = field.isPartitionKey.getOrElse(false),
      nullable = field.nullable.getOrElse(false)
    )
}
