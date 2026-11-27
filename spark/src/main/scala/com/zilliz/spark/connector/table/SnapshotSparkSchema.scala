package com.zilliz.spark.connector.table

import org.apache.spark.sql.types.{DataType, StructField, StructType}

import com.zilliz.milvus.storage.snapshot.json.{CollectionSchemaJson, FieldJson}
import com.zilliz.spark.connector.types.SparkTypes

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
    StructType(
      userFields ++ schema.structArrayFields.map(parent =>
        SparkTypes.toStructArrayField(parent.toProtobuf, rawVectors = false)
      )
    )
  }

  /** Convert a FieldJson to Spark StructField with Milvus metadata preserved.
    */
  def fieldToStructField(field: FieldJson): StructField =
    SparkTypes.toStructField(field.toProtobuf)

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
}
