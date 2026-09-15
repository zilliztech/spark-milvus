package com.zilliz.spark.connector.write

import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.FieldMetadata
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** One test per rule of docs/design/architecture/write.html section 3. */
class WriteSchemaTest extends AnyFunSuite with Matchers {

  private val id =
    FieldSchema(
      fieldID = 100,
      name = "id",
      dataType = DataType.Int64,
      isPrimaryKey = true
    )
  private val name = FieldSchema(
    fieldID = 101,
    name = "name",
    dataType = DataType.VarChar,
    typeParams = Seq(KeyValuePair("max_length", "64"))
  )
  private val v = FieldSchema(
    fieldID = 102,
    name = "v",
    dataType = DataType.FloatVector,
    typeParams = Seq(KeyValuePair("dim", "4"))
  )
  private val opt =
    FieldSchema(
      fieldID = 103,
      name = "opt",
      dataType = DataType.Int32,
      nullable = true
    )
  private val sparse = FieldSchema(
    fieldID = 104,
    name = "sparse",
    dataType = DataType.SparseFloatVector,
    isFunctionOutput = true
  )
  private val collection =
    CollectionSchema(name = "c", fields = Seq(id, name, v, opt, sparse))

  private val whole = StructType(
    Seq(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("v", ArrayType(FloatType)),
      StructField("opt", IntegerType)
    )
  )

  test(
    "a complete DataFrame resolves with type, field id and dimension on every column"
  ) {
    val resolved =
      WriteSchema.resolve(whole, collection, WriteSchema.Mode.Append)
    resolved.fieldNames.toSeq shouldBe Seq("id", "name", "v", "opt")
    val vec = resolved("v")
    vec.dataType shouldBe ArrayType(FloatType)
    vec.nullable shouldBe false
    vec.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey) shouldBe 102L
    vec.metadata.getLong(
      FieldMetadata.MilvusVectorDimensionMetadataKey
    ) shouldBe 4L
    vec.metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    ) shouldBe DataType.FloatVector.value.toLong
    resolved("opt").nullable shouldBe true
    resolved("id").metadata.getLong(
      FieldMetadata.MilvusFieldIdMetadataKey
    ) shouldBe 100L
  }

  test("a column that is no field of the collection is refused") {
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(
        whole.add(StructField("extra", StringType)),
        collection,
        WriteSchema.Mode.Append
      )
    )
    e.getMessage should include(
      "Columns extra are not fields of collection 'c'"
    )
  }

  test("a column of another type than the field is refused, no implicit cast") {
    val wrong = StructType(whole.fields.map {
      case f if f.name == "id" => f.copy(dataType = IntegerType)
      case f                   => f
    })
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(wrong, collection, WriteSchema.Mode.Append)
    )
    e.getMessage should include(
      "Column 'id' is int, field 'id' (Int64) takes bigint"
    )
  }

  test("a vector column is taken as an array or as the raw bytes") {
    val raw = StructType(whole.fields.map {
      case f if f.name == "v" => f.copy(dataType = BinaryType)
      case f                  => f
    })
    WriteSchema
      .resolve(raw, collection, WriteSchema.Mode.Append)("v")
      .dataType shouldBe BinaryType
    val wrong = StructType(whole.fields.map {
      case f if f.name == "v" => f.copy(dataType = MapType(LongType, FloatType))
      case f                  => f
    })
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(wrong, collection, WriteSchema.Mode.Append)
    )
    e.getMessage should include("takes array<float> or binary")
  }

  test("an append must carry every field; a column write need not") {
    val partial = StructType(whole.fields.filterNot(_.name == "opt"))
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(partial, collection, WriteSchema.Mode.Append)
    )
    e.getMessage should include(
      "Fields opt of collection 'c' are missing from the DataFrame"
    )
    WriteSchema
      .resolve(partial, collection, WriteSchema.Mode.Columns)
      .fieldNames
      .toSeq shouldBe Seq("id", "name", "v")
  }

  test("a function output is never written, and is not counted as missing") {
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(
        whole.add(StructField("sparse", MapType(LongType, FloatType))),
        collection,
        WriteSchema.Mode.Append
      )
    )
    e.getMessage should include(
      "Column 'sparse' is the output of a Milvus function"
    )
  }

  test(
    "an autoID collection cannot be appended to; a column write can target it"
  ) {
    val auto = collection.copy(fields = collection.fields.map {
      case f if f.name == "id" => f.copy(autoID = true)
      case f                   => f
    })
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(whole, auto, WriteSchema.Mode.Append)
    )
    e.getMessage should include("autoID")
    WriteSchema.resolve(whole, auto, WriteSchema.Mode.Columns).size shouldBe 4
  }

  test("a partition-key collection is not supported") {
    val keyed = collection.copy(fields = collection.fields.map {
      case f if f.name == "name" => f.copy(isPartitionKey = true)
      case f                     => f
    })
    val e = intercept[IllegalArgumentException](
      WriteSchema.resolve(whole, keyed, WriteSchema.Mode.Append)
    )
    e.getMessage should include("partition key 'name'")
  }
}
