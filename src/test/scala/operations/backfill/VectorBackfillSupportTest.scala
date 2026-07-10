package com.zilliz.spark.connector.operations.backfill

import java.nio.{ByteBuffer, ByteOrder}

import com.fasterxml.jackson.databind.node.IntNode
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.read.{Field, TypeParam}
import com.zilliz.spark.connector.serde.ArrowConverter
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class VectorBackfillSupportTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("VectorBackfillSupportTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def vectorField(
      name: String,
      dataType: MilvusDataType,
      dim: Option[Int] = None,
      nullable: Boolean = false
  ): Field =
    Field(
      name = name,
      rawDataType = Some(IntNode.valueOf(dataType.value)),
      typeParams = dim.map(value => Seq(TypeParam("dim", value.toString))),
      nullable = Some(nullable)
    )

  test("normalizes common parquet vector shapes to Milvus internal bytes") {
    val schema = StructType(
      Seq(
        StructField("pk", LongType, nullable = false),
        StructField("float_vec", ArrayType(DoubleType), nullable = false),
        StructField("binary_vec", ArrayType(ShortType), nullable = false),
        StructField("float16_vec", ArrayType(FloatType), nullable = false),
        StructField("bfloat16_vec", ArrayType(ShortType), nullable = false),
        StructField("int8_vec", ArrayType(IntegerType), nullable = false),
        StructField(
          "sparse_vec",
          MapType(StringType, DoubleType),
          nullable = false
        )
      )
    )
    val row = Row(
      1L,
      Seq(1.25d, -2.5d),
      Seq[Short](0, 255),
      Seq(1.5f, -2.0f),
      Seq[Short](192, 63, 0, 192),
      Seq(-128, 127),
      Map("3" -> 2.5d, "1" -> 1.25d)
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(row)),
      schema
    )

    val targets = Seq(
      vectorField("float_vec", MilvusDataType.FloatVector, Some(2)),
      vectorField("binary_vec", MilvusDataType.BinaryVector, Some(16)),
      vectorField("float16_vec", MilvusDataType.Float16Vector, Some(2)),
      vectorField("bfloat16_vec", MilvusDataType.BFloat16Vector, Some(2)),
      vectorField("int8_vec", MilvusDataType.Int8Vector, Some(2)),
      vectorField("sparse_vec", MilvusDataType.SparseFloatVector)
    ).map(field => field.name -> field).toMap

    val normalized = VectorBackfillSupport
      .normalizeVectorColumns(df, targets)
      .toOption
      .get

    targets.keys.foreach { name =>
      normalized.schema(name).dataType shouldBe BinaryType
      normalized
        .schema(name)
        .metadata
        .getLong(ArrowConverter.MilvusDataTypeMetadataKey) shouldBe
        targets(name).dataType.toLong
    }
    normalized
      .schema("float_vec")
      .metadata
      .getLong(ArrowConverter.MilvusVectorDimensionMetadataKey) shouldBe 2L

    val result = normalized.head()
    result.getAs[Array[Byte]]("float_vec") shouldBe ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(1.25f)
      .putFloat(-2.5f)
      .array()
    result.getAs[Array[Byte]]("binary_vec") shouldBe Array[Byte](0, -1)
    result.getAs[Array[Byte]]("float16_vec") shouldBe
      VectorBackfillSupport.encodeDenseJsonArray(
        "float16_vec",
        MilvusDataType.Float16Vector,
        2,
        "[1.5,-2.0]",
        encodedHalfBytes = false
      )
    result.getAs[Array[Byte]]("bfloat16_vec") shouldBe
      Array[Byte](192.toByte, 63.toByte, 0.toByte, 192.toByte)
    result.getAs[Array[Byte]]("int8_vec") shouldBe Array[Byte](-128, 127)
    result.getAs[Array[Byte]]("sparse_vec") shouldBe
      VectorBackfillSupport.encodeSparseJson(
        "sparse_vec",
        """{"1":1.25,"3":2.5}"""
      )
  }

  test("sparse map JSON and indices-values struct encode identically") {
    val mapBytes = VectorBackfillSupport.encodeSparseJson(
      "sparse",
      """{"5":3.0,"1":2.0}"""
    )
    val structBytes = VectorBackfillSupport.encodeSparseJson(
      "sparse",
      """{"indices":[5,1],"values":[3.0,2.0]}"""
    )

    structBytes shouldBe mapBytes
    val buffer = ByteBuffer.wrap(mapBytes).order(ByteOrder.LITTLE_ENDIAN)
    (buffer.getInt() & 0xffffffffL) shouldBe 1L
    buffer.getFloat() shouldBe 2.0f
    (buffer.getInt() & 0xffffffffL) shouldBe 5L
    buffer.getFloat() shouldBe 3.0f
  }

  test("Float16 conversion matches Milvus round-to-nearest byte layout") {
    VectorBackfillSupport.encodeDenseJsonArray(
      "float16",
      MilvusDataType.Float16Vector,
      2,
      "[0.11111,0.22222]",
      encodedHalfBytes = false
    ) shouldBe Array[Byte](0x1c, 0x2f, 0x1c, 0x33)
  }

  test("rejects negative sparse weights like Milvus validation") {
    val error = intercept[IllegalArgumentException] {
      VectorBackfillSupport.encodeSparseJson(
        "sparse",
        """{"1":-0.5}"""
      )
    }
    error.getMessage should include("non-negative")
  }

  test("internal binary vectors are validated before write") {
    val valid = ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(1.0f)
      .putFloat(2.0f)
      .array()

    VectorBackfillSupport.validateInternalBytes(
      "vec",
      MilvusDataType.FloatVector,
      2,
      valid
    ) shouldBe valid

    val error = intercept[IllegalArgumentException] {
      VectorBackfillSupport.validateInternalBytes(
        "vec",
        MilvusDataType.FloatVector,
        2,
        valid.dropRight(1)
      )
    }
    error.getMessage should include("byte-width mismatch")
  }

  test("rejects incompatible user shape with accepted-format guidance") {
    val schema = StructType(
      Seq(StructField("binary_vec", ArrayType(DoubleType), nullable = false))
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(Seq(1.0d, 2.0d)))),
      schema
    )
    val target = vectorField(
      "binary_vec",
      MilvusDataType.BinaryVector,
      Some(16)
    )

    val error = VectorBackfillSupport
      .normalizeVectorColumns(df, Map(target.name -> target))
      .left
      .toOption
      .get
    error.message should include("array<double>")
    error.message should include("array<integral byte>")
  }
}
