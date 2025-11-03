package serde

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Test suite for MilvusSchemaUtil
 */
class SchemaUtilTest extends AnyFunSuite with Matchers {

  test("Convert DataFrame schema to Arrow schema correctly") {
    val spark = SparkSession.builder()
      .appName("SchemaConversionTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._
      import com.zilliz.spark.connector.MilvusSchemaUtil

      // Test with various data types
      val df = Seq(
        (1L, 100, "name1", 3.14f, 2.718, true, Array(1, 2, 3), Array(0.1f, 0.2f, 0.3f)),
        (2L, 200, "name2", 1.41f, 1.732, false, Array(4, 5, 6), Array(0.4f, 0.5f, 0.6f))
      ).toDF("id", "age", "name", "score", "rating", "active", "tags", "embedding")

      // Convert without vector dimensions (embedding will be List)
      val arrowSchemaNoVec = MilvusSchemaUtil.convertSparkSchemaToArrow(df.schema)

      arrowSchemaNoVec should not be null
      arrowSchemaNoVec.getFields.size() shouldBe 8

      val fieldNames = arrowSchemaNoVec.getFields.asScala.map(_.getName)
      fieldNames should contain allOf("id", "age", "name", "score", "rating", "active", "tags", "embedding")

      info(s"Successfully converted schema with ${arrowSchemaNoVec.getFields.size()} fields")
      arrowSchemaNoVec.getFields.asScala.foreach { field =>
        info(s"  - ${field.getName}: ${field.getType}")
      }

      // Verify types
      val idField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "id").get
      idField.getType.toString should include("Int(64")

      val ageField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "age").get
      ageField.getType.toString should include("Int(32")

      val nameField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "name").get
      nameField.getType.toString should be("Utf8")

      val scoreField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "score").get
      scoreField.getType.toString should include("FloatingPoint")

      val ratingField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "rating").get
      ratingField.getType.toString should include("FloatingPoint")

      val activeField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "active").get
      activeField.getType.toString should be("Bool")

      val tagsField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "tags").get
      tagsField.getType.toString should be("List")

      val embeddingField = arrowSchemaNoVec.getFields.asScala.find(_.getName == "embedding").get
      embeddingField.getType.toString should be("List") // Without vector dimension config

    } finally {
      spark.stop()
    }
  }

  test("Convert float arrays to FixedSizeBinary when vector dimensions provided") {
    val spark = SparkSession.builder()
      .appName("VectorSchemaTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._
      import com.zilliz.spark.connector.MilvusSchemaUtil

      val df = Seq(
        (1L, "item1", Array(0.1f, 0.2f, 0.3f)),
        (2L, "item2", Array(0.4f, 0.5f, 0.6f))
      ).toDF("id", "name", "embedding")

      // Convert with vector dimensions
      val vectorDimensions = Map("embedding" -> 3)
      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(df.schema, vectorDimensions)

      arrowSchema should not be null
      arrowSchema.getFields.size() shouldBe 3

      val embeddingField = arrowSchema.getFields.asScala.find(_.getName == "embedding").get
      embeddingField.getType.toString should include("FixedSizeBinary")
      embeddingField.getType.toString should include("12") // 3 floats * 4 bytes

      info(s"Vector field converted to: ${embeddingField.getType}")

    } finally {
      spark.stop()
    }
  }

  test("Handle multiple vector fields with different dimensions") {
    val spark = SparkSession.builder()
      .appName("MultiVectorTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._
      import com.zilliz.spark.connector.MilvusSchemaUtil

      val df = Seq(
        (1L, Array(0.1f, 0.2f), Array(0.1f, 0.2f, 0.3f, 0.4f)),
        (2L, Array(0.3f, 0.4f), Array(0.5f, 0.6f, 0.7f, 0.8f))
      ).toDF("id", "vec2d", "vec4d")

      val vectorDimensions = Map(
        "vec2d" -> 2,
        "vec4d" -> 4
      )
      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(df.schema, vectorDimensions)

      arrowSchema.getFields.size() shouldBe 3

      val vec2dField = arrowSchema.getFields.asScala.find(_.getName == "vec2d").get
      vec2dField.getType.toString should include("FixedSizeBinary(8)") // 2 * 4 bytes

      val vec4dField = arrowSchema.getFields.asScala.find(_.getName == "vec4d").get
      vec4dField.getType.toString should include("FixedSizeBinary(16)") // 4 * 4 bytes

      info(s"Multiple vector fields converted correctly")
      info(s"  - vec2d: ${vec2dField.getType}")
      info(s"  - vec4d: ${vec4dField.getType}")

    } finally {
      spark.stop()
    }
  }

  test("Handle complex types (Map and Struct)") {
    val spark = SparkSession.builder()
      .appName("ComplexTypesTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._
      import com.zilliz.spark.connector.MilvusSchemaUtil
      import org.apache.spark.sql.types._

      // Create DataFrame with Map type
      val schema = StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("properties", MapType(StringType, IntegerType), nullable = true)
      ))

      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(schema)

      arrowSchema.getFields.size() shouldBe 2

      val propertiesField = arrowSchema.getFields.asScala.find(_.getName == "properties").get
      propertiesField.getType.toString should include("Map")

      info(s"Map type converted to: ${propertiesField.getType}")

    } finally {
      spark.stop()
    }
  }

  test("Handle all basic Spark types") {
    val spark = SparkSession.builder()
      .appName("AllTypesTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._
      import com.zilliz.spark.connector.MilvusSchemaUtil
      import org.apache.spark.sql.types._

      val schema = StructType(Seq(
        StructField("long_field", LongType),
        StructField("int_field", IntegerType),
        StructField("short_field", ShortType),
        StructField("byte_field", ByteType),
        StructField("float_field", FloatType),
        StructField("double_field", DoubleType),
        StructField("bool_field", BooleanType),
        StructField("string_field", StringType),
        StructField("binary_field", BinaryType),
        StructField("int_array", ArrayType(IntegerType)),
        StructField("long_array", ArrayType(LongType)),
        StructField("double_array", ArrayType(DoubleType)),
        StructField("string_array", ArrayType(StringType))
      ))

      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(schema)

      arrowSchema.getFields.size() shouldBe 13

      // Verify each type
      val typeMap = arrowSchema.getFields.asScala.map(f => f.getName -> f.getType.toString).toMap

      typeMap("long_field") should include("Int(64")
      typeMap("int_field") should include("Int(32")
      typeMap("short_field") should include("Int(16")
      typeMap("byte_field") should include("Int(8")
      typeMap("float_field") should include("FloatingPoint")
      typeMap("double_field") should include("FloatingPoint")
      typeMap("bool_field") should be("Bool")
      typeMap("string_field") should be("Utf8")
      typeMap("binary_field") should be("Binary")
      typeMap("int_array") should be("List")
      typeMap("long_array") should be("List")
      typeMap("double_array") should be("List")
      typeMap("string_array") should be("List")

      info(s"All basic types converted successfully")
      arrowSchema.getFields.asScala.foreach { field =>
        info(s"  - ${field.getName}: ${field.getType}")
      }

    } finally {
      spark.stop()
    }
  }

  test("Handle nullable fields correctly") {
    val spark = SparkSession.builder()
      .appName("NullableTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import com.zilliz.spark.connector.MilvusSchemaUtil
      import org.apache.spark.sql.types._

      val schema = StructType(Seq(
        StructField("required_field", LongType, nullable = false),
        StructField("optional_field", StringType, nullable = true)
      ))

      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(schema)

      arrowSchema.getFields.size() shouldBe 2

      // All Arrow fields are created as nullable=true by design
      val requiredField = arrowSchema.getFields.asScala.find(_.getName == "required_field").get
      requiredField.isNullable shouldBe true

      val optionalField = arrowSchema.getFields.asScala.find(_.getName == "optional_field").get
      optionalField.isNullable shouldBe true

      info(s"✓ Nullable fields handled correctly")

    } finally {
      spark.stop()
    }
  }
}
