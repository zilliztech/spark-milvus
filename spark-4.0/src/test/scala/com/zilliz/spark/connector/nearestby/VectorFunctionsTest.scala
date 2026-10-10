package com.zilliz.spark.connector.nearestby

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.extensions.{
  MilvusSparkSessionExtensions,
  VectorKernels
}

/** The connector's `vector_l2_distance`, `vector_cosine_similarity` and
  * `vector_inner_product`, which the lines before 4.2 register
  * (docs/design/architecture/dataframe-api.html section 6): Spark 4.2's NULL,
  * empty-array and dimension rules, and Spark 4.3's double sums.
  */
class VectorFunctionsTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val line =
    org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("vector-functions")
      .config(
        "spark.sql.extensions",
        classOf[MilvusSparkSessionExtensions].getName
      )
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val name = UTF8String.fromString("f")

  private def array(values: Float*) =
    UnsafeArrayData.fromPrimitiveArray(values.toArray)

  test("the kernels compute what the functions define") {
    VectorKernels.l2Distance(array(3f, 4f), array(0f, 0f), name) shouldBe 5f
    VectorKernels.innerProduct(
      array(1f, 2f, 3f),
      array(4f, 5f, 6f),
      name
    ) shouldBe
      32f
    // 32 / sqrt(1078), rounded once, as Spark 4.3 gives it.
    VectorKernels.cosineSimilarity(
      array(1f, 2f, 3f),
      array(4f, 5f, 6f),
      name
    ) shouldBe 0.97463185f
    // More than eight elements: the grouped sums and the tail.
    val nine = array((1 to 9).map(_.toFloat): _*)
    VectorKernels.innerProduct(nine, nine, name) shouldBe 285f
  }

  test("sums in double do not overflow or underflow where float sums would") {
    VectorKernels.l2Distance(array(3e19f, 4e19f), array(0f, 0f), name) shouldBe
      5e19f
    VectorKernels.cosineSimilarity(
      array(3e19f, 4e19f),
      array(3e19f, 4e19f),
      name
    ) shouldBe 1f
    VectorKernels.cosineSimilarity(
      array(1e-23f, 0f),
      array(1e-23f, 0f),
      name
    ) shouldBe 1f
  }

  test("NULL elements, empty arrays and zero vectors as Spark 4.2 has them") {
    val withNull = new GenericArrayData(Array[Any](1f, null))
    VectorKernels.l2Distance(withNull, array(1f, 2f), name) shouldBe null
    VectorKernels.innerProduct(array(), array(), name) shouldBe 0f
    VectorKernels.l2Distance(array(), array(), name) shouldBe 0f
    VectorKernels.cosineSimilarity(array(), array(), name) shouldBe null
    VectorKernels.cosineSimilarity(array(0f, 0f), array(1f, 2f), name) shouldBe
      null
    // A NaN or an infinity is a value, not NULL.
    VectorKernels
      .l2Distance(array(Float.NaN, 0f), array(1f, 2f), name)
      .isNaN shouldBe
      true
  }

  test("arrays of different lengths fail as Spark 4.2's do") {
    val failure = the[IllegalArgumentException] thrownBy
      VectorKernels.l2Distance(array(1f, 2f, 3f), array(1f, 2f), name)
    failure.getMessage should include("[VECTOR_DIMENSION_MISMATCH]")
    failure.getMessage should include(
      "Vectors passed to f must have the same dimension, but got 3 and 2."
    )
  }

  test("the lines before 4.2 register the three functions in SQL") {
    assume(line != "4.2", "Spark 4.2 has the three functions built in")
    val row = spark
      .sql(
        "SELECT vector_l2_distance(array(3.0F, 4.0F), array(0.0F, 0.0F)), " +
          "vector_inner_product(array(1.0F, 2.0F), array(3.0F, 4.0F)), " +
          "vector_cosine_similarity(array(1.0F, 0.0F), array(0.0F, 1.0F)), " +
          "vector_l2_distance(CAST(NULL AS ARRAY<FLOAT>), array(1.0F))"
      )
      .head()
    row.getFloat(0) shouldBe 5f
    row.getFloat(1) shouldBe 11f
    row.getFloat(2) shouldBe 0f
    row.isNullAt(3) shouldBe true
    val failure = the[Exception] thrownBy spark
      .sql("SELECT vector_l2_distance(array(1.0D), array(1.0D))")
      .collect()
    failure.getMessage should include("UNEXPECTED_INPUT_TYPE")
  }
}
