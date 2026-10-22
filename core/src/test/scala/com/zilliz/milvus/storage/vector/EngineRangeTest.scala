package com.zilliz.milvus.storage.vector

import java.lang.{Double => JavaDouble, Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.BitSet

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** Which vectors Knowhere scores as Spark's float functions do (design document
  * architecture/dataframe-api.html section 2).
  */
class EngineRangeTest extends AnyFunSuite with Matchers {

  import EngineRange.{Engine, Outside, Zero}

  private val float32 = VectorLayout(VectorElementType.Float32, 2)
  private val float16 = VectorLayout(VectorElementType.Float16, 2)
  private val bfloat16 = VectorLayout(VectorElementType.BFloat16, 2)

  private val TwoTo33 = math.pow(2, 33).toFloat
  private val TwoTo17 = math.pow(2, 17).toFloat
  private val TwoToMinus25 = math.pow(2, -25).toFloat

  test("L2 and IP take every finite vector whose elements are below 2^33") {
    Seq(MetricType.L2, MetricType.IP).foreach { metric =>
      def of(values: Float*) =
        EngineRange.query(values.toArray, float32, metric)
      of(1f, -2f) shouldBe Engine
      of(0f, -0f) shouldBe Engine
      of(Math.nextDown(TwoTo33), 0f) shouldBe Engine
      of(-Math.nextDown(TwoTo33), 0f) shouldBe Engine
      of(TwoTo33, 0f) shouldBe Outside
      of(0f, -TwoTo33) shouldBe Outside
      of(Float.NaN, 0f) shouldBe Outside
      of(1f, Float.NegativeInfinity) shouldBe Outside
    }
  }

  test(
    "COSINE takes a largest element in [2^-25, 2^17); all zeros has no value"
  ) {
    def of(values: Float*) =
      EngineRange.query(values.toArray, float32, MetricType.Cosine)
    of(0f, -0f) shouldBe Zero
    of(TwoToMinus25, 0f) shouldBe Engine
    of(Math.nextDown(TwoToMinus25), 0f) shouldBe Outside
    of(Math.nextDown(TwoTo17), -1f) shouldBe Engine
    of(TwoTo17, 0f) shouldBe Outside
    of(Float.NaN, 1f) shouldBe Outside
    of(Float.PositiveInfinity, 1f) shouldBe Outside
  }

  test("a float16 field judges a query as the float16 it is packed into") {
    // The bounds are where the conversion's rounding takes a value out.
    FloatConverter.toFloat16Bytes(65519f)
    an[Exception] should be thrownBy FloatConverter.toFloat16Bytes(65520f)
    FloatConverter.toFloat16Bytes(TwoToMinus25) shouldBe Seq[Byte](0, 0)
    FloatConverter.toFloat16Bytes(Math.nextUp(TwoToMinus25)) should not be
      Seq[Byte](0, 0)

    Seq(MetricType.L2, MetricType.IP).foreach { metric =>
      def of(values: Float*) =
        EngineRange.query(values.toArray, float16, metric)
      of(65519f, -65504f) shouldBe Engine
      of(65520f, 0f) shouldBe Outside
      of(Float.NaN, 0f) shouldBe Outside
      of(0f, 0f) shouldBe Engine
    }
    def cosine(values: Float*) =
      EngineRange.query(values.toArray, float16, MetricType.Cosine)
    cosine(TwoToMinus25, -TwoToMinus25) shouldBe Zero
    cosine(Math.nextUp(TwoToMinus25), 0f) shouldBe Engine
    cosine(65519f, 1f) shouldBe Engine
    cosine(65520f, 1f) shouldBe Outside
  }

  test("a bfloat16 field judges a query by the float32 bounds") {
    EngineRange.query(
      Array(Math.nextDown(TwoTo33), 0f),
      bfloat16,
      MetricType.L2
    ) shouldBe
      Engine
    EngineRange.query(
      Array(TwoTo33, 0f),
      bfloat16,
      MetricType.L2
    ) shouldBe Outside
    EngineRange.query(
      Array(TwoToMinus25, 0f),
      bfloat16,
      MetricType.Cosine
    ) shouldBe
      Engine
    EngineRange.query(Array(0f, 0f), bfloat16, MetricType.Cosine) shouldBe Zero
  }

  private def float32Rows(rows: Seq[Float]*): ByteBuffer = {
    val buffer = ByteBuffer
      .allocate(rows.map(_.size).sum * 4)
      .order(ByteOrder.nativeOrder())
    rows.flatten.foreach(buffer.putFloat)
    buffer
  }

  private def bits(rows: Seq[Int]*): ByteBuffer = {
    val buffer =
      ByteBuffer.allocate(rows.flatten.size * 2).order(ByteOrder.nativeOrder())
    rows.flatten.foreach(value => buffer.putShort(value.toShort))
    buffer
  }

  private def excluding(rows: Int*): BitSet = {
    val set = new BitSet()
    rows.foreach(set.set)
    set
  }

  test("a batch's zero rows are skipped and its rows outside are ranked") {
    val buffer = float32Rows(
      Seq(1f, 2f),
      Seq(0f, -0f),
      Seq(Float.NaN, 1f),
      Seq(math.pow(2, 40).toFloat, 0f),
      // Excluded already, so never read.
      Seq(Float.NaN, Float.NaN)
    )
    val (cosineExcluded, cosineRanked, cosineNorm) =
      EngineRange.classify(buffer, 5, excluding(4), float32, MetricType.Cosine)
    cosineRanked.toSeq shouldBe Seq(2, 3)
    (0 until 5).filter(cosineExcluded.get) shouldBe Seq(1, 2, 3, 4)
    // The norm bounds the batched entry's L2 only.
    cosineNorm shouldBe 0.0

    val (l2Excluded, l2Ranked, l2Norm) =
      EngineRange.classify(buffer, 5, excluding(4), float32, MetricType.L2)
    l2Ranked.toSeq shouldBe Seq(2, 3)
    (0 until 5).filter(l2Excluded.get) shouldBe Seq(2, 3, 4)
    // Of the rows Knowhere scores under L2, (1, 2) is the longest; the NaN and
    // the too large rows are the function's, and the excluded one is not read.
    l2Norm shouldBe 5.0
  }

  test("float32 L2 norms keep the scalar formula's bits and exclusions") {
    val rows = Seq(
      Seq(0f, -0.0f),
      Seq(1.25f, -2.5f, 3.75f),
      Seq(
        JavaFloat.MIN_VALUE,
        -JavaFloat.MIN_NORMAL,
        Math.nextDown(JavaFloat.MIN_NORMAL)
      ),
      // Adding each 1 after 2^54 rounds it away; adding them first does not.
      Seq(134217728f, 1f, 1f, 1f, 1f),
      Seq(1f, 1f, 1f, 1f, 134217728f),
      Seq(Math.nextDown(TwoTo33), -Math.nextDown(TwoTo33), 1f),
      Seq(TwoTo33, 1f),
      Seq(-TwoTo33, 1f),
      Seq(JavaFloat.MAX_VALUE, 1f),
      Seq(Float.NaN, 1f),
      Seq(Float.PositiveInfinity, 1f),
      Seq(Float.NegativeInfinity, 1f)
    )
    rows.foreach { values =>
      withClue(s"$values: ") {
        val inRange = values.forall(value =>
          JavaFloat.isFinite(value) && math.abs(value) < TwoTo33
        )
        val expected =
          if (inRange)
            values.foldLeft(0.0) { (sum, value) =>
              sum + value.toDouble * value.toDouble
            }
          else 0.0
        val (excluded, ranked, norm) = EngineRange.classify(
          float32Rows(values),
          1,
          new BitSet(),
          VectorLayout(VectorElementType.Float32, values.size),
          MetricType.L2
        )
        excluded shouldBe (if (inRange) new BitSet() else excluding(0))
        ranked.toSeq shouldBe (if (inRange) Seq.empty[Int] else Seq(0))
        JavaDouble.doubleToRawLongBits(norm) shouldBe
          JavaDouble.doubleToRawLongBits(expected)
      }
    }

    // Even an in-range row with a larger norm is not read when excluded.
    val buffer = float32Rows(Seq(3f, 4f), Seq(100f, 100f))
    buffer.limit(float32.rowBytes)
    val (excluded, ranked, norm) =
      EngineRange.classify(buffer, 2, excluding(1), float32, MetricType.L2)
    excluded shouldBe excluding(1)
    ranked shouldBe empty
    norm shouldBe 25.0
  }

  test("float16 and bfloat16 rows are judged in their own bits") {
    // float16: 1.0, -0, +0, infinity, the smallest subnormal.
    val half = bits(
      Seq(0x3c00, 0x8000),
      Seq(0x0000, 0x8000),
      Seq(0x7c00, 0x3c00),
      Seq(0x0001, 0)
    )
    val (halfExcluded, halfRanked, _) =
      EngineRange.classify(half, 4, new BitSet(), float16, MetricType.Cosine)
    halfRanked.toSeq shouldBe Seq(2)
    (0 until 4).filter(halfExcluded.get) shouldBe Seq(1, 2)

    def bfloat(value: Float) = JavaFloat.floatToRawIntBits(value) >>> 16
    val brain = bits(
      Seq(bfloat(1f), bfloat(-2f)),
      Seq(bfloat(math.pow(2, 40).toFloat), 0),
      Seq(bfloat(Float.NaN), 0)
    )
    val (brainExcluded, brainRanked, _) =
      EngineRange.classify(brain, 3, new BitSet(), bfloat16, MetricType.IP)
    brainRanked.toSeq shouldBe Seq(1, 2)
    (0 until 3).filter(brainExcluded.get) shouldBe Seq(1, 2)
  }

  test("a row decodes to the floats a Spark row of the field holds") {
    val into = new Array[Float](2)
    EngineRange.decode(
      float32Rows(Seq(1f, 2f), Seq(-3f, Float.NaN)),
      1,
      float32,
      into
    )
    into(0) shouldBe -3f
    JavaFloat.isNaN(into(1)) shouldBe true

    EngineRange.decode(bits(Seq(0x3c00, 0xc000)), 0, float16, into)
    into.toSeq shouldBe Seq(1f, -2f)

    EngineRange.decode(bits(Seq(0x3f80, 0x7f80)), 0, bfloat16, into)
    into.toSeq shouldBe Seq(1f, Float.PositiveInfinity)
  }
}
