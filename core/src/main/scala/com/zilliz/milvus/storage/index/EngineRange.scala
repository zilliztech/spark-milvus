package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.ByteBuffer
import java.util.BitSet

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** Which vectors Knowhere scores the way a [[RankingFunction]] computed in
  * float does (docs/design/architecture/dataframe-api.html section 2).
  *
  * Knowhere computes in float32: L2 as |x|^2 + |y|^2 - 2x.y over a whole batch
  * (decision 27), the inner product as a dot product, COSINE over the vectors'
  * norms. Spark's vector functions accumulate in float. A vector is in range
  * when the largest magnitude among its elements, in the element type Knowhere
  * receives, is
  *
  *   - for L2 and IP, finite and below 2^33, so that no square, product or sum
  *     of up to 32,768 of them overflows either computation;
  *   - for COSINE, at least 2^-25 and below 2^17, so that the squared norm lies
  *     in [2^-50, 2^49] and the product of two such norms, which Spark's cosine
  *     divides by, stays far from float overflow and from the underflow that
  *     makes Spark's cosine NULL. A float16 vector is in range when it is
  *     finite and not all zero: its elements lie in [2^-24, 65504].
  *
  * Between two vectors in range the engine's score and the function's differ by
  * rounding alone. Under COSINE a vector whose every element is zero has no
  * value with a vector in range, the product of the squared norms being zero.
  * Every other vector is ranked by the function. The test is one integer
  * comparison per element.
  */
object EngineRange {

  /** Knowhere scores it. */
  final val Engine = 0

  /** Under COSINE every element is zero: no value with any vector in range. */
  final val Zero = 1

  /** The function scores it. */
  final val Outside = 2

  // Magnitude bits (the sign cleared) of float32 values at the bounds.
  private final val Float32Below2To33 = 0x50000000
  private final val Float32From2ToMinus25 = 0x33000000
  private final val Float32Below2To17 = 0x48000000

  /** A float16's magnitude bits from infinity up. */
  private final val Float16NonFinite = 0x7c00

  /** The float32 inputs that the float16 conversion (round to nearest even,
    * `FloatConverter.toFloat16Bytes`) takes out of range: 65520 and above round
    * to infinity, 2^-25 and below to zero.
    */
  private final val Float16RoundsToInfinity = 0x477ff000
  private final val Float16RoundsToZero = 0x33000000

  private def float32(largest: Int, metric: MetricType): Int =
    if (metric == MetricType.Cosine) {
      if (largest == 0) Zero
      else if (largest >= Float32From2ToMinus25 && largest < Float32Below2To17)
        Engine
      else Outside
    } else if (largest < Float32Below2To33) Engine
    else Outside

  private def float16(largest: Int, metric: MetricType): Int =
    if (largest >= Float16NonFinite) Outside
    else if (largest == 0 && metric == MetricType.Cosine) Zero
    else Engine

  /** A query given as floats, judged as the field's element type will hold it
    * once packed. A bfloat16 keeps the float's top 16 bits, and the float32
    * bounds have none below them, so the float32 test gives the same answer.
    */
  def query(
      values: Array[Float],
      layout: VectorLayout,
      metric: MetricType
  ): Int = {
    require(
      values.length == layout.dimension,
      s"A query of ${values.length} values for a field of ${layout.dimension} dimensions"
    )
    var largest = 0
    var at = 0
    while (at < values.length) {
      val magnitude = JavaFloat.floatToRawIntBits(values(at)) & 0x7fffffff
      if (magnitude > largest) largest = magnitude
      at += 1
    }
    layout.elementType match {
      case VectorElementType.Float32 | VectorElementType.BFloat16 =>
        float32(largest, metric)
      case VectorElementType.Float16 =>
        if (largest >= Float16RoundsToInfinity) Outside
        else if (largest <= Float16RoundsToZero && metric == MetricType.Cosine)
          Zero
        else Engine
      case other =>
        throw new IllegalArgumentException(
          s"A ranking function takes float vectors, not $other"
        )
    }
  }

  /** The class of one row of `buffer`, read in the field's element type. */
  private def row(
      buffer: ByteBuffer,
      offset: Int,
      layout: VectorLayout,
      metric: MetricType
  ): Int = {
    val end = offset + layout.rowBytes
    var largest = 0
    var at = offset
    layout.elementType match {
      case VectorElementType.Float32 =>
        while (at < end) {
          val magnitude = buffer.getInt(at) & 0x7fffffff
          if (magnitude > largest) largest = magnitude
          at += 4
        }
        float32(largest, metric)
      case VectorElementType.BFloat16 =>
        while (at < end) {
          val magnitude = buffer.getShort(at) & 0x7fff
          if (magnitude > largest) largest = magnitude
          at += 2
        }
        float32(largest << 16, metric)
      case VectorElementType.Float16 =>
        while (at < end) {
          val magnitude = buffer.getShort(at) & 0x7fff
          if (magnitude > largest) largest = magnitude
          at += 2
        }
        float16(largest, metric)
      case other =>
        throw new IllegalArgumentException(
          s"A ranking function takes float vectors, not $other"
        )
    }
  }

  /** Row `row` of `buffer` as the floats a Spark row of the field holds. */
  def decode(
      buffer: ByteBuffer,
      row: Int,
      layout: VectorLayout,
      into: Array[Float]
  ): Unit = {
    require(
      into.length == layout.dimension,
      s"${into.length} floats for a vector of ${layout.dimension}"
    )
    var at = row * layout.rowBytes
    var element = 0
    layout.elementType match {
      case VectorElementType.Float32 =>
        while (element < into.length) {
          into(element) = buffer.getFloat(at)
          element += 1
          at += 4
        }
      case VectorElementType.Float16 =>
        while (element < into.length) {
          into(element) =
            FloatConverter.float16BitsToFloat(buffer.getShort(at) & 0xffff)
          element += 1
          at += 2
        }
      case VectorElementType.BFloat16 =>
        while (element < into.length) {
          into(element) =
            FloatConverter.bfloat16BitsToFloat(buffer.getShort(at) & 0xffff)
          element += 1
          at += 2
        }
      case other =>
        throw new IllegalArgumentException(
          s"A ranking function takes float vectors, not $other"
        )
    }
  }

  /** One batch as a search with a ranking function sees it: worked out once and
    * used by every query group the batch is searched for.
    *
    * @param engineExcluded
    *   the rows Knowhere does not score: the batch's own exclusions, its zero
    *   rows under COSINE, and the rows the function scores
    * @param rankedRows
    *   the batch rows the function scores, ascending
    * @param rankedValues
    *   their vectors as floats, in the same order
    */
  final class Batch private[index] (
      val function: RankingFunction,
      val engineExcluded: BitSet,
      val engineRows: Int,
      val rankedRows: Array[Int],
      val rankedValues: Array[Array[Float]]
  )

  object Batch {

    /** Reads every row the batch offers once, on the JVM. */
    def of(
        batch: VectorBatch,
        layout: VectorLayout,
        metric: MetricType,
        function: RankingFunction
    ): Batch = {
      val buffer = batch.base.buffer
      val (excluded, ranked) =
        classify(buffer, batch.rows, batch.excluded, layout, metric)
      val values = ranked.map { at =>
        val vector = new Array[Float](layout.dimension)
        decode(buffer, at, layout, vector)
        vector
      }
      new Batch(
        function,
        excluded,
        batch.rows - excluded.get(0, batch.rows).cardinality(),
        ranked,
        values
      )
    }
  }

  /** The rows of `buffer` Knowhere does not score -- `excluded`, the zero rows
    * under COSINE, the rows outside the range -- and, of those, the rows the
    * function scores. A row already excluded is not read.
    */
  private[index] def classify(
      buffer: ByteBuffer,
      rows: Int,
      excluded: BitSet,
      layout: VectorLayout,
      metric: MetricType
  ): (BitSet, Array[Int]) = {
    val engineExcluded = excluded.clone().asInstanceOf[BitSet]
    val ranked = Array.newBuilder[Int]
    var current = excluded.nextClearBit(0)
    while (current < rows) {
      row(buffer, current * layout.rowBytes, layout, metric) match {
        case Engine =>
        case Zero   => engineExcluded.set(current)
        case _ =>
          engineExcluded.set(current)
          ranked += current
      }
      current = excluded.nextClearBit(current + 1)
    }
    (engineExcluded, ranked.result())
  }
}
