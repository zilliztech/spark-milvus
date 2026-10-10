package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  ExpressionInfo,
  Literal,
  RuntimeReplaceable,
  UnsafeArrayData
}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.index.RankingFunction
import com.zilliz.milvus.storage.schema.MetricType

/** `vector_l2_distance`, `vector_cosine_similarity` and `vector_inner_product`
  * on the Spark lines that do not have them built in, 3.5 to 4.1
  * (docs/design/architecture/dataframe-api.html section 6).
  *
  * Spark 4.2's NULL, empty-array and dimension rules apply: a NULL array is
  * NULL without a call, arrays of different lengths fail, an array with a NULL
  * element is NULL, two empty arrays are 0 for L2 and the inner product and
  * NULL for the cosine. The values are computed as Spark 4.3 computes them
  * (SPARK-58544): the floats are read as doubles, sums are taken in double, and
  * only the result is rounded to a float, so no intermediate sum overflows or
  * underflows; the cosine is NULL when the double product of the squared norms
  * is zero. `metric` names the function: L2, COSINE or IP.
  */
final case class VectorFunction(
    metric: MetricType,
    left: Expression,
    right: Expression
) extends RuntimeReplaceable
    with BinaryLike[Expression] {

  override def prettyName: String = VectorFunction.nameOf(metric)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
      case (ArrayType(FloatType, _), ArrayType(FloatType, _)) =>
        TypeCheckResult.TypeCheckSuccess
      case (ArrayType(FloatType, _), _) =>
        VectorFunction.unexpected("second", right)
      case _ => VectorFunction.unexpected("first", left)
    }

  override lazy val replacement: Expression = StaticInvoke(
    VectorKernels.getClass,
    FloatType,
    VectorFunction.kernelOf(metric),
    Seq(left, right, Literal(prettyName)),
    Seq(ArrayType(FloatType), ArrayType(FloatType), StringType)
  )

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): VectorFunction = copy(left = newLeft, right = newRight)
}

object VectorFunction {

  private val Names =
    Map(
      MetricType.L2 -> "vector_l2_distance",
      MetricType.Cosine -> "vector_cosine_similarity",
      MetricType.IP -> "vector_inner_product"
    )

  private val Kernels =
    Map(
      MetricType.L2 -> "l2Distance",
      MetricType.Cosine -> "cosineSimilarity",
      MetricType.IP -> "innerProduct"
    )

  def nameOf(metric: MetricType): String = Names.getOrElse(
    metric,
    throw new IllegalArgumentException(s"No vector function ranks by $metric")
  )

  private def kernelOf(metric: MetricType): String = Kernels(metric)

  private val Usage = Map(
    MetricType.L2 -> "_FUNC_(array1, array2) - Returns the Euclidean distance between two float arrays of the same length.",
    MetricType.Cosine -> "_FUNC_(array1, array2) - Returns the cosine similarity of two float arrays of the same length.",
    MetricType.IP -> "_FUNC_(array1, array2) - Returns the inner product of two float arrays of the same length."
  )

  /** The three functions as `SparkSessionExtensions.injectFunction` takes them.
    */
  def registrations: Seq[
    (FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)
  ] =
    Names.toSeq.sortBy(_._2).map { case (metric, name) =>
      (
        FunctionIdentifier(name),
        new ExpressionInfo(
          classOf[VectorFunction].getName,
          null,
          name,
          Usage(metric),
          "",
          "",
          "",
          "misc_funcs",
          "2.0.0",
          "",
          "built-in"
        ),
        (arguments: Seq[Expression]) => {
          if (arguments.size != 2)
            throw new IllegalArgumentException(
              s"$name takes two arguments, not ${arguments.size}"
            )
          VectorFunction(metric, arguments.head, arguments(1))
        }
      )
    }

  private def unexpected(position: String, argument: Expression) =
    DataTypeMismatch(
      errorSubClass = "UNEXPECTED_INPUT_TYPE",
      messageParameters = Map(
        "paramIndex" -> position,
        "requiredType" -> "\"ARRAY<FLOAT>\"",
        "inputSql" -> s"\"${argument.sql}\"",
        "inputType" -> s"\"${argument.dataType.sql}\""
      )
    )
}

/** The arithmetic of [[VectorFunction]], called through `StaticInvoke`. Each
  * kernel takes the arrays eight elements at a time and sums each group before
  * adding it, as Spark's own does, so the double sums round the same way.
  */
object VectorKernels {

  private def checkedLength(
      left: ArrayData,
      right: ArrayData,
      functionName: UTF8String
  ): Int = {
    val leftLength = left.numElements()
    val rightLength = right.numElements()
    if (leftLength != rightLength)
      throw new IllegalArgumentException(
        s"[VECTOR_DIMENSION_MISMATCH] Vectors passed to $functionName must have " +
          s"the same dimension, but got $leftLength and $rightLength. SQLSTATE: 22000"
      )
    leftLength
  }

  private def anyNull(left: ArrayData, right: ArrayData, from: Int): Boolean = {
    var i = from
    while (i < from + 8) {
      if (left.isNullAt(i) || right.isNullAt(i)) return true
      i += 1
    }
    false
  }

  def cosineSimilarity(
      left: ArrayData,
      right: ArrayData,
      functionName: UTF8String
  ): java.lang.Float = {
    val length = checkedLength(left, right, functionName)
    if (length == 0) return null
    var dot = 0.0d
    var leftSquares = 0.0d
    var rightSquares = 0.0d
    var i = 0
    val grouped = (length / 8) * 8
    while (i < grouped) {
      if (anyNull(left, right, i)) return null
      var groupDot = 0.0d
      var groupLeft = 0.0d
      var groupRight = 0.0d
      var j = i
      while (j < i + 8) {
        val a = left.getFloat(j).toDouble
        val b = right.getFloat(j).toDouble
        groupDot += a * b
        groupLeft += a * a
        groupRight += b * b
        j += 1
      }
      dot += groupDot
      leftSquares += groupLeft
      rightSquares += groupRight
      i += 8
    }
    while (i < length) {
      if (left.isNullAt(i) || right.isNullAt(i)) return null
      val a = left.getFloat(i).toDouble
      val b = right.getFloat(i).toDouble
      dot += a * b
      leftSquares += a * a
      rightSquares += b * b
      i += 1
    }
    val norms = math.sqrt(leftSquares * rightSquares)
    if (norms == 0.0d) null
    else java.lang.Float.valueOf((dot / norms).toFloat)
  }

  def innerProduct(
      left: ArrayData,
      right: ArrayData,
      functionName: UTF8String
  ): java.lang.Float = {
    val length = checkedLength(left, right, functionName)
    if (length == 0) return java.lang.Float.valueOf(0.0f)
    var dot = 0.0d
    var i = 0
    val grouped = (length / 8) * 8
    while (i < grouped) {
      if (anyNull(left, right, i)) return null
      var group = 0.0d
      var j = i
      while (j < i + 8) {
        group += left.getFloat(j).toDouble * right.getFloat(j).toDouble
        j += 1
      }
      dot += group
      i += 8
    }
    while (i < length) {
      if (left.isNullAt(i) || right.isNullAt(i)) return null
      dot += left.getFloat(i).toDouble * right.getFloat(i).toDouble
      i += 1
    }
    java.lang.Float.valueOf(dot.toFloat)
  }

  def l2Distance(
      left: ArrayData,
      right: ArrayData,
      functionName: UTF8String
  ): java.lang.Float = {
    val length = checkedLength(left, right, functionName)
    if (length == 0) return java.lang.Float.valueOf(0.0f)
    var squares = 0.0d
    var i = 0
    val grouped = (length / 8) * 8
    while (i < grouped) {
      if (anyNull(left, right, i)) return null
      var group = 0.0d
      var j = i
      while (j < i + 8) {
        val d = left.getFloat(j).toDouble - right.getFloat(j).toDouble
        group += d * d
        j += 1
      }
      squares += group
      i += 8
    }
    while (i < length) {
      if (left.isNullAt(i) || right.isNullAt(i)) return null
      val d = left.getFloat(i).toDouble - right.getFloat(i).toDouble
      squares += d * d
      i += 1
    }
    java.lang.Float.valueOf(math.sqrt(squares).toFloat)
  }
}

/** One of the three vector functions as a [[RankingFunction]]: the function
  * called on the query and the row in the order the ranking names them, and its
  * value on the engine's scale, so L2's distance is squared.
  */
abstract class VectorRanking(
    metric: MetricType,
    name: String,
    queryFirst: Boolean
) extends RankingFunction {

  /** The function's value, NULL as null. */
  protected def value(
      first: ArrayData,
      second: ArrayData,
      functionName: UTF8String
  ): java.lang.Float

  @transient private lazy val functionName = UTF8String.fromString(name)

  override final def score(
      query: Array[Float],
      row: Array[Float]
  ): java.lang.Double = {
    val queryArray = UnsafeArrayData.fromPrimitiveArray(query)
    val rowArray = UnsafeArrayData.fromPrimitiveArray(row)
    val result =
      if (queryFirst) value(queryArray, rowArray, functionName)
      else value(rowArray, queryArray, functionName)
    if (result == null) null
    else {
      val score = result.doubleValue()
      java.lang.Double.valueOf(
        if (metric == MetricType.L2) score * score else score
      )
    }
  }
}

/** The connector's own functions as a ranking, on the lines before 4.2. */
final case class ConnectorVectorRanking(
    metric: MetricType,
    name: String,
    queryFirst: Boolean
) extends VectorRanking(metric, name, queryFirst) {

  override protected def value(
      first: ArrayData,
      second: ArrayData,
      functionName: UTF8String
  ): java.lang.Float = metric match {
    case MetricType.L2 => VectorKernels.l2Distance(first, second, functionName)
    case MetricType.Cosine =>
      VectorKernels.cosineSimilarity(first, second, functionName)
    case MetricType.IP =>
      VectorKernels.innerProduct(first, second, functionName)
    case other =>
      throw new IllegalStateException(s"No vector function ranks by $other")
  }
}
