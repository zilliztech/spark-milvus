package com.zilliz.spark.connector.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.ml.linalg.{Vector, DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.linalg.{Vector => OldVector, DenseVector => OldDenseVector, SparseVector => OldSparseVector}

/**
 * Base trait for binary vector expressions
 */
abstract class BinaryVectorExpression(
    _left: Expression,
    _right: Expression)
  extends BinaryExpression with CodegenFallback with Serializable {
  
  override def left: Expression = _left
  override def right: Expression = _right
  
  // Thread-local cache for vector conversions to avoid repeated conversions
  @transient private var cachedLeftVector: Vector = _
  @transient private var cachedRightVector: Vector = _
  @transient private var lastLeftValue: Any = _
  @transient private var lastRightValue: Any = _
  
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  
  protected def validateVectorTypes(leftType: DataType, rightType: DataType): Boolean = {
    import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
    
    (leftType, rightType) match {
      // Spark ML Vector types (preferred)
      case (VectorType, VectorType) => true
      case (VectorType, ArrayType(_, _)) => true
      case (ArrayType(_, _), VectorType) => true
      
      // Array types (fallback)
      case (ArrayType(FloatType, _), ArrayType(FloatType, _)) => true
      case (ArrayType(DoubleType, _), ArrayType(DoubleType, _)) => true
      case (ArrayType(IntegerType, _), ArrayType(IntegerType, _)) => true
      case (ArrayType(LongType, _), ArrayType(LongType, _)) => true
      case (ArrayType(DecimalType(), _), ArrayType(DecimalType(), _)) => true
      case (ArrayType(FloatType, _), ArrayType(DoubleType, _)) => true
      case (ArrayType(DoubleType, _), ArrayType(FloatType, _)) => true
      case (ArrayType(_, _), ArrayType(_, _)) => true // Allow mixed numeric types
      case _ => false
    }
  }
  
  protected def extractVector(value: Any): Vector = {
    value match {
      // Spark ML Vector (preferred)
      case vector: Vector => vector
      
      // Spark MLlib Vector (legacy)
      case oldVector: OldVector => 
        oldVector match {
          case dense: OldDenseVector => Vectors.dense(dense.values)
          case sparse: OldSparseVector => Vectors.sparse(sparse.size, sparse.indices, sparse.values)
        }
      
      // Array data - convert to DenseVector (optimized)
      case arrayData: ArrayData =>
        extractVectorFromArrayData(arrayData)
        
      case _ => null
    }
  }
  
  // Cached vector extraction with reference equality check
  protected def extractVectorCached(value: Any, isLeft: Boolean): Vector = {
    if (isLeft) {
      if (lastLeftValue != null && (lastLeftValue.asInstanceOf[AnyRef] eq value.asInstanceOf[AnyRef])) {
        cachedLeftVector
      } else {
        cachedLeftVector = extractVector(value)
        lastLeftValue = value
        cachedLeftVector
      }
    } else {
      if (lastRightValue != null && (lastRightValue.asInstanceOf[AnyRef] eq value.asInstanceOf[AnyRef])) {
        cachedRightVector
      } else {
        cachedRightVector = extractVector(value)
        lastRightValue = value
        cachedRightVector
      }
    }
  }
  
  // Optimized method to extract vector from ArrayData
  private def extractVectorFromArrayData(arrayData: ArrayData): Vector = {
    val size = arrayData.numElements()
    val values = new Array[Double](size)
    
    // Optimized extraction - try double first, then fallback
    var i = 0
    try {
      // Assume most arrays are double arrays
      while (i < size) {
        values(i) = arrayData.getDouble(i)
        i += 1
      }
    } catch {
      case _: Exception =>
        // Fallback to generic approach
        i = 0
        while (i < size) {
          val element = arrayData.get(i, org.apache.spark.sql.types.DoubleType)
          values(i) = convertToDouble(element)
          i += 1
        }
    }
    
    Vectors.dense(values)
  }
  
  // Keep the old method for backward compatibility
  protected def extractFloatArray(value: Any): Array[Float] = {
    val vector = extractVector(value)
    if (vector == null) {
      null
    } else {
      vector.toArray.map(_.toFloat)
    }
  }
  
  protected def extractDoubleArray(value: Any): Array[Double] = {
    val vector = extractVector(value)
    if (vector == null) {
      null
    } else {
      vector.toArray
    }
  }
  
  protected def convertToFloat(value: Any): Float = {
    value match {
      case f: Float => f
      case d: Double => d.toFloat
      case i: Int => i.toFloat
      case l: Long => l.toFloat
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toDouble.toFloat
      case null => 0.0f
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Float: $value")
    }
  }
  
  protected def convertToDouble(value: Any): Double = {
    value match {
      case f: Float => f.toDouble
      case d: Double => d
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toDouble
      case null => 0.0
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Double: $value")
    }
  }
}

/**
 * Cosine Similarity Expression
 */
case class CosineSimilarityExpression(
    _left: Expression,
    _right: Expression)
  extends BinaryVectorExpression(_left, _right) with Serializable {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(_left = newLeft, _right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftVector = extractVectorCached(leftValue, isLeft = true)
    val rightVector = extractVectorCached(rightValue, isLeft = false)
    
    if (leftVector == null || rightVector == null || leftVector.size != rightVector.size) {
      null
    } else {
      computeCosineSimilarity(leftVector, rightVector)
    }
  }
  
  private def computeCosineSimilarity(v1: Vector, v2: Vector): Double = {
    if (v1.size != v2.size) return 0.0
    
    // Optimized computation using raw arrays for better performance
    val arr1 = v1.toArray
    val arr2 = v2.toArray
    val size = arr1.length
    
    var dotProduct = 0.0
    var norm1Sq = 0.0
    var norm2Sq = 0.0
    
    // Single loop to compute all values
    var i = 0
    while (i < size) {
      val v1Val = arr1(i)
      val v2Val = arr2(i)
      dotProduct += v1Val * v2Val
      norm1Sq += v1Val * v1Val
      norm2Sq += v2Val * v2Val
      i += 1
    }
    
    if (norm1Sq == 0.0 || norm2Sq == 0.0) {
      0.0
    } else {
      dotProduct / (math.sqrt(norm1Sq) * math.sqrt(norm2Sq))
    }
  }
  
  override def prettyName: String = "cosine_similarity"
}

/**
 * L2 Distance Expression
 */
case class L2DistanceExpression(
    _left: Expression,
    _right: Expression)
  extends BinaryVectorExpression(_left, _right) with Serializable {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(_left = newLeft, _right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftVector = extractVectorCached(leftValue, isLeft = true)
    val rightVector = extractVectorCached(rightValue, isLeft = false)
    
    if (leftVector == null || rightVector == null || leftVector.size != rightVector.size) {
      null
    } else {
      computeL2Distance(leftVector, rightVector)
    }
  }
  
  private def computeL2Distance(v1: Vector, v2: Vector): Double = {
    if (v1.size != v2.size) return Double.MaxValue
    
    // Optimized computation using raw arrays for better performance
    val arr1 = v1.toArray
    val arr2 = v2.toArray
    val size = arr1.length
    
    var sum = 0.0
    var i = 0
    while (i < size) {
      val diff = arr1(i) - arr2(i)
      sum += diff * diff
      i += 1
    }
    math.sqrt(sum)
  }
  
  override def prettyName: String = "l2_distance"
}

/**
 * Inner Product Expression
 */
case class InnerProductExpression(
    _left: Expression,
    _right: Expression)
  extends BinaryVectorExpression(_left, _right) with Serializable {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(_left = newLeft, _right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftVector = extractVector(leftValue)
    val rightVector = extractVector(rightValue)
    
    if (leftVector == null || rightVector == null || leftVector.size != rightVector.size) {
      null
    } else {
      computeInnerProduct(leftVector, rightVector)
    }
  }
  
  private def computeInnerProduct(v1: Vector, v2: Vector): Double = {
    if (v1.size != v2.size) return 0.0
    
    // Use optimized Vector dot product like VectorBruteForceSearch
    v1.dot(v2)
  }
  
  override def prettyName: String = "inner_product"
}

/**
 * Hamming Distance Expression for binary vectors
 */
case class HammingDistanceExpression(
    _left: Expression,
    _right: Expression)
  extends BinaryVectorExpression(_left, _right) with Serializable {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(_left = newLeft, _right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractByteArray(leftValue)
    val rightArray = extractByteArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeHammingDistance(leftArray, rightArray)
    }
  }
  
  private def extractByteArray(value: Any): Array[Byte] = {
    value match {
      case arrayData: ArrayData =>
        val size = arrayData.numElements()
        val result = new Array[Byte](size)
        var i = 0
        while (i < size) {
          val element = arrayData.get(i, org.apache.spark.sql.types.ByteType)
          result(i) = convertToByte(element)
          i += 1
        }
        result
      case _ => null
    }
  }
  
  private def convertToByte(value: Any): Byte = {
    value match {
      case b: Byte => b
      case i: Int => i.toByte
      case l: Long => l.toByte
      case f: Float => f.toByte
      case d: Double => d.toByte
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toInt.toByte
      case null => 0.toByte
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Byte: $value")
    }
  }
  
  private def computeHammingDistance(v1: Array[Byte], v2: Array[Byte]): Double = {
    if (v1.length != v2.length) return Double.MaxValue
    
    var distance = 0
    var i = 0
    while (i < v1.length) {
      val xor = v1(i) ^ v2(i)
      distance += java.lang.Integer.bitCount(xor & 0xff)
      i += 1
    }
    
    distance.toDouble
  }
  
  override def prettyName: String = "hamming_distance"
}

/**
 * Jaccard Distance Expression for binary vectors
 */
case class JaccardDistanceExpression(
    _left: Expression,
    _right: Expression)
  extends BinaryVectorExpression(_left, _right) with Serializable {
  
  override protected def withNewChildrenInternal(
    newLeft: Expression, 
    newRight: Expression
  ): Expression = copy(_left = newLeft, _right = newRight)
  
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = extractByteArray(leftValue)
    val rightArray = extractByteArray(rightValue)
    
    if (leftArray == null || rightArray == null || leftArray.length != rightArray.length) {
      null
    } else {
      computeJaccardDistance(leftArray, rightArray)
    }
  }
  
  private def extractByteArray(value: Any): Array[Byte] = {
    value match {
      case arrayData: ArrayData =>
        val size = arrayData.numElements()
        val result = new Array[Byte](size)
        var i = 0
        while (i < size) {
          val element = arrayData.get(i, org.apache.spark.sql.types.ByteType)
          result(i) = convertToByte(element)
          i += 1
        }
        result
      case _ => null
    }
  }
  
  private def convertToByte(value: Any): Byte = {
    value match {
      case b: Byte => b
      case i: Int => i.toByte
      case l: Long => l.toByte
      case f: Float => f.toByte
      case d: Double => d.toByte
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toInt.toByte
      case null => 0.toByte
      case _ => throw new IllegalArgumentException(s"Cannot convert ${value.getClass} to Byte: $value")
    }
  }
  
  private def computeJaccardDistance(v1: Array[Byte], v2: Array[Byte]): Double = {
    if (v1.length != v2.length) return 1.0 // Maximum distance
    
    var intersection = 0
    var union = 0
    
    var i = 0
    while (i < v1.length) {
      val and = v1(i) & v2(i)
      val or = v1(i) | v2(i)
      intersection += java.lang.Integer.bitCount(and & 0xff)
      union += java.lang.Integer.bitCount(or & 0xff)
      i += 1
    }
    
    if (union == 0) 0.0 else 1.0 - (intersection.toDouble / union.toDouble)
  }
  
  override def prettyName: String = "jaccard_distance"
}

/**
 * Vector KNN Expression - simplified version
 * This is a basic implementation for demonstration
 */
case class VectorKNNExpression(
  vectors: Expression,
  queryVector: Expression, 
  k: Expression,
  distanceType: Expression
) extends Expression with CodegenFallback with Serializable {
  
  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]
  ): Expression = {
    require(newChildren.length == 4)
    copy(
      vectors = newChildren(0),
      queryVector = newChildren(1),
      k = newChildren(2),
      distanceType = newChildren(3)
    )
  }
  
  override def children: Seq[Expression] = Seq(vectors, queryVector, k, distanceType)
  override def dataType: DataType = ArrayType(StructType(Seq(
    StructField("index", IntegerType, false),
    StructField("score", DoubleType, false)
  )))
  override def nullable: Boolean = true
  
  override def eval(input: InternalRow): Any = {
    // This is a simplified implementation
    // In practice, this would need more sophisticated logic
    null
  }
  
  override def prettyName: String = "vector_knn"
}