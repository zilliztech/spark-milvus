package com.zilliz.spark.connector.filter

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VectorBruteForceSearch {

  // Distance metric type enumeration
  object DistanceType extends Enumeration {
    type DistanceType = Value
    val COSINE, L2, IP, HAMMING, JACCARD, DEFAULT = Value
  }

  // Search type enumeration
  object SearchType extends Enumeration {
    type SearchType = Value
    val KNN, RANGE = Value
  }

  // Vector type enumeration
  object VectorType extends Enumeration {
    type VectorType = Value
    val DENSE_FLOAT, BINARY, SPARSE = Value
  }

  import DistanceType._
  import SearchType._
  import VectorType._

  // Cosine similarity calculation
  private def cosineSimilarity(v1: Vector, v2: Vector): Double = {
    val dotProduct = v1.dot(v2)
    val normV1 = Vectors.norm(v1, 2.0)
    val normV2 = Vectors.norm(v2, 2.0)

    if (normV1 == 0.0 || normV2 == 0.0) {
      0.0
    } else {
      dotProduct / (normV1 * normV2)
    }
  }

  // L2 distance calculation
  private def l2Distance(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size, "Vectors must have the same size")
    var sum = 0.0
    for (i <- 0 until v1.size) {
      val diff = v1(i) - v2(i)
      sum += diff * diff
    }
    scala.math.sqrt(sum)
  }

  // Inner product calculation
  private def innerProduct(v1: Vector, v2: Vector): Double = {
    v1.dot(v2)
  }

  // Hamming distance calculation for binary vectors
  private def hammingDistance(v1: Array[Byte], v2: Array[Byte]): Double = {
    require(v1.length == v2.length, "Binary vectors must have the same length")
    var distance = 0
    for (i <- v1.indices) {
      // XOR and count set bits
      val xor = v1(i) ^ v2(i)
      distance += java.lang.Integer.bitCount(xor & 0xff)
    }
    distance.toDouble
  }

  // Jaccard distance calculation for binary vectors
  private def jaccardDistance(v1: Array[Byte], v2: Array[Byte]): Double = {
    require(v1.length == v2.length, "Binary vectors must have the same length")
    var intersection = 0
    var union = 0

    for (i <- v1.indices) {
      val and = v1(i) & v2(i)
      val or = v1(i) | v2(i)
      intersection += java.lang.Integer.bitCount(and & 0xff)
      union += java.lang.Integer.bitCount(or & 0xff)
    }

    if (union == 0) 0.0 else 1.0 - (intersection.toDouble / union.toDouble)
  }

  // Sparse vector cosine similarity
  private def sparseCosineSimilarity(
      v1: SparseVector,
      v2: SparseVector
  ): Double = {
    val dotProduct = v1.dot(v2)
    val normV1 = Vectors.norm(v1, 2.0)
    val normV2 = Vectors.norm(v2, 2.0)

    if (normV1 == 0.0 || normV2 == 0.0) {
      0.0
    } else {
      dotProduct / (normV1 * normV2)
    }
  }

  // Sparse vector L2 distance
  private def sparseL2Distance(v1: SparseVector, v2: SparseVector): Double = {
    require(v1.size == v2.size, "Sparse vectors must have the same size")

    // Convert to dense for calculation (could be optimized)
    val dense1 = v1.toDense
    val dense2 = v2.toDense
    l2Distance(dense1, dense2)
  }

  // Sparse vector inner product
  private def sparseInnerProduct(v1: SparseVector, v2: SparseVector): Double = {
    v1.dot(v2)
  }

  // Unified distance calculation function for dense vectors
  private def calculateDistance(
      v1: Vector,
      v2: Vector,
      distanceType: DistanceType
  ): Double = {
    distanceType match {
      case COSINE => cosineSimilarity(v1, v2)
      case L2     => l2Distance(v1, v2)
      case IP     => innerProduct(v1, v2)
      case _ =>
        throw new IllegalArgumentException(
          s"Distance type $distanceType not supported for dense vectors"
        )
    }
  }

  // Distance calculation for binary vectors
  private def calculateBinaryDistance(
      v1: Array[Byte],
      v2: Array[Byte],
      distanceType: DistanceType
  ): Double = {
    distanceType match {
      case HAMMING => hammingDistance(v1, v2)
      case JACCARD => jaccardDistance(v1, v2)
      case _ =>
        throw new IllegalArgumentException(
          s"Distance type $distanceType not supported for binary vectors"
        )
    }
  }

  // Distance calculation for sparse vectors
  private def calculateSparseDistance(
      v1: SparseVector,
      v2: SparseVector,
      distanceType: DistanceType
  ): Double = {
    distanceType match {
      case COSINE => sparseCosineSimilarity(v1, v2)
      case L2     => sparseL2Distance(v1, v2)
      case IP     => sparseInnerProduct(v1, v2)
      case _ =>
        throw new IllegalArgumentException(
          s"Distance type $distanceType not supported for sparse vectors"
        )
    }
  }

  // Check if distance meets threshold condition
  private def meetsThreshold(
      distance: Double,
      threshold: Double,
      distanceType: DistanceType
  ): Boolean = {
    distanceType match {
      case COSINE =>
        distance >= threshold // Cosine similarity, higher values are more similar
      case L2 =>
        distance <= threshold // L2 distance, lower values are more similar
      case IP =>
        distance >= threshold // Inner product, higher values are more similar
      case HAMMING =>
        distance <= threshold // Hamming distance, lower values are more similar
      case JACCARD =>
        distance <= threshold // Jaccard distance, lower values are more similar
    }
  }

  // UDF for converting Array[Float] to DenseVector
  private val arrayFloatToDenseVectorUDF = udf((arr: Seq[Float]) => {
    if (arr == null) {
      null
    } else {
      Vectors.dense(arr.map(_.toDouble).toArray)
    }
  })

  // UDF for converting Array[Byte] to binary vector (keep as Array[Byte])
  private val arrayByteToBinaryVectorUDF = udf((arr: Seq[Byte]) => {
    if (arr == null) {
      null
    } else {
      arr.toArray
    }
  })

  // UDF for converting Map[Long, Float] to SparseVector
  private val mapToSparseVectorUDF =
    udf((sparseMap: Map[Long, Float], size: Int) => {
      if (sparseMap == null || sparseMap.isEmpty) {
        null
      } else {
        val indices = sparseMap.keys.toArray.map(_.toInt).sorted
        val values = indices.map(i => sparseMap(i.toLong).toDouble)
        Vectors.sparse(size, indices, values).asInstanceOf[SparseVector]
      }
    })

  // Get default distance type based on vector type
  private def getDefaultDistanceType(vectorType: VectorType): DistanceType = {
    vectorType match {
      case DENSE_FLOAT =>
        COSINE // Default to cosine similarity for dense float vectors
      case BINARY => HAMMING // Default to Hamming distance for binary vectors
      case SPARSE => COSINE // Default to cosine similarity for sparse vectors
    }
  }

  // Detect vector type from DataFrame schema
  private def detectVectorType(df: DataFrame, vectorCol: String): VectorType = {
    val schema = df.schema
    if (!schema.fieldNames.contains(vectorCol)) {
      throw new IllegalArgumentException(
        s"DataFrame does not contain vector column: $vectorCol"
      )
    }

    schema(vectorCol).dataType match {
      case ArrayType(FloatType, _)         => DENSE_FLOAT
      case ArrayType(ByteType, _)          => BINARY
      case MapType(LongType, FloatType, _) => SPARSE
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported vector column type: ${schema(vectorCol).dataType}. " +
            s"Supported types are: Array[Float] (dense float), Array[Byte] (binary), Map[Long, Float] (sparse)"
        )
    }
  }

  // Unified vector search method that automatically detects vector type and calls appropriate method
  def filterSimilarVectors(
      df: DataFrame,
      queryVector: Any, // Can be Seq[Float], Array[Byte], or Map[Long, Float]
      k: Int = 10,
      threshold: Double = 0.0,
      vectorCol: String = "vector",
      idCol: Option[String] = None,
      distanceType: DistanceType = DEFAULT,
      searchType: SearchType = KNN,
      radius: Option[Double] = None,
      vectorSize: Int = 10000 // Only used for sparse vectors
  ): DataFrame = {

    // Detect vector type from DataFrame schema
    val detectedVectorType = detectVectorType(df, vectorCol)

    // Determine actual distance type (resolve DEFAULT)
    val actualDistanceType = if (distanceType == DEFAULT) {
      getDefaultDistanceType(detectedVectorType)
    } else {
      distanceType
    }

    // Validate distance type compatibility with detected vector type
    detectedVectorType match {
      case DENSE_FLOAT =>
        actualDistanceType match {
          case COSINE | L2 | IP => // Valid
          case _ =>
            throw new IllegalArgumentException(
              s"Distance type $actualDistanceType is not supported for dense float vectors. Use COSINE, L2, or IP."
            )
        }
      case BINARY =>
        actualDistanceType match {
          case HAMMING | JACCARD => // Valid
          case _ =>
            throw new IllegalArgumentException(
              s"Distance type $actualDistanceType is not supported for binary vectors. Use HAMMING or JACCARD."
            )
        }
      case SPARSE =>
        actualDistanceType match {
          case COSINE | L2 | IP => // Valid
          case _ =>
            throw new IllegalArgumentException(
              s"Distance type $actualDistanceType is not supported for sparse vectors. Use COSINE, L2, or IP."
            )
        }
    }

    // Call appropriate method based on detected vector type
    detectedVectorType match {
      case DENSE_FLOAT =>
        queryVector match {
          case seq: Seq[_] if seq.nonEmpty && seq.head.isInstanceOf[Float] =>
            val floatSeq = seq.asInstanceOf[Seq[Float]]
            filterSimilarFloatVectors(
              df = df,
              queryVector = floatSeq,
              k = k,
              threshold = threshold,
              vectorCol = vectorCol,
              idCol = idCol,
              distanceType = actualDistanceType,
              searchType = searchType,
              radius = radius
            )
          case _ =>
            throw new IllegalArgumentException(
              s"Query vector must be of type Seq[Float] for dense float vectors. Found: ${queryVector.getClass.getName}"
            )
        }

      case BINARY =>
        queryVector match {
          case arr: Array[Byte] =>
            filterSimilarBinaryVectors(
              df = df,
              queryVector = arr,
              k = k,
              threshold = threshold,
              vectorCol = vectorCol,
              idCol = idCol,
              distanceType = actualDistanceType,
              searchType = searchType,
              radius = radius
            )
          case _ =>
            throw new IllegalArgumentException(
              s"Query vector must be of type Array[Byte] for binary vectors. Found: ${queryVector.getClass.getName}"
            )
        }

      case SPARSE =>
        queryVector match {
          case map: Map[_, _]
              if map.nonEmpty &&
                map.head._1.isInstanceOf[Long] &&
                map.head._2.isInstanceOf[Float] =>
            val typedMap = map.asInstanceOf[Map[Long, Float]]
            filterSimilarSparseVectors(
              df = df,
              queryVector = typedMap,
              k = k,
              threshold = threshold,
              vectorCol = vectorCol,
              idCol = idCol,
              distanceType = actualDistanceType,
              searchType = searchType,
              radius = radius,
              vectorSize = vectorSize
            )
          case _ =>
            throw new IllegalArgumentException(
              s"Query vector must be of type Map[Long, Float] for sparse vectors. Found: ${queryVector.getClass.getName}"
            )
        }
    }
  }

  private def filterSimilarFloatVectors(
      df: DataFrame,
      queryVector: Seq[Float],
      k: Int = 10,
      threshold: Double = 0.0,
      vectorCol: String = "vector",
      idCol: Option[String] = None,
      distanceType: DistanceType = COSINE,
      searchType: SearchType = KNN,
      radius: Option[Double] = None
  ): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val schema = df.schema
    if (!schema.fieldNames.contains(vectorCol)) {
      throw new IllegalArgumentException(
        s"DataFrame does not contain vector column: $vectorCol"
      )
    }

    // Validate search type and parameters
    searchType match {
      case RANGE if radius.isEmpty =>
        throw new IllegalArgumentException(
          "Range search requires a radius parameter"
        )
      case _ => // All other cases are valid
    }

    // Validate vector type - only support Dense Float Vector
    schema(vectorCol).dataType match {
      case ArrayType(FloatType, _) => // Valid
      case _ =>
        throw new IllegalArgumentException(
          s"Vector column '$vectorCol' must be of type Array[Float]. Found: ${schema(vectorCol).dataType}. " +
            s"For binary vectors use filterSimilarBinaryVectors(), for sparse vectors use filterSimilarSparseVectors()."
        )
    }

    // Validate distance type compatibility with dense float vectors
    distanceType match {
      case COSINE | L2 | IP => // Valid
      case _ =>
        throw new IllegalArgumentException(
          s"Distance type $distanceType is not supported for dense float vectors. Use COSINE, L2, or IP."
        )
    }

    val tempVectorColName = "_converted_dense_vector_"
    var processedDF = df.withColumn(
      tempVectorColName,
      arrayFloatToDenseVectorUDF(col(vectorCol))
    )

    val tempIdCol = "_brute_force_search_row_id_"
    val effectiveIdCol = idCol match {
      case Some(colName) =>
        if (!schema.fieldNames.contains(colName)) {
          throw new IllegalArgumentException(
            s"DataFrame does not contain ID column: $colName"
          )
        }
        colName
      case None =>
        println(
          "Warning: No ID column provided. Adding a default row index, which may cause a shuffle. " +
            "For production, consider ensuring your DataFrame has a unique identifier column."
        )
        processedDF =
          processedDF.withColumn(tempIdCol, monotonically_increasing_id())
        tempIdCol
    }

    // Prepare query vector for dense float vectors
    val broadcastQueryVector = spark.sparkContext.broadcast(
      Vectors.dense(queryVector.map(_.toDouble).toArray)
    )

    val broadcastThreshold = spark.sparkContext.broadcast(threshold)
    val broadcastK = spark.sparkContext.broadcast(k)
    val broadcastDistanceType = spark.sparkContext.broadcast(distanceType)
    val broadcastSearchType = spark.sparkContext.broadcast(searchType)
    val broadcastRadius = spark.sparkContext.broadcast(radius)

    val searchResultsRDD = processedDF.rdd.mapPartitions { partitionIter =>
      val qv = broadcastQueryVector.value
      val currentK = broadcastK.value
      val currentDistanceType = broadcastDistanceType.value
      val currentSearchType = broadcastSearchType.value
      val currentRadius = broadcastRadius.value
      val partitionResults = ArrayBuffer[(Row, Double)]()

      partitionIter.foreach { row =>
        val id = row.getAs[Any](effectiveIdCol) match {
          case i: Int  => i.toLong
          case l: Long => l
          case _ =>
            throw new IllegalArgumentException(
              s"ID column '$effectiveIdCol' must be a numeric type (Int or Long). Found: ${row.getAs[Any](effectiveIdCol).getClass.getName}"
            )
        }

        // Process dense float vector only
        val vector = row.getAs[Vector](tempVectorColName)
        val distance = calculateDistance(qv, vector, currentDistanceType)

        // Determine whether to add result based on search type
        val shouldAdd = currentSearchType match {
          case KNN =>
            meetsThreshold(
              distance,
              broadcastThreshold.value,
              currentDistanceType
            )
          case RANGE =>
            currentRadius match {
              case Some(r) =>
                currentDistanceType match {
                  case COSINE =>
                    distance >= (1.0 - r) // Range search for cosine distance
                  case L2 =>
                    distance <= r // Range search for L2 distance
                  case IP => distance >= r // Range search for inner product
                }
              case None =>
                false // This case should not happen as it's validated above
            }
        }

        if (shouldAdd) {
          partitionResults += ((row, distance))
        }
      }

      // Sort results based on search type and distance type
      val sortedResults = currentDistanceType match {
        case COSINE | IP =>
          partitionResults.sortBy(-_._2) // Descending, higher values are better
        case L2 =>
          partitionResults.sortBy(_._2) // Ascending, lower values are better
      }

      // Determine number of results to return based on search type
      currentSearchType match {
        case KNN => sortedResults.take(currentK).iterator
        case RANGE =>
          sortedResults.iterator // Range search returns all matching results
      }
    }

    val originalSchema = processedDF.schema
    val distanceColName = distanceType match {
      case COSINE => "similarity"
      case L2     => "distance"
      case IP     => "inner_product"
    }
    val resultSchema = originalSchema.add(distanceColName, DoubleType)

    val searchResultsWithDistance = searchResultsRDD.map {
      case (row, distance) =>
        val values = row.toSeq :+ distance
        Row.fromSeq(values)
    }

    var finalResultsDF =
      spark.createDataFrame(searchResultsWithDistance, resultSchema)

    // Sort results based on distance type
    finalResultsDF = distanceType match {
      case COSINE | IP => finalResultsDF.orderBy(desc(distanceColName))
      case L2          => finalResultsDF.orderBy(asc(distanceColName))
    }

    // Limit result count based on search type
    if (searchType == KNN) {
      finalResultsDF = finalResultsDF.limit(k)
    }

    finalResultsDF = finalResultsDF.drop(tempVectorColName)
    if (idCol.isEmpty) {
      finalResultsDF.drop(tempIdCol)
    } else {
      finalResultsDF
    }
  }

  // Overloaded method for binary vector search
  private def filterSimilarBinaryVectors(
      df: DataFrame,
      queryVector: Array[Byte],
      k: Int = 10,
      threshold: Double = 0.0,
      vectorCol: String = "vector",
      idCol: Option[String] = None,
      distanceType: DistanceType = HAMMING,
      searchType: SearchType = KNN,
      radius: Option[Double] = None
  ): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val schema = df.schema
    if (!schema.fieldNames.contains(vectorCol)) {
      throw new IllegalArgumentException(
        s"DataFrame does not contain vector column: $vectorCol"
      )
    }

    // Validate search type and parameters
    searchType match {
      case RANGE if radius.isEmpty =>
        throw new IllegalArgumentException(
          "Range search requires a radius parameter"
        )
      case _ => // All other cases are valid
    }

    // Validate vector type
    schema(vectorCol).dataType match {
      case ArrayType(ByteType, _) => // Valid
      case _ =>
        throw new IllegalArgumentException(
          s"Vector column '$vectorCol' must be of type Array[Byte]. Found: ${schema(vectorCol).dataType}"
        )
    }

    // Validate distance type
    distanceType match {
      case HAMMING | JACCARD => // Valid
      case _ =>
        throw new IllegalArgumentException(
          s"Distance type $distanceType is not supported for binary vectors. Use HAMMING or JACCARD."
        )
    }

    val tempVectorColName = "_converted_binary_vector_"
    var processedDF = df.withColumn(
      tempVectorColName,
      arrayByteToBinaryVectorUDF(col(vectorCol))
    )

    val tempIdCol = "_brute_force_search_row_id_"
    val effectiveIdCol = idCol match {
      case Some(colName) =>
        if (!schema.fieldNames.contains(colName)) {
          throw new IllegalArgumentException(
            s"DataFrame does not contain ID column: $colName"
          )
        }
        colName
      case None =>
        println(
          "Warning: No ID column provided. Adding a default row index, which may cause a shuffle. " +
            "For production, consider ensuring your DataFrame has a unique identifier column."
        )
        processedDF =
          processedDF.withColumn(tempIdCol, monotonically_increasing_id())
        tempIdCol
    }

    val broadcastQueryVector = spark.sparkContext.broadcast(queryVector)
    val broadcastThreshold = spark.sparkContext.broadcast(threshold)
    val broadcastK = spark.sparkContext.broadcast(k)
    val broadcastDistanceType = spark.sparkContext.broadcast(distanceType)
    val broadcastSearchType = spark.sparkContext.broadcast(searchType)
    val broadcastRadius = spark.sparkContext.broadcast(radius)

    val searchResultsRDD = processedDF.rdd.mapPartitions { partitionIter =>
      val qv = broadcastQueryVector.value
      val currentK = broadcastK.value
      val currentDistanceType = broadcastDistanceType.value
      val currentSearchType = broadcastSearchType.value
      val currentRadius = broadcastRadius.value
      val partitionResults = ArrayBuffer[(Row, Double)]()

      partitionIter.foreach { row =>
        val id = row.getAs[Any](effectiveIdCol) match {
          case i: Int  => i.toLong
          case l: Long => l
          case _ =>
            throw new IllegalArgumentException(
              s"ID column '$effectiveIdCol' must be a numeric type (Int or Long). Found: ${row.getAs[Any](effectiveIdCol).getClass.getName}"
            )
        }

        val vector = row.getAs[Array[Byte]](tempVectorColName)
        val distance = calculateBinaryDistance(qv, vector, currentDistanceType)

        // Determine whether to add result based on search type
        val shouldAdd = currentSearchType match {
          case KNN =>
            meetsThreshold(
              distance,
              broadcastThreshold.value,
              currentDistanceType
            )
          case RANGE =>
            currentRadius match {
              case Some(r) =>
                distance <= r // Both HAMMING and JACCARD are distance metrics (lower is better)
              case None =>
                false // This case should not happen as it's validated above
            }
        }

        if (shouldAdd) {
          partitionResults += ((row, distance))
        }
      }

      // Sort results (both HAMMING and JACCARD are distance metrics, lower is better)
      val sortedResults = partitionResults.sortBy(_._2)

      // Determine number of results to return based on search type
      currentSearchType match {
        case KNN => sortedResults.take(currentK).iterator
        case RANGE =>
          sortedResults.iterator // Range search returns all matching results
      }
    }

    val originalSchema = processedDF.schema
    val distanceColName = distanceType match {
      case HAMMING => "hamming_distance"
      case JACCARD => "jaccard_distance"
      case _       => "distance"
    }
    val resultSchema = originalSchema.add(distanceColName, DoubleType)

    val searchResultsWithDistance = searchResultsRDD.map {
      case (row, distance) =>
        val values = row.toSeq :+ distance
        Row.fromSeq(values)
    }

    var finalResultsDF =
      spark.createDataFrame(searchResultsWithDistance, resultSchema)

    // Sort results (distance metrics, lower is better)
    finalResultsDF = finalResultsDF.orderBy(asc(distanceColName))

    // Limit result count based on search type
    if (searchType == KNN) {
      finalResultsDF = finalResultsDF.limit(k)
    }

    finalResultsDF = finalResultsDF.drop(tempVectorColName)
    if (idCol.isEmpty) {
      finalResultsDF.drop(tempIdCol)
    } else {
      finalResultsDF
    }
  }

  // Overloaded method for sparse vector search
  private def filterSimilarSparseVectors(
      df: DataFrame,
      queryVector: Map[Long, Float],
      k: Int = 10,
      threshold: Double = 0.0,
      vectorCol: String = "vector",
      idCol: Option[String] = None,
      distanceType: DistanceType = COSINE,
      searchType: SearchType = KNN,
      radius: Option[Double] = None,
      vectorSize: Int = 10000 // Default sparse vector dimension
  ): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val schema = df.schema
    if (!schema.fieldNames.contains(vectorCol)) {
      throw new IllegalArgumentException(
        s"DataFrame does not contain vector column: $vectorCol"
      )
    }

    // Validate search type and parameters
    searchType match {
      case RANGE if radius.isEmpty =>
        throw new IllegalArgumentException(
          "Range search requires a radius parameter"
        )
      case _ => // All other cases are valid
    }

    // Validate vector type
    schema(vectorCol).dataType match {
      case MapType(LongType, FloatType, _) => // Valid
      case _ =>
        throw new IllegalArgumentException(
          s"Vector column '$vectorCol' must be of type Map[Long, Float]. Found: ${schema(vectorCol).dataType}"
        )
    }

    // Validate distance type
    distanceType match {
      case COSINE | L2 | IP => // Valid
      case _ =>
        throw new IllegalArgumentException(
          s"Distance type $distanceType is not supported for sparse vectors. Use COSINE, L2, or IP."
        )
    }

    val tempVectorColName = "_converted_sparse_vector_"
    var processedDF = df.withColumn(
      tempVectorColName,
      mapToSparseVectorUDF(col(vectorCol), lit(vectorSize))
    )

    val tempIdCol = "_brute_force_search_row_id_"
    val effectiveIdCol = idCol match {
      case Some(colName) =>
        if (!schema.fieldNames.contains(colName)) {
          throw new IllegalArgumentException(
            s"DataFrame does not contain ID column: $colName"
          )
        }
        colName
      case None =>
        println(
          "Warning: No ID column provided. Adding a default row index, which may cause a shuffle. " +
            "For production, consider ensuring your DataFrame has a unique identifier column."
        )
        processedDF =
          processedDF.withColumn(tempIdCol, monotonically_increasing_id())
        tempIdCol
    }

    // Convert query vector to SparseVector
    val queryIndices = queryVector.keys.toArray.map(_.toInt).sorted
    val queryValues = queryIndices.map(i => queryVector(i.toLong).toDouble)
    val querySparseVector = Vectors
      .sparse(vectorSize, queryIndices, queryValues)
      .asInstanceOf[SparseVector]

    val broadcastQueryVector = spark.sparkContext.broadcast(querySparseVector)
    val broadcastThreshold = spark.sparkContext.broadcast(threshold)
    val broadcastK = spark.sparkContext.broadcast(k)
    val broadcastDistanceType = spark.sparkContext.broadcast(distanceType)
    val broadcastSearchType = spark.sparkContext.broadcast(searchType)
    val broadcastRadius = spark.sparkContext.broadcast(radius)

    val searchResultsRDD = processedDF.rdd.mapPartitions { partitionIter =>
      val qv = broadcastQueryVector.value
      val currentK = broadcastK.value
      val currentDistanceType = broadcastDistanceType.value
      val currentSearchType = broadcastSearchType.value
      val currentRadius = broadcastRadius.value
      val partitionResults = ArrayBuffer[(Row, Double)]()

      partitionIter.foreach { row =>
        val id = row.getAs[Any](effectiveIdCol) match {
          case i: Int  => i.toLong
          case l: Long => l
          case _ =>
            throw new IllegalArgumentException(
              s"ID column '$effectiveIdCol' must be a numeric type (Int or Long). Found: ${row.getAs[Any](effectiveIdCol).getClass.getName}"
            )
        }

        val vector = row.getAs[SparseVector](tempVectorColName)
        val distance = calculateSparseDistance(qv, vector, currentDistanceType)

        // Determine whether to add result based on search type
        val shouldAdd = currentSearchType match {
          case KNN =>
            meetsThreshold(
              distance,
              broadcastThreshold.value,
              currentDistanceType
            )
          case RANGE =>
            currentRadius match {
              case Some(r) =>
                currentDistanceType match {
                  case COSINE =>
                    distance >= (1.0 - r) // Range search for cosine distance
                  case L2 => distance <= r // Range search for L2 distance
                  case IP => distance >= r // Range search for inner product
                }
              case None =>
                false // This case should not happen as it's validated above
            }
        }

        if (shouldAdd) {
          partitionResults += ((row, distance))
        }
      }

      // Sort results based on distance type
      val sortedResults = currentDistanceType match {
        case COSINE | IP =>
          partitionResults.sortBy(-_._2) // Descending, higher values are better
        case L2 =>
          partitionResults.sortBy(_._2) // Ascending, lower values are better
      }

      // Determine number of results to return based on search type
      currentSearchType match {
        case KNN => sortedResults.take(currentK).iterator
        case RANGE =>
          sortedResults.iterator // Range search returns all matching results
      }
    }

    val originalSchema = processedDF.schema
    val distanceColName = distanceType match {
      case COSINE => "similarity"
      case L2     => "distance"
      case IP     => "inner_product"
    }
    val resultSchema = originalSchema.add(distanceColName, DoubleType)

    val searchResultsWithDistance = searchResultsRDD.map {
      case (row, distance) =>
        val values = row.toSeq :+ distance
        Row.fromSeq(values)
    }

    var finalResultsDF =
      spark.createDataFrame(searchResultsWithDistance, resultSchema)

    // Sort results based on distance type
    finalResultsDF = distanceType match {
      case COSINE | IP => finalResultsDF.orderBy(desc(distanceColName))
      case L2          => finalResultsDF.orderBy(asc(distanceColName))
    }

    // Limit result count based on search type
    if (searchType == KNN) {
      finalResultsDF = finalResultsDF.limit(k)
    }

    finalResultsDF = finalResultsDF.drop(tempVectorColName)
    if (idCol.isEmpty) {
      finalResultsDF.drop(tempIdCol)
    } else {
      finalResultsDF
    }
  }
}
