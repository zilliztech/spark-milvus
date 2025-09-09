package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSessionExtensions

import com.zilliz.spark.connector.expressions._

/** Spark SQL Extension for Vector Search
  *
  * Usage: spark.conf.set("spark.sql.extensions",
  * "com.zilliz.spark.connector.extensions.VectorSearchExtensions")
  *
  * Or when creating SparkSession: SparkSession.builder()
  * .config("spark.sql.extensions",
  * "com.zilliz.spark.connector.extensions.VectorSearchExtensions")
  * .config("spark.jars", "/path/to/milvus-spark-connector.jar") .getOrCreate()
  */
class VectorSearchExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register all vector search functions
    extensions.injectFunction(CosineSimilarityFunction.description)
    extensions.injectFunction(L2DistanceFunction.description)
    extensions.injectFunction(InnerProductFunction.description)
    extensions.injectFunction(VectorKNNFunction.description)
    extensions.injectFunction(HammingDistanceFunction.description)
    extensions.injectFunction(JaccardDistanceFunction.description)
  }
}

/** Base trait for vector function descriptions
  */
trait VectorFunctionDescription {
  def name: String
  def builder: Seq[Expression] => Expression
  def usage: String
  def examples: String

  def description: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val identifier = FunctionIdentifier(name)
    val info = new ExpressionInfo(
      this.getClass.getCanonicalName,
      name,
      usage
    )
    (identifier, info, builder)
  }
}

/** Cosine Similarity Function
  */
object CosineSimilarityFunction extends VectorFunctionDescription {
  override def name: String = "cosine_similarity"

  override def usage: String =
    "_FUNC_(vector1, vector2) - Returns the cosine similarity between two vectors (0 to 1, higher is more similar)"

  override def examples: String = """
    |Examples:
    |  > SELECT cosine_similarity(array(1.0, 2.0, 3.0), array(4.0, 5.0, 6.0));
    |   0.9746318461970762
    |  > SELECT id FROM vectors WHERE cosine_similarity(embedding, array(0.1, 0.2, 0.3)) > 0.9;
  """.stripMargin

  override def builder: Seq[Expression] => Expression = {
    case Seq(vector1, vector2) =>
      new CosineSimilarityExpression(vector1, vector2)
    case _ =>
      throw new IllegalArgumentException(s"$name requires exactly 2 arguments")
  }
}

/** L2 Distance Function
  */
object L2DistanceFunction extends VectorFunctionDescription {
  override def name: String = "l2_distance"

  override def usage: String =
    "_FUNC_(vector1, vector2) - Returns the L2 (Euclidean) distance between two vectors (lower is more similar)"

  override def examples: String = """
    |Examples:
    |  > SELECT l2_distance(array(1.0, 2.0, 3.0), array(4.0, 5.0, 6.0));
    |   5.196152422706632
    |  > SELECT id FROM vectors WHERE l2_distance(embedding, array(0.1, 0.2, 0.3)) < 1.0;
  """.stripMargin

  override def builder: Seq[Expression] => Expression = {
    case Seq(vector1, vector2) => new L2DistanceExpression(vector1, vector2)
    case _ =>
      throw new IllegalArgumentException(s"$name requires exactly 2 arguments")
  }
}

/** Inner Product Function
  */
object InnerProductFunction extends VectorFunctionDescription {
  override def name: String = "inner_product"

  override def usage: String =
    "_FUNC_(vector1, vector2) - Returns the inner product (dot product) of two vectors"

  override def examples: String = """
    |Examples:
    |  > SELECT inner_product(array(1.0, 2.0, 3.0), array(4.0, 5.0, 6.0));
    |   32.0
    |  > SELECT id FROM vectors WHERE inner_product(embedding, query_vector) > 100;
  """.stripMargin

  override def builder: Seq[Expression] => Expression = {
    case Seq(vector1, vector2) => new InnerProductExpression(vector1, vector2)
    case _ =>
      throw new IllegalArgumentException(s"$name requires exactly 2 arguments")
  }
}

/** Vector KNN Function - returns top K similar vectors
  */
object VectorKNNFunction extends VectorFunctionDescription {
  override def name: String = "vector_knn"

  override def usage: String =
    "_FUNC_(vectors, query_vector, k, distance_type) - Returns top K nearest neighbors from a vector array"

  override def examples: String = """
    |Examples:
    |  > SELECT vector_knn(collect_list(embedding), array(0.1, 0.2, 0.3), 5, 'cosine');
    |   [{"id": 1, "score": 0.99}, {"id": 5, "score": 0.95}, ...]
  """.stripMargin

  override def builder: Seq[Expression] => Expression = {
    case Seq(vectors, queryVector, k, distanceType) =>
      new VectorKNNExpression(vectors, queryVector, k, distanceType)
    case Seq(vectors, queryVector, k) =>
      new VectorKNNExpression(
        vectors,
        queryVector,
        k,
        org.apache.spark.sql.catalyst.expressions.Literal("cosine")
      )
    case _ =>
      throw new IllegalArgumentException(s"$name requires 3 or 4 arguments")
  }
}

/** Hamming Distance Function for binary vectors
  */
object HammingDistanceFunction extends VectorFunctionDescription {
  override def name: String = "hamming_distance"

  override def usage: String =
    "_FUNC_(binary_vector1, binary_vector2) - Returns the Hamming distance between two binary vectors"

  override def examples: String = """
    |Examples:
    |  > SELECT hamming_distance(binary_col1, binary_col2) FROM binary_vectors;
    |   15
  """.stripMargin

  override def builder: Seq[Expression] => Expression = {
    case Seq(vector1, vector2) =>
      new HammingDistanceExpression(vector1, vector2)
    case _ =>
      throw new IllegalArgumentException(s"$name requires exactly 2 arguments")
  }
}

/** Jaccard Distance Function for binary vectors
  */
object JaccardDistanceFunction extends VectorFunctionDescription {
  override def name: String = "jaccard_distance"

  override def usage: String =
    "_FUNC_(binary_vector1, binary_vector2) - Returns the Jaccard distance between two binary vectors"

  override def examples: String = """
    |Examples:
    |  > SELECT jaccard_distance(binary_col1, binary_col2) FROM binary_vectors;
    |   0.25
  """.stripMargin

  override def builder: Seq[Expression] => Expression = {
    case Seq(vector1, vector2) =>
      new JaccardDistanceExpression(vector1, vector2)
    case _ =>
      throw new IllegalArgumentException(s"$name requires exactly 2 arguments")
  }
}
