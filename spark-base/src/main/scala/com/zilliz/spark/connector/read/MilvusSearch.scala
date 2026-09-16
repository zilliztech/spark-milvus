package com.zilliz.spark.connector.read

import java.lang.{Float => JavaFloat}
import java.util.Locale
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.spark.connector.options.MilvusOption

/** Searches persisted segment indexes and returns a lazy global TopK plan. A
  * filter passed here is evaluated before search. DataFrame.filter on the
  * returned frame instead filters the already selected results.
  */
object MilvusSearch {
  def search(
      spark: SparkSession,
      options: Map[String, String],
      vectorColumn: String,
      queryVector: Array[Float],
      k: Int,
      metric: String = "COSINE",
      searchParameters: Map[String, String] = Map.empty,
      filter: Option[String] = None,
      outputColumns: Seq[String] = Seq.empty,
      allowUnindexed: Boolean = false
  ): DataFrame = {
    require(
      options != null && outputColumns != null && searchParameters != null,
      "Search arguments must not be null"
    )
    require(
      vectorColumn != null && vectorColumn.nonEmpty && metric != null,
      "Vector field and metric must be present"
    )
    require(
      queryVector != null && queryVector.nonEmpty && queryVector.forall(
        JavaFloat.isFinite
      ),
      "Query vector must be nonempty and finite"
    )
    require(k > 0, "k must be positive")
    val normalizedMetric = metric.toUpperCase(Locale.ROOT)
    require(
      Set("L2", "IP", "COSINE").contains(normalizedMetric),
      "Unsupported metric"
    )
    require(
      normalizedMetric != "COSINE" || queryVector.exists(_ != 0.0f),
      "COSINE query must have nonzero norm"
    )
    filter.foreach(PlanParser.parse)
    val reserved = Set("_score", "_segment_id", "_row_offset")
    require(
      outputColumns.distinct.size == outputColumns.size,
      "Duplicate output columns"
    )
    require(
      !outputColumns.exists(reserved),
      "Output columns conflict with search metadata"
    )
    require(
      !options.exists { case (key, value) =>
        key.equalsIgnoreCase(
          MilvusOption.ReaderFieldIDs
        ) && value != null && value.trim.nonEmpty
      },
      "Use outputColumns instead of fieldIDs for index search"
    )
    val explicitKeys = Set(
      MilvusOption.VectorSearchQueryVector,
      MilvusOption.VectorSearchTopK,
      MilvusOption.VectorSearchVectorColumn,
      MilvusOption.VectorSearchMetric,
      MilvusOption.VectorSearchMode,
      MilvusOption.VectorSearchParameters,
      MilvusOption.VectorSearchFilter,
      MilvusOption.VectorSearchAllowUnindexed,
      MilvusOption.MilvusExtraColumns
    ).map(_.toLowerCase(Locale.ROOT))
    val searchOptions = options.filterNot { case (key, _) =>
      explicitKeys(key.toLowerCase(Locale.ROOT))
    } ++ Map(
      MilvusOption.VectorSearchQueryVector -> queryVector
        .mkString("[", ",", "]"),
      MilvusOption.VectorSearchTopK -> k.toString,
      MilvusOption.VectorSearchVectorColumn -> vectorColumn,
      MilvusOption.VectorSearchMetric -> normalizedMetric,
      MilvusOption.VectorSearchMode -> "index",
      MilvusOption.VectorSearchParameters -> new ObjectMapper()
        .writeValueAsString(searchParameters.asJava),
      MilvusOption.VectorSearchFilter -> filter.getOrElse(""),
      MilvusOption.VectorSearchAllowUnindexed -> allowUnindexed.toString,
      MilvusOption.MilvusExtraColumns -> Seq(
        MilvusOption.MilvusExtraColumnSegmentID,
        MilvusOption.MilvusExtraColumnRowOffset
      ).mkString(",")
    )
    val frame = spark.read.format("milvus").options(searchOptions).load()
    val business =
      if (outputColumns.nonEmpty) outputColumns
      else
        frame.columns.toSeq.filterNot(
          Set(
            "_score",
            MilvusOption.MilvusExtraColumnSegmentID,
            MilvusOption.MilvusExtraColumnRowOffset
          )
        )
    def named(name: String) = col("`" + name.replace("`", "``") + "`")
    val projected = frame.select(
      (business.map(named) ++ Seq(
        named(MilvusOption.MilvusExtraColumnSegmentID).as("_segment_id"),
        named(MilvusOption.MilvusExtraColumnRowOffset).as("_row_offset"),
        named("_score")
      )): _*
    )
    val order =
      if (normalizedMetric == "L2") col("_score").asc else col("_score").desc
    projected
      .orderBy(order, col("_segment_id").asc, col("_row_offset").asc)
      .limit(k)
  }
}
