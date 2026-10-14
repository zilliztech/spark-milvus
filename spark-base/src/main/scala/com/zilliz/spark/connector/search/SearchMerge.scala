package com.zilliz.spark.connector.search

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.HashPartitioner

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.CandidateBytes

/** The merge stage both inputs share, and the hits a search gives back.
  *
  * A first-stage task answers each query it found anything for with one packed
  * record of that query's best k; the merge combines a query's records, one per
  * task, into its global top k by query id over an RDD shuffle
  * (docs/design/architecture/vector-search.html sections 2.1 and 2.6). A Milvus
  * table input merges `CandidateBytes` into addresses of [[HitSchema]], which
  * its take stage reads for the base columns; a DataFrame input's candidates
  * carry their rows (`CarriedCandidates`), so its merge gives the hits whole.
  */
private[search] object SearchMerge {

  /** Each query's selected row addresses. Scores are needed only until the
    * global top-k is settled; the take reads the selected addresses.
    */
  val HitSchema: StructType = Encoders.product[SearchHit].schema

  /** The hit columns when some query is required to appear without a hit: the
    * columns of the hit are NULL in its row.
    */
  val NullableHitSchema: StructType = StructType(
    HitSchema.fields.map(field =>
      if (field.name == SearchQueries.IdColumn) field
      else field.copy(nullable = true)
    )
  )

  /** A Milvus table input's merge: every query's candidates become its global
    * top-k addresses, best first, as rows of [[HitSchema]]; with `required`, an
    * id that found nothing has a row whose hit columns are NULL.
    */
  def merged(
      spark: SparkSession,
      candidates: RDD[(Long, Array[Byte])],
      k: Int,
      metric: MetricType,
      partitions: Int,
      required: Option[Array[Long]] = None
  ): DataFrame =
    spark.createDataFrame(
      byQuery[Row](
        spark,
        candidates,
        partitions,
        CandidateBytes.merge(_, _, k, metric),
        rows,
        required,
        id => Row(id, null, null)
      ),
      if (required.isEmpty) HitSchema else NullableHitSchema
    )

  /** The merge of either input: every query's answers merged into one, the rows
    * `rowsOf` makes of it, and with `required` the `absent` row of each id that
    * found nothing.
    *
    * Each task already merged its own segments, so what arrives here is one
    * packed answer per (query, task) and the merge is a walk over two sorted
    * lists, not an aggregation over candidates. Map-side combining is off on
    * purpose: a task emits each of its queries once, so combining on the map
    * side would hold every query's answer until the task ends instead of
    * writing each one as it is packed (section 2.1).
    */
  def byQuery[A: ClassTag](
      spark: SparkSession,
      answers: RDD[(Long, Array[Byte])],
      partitions: Int,
      merge: (Array[Byte], Array[Byte]) => Array[Byte],
      rowsOf: (Long, Array[Byte]) => Iterator[A],
      required: Option[Array[Long]],
      absent: Long => A
  ): RDD[A] = {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    val partitioner = new HashPartitioner(partitions)
    val combined = answers.combineByKeyWithClassTag[Array[Byte]](
      (packed: Array[Byte]) => packed,
      merge,
      merge,
      partitioner,
      mapSideCombine = false
    )
    required match {
      case None =>
        combined.flatMap { case (query, packed) => rowsOf(query, packed) }
      case Some(ids) =>
        // A required id that no task found anything for has no key here; the
        // partition its key would hash to says so after its own answers.
        val wanted = spark.sparkContext.broadcast(ids)
        combined.mapPartitionsWithIndex { (index, merged) =>
          val found = new java.util.HashSet[java.lang.Long]()
          merged.flatMap { case (query, packed) =>
            found.add(query)
            rowsOf(query, packed)
          } ++ wanted.value.iterator
            .filter(id =>
              partitioner.getPartition(id) == index && !found.contains(id)
            )
            .map(absent)
        }
    }
  }

  /** Required ids with NULL base columns when no search ran. */
  def missing(
      spark: SparkSession,
      required: Option[Array[Long]],
      width: Int
  ): RDD[(Long, InternalRow)] = required match {
    case None => spark.sparkContext.emptyRDD[(Long, InternalRow)]
    case Some(ids) =>
      spark.sparkContext
        .parallelize(ids.toSeq, math.max(1, math.min(ids.length, 64)))
        .map(id => id -> (new GenericInternalRow(width): InternalRow))
  }

  /** One query's selected addresses, best first. */
  def rows(query: Long, packed: Array[Byte]): Iterator[Row] = {
    // One row at a time: at k=16,384 a query's answer is 16,384 rows, and a
    // vector of them per query is what the reduce task would otherwise hold
    // on top of the packed bytes.
    val count = CandidateBytes.count(packed)
    Iterator.tabulate(count)(at =>
      Row(
        query,
        CandidateBytes.segmentId(packed, at),
        CandidateBytes.rowOffset(packed, at)
      )
    )
  }
}

/** A selected row address associated with its query. */
private[search] case class SearchHit(
    query_id: Long,
    _segment_id: Long,
    _row_offset: Long
)
