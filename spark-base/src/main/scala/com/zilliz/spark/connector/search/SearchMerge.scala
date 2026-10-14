package com.zilliz.spark.connector.search

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.HashPartitioner

import com.zilliz.milvus.storage.index.CandidateBytes
import com.zilliz.milvus.storage.schema.MetricType

/** The merge stage both inputs share, and the hits a search gives back.
  *
  * A first-stage task answers each query it found anything for with one packed
  * record of that query's best k; the merge combines a query's records, one per
  * task, into its global top k by query id over an RDD shuffle
  * (docs/design/architecture/vector-search.html sections 2.1 and 2.6). A Milvus
  * table input merges `CandidateBytes` into rows of [[HitSchema]], which its
  * take stage extends with the base columns; a DataFrame input's candidates
  * carry their rows (`CarriedCandidates`), so its merge gives the hits whole.
  */
private[search] object SearchMerge {

  /** What a search gives back: the hits of every query with the output columns
    * asked for, as rows of `schema`. An iterator of these rows may hand the
    * same object back with new values, so a consumer that keeps a row copies
    * it.
    */
  final case class Hits(rows: RDD[InternalRow], schema: StructType)

  /** What a Milvus table input's search returns before the output columns, and
    * the one place that shape is written down: [[SearchResult]] names the
    * columns, so an empty result and a merged one cannot drift apart.
    */
  val HitSchema: StructType = Encoders.product[SearchResult].schema

  /** The hit columns when some query is required to appear without a hit: the
    * columns of the hit are NULL in its row.
    */
  val NullableHitSchema: StructType = StructType(
    HitSchema.fields.map(field =>
      if (field.name == SearchQueries.IdColumn) field
      else field.copy(nullable = true)
    )
  )

  /** How many queries one merge partition is meant to take. The merge walks
    * sorted lists, so a partition's work is linear in the candidates it reads;
    * this only keeps a partition's reduce-side map from holding more than a few
    * thousand queries' answers at once on a cluster whose parallelism is far
    * below its query count.
    */
  val QueriesPerMergePartition: Int = 4096

  /** How many partitions the merge runs in: one for each task slot the job has,
    * and never more than there are queries. The merge is an RDD shuffle rather
    * than a SQL aggregation, so nothing downstream re-partitions it and
    * `spark.sql.shuffle.partitions` does not apply.
    */
  def partitions(
      spark: SparkSession,
      queries: Long,
      tasks: Int,
      k: Int,
      heapBytesPerTask: Long,
      candidateBytes: Long = CandidateBytes.Width.toLong
  ): Int = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(tasks > 0, s"A search needs at least one task: $tasks")
    require(k > 0, s"A search needs a positive k: $k")
    // At least one partition for every task slot the job has and for every
    // first-stage task, so no executor sits out the merge; more when the
    // queries would otherwise pile up in one partition; never more than there
    // are queries, since a query is one key and cannot be split.
    //
    // How many queries a partition may hold is the smaller of a fixed few
    // thousand and what the task's heap fits: every query arrives as one
    // packed answer per first-stage task, `tasks x k x Width` bytes, and the
    // reduce-side map holds them until they are merged. 100k queries at
    // k=16,384 over 15 tasks were 5.9 MB a query; 64 partitions of 1,563
    // queries each ran the 6 GiB heap out (2026-09-26 decision). Half the
    // heap share is left for the merged answers and the rows they become.
    val slots = math.max(1, spark.sparkContext.defaultParallelism)
    val bytesPerQuery = tasks.toLong * k.toLong * candidateBytes
    val byHeap =
      math.max(1L, math.max(0L, heapBytesPerTask) / 2L / bytesPerQuery)
    val perPartition = math.min(QueriesPerMergePartition.toLong, byHeap)
    val byQueries = (queries + perPartition - 1L) / perPartition
    val wanted = math.max(math.max(slots.toLong, tasks.toLong), byQueries)
    math.max(1L, math.min(queries, wanted)).toInt
  }

  /** A Milvus table input's merge: every query's candidates become its global
    * top-k, best first, as rows of [[HitSchema]]; with `required`, an id that
    * found nothing has a row whose hit columns are NULL.
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
        id => Row(id, null, null, null, null)
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

  /** The rows of required ids when no search ran: each the id, then NULLs to
    * make `width` columns.
    */
  def missing(
      spark: SparkSession,
      required: Option[Array[Long]],
      width: Int
  ): RDD[InternalRow] = required match {
    case None => spark.sparkContext.emptyRDD[InternalRow]
    case Some(ids) =>
      spark.sparkContext
        .parallelize(ids.toSeq, math.max(1, math.min(ids.length, 64)))
        .map(absentRow(_, width))
  }

  /** A required id's row without a hit: the id, then NULLs to make `width`
    * columns.
    */
  def absentRow(id: Long, width: Int): InternalRow = {
    val row = new GenericInternalRow(width)
    row.setLong(0, id)
    row
  }

  /** One query's answer as the rows the result contract promises: rank from
    * one, best first.
    */
  def rows(query: Long, packed: Array[Byte]): Iterator[Row] = {
    // One row at a time: at k=16,384 a query's answer is 16,384 rows, and a
    // vector of them per query is what the reduce task would otherwise hold
    // on top of the packed bytes.
    val count = CandidateBytes.count(packed)
    Iterator.tabulate(count)(at =>
      Row(
        query,
        at + 1,
        CandidateBytes.score(packed, at),
        CandidateBytes.segmentId(packed, at),
        CandidateBytes.rowOffset(packed, at)
      )
    )
  }
}

/** The columns a Milvus table input's search returns, before the output
  * columns.
  */
private[search] case class SearchResult(
    query_id: Long,
    rank: Int,
    _score: Double,
    _segment_id: Long,
    _row_offset: Long
)
