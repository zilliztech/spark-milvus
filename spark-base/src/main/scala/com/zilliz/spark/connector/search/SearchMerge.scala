package com.zilliz.spark.connector.search

import java.lang.{Long => JavaLong}
import java.util.HashSet
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
        val wanted = spark.sparkContext.broadcast(requiredIds(ids, partitioner))
        combined.mapPartitionsWithIndex { (index, merged) =>
          val found = new HashSet[JavaLong]()
          merged.flatMap { case (query, packed) =>
            found.add(query)
            rowsOf(query, packed)
          } ++ wanted.value
            .missing(index, partitioner, found)
            .map(absent)
        }
    }
  }

  /** Required ids in their original order, or stably grouped by merge
    * partition. Empty ends keep the original filtering path. The bucketed
    * representation adds one Int per partition to the ids already broadcast.
    */
  private[search] final class RequiredIds(
      val ids: Array[Long],
      val ends: Array[Int]
  ) extends Serializable {
    def missing(
        index: Int,
        partitioner: HashPartitioner,
        found: HashSet[JavaLong]
    ): Iterator[Long] =
      if (ends.isEmpty)
        ids.iterator.filter(id =>
          partitioner.getPartition(id) == index && !found.contains(id)
        )
      else {
        val start = if (index == 0) 0 else ends(index - 1)
        new RequiredIdCursor(ids, start, ends(index), found)
      }
  }

  private final class RequiredIdCursor(
      ids: Array[Long],
      start: Int,
      end: Int,
      found: HashSet[JavaLong]
  ) extends Iterator[Long] {
    private var at = start
    private var ready = false
    private var value = 0L

    override def hasNext: Boolean = {
      while (!ready && at < end) {
        val id = ids(at)
        at += 1
        if (!found.contains(id)) {
          value = id
          ready = true
        }
      }
      ready
    }

    override def next(): Long = {
      if (!hasNext) throw new NoSuchElementException("next on empty iterator")
      ready = false
      value
    }
  }

  private[search] def requiredIds(
      ids: Array[Long],
      partitioner: HashPartitioner
  ): RequiredIds = {
    val partitions = partitioner.numPartitions
    // Two passes over the ids prepare the buckets. Keep small partition counts
    // on the original path, and bound the added offsets by the id count.
    if (partitions <= 2 || ids.length < partitions)
      return new RequiredIds(ids, Array.empty[Int])
    val ends = new Array[Int](partitions)
    var at = 0
    while (at < ids.length) {
      ends(partitioner.getPartition(ids(at))) += 1
      at += 1
    }
    var partition = 0
    var total = 0
    while (partition < partitions) {
      total += ends(partition)
      ends(partition) = total
      partition += 1
    }
    val positions = ends.clone()
    val grouped = new Array[Long](ids.length)
    at = ids.length - 1
    while (at >= 0) {
      val id = ids(at)
      partition = partitioner.getPartition(id)
      positions(partition) -= 1
      grouped(positions(partition)) = id
      at -= 1
    }
    new RequiredIds(grouped, ends)
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
