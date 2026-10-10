package com.zilliz.spark.connector.read

import java.lang.{Float => JavaFloat}
import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow

import com.zilliz.milvus.storage.index.QueryMatrix
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.spark.connector.options.MilvusOption

/** The query set a search takes: `query_id` and `vector`, the query rows of a
  * nearest-by join numbered by their position, packed into the bytes Knowhere
  * reads.
  *
  * The same packing serves both delivery paths of section 2.1: when the set
  * fits `milvus.search.queries.max.bytes` the executors pack their partitions
  * and the driver only concatenates the bytes before broadcasting them
  * ([[packOnExecutors]]); when it does not, the packing job packs one group per
  * task and the groups travel with the shuffle. A query vector is ARRAY<FLOAT>,
  * what Spark's vector functions take, and is packed in the field's element
  * type.
  */
private[read] object SearchQueries {

  val IdColumn = "query_id"
  val VectorColumn = "vector"

  /** A query set as a search takes it. */
  sealed trait Input

  /** `query_id` and `vector` as a frame, counted and packed by Spark jobs. */
  final case class Frame(selected: DataFrame) extends Input

  /** A set already packed on the driver, as [[pack]] packs one: row `i` of
    * `vectors` is the query of `ids(i)`. It is broadcast, and nothing about it
    * runs a job (docs/design/architecture/dataframe-api.html section 4).
    */
  final case class Packed(ids: Array[Long], vectors: Array[Byte]) extends Input

  /** One query group as it reaches a task: the ids of its queries, the bytes
    * they were packed into, and where the group starts in those bytes. A
    * broadcast query set is packed once and every group points into it; a group
    * delivered with the shuffle carries its own bytes and starts at zero.
    */
  final case class Group(
      ids: Array[Long],
      vectors: Array[Byte],
      firstQuery: Int
  ) extends Serializable {
    def queries: Int = ids.length
  }

  /** The bytes a query set of this many queries occupies. */
  def bytes(queries: Long, layout: VectorLayout): Long =
    queries * layout.rowBytes.toLong

  /** Packs rows already in hand. Query ids keep the order they arrive in, and
    * row `i` of the packed bytes is the query of `ids(i)`.
    */
  def pack(
      rows: Seq[Row],
      layout: VectorLayout,
      metric: MetricType
  ): (Array[Long], Array[Byte]) = {
    val ids = new Array[Long](rows.size)
    rows.iterator.zipWithIndex.foreach { case (row, index) =>
      require(
        !row.isNullAt(0),
        s"Query ${index + 1} of the set has no $IdColumn"
      )
      require(
        !row.isNullAt(1),
        s"Query ${row.getLong(0)} has no $VectorColumn"
      )
      ids(index) = row.getLong(0)
    }
    val floats = rows.map(row => finite(row, layout, metric))
    (ids, QueryMatrix.packFloats(floats, layout))
  }

  /** Rows an executor packs at a time: 2,048 queries of 768 dimensions are 6
    * MiB, small enough to hold as float arrays beside the packed bytes.
    */
  private val PackChunkRows = 2048

  /** The whole query set packed on the executors, one block per partition, and
    * brought to the driver as bytes: what the broadcast path ships.
    *
    * `collect()` would bring the rows themselves: every vector boxed to a
    * `Seq[Float]` and unpacked again on the driver, one thread doing it all.
    * That work falls between the collect job and the next stage, where no job
    * timing shows it: 10 s of a 250,000-query chunk on eight executors, of
    * which 2 s was the broadcast and 8 s the driver repacking. Here each
    * partition reads its rows as `InternalRow`, takes the vector out as a
    * primitive array, and packs it in place; the driver receives `rowBytes` per
    * query and no objects. Partitions keep their order, so row `i` of the
    * result is query `ids(i)` exactly as [[pack]] would have placed it.
    */
  def packOnExecutors(
      selected: DataFrame,
      layout: VectorLayout,
      metric: MetricType
  ): (Array[Long], Array[Byte]) = {
    val blocks = selected.queryExecution.toRdd
      .mapPartitionsWithIndex { (partition, rows) =>
        Iterator.single((partition, packPartition(rows, layout, metric)))
      }
      .collect()
      .sortBy(_._1)
    val queries = blocks.iterator.map(_._2._1.length.toLong).sum
    val total = blocks.iterator.map(_._2._2.length.toLong).sum
    require(
      total <= Int.MaxValue,
      s"The packed query set is $total bytes, over what one array holds; lower ${MilvusOption.SearchQueriesMaxBytes} so the set travels with the shuffle"
    )
    val ids = new Array[Long](queries.toInt)
    val vectors = new Array[Byte](total.toInt)
    var idAt = 0
    var byteAt = 0
    blocks.foreach { case (_, (blockIds, blockBytes)) =>
      System.arraycopy(blockIds, 0, ids, idAt, blockIds.length)
      System.arraycopy(blockBytes, 0, vectors, byteAt, blockBytes.length)
      idAt += blockIds.length
      byteAt += blockBytes.length
    }
    (ids, vectors)
  }

  /** One partition's rows packed on the executor that read them. Rows are taken
    * as they come and never retained: a chunk of primitive arrays is packed and
    * dropped before the next is read.
    */
  private[read] def packPartition(
      rows: Iterator[InternalRow],
      layout: VectorLayout,
      metric: MetricType
  ): (Array[Long], Array[Byte]) = {
    val ids = mutable.ArrayBuilder.make[Long]
    val chunks = mutable.ArrayBuffer.empty[Array[Byte]]
    var total = 0L
    val floats = mutable.ArrayBuffer.empty[Array[Float]]
    def flush(): Unit = {
      val packed = QueryMatrix.packFloats(floats.toSeq, layout)
      floats.clear()
      if (packed.nonEmpty) {
        chunks += packed
        total += packed.length
      }
    }
    var index = 0
    while (rows.hasNext) {
      val row = rows.next()
      require(
        !row.isNullAt(0),
        s"Query ${index + 1} of the set has no $IdColumn"
      )
      val id = row.getLong(0)
      require(!row.isNullAt(1), s"Query $id has no $VectorColumn")
      floats += finiteValues(id, row.getArray(1).toFloatArray(), layout, metric)
      ids += id
      index += 1
      if (index % PackChunkRows == 0) flush()
    }
    flush()
    val packed = new Array[Byte](Math.toIntExact(total))
    var at = 0
    chunks.foreach { chunk =>
      System.arraycopy(chunk, 0, packed, at, chunk.length)
      at += chunk.length
    }
    (ids.result(), packed)
  }

  private def finite(
      row: Row,
      layout: VectorLayout,
      metric: MetricType
  ): Array[Float] =
    finiteValues(row.getLong(0), row.getSeq[Float](1).toArray, layout, metric)

  private def finiteValues(
      id: Long,
      values: Array[Float],
      layout: VectorLayout,
      metric: MetricType
  ): Array[Float] = {
    require(
      values.length == layout.dimension,
      s"Query $id has ${values.length} values; the field has ${layout.dimension} dimensions"
    )
    require(
      values.forall(JavaFloat.isFinite),
      s"Query $id holds a value that is not finite"
    )
    require(
      metric != MetricType.Cosine || values.exists(_ != 0.0f),
      s"Query $id has a zero norm, which COSINE has no answer for"
    )
    values
  }
}
