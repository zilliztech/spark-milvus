package com.zilliz.spark.connector.read

import java.lang.{Float => JavaFloat}
import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  FloatType,
  LongType,
  ShortType,
  StructType
}

import com.zilliz.milvus.storage.index.QueryMatrix
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.spark.connector.options.MilvusOption

/** The query set a search takes: `query_id` and `vector`, checked against the
  * field being searched and packed into the bytes Knowhere reads.
  *
  * The same packing serves both delivery paths of section 2.1: when the set
  * fits `milvus.search.queries.max.bytes` the executors pack their partitions
  * and the driver only concatenates the bytes before broadcasting them
  * ([[packOnExecutors]]); when it does not, the packing job packs one group per
  * task and the groups travel with the shuffle.
  */
private[read] object SearchQueries {

  val IdColumn = "query_id"
  val VectorColumn = "vector"

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

  /** Fails unless the frame carries the two columns in the types this field's
    * vectors take.
    */
  def check(schema: StructType, layout: VectorLayout): Unit = {
    val id = schema.fields
      .find(_.name == IdColumn)
      .getOrElse(
        throw new IllegalArgumentException(
          s"A query set needs a '$IdColumn' column; this one has ${schema.fieldNames.mkString(", ")}"
        )
      )
    require(
      id.dataType == LongType,
      s"'$IdColumn' is ${id.dataType.simpleString}; a query set needs BIGINT"
    )
    val vector = schema.fields
      .find(_.name == VectorColumn)
      .getOrElse(
        throw new IllegalArgumentException(
          s"A query set needs a '$VectorColumn' column; this one has ${schema.fieldNames.mkString(", ")}"
        )
      )
    val expected = layout.elementType match {
      case VectorElementType.Int8 => "ARRAY<SMALLINT>"
      case VectorElementType.Bit  => "BINARY"
      case _                      => "ARRAY<FLOAT>"
    }
    val matches = (layout.elementType, vector.dataType) match {
      case (VectorElementType.Int8, ArrayType(ShortType, _)) => true
      case (VectorElementType.Bit, BinaryType)               => true
      case (VectorElementType.Int8, _)                       => false
      case (VectorElementType.Bit, _)                        => false
      case (_, ArrayType(FloatType, _))                      => true
      case _                                                 => false
    }
    require(
      matches,
      s"'$VectorColumn' is ${vector.dataType.simpleString}; a ${layout.elementType} field of ${layout.dimension} dimensions takes $expected"
    )
  }

  /** The two columns in the order this object reads them. */
  def selected(queries: DataFrame): DataFrame =
    queries.select(col(IdColumn), col(VectorColumn))

  /** Packs rows already in hand. Query ids keep the order they arrive in, and
    * row `i` of the packed bytes is the query of `ids(i)`.
    */
  def pack(
      rows: Seq[Row],
      layout: VectorLayout,
      metric: String
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
    val vectors = layout.elementType match {
      case VectorElementType.Int8 =>
        QueryMatrix.packBytes(rows.map(int8(_, layout)), layout)
      case VectorElementType.Bit =>
        QueryMatrix.packBytes(rows.map(_.getAs[Array[Byte]](1)), layout)
      case _ =>
        val floats = rows.map(row => finite(row, layout, metric))
        QueryMatrix.packFloats(floats, layout)
    }
    (ids, vectors)
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
      metric: String
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
      metric: String
  ): (Array[Long], Array[Byte]) = {
    val ids = mutable.ArrayBuilder.make[Long]
    val chunks = mutable.ArrayBuffer.empty[Array[Byte]]
    var total = 0L
    val floats = mutable.ArrayBuffer.empty[Array[Float]]
    val bytes = mutable.ArrayBuffer.empty[Array[Byte]]
    def flush(): Unit = {
      val packed = layout.elementType match {
        case VectorElementType.Int8 | VectorElementType.Bit =>
          val out = QueryMatrix.packBytes(bytes.toSeq, layout)
          bytes.clear()
          out
        case _ =>
          val out = QueryMatrix.packFloats(floats.toSeq, layout)
          floats.clear()
          out
      }
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
      layout.elementType match {
        case VectorElementType.Int8 =>
          bytes += int8Values(id, row.getArray(1).toShortArray(), layout)
        case VectorElementType.Bit =>
          bytes += row.getBinary(1)
        case _ =>
          floats += finiteValues(
            id,
            row.getArray(1).toFloatArray(),
            layout,
            metric
          )
      }
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

  /** Every query id appears once. A repeated id would make two different
    * answers carry the same name.
    */
  def checkUnique(ids: Array[Long]): Unit = {
    val seen = new java.util.HashSet[java.lang.Long](ids.length * 2)
    ids.foreach { id =>
      require(
        seen.add(id),
        s"Query id $id appears more than once in the query set"
      )
    }
  }

  private def finite(
      row: Row,
      layout: VectorLayout,
      metric: String
  ): Array[Float] =
    finiteValues(row.getLong(0), row.getSeq[Float](1).toArray, layout, metric)

  private def finiteValues(
      id: Long,
      values: Array[Float],
      layout: VectorLayout,
      metric: String
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
      metric != "COSINE" || values.exists(_ != 0.0f),
      s"Query $id has a zero norm, which COSINE has no answer for"
    )
    values
  }

  private def int8(row: Row, layout: VectorLayout): Array[Byte] =
    int8Values(row.getLong(0), row.getSeq[Short](1).toArray, layout)

  private def int8Values(
      id: Long,
      values: Array[Short],
      layout: VectorLayout
  ): Array[Byte] = {
    require(
      values.length == layout.dimension,
      s"Query $id has ${values.length} values; the field has ${layout.dimension} dimensions"
    )
    values.map { value =>
      require(
        value >= -128 && value <= 127,
        s"Query $id holds $value, outside what an int8 vector takes"
      )
      value.toByte
    }
  }
}
