package com.zilliz.spark.connector.read

import java.lang.{Float => JavaFloat}

import org.apache.spark.sql.{DataFrame, Row}
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

/** The query set a search takes: `query_id` and `vector`, checked against the
  * field being searched and packed into the bytes Knowhere reads.
  *
  * The same packing serves both delivery paths of section 2.1: the driver packs
  * the whole set when it fits `milvus.search.queries.max.bytes` and broadcasts
  * it, and the packing job packs one group per task when it does not.
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
  ): Array[Float] = {
    val values = row.getSeq[Float](1).toArray
    require(
      values.length == layout.dimension,
      s"Query ${row.getLong(0)} has ${values.length} values; the field has ${layout.dimension} dimensions"
    )
    require(
      values.forall(JavaFloat.isFinite),
      s"Query ${row.getLong(0)} holds a value that is not finite"
    )
    require(
      metric != "COSINE" || values.exists(_ != 0.0f),
      s"Query ${row.getLong(0)} has a zero norm, which COSINE has no answer for"
    )
    values
  }

  private def int8(row: Row, layout: VectorLayout): Array[Byte] = {
    val values = row.getSeq[Short](1)
    require(
      values.length == layout.dimension,
      s"Query ${row.getLong(0)} has ${values.length} values; the field has ${layout.dimension} dimensions"
    )
    values.map { value =>
      require(
        value >= -128 && value <= 127,
        s"Query ${row.getLong(0)} holds $value, outside what an int8 vector takes"
      )
      value.toByte
    }.toArray
  }
}
