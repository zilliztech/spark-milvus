package com.zilliz.spark.connector.types

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.vector.{
  FieldVector,
  FixedSizeBinaryVector,
  VarBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  Decimal,
  StructField,
  StructType
}
import org.apache.spark.sql.vectorized.{
  ArrowColumnVector,
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.schema.{ArrowTypes, MilvusTypes}
import io.milvus.grpc.schema.{
  DataType => MilvusType,
  FieldSchema,
  StructArrayFieldSchema
}

/** Presents sibling Milvus arrays as one Spark array of structs. All stored
  * members of a parent have the same validity and length in each row. Their
  * flat value vectors supply the fields of each struct directly.
  */
private[connector] object MilvusStructArrayColumn {
  private case class Member(
      name: String,
      isNull: Int => Boolean,
      start: Int => Int,
      length: Int => Int,
      values: ColumnVector
  )

  def apply(
      root: VectorSchemaRoot,
      parent: StructArrayFieldSchema,
      requested: StructField,
      columnNameFor: Long => Option[String],
      rawVectors: Boolean
  ): ColumnVector = {
    val requestedType = requested.dataType.asInstanceOf[ArrayType]
    val output = requestedType.elementType.asInstanceOf[StructType]
    val full = SparkTypes
      .toStructArrayField(parent, rawVectors)
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    val rows = root.getRowCount
    // Arrow vectors are borrowed from root. Only decoded protobuf arrays own
    // extra storage, and must also be closed if a later sibling fails.
    val decoded = ArrayBuffer.empty[MilvusArrayColumn]
    try {
      val members =
        parent.fields.zip(full.fields).flatMap { case (child, field) =>
          columnNameFor(child.fieldID)
            .flatMap(name => Option(root.getVector(name)))
            .map { vector =>
              require(
                vector.getValueCount == rows,
                s"Member '${child.name}' has ${vector.getValueCount} rows, expected $rows"
              )
              member(child, field, vector, rawVectors, decoded)
            }
        }
      if (members.isEmpty) {
        require(
          parent.nullable,
          s"Segment has no stored members of non-nullable struct array '${parent.name}'"
        )
        return nullColumn(rows, requestedType)
      }
      val anchor = members.head
      val siblings = members.tail
      val offsets = new Array[Int](rows + 1)
      var row = 0
      while (row < rows) {
        val absent = anchor.isNull(row)
        require(
          !absent || parent.nullable,
          s"Non-nullable struct array '${parent.name}' is null at row $row"
        )
        val length = if (absent) 0 else anchor.length(row)
        siblings.foreach { sibling =>
          require(
            sibling.isNull(row) == absent &&
              (absent || sibling.length(row) == length),
            s"Struct array '${parent.name}' has inconsistent validity or length at row $row in '${sibling.name}' and '${anchor.name}'"
          )
        }
        offsets(row + 1) = Math.addExact(offsets(row), length)
        row += 1
      }
      val byName = members.map(m => m.name -> m).toMap
      full.fields.foreach { field =>
        require(
          byName.contains(field.name) || field.nullable,
          s"Segment is missing required member '${parent.name}[${field.name}]'"
        )
      }
      val children = output.fields.map { field =>
        byName.get(field.name) match {
          case Some(value) => aligned(value, offsets)
          case None        => nullColumn(offsets.last, field.dataType)
        }
      }
      val structs = new NestedColumn(output) {
        override def close(): Unit = ()
        override def numNulls(): Int = 0
        override def isNullAt(row: Int): Boolean = false
        override def getChild(ordinal: Int): ColumnVector = children(ordinal)
      }
      new NestedColumn(requestedType) {
        override def close(): Unit = decoded.foreach(_.close())
        private lazy val nullRows = (0 until rows).count(anchor.isNull)
        override def numNulls(): Int = nullRows
        override def isNullAt(row: Int): Boolean = anchor.isNull(row)
        override def getArray(row: Int): ColumnarArray =
          new ColumnarArray(
            structs,
            offsets(row),
            offsets(row + 1) - offsets(row)
          )
        override def getChild(ordinal: Int): ColumnVector = structs
      }
    } catch {
      case error: Throwable =>
        decoded.foreach(_.close())
        throw error
    }
  }

  private def nullColumn(size: Int, dataType: DataType): ColumnVector = {
    val column = new ConstantColumnVector(size, dataType)
    column.setNull()
    column
  }

  /** Sliced Arrow lists and null list slots can leave gaps between values. Most
    * readers normalize offsets already, so those batches keep their original
    * vector with no per-element indirection.
    */
  private def aligned(member: Member, offsets: Array[Int]): ColumnVector = {
    val rows = 0 until offsets.length - 1
    if (
      rows.forall(i =>
        offsets(i + 1) == offsets(i) || member.start(i) == offsets(i)
      )
    )
      member.values
    else {
      val selected = new Array[Int](offsets.last)
      rows.foreach { i =>
        var j = offsets(i)
        while (j < offsets(i + 1)) {
          selected(j) = member.start(i) + j - offsets(i)
          j += 1
        }
      }
      new SelectedRowsColumn(member.values, selected, member.values.dataType())
    }
  }

  private def member(
      child: FieldSchema,
      field: StructField,
      vector: FieldVector,
      rawVectors: Boolean,
      decoded: ArrayBuffer[MilvusArrayColumn]
  ): Member = vector match {
    case bytes: VarBinaryVector
        if child.dataType == MilvusType.Array &&
          !child.elementNullable && child.elementType != MilvusType.Array =>
      val column = new MilvusArrayColumn(bytes, field.dataType)
      decoded += column
      Member(
        field.name,
        bytes.isNull,
        column.elementOffset,
        column.elementCount,
        column.getChild(0)
      )
    case list: ListVector
        if child.dataType == MilvusType.ArrayOfVector ||
          child.elementNullable || child.elementType == MilvusType.Array =>
      val data = list.getDataVector
      if (!child.elementNullable && data.getNullCount > 0) {
        foreachElement(list) { i =>
          require(
            !data.isNull(i),
            s"Non-nullable member '${child.name}' contains a null element"
          )
        }
      }
      val values = if (child.dataType == MilvusType.ArrayOfVector) {
        val dim = MilvusTypes.dimensionOf(child)
        val width = ArrowTypes
          .toArrowType(dim, child.elementType)
          .asInstanceOf[ArrowType.FixedSizeBinary]
          .getByteWidth
        data match {
          case bytes: FixedSizeBinaryVector if !child.elementNullable =>
            require(
              bytes.getByteWidth == width,
              s"Vector member '${child.name}' expected $width bytes, found ${bytes.getByteWidth}"
            )
          case bytes: VarBinaryVector if child.elementNullable =>
            foreachElement(list) { i =>
              require(
                bytes.isNull(i) || bytes.getEndOffset(i) - bytes.getStartOffset(
                  i
                ) == width,
                s"Vector member '${child.name}' expected $width bytes at element $i"
              )
            }
          case _ =>
            throw new IllegalArgumentException(
              s"Vector member '${child.name}' has unsupported Arrow storage ${data.getField}"
            )
        }
        MilvusVectorColumn(data, child.elementType, dim, rawVectors)
      } else {
        val column = new ArrowColumnVector(data)
        require(
          DataType.equalsIgnoreNullability(column.dataType(), field.dataType),
          s"Member '${child.name}' has Arrow type ${data.getField}, expected ${field.dataType}"
        )
        field.dataType match {
          case ArrayType(_, false) =>
            val inner = data.asInstanceOf[ListVector]
            val scalars = inner.getDataVector
            if (scalars.getNullCount > 0) {
              foreachElement(list) { i =>
                if (!inner.isNull(i)) {
                  val end = inner.getElementEndIndex(i)
                  var at = inner.getElementStartIndex(i)
                  while (at < end) {
                    require(
                      !scalars.isNull(at),
                      s"Nested array '${child.name}' contains a null scalar element"
                    )
                    at += 1
                  }
                }
              }
            }
          case _ =>
        }
        column
      }
      Member(
        field.name,
        list.isNull,
        list.getElementStartIndex,
        row => list.getElementEndIndex(row) - list.getElementStartIndex(row),
        values
      )
    case _ =>
      throw new IllegalArgumentException(
        s"Struct array member '${child.name}' has unsupported Arrow storage ${vector.getField}"
      )
  }

  /** Arrow permits a null row to span unused physical elements. */
  private def foreachElement(list: ListVector)(visit: Int => Unit): Unit = {
    var row = 0
    while (row < list.getValueCount) {
      if (!list.isNull(row)) {
        var i = list.getElementStartIndex(row)
        val end = list.getElementEndIndex(row)
        while (i < end) {
          visit(i)
          i += 1
        }
      }
      row += 1
    }
  }

  /** Spark reads structs through getChild and arrays through getArray. */
  private abstract class NestedColumn(dataType: DataType)
      extends ColumnVector(dataType) {
    override def hasNull: Boolean = numNulls() > 0
    override def getBoolean(i: Int): Boolean = unsupported
    override def getByte(i: Int): Byte = unsupported
    override def getShort(i: Int): Short = unsupported
    override def getInt(i: Int): Int = unsupported
    override def getLong(i: Int): Long = unsupported
    override def getFloat(i: Int): Float = unsupported
    override def getDouble(i: Int): Double = unsupported
    override def getDecimal(i: Int, precision: Int, scale: Int): Decimal =
      unsupported
    override def getUTF8String(i: Int): UTF8String = unsupported
    override def getBinary(i: Int): Array[Byte] = unsupported
    override def getArray(i: Int): ColumnarArray = unsupported
    override def getMap(i: Int): ColumnarMap = unsupported
    private def unsupported: Nothing = throw new UnsupportedOperationException(
      s"Invalid accessor for $dataType"
    )
  }
}
