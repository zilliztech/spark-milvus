package com.zilliz.spark.connector.operations.backfill

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, to_json, udf}
import org.apache.spark.sql.types._

import com.zilliz.spark.connector.{DataParseException, FloatConverter}
import com.zilliz.spark.connector.read.Field
import com.zilliz.spark.connector.serde.ArrowConverter
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Normalizes user-facing vector values to the byte layout Milvus stores in
  * Arrow/parquet column groups.
  *
  * Dense vectors are represented as one little-endian byte array per row:
  *
  *   - FloatVector: dim * 4 bytes
  *   - BinaryVector: dim / 8 bytes
  *   - Float16Vector / BFloat16Vector: dim * 2 bytes
  *   - Int8Vector: dim bytes
  *
  * SparseFloatVector is represented as a variable-width byte array containing
  * sorted `(uint32 index, float32 value)` pairs (8 bytes per non-zero entry).
  * The writer later chooses FixedSizeBinary for non-nullable dense vectors and
  * Binary for nullable dense vectors / sparse vectors, matching Milvus's
  * storage schema.
  */
private[backfill] object VectorBackfillSupport {

  private val DenseVectorTypes: Set[MilvusDataType] = Set(
    MilvusDataType.FloatVector,
    MilvusDataType.BinaryVector,
    MilvusDataType.Float16Vector,
    MilvusDataType.BFloat16Vector,
    MilvusDataType.Int8Vector
  )

  private val VectorTypes: Set[MilvusDataType] =
    DenseVectorTypes + MilvusDataType.SparseFloatVector

  private val mapper = new ObjectMapper()

  def isVectorField(field: Field): Boolean =
    VectorTypes.contains(MilvusDataType.fromValue(field.dataType))

  /** Spark schema used only inside backfill. BinaryType deliberately carries
    * the already-normalized Milvus row bytes so joins never widen or
    * reinterpret half/binary/int8/sparse vector values.
    */
  def canonicalStructField(field: Field): StructField = {
    val milvusType = MilvusDataType.fromValue(field.dataType)
    require(
      VectorTypes.contains(milvusType),
      s"Field '${field.name}' is not a supported vector type: $milvusType"
    )

    val metadata = new MetadataBuilder()
      .putLong(ArrowConverter.MilvusDataTypeMetadataKey, field.dataType.toLong)

    if (DenseVectorTypes.contains(milvusType)) {
      metadata.putLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey,
        dimension(field).toLong
      )
    }

    StructField(
      field.name,
      BinaryType,
      nullable = field.nullable.getOrElse(false),
      metadata = metadata.build()
    )
  }

  /** Convert every vector column in `targetFieldsByName` to canonical Milvus
    * bytes while preserving non-vector columns unchanged.
    */
  def normalizeVectorColumns(
      df: DataFrame,
      targetFieldsByName: Map[String, Field]
  ): Either[BackfillError, DataFrame] = {
    if (!targetFieldsByName.values.exists(isVectorField)) {
      return Right(df)
    }

    val projected = df.schema.fields.map { inputField =>
      targetFieldsByName.get(inputField.name) match {
        case Some(targetField) if isVectorField(targetField) =>
          normalizeVectorColumn(inputField, targetField).map { normalized =>
            val canonical = canonicalStructField(targetField)
            normalized.as(inputField.name, canonical.metadata)
          }
        case _ => Right(col(inputField.name))
      }
    }

    projected.collectFirst { case Left(error) => error } match {
      case Some(error) => Left(error)
      case None =>
        Right(df.select(projected.collect { case Right(column) => column }: _*))
    }
  }

  private def normalizeVectorColumn(
      inputField: StructField,
      targetField: Field
  ): Either[BackfillError, Column] = {
    val fieldName = inputField.name
    val milvusType = MilvusDataType.fromValue(targetField.dataType)
    val dim =
      if (DenseVectorTypes.contains(milvusType)) dimension(targetField) else 0

    def internalBinary(column: Column): Column = {
      val encoder = udf((bytes: Array[Byte]) =>
        if (bytes == null) null
        else validateInternalBytes(fieldName, milvusType, dim, bytes)
      )
      encoder(column)
    }

    def jsonArray(column: Column, encodedHalfBytes: Boolean): Column = {
      val encoder = udf((json: String) =>
        if (json == null) null
        else
          encodeDenseJsonArray(
            fieldName,
            milvusType,
            dim,
            json,
            encodedHalfBytes
          )
      )
      encoder(column)
    }

    def denseArray(
        column: Column,
        elementType: DataType,
        encodedHalfBytes: Boolean
    ): Column = {
      elementType match {
        case ByteType =>
          val encoder = udf((values: Seq[Byte]) =>
            encodeDenseIntegralNumbers(
              fieldName,
              milvusType,
              dim,
              values,
              encodedHalfBytes
            )
          )
          encoder(column)
        case ShortType =>
          val encoder = udf((values: Seq[Short]) =>
            encodeDenseIntegralNumbers(
              fieldName,
              milvusType,
              dim,
              values,
              encodedHalfBytes
            )
          )
          encoder(column)
        case IntegerType =>
          val encoder = udf((values: Seq[Int]) =>
            encodeDenseIntegralNumbers(
              fieldName,
              milvusType,
              dim,
              values,
              encodedHalfBytes
            )
          )
          encoder(column)
        case LongType =>
          val encoder = udf((values: Seq[Long]) =>
            encodeDenseIntegralNumbers(
              fieldName,
              milvusType,
              dim,
              values,
              encodedHalfBytes
            )
          )
          encoder(column)
        case FloatType =>
          val encoder = udf((values: Seq[Float]) =>
            encodeDenseFloatingNumbers(
              fieldName,
              milvusType,
              dim,
              values
            )
          )
          encoder(column)
        case DoubleType =>
          val encoder = udf((values: Seq[Double]) =>
            encodeDenseFloatingNumbers(
              fieldName,
              milvusType,
              dim,
              values
            )
          )
          encoder(column)
        case _: DecimalType =>
          // Decimal arrays are uncommon for vectors. Keep compatibility while
          // avoiding a fragile Decimal external-type UDF contract.
          jsonArray(to_json(column), encodedHalfBytes = false)
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported dense vector element type $other"
          )
      }
    }

    def sparseJson(column: Column): Column = {
      val encoder = udf((json: String) =>
        if (json == null) null else encodeSparseJson(fieldName, json)
      )
      encoder(column)
    }

    val input = col(fieldName)
    inputField.dataType match {
      case BinaryType =>
        Right(internalBinary(input))

      case StringType if milvusType == MilvusDataType.SparseFloatVector =>
        Right(sparseJson(input))

      case StringType if DenseVectorTypes.contains(milvusType) =>
        // CSV-style / generic parquet writers often persist a JSON array as a
        // string. For half vectors this form means numeric float values; raw
        // encoded half bytes should use BinaryType or Array[Byte/Short].
        Right(jsonArray(input, encodedHalfBytes = false))

      case ArrayType(elementType, _)
          if DenseVectorTypes.contains(milvusType) &&
            isSupportedDenseElementType(milvusType, elementType) =>
        val encodedHalfBytes =
          (milvusType == MilvusDataType.Float16Vector ||
            milvusType == MilvusDataType.BFloat16Vector) &&
            (elementType == ByteType || elementType == ShortType)
        Right(denseArray(input, elementType, encodedHalfBytes))

      case MapType(keyType, valueType, _)
          if milvusType == MilvusDataType.SparseFloatVector &&
            isSparseKeyType(keyType) && isNumericType(valueType) =>
        Right(sparseJson(to_json(input)))

      case structType: StructType
          if milvusType == MilvusDataType.SparseFloatVector &&
            isSparseStruct(structType) =>
        Right(sparseJson(to_json(input)))

      case other =>
        Left(
          SchemaValidationError(
            s"Vector field '$fieldName' targets $milvusType but parquet type " +
              s"${other.simpleString} is unsupported. Accepted forms: " +
              acceptedForms(milvusType)
          )
        )
    }
  }

  private def acceptedForms(milvusType: MilvusDataType): String =
    milvusType match {
      case MilvusDataType.FloatVector =>
        "array<numeric>, JSON array string, or internal binary"
      case MilvusDataType.BinaryVector =>
        "array<integral byte>, JSON byte-array string, or internal binary"
      case MilvusDataType.Float16Vector | MilvusDataType.BFloat16Vector =>
        "array<float/double> values, array<byte/short> encoded bytes, JSON numeric-array string, or internal binary"
      case MilvusDataType.Int8Vector =>
        "array<integral>, JSON integral-array string, or internal binary"
      case MilvusDataType.SparseFloatVector =>
        "map<integral|string,numeric>, struct<indices:array,values:array>, JSON object string, or internal binary"
      case _ => "a supported Milvus vector representation"
    }

  private def isSupportedDenseElementType(
      milvusType: MilvusDataType,
      elementType: DataType
  ): Boolean = milvusType match {
    case MilvusDataType.FloatVector => isNumericType(elementType)
    case MilvusDataType.Float16Vector | MilvusDataType.BFloat16Vector =>
      isNumericType(elementType)
    case MilvusDataType.BinaryVector | MilvusDataType.Int8Vector =>
      isIntegralType(elementType)
    case _ => false
  }

  private def isNumericType(dataType: DataType): Boolean =
    isIntegralType(dataType) || dataType == FloatType ||
      dataType == DoubleType || dataType.isInstanceOf[DecimalType]

  private def isIntegralType(dataType: DataType): Boolean =
    dataType == ByteType || dataType == ShortType ||
      dataType == IntegerType || dataType == LongType

  private def isSparseKeyType(dataType: DataType): Boolean =
    isIntegralType(dataType) || dataType == StringType

  private def isSparseStruct(structType: StructType): Boolean = {
    val fields =
      structType.fields.map(field => field.name -> field.dataType).toMap
    (fields.get("indices"), fields.get("values")) match {
      case (Some(ArrayType(indexType, _)), Some(ArrayType(valueType, _))) =>
        isIntegralType(indexType) && isNumericType(valueType)
      case _ => false
    }
  }

  private def dimension(field: Field): Int = {
    val dim = field
      .getTypeParam("dim")
      .flatMap(value => scala.util.Try(value.toInt).toOption)
      .filter(_ > 0)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Vector field '${field.name}' (${MilvusDataType.fromValue(field.dataType)}) has no valid positive dim"
        )
      }

    if (
      MilvusDataType.fromValue(field.dataType) == MilvusDataType.BinaryVector &&
      dim % 8 != 0
    ) {
      throw new IllegalArgumentException(
        s"BinaryVector field '${field.name}' dimension must be a multiple of 8, got $dim"
      )
    }
    dim
  }

  private def requireNoNullElements(
      fieldName: String,
      values: Seq[_]
  ): Unit = {
    if (values.exists(_ == null)) {
      fail(fieldName, "vector arrays cannot contain null elements")
    }
  }

  private def encodeDenseIntegralNumbers(
      fieldName: String,
      milvusType: MilvusDataType,
      dim: Int,
      values: Seq[_],
      encodedHalfBytes: Boolean
  ): Array[Byte] =
    if (values == null) null
    else {
      requireNoNullElements(fieldName, values)
      encodeDenseIntegralArray(
        fieldName,
        milvusType,
        dim,
        values.map(_.asInstanceOf[java.lang.Number].longValue()),
        encodedHalfBytes
      )
    }

  private def encodeDenseFloatingNumbers(
      fieldName: String,
      milvusType: MilvusDataType,
      dim: Int,
      values: Seq[_]
  ): Array[Byte] =
    if (values == null) null
    else {
      requireNoNullElements(fieldName, values)
      encodeDenseFloatingArray(
        fieldName,
        milvusType,
        dim,
        values.map(_.asInstanceOf[java.lang.Number].doubleValue())
      )
    }

  private def encodeDenseFloatingArray(
      fieldName: String,
      milvusType: MilvusDataType,
      dim: Int,
      values: Seq[Double]
  ): Array[Byte] = {
    requireDimension(fieldName, dim, values.size)
    milvusType match {
      case MilvusDataType.FloatVector =>
        val bytes = ByteBuffer
          .allocate(dim * 4)
          .order(ByteOrder.LITTLE_ENDIAN)
        values.foreach(value => bytes.putFloat(finiteFloat(fieldName, value)))
        bytes.array()

      case MilvusDataType.Float16Vector =>
        val bytes = ByteBuffer
          .allocate(dim * 2)
          .order(ByteOrder.LITTLE_ENDIAN)
        values.foreach { value =>
          val floatValue = finiteFloat(fieldName, value)
          bytes.put(float16Bytes(fieldName, floatValue))
        }
        bytes.array()

      case MilvusDataType.BFloat16Vector =>
        val bytes = ByteBuffer
          .allocate(dim * 2)
          .order(ByteOrder.LITTLE_ENDIAN)
        values.foreach { value =>
          val floatValue = finiteFloat(fieldName, value)
          bytes.putShort(
            (java.lang.Float.floatToRawIntBits(floatValue) >>> 16).toShort
          )
        }
        bytes.array()

      case other =>
        fail(fieldName, s"$other does not accept floating-point arrays")
    }
  }

  private def encodeDenseIntegralArray(
      fieldName: String,
      milvusType: MilvusDataType,
      dim: Int,
      values: Seq[Long],
      encodedHalfBytes: Boolean
  ): Array[Byte] = milvusType match {
    case MilvusDataType.FloatVector =>
      encodeDenseFloatingArray(
        fieldName,
        milvusType,
        dim,
        values.map(_.toDouble)
      )

    case MilvusDataType.Float16Vector | MilvusDataType.BFloat16Vector
        if !encodedHalfBytes =>
      encodeDenseFloatingArray(
        fieldName,
        milvusType,
        dim,
        values.map(_.toDouble)
      )

    case MilvusDataType.BinaryVector =>
      requireByteWidth(fieldName, milvusType, dim / 8, values.size)
      values
        .map(value => byteValue(fieldName, value, allowUnsigned = true))
        .toArray

    case MilvusDataType.Int8Vector =>
      requireDimension(fieldName, dim, values.size)
      values
        .map(value => byteValue(fieldName, value, allowUnsigned = false))
        .toArray

    case MilvusDataType.Float16Vector | MilvusDataType.BFloat16Vector =>
      requireByteWidth(fieldName, milvusType, dim * 2, values.size)
      val bytes = values
        .map(value => byteValue(fieldName, value, allowUnsigned = true))
        .toArray
      validateInternalBytes(fieldName, milvusType, dim, bytes)

    case other =>
      fail(fieldName, s"$other does not accept integral arrays")
  }

  private[backfill] def encodeDenseJsonArray(
      fieldName: String,
      milvusType: MilvusDataType,
      dim: Int,
      json: String,
      encodedHalfBytes: Boolean
  ): Array[Byte] = {
    val node = parseJson(fieldName, json)
    if (!node.isArray) {
      fail(fieldName, s"expected a JSON array, got ${node.getNodeType}")
    }
    val elements = node.elements().asScala.toSeq

    milvusType match {
      case MilvusDataType.FloatVector =>
        requireDimension(fieldName, dim, elements.size)
        val bytes = ByteBuffer
          .allocate(dim * 4)
          .order(ByteOrder.LITTLE_ENDIAN)
        elements.foreach(node => bytes.putFloat(finiteFloat(fieldName, node)))
        bytes.array()

      case MilvusDataType.BinaryVector =>
        val expectedBytes = dim / 8
        requireByteWidth(fieldName, milvusType, expectedBytes, elements.size)
        elements
          .map(node => byteValue(fieldName, node, allowUnsigned = true))
          .toArray

      case MilvusDataType.Int8Vector =>
        requireDimension(fieldName, dim, elements.size)
        elements.map(node => int8Value(fieldName, node)).toArray

      case MilvusDataType.Float16Vector | MilvusDataType.BFloat16Vector
          if encodedHalfBytes =>
        val expectedBytes = dim * 2
        requireByteWidth(fieldName, milvusType, expectedBytes, elements.size)
        val bytes = elements
          .map(node => byteValue(fieldName, node, allowUnsigned = true))
          .toArray
        validateInternalBytes(fieldName, milvusType, dim, bytes)

      case MilvusDataType.Float16Vector =>
        requireDimension(fieldName, dim, elements.size)
        val bytes = ByteBuffer
          .allocate(dim * 2)
          .order(ByteOrder.LITTLE_ENDIAN)
        elements.foreach { node =>
          val value = finiteFloat(fieldName, node)
          bytes.put(float16Bytes(fieldName, value))
        }
        bytes.array()

      case MilvusDataType.BFloat16Vector =>
        requireDimension(fieldName, dim, elements.size)
        val bytes = ByteBuffer
          .allocate(dim * 2)
          .order(ByteOrder.LITTLE_ENDIAN)
        elements.foreach { node =>
          val value = finiteFloat(fieldName, node)
          bytes.putShort(
            (java.lang.Float.floatToRawIntBits(value) >>> 16).toShort
          )
        }
        bytes.array()

      case other =>
        fail(fieldName, s"$other is not a dense vector type")
    }
  }

  private[backfill] def encodeSparseJson(
      fieldName: String,
      json: String
  ): Array[Byte] = {
    val node = parseJson(fieldName, json)
    if (!node.isObject) {
      fail(fieldName, s"expected a sparse JSON object, got ${node.getNodeType}")
    }

    val entries: Seq[(Long, Float)] = {
      val indices = node.get("indices")
      val values = node.get("values")
      if (indices != null || values != null) {
        if (
          indices == null || values == null || !indices.isArray || !values.isArray
        ) {
          fail(
            fieldName,
            "sparse struct format requires array fields 'indices' and 'values'"
          )
        }
        val indexNodes = indices.elements().asScala.toSeq
        val valueNodes = values.elements().asScala.toSeq
        if (indexNodes.size != valueNodes.size) {
          fail(
            fieldName,
            s"sparse indices/values length mismatch: ${indexNodes.size} != ${valueNodes.size}"
          )
        }
        indexNodes.zip(valueNodes).map { case (index, value) =>
          sparseIndex(fieldName, index) -> sparseValue(fieldName, value)
        }
      } else {
        node
          .fields()
          .asScala
          .map { entry =>
            val indexNode = mapper.getNodeFactory.numberNode(
              parseSparseMapIndex(fieldName, entry.getKey)
            )
            sparseIndex(fieldName, indexNode) -> sparseValue(
              fieldName,
              entry.getValue
            )
          }
          .toSeq
      }
    }

    val sorted = entries.sortBy(_._1)
    sorted.sliding(2).foreach {
      case Seq((left, _), (right, _)) if left == right =>
        fail(fieldName, s"sparse vector contains duplicate index $left")
      case _ =>
    }

    val bytes = ByteBuffer
      .allocate(sorted.size * 8)
      .order(ByteOrder.LITTLE_ENDIAN)
    sorted.foreach { case (index, value) =>
      bytes.putInt(index.toInt)
      bytes.putFloat(value)
    }
    bytes.array()
  }

  private[backfill] def validateInternalBytes(
      fieldName: String,
      milvusType: MilvusDataType,
      dim: Int,
      bytes: Array[Byte]
  ): Array[Byte] = {
    milvusType match {
      case MilvusDataType.FloatVector =>
        requireByteWidth(fieldName, milvusType, dim * 4, bytes.length)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        while (buffer.remaining() >= 4) {
          val value = buffer.getFloat()
          if (!java.lang.Float.isFinite(value)) {
            fail(fieldName, s"$milvusType contains non-finite value $value")
          }
        }

      case MilvusDataType.BinaryVector =>
        requireByteWidth(fieldName, milvusType, dim / 8, bytes.length)

      case MilvusDataType.Int8Vector =>
        requireByteWidth(fieldName, milvusType, dim, bytes.length)

      case MilvusDataType.Float16Vector =>
        requireByteWidth(fieldName, milvusType, dim * 2, bytes.length)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        while (buffer.remaining() >= 2) {
          val bits = buffer.getShort() & 0xffff
          if (((bits >>> 10) & 0x1f) == 0x1f) {
            fail(fieldName, s"$milvusType contains NaN or infinity")
          }
        }

      case MilvusDataType.BFloat16Vector =>
        requireByteWidth(fieldName, milvusType, dim * 2, bytes.length)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        while (buffer.remaining() >= 2) {
          val bits = buffer.getShort() & 0xffff
          if (((bits >>> 7) & 0xff) == 0xff) {
            fail(fieldName, s"$milvusType contains NaN or infinity")
          }
        }

      case MilvusDataType.SparseFloatVector =>
        if (bytes.length % 8 != 0) {
          fail(
            fieldName,
            s"SparseFloatVector binary length must be divisible by 8, got ${bytes.length}"
          )
        }
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        var previous = -1L
        while (buffer.remaining() >= 8) {
          val index = buffer.getInt() & 0xffffffffL
          val value = buffer.getFloat()
          if (index == 0xffffffffL) {
            fail(
              fieldName,
              "SparseFloatVector index must be less than 2^32 - 1"
            )
          }
          if (index <= previous) {
            fail(
              fieldName,
              s"SparseFloatVector indices must be strictly increasing, got $index after $previous"
            )
          }
          if (!java.lang.Float.isFinite(value)) {
            fail(
              fieldName,
              s"SparseFloatVector contains non-finite value $value"
            )
          }
          if (value < 0.0f) {
            fail(fieldName, s"SparseFloatVector contains negative value $value")
          }
          previous = index
        }

      case other =>
        fail(fieldName, s"$other is not a supported vector type")
    }
    bytes
  }

  private def parseJson(fieldName: String, json: String): JsonNode =
    try mapper.readTree(json)
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Vector field '$fieldName' contains invalid JSON: ${e.getMessage}",
          e
        )
    }

  private def finiteFloat(fieldName: String, node: JsonNode): Float = {
    if (node == null || !node.isNumber) {
      fail(fieldName, s"expected a numeric vector value, got $node")
    }
    finiteFloat(fieldName, node.doubleValue())
  }

  private def finiteFloat(fieldName: String, doubleValue: Double): Float = {
    val floatValue = doubleValue.toFloat
    if (
      !java.lang.Double
        .isFinite(doubleValue) || !java.lang.Float.isFinite(floatValue)
    ) {
      fail(
        fieldName,
        s"vector value is non-finite or outside float32 range: $doubleValue"
      )
    }
    floatValue
  }

  private def sparseValue(fieldName: String, node: JsonNode): Float = {
    val value = finiteFloat(fieldName, node)
    if (value < 0.0f) {
      fail(fieldName, s"sparse vector value must be non-negative, got $value")
    }
    value
  }

  private def integralValue(fieldName: String, node: JsonNode): Long = {
    if (node == null || !node.isNumber) {
      fail(fieldName, s"expected an integral vector value, got $node")
    }
    val decimal = node.decimalValue()
    try decimal.longValueExact()
    catch {
      case _: ArithmeticException =>
        fail(fieldName, s"expected an integral vector value, got $node")
    }
  }

  private def byteValue(
      fieldName: String,
      node: JsonNode,
      allowUnsigned: Boolean
  ): Byte =
    byteValue(fieldName, integralValue(fieldName, node), allowUnsigned)

  private def byteValue(
      fieldName: String,
      value: Long,
      allowUnsigned: Boolean
  ): Byte = {
    val max = if (allowUnsigned) 255L else 127L
    if (value < -128L || value > max) {
      fail(fieldName, s"byte value $value is outside [-128, $max]")
    }
    value.toByte
  }

  private def int8Value(fieldName: String, node: JsonNode): Byte =
    byteValue(fieldName, node, allowUnsigned = false)

  private def sparseIndex(fieldName: String, node: JsonNode): Long = {
    val value = integralValue(fieldName, node)
    if (value < 0L || value >= 0xffffffffL) {
      fail(
        fieldName,
        s"sparse index $value is outside [0, ${0xffffffffL - 1L}]"
      )
    }
    value
  }

  private def parseSparseMapIndex(fieldName: String, value: String): Long =
    try java.lang.Long.parseLong(value)
    catch {
      case _: NumberFormatException =>
        fail(fieldName, s"sparse map key '$value' is not an integer index")
    }

  private def requireDimension(
      fieldName: String,
      expected: Int,
      actual: Int
  ): Unit = {
    if (actual != expected) {
      fail(
        fieldName,
        s"vector dimension mismatch: expected $expected, got $actual"
      )
    }
  }

  private def requireByteWidth(
      fieldName: String,
      milvusType: MilvusDataType,
      expected: Int,
      actual: Int
  ): Unit = {
    if (actual != expected) {
      fail(
        fieldName,
        s"$milvusType byte-width mismatch: expected $expected, got $actual"
      )
    }
  }

  private def float16Bytes(fieldName: String, value: Float): Array[Byte] =
    try FloatConverter.toFloat16Bytes(value).toArray
    catch {
      case e: DataParseException => fail(fieldName, e.getMessage)
    }

  private def fail(fieldName: String, message: String): Nothing =
    throw new IllegalArgumentException(s"Vector field '$fieldName': $message")
}
