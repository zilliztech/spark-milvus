package com.zilliz.spark.connector

import java.nio.{ByteBuffer, ByteOrder}

import com.google.protobuf.ByteString

import com.zilliz.milvus.storage.codec.{
  FloatConverter,
  SparseFloatVectorConverter
}
import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{
  ArrayArray,
  BoolArray,
  BytesArray,
  DataType,
  DoubleArray,
  FieldData,
  FieldSchema,
  FloatArray,
  GeometryArray,
  IntArray,
  JSONArray,
  LongArray,
  ScalarField,
  SparseFloatArray,
  StringArray,
  VectorField
}

object MilvusFieldData {
  def packBoolFieldData(
      fieldName: String,
      fieldValues: Seq[Boolean]
  ): FieldData = {
    FieldData(
      `type` = DataType.Bool,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.BoolData(BoolArray(data = fieldValues))
        )
      )
    )
  }

  def packInt8FieldData(
      fieldName: String,
      fieldValues: Seq[Short]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int8,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.IntData(IntArray(data = fieldValues.map(_.toInt)))
        )
      )
    )
  }

  def packInt16FieldData(
      fieldName: String,
      fieldValues: Seq[Short]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int16,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.IntData(IntArray(data = fieldValues.map(_.toInt)))
        )
      )
    )
  }

  def packInt32FieldData(
      fieldName: String,
      fieldValues: Seq[Int]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int32,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(
          data = ScalarField.Data.IntData(
            IntArray(data = fieldValues)
          )
        )
      )
    )
  }

  def packInt64FieldData(
      fieldName: String,
      fieldValues: Seq[Long]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int64,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.LongData(LongArray(data = fieldValues))
        )
      )
    )
  }

  def packFloatFieldData(
      fieldName: String,
      fieldValues: Seq[Float]
  ): FieldData = {
    FieldData(
      `type` = DataType.Float,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.FloatData(FloatArray(data = fieldValues))
        )
      )
    )
  }

  def packDoubleFieldData(
      fieldName: String,
      fieldValues: Seq[Double]
  ): FieldData = {
    FieldData(
      `type` = DataType.Double,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.DoubleData(DoubleArray(data = fieldValues))
        )
      )
    )
  }

  def packStringFieldData(
      fieldName: String,
      fieldValues: Seq[String]
  ): FieldData = {
    FieldData(
      `type` = DataType.VarChar,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.StringData(StringArray(data = fieldValues))
        )
      )
    )
  }

  def packArrayFieldData(
      fieldName: String,
      fieldValues: Seq[ScalarField],
      elementType: DataType
  ): FieldData = {
    FieldData(
      `type` = DataType.Array,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.ArrayData(
            ArrayArray(
              data = fieldValues,
              elementType = elementType
            )
          )
        )
      )
    )
  }

  def packJsonFieldData(
      fieldName: String,
      fieldValues: Seq[String]
  ): FieldData = {
    FieldData(
      `type` = DataType.JSON,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.JsonData(
            JSONArray(data = fieldValues.map(ByteString.copyFromUtf8))
          )
        )
      )
    )
  }

  def packGeometryFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Byte]]
  ): FieldData = {
    FieldData(
      `type` = DataType.Geometry,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.GeometryData(
            GeometryArray(
              data =
                fieldValues.map(bytes => ByteString.copyFrom(bytes.toArray))
            )
          )
        )
      )
    )
  }

  def packFloatVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues = fieldValues.flatten.toArray
    FieldData(
      `type` = DataType.FloatVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.FloatVector(
            value = FloatArray(data = allValues)
          )
        )
      )
    )
  }

  def packBinaryVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Byte]],
      dim: Int
  ): FieldData = {
    val allValues = ByteString.copyFrom(fieldValues.flatten.toArray)
    FieldData(
      `type` = DataType.BinaryVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.BinaryVector(allValues)
        )
      )
    )
  }

  def packInt8VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Short]],
      dim: Int
  ): FieldData = {
    val allValues = fieldValues.flatten.toArray.map(_.toByte)
    FieldData(
      `type` = DataType.Int8Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Int8Vector(
            value = ByteString.copyFrom(allValues)
          )
        )
      )
    )
  }

  def packFloat16VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues =
      fieldValues.flatten.map(FloatConverter.toFloat16Bytes).flatten
    FieldData(
      `type` = DataType.Float16Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Float16Vector(
            value = ByteString.copyFrom(allValues.toArray)
          )
        )
      )
    )
  }

  def packBFloat16VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues =
      fieldValues.flatten.map(FloatConverter.toBFloat16Bytes).flatten
    FieldData(
      `type` = DataType.BFloat16Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Bfloat16Vector(
            value = ByteString.copyFrom(allValues.toArray)
          )
        )
      )
    )
  }

  def packSparseFloatVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Map[Long, Float]],
      dim: Int
  ): FieldData = {
    val sparseDim = fieldValues.map(_.size).max
    val allValues = fieldValues
      .map(SparseFloatVectorConverter.encodeSparseFloatVector)
      .toArray
    FieldData(
      `type` = DataType.SparseFloatVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.SparseFloatVector(
            SparseFloatArray(
              contents = allValues.map(ByteString.copyFrom),
              dim = sparseDim
            )
          )
        )
      )
    )
  }
}
