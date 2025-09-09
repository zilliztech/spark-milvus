package com.zilliz.spark.connector.sources

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{
  ArrayType,
  DataTypes => SparkDataTypes,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.{
  DataTypeException,
  MilvusClient,
  MilvusOption,
  MilvusSchemaUtil
}
import com.zilliz.spark.connector.{MilvusFieldData, MilvusRpcException}
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType,
  FieldData,
  FieldSchema
}

case class MilvusInsertDataWriter(
    partitionId: Int,
    taskId: Long,
    milvusOption: MilvusOption,
    sparkSchema: StructType
) extends DataWriter[InternalRow]
    with Serializable
    with Logging {
  private val milvusClient = MilvusClient(milvusOption)
  private val collectionSchema = milvusClient.getCollectionSchema(
    milvusOption.databaseName,
    milvusOption.collectionName
  )
  private val fieldMap = MilvusInsertDataWriter.getFieldMap(
    collectionSchema.getOrElse(
      throw new MilvusRpcException("Collection schema not found")
    )
  )
  private val maxBatchSize = milvusOption.insertMaxBatchSize
  private var dataBuffer = MilvusInsertDataWriter.newDataBuffer(
    collectionSchema.getOrElse(
      throw new MilvusRpcException("Collection schema not found")
    )
  )
  private var currentSizeInBuffer = 0
  private var totalSize = 0
  private var currentHandledBuffer = Seq.empty[FieldData]

  private def flushBuffer(retries: Int = milvusOption.retryCount): Unit = {
    if (retries <= 0) {
      throw new MilvusRpcException("Flush buffer failed")
    }
    if (currentHandledBuffer.isEmpty) {
      currentHandledBuffer = MilvusInsertDataWriter.getInsertFieldsData(
        collectionSchema.get,
        dataBuffer
      )
    }

    try {
      val insertResult = milvusClient.insert(
        milvusOption.databaseName,
        milvusOption.collectionName,
        if (milvusOption.partitionName.isEmpty) None
        else Some(milvusOption.partitionName),
        currentHandledBuffer,
        numRows = currentSizeInBuffer
      ) match {
        case Success(status) =>
          currentHandledBuffer = Seq.empty[FieldData]
          currentSizeInBuffer = 0
        case Failure(e) =>
          throw e
      }
    } catch {
      case e: Exception =>
        logWarning(
          s"Flush buffer failed, retries: ${retries}, error: ${e.getMessage}"
        )
        Thread.sleep(milvusOption.retryInterval)
        flushBuffer(retries - 1)
    }
  }

  override def write(record: InternalRow): Unit = {
    try {
      MilvusInsertDataWriter.addDataToBuffer(
        dataBuffer,
        record,
        fieldMap,
        sparkSchema
      )
      currentSizeInBuffer += 1
      if (currentSizeInBuffer >= maxBatchSize) {
        flushBuffer()
      }
    } catch {
      case e: Exception =>
        logInfo(s"Exception occurs: ${e.getMessage}")
        throw e
    }
  }

  override def commit(): WriterCommitMessage = {
    if (currentSizeInBuffer > 0) {
      flushBuffer()
    }
    MilvusCommitMessage(totalSize)
  }

  override def abort(): Unit = {
    commit()
    milvusClient.close()
    logWarning("Aborted data write")
  }

  override def close(): Unit = {
    commit()
    milvusClient.close()
    logInfo("Closed data writer")
  }
}

object MilvusInsertDataWriter {
  def newDataBuffer(
      schema: CollectionSchema
  ): Map[String, Any] = {
    schema.fields.map { field =>
      val buffer = field.dataType match {
        case MilvusDataType.Bool    => ArrayBuffer[Boolean]()
        case MilvusDataType.Int8    => ArrayBuffer[Byte]()
        case MilvusDataType.Int16   => ArrayBuffer[Short]()
        case MilvusDataType.Int32   => ArrayBuffer[Int]()
        case MilvusDataType.Int64   => ArrayBuffer[Long]()
        case MilvusDataType.Float   => ArrayBuffer[Float]()
        case MilvusDataType.Double  => ArrayBuffer[Double]()
        case MilvusDataType.String  => ArrayBuffer[String]()
        case MilvusDataType.VarChar => ArrayBuffer[String]()
        case MilvusDataType.JSON    => ArrayBuffer[String]()
        case MilvusDataType.Array => {
          field.elementType match {
            case MilvusDataType.Bool    => ArrayBuffer[Array[Boolean]]()
            case MilvusDataType.Int8    => ArrayBuffer[Array[Short]]()
            case MilvusDataType.Int16   => ArrayBuffer[Array[Short]]()
            case MilvusDataType.Int32   => ArrayBuffer[Array[Int]]()
            case MilvusDataType.Int64   => ArrayBuffer[Array[Long]]()
            case MilvusDataType.Float   => ArrayBuffer[Array[Float]]()
            case MilvusDataType.Double  => ArrayBuffer[Array[Double]]()
            case MilvusDataType.VarChar => ArrayBuffer[Array[String]]()
            case MilvusDataType.String  => ArrayBuffer[Array[String]]()
            case _ =>
              throw new DataTypeException(
                s"Unsupported data type: ${field.dataType}, field name: ${field.name}, element type: ${field.elementType}"
              )
          }
        }
        case MilvusDataType.Geometry =>
          ArrayBuffer[Array[Byte]]() // TODO: fubang support geometry
        case MilvusDataType.FloatVector    => ArrayBuffer[Array[Float]]()
        case MilvusDataType.BinaryVector   => ArrayBuffer[Array[Byte]]()
        case MilvusDataType.Int8Vector     => ArrayBuffer[Array[Short]]()
        case MilvusDataType.Float16Vector  => ArrayBuffer[Array[Float]]()
        case MilvusDataType.BFloat16Vector => ArrayBuffer[Array[Float]]()
        case MilvusDataType.SparseFloatVector =>
          ArrayBuffer[Map[Long, Float]]()
        case _ => ArrayBuffer[Any]()
      }
      field.name -> buffer
    }.toMap
  }

  def addDataToBuffer(
      buffer: Map[String, Any],
      row: InternalRow,
      fieldMap: Map[String, FieldSchema],
      sparkSchema: StructType
  ): Unit = {
    sparkSchema.names.zipWithIndex.foreach { case (fieldName, fieldIndex) =>
      val fieldSchema = fieldMap(fieldName)
      fieldSchema.dataType match {
        case MilvusDataType.Bool =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Boolean]] += row.getBoolean(fieldIndex)
        case MilvusDataType.Int8 =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Byte]] += row.getByte(fieldIndex)
        case MilvusDataType.Int16 =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Short]] += row.getShort(fieldIndex)
        case MilvusDataType.Int32 =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Int]] += row.getInt(fieldIndex)
        case MilvusDataType.Int64 =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Long]] += row.getLong(fieldIndex)
        case MilvusDataType.Float =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Float]] += row.getFloat(fieldIndex)
        case MilvusDataType.Double =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Double]] += row.getDouble(fieldIndex)
        case MilvusDataType.String =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[String]] += row.getString(fieldIndex)
        case MilvusDataType.VarChar =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[String]] += row.getString(fieldIndex)
        case MilvusDataType.JSON =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[String]] += row.getString(fieldIndex)
        case MilvusDataType.Array =>
          val arrayData = row
            .getArray(fieldIndex)
          val arrayElementType = fieldSchema.elementType match {
            case MilvusDataType.Bool => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Boolean]]] += arrayData
                .toArray[Boolean](
                  SparkDataTypes.BooleanType
                )
            }
            case MilvusDataType.Int8 => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Short]]] += arrayData
                .toArray[Short](
                  SparkDataTypes.ShortType
                )
            }
            case MilvusDataType.Int16 => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Short]]] += arrayData
                .toArray[Short](
                  SparkDataTypes.ShortType
                )
            }
            case MilvusDataType.Int32 => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Int]]] += arrayData
                .toArray[Int](
                  SparkDataTypes.IntegerType
                )
            }
            case MilvusDataType.Int64 => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Long]]] += arrayData
                .toArray[Long](
                  SparkDataTypes.LongType
                )
            }
            case MilvusDataType.Float => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Float]]] += arrayData
                .toArray[Float](
                  SparkDataTypes.FloatType
                )
            }
            case MilvusDataType.Double => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[Double]]] += arrayData
                .toArray[Double](
                  SparkDataTypes.DoubleType
                )
            }
            case MilvusDataType.VarChar => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[String]]] += arrayData
                .toSeq[UTF8String](SparkDataTypes.StringType)
                .map(_.toString)
                .toArray
            }
            case MilvusDataType.String => {
              buffer(fieldName)
                .asInstanceOf[ArrayBuffer[Array[String]]] += arrayData
                .toSeq[UTF8String](SparkDataTypes.StringType)
                .map(_.toString)
                .toArray
            }
            case _ =>
              throw new DataTypeException(
                s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, element type: ${fieldSchema.elementType}"
              )
          }
        case MilvusDataType.Geometry =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Array[Byte]]] += row
            .getBinary(fieldIndex) // TODO: fubang support geometry
        case MilvusDataType.FloatVector =>
          var bufferVectorData = buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Array[Float]]]
          val sparkField: StructField = sparkSchema(fieldIndex)
          sparkField.dataType match {
            case arrayType: ArrayType =>
              arrayType.elementType match {
                case SparkDataTypes.FloatType =>
                  bufferVectorData += row
                    .getArray(fieldIndex)
                    .toFloatArray
                case SparkDataTypes.DoubleType =>
                  bufferVectorData += row
                    .getArray(fieldIndex)
                    .toDoubleArray
                    .map(_.toFloat)
                case _ =>
                  throw new DataTypeException(
                    s"Unsupported data type: ${sparkField.dataType}, field name: ${fieldName}, element type: ${arrayType.elementType}"
                  )
              }
            case _ =>
              throw new DataTypeException(
                s"Unsupported data type: ${sparkField.dataType}, field name: ${fieldName}"
              )
          }
        case MilvusDataType.BinaryVector =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Array[Byte]]] += row
            .getArray(fieldIndex)
            .toByteArray()
        case MilvusDataType.Int8Vector =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Array[Short]]] += row
            .getArray(fieldIndex)
            .toShortArray()
        case MilvusDataType.Float16Vector =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Array[Float]]] += row
            .getArray(fieldIndex)
            .toFloatArray
        case MilvusDataType.BFloat16Vector =>
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Array[Float]]] += row
            .getArray(fieldIndex)
            .toFloatArray
        case MilvusDataType.SparseFloatVector =>
          val mapData = row
            .getMap(fieldIndex)
          val bufferMapData = Map.newBuilder[Long, Float]
          mapData.foreach(
            SparkDataTypes.LongType,
            SparkDataTypes.FloatType,
            (key, value) => {
              bufferMapData += (key.asInstanceOf[Long] -> value
                .asInstanceOf[Float])
            }
          )
          buffer(fieldName)
            .asInstanceOf[ArrayBuffer[Map[Long, Float]]] += bufferMapData
            .result()
        case _ =>
          throw new DataTypeException(
            s"Unsupported data type: ${fieldSchema.dataType}"
          )
      }
    }
  }

  def getInsertFieldsData(
      schema: CollectionSchema,
      buffer: Map[String, Any]
  ): Seq[FieldData] = {
    val fieldMap = getFieldMap(schema)
    val insertFieldsData = Seq.newBuilder[FieldData]
    buffer.foreach { case (fieldName, fieldBufferData) =>
      val fieldSchema = fieldMap(fieldName)
      fieldSchema.dataType match {
        case MilvusDataType.Bool =>
          insertFieldsData += MilvusFieldData.packBoolFieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Boolean]].toSeq
          )
        case MilvusDataType.Int8 =>
          insertFieldsData += MilvusFieldData.packInt8FieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Short]].toSeq
          )
        case MilvusDataType.Int16 =>
          insertFieldsData += MilvusFieldData.packInt16FieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Short]].toSeq
          )
        case MilvusDataType.Int32 =>
          insertFieldsData += MilvusFieldData.packInt32FieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Int]].toSeq
          )
        case MilvusDataType.Int64 =>
          insertFieldsData += MilvusFieldData.packInt64FieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Long]].toSeq
          )
        case MilvusDataType.Float =>
          insertFieldsData += MilvusFieldData.packFloatFieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Float]].toSeq
          )
        case MilvusDataType.Double =>
          insertFieldsData += MilvusFieldData.packDoubleFieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[Double]].toSeq
          )
        case MilvusDataType.String =>
          insertFieldsData += MilvusFieldData.packStringFieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[String]].toSeq
          )
        case MilvusDataType.VarChar =>
          insertFieldsData += MilvusFieldData.packStringFieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[String]].toSeq
          )
        case MilvusDataType.JSON =>
          insertFieldsData += MilvusFieldData.packJsonFieldData(
            fieldName,
            fieldBufferData.asInstanceOf[ArrayBuffer[String]].toSeq
          )
        case MilvusDataType.Array =>
          fieldSchema.elementType match {
            case MilvusDataType.Bool => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Boolean]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packBoolFieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.Int8 => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Short]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packInt8FieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.Int16 => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Short]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packInt16FieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.Int32 => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Int]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packInt32FieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.Int64 => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Long]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packInt64FieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.Float => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Float]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packFloatFieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.Double => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[Double]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packDoubleFieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.VarChar => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[String]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packStringFieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case MilvusDataType.String => {
              insertFieldsData += MilvusFieldData.packArrayFieldData(
                fieldName,
                fieldBufferData
                  .asInstanceOf[ArrayBuffer[Array[String]]]
                  .map(f => {
                    val tmpFieldData = MilvusFieldData.packStringFieldData(
                      fieldName,
                      f.toSeq
                    )
                    tmpFieldData.field.scalars.getOrElse(
                      throw new DataTypeException(
                        s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}, scalar data is empty"
                      )
                    )
                  })
                  .toSeq,
                fieldSchema.elementType
              )
            }
            case _ => {
              throw new DataTypeException(
                s"Unsupported data type: ${fieldSchema.dataType}, field name: ${fieldName}"
              )
            }
          }
        case MilvusDataType.Geometry =>
          insertFieldsData += MilvusFieldData.packGeometryFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Array[Byte]]]
              .toSeq
              .map(
                _.toSeq
              )
          )
        case MilvusDataType.FloatVector =>
          val dim = MilvusSchemaUtil.getDim(fieldSchema)
          insertFieldsData += MilvusFieldData.packFloatVectorFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Array[Float]]]
              .toSeq
              .map(_.toSeq),
            dim
          )
        case MilvusDataType.BinaryVector =>
          val dim = MilvusSchemaUtil.getDim(fieldSchema)
          insertFieldsData += MilvusFieldData.packBinaryVectorFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Array[Byte]]]
              .toSeq
              .map(_.toSeq),
            dim
          )
        case MilvusDataType.Int8Vector =>
          val dim = MilvusSchemaUtil.getDim(fieldSchema)
          insertFieldsData += MilvusFieldData.packInt8VectorFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Array[Short]]]
              .toSeq
              .map(_.toSeq),
            dim
          )
        case MilvusDataType.Float16Vector =>
          val dim = MilvusSchemaUtil.getDim(fieldSchema)
          insertFieldsData += MilvusFieldData.packFloat16VectorFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Array[Float]]]
              .toSeq
              .map(_.toSeq),
            dim
          )
        case MilvusDataType.BFloat16Vector =>
          val dim = MilvusSchemaUtil.getDim(fieldSchema)
          insertFieldsData += MilvusFieldData.packBFloat16VectorFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Array[Float]]]
              .toSeq
              .map(_.toSeq),
            dim
          )
        case MilvusDataType.SparseFloatVector =>
          val dim = MilvusSchemaUtil.getDim(fieldSchema)
          insertFieldsData += MilvusFieldData.packSparseFloatVectorFieldData(
            fieldName,
            fieldBufferData
              .asInstanceOf[ArrayBuffer[Map[Long, Float]]]
              .toSeq,
            dim
          )
        case _ =>
          throw new DataTypeException(
            s"Unsupported data type: ${fieldSchema.dataType}"
          )
      }
    }
    insertFieldsData.result()
  }

  def getFieldMap(schema: CollectionSchema): Map[String, FieldSchema] = {
    schema.fields.map { field =>
      field.name -> field
    }.toMap
  }
}
