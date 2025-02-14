package zilliztech.spark.milvus.writer

import com.google.gson.{JsonElement, JsonParser}
import io.milvus.grpc.{CollectionSchema, DataType, ErrorCode}
import io.milvus.param.dml.InsertParam
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.writer.MilvusDataWriter.{addRowToBuffer, newInsertBuffer}
import zilliztech.spark.milvus.{MilvusCollection, MilvusConnection, MilvusOptions, MilvusUtils}

import java.nio.ByteBuffer
import java.util
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

case class MilvusDataWriter(partitionId: Int, taskId: Long, milvusOptions: MilvusOptions) extends DataWriter[InternalRow]
  with Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  private val milvusClient = MilvusConnection.acquire(milvusOptions)
  private val milvusCollection = new MilvusCollection(milvusOptions, Option.empty)

  private val maxBatchSize = milvusOptions.maxBatchSize

  private val milvusSchema = milvusCollection.collectionSchema

  private var buffer = newInsertBuffer(milvusSchema)

  private var currentSizeInBuffer = 0
  private var totalSize = 0

  override def write(record: InternalRow): Unit = {
    try {
      addRowToBuffer(record, milvusSchema, buffer)
      currentSizeInBuffer = currentSizeInBuffer + 1
      totalSize = totalSize + 1

      if (currentSizeInBuffer >= maxBatchSize) {
        var builder = InsertParam.newBuilder
        if (!(milvusOptions.isZillizCloud() && milvusCollection.milvusOptions.databaseName.equals(""))) {
          builder = builder.withDatabaseName(milvusCollection.milvusOptions.databaseName)
        }
        if (milvusCollection.milvusOptions.partitionName.isEmpty) {
          builder = builder.withPartitionName(milvusCollection.milvusOptions.partitionName)
        }
        val insertParam = builder
          .withCollectionName(milvusCollection.milvusOptions.collectionName)
          .withFields(buffer)
          .build

        val insertR = milvusClient.withTimeout(10, TimeUnit.SECONDS).insert(insertParam)
        log.debug(s"insert batch status ${insertR.toString} size: ${currentSizeInBuffer}")
        if (insertR.getStatus != ErrorCode.Success.getNumber) {
          throw new Exception(s"Fail to insert batch: ${insertR.toString}")
        }
        buffer = newInsertBuffer(milvusSchema)
        currentSizeInBuffer = 0
      }
    } catch {
      case e: Exception =>
        log.error(s"Exception occurs: ${e.getMessage}")
        throw e
    }
  }

  override def commit(): WriterCommitMessage = {
    if (currentSizeInBuffer > 0) {
      var builder = InsertParam.newBuilder
      if (milvusCollection.milvusOptions.databaseName.nonEmpty) {
        builder = builder.withDatabaseName(milvusCollection.milvusOptions.databaseName)
      }
      if (milvusCollection.milvusOptions.partitionName.nonEmpty) {
        builder = builder.withPartitionName(milvusCollection.milvusOptions.partitionName)
      }
      val insertParam = builder
        .withCollectionName(milvusCollection.milvusOptions.collectionName)
        .withFields(buffer)
        .build
      val insertR = milvusClient.withTimeout(10, TimeUnit.SECONDS).insert(insertParam)
      log.info(s"commit insert status ${insertR.getStatus.toString} size: ${currentSizeInBuffer}, toString: ${insertR.toString}")
      if (insertR.getStatus != ErrorCode.Success.getNumber) {
        throw new Exception(s"Fail to commit insert: ${insertR.toString}")
      }
      buffer = newInsertBuffer(milvusSchema)
      currentSizeInBuffer = 0
    }
    MilvusCommitMessage(totalSize)
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    commit()
    milvusClient.close()
    log.info(s"finished insert size: ${totalSize}")
  }
}

object MilvusDataWriter {
  private val log = LoggerFactory.getLogger(getClass)

  def newInsertBuffer(schema: CollectionSchema): util.ArrayList[InsertParam.Field] = {
    val fieldsInsert: util.ArrayList[InsertParam.Field] = new util.ArrayList[InsertParam.Field]
    for (i: Int <- 0 to schema.getFieldsCount - 1) {
      val fieldList = schema.getFields(i).getDataType match {
        case DataType.Bool => new util.ArrayList[Boolean]()
        case DataType.Int8 => new util.ArrayList[Short]()
        case DataType.Int16 => new util.ArrayList[Short]()
        case DataType.Int32 => new util.ArrayList[Int]()
        case DataType.Int64 => new util.ArrayList[Long]()
        case DataType.Float => new util.ArrayList[Float]()
        case DataType.Double => new util.ArrayList[Double]()
        case DataType.String => new util.ArrayList[String]()
        case DataType.VarChar => new util.ArrayList[String]()
        case DataType.JSON => new util.ArrayList[JsonElement]()
        case DataType.Array => {
          val elementType = schema.getFields(i).getElementType
          val convertType = elementType match {
            case DataType.Bool => new util.ArrayList[util.ArrayList[Boolean]]()
            case DataType.Int8 => new util.ArrayList[util.ArrayList[Short]]()
            case DataType.Int16 => new util.ArrayList[util.ArrayList[Short]]()
            case DataType.Int32 => new util.ArrayList[util.ArrayList[Int]]()
            case DataType.Int64 => new util.ArrayList[util.ArrayList[Long]]()
            case DataType.Float => new util.ArrayList[util.ArrayList[Float]]()
            case DataType.Double => new util.ArrayList[util.ArrayList[Double]]()
            case DataType.String => new util.ArrayList[util.ArrayList[String]]()
            case DataType.VarChar => new util.ArrayList[util.ArrayList[String]]()
          }
          convertType
        }
        case DataType.BinaryVector => new util.ArrayList[ByteBuffer]()
        case DataType.FloatVector => new util.ArrayList[util.ArrayList[Float]]()
        case DataType.Float16Vector => new util.ArrayList[ByteBuffer]()
        case DataType.BFloat16Vector => new util.ArrayList[ByteBuffer]()
        case DataType.SparseFloatVector => new util.ArrayList[util.SortedMap[Long, Float]]()
      }
      fieldsInsert.add(new InsertParam.Field(schema.getFields(i).getName, fieldList))
    }
    fieldsInsert
  }

  def addRowToBuffer(record: InternalRow, schema: CollectionSchema, buffer: util.ArrayList[InsertParam.Field]): util.ArrayList[InsertParam.Field] = {
    for (i: Int <- 0 to schema.getFieldsCount - 1) {
      schema.getFields(i).getDataType match {
        case DataType.Bool => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Boolean]].add(record.getBoolean(i))
        case DataType.Int8 => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Short]].add(record.getShort(i))
        case DataType.Int16 => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Short]].add(record.getShort(i))
        case DataType.Int32 => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Int]].add(record.getInt(i))
        case DataType.Int64 => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Long]].add(record.getLong(i))
        case DataType.Float => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Float]].add(record.getFloat(i))
        case DataType.Double => buffer.get(i).getValues.asInstanceOf[util.ArrayList[Double]].add(record.getDouble(i))
        case DataType.String => buffer.get(i).getValues.asInstanceOf[util.ArrayList[String]].add(record.getString(i))
        case DataType.VarChar => buffer.get(i).getValues.asInstanceOf[util.ArrayList[String]].add(record.getString(i))
        case DataType.JSON => {
          val json = JsonParser.parseString(record.getString(i))
          buffer.get(i).getValues.asInstanceOf[util.ArrayList[JsonElement]].add(json)
        }
        case DataType.Array => {
          val elementType = schema.getFields(i).getElementType
          elementType match {
            case DataType.Bool => {
              val arr = record.getArray(i).toBooleanArray()
              val ele = new util.ArrayList[Boolean](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Boolean]]].add(ele)
            }
            case DataType.Int8 => {
              val arr = record.getArray(i).toShortArray()
              val ele = new util.ArrayList[Short](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Short]]].add(ele)
            }
            case DataType.Int16 => {
              val arr = record.getArray(i).toShortArray()
              val ele = new util.ArrayList[Short](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Short]]].add(ele)
            }
            case DataType.Int32 => {
              val arr = record.getArray(i).toIntArray()
              val ele = new util.ArrayList[Int](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Int]]].add(ele)
            }
            case DataType.Int64 => {
              val arr = record.getArray(i).toLongArray()
              val ele = new util.ArrayList[Long](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Long]]].add(ele)
            }
            case DataType.Float => {
              val arr = record.getArray(i).toFloatArray()
              val ele = new util.ArrayList[Float](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Float]]].add(ele)
            }
            case DataType.Double => {
              val arr = record.getArray(i).toDoubleArray()
              val ele = new util.ArrayList[Double](arr.toBuffer.asJava)
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Double]]].add(ele)
            }
            case DataType.String => {
              val arr = record.getArray(i).toSeq[UTF8String](StringType)
              val javaList = new util.ArrayList[String]()
              arr.foreach(utf8Str => javaList.add(utf8Str.toString))
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[String]]].add(javaList)
            }
            case DataType.VarChar => {
              val arr = record.getArray(i).toSeq[UTF8String](StringType)
              val javaList = new util.ArrayList[String]()
              arr.foreach(utf8Str => javaList.add(utf8Str.toString))
              buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[String]]].add(javaList)
            }
          }
        }
        case DataType.BinaryVector => {
          val dim = MilvusUtils.getVectorDim(schema.getFields(i))
          val recordStr = record.getString(i)
          // handle if the input is base64 encoded
          val bytes = if (recordStr.length * 8 == dim) {
            recordStr.getBytes
          } else {
            Base64.getDecoder.decode(recordStr)
          }
          // quite ridiculous I can't use ByteBuffer.wrap(vector). it will fail in Java SDK checkFieldData
          // val vector = ByteBuffer.wrap(vector)
          val vector = ByteBuffer.allocate(bytes.length)
          for (i <- 0 until bytes.length) {
            vector.put(bytes(i))
          }
          buffer.get(i).getValues.asInstanceOf[util.ArrayList[ByteBuffer]].add(vector)
        }
        case DataType.FloatVector => {
          val vectorList = buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.ArrayList[Float]]]
          val vector = record.getArray(i).toFloatArray()
          val javaList: util.ArrayList[Float] = new util.ArrayList[Float](vector.length)
          for (element <- vector) {
            element match {
              case floatValue: Float => javaList.add(floatValue)
              case _ => throw new IllegalArgumentException("Unsupported element type")
            }
          }
          vectorList.add(javaList)
        }
        case DataType.Float16Vector => {
          val floatArr = record.getArray(i).toFloatArray()
          // quite ridiculous I can't use ByteBuffer.wrap(vector). it will fail in Java SDK checkFieldData
          // val vector = ByteBuffer.wrap(vector)
          val vector = ByteBuffer.allocate(floatArr.length * 2)
          for (i <- 0 until floatArr.length) {
            vector.put(MilvusUtils.convertFloatToFloat16ByteArray(floatArr(i)))
          }
          buffer.get(i).getValues.asInstanceOf[util.ArrayList[ByteBuffer]].add(vector)
        }
        case DataType.BFloat16Vector => {
          val dim = MilvusUtils.getVectorDim(schema.getFields(i))
          val recordStr = record.getString(i)
          // handle if the input is base64 encoded
          val bytes = if (recordStr.length * 8 == dim) {
            recordStr.getBytes
          } else {
            Base64.getDecoder.decode(recordStr)
          }
          // quite ridiculous I can't use ByteBuffer.wrap(vector). it will fail in Java SDK checkFieldData
          // val vector = ByteBuffer.wrap(vector)
          val vector = ByteBuffer.allocate(bytes.length)
          for (i <- 0 until bytes.length) {
            vector.put(bytes(i))
          }
          buffer.get(i).getValues.asInstanceOf[util.ArrayList[ByteBuffer]].add(vector)
        }
        case DataType.SparseFloatVector => {
          val json = JsonParser.parseString(record.getString(i)).getAsJsonObject()
          val vector = MilvusUtils.jsonToSparseVector(json)
          buffer.get(i).getValues.asInstanceOf[util.ArrayList[util.SortedMap[Long, Float]]].add(vector)
        }
      }
    }
    buffer
  }
}
