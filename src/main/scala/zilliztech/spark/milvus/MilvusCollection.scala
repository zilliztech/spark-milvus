package zilliztech.spark.milvus

import io.milvus.grpc.{CollectionSchema, DataType, ErrorCode}
import io.milvus.param.collection.{DescribeCollectionParam, FieldType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types._
import zilliztech.spark.milvus.MilvusCollection.ToSparkSchema
import zilliztech.spark.milvus.writer.MilvusWriteBuilder

import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * @param milvusOptions
 */
case class MilvusCollection(milvusOptions: MilvusOptions, sparkSchema: Option[StructType]) extends SupportsWrite {

  lazy val milvusCollectionMeta = initCollectionMeta()

  lazy val collectionSchema = milvusCollectionMeta.schema

  lazy val collectionID = milvusCollectionMeta.id

  lazy val sparkSession = SparkSession.active

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    MilvusWriteBuilder(milvusOptions)

  override def schema(): StructType = {
    if (sparkSchema.isEmpty) {
      ToSparkSchema(collectionSchema)
    } else {
      sparkSchema.get
    }
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE,
//    TableCapability.BATCH_READ
  ).asJava

  override def name(): String = milvusOptions.collectionName

  def initCollectionMeta(): MilvusCollectionMeta = {
    val client = MilvusConnection.acquire(milvusOptions)
    val describeCollectionParams = DescribeCollectionParam.newBuilder
      .withDatabaseName(milvusOptions.databaseName)
      .withCollectionName(milvusOptions.collectionName)
      .build()
    val response = client.describeCollection(describeCollectionParams)
    if (response.getStatus != ErrorCode.Success.getNumber) {
      if (response.getException != None) {
        throw new Exception("Fail to describe collection", response.getException)
      }
      throw new Exception(s"Fail to describe collection response: ${response.toString}")
    }
    client.close()
    MilvusCollectionMeta(response.getData.getSchema, response.getData.getCollectionID)
  }

//  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
//    MilvusCollectionScanBuilder2(sparkSession, schema(), options, sparkSession.sparkContext.hadoopConfiguration)
//  }

}

case class MilvusCollectionMeta(schema: CollectionSchema, id: Long)

object MilvusCollection {

  def ToSparkSchema(collectionSchema: CollectionSchema): StructType = {
    StructType(
      collectionSchema.getFieldsList.map(
        field => StructField(field.getName,ToSparkDataType(field.getDataType))
      ).toArray
    )
  }

  def ToMilvusField(field: StructField, milvusOptions: MilvusOptions): FieldType = {
    val fieldBuilder = FieldType.newBuilder.withName(field.name)
    field.dataType match {
      case BooleanType => fieldBuilder.withDataType(DataType.Bool)
      case ShortType =>
        if (milvusOptions.primaryKeyField.equals(field.name)) {
          fieldBuilder.withDataType(DataType.Int64)
        } else {
          fieldBuilder.withDataType(DataType.Int16)
        }
      case IntegerType =>
        if (milvusOptions.primaryKeyField.equals(field.name)) {
          fieldBuilder.withDataType(DataType.Int64)
        } else {
          fieldBuilder.withDataType(DataType.Int32)
        }
      case LongType => fieldBuilder.withDataType(DataType.Int64)
      case FloatType => fieldBuilder.withDataType(DataType.Float)
      case DoubleType => fieldBuilder.withDataType(DataType.Double)
      case VarcharType(x) => fieldBuilder.withDataType(DataType.VarChar).withMaxLength(x)
      case StringType => fieldBuilder.withDataType(DataType.VarChar).withMaxLength(65535)
      case BinaryType =>
        if (milvusOptions.vectorField.equals(field.name)) {
          fieldBuilder.withDataType(DataType.BinaryVector)
          fieldBuilder.withMaxLength(milvusOptions.vectorDim)
        }
      case ArrayType(FloatType, _) =>
        if (milvusOptions.vectorField.equals(field.name)) {
          fieldBuilder.withDataType(DataType.FloatVector)
          fieldBuilder.withDimension(milvusOptions.vectorDim)
        } else {
          // Support array
        }
      case ArrayType(DoubleType, _) =>
        if (milvusOptions.vectorField.equals(field.name)) {
          fieldBuilder.withDataType(DataType.FloatVector)
          fieldBuilder.withDimension(milvusOptions.vectorDim)
        } else {
          // Support array
        }
      case _ => throw new Exception(s"Unsupported data type ${field.dataType.typeName}")
    }

    if (milvusOptions.primaryKeyField.equals(field.name)) {
      fieldBuilder.withPrimaryKey(true)
      fieldBuilder.withAutoID(milvusOptions.isAutoID)
    }

    fieldBuilder.build()
  }

  def ToSparkDataType(dataType: io.milvus.grpc.DataType): org.apache.spark.sql.types.DataType = {
    dataType match {
      case DataType.Bool => BooleanType
      case DataType.Int8 => ShortType
      case DataType.Int16 => ShortType
      case DataType.Int32 => IntegerType
      case DataType.Int64 => LongType
      case DataType.Float => FloatType
      case DataType.Double => DoubleType
      case DataType.VarChar => VarcharType(65535)
      case DataType.JSON => VarcharType(65535)
//      case DataType.BinaryVector => ArrayType(BinaryType) // todo
//      case DataType.Array => ArrayType(BinaryType) // todo
      // vector
      case DataType.FloatVector => ArrayType(FloatType)
//      case DataType.Float16Vector => ArrayType(FloatType)
//      case DataType.BFloat16Vector => ArrayType(BinaryType) // todo
//      case DataType.SparseFloatVector => ArrayType(FloatType) // todo
    }
  }
}

