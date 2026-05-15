package com.zilliz.spark.connector.read

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import com.zilliz.spark.connector.MilvusOption
import io.milvus.grpc.schema.CollectionSchema

// PartitionReaderFactory for Storage V2 (Milvus 2.6+)
class MilvusPartitionReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String],
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReaderFactory
    with Logging {

  private val readDebugEnabled = optionsMap
    .get(MilvusOption.ReaderDebug)
    .exists(_.equalsIgnoreCase("true"))

  private def debugRead(message: => String): Unit = {
    if (readDebugEnabled) logInfo(s"[MilvusReadDebug] $message")
  }

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    partition match {
      case p: MilvusStorageV3InputPartition =>
        logInfo(
          s"Creating V3 reader for partition with segmentID=${p.segmentID}"
        )

        val v2Schema = StructType(schema.fields.filter { field =>
          field.name != MilvusOption.MilvusExtraColumnSegmentID &&
          field.name != MilvusOption.MilvusExtraColumnRowOffset
        })

        // Deserialize the protobuf schema
        val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)
        debugRead(
          s"factory creating readerType=V3 segmentID=${p.segmentID} " +
            s"partition=${p.partitionName} manifestPath=${p.manifestPath} " +
            s"readVersion=${p.readVersion} " +
            s"requestedSchema=${schema.fieldNames.mkString(",")} " +
            s"innerSchema=${v2Schema.fieldNames.mkString(",")}"
        )

        // Create MilvusLoonPartitionReader directly
        val underlyingReader = new MilvusLoonPartitionReader(
          v2Schema,
          p.manifestPath,
          milvusSchema,
          p.milvusOption,
          optionsMap,
          p.topK,
          p.queryVector,
          p.metricType,
          p.vectorColumn,
          pushedFilters,
          p.readVersion
        )

        val hasSegmentId = schema.fieldNames.contains(
          MilvusOption.MilvusExtraColumnSegmentID
        )
        val hasRowOffset = schema.fieldNames.contains(
          MilvusOption.MilvusExtraColumnRowOffset
        )

        if (hasSegmentId || hasRowOffset) {
          new PartitionReader[InternalRow] {
            private var rowOffset: Long = 0L

            override def next(): Boolean = underlyingReader.next()

            override def get(): InternalRow = {
              val row = underlyingReader.get()
              val resultValues = new Array[Any](schema.fields.length)
              var readIdx = 0

              schema.fields.zipWithIndex.foreach { case (field, writeIdx) =>
                field.name match {
                  case MilvusOption.MilvusExtraColumnSegmentID =>
                    resultValues(writeIdx) = p.segmentID
                  case MilvusOption.MilvusExtraColumnRowOffset =>
                    resultValues(writeIdx) = rowOffset
                  case _ =>
                    resultValues(writeIdx) = row.get(readIdx, field.dataType)
                    readIdx += 1
                }
              }
              rowOffset += 1

              InternalRow.fromSeq(resultValues.toSeq)
            }

            override def close(): Unit = underlyingReader.close()
          }
        } else {
          underlyingReader
        }

      case p: MilvusPackedV2InputPartition =>
        logInfo(
          s"Creating packed-V2 reader for segmentID=${p.segmentID} " +
            s"with ${p.columnGroups.size} column group(s)"
        )

        val innerSchema = StructType(schema.fields.filter { f =>
          f.name != MilvusOption.MilvusExtraColumnSegmentID &&
          f.name != MilvusOption.MilvusExtraColumnRowOffset
        })

        val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)
        debugRead(
          s"factory creating readerType=V2 segmentID=${p.segmentID} " +
            s"partitionID=${p.partitionID} columnGroups=${p.columnGroups.size} " +
            s"neededColumnFieldIds=${p.neededColumnFieldIds.mkString(",")} " +
            s"requestedSchema=${schema.fieldNames.mkString(",")} " +
            s"innerSchema=${innerSchema.fieldNames.mkString(",")}"
        )

        val underlying = new MilvusPackedV2PartitionReader(
          innerSchema,
          p.columnGroups,
          milvusSchema,
          p.milvusOption,
          p.neededColumnFieldIds
        )

        val hasSegmentId = schema.fieldNames.contains(
          MilvusOption.MilvusExtraColumnSegmentID
        )
        val hasRowOffset = schema.fieldNames.contains(
          MilvusOption.MilvusExtraColumnRowOffset
        )

        if (hasSegmentId || hasRowOffset) {
          new PartitionReader[InternalRow] {
            private var rowOffset: Long = 0L

            override def next(): Boolean = underlying.next()

            override def get(): InternalRow = {
              val row = underlying.get()
              val out = new Array[Any](schema.fields.length)
              var readIdx = 0

              schema.fields.zipWithIndex.foreach { case (field, writeIdx) =>
                field.name match {
                  case MilvusOption.MilvusExtraColumnSegmentID =>
                    out(writeIdx) = p.segmentID
                  case MilvusOption.MilvusExtraColumnRowOffset =>
                    out(writeIdx) = rowOffset
                  case _ =>
                    out(writeIdx) = row.get(readIdx, field.dataType)
                    readIdx += 1
                }
              }
              rowOffset += 1

              InternalRow.fromSeq(out.toSeq)
            }

            override def close(): Unit = underlying.close()
          }
        } else {
          underlying
        }

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported partition type: ${partition.getClass.getName}. " +
            "This connector requires Milvus 2.6+ (Storage V2)."
        )
    }
  }
}
